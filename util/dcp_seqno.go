package util

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/collections"
	couchbase "github.com/couchbase/eventing/dcp"
	memcached "github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
)

const seqsReqChanSize = 20000
const seqsBufSize = 64 * 1024

var errConnClosed = errors.New("dcpSeqnos - conn closed already")

// cache Bucket{} and DcpFeed{} objects, its underlying connections
// to make Stats-Seqnos fast.
var dcp_buckets_seqnos struct {
	rw        sync.RWMutex
	numVbs    int
	buckets   map[string]*couchbase.Bucket // bucket ->*couchbase.Bucket
	errors    map[string]error             // bucket -> error
	readerMap map[string]*vbSeqnosReader   // bucket->*vbSeqnosReader
}

func init() {
	dcp_buckets_seqnos.buckets = make(map[string]*couchbase.Bucket)
	dcp_buckets_seqnos.errors = make(map[string]error)
	dcp_buckets_seqnos.readerMap = make(map[string]*vbSeqnosReader)

	go pollForDeletedBuckets()
}

type vbSeqnosResponse struct {
	seqnos []uint64
	err    error
}

type kvConn struct {
	mc      *memcached.Client
	seqsbuf []uint64
	tmpbuf  []byte
}

func newKVConn(mc *memcached.Client) *kvConn {
	return &kvConn{mc: mc, seqsbuf: make([]uint64, 1024), tmpbuf: make([]byte, seqsBufSize)}
}

type vbSeqnosRequest struct {
	cid         uint32
	bucketLevel bool
	respCh      chan *vbSeqnosResponse
}

func (req *vbSeqnosRequest) Reply(response *vbSeqnosResponse) {
	req.respCh <- response
}

func (req *vbSeqnosRequest) Response() ([]uint64, error) {
	response := <-req.respCh
	return response.seqnos, response.err
}

// Bucket level seqnos reader for the cluster
type vbSeqnosReader struct {
	bucket    string
	kvfeeds   map[string]*kvConn
	requestCh chan vbSeqnosRequest
}

func newVbSeqnosReader(bucket string, kvfeeds map[string]*kvConn) *vbSeqnosReader {
	r := &vbSeqnosReader{
		bucket:    bucket,
		kvfeeds:   kvfeeds,
		requestCh: make(chan vbSeqnosRequest, seqsReqChanSize),
	}

	go r.Routine()
	return r
}

func (r *vbSeqnosReader) Close() {
	close(r.requestCh)
}

func (r *vbSeqnosReader) GetBucketLevelSeqnos() (seqs []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errConnClosed
		}
	}()

	req := vbSeqnosRequest{
		cid:         collections.CID_FOR_BUCKET,
		bucketLevel: true,
		respCh:      make(chan *vbSeqnosResponse, 1),
	}
	r.requestCh <- req
	seqs, err = req.Response()
	return
}

func (r *vbSeqnosReader) GetScopeOrCollectionSeqnos(cid uint32) (seqs []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errConnClosed
		}
	}()

	req := vbSeqnosRequest{
		cid:    cid,
		respCh: make(chan *vbSeqnosResponse, 1),
	}

	r.requestCh <- req
	seqs, err = req.Response()
	return
}

// This routine is responsible for computing request batches on the fly
// and issue single 'dcp seqno' per batch.
func (r *vbSeqnosReader) Routine() {
	for req := range r.requestCh {
		l := len(r.requestCh)
		seqnos, err := CollectSeqnos(r.kvfeeds, req.bucketLevel, req.cid)
		response := &vbSeqnosResponse{
			seqnos: seqnos,
			err:    err,
		}
		if err != nil {
			dcp_buckets_seqnos.rw.Lock()
			dcp_buckets_seqnos.errors[r.bucket] = err
			dcp_buckets_seqnos.rw.Unlock()
		}
		req.Reply(response)

		// Read outstanding requests that can be served by
		// using the same response
		for i := 0; i < l; i++ {
			req := <-r.requestCh
			req.Reply(response)
		}
	}

	// Cleanup all feeds
	for _, kvfeed := range r.kvfeeds {
		kvfeed.mc.Close()
	}
}

func addDBSbucket(cluster, pooln, bucketn string) (err error) {
	var bucket *couchbase.Bucket
	var clusterVer uint32

	bucket, clusterVer, err = ConnectBucket(cluster, pooln, bucketn)
	if err != nil {
		logging.Errorf("Unable to connect with bucket %q\n", bucketn)
		return err
	}

	kvfeeds := make(map[string]*kvConn)

	defer func() {
		if err == nil {
			dcp_buckets_seqnos.buckets[bucketn] = bucket
			dcp_buckets_seqnos.readerMap[bucketn] = newVbSeqnosReader(bucketn, kvfeeds)
		} else {
			for _, kvfeed := range kvfeeds {
				kvfeed.mc.Close()
			}
		}
	}()

	// get all kv-nodes
	if err = bucket.Refresh(); err != nil {
		logging.Errorf("bucket.Refresh(): %v\n", err)
		return err
	}

	// get current list of kv-nodes
	var m map[string][]uint16
	m, err = bucket.GetVBmap(nil)
	if err != nil {
		logging.Errorf("GetVBmap() failed: %v\n", err)
		return err
	}
	// calculate and cache the number of vbuckets.
	if dcp_buckets_seqnos.numVbs == 0 { // to happen only first time.
		for _, vbnos := range m {
			dcp_buckets_seqnos.numVbs += len(vbnos)
		}
	}

	if dcp_buckets_seqnos.numVbs == 0 {
		err = fmt.Errorf("Found 0 vbuckets - perhaps the bucket is not ready yet")
		return
	}

	// make sure a feed is available for all kv-nodes
	var conn *memcached.Client

	for kvaddr := range m {
		conn, err = bucket.GetMcConn(kvaddr)
		if err != nil {
			logging.Errorf("StartDcpFeedOver(): %v\n", err)
			return err
		}

		err := tryEnableCollection(conn, clusterVer)
		if err != nil {
			logging.Errorf("StartDcpFeedOver(): %v\n", err)
			return err
		}
		kvfeeds[kvaddr] = newKVConn(conn)
	}

	logging.Infof("{bucket,feeds} %q created for dcp_seqno cache...\n", bucketn)
	return nil
}

func delDBSbucket(bucketn string, checkErr bool) {
	dcp_buckets_seqnos.rw.Lock()
	defer dcp_buckets_seqnos.rw.Unlock()

	if !checkErr || dcp_buckets_seqnos.errors[bucketn] != nil {
		bucket, ok := dcp_buckets_seqnos.buckets[bucketn]
		if ok && bucket != nil {
			bucket.Close()
		}
		delete(dcp_buckets_seqnos.buckets, bucketn)

		reader, ok := dcp_buckets_seqnos.readerMap[bucketn]
		if ok && reader != nil {
			reader.Close()
		}
		delete(dcp_buckets_seqnos.readerMap, bucketn)

		delete(dcp_buckets_seqnos.errors, bucketn)
	}
}

// BucketSeqnos return list of {{vbno,seqno}..} for all vbuckets.
// this call might fail due to,
// - concurrent access that can preserve a deleted/failed bucket object.
// - pollForDeletedBuckets() did not get a chance to cleanup
//   a deleted bucket.
// in both the cases if the call is retried it should get fixed, provided
// a valid bucket exists.
func BucketSeqnos(cluster, pooln, bucketn string) (l_seqnos []uint64, err error) {
	// any type of error will cleanup the bucket and its kvfeeds.
	defer func() {
		if err != nil {
			delDBSbucket(bucketn, true)
		}
	}()

	var reader *vbSeqnosReader

	reader, err = func() (*vbSeqnosReader, error) {
		dcp_buckets_seqnos.rw.RLock()
		reader, ok := dcp_buckets_seqnos.readerMap[bucketn]
		dcp_buckets_seqnos.rw.RUnlock()
		if !ok { // no {bucket,kvfeeds} found, create!
			dcp_buckets_seqnos.rw.Lock()
			defer dcp_buckets_seqnos.rw.Unlock()

			// Recheck if reader is still not present since we acquired write lock
			// after releasing the read lock.
			if reader, ok = dcp_buckets_seqnos.readerMap[bucketn]; !ok {
				if err = addDBSbucket(cluster, pooln, bucketn); err != nil {
					return nil, err
				}
				// addDBSbucket has populated the reader
				reader = dcp_buckets_seqnos.readerMap[bucketn]
			}
		}
		return reader, nil
	}()
	if err != nil {
		return nil, err
	}

	l_seqnos, err = reader.GetBucketLevelSeqnos()
	return
}

func ScopeOrCollectionSeqnos(cluster, pooln, bucketn string,
	id uint32) (l_seqnos []uint64, locationFound bool, err error) {

	// any type of error will cleanup the bucket and its kvfeeds.
	defer func() {
		if err != nil {
			delDBSbucket(bucketn, true)
		}
	}()

	var reader *vbSeqnosReader

	reader, err = func() (*vbSeqnosReader, error) {
		dcp_buckets_seqnos.rw.RLock()
		reader, ok := dcp_buckets_seqnos.readerMap[bucketn]
		dcp_buckets_seqnos.rw.RUnlock()
		if !ok { // no {bucket,kvfeeds} found, create!
			dcp_buckets_seqnos.rw.Lock()
			defer dcp_buckets_seqnos.rw.Unlock()

			// Recheck if reader is still not present since we acquired write lock
			// after releasing the read lock.
			if reader, ok = dcp_buckets_seqnos.readerMap[bucketn]; !ok {
				if err = addDBSbucket(cluster, pooln, bucketn); err != nil {
					return nil, err
				}
				// addDBSbucket has populated the reader
				reader = dcp_buckets_seqnos.readerMap[bucketn]
			}
		}
		return reader, nil
	}()

	locationFound = true
	if err != nil {
		return nil, false, err
	}

	l_seqnos, err = reader.GetScopeOrCollectionSeqnos(id)
	if err != nil && strings.Contains(err.Error(), "Unknown scope or collection") {
		return nil, false, nil
	}
	return
}

func GetSeqnos(args ...interface{}) error {
	cluster := args[0].(string)
	pool := args[1].(string)
	bucket := args[2].(string)
	streamKeyspace := args[3].(common.KeyspaceID)
	l_seqnos := args[4].(*[]uint64)
	bucketLevel := args[5].(bool)

	id := streamKeyspace.Cid
	if streamKeyspace.StreamType == common.STREAM_SCOPE {
		id = streamKeyspace.Sid
	}

	if streamKeyspace.StreamType != common.STREAM_BUCKET && !bucketLevel {
		seqnos, locationFound, err := ScopeOrCollectionSeqnos(cluster, pool, bucket, id)
		if !locationFound {
			logging.Infof("No Location exist with id: %v", id)
			return nil
		}
		if err == nil {
			*l_seqnos = seqnos
			return nil
		}
		if strings.Contains(err.Error(), "No bucket") {
			logging.Infof("No bucket named %s found", bucket)
			return nil
		}
		logging.Warnf("Got error while trying to get collection sequence numbers: %v Retrying...", err)
		return err
	}
	seqnos, err := BucketSeqnos(cluster, pool, bucket)
	if err == nil {
		*l_seqnos = seqnos
		return nil
	}
	if strings.Contains(err.Error(), "No bucket") {
		logging.Infof("No bucket named %s found", bucket)
		return nil
	}
	logging.Warnf("Got error while trying to get bucket sequence numbers: %v Retrying...", err)
	return err
}

func CollectSeqnos(kvfeeds map[string]*kvConn, bucketLevel bool, cid uint32) (l_seqnos []uint64, err error) {
	var wg sync.WaitGroup

	// Buffer for storing kv_seqs from each node
	kv_seqnos_node := make([][]uint64, len(kvfeeds))
	errors := make([]error, len(kvfeeds))

	i := 0
	for _, feed := range kvfeeds {
		wg.Add(1)
		go func(index int, feed *kvConn) {
			defer wg.Done()
			kv_seqnos_node[index] = feed.seqsbuf
			if bucketLevel {
				errors[index] = couchbase.GetSeqs(feed.mc, kv_seqnos_node[index], feed.tmpbuf)
			} else {
				err = tryEnableCollection(feed.mc, 7)
				if err != nil {
					errors[index] = err
					return
				}
				errors[index] = couchbase.GetCollectionSeqs(feed.mc, kv_seqnos_node[index], feed.tmpbuf, cid)
			}
		}(i, feed)
		i++
	}

	wg.Wait()

	seqnos := kv_seqnos_node[0]
	for i, kv_seqnos := range kv_seqnos_node {
		err := errors[i]
		if err != nil {
			logging.Errorf("feed.DcpGetSeqnos(): %v\n", err)
			return nil, err
		}

		for vbno, seqno := range kv_seqnos {
			prev := seqnos[vbno]
			if prev < seqno {
				seqnos[vbno] = seqno
			}
		}
	}
	// The following code is to detect rebalance or recovery !!
	// this is not yet supported in KV, GET_SEQNOS returns all
	// seqnos.
	if len(seqnos) < dcp_buckets_seqnos.numVbs {
		fmsg := "unable to get seqnos ts for all vbuckets (%v out of %v)"
		err = fmt.Errorf(fmsg, len(seqnos), dcp_buckets_seqnos.numVbs)
		logging.Errorf("%v\n", err)
		return nil, err
	}
	// sort them
	vbnos := make([]int, 0, dcp_buckets_seqnos.numVbs)
	for vbno := range seqnos {
		vbnos = append(vbnos, int(vbno))
	}
	sort.Ints(vbnos)
	// gather seqnos.
	l_seqnos = make([]uint64, 0, dcp_buckets_seqnos.numVbs)
	for _, vbno := range vbnos {
		l_seqnos = append(l_seqnos, seqnos[uint16(vbno)])
	}
	return l_seqnos, nil
}

func pollForDeletedBuckets() {
	for {
		time.Sleep(10 * time.Second)
		todels := []string{}
		func() {
			dcp_buckets_seqnos.rw.Lock()
			defer dcp_buckets_seqnos.rw.Unlock()
			for bucketn, bucket := range dcp_buckets_seqnos.buckets {
				if bucket.Refresh() != nil {
					// lazy detect bucket deletes
					todels = append(todels, bucketn)
				}
			}
		}()
		func() {
			var bucketn string
			var bucket *couchbase.Bucket

			dcp_buckets_seqnos.rw.RLock()
			defer func() {
				if r := recover(); r != nil {
					logging.Warnf("failover race in bucket: %v", r)
					todels = append(todels, bucketn)
				}
				dcp_buckets_seqnos.rw.RUnlock()
			}()
			for bucketn, bucket = range dcp_buckets_seqnos.buckets {
				if m, err := bucket.GetVBmap(nil); err != nil {
					// idle detect failures.
					todels = append(todels, bucketn)
				} else if len(m) != len(dcp_buckets_seqnos.readerMap[bucketn].kvfeeds) {
					// lazy detect kv-rebalance
					todels = append(todels, bucketn)
				}
			}
		}()
		for _, bucketn := range todels {
			delDBSbucket(bucketn, false)
		}
	}
}

func SetDcpMemcachedTimeout(val uint32) {
	memcached.SetDcpMemcachedTimeout(val)
}

func getConnName() (string, error) {
	uuid, _ := NewUUID()
	name := uuid.Str()
	if name == "" {
		err := fmt.Errorf("getConnName: invalid uuid.")

		// probably not a good idea to fail if uuid
		// based name fails. Can return const string
		return "", err
	}
	connName := "getseqnos-" + name
	return connName, nil
}

func tryEnableCollection(conn *memcached.Client, clusterVer uint32) error {
	if clusterVer >= collections.COLLECTION_SUPPORTED_VERSION {
		if !conn.IsCollectionsEnabled() {
			connName, err := getConnName()
			if err != nil {
				return err
			}
			err = conn.EnableCollections(connName)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
