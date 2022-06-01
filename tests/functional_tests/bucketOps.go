package eventing

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/gocb/v2"
)

type user struct {
	ID        int      `json:"uid"`
	Email     string   `json:"email"`
	Interests []string `json:"interests"`
}

type opsType struct {
	count       int
	expiry      int
	delete      bool
	writeXattrs bool
	xattrPrefix string
	startIndex  int
	isBinary    bool
}

func pumpBucketOps(ops opsType, rate *rateLimit) {
	pumpBucketOpsSrc(ops, "default", rate)
}

func random(min int, max int) int {
	return rand.Intn(max-min) + min
}

// Purpose of this routine is to mimick rollback of checkpoint blobs to a previous
// snapshot, that may be invalid as per the planner.
func mangleCheckpointBlobs(appName, prefix string, start, end int) {
	time.Sleep(15 * time.Second)

	cluster, err := gocb.Connect("couchbase://127.0.0.1:12000", gocb.ClusterOptions{
		Username: rbacuser,
		Password: rbacpass,
	})
	if err != nil {
		fmt.Println("Error connecting to cluster, err: ", err)
		return
	}

	bucket := cluster.Bucket(metaBucket)
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		fmt.Printf("Error connecting to bucket %s  err: %s \n", metaBucket, err)
		return
	}
	collection := bucket.DefaultCollection()

	// Grab functionID from metakv
	metakvPath := fmt.Sprintf("/eventing/tempApps/%s/0", appName)
	data, _, err := metakv.Get(metakvPath)
	if err != nil {
		log.Printf("Metakv lookup failed, err: %v\n", err)
		return
	}

	var app map[string]interface{}
	err = json.Unmarshal(data, &app)
	if err != nil {
		log.Printf("Failed to unmarshal app content from metakv, err: %v\n", err)
		return
	}

	possibleVbOwners := []string{"127.0.0.1:9302", "127.0.0.1:9301", "127.0.0.1:9300", "127.0.0.1:9305"}
	// possibleDcpStreamStates := []string{"running", "stopped", ""}
	possibleDcpStreamStates := []string{"running"}

	// Commenting it for now, as in real world a node uuid is tied specifically to a host:port combination.
	// One host:port combination can't have more than one node uuid.
	// possibleNodeUUIDs := []string{"abcd", "defg", "ghij"}
	possibleWorkers := make([]string, 0)
	for i := 0; i < 100; i++ {
		possibleWorkers = append(possibleWorkers, fmt.Sprintf("worker_%s_%d", appName, i))
	}

	rand.Seed(time.Now().UnixNano())

	for vb := start; vb <= end; vb++ {
		docID := fmt.Sprintf("%s::%d::%s::vb::%d", prefix, uint64(app["function_id"].(float64)), appName, vb)

		worker := possibleWorkers[random(0, len(possibleWorkers))]
		ownerNode := possibleVbOwners[random(0, len(possibleVbOwners))]

		blob := make([]gocb.MutateInSpec, 0)
		upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}
		blob = append(blob, gocb.UpsertSpec("assigned_worker", worker, upsertOptions))
		blob = append(blob, gocb.UpsertSpec("current_vb_owner", ownerNode, upsertOptions))
		blob = append(blob, gocb.UpsertSpec("dcp_stream_status", possibleDcpStreamStates[random(0, len(possibleDcpStreamStates))], upsertOptions))
		_, err = collection.MutateIn(docID, blob, nil)

		if err != nil {
			log.Printf("DocID: %s err: %v\n", docID, err)
		}
	}

	log.Printf("Mangled checkpoint blobs from start vb: %d to end vb: %d\n", start, end)
}

func purgeCheckpointBlobs(appName, prefix string, start, end int) {
	time.Sleep(15 * time.Second) // Hopefully enough time for bootstrap loop to exit on new node

	cluster, err := gocb.Connect("couchbase://127.0.0.1:12000", gocb.ClusterOptions{
		Username: rbacuser,
		Password: rbacpass,
	})
	if err != nil {
		fmt.Println("Error connecting to cluster, err: ", err)
		return
	}
	bucket := cluster.Bucket(metaBucket)
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		fmt.Printf("Error connecting to bucket %s  err: %s \n", metaBucket, err)
		return
	}
	collection := bucket.DefaultCollection()

	// Grab functionID from metakv
	metakvPath := fmt.Sprintf("/eventing/tempApps/%s/0", appName)
	data, _, err := metakv.Get(metakvPath)
	if err != nil {
		log.Printf("Metakv lookup failed, err: %v\n", err)
		return
	}

	var app map[string]interface{}
	err = json.Unmarshal(data, &app)
	if err != nil {
		log.Printf("Failed to unmarshal app content from metakv, err: %v\n", err)
		return
	}

	for vb := start; vb <= end; vb++ {
		docID := fmt.Sprintf("%s::%d::%s::vb::%d", prefix, uint64(app["function_id"].(float64)), appName, vb)
		_, err = collection.Remove(docID, nil)
		if err != nil {
			log.Printf("DocID: %s err: %v\n", docID, err)
		}
	}

	log.Printf("Purged checkpoint blobs from start vb: %d to end vb: %d\n", start, end)
}

func pumpBucketOpsSrc(ops opsType, bucket string, rate *rateLimit) {
	k := common.Keyspace{
		BucketName:     bucket,
		ScopeName:      "_default",
		CollectionName: "_default",
	}
	pumpBucketOpsKeyspace(ops, k, rate)
}

func pumpBucketOpsKeyspace(ops opsType, srcKeyspace common.Keyspace, rate *rateLimit) {
	log.Println("Starting bucket ops to source bucket")
	srcBucket := srcKeyspace.BucketName
	srcScope := srcKeyspace.ScopeName
	srcCollection := srcKeyspace.CollectionName

	if ops.count == 0 {
		ops.count = itemCount
	}

	cluster, err := gocb.Connect("couchbase://127.0.0.1:12000", gocb.ClusterOptions{
		Username: rbacuser,
		Password: rbacpass,
	})
	if err != nil {
		panic(fmt.Sprintf("Bucket open, err: %s", err))
	}
	bucket := cluster.Bucket(srcBucket)
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		fmt.Printf("Error connecting to bucket %s  err: %s \n", srcBucket, err)
		return
	}
	scope := bucket.Scope(srcScope)
	collection := scope.Collection(srcCollection)

	bin := []byte{1, 2, 3, 4, 0, 5, 6, 0, 7}
	u := user{
		Email:     "kingarthur@couchbase.com",
		Interests: []string{"Holy Grail", "African Swallows"},
	}

	// TODO: if ops.delete is set then only delete the item else only insert the item.
	if !rate.limit {
		for i := 0; i < ops.count; i++ {
			u.ID = i + ops.startIndex
			if !ops.writeXattrs {
				upsertOptions := &gocb.UpsertOptions{Expiry: time.Duration(ops.expiry)}
				upsertOptionsBinary := &gocb.UpsertOptions{Expiry: time.Duration(ops.expiry), Transcoder: &gocb.RawBinaryTranscoder{}}

			retryOp1:
				if ops.isBinary {
					_, err = collection.Upsert(fmt.Sprintf("doc_id_%d", i+ops.startIndex), bin, upsertOptionsBinary)
				} else {
					_, err = collection.Upsert(fmt.Sprintf("doc_id_%d", i+ops.startIndex), u, upsertOptions)
				}
				if err != nil {
					time.Sleep(time.Second)
					goto retryOp1
				}
			} else {

			retryOp2:

				mutateIn := make([]gocb.MutateInSpec, 0)
				upsertOptions := &gocb.MutateInOptions{Expiry: time.Duration(ops.expiry)}
				upsertSpecOptionsBothSet := &gocb.UpsertSpecOptions{CreatePath: true, IsXattr: true}
				upsertSpecOptionsNoneSet := &gocb.UpsertSpecOptions{CreatePath: false, IsXattr: false}
				mutateIn = append(mutateIn, gocb.UpsertSpec(fmt.Sprintf("test_%s", ops.xattrPrefix), "user xattr test value", upsertSpecOptionsBothSet))
				mutateIn = append(mutateIn, gocb.UpsertSpec("normalproperty", "normal property value", upsertSpecOptionsNoneSet))
				_, err = collection.MutateIn(fmt.Sprintf("doc_id_%d", i+ops.startIndex), mutateIn, upsertOptions)

				if err != nil {
					time.Sleep(time.Second)
					goto retryOp2
				}
			}
		}

		if ops.delete {
			for i := 0; i < ops.count; i++ {

			retryOp3:
				_, err := collection.Remove(fmt.Sprintf("doc_id_%d", i), nil)
				if err != nil && !errors.Is(err, gocb.ErrDocumentNotFound) {
					time.Sleep(time.Second)
					goto retryOp3
				}
			}
		}

	} else {
		ticker := time.NewTicker(time.Second / time.Duration(rate.opsPSec))
		i := 0
		for {
			select {
			case <-ticker.C:
				u.ID = i + ops.startIndex
				if ops.delete {

				retryOp4:
					_, err := collection.Remove(fmt.Sprintf("doc_id_%d", u.ID), nil)
					if err != nil && !errors.Is(err, gocb.ErrDocumentNotFound) {
						time.Sleep(time.Second)
						goto retryOp4
					}
				} else {
					if !ops.writeXattrs {

					retryOp5:
						upsertOptions := &gocb.UpsertOptions{Expiry: time.Duration(ops.expiry)}
						upsertOptionsBinary := &gocb.UpsertOptions{Expiry: time.Duration(ops.expiry), Transcoder: &gocb.RawBinaryTranscoder{}}

						if ops.isBinary {
							_, err = collection.Upsert(fmt.Sprintf("doc_id_%d", i+ops.startIndex), bin, upsertOptionsBinary)
						} else {
							_, err = collection.Upsert(fmt.Sprintf("doc_id_%d", i+ops.startIndex), u, upsertOptions)
						}
						if err != nil {
							time.Sleep(time.Second)
							goto retryOp5
						}
					} else {

					retryOp6:
						mutateIn := make([]gocb.MutateInSpec, 0)
						upsertOptions := &gocb.MutateInOptions{Expiry: time.Duration(ops.expiry)}
						upsertSpecOptionsBothSet := &gocb.UpsertSpecOptions{CreatePath: true, IsXattr: true}
						upsertSpecOptionsNoneSet := &gocb.UpsertSpecOptions{CreatePath: false, IsXattr: false}
						mutateIn = append(mutateIn, gocb.UpsertSpec(fmt.Sprintf("test_%s", ops.xattrPrefix), "user xattr test value", upsertSpecOptionsBothSet))
						mutateIn = append(mutateIn, gocb.UpsertSpec("normalproperty", "normal property value", upsertSpecOptionsNoneSet))
						_, err = collection.MutateIn(fmt.Sprintf("doc_id_%d", i+ops.startIndex), mutateIn, upsertOptions)

						if err != nil {
							time.Sleep(time.Second)
							goto retryOp6
						}
					}
				}
				i++

				if i == rate.count {
					if !rate.loop {
						ticker.Stop()
						return
					}
					i = 0
					continue
				}

			case <-rate.stopCh:
				ticker.Stop()
				return
			}
		}
	}
}

func CreateCollection(bucketName, scopeName, collectionName string) error {
	reqScopeApi := fmt.Sprintf(scopeApi, bucketName)
	payload := fmt.Sprintf("name=%s", scopeName)
	makeRequest("POST", strings.NewReader(payload), reqScopeApi)
	reqCollectionApi := fmt.Sprintf(collectionApi, bucketName, scopeName)
	payload = fmt.Sprintf("name=%s", collectionName)
	makeRequest("POST", strings.NewReader(payload), reqCollectionApi)
	return nil
}

func DropCollection(bucketName, scopeName, collectionName string) error {
	cluster, err := gocb.Connect("couchbase://127.0.0.1:12000", gocb.ClusterOptions{
		Username: rbacuser,
		Password: rbacpass,
	})
	if err != nil {
		panic(fmt.Sprintf("Bucket open, err: %s", err))
	}
	bucket := cluster.Bucket(bucketName)
	mgr := bucket.Collections()

	err = mgr.DropCollection(gocb.CollectionSpec{
		Name:      collectionName,
		ScopeName: scopeName,
	}, nil)
	return err
}
