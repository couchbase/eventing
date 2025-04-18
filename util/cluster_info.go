package util

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/collections"
	couchbase "github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
)

var (
	ErrInvalidNodeId       = errors.New("Invalid NodeId")
	ErrInvalidService      = errors.New("Invalid service")
	ErrNodeNotBucketMember = errors.New("Node is not a member of bucket")
	ErrValidationFailed    = errors.New("ClusterInfo Validation Failed")
)

var ServiceAddrMap map[string]string

// Helper object to refresh the cluster info cache
// based on service change notifier's notification.
var cicSingleton *ClusterInfoClient
var once sync.Once
var onretry = &sync.Mutex{}

const (
	EVENTING_ADMIN_SERVICE = "eventingAdminPort"
	EVENTING_SSL_SERVICE   = "eventingSSL"
)

const CLUSTER_VERSION_7 uint32 = 7
const CLUSTER_INFO_INIT_RETRIES = 5
const CLUSTER_INFO_VALIDATION_RETRIES = 10
const BUCKET_UUID_NIL = ""

// Helper object for fetching cluster information
// Can be used by services running on a cluster node to connect with
// local management service for obtaining cluster information.
// Info cache can be updated by using Refresh() method.
type ClusterInfoCache struct {
	sync.RWMutex
	url       string
	poolName  string
	logPrefix string
	retries   int

	useStaticPorts bool
	servicePortMap map[string]string

	client       couchbase.Client
	pool         couchbase.Pool
	nodes        []couchbase.Node
	nodesvs      []couchbase.NodeServices
	failedNodes  []couchbase.Node
	addNodes     []couchbase.Node
	version      uint32
	minorVersion uint32
}

type ClusterInfoClient struct {
	cinfo                   *ClusterInfoCache
	clusterURL              string
	pool                    string
	servicesNotifierRetryTm uint
	finch                   chan bool

	scn *ServicesChangeNotifier
}

type NodeId int

func FetchNewClusterInfoCache(clusterUrl string) (*ClusterInfoCache, error) {
	cUrl := url.URL{
		Scheme: "http",
		Host:   clusterUrl,
	}

	c := &ClusterInfoCache{
		url:      cUrl.String(),
		poolName: "default",
		retries:  CLUSTER_INFO_INIT_RETRIES,
	}

	if ServiceAddrMap != nil {
		c.SetServicePorts(ServiceAddrMap)
	}

	if err := c.Fetch(nil); err != nil {
		return nil, err
	}

	return c, nil
}

func SetServicePorts(portMap map[string]string) {
	ServiceAddrMap = portMap
}

func (c *ClusterInfoCache) SetLogPrefix(p string) {
	c.logPrefix = p
}

func (c *ClusterInfoCache) SetMaxRetries(r int) {
	c.retries = r
}

func (c *ClusterInfoCache) SetServicePorts(portMap map[string]string) {

	c.useStaticPorts = true
	c.servicePortMap = portMap

}

func (c *ClusterInfoCache) Fetch(np *couchbase.Pool) error {

	fn := func(r int, err error) error {
		if r > 0 {
			logging.Infof("%vError occured during cluster info update (%v) .. Retrying(%d)",
				c.logPrefix, err, r)
		}

		vretry := 0
	retry:
		if np != nil && vretry == 0 {
			err = c.updatePool(np)
			if err != nil {
				return err
			}
		} else {
			c.client, err = couchbase.Connect(c.url)
			if err != nil {
				return err
			}
			c.pool, err = c.client.GetPool(c.poolName)
			if err != nil {
				return err
			}
		}
		c.updateNodesData()

		found := false
		for _, node := range c.nodes {
			if node.ThisNode {
				found = true
			}
		}

		if !found {
			return errors.New("Current node's cluster membership is not active")
		}

		var poolServs couchbase.PoolServices
		poolServs, err = c.client.GetPoolServices(c.poolName)
		if err != nil {
			return err
		}
		c.nodesvs = poolServs.NodesExt

		if !c.validateCache() {
			if vretry < CLUSTER_INFO_VALIDATION_RETRIES {
				vretry++
				logging.Infof("%vValidation Failed for cluster info.. Retrying(%d)",
					c.logPrefix, vretry)
				goto retry
			} else {
				logging.Infof("%vValidation Failed for cluster info.. %v",
					c.logPrefix, c)
				return ErrValidationFailed
			}
		}

		return nil
	}

	rh := NewRetryHelper(c.retries, time.Second, 1, fn)
	return rh.Run()
}

func (c *ClusterInfoCache) GetClusterVersion() (int, int) {
	return int(c.version), int(c.minorVersion)
}

func (c *ClusterInfoCache) GetNodesByServiceType(srvc string) (nids []NodeId) {
	for i, svs := range c.nodesvs {
		if _, ok := svs.Services[srvc]; ok {
			nids = append(nids, NodeId(i))
		}
	}

	return
}

func (c *ClusterInfoCache) GetAddressOfActiveKVNodes() (addresses []string, err error) {
	var serviceType string
	if couchbase.GetUseTLS() {
		serviceType = DataServiceSSL
	} else {
		serviceType = DataService
	}
	for _, nodeID := range c.GetNodesByServiceType(serviceType) {
		address, err := c.GetServiceAddress(nodeID, serviceType)
		if err != nil {
			return addresses, err
		}
		addresses = append(addresses, address)
	}
	return
}

func (c *ClusterInfoCache) GetAllNodes() []*Node {
	var nodes []*Node
	for _, n := range c.nodes {
		node, err := NewNode(n.Hostname)
		if err != nil {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes
}

func (c *ClusterInfoCache) GetActiveEventingNodes() (nodes []couchbase.Node) {
	for _, n := range c.nodes {
		for _, s := range n.Services {
			if s == "eventing" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCache) GetFailedEventingNodes() (nodes []couchbase.Node) {
	for _, n := range c.failedNodes {
		for _, s := range n.Services {
			if s == "eventing" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCache) GetNewEventingNodes() (nodes []couchbase.Node) {
	for _, n := range c.addNodes {
		for _, s := range n.Services {
			if s == "eventing" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCache) GetNodesByBucket(bucket string) (nids []NodeId, err error) {
	b, berr := c.pool.GetBucket(bucket)
	if berr != nil {
		err = berr
		return
	}
	defer b.Close()

	for i, _ := range c.nodes {
		nid := NodeId(i)
		if _, ok := c.findVBServerIndex(b, nid); ok {
			nids = append(nids, nid)
		}
	}

	return
}

func (c *ClusterInfoCache) GetBuckets() []string {
	buckets := c.pool.GetBuckets()
	return buckets
}

// Return UUID of a given bucket.
func (c *ClusterInfoCache) GetBucketUUID(bucket string) (uuid string) {

	b, err := c.pool.GetBucket(bucket)
	if err != nil {
		return BUCKET_UUID_NIL
	}
	defer b.Close()

	// This node recognize this bucket.   Make sure its vb is resided in at least one node.
	for i, _ := range c.nodes {
		nid := NodeId(i)
		if _, ok := c.findVBServerIndex(b, nid); ok {
			// find the bucket resides in at least one node
			return b.UUID
		}
	}

	// no nodes recognize this bucket
	return BUCKET_UUID_NIL
}

func (c *ClusterInfoCache) GetUniqueBSCIds(bucket, scope, collection string) (string, uint32, uint32, error) {
	bucketUUID := c.GetBucketUUID(bucket)
	if bucketUUID == BUCKET_UUID_NIL {
		return BUCKET_UUID_NIL, 0, 0, couchbase.ErrBucketNotFound
	}
	sid, cid, err := c.pool.GetUniqueBSCIds(bucket, scope, collection)
	return bucketUUID, sid, cid, err
}

func (c *ClusterInfoCache) IsEphemeral(bucket string) (bool, error) {
	b, err := c.pool.GetBucket(bucket)
	if err != nil {
		return false, err
	}
	defer b.Close()
	return strings.EqualFold(b.Type, "ephemeral"), nil
}

func (c *ClusterInfoCache) IsMemcached(bucket string) (bool, error) {
	b, err := c.pool.GetBucket(bucket)
	if err != nil {
		return false, err
	}
	defer b.Close()
	return strings.EqualFold(b.Type, "memcached"), nil
}

func (c *ClusterInfoCache) StorageEngine(bucketName string) (common.StorageEngine, error) {
	storage, err := c.pool.GetBucketStorage(bucketName)
	return common.StorageEngine(storage), err
}

func (c *ClusterInfoCache) GetCurrentNode() NodeId {
	for i, node := range c.nodes {
		if node.ThisNode {
			return NodeId(i)
		}
	}
	// TODO: can we avoid this panic ?
	panic("Current node is not in active membership")
}

func (c *ClusterInfoCache) IsNodeHealthy(nid NodeId) (bool, error) {
	if int(nid) >= len(c.nodes) {
		return false, ErrInvalidNodeId
	}

	return c.nodes[nid].Status == "healthy", nil
}

func (c *ClusterInfoCache) GetNodeStatus(nid NodeId) (string, error) {
	if int(nid) >= len(c.nodes) {
		return "", ErrInvalidNodeId
	}

	return c.nodes[nid].Status, nil
}

func (c *ClusterInfoCache) GetServiceAddress(nid NodeId, srvc string) (addr string, err error) {
	var port int
	var ok bool

	if int(nid) >= len(c.nodesvs) {
		err = ErrInvalidNodeId
		return
	}

	node := c.nodesvs[nid]
	if port, ok = node.Services[srvc]; !ok {
		logging.Errorf("%vInvalid Service %v for node %rs. Nodes %rs \n NodeServices %v",
			c.logPrefix, srvc, node, c.nodes, c.nodesvs)
		err = ErrInvalidService
		return
	}

	// For current node, hostname might be empty
	// Insert hostname used to connect to the cluster
	cUrl, err := url.Parse(c.url)
	if err != nil {
		return "", errors.New("Unable to parse cluster url - " + err.Error())
	}
	h, _, _ := net.SplitHostPort(cUrl.Host)
	if node.Hostname == "" {
		node.Hostname = h
	}

	addr = net.JoinHostPort(node.Hostname, fmt.Sprint(port))
	return
}

func (c *ClusterInfoCache) GetVBuckets(nid NodeId, bucket string) (vbs []uint32, err error) {
	b, berr := c.pool.GetBucket(bucket)
	if berr != nil {
		err = berr
		return
	}
	defer b.Close()

	idx, ok := c.findVBServerIndex(b, nid)
	if !ok {
		err = ErrNodeNotBucketMember
		return
	}

	vbmap := b.VBServerMap()

	for vb, idxs := range vbmap.VBucketMap {
		if idxs[0] == idx {
			vbs = append(vbs, uint32(vb))
		}
	}

	return
}

func (c *ClusterInfoCache) findVBServerIndex(b *couchbase.Bucket, nid NodeId) (int, bool) {
	bnodes := b.Nodes()

	for idx, n := range bnodes {
		if c.sameNode(n, c.nodes[nid]) {
			return idx, true
		}
	}

	return 0, false
}

func (c *ClusterInfoCache) sameNode(n1 couchbase.Node, n2 couchbase.Node) bool {
	return n1.Hostname == n2.Hostname
}

func (c *ClusterInfoCache) GetLocalServiceAddress(srvc string) (string, error) {

	if c.useStaticPorts {

		h, err := c.GetLocalHostname()
		if err != nil {
			return "", err
		}

		p, e := c.getStaticServicePort(srvc)
		if e != nil {
			return "", e
		}
		return net.JoinHostPort(h, p), nil

	} else {
		node := c.GetCurrentNode()
		return c.GetServiceAddress(node, srvc)
	}
}

func (c *ClusterInfoCache) GetLocalServicePort(srvc string) (string, error) {
	addr, err := c.GetLocalServiceAddress(srvc)
	if err != nil {
		return addr, err
	}

	_, p, e := net.SplitHostPort(addr)
	if e != nil {
		return p, e
	}

	return net.JoinHostPort("", p), nil
}

func (c *ClusterInfoCache) GetLocalServiceHost(srvc string) (string, error) {

	addr, err := c.GetLocalServiceAddress(srvc)
	if err != nil {
		return addr, err
	}

	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	return h, nil
}

func (c *ClusterInfoCache) GetExternalIPOfThisNode(hostnames []string) (string, error) {
	if len(hostnames) != len(c.nodes) {
		return "", errors.New("Cluster info cache is inconsistent")
	}
	for i, node := range c.nodes {
		if !node.ThisNode {
			continue
		}
		hostnameWithAltAddr := strings.Split(hostnames[i], "<TOK>")
		if len(hostnameWithAltAddr) > 1 {
			return hostnameWithAltAddr[1], nil
		}
		hostIp, _, err := net.SplitHostPort(hostnames[i])
		if err != nil {
			return "", err
		}
		return hostIp, nil
	}
	return "", errors.New("Nodes are empty in cluster info cache")
}

func (c *ClusterInfoCache) GetLocalHostAddress() (string, error) {

	cUrl, err := url.Parse(c.url)
	if err != nil {
		return "", errors.New("Unable to parse cluster url - " + err.Error())
	}

	_, p, _ := net.SplitHostPort(cUrl.Host)

	h, err := c.GetLocalHostname()
	if err != nil {
		return "", err
	}

	return net.JoinHostPort(h, p), nil

}

func (c *ClusterInfoCache) GetLocalHostname() (string, error) {

	cUrl, err := url.Parse(c.url)
	if err != nil {
		return "", errors.New("Unable to parse cluster url - " + err.Error())
	}

	h, _, _ := net.SplitHostPort(cUrl.Host)

	nid := c.GetCurrentNode()

	if int(nid) >= len(c.nodesvs) {
		return "", ErrInvalidNodeId
	}

	node := c.nodesvs[nid]
	if node.Hostname == "" {
		node.Hostname = h
	}

	return node.Hostname, nil

}

func (c *ClusterInfoCache) GetNumVbucketsForBucket(bucketName string) int {
	return c.pool.GetNumVbuckets(bucketName)
}

func (c *ClusterInfoCache) validateCache() bool {

	if len(c.nodes) != len(c.nodesvs) {
		return false
	}

	//validation not required for single node setup(MB-16494)
	if len(c.nodes) == 1 && len(c.nodesvs) == 1 {
		return true
	}

	var hostList1 []string

	for _, n := range c.nodes {
		hostList1 = append(hostList1, n.Hostname)
	}

	for i, svc := range c.nodesvs {
		h := svc.Hostname
		p := svc.Services["mgmt"]

		if h == "" {
			h = Localhost()
		}

		hp := net.JoinHostPort(h, fmt.Sprint(p))

		if hostList1[i] != hp {
			return false
		}
	}

	return true
}

func (c *ClusterInfoCache) getStaticServicePort(srvc string) (string, error) {

	if p, ok := c.servicePortMap[srvc]; ok {
		return p, nil
	} else {
		return "", ErrInvalidService
	}

}

// updatePool will fetch bucket info if the verion hash in bucketURL changes
// else it will copy it from existing pool avoiding REST Calls to ns-server.
func (c *ClusterInfoCache) updatePool(np *couchbase.Pool) error {
	err := np.Connect(c.url)
	if err != nil {
		return err
	}

	ovh, err := c.pool.GetBucketURLVersionHash()
	if err != nil {
		return err
	}

	nvh, err := np.GetBucketURLVersionHash()
	if err != nil {
		return err
	}

	if ovh != nvh {
		err = np.Refresh()
		if err != nil {
			return err
		}
	} else {
		np.BucketMap = c.pool.BucketMap
		np.Manifest = c.pool.Manifest
	}

	c.pool = *np
	return nil
}

func (c *ClusterInfoCache) FetchWithLock(np *couchbase.Pool) error {
	c.Lock()
	defer c.Unlock()
	return c.Fetch(np)
}

func (c *ClusterInfoCache) updateNodesData() {
	var nodes []couchbase.Node
	var failedNodes []couchbase.Node
	var addNodes []couchbase.Node
	version := uint32(math.MaxUint32)
	minorVersion := uint32(math.MaxUint32)

	for _, n := range c.pool.Nodes {
		if n.ClusterMembership == "active" {
			nodes = append(nodes, n)
		} else if n.ClusterMembership == "inactiveFailed" {
			// node being failed over
			failedNodes = append(failedNodes, n)
		} else if n.ClusterMembership == "inactiveAdded" {
			// node being added (but not yet rebalanced in)
			addNodes = append(addNodes, n)
		} else {
			logging.Warnf("ClusterInfoCache: unrecognized node membership %v", n.ClusterMembership)
		}

		// Find the minimum cluster compatibility
		v := uint32(n.ClusterCompatibility / 65536)
		minorv := uint32(n.ClusterCompatibility) - (v * 65536)
		if v < version || (v == version && minorv < minorVersion) {
			version = v
			minorVersion = minorv
		}
	}

	c.nodes = nodes
	c.failedNodes = failedNodes
	c.addNodes = addNodes
	c.version = version
	c.minorVersion = minorVersion
	if c.version == math.MaxUint32 {
		c.version = 0
	}
}

func FetchClusterInfoClient(clusterURL string) (c *ClusterInfoClient, err error) {
	once.Do(func() {
		cicSingleton = &ClusterInfoClient{
			clusterURL: clusterURL,
			pool:       "default",
			finch:      make(chan bool),
		}

		config := getConfig()
		cicSingleton.servicesNotifierRetryTm = 5
		if tm, ok := config["service_notifier_timeout"]; ok {
			retryTm, tOk := tm.(float64)
			if tOk {
				cicSingleton.servicesNotifierRetryTm = uint(retryTm)
			}
		}

		cinfo, err := FetchNewClusterInfoCache(clusterURL)
		if err != nil {
			cicSingleton.cinfo = nil
			logging.Errorf("FetchClusterInfoClient(%s) inital FetchNewClusterInfoCache try once.Do failed %v\n", clusterURL, err)
			return
		}
		cicSingleton.cinfo = cinfo
		if cicSingleton.cinfo != nil {
			go cicSingleton.watchClusterChanges()
		}
	})
	if cicSingleton.cinfo == nil {
		// Recovery process in case cluster info client is nil
		onretry.Lock()
		defer onretry.Unlock()
		cinfo, err := FetchNewClusterInfoCache(clusterURL)
		if err != nil {
			logging.Errorf("FetchClusterInfoClient(%s) retry FetchNewClusterInfoCache failed %v\n", clusterURL, err)
			return nil, err
		}
		cicSingleton.cinfo = cinfo
		if cicSingleton.cinfo != nil {
			go cicSingleton.watchClusterChanges()
		}
	}
	return cicSingleton, nil
}

// Consumer must lock returned cinfo before using it
func (c *ClusterInfoClient) GetClusterInfoCache() *ClusterInfoCache {
	return c.cinfo
}

func (c *ClusterInfoClient) watchClusterChanges() {
	logPrefix := "ClusterInfoClient::watchClusterChanges"

	selfRestart := func() {
		if c.scn != nil {
			c.scn.Close()
			c.scn = nil
		}
		time.Sleep(time.Duration(c.servicesNotifierRetryTm) * time.Millisecond)
		go c.watchClusterChanges()
	}

	if err := c.cinfo.FetchWithLock(nil); err != nil {
		logging.Errorf("cic.cinfo.FetchWithLock(): %v\n", err)
		selfRestart()
		return
	}

	var err error
	c.scn, err = NewServicesChangeNotifier(c.clusterURL, c.pool)
	if err != nil {
		logging.Errorf("ClusterInfoClient NewServicesChangeNotifier(): %v\n", err)
		selfRestart()
		return
	}

	c.checkAndObserveManifestChanges()

	ticker := time.NewTicker(time.Duration(c.servicesNotifierRetryTm) * time.Minute)
	defer ticker.Stop()

	// For observing node services config
	ch := c.scn.GetNotifyCh()
	for {
		select {
		case notif, ok := <-ch:
			if !ok {
				logging.Errorf("%s ServicesChangeNotifier channel closed. Restarting..", logPrefix)
				selfRestart()
				return
			}

			switch notif.Type {
			case CollectionManifestChangeNotification:
				bucket := (notif.Msg).(*couchbase.Bucket)
				if err := c.cinfo.FetchManifestInfo(bucket.Name); err != nil {
					logging.Errorf("cic.cinfo.FetchManifestInfo(): %v\n", err)
					selfRestart()
					return
				}
			case PoolChangeNotification:
				pool := (notif.Msg).(*couchbase.Pool)
				if err := c.cinfo.FetchWithLock(pool); err != nil {
					logging.Errorf("cic.cinfo.FetchWithLock(): %v\n", err, pool)
					selfRestart()
					return
				}
			default:
				if err := c.cinfo.FetchWithLock(nil); err != nil {
					logging.Errorf("cic.cinfo.FetchWithLock(nil): %v\n", err)
					selfRestart()
					return
				}
			}

			c.checkAndObserveManifestChanges()

		case <-ticker.C:
			if err := c.cinfo.FetchWithLock(nil); err != nil {
				logging.Errorf("cic.cinfo.FetchWithLock(nil): %v\n", err)
				selfRestart()
				return
			}
		case <-c.finch:
			return
		}
	}
}

func (c *ClusterInfoClient) Close() {
	defer func() { recover() }()

	close(c.finch)
}

func (c *ClusterInfoClient) checkAndObserveManifestChanges() {
	cinfo := c.cinfo
	cinfo.RLock()
	defer cinfo.RUnlock()

	buckets := cinfo.pool.GetBucketList()
	for bucketName := range buckets {
		c.scn.RunObserveCollectionManifestChanges(bucketName)
	}
	c.scn.GarbageCollect(buckets)
}

func (c *ClusterInfoCache) GetCollectionID(bucket, scope, collection string) (uint32, error) {
	return c.pool.GetCollectionID(bucket, scope, collection)
}

func (c *ClusterInfoCache) GetScopes(bucketName string) map[string][]string {
	return c.pool.GetScopes(bucketName)
}

func (c *ClusterInfoCache) GetManifestID(bucket string) (string, error) {
	return c.pool.GetManifestID(bucket)
}

func (c *ClusterInfoCache) GetNodeCompatVersion() (uint32, uint32) {
	return c.version, c.minorVersion
}

func (c *ClusterInfoCache) GetCollectionManifest(bucketName string) *collections.CollectionManifest {
	return c.pool.GetCollectionManifest(bucketName)
}

func (c *ClusterInfoCache) FetchManifestInfo(bucketName string) error {
	c.Lock()
	defer c.Unlock()

	compactVersion, _ := c.GetNodeCompatVersion()
	if compactVersion >= collections.COLLECTION_SUPPORTED_VERSION {
		pool := &c.pool
		manifest, err := pool.RefreshBucketManifest(bucketName)
		if err != nil {
			return err
		}
		pool.Manifest[bucketName] = manifest
	}
	return nil
}

func getConfig() (c common.Config) {

	data, err := MetakvGet(common.EventingConfigPath)
	if err != nil {
		logging.Errorf("Failed to get config, err: %v", err)
		return
	}

	if !bytes.Equal(data, nil) {
		err = json.Unmarshal(data, &c)
		if err != nil {
			logging.Errorf("Failed to unmarshal payload from metakv, err: %v", err)
			return
		}
	}
	return
}
