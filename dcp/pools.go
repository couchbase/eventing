package couchbase

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/common/collections"
	memcached "github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
)

var (
	ErrBucketNotFound = errors.New("Bucket does not exist")
)

// HTTPClient to use for REST and view operations.
var MaxIdleConnsPerHost = 256
var HTTPTransport = &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
var HTTPClient = &http.Client{Transport: HTTPTransport}

// PoolSize is the size of each connection pool (per host).
var PoolSize = 64

// PoolOverflow is the number of overflow connections allowed in a
// pool.
var PoolOverflow = PoolSize

var StreamingEndpointClosed = errors.New("Streaming endpoint closed by caller")

// Used to decide whether to skip verification of certificates when
// connecting to an ssl port.

type securitySettings struct {
	useTLS        bool
	securityMutex sync.RWMutex
	caFile        string
	certFile      string
	keyFile       string
}

var settings = &securitySettings{
	useTLS:        false,
	securityMutex: sync.RWMutex{},
	caFile:        "",
	certFile:      "",
	keyFile:       "",
}

func SetUseTLS(value bool) {
	settings.securityMutex.Lock()
	defer settings.securityMutex.Unlock()
	settings.useTLS = value
}

func SetCAFile(ca string) {
	settings.securityMutex.Lock()
	defer settings.securityMutex.Unlock()
	settings.caFile = ca
}

func SetCertFile(cert string) {
	settings.securityMutex.Lock()
	defer settings.securityMutex.Unlock()
	settings.certFile = cert
}

func SetKeyFile(key string) {
	settings.securityMutex.Lock()
	defer settings.securityMutex.Unlock()
	settings.keyFile = key
}

func GetUseTLS() bool {
	settings.securityMutex.RLock()
	defer settings.securityMutex.RUnlock()
	return settings.useTLS
}

func GetCAFile() string {
	settings.securityMutex.RLock()
	defer settings.securityMutex.RUnlock()
	return settings.caFile
}

func GetCertFile() string {
	settings.securityMutex.RLock()
	defer settings.securityMutex.RUnlock()
	return settings.certFile
}

func GetKeyFile() string {
	settings.securityMutex.RLock()
	defer settings.securityMutex.RUnlock()
	return settings.keyFile
}

// AuthHandler is a callback that gets the auth username and password
// for the given bucket.
type AuthHandler interface {
	GetCredentials() (string, string)
}

// RestPool represents a single pool returned from the pools REST API.
type RestPool struct {
	Name         string `json:"name"`
	StreamingURI string `json:"streamingUri"`
	URI          string `json:"uri"`
}

// Pools represents the collection of pools as returned from the REST API.
type Pools struct {
	ComponentsVersion     map[string]string `json:"componentsVersion,omitempty"`
	ImplementationVersion string            `json:"implementationVersion"`
	IsAdmin               bool              `json:"isAdminCreds"`
	UUID                  string            `json:"uuid"`
	Pools                 []RestPool        `json:"pools"`
}

// A Node is a computer in a cluster running the couchbase software.
type Node struct {
	ClusterCompatibility int                `json:"clusterCompatibility"`
	ClusterMembership    string             `json:"clusterMembership"`
	CouchAPIBase         string             `json:"couchApiBase"`
	Hostname             string             `json:"hostname"`
	InterestingStats     map[string]float64 `json:"interestingStats,omitempty"`
	MCDMemoryAllocated   float64            `json:"mcdMemoryAllocated"`
	MCDMemoryReserved    float64            `json:"mcdMemoryReserved"`
	MemoryFree           float64            `json:"memoryFree"`
	MemoryTotal          float64            `json:"memoryTotal"`
	OS                   string             `json:"os"`
	Ports                map[string]int     `json:"ports"`
	Status               string             `json:"status"`
	Uptime               int                `json:"uptime,string"`
	Version              string             `json:"version"`
	ThisNode             bool               `json:"thisNode,omitempty"`
	Services             []string           `json:"services,omitempty"`
}

type BucketInfo struct {
	BucketName string `json:"bucketName"`
	Uuid       string `json:"uuid"`
}

// A Pool of nodes and buckets.
type Pool struct {
	BucketMap  map[string]Bucket
	Nodes      []Node
	Manifest   map[string]*collections.CollectionManifest
	BucketList []*BucketInfo `json:"bucketNames"`

	BucketURL       map[string]string `json:"buckets"`
	ServerGroupsUri string            `json:"serverGroupsUri"`

	client Client
}

// VBucketServerMap is the a mapping of vbuckets to nodes.
type VBucketServerMap struct {
	HashAlgorithm     string   `json:"hashAlgorithm"`
	NumReplicas       int      `json:"numReplicas"`
	ServerList        []string `json:"serverList"`
	VBucketMap        [][]int  `json:"vBucketMap"`
	VBucketForwardMap [][]int  `json:"vBucketMapForward,omitempty"`
}

// Bucket is the primary entry point for most data operations.
type Bucket struct {
	connPools        unsafe.Pointer // *[]*connectionPool
	vBucketServerMap unsafe.Pointer // *VBucketServerMap
	nodeList         unsafe.Pointer // *[]Node

	Capabilities        []string               `json:"bucketCapabilities"`
	CapabilitiesVersion string                 `json:"bucketCapabilitiesVer"`
	Type                string                 `json:"bucketType"`
	Name                string                 `json:"name"`
	NodeLocator         string                 `json:"nodeLocator"`
	Quota               map[string]float64     `json:"quota,omitempty"`
	Replicas            int                    `json:"replicaNumber"`
	URI                 string                 `json:"uri"`
	StreamingURI        string                 `json:"streamingUri"`
	LocalRandomKeyURI   string                 `json:"localRandomKeyUri,omitempty"`
	UUID                string                 `json:"uuid"`
	BasicStats          map[string]interface{} `json:"basicStats,omitempty"`
	Controllers         map[string]interface{} `json:"controllers,omitempty"`
	Storage             string                 `json:"storageBackend,omitempty"`
	NumVbuckets         int                    `json:"numVBuckets,omitempty"`
	// These are used for JSON IO, but isn't used for processing
	// since it needs to be swapped out safely.
	VBSMJson              VBucketServerMap `json:"vBucketServerMap"`
	NodesJSON             []Node           `json:"nodes"`
	Manifest              *collections.CollectionManifest
	CollectionManifestUID string `json:"collectionsManifestUid,omitempty"`

	pool        *Pool
	commonSufix string
}

// PoolServices is all the bucket-independent services in a pool
type PoolServices struct {
	Rev      int            `json:"rev"`
	NodesExt []NodeServices `json:"nodesExt"`
}

// NodeServices is all the bucket-independent services running on
// a node (given by Hostname)
type NodeServices struct {
	Services map[string]int `json:"services,omitempty"`
	Hostname string         `json:"hostname"`
	ThisNode bool           `json:"thisNode"`
}

type ServerGroups struct {
	Groups []ServerGroup `json:"groups"`
}

type ServerGroup struct {
	Name  string `json:"name"`
	Nodes []Node `json:"nodes"`
}

// VBServerMap returns the current VBucketServerMap.
func (b *Bucket) VBServerMap() *VBucketServerMap {
	return (*VBucketServerMap)(atomic.LoadPointer(&(b.vBucketServerMap)))
}

func (b *Bucket) GetMasterNodeForVb(vb uint16) (string, error) {
	vbm := b.VBServerMap()
	if l := len(vbm.VBucketMap); int(vb) >= l {
		return "", ErrorInvalidVbucket
	}

	masterID := vbm.VBucketMap[vb][0]
	if len(vbm.VBucketForwardMap) != 0 {
		masterID = vbm.VBucketForwardMap[vb][0]
	}
	master := b.getMasterNode(masterID)
	return master, nil
}

func (b *Bucket) GetVBmap(addrs []string) (map[string][]uint16, error) {
	vbmap := b.VBServerMap()
	servers := vbmap.ServerList
	if addrs == nil {
		addrs = vbmap.ServerList
	}

	m := make(map[string][]uint16)
	for _, addr := range addrs {
		m[addr] = make([]uint16, 0)
	}

	vbucketMap := vbmap.VBucketMap
	if len(vbmap.VBucketForwardMap) != 0 {
		vbucketMap = vbmap.VBucketForwardMap
	}

	for vbno, idxs := range vbucketMap {
		if len(idxs) == 0 {
			return nil, fmt.Errorf("vbmap: No KV node no for vb %d", vbno)
		} else if idxs[0] < 0 || idxs[0] >= len(servers) {
			return nil, fmt.Errorf("vbmap: Invalid KV node no %d for vb %d", idxs[0], vbno)
		}
		addr := servers[idxs[0]]
		if _, ok := m[addr]; ok {
			m[addr] = append(m[addr], uint16(vbno))
		}
	}

	return m, nil
}

// Returns vb to kv node mapping
func (b *Bucket) GetKvVbMap() (map[uint16]string, error) {
	vbmap := b.VBServerMap()
	servers := vbmap.ServerList
	vbToKvMap := make(map[uint16]string)

	vbucketMap := vbmap.VBucketMap
	if len(vbmap.VBucketForwardMap) != 0 {
		vbucketMap = vbmap.VBucketForwardMap
	}

	for vbno, idxs := range vbucketMap {
		if len(idxs) == 0 {
			return nil, fmt.Errorf("vbmap: No KV node no for vb %d", vbno)
		} else if idxs[0] < 0 || idxs[0] >= len(servers) {
			return nil, fmt.Errorf("vbmap: Invalid KV node no %d for vb %d", idxs[0], vbno)
		}
		vbToKvMap[uint16(vbno)] = servers[idxs[0]]
	}
	return vbToKvMap, nil
}

// Nodes returns the current list of nodes servicing this bucket.
func (b Bucket) Nodes() []Node {
	return *(*[]Node)(atomic.LoadPointer(&b.nodeList))
}

func (b Bucket) getConnPools() []*connectionPool {
	ptr := atomic.LoadPointer(&b.connPools)
	if ptr != nil {
		return *(*[]*connectionPool)(ptr)
	}
	return nil
}

func (b *Bucket) GetBucketURLVersionHash() (string, error) {
	return b.pool.GetBucketURLVersionHash()
}

func (b *Bucket) SetBucketUri(nUri string) {
	b.pool.BucketURL["uri"] = nUri
}

func (b *Bucket) replaceConnPools(with []*connectionPool) {
	for {
		old := atomic.LoadPointer(&b.connPools)
		if atomic.CompareAndSwapPointer(&b.connPools, old, unsafe.Pointer(&with)) {
			if old != nil {
				for _, pool := range *(*[]*connectionPool)(old) {
					if pool != nil {
						pool.Close()
					}
				}
			}
			return
		}
	}
}

func (b Bucket) getConnPool(i int) *connectionPool {
	if i < 0 {
		return nil
	}

	p := b.getConnPools()
	if len(p) > i {
		return p[i]
	}
	return nil
}

func (b Bucket) getMasterNode(i int) (host string) {
	if i < 0 {
		return host
	}

	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("bucket(%v) getMasterNode crashed: %v\n", b.Name, r)
			logging.Errorf("%s", logging.StackTrace())
			host = ""
		}
	}()

	p := b.getConnPools()
	if len(p) > i && p[i] != nil {
		host = p[i].host
	}
	return host
}

func (b Bucket) authHandler() (ah AuthHandler) {
	if b.pool != nil {
		ah = b.pool.client.ah
	}
	return
}

// NodeAddresses gets the (sorted) list of memcached node addresses
// (hostname:port).
func (b Bucket) NodeAddresses() []string {
	vsm := b.VBServerMap()
	rv := make([]string, len(vsm.ServerList))
	copy(rv, vsm.ServerList)
	sort.Strings(rv)
	return rv
}

// CommonAddressSuffix finds the longest common suffix of all
// host:port strings in the node list.
func (b Bucket) CommonAddressSuffix() string {
	input := []string{}
	for _, n := range b.Nodes() {
		input = append(input, n.Hostname)
	}
	return FindCommonSuffix(input)
}

// A Client is the starting point for all services across all buckets
// in a Couchbase cluster.
type Client struct {
	BaseURL *url.URL
	ah      AuthHandler
	Info    Pools
}

func maybeAddAuth(req *http.Request, ah AuthHandler) {
	if ah != nil {
		user, pass := ah.GetCredentials()
		req.Header.Set("Authorization", "Basic "+
			base64.StdEncoding.EncodeToString([]byte(user+":"+pass)))
	}
}

func clientConfigForX509(caFile, certFile string) (*tls.Config, error) {
	config := &tls.Config{}
	caCertPool := x509.NewCertPool()
	pemFile := certFile
	// prefer reading from caFile if present
	if len(caFile) > 0 {
		pemFile = caFile
	}
	caCert, err := ioutil.ReadFile(pemFile)
	if err != nil {
		logging.Errorf("While reading SSL certificates from file: %s encountered error : %v", pemFile, err)
		return nil, err
	}
	caCertPool.AppendCertsFromPEM(caCert)
	config.RootCAs = caCertPool
	return config, nil
}

func queryRestAPI(
	baseURL *url.URL,
	path string,
	authHandler AuthHandler,
	out interface{}) error {
	u := *baseURL
	u.User = nil
	if q := strings.Index(path, "?"); q > 0 {
		u.Path = path[:q]
		u.RawQuery = path[q+1:]
	} else {
		u.Path = path
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}
	maybeAddAuth(req, authHandler)

	res, err := HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(res.Body, 512))
		return fmt.Errorf("HTTP error %v getting %q: %s",
			res.Status, u.String(), bod)
	}

	bodyBytes, _ := ioutil.ReadAll(res.Body)
	responseBody := ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
	d := json.NewDecoder(responseBody)
	if err = d.Decode(&out); err != nil {
		logging.Errorf("queryRestAPI: Error while decoding the response from path: %s, response body: %s, err: %v", path, string(bodyBytes), err)
		return err
	}
	logging.Tracef("Query %v returns %+v", u.String(), out)
	return nil
}

// Pool streaming API based observe-callback wrapper
func (c *Client) RunObservePool(pool string, callb func(interface{}) error, cancel chan bool) error {

	path := "/poolsStreaming/" + pool
	decoder := func(bs []byte) (interface{}, error) {
		var pool Pool
		var err error
		if err = json.Unmarshal(bs, &pool); err != nil {
			logging.Errorf("RunObservePool: Error while decoding the response from path: %s, response body: %s, err: %v", path, string(bs), err)
		}
		return &pool, err
	}

	return c.runObserveStreamingEndpoint(path, decoder, callb, cancel)
}

func (c *Client) RunObserveCollectionManifestChanges(pool, bucket string, callb func(interface{}) error, cancel chan bool) error {

	path := "/pools/" + pool + "/bs/" + bucket
	decoder := func(bs []byte) (interface{}, error) {
		var b Bucket
		var err error
		if err = json.Unmarshal(bs, &b); err != nil {
			logging.Errorf("RunObserveCollectionManifestChanges: Error while decoding the response from path: %s, response body: %s, err: %v", path, string(bs), err)
		}
		return &b, err
	}

	return c.runObserveStreamingEndpoint(path, decoder, callb, cancel)
}

// NodeServices streaming API based observe-callback wrapper
func (c *Client) RunObserveNodeServices(pool string, callb func(interface{}) error, cancel chan bool) error {

	path := "/pools/" + pool + "/nodeServicesStreaming"
	decoder := func(bs []byte) (interface{}, error) {
		var ps PoolServices
		var err error
		if err = json.Unmarshal(bs, &ps); err != nil {
			logging.Errorf("RunObserveNodeServices: Error while decoding the response from path: %s, response body: %s, err: %v", path, string(bs), err)
		}
		return &ps, err
	}

	return c.runObserveStreamingEndpoint(path, decoder, callb, cancel)
}

// Helper for observing and calling back streaming endpoint
func (c *Client) runObserveStreamingEndpoint(path string,
	decoder func([]byte) (interface{}, error),
	callb func(interface{}) error,
	cancel chan bool) error {

	u := *c.BaseURL
	u.User = nil
	authHandler := c.ah
	if q := strings.Index(path, "?"); q > 0 {
		u.Path = path[:q]
		u.RawQuery = path[q+1:]
	} else {
		u.Path = path
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}
	maybeAddAuth(req, authHandler)

	res, err := HTTPClient.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(res.Body, 512))
		res.Body.Close()
		return fmt.Errorf("HTTP error %v getting %q: %s",
			res.Status, u.String(), bod)
	}

	reader := bufio.NewReader(res.Body)

	resChannel := make(chan []byte)
	errorChannel := make(chan error, 1)
	go func(resChannel chan<- []byte, errorChannel chan<- error) {
		for {
			bs, err := reader.ReadBytes('\n')
			if err != nil {
				errorChannel <- err
				return
			}
			resChannel <- bs
		}
	}(resChannel, errorChannel)

	defer res.Body.Close()

	for {
		select {
		case <-cancel:
			return StreamingEndpointClosed

		case bs := <-resChannel:
			if len(bs) == 1 && bs[0] == '\n' {
				continue
			}

			object, err := decoder(bs)
			if err != nil {
				return err
			}

			err = callb(object)
			if err != nil {
				return err
			}

		case err := <-errorChannel:
			return err
		}
	}
}

func (c *Client) parseURLResponse(path string, out interface{}) error {
	return queryRestAPI(c.BaseURL, path, c.ah, out)
}

func (b *Bucket) parseURLResponse(path string, out interface{}) error {
	nodes := b.Nodes()
	if len(nodes) == 0 {
		return errors.New("no couch rest URLs")
	}

	// Pick a random node to start querying.
	startNode := rand.Intn(len(nodes))
	maxRetries := len(nodes)
	for i := 0; i < maxRetries; i++ {
		node := nodes[(startNode+i)%len(nodes)] // Wrap around the nodes list.
		// Skip non-healthy nodes.
		if node.Status != "healthy" {
			continue
		}

		url := &url.URL{
			Host:   node.Hostname,
			Scheme: "http",
		}

		err := queryRestAPI(url, path, b.pool.client.ah, out)
		if err == nil {
			return err
		}
	}
	return errors.New("all nodes failed to respond")
}

type basicAuth struct {
	url string
}

func (b basicAuth) GetCredentials() (string, string) {
	u, err := ParseURL(b.url)
	if err != nil {
		return "", ""
	}

	adminUser, adminPasswd, err := cbauth.GetHTTPServiceAuth(u.Host)
	if err != nil {
		return "", ""
	}
	return adminUser, adminPasswd
}

func basicAuthFromURL(us string) (ah AuthHandler) {
	return basicAuth{url: us}
}

// ConnectWithAuth connects to a couchbase cluster with the given
// authentication handler.
func ConnectWithAuth(baseU string, ah AuthHandler) (c Client, err error) {
	c.BaseURL, err = ParseURL(baseU)
	if err != nil {
		return
	}
	c.ah = ah

	return c, c.parseURLResponse("/pools", &c.Info)
}

// Connect to a couchbase cluster.  An authentication handler will be
// created from the userinfo in the URL if provided.
func Connect(baseU string) (Client, error) {
	return ConnectWithAuth(baseU, basicAuthFromURL(baseU))
}

func (b *Bucket) RefreshFromBucket(bucket *Bucket) {
	b.init(bucket)
}

func (b *Bucket) Refresh() error {
	pool := b.pool
	tmpb := &Bucket{}

	// Unescape the b.URI as it pool.client.parseURLResponse will again escape it.
	bucketURI, err1 := url.PathUnescape(b.URI)
	if err1 != nil {
		return fmt.Errorf("Malformed bucket URI path %v, error %v", b.URI, err1)
	}

	err := pool.client.parseURLResponse(bucketURI, tmpb)
	if err != nil {
		return err
	}
	b.init(tmpb)

	return nil
}

func (b *Bucket) RefreshWithTerseBucket() error {
	pool := b.pool

	_, tmpb, err := pool.getTerseBucket(b.Name)
	if err != nil {
		return err
	}

	b.init(tmpb)
	return nil
}

func (b *Bucket) RefreshBucketManifest() error {
	pool := b.pool
	manifest, err := pool.RefreshBucketManifest(b.Name)
	if err != nil {
		return err
	}
	b.Manifest = manifest
	return nil
}

func (b *Bucket) init(nb *Bucket) {
	connHost, _, _ := net.SplitHostPort(b.pool.client.BaseURL.Host)
	for i := range nb.NodesJSON {
		nb.NodesJSON[i].Hostname = normalizeHost(connHost, nb.NodesJSON[i].Hostname)
	}

	newcps := make([]*connectionPool, len(nb.VBSMJson.ServerList))
	var cfg *tls.Config = nil
	var err error
	if GetUseTLS() == true {
		cfg, err = clientConfigForX509(GetCAFile(), GetCertFile())
		if err != nil {
			logging.Errorf("Error fetching TLS config: %v", err)
			cfg = nil
		}
	}

	for i := range newcps {
		nb.VBSMJson.ServerList[i] = normalizeHost(connHost, nb.VBSMJson.ServerList[i])
		if GetUseTLS() == true {
			ps, err := b.pool.client.GetPoolServices("default")
			if err == nil {
				server, notSameNode, errorKv := MapKVtoSSL(nb.VBSMJson.ServerList[i], &ps)
				if errorKv == nil && notSameNode == true {
					nb.VBSMJson.ServerList[i] = server
				} else {
					logging.Errorf("Error fetching the KV SSL port: %s", errorKv)
					SetUseTLS(false)
					cfg = nil
				}
			} else {
				logging.Errorf("Error while fetching Pool Services: %s \n", err)
				SetUseTLS(false)
				cfg = nil
			}
		}
		newcps[i] = newConnectionPool(
			nb.VBSMJson.ServerList[i],
			b.authHandler(), PoolSize, PoolOverflow, cfg)
	}
	b.replaceConnPools(newcps)
	atomic.StorePointer(&b.vBucketServerMap, unsafe.Pointer(&nb.VBSMJson))
	atomic.StorePointer(&b.nodeList, unsafe.Pointer(&nb.NodesJSON))
}

func (p *Pool) getTerseBucket(bucketn string) (bool, *Bucket, error) {
	nb := &Bucket{}
	err := p.client.parseURLResponse(p.BucketURL["terseBucketsBase"]+bucketn, nb)
	if err != nil {
		// bucket list is out of sync with cluster bucket list
		// bucket might have got deleted.
		if strings.Contains(err.Error(), "HTTP error 404") {
			return true, nil, err
		}
		return false, nil, err
	}
	return false, nb, nil
}

func (p *Pool) getCollectionManifest(bucketn string, version uint32) (retry bool,
	manifest *collections.CollectionManifest, err error) {
	if version >= collections.COLLECTION_SUPPORTED_VERSION {
		// For each bucket, update collection manifest
		manifest = &collections.CollectionManifest{}
		err = p.client.parseURLResponse("pools/default/buckets/"+bucketn+"/scopes", manifest)
		if err != nil {
			// bucket list is out of sync with cluster bucket list
			// bucket might have got deleted.
			if strings.Contains(err.Error(), "HTTP error 404") {
				retry = true
				return
			}
			return
		}
	}

	return
}

// refreshBucket only calls terseBucket endpoint to fetch the bucket info.
func (p *Pool) refreshBucket(bucketn string) error {
	retryCount := 0
loop:
	retry, nb, err := p.getTerseBucket(bucketn)
	if retry {
		retryCount++
		if retryCount > 5 {
			return err
		}
		logging.Warnf("cluster_info: Out of sync for bucket %s. Retrying to getTerseBucket. retry count %v", bucketn, retryCount)
		time.Sleep(5 * time.Millisecond)
		goto loop
	}
	if err != nil {
		return err
	}
	nb.pool = p
	nb.init(nb)
	p.BucketMap[nb.Name] = *nb

	return nil
}

func (p *Pool) Refresh() (err error) {
	bucketMap := make(map[string]Bucket)
	manifestMap := make(map[string]*collections.CollectionManifest)

	version := p.GetClusterCompatVersion()
loop:
	buckets := []Bucket{}
	err = p.client.parseURLResponse(p.BucketURL["uri"], &buckets)
	if err != nil {
		return err
	}
	for _, b := range buckets {
		// TODO: We can remove this call
		retry, nb, err := p.getTerseBucket(b.Name)
		if retry {
			logging.Warnf("cluster_info: Out of sync for bucket %s. Retrying..", b.Name)
			goto loop
		}
		if err != nil {
			return err
		}

		// Add more info if needed
		nb.Storage = b.Storage
		nb.NumVbuckets = b.NumVbuckets
		nb.pool = p
		nb.init(nb)
		bucketMap[nb.Name] = *nb

		retry, manifest, err := p.getCollectionManifest(b.Name, version)
		if retry {
			logging.Warnf("cluster_info: Out of sync for bucket %s. Retrying for getBucketManifest..", b.Name)
			time.Sleep(5 * time.Millisecond)
			goto loop
		}
		if err != nil {
			return err
		}

		if version >= collections.COLLECTION_SUPPORTED_VERSION {
			manifestMap[b.Name] = manifest
		}
	}

	p.BucketMap = bucketMap
	p.Manifest = manifestMap
	return nil
}

func (p *Pool) RefreshBucketManifest(bucket string) (*collections.CollectionManifest, error) {
	retryCount := 0
retry:
	manifest := &collections.CollectionManifest{}
	err := p.client.parseURLResponse("pools/default/buckets/"+bucket+"/scopes", manifest)
	if err != nil {
		// bucket list is out of sync with cluster bucket list
		// bucket might have got deleted.
		if strings.Contains(err.Error(), "HTTP error 404") {
			logging.Warnf("cluster_info: Out of sync for bucket %s. Retrying..", bucket)
			time.Sleep(1 * time.Millisecond)
			retryCount++
			if retryCount > 5 {
				return nil, err
			}
			goto retry
		}
		return nil, err
	}
	return manifest, nil
}

func (p *Pool) GetServerGroups() (groups ServerGroups, err error) {
	err = p.client.parseURLResponse(p.ServerGroupsUri, &groups)
	return
}

// GetBucketURLVersionHash will prase the p.BucketURI and extract version hash from it
// Parses /pools/default/buckets?v=<$ver>&uuid=<$uid> and returns $ver
func (p *Pool) GetBucketURLVersionHash() (string, error) {
	b := p.BucketURL["uri"]
	u, err := url.Parse(b)
	if err != nil {
		return "", fmt.Errorf("Unable to parse BucketURL: %v in PoolChangeNotification", b)
	}
	m, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", fmt.Errorf("Unable to extract version hash from BucketURL: %v in PoolChangeNotification", b)
	}
	return m["v"][0], nil
}

func (c *Client) CallPoolURI(name string) (p Pool, err error) {
	var poolURI string
	for _, p := range c.Info.Pools {
		if p.Name == name {
			poolURI = p.URI
			break
		}
	}
	if poolURI == "" {
		return p, errors.New("No pool named " + name)
	}

	if err = c.parseURLResponse(poolURI, &p); err != nil {
		return
	}

	p.client = *c
	return
}

// GetPool gets a pool from within the couchbase cluster (usually
// "default").
func (c *Client) GetPool(name string) (p Pool, err error) {
	var poolURI string
	for _, p := range c.Info.Pools {
		if p.Name == name {
			poolURI = p.URI
		}
	}
	if poolURI == "" {
		return p, errors.New("No pool named " + name)
	}

	err = c.parseURLResponse(poolURI, &p)

	p.client = *c

	err = p.Refresh()
	return
}

// GetPoolServices returns all the bucket-independent services in a pool.
// (See "Exposing services outside of bucket context" in http://goo.gl/uuXRkV)
func (c *Client) GetPoolServices(name string) (ps PoolServices, err error) {
	var poolName string
	for _, p := range c.Info.Pools {
		if p.Name == name {
			poolName = p.Name
		}
	}
	if poolName == "" {
		return ps, errors.New("No pool named " + name)
	}

	poolURI := "/pools/" + poolName + "/nodeServices"
	err = c.parseURLResponse(poolURI, &ps)

	return
}

// Close marks this bucket as no longer needed, closing connections it
// may have open.
func (b *Bucket) Close() {
	connPools := b.getConnPools()
	if connPools != nil {
		for _, c := range connPools {
			if c != nil {
				c.Close()
			}
		}
		atomic.StorePointer(&b.connPools, nil)
	}
}

func bucketFinalizer(b *Bucket) {
	connPools := b.getConnPools()
	if connPools != nil {
		logging.Debugf("Warning: Finalizing a bucket with active connections.")
	}
}

func (p *Pool) Connect(baseU string) (err error) {
	p.client, err = Connect(baseU)
	return
}

// Filter out magma buckets
func (p *Pool) GetBuckets() []string {
	buckets := make([]string, 0, len(p.BucketMap))
	for bucketName := range p.BucketMap {
		buckets = append(buckets, bucketName)
	}
	return buckets
}

// GetBucket gets a bucket from within this pool.
func (p *Pool) GetBucket(name string) (*Bucket, error) {
	rv, ok := p.BucketMap[name]
	if !ok {
		return nil, errors.New("No bucket named " + name)
	}
	runtime.SetFinalizer(&rv, bucketFinalizer)
	return &rv, nil
}

func (p *Pool) GetBucketStorage(bucketName string) (string, error) {
	b, ok := p.BucketMap[bucketName]
	if !ok {
		return "", fmt.Errorf("No bucket named %s", bucketName)
	}
	return b.Storage, nil
}

func (p *Pool) GetNumVbuckets(bucketName string) int {
	b, ok := p.BucketMap[bucketName]
	if !ok {
		return 1024
	}
	return b.NumVbuckets
}

func (p *Pool) GetBucketList() map[string]struct{} {
	bucketList := make(map[string]struct{})
	for bucketName := range p.BucketMap {
		bucketList[bucketName] = struct{}{}
	}
	return bucketList
}

func (p *Pool) GetCollectionID(bucket, scope, collection string) (uint32, error) {
	version := p.GetClusterCompatVersion()
	if version >= collections.COLLECTION_SUPPORTED_VERSION {
		if manifest, ok := p.Manifest[bucket]; ok {
			return manifest.GetCollectionID(scope, collection)
		}
		return 0, ErrBucketNotFound
	}
	return 0, nil
}

func (p *Pool) GetScopes(bucketName string) map[string][]string {
	version := p.GetClusterCompatVersion()
	if version >= collections.COLLECTION_SUPPORTED_VERSION {
		if manifest, ok := p.Manifest[bucketName]; ok {
			return manifest.GetScopes()
		}
	}
	return nil
}

func (p *Pool) GetCollectionManifest(bucketName string) *collections.CollectionManifest {
	cm := &collections.CollectionManifest{}
	version := p.GetClusterCompatVersion()
	if version < collections.COLLECTION_SUPPORTED_VERSION {
		return cm
	}

	tmpManifest, ok := p.Manifest[bucketName]
	if !ok {
		return cm
	}

	cm.UID = tmpManifest.UID
	cm.Scopes = make([]collections.CollectionScope, 0, len(tmpManifest.Scopes))
	for _, scope := range tmpManifest.Scopes {
		s := collections.CollectionScope{
			Name:        scope.Name,
			UID:         scope.UID,
			Collections: make([]collections.Collection, 0, len(scope.Collections)),
		}

		for _, col := range scope.Collections {
			c := collections.Collection{
				Name: col.Name,
				UID:  col.UID,
			}

			s.Collections = append(s.Collections, c)
		}
		cm.Scopes = append(cm.Scopes, s)
	}
	return cm
}

func (p *Pool) GetUniqueBSCIds(bucket, scope, collection string) (uint32, uint32, error) {
	manifest, ok := p.Manifest[bucket]
	if !ok {
		return 0, 0, ErrBucketNotFound
	}
	return manifest.GetScopeAndCollectionID(scope, collection)
}

func (p *Pool) GetManifestID(bucket string) (string, error) {
	version := p.GetClusterCompatVersion()
	if version >= collections.COLLECTION_SUPPORTED_VERSION {
		if manifest, ok := p.Manifest[bucket]; ok {
			return manifest.GetManifestId(), nil
		}
		return "0", collections.COLLECTION_ID_NIL
	}
	return "0", nil
}

func (p *Pool) GetClusterCompatVersion() uint32 {
	version := (uint32)(math.MaxUint32)
	for _, n := range p.Nodes {
		// ClusterCompatibility = major * 0x10000 + minor
		v := uint32(n.ClusterCompatibility / 65536)
		if v < version {
			version = v
		}
	}
	return version
}

// GetPool gets the pool to which this bucket belongs.
func (b *Bucket) GetPool() *Pool {
	return b.pool
}

// GetClient gets the client from which we got this pool.
func (p *Pool) GetClient() *Client {
	return &p.client
}

// GetBucket is a convenience function for getting a named bucket from
// a URL
func GetBucket(endpoint, poolname, bucketname string) (*Bucket, error) {
	var err error
	client, err := Connect(endpoint)
	if err != nil {
		return nil, err
	}

	pool, err := client.GetPool(poolname)
	if err != nil {
		return nil, err
	}

	return pool.GetBucket(bucketname)
}

// Make hostnames comparable for terse-buckets info and old buckets info
func normalizeHost(ch, h string) string {
	host, port, err := net.SplitHostPort(h)
	if err != nil {
		logging.Errorf("Error parsing %rs: %v", h, err)
		return h
	}
	if host == "$HOST" {
		host = ch
	}
	return net.JoinHostPort(host, port)
}

func (b *Bucket) GetDcpConn(name DcpFeedName, host string) (*memcached.Client, error) {
	for _, sconn := range b.getConnPools() {
		if sconn.host == host {
			return sconn.GetDcpConn(name)
		}
	}

	return nil, fmt.Errorf("no pool found")
}

func (b *Bucket) GetMcConn(host string) (*memcached.Client, error) {
	for _, sconn := range b.getConnPools() {
		if sconn.host == host {
			// Get connection without doing DCP_OPEN
			mc, err := sconn.Get()
			if err != nil {
				return nil, err
			}
			return mc, nil
		}
	}

	return nil, fmt.Errorf("no pool found")
}
