package util

import (
	"bytes"
	"compress/flate"
	"crypto/md5"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/common"
	cm "github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/collections"
	couchbase "github.com/couchbase/eventing/dcp"
	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/gen/flatbuf/cfg"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gomemcached"
	flatbuffers "github.com/google/flatbuffers/go"
)

const (
	EventingAdminService = "eventingAdminPort"
	EventingAdminSSL     = "eventingSSL"
	DataService          = "kv"
	DataServiceSSL       = "kvSSL"
	MgmtService          = "mgmt"
	MgmtServiceSSL       = "mgmtSSL"

	EPSILON  = 1e-5
	dict     = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ*&"
	nameDict = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

var GocbCredsRequestCounter = 0
var AppNotExist = errors.New("App content doesn't exist or is empty")
var ErrWildcardNotAllowed = errors.New("Wildcard not allowed")

type securitySettings struct {
	useTLS         bool
	mu             sync.RWMutex
	securityConfig *common.SecuritySetting
}

var settings = &securitySettings{
	useTLS:         false,
	mu:             sync.RWMutex{},
	securityConfig: nil,
}

func SetUseTLS(value bool) {
	settings.mu.Lock()
	defer settings.mu.Unlock()
	settings.useTLS = value
}

func getLocalUseTLS() bool {
	settings.mu.RLock()
	defer settings.mu.RUnlock()
	return settings.useTLS
}

func SetSecurityConfig(config *common.SecuritySetting) {
	settings.mu.RLock()
	defer settings.mu.RUnlock()
	settings.securityConfig = config
}

func GetSecurityConfig() *common.SecuritySetting {
	settings.mu.RLock()
	defer settings.mu.RUnlock()
	return settings.securityConfig
}

func CheckTLSandGetClient(HTTPRequestTimeout time.Duration) *Client {
	var netClient *Client
	if getLocalUseTLS() {
		netClient = NewTLSClient(HTTPRequestTimeout, GetSecurityConfig())
	} else {
		netClient = NewClient(HTTPRequestTimeout)
	}
	return netClient
}

func CheckTLSandReplaceProtocol(connStr string, args ...interface{}) string {
	var connectionStr string
	connectionStr = fmt.Sprintf(connStr, args...)
	if getLocalUseTLS() {
		if u, err := url.Parse(connectionStr); err == nil {
			host, _, _ := net.SplitHostPort(u.Host)
			ip := net.ParseIP(host)
			if (ip == nil && strings.EqualFold(host, "localhost")) || (ip != nil && ip.IsLoopback()) {
				connectionStr = strings.ReplaceAll(connectionStr, "https://", "http://")
				return connectionStr
			}
		}
		connectionStr = strings.ReplaceAll(connectionStr, "http://", "https://")
		return connectionStr
	}
	connectionStr = strings.ReplaceAll(connectionStr, "https://", "http://")
	return connectionStr
}

type Uint16Slice []uint16

func (s Uint16Slice) Len() int           { return len(s) }
func (s Uint16Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Uint16Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type Config map[string]interface{}

type ConfigHolder struct {
	ptr unsafe.Pointer
}

type DynamicAuthenticator struct {
	Caller string
}

func (dynAuth *DynamicAuthenticator) SupportsTLS() bool {
	return true
}

func (dynAuth *DynamicAuthenticator) SupportsNonTLS() bool {
	return true
}

func (dynAuth *DynamicAuthenticator) Certificate(req gocb.AuthCertRequest) (*tls.Certificate, error) {
	return nil, nil
}

func (h *ConfigHolder) Store(conf Config) {
	atomic.StorePointer(&h.ptr, unsafe.Pointer(&conf))
}

func (h *ConfigHolder) Load() Config {
	confPtr := atomic.LoadPointer(&h.ptr)
	return *(*Config)(confPtr)
}

func NewConfig(data interface{}) (Config, error) {
	config := make(Config)
	err := config.Update(data)
	return config, err
}

func (config Config) Update(data interface{}) error {
	switch v := data.(type) {
	case Config:
		for key, value := range v {
			config.Set(key, value)
		}
	default:
	}
	return nil
}

func (config Config) Set(key string, value interface{}) Config {
	config[key] = value
	return config
}

func ComputeMD5(data []byte) ([]byte, error) {
	hash := md5.New()
	if _, err := io.Copy(hash, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	checksum := hash.Sum(nil)
	return checksum, nil
}

type PayloadHash struct {
	Fragmentcnt  int      `json:"fragment_count"`
	Fragmenthash [][]byte `json:"fragment_hash"`
}

func (payloadhash *PayloadHash) Update(payload []byte, fragmentSize int) error {
	length := len(payload)
	fragmentCount := length / fragmentSize
	if length%fragmentSize != 0 {
		fragmentCount++
	}
	payloadhash.Fragmentcnt = fragmentCount
	payloadhash.Fragmenthash = make([][]byte, fragmentCount)
	for idx := 0; idx < fragmentCount; idx++ {
		curridx := idx * fragmentSize
		lastidx := (idx + 1) * fragmentSize
		if lastidx > length {
			lastidx = length
		}
		checksum, err := ComputeMD5(payload[curridx:lastidx])
		if err != nil {
			return err
		}
		payloadhash.Fragmenthash[idx] = checksum
	}
	return nil
}

func SprintDCPCounts(counts map[mcd.CommandCode]uint64) (string, uint64, time.Time) {
	line := ""
	ops := uint64(0)
	currTimestamp := time.Now()

	for i := 0; i < 256; i++ {
		opcode := mcd.CommandCode(i)
		if n, ok := counts[opcode]; ok {
			line += fmt.Sprintf("%s:%v ", mcd.CommandNames[opcode], n)
			ops += n
		}
	}
	return strings.TrimRight(line, " "), ops, currTimestamp
}

func SprintV8Counts(counts map[string]uint64) string {
	line := ""
	for k, v := range counts {
		line += fmt.Sprintf("%s:%v ", k, v)
	}
	return strings.TrimRight(line, " ")
}

func NsServerNodesAddresses(hostaddress string) ([]string, error) {
	logPrefix := "util::NsServerNodesAddresses"
	cic, err := FetchClusterInfoClient(hostaddress)
	if err != nil {
		return nil, err
	}
	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	var nsServerAddrs []NodeId
	if getLocalUseTLS() {
		nsServerAddrs = cinfo.GetNodesByServiceType(MgmtServiceSSL)
	} else {
		nsServerAddrs = cinfo.GetNodesByServiceType(MgmtService)
	}

	nsServerNodes := []string{}
	var addr string
	for _, nsServerAddr := range nsServerAddrs {
		if getLocalUseTLS() {
			addr, err = cinfo.GetServiceAddress(nsServerAddr, MgmtServiceSSL)
		} else {
			addr, err = cinfo.GetServiceAddress(nsServerAddr, MgmtService)
		}
		if err != nil {
			logging.Errorf("%s Failed to get ns_server node address, err: %v", logPrefix, err)
			continue
		}
		nsServerNodes = append(nsServerNodes, addr)
	}
	sort.Strings(nsServerNodes)
	return nsServerNodes, nil
}

func KVNodesAddresses(hostaddress, bucket string) ([]string, error) {
	cic, err := FetchClusterInfoClient(hostaddress)
	if err != nil {
		return nil, err
	}
	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	kvAddrs, err := cinfo.GetNodesByBucket(bucket)
	if err != nil {
		return nil, err
	}

	kvNodes := []string{}
	var addr string
	for _, kvAddr := range kvAddrs {
		if couchbase.GetUseTLS() {
			addr, _ = cinfo.GetServiceAddress(kvAddr, DataServiceSSL)
		} else {
			addr, _ = cinfo.GetServiceAddress(kvAddr, DataService)
		}
		kvNodes = append(kvNodes, addr)
	}

	sort.Strings(kvNodes)

	return kvNodes, nil
}

func fetchClusterCompatVersion(hostaddress string) (uint32, uint32, error) {
	var major, minor uint32
	cic, err := FetchClusterInfoClient(hostaddress)
	if err != nil {
		return major, minor, err
	}
	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	major, minor = cinfo.GetNodeCompatVersion()
	return major, minor, nil
}

func CollectionAware(hostaddress string) (bool, error) {
	ver, _, err := fetchClusterCompatVersion(hostaddress)
	if err != nil {
		return false, err
	}
	if ver >= collections.COLLECTION_SUPPORTED_VERSION {
		return true, nil
	}
	return false, nil
}

func IgnorePurgeSeqFlagAvailable(hostaddress string) (bool, error) {
	majorVer, minorVer, err := fetchClusterCompatVersion(hostaddress)
	if err != nil {
		return false, err
	}

	if majorVer >= 7 && minorVer >= 2 {
		return true, nil
	}

	return false, nil
}

func EventingNodesAddresses(hostaddress string) ([]string, error) {
	logPrefix := "util::EventingNodesAddresses"
	cic, err := FetchClusterInfoClient(hostaddress)
	if err != nil {
		return nil, err
	}
	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	var eventingAddrs []NodeId

	if getLocalUseTLS() {
		eventingAddrs = cinfo.GetNodesByServiceType(EventingAdminSSL)
	} else {
		eventingAddrs = cinfo.GetNodesByServiceType(EventingAdminService)
	}

	eventingNodes := []string{}
	var addr string
	for _, eventingAddr := range eventingAddrs {
		if getLocalUseTLS() {
			addr, err = cinfo.GetServiceAddress(eventingAddr, EventingAdminSSL)
			host, _, _ := net.SplitHostPort(addr)
			ip := net.ParseIP(host)
			if err == nil && (ip == nil && strings.EqualFold(host, "localhost")) || (ip != nil && ip.IsLoopback()) {
				addr, err = cinfo.GetServiceAddress(eventingAddr, EventingAdminService)
			}
		} else {
			addr, err = cinfo.GetServiceAddress(eventingAddr, EventingAdminService)
		}
		if err != nil {
			logging.Errorf("%s Failed to get eventing node address, err: %v", logPrefix, err)
			continue
		}
		eventingNodes = append(eventingNodes, addr)
	}
	sort.Strings(eventingNodes)
	return eventingNodes, nil
}

func CurrentEventingNodeAddress(hostaddress string) (string, error) {
	logPrefix := "util::CurrentEventingNodeAddress"

	cic, err := FetchClusterInfoClient(hostaddress)
	if err != nil {
		return "", err
	}

	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	cNodeID := cinfo.GetCurrentNode()
	var eventingNode string
	if getLocalUseTLS() {
		eventingNode, err = cinfo.GetServiceAddress(cNodeID, EventingAdminSSL)
		host, _, _ := net.SplitHostPort(eventingNode)
		ip := net.ParseIP(host)
		if err == nil && (ip == nil && strings.EqualFold(host, "localhost")) || (ip != nil && ip.IsLoopback()) {
			eventingNode, err = cinfo.GetServiceAddress(cNodeID, EventingAdminService)
		}

	} else {
		eventingNode, err = cinfo.GetServiceAddress(cNodeID, EventingAdminService)
	}
	if err != nil {
		logging.Errorf("%s Failed to get current eventing node address, err: %v", logPrefix, err)
		return "", err
	}
	return eventingNode, nil
}

func LocalEventingServiceHost(hostaddress string) (string, error) {
	cinfo, err := FetchNewClusterInfoCache(hostaddress)
	if err != nil {
		return "", err
	}

	srvAddr, err := cinfo.GetLocalServiceHost(EventingAdminService)
	if err != nil {
		return "", err
	}

	return srvAddr, nil
}

// empty scope and collection argument will check for bucket existence
func ValidateAndCheckKeyspaceExist(bucket, scope, collection, hostaddress string, wildcardAllowed bool) error {
	exist := CheckBucketExist(bucket, hostaddress)
	if !exist {
		return couchbase.ErrBucketNotFound
	}

	if scope == "*" || collection == "*" {
		if !wildcardAllowed {
			return ErrWildcardNotAllowed
		}

		if scope == "*" && collection == "*" {
			return nil
		}

		if scope != "*" && collection == "*" {
			collection = ""
		}
	}

	cic, err := FetchClusterInfoClient(hostaddress)
	if err != nil {
		return err
	}

	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	_, _, _, err = cinfo.GetUniqueBSCIds(bucket, scope, collection)
	return err
}

func CheckKeyspaceExist(keyspace *common.Keyspace, hostaddress string) bool {
	err := ValidateAndCheckKeyspaceExist(keyspace.BucketName, keyspace.ScopeName, keyspace.CollectionName, hostaddress, false)
	return (err == couchbase.ErrBucketNotFound || err == collections.SCOPE_NOT_FOUND || err == collections.COLLECTION_NOT_FOUND)
}

func CheckBucketExist(bucket, hostaddress string) bool {
	cic, err := FetchClusterInfoClient(hostaddress)
	if err != nil {
		return true
	}

	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()
	kvAddrs, err := cinfo.GetNodesByBucket(bucket)
	if err != nil {
		if err.Error() == fmt.Sprintf("No bucket named "+bucket) {
			return false
		}
		return true
	}

	return (len(kvAddrs) != 0)
}

func CountActiveKVNodes(bucket, hostaddress string) int {
	cic, err := FetchClusterInfoClient(hostaddress)
	if err != nil {
		return -1
	}
	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	kvAddrs, err := cinfo.GetNodesByBucket(bucket)
	if err != nil {
		if err.Error() == fmt.Sprintf("No bucket named "+bucket) {
			return 0
		}
		return -1
	}

	return len(kvAddrs)
}

func KVVbMap(bucket, hostaddress string) (map[uint16]string, error) {
	logPrefix := "util::KVVbMap"
	cic, err := FetchClusterInfoClient(hostaddress)
	if err != nil {
		return nil, err
	}
	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	var serviceType string

	if couchbase.GetUseTLS() {
		serviceType = DataServiceSSL
	} else {
		serviceType = DataService
	}

	kvAddrs := cinfo.GetNodesByServiceType(serviceType)
	kvVbMap := make(map[uint16]string)

	for _, kvAddr := range kvAddrs {
		addr, err := cinfo.GetServiceAddress(kvAddr, serviceType)
		if err != nil {
			logging.Errorf("%s Failed to get address of KV host: %rs, err: %v", logPrefix, kvAddr, err)
			return nil, err
		}

		vbs, err := cinfo.GetVBuckets(kvAddr, bucket)
		if err != nil {
			logging.Errorf("%s Failed to get vbuckets for given kv: %rs, err: %v", logPrefix, kvAddr, err)
			continue
		}

		for i := 0; i < len(vbs); i++ {
			kvVbMap[uint16(vbs[i])] = addr
		}
	}

	return kvVbMap, nil
}

// Write to the admin console
func Console(clusterAddr string, format string, v ...interface{}) error {
	// Not used anywhere: Need not maintain
	msg := fmt.Sprintf(format, v...)
	values := url.Values{"message": {msg}, "logLevel": {"info"}, "component": {"indexing"}}

	if !strings.HasPrefix(clusterAddr, "http://") {
		clusterAddr = "http://" + clusterAddr
	}
	clusterAddr += "/_log"

	_, err := PostForm(clusterAddr, values)

	return err
}

func StopDebugger(nodeAddr, appLocation string) {
	var endpointURL string
	var netClient *Client

	netClient = CheckTLSandGetClient(HTTPRequestTimeout)
	endpointURL = CheckTLSandReplaceProtocol("https://%s/stopDebugger/?name=%s", nodeAddr, appLocation)

	res, err := netClient.Get(endpointURL)
	if err != nil {
		logging.Errorf("UTIL Failed to capture v8 debugger url from url: %rs, err: %v", endpointURL, err)
		return
	}

	defer res.Body.Close()
	return
}

func GetNodeUUIDs(urlSuffix string, nodeAddrs []string) (map[string]string, error) {
	logPrefix := "util::GetNodeUUIDs"
	var netClient *Client

	addrUUIDMap := make(map[string]string)
	netClient = CheckTLSandGetClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		var endpointURL string

		endpointURL = CheckTLSandReplaceProtocol("http://%s%s", nodeAddr, urlSuffix)
		res, err := netClient.Get(endpointURL)
		if err != nil {
			logging.Errorf("%s Failed to fetch node uuid from url: %rs, err: %v", logPrefix, endpointURL, err)
			return nil, err
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("%s Failed to read response body from url: %rs, err: %v", logPrefix, endpointURL, err)
			return nil, err
		}

		addrUUIDMap[string(buf)] = nodeAddr
	}
	return addrUUIDMap, nil
}

func GetEventProcessingStats(urlSuffix string, nodeAddrs []string) (map[string]int64, error) {
	logPrefix := "util::GetEventProcessingStats"

	pStats := make(map[string]int64)
	var netClient *Client
	netClient = CheckTLSandGetClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		var endpointURL string

		endpointURL = CheckTLSandReplaceProtocol("http://%s%s", nodeAddr, urlSuffix)
		res, err := netClient.Get(endpointURL)
		if err != nil {
			logging.Errorf("%s Failed to gather event processing stats from url: %rs, err: %v", logPrefix, endpointURL, err)
			return nil, err
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("%s Failed to read response body for event processing stats from url: %rs, err: %v", logPrefix, endpointURL, err)
			return nil, err
		}

		var nodePStats map[string]int64
		err = json.Unmarshal(buf, &nodePStats)
		if err != nil {
			logging.Errorf("%s Failed to unmarshal event processing stats from url: %rs, err: %v", logPrefix, endpointURL, err)
			return nil, err
		}

		for k, v := range nodePStats {
			if _, ok := pStats[k]; !ok {
				pStats[k] = 0
			}
			pStats[k] += v
		}
	}

	return pStats, nil
}

func ResetStatsCounters(urlSuffix string, nodeAddrs []string) ([]byte, error) {
	logPrefix := "util::ResetStatsCounters"
	var netClient *Client
	netClient = CheckTLSandGetClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		var endpointURL string
		endpointURL = CheckTLSandReplaceProtocol("http://%s%s", nodeAddr, urlSuffix)
		res, err := netClient.Get(endpointURL)
		if err != nil {
			logging.Errorf("%s Failed to reset stats counters on node: %rs, err: %v", logPrefix, endpointURL, err)
			return nil, err
		}

		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			buf, err := ioutil.ReadAll(res.Body)
			if err != nil {
				return nil, err
			}
			return buf, fmt.Errorf("%v", res.StatusCode)
		}
	}
	return nil, nil
}

func GetProgress(urlSuffix string, nodeAddrs []string) (*cm.RebalanceProgress, map[string]interface{}, map[string]error) {
	logPrefix := "util::GetProgress"

	aggProgress := &cm.RebalanceProgress{}
	var netClient *Client

	netClient = CheckTLSandGetClient(HTTPRequestTimeout)

	errMap := make(map[string]error)

	progressMap := make(map[string]interface{})

	for _, nodeAddr := range nodeAddrs {
		var endpointURL string
		endpointURL = CheckTLSandReplaceProtocol("http://%s%s", nodeAddr, urlSuffix)
		res, err := netClient.Get(endpointURL)
		if err != nil {
			logging.Errorf("%s Failed to gather task status from url: %rs, err: %v", logPrefix, endpointURL, err)
			errMap[nodeAddr] = err
			continue
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("%s Failed to read response body from url: %rs, err: %v", logPrefix, endpointURL, err)
			errMap[nodeAddr] = err
			continue
		}

		var progress cm.RebalanceProgress
		err = json.Unmarshal(buf, &progress)
		if err != nil {
			logging.Warnf("%s Failed to unmarshal progress from url: %rs, err: %v", logPrefix, endpointURL, err)
			errMap[nodeAddr] = err
			continue
		}

		logging.Infof("%s endpointURL: %rs VbsRemainingToShuffle: %d VbsOwnedPerPlan: %d",
			logPrefix, endpointURL, progress.VbsRemainingToShuffle, progress.VbsOwnedPerPlan)

		rebProgress := make(map[string]interface{})
		rebProgress["close_stream_vbs_len"] = progress.CloseStreamVbsLen
		rebProgress["stream_req_vbs_len"] = progress.StreamReqVbsLen
		rebProgress["vbs_owned_per_plan"] = progress.VbsOwnedPerPlan
		rebProgress["vbs_remaining_to_shuffle"] = progress.VbsRemainingToShuffle

		progressMap[nodeAddr] = rebProgress

		aggProgress.VbsRemainingToShuffle += progress.VbsRemainingToShuffle
		aggProgress.VbsOwnedPerPlan += progress.VbsOwnedPerPlan

		if urlSuffix == "/getAggRebalanceProgress" {
			aggProgress.NodeLevelStats = progress.NodeLevelStats
		}
	}

	return aggProgress, progressMap, errMap
}

func GetAppStatus(urlSuffix string, nodeAddrs []string) (map[string]map[string]string, error) {
	logPrefix := "util::GetAppStatus"

	appStatuses := make(map[string]map[string]string)

	var netClient *Client
	netClient = CheckTLSandGetClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		var endpointURL string
		endpointURL = CheckTLSandReplaceProtocol("http://%s%s", nodeAddr, urlSuffix)

		res, err := netClient.Get(endpointURL)
		if err != nil {
			logging.Errorf("%s Failed to get app statuses from url: %rs, err: %v", logPrefix, endpointURL, err)
			return nil, err
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("%s Failed to read response body from url: %rs, err: %v", logPrefix, endpointURL, err)
			return nil, err
		}

		var appStatus map[string]string
		err = json.Unmarshal(buf, &appStatus)
		if err != nil {
			logging.Errorf("%s Failed to unmarshal apps statuses from url: %rs, err: %v", logPrefix, endpointURL, err)
			return nil, err
		}

		appStatuses[nodeAddr] = make(map[string]string)
		appStatuses[nodeAddr] = appStatus
	}

	return appStatuses, nil
}

func ListChildren(path string) ([]string, error) {
	logPrefix := "util::ListChildren"

	entries, err := metakv.ListAllChildren(path)
	if err != nil {
		logging.Errorf("%s Failed to fetch deployed app list from metakv, err: %v", logPrefix, err)
		return nil, err
	}

	var children []string
	hmap := make(map[string]bool)
	for _, entry := range entries {
		splitRes := strings.Split(entry.Path, "/")
		appPath := splitRes[:len(splitRes)-1]
		child, err := getAppNameFromSplitPath(appPath)
		if err != nil {
			logging.Errorf("%s Failed to fetch deployed app list from metakv, err: %v", logPrefix, err)
			return nil, err
		}
		if _, seen := hmap[child]; seen == false {
			hmap[child] = true
			children = append(children, child)
		}
	}
	return children, nil
}

func MetakvGet(path string) ([]byte, error) {
	data, _, err := metakv.Get(path)
	if err != nil {
		return nil, err
	}
	return data, err
}

var metakvSetCallback = func(args ...interface{}) error {
	logPrefix := "Util::metakvSetCallback"

	metakvPath := args[0].(string)
	data := args[1].([]byte)
	rev := args[2]

	err := metakv.Set(metakvPath, data, rev)
	if err != nil {
		logging.Errorf("%s metakv set failed for path: %s, err: %v", logPrefix, metakvPath, err)
	}
	return err
}

func MetakvSet(path string, value []byte, rev interface{}) error {
	return Retry(NewFixedBackoff(time.Second), &cm.MetakvMaxRetries, metakvSetCallback, path, value, rev)
}

var metakvSetSensitiveCallback = func(args ...interface{}) error {
	logPrefix := "Util::metakvSetSensitiveCallback"

	metakvPath := args[0].(string)
	data := args[1].([]byte)
	rev := args[2]

	err := metakv.SetSensitive(metakvPath, data, rev)
	if err != nil {
		logging.Errorf("%s metakv set sensitive failed for path: %s, err: %v", logPrefix, metakvPath, err)
	}
	return err
}

func MetakvSetSensitive(path string, value []byte, rev interface{}) error {
	return Retry(NewFixedBackoff(time.Second), &cm.MetakvMaxRetries, metakvSetSensitiveCallback, path, value, rev)
}

var metakvDelCallback = func(args ...interface{}) error {
	logPrefix := "Util::metakvDelCallback"

	metakvPath := args[0].(string)
	rev := args[1]

	err := metakv.Delete(metakvPath, rev)
	if err != nil {
		logging.Errorf("%s metakv delete failed for path: %s, err: %v", logPrefix, metakvPath, err)
	}
	return err
}

func MetaKvDelete(path string, rev interface{}) error {
	return Retry(NewFixedBackoff(time.Second), &cm.MetakvMaxRetries, metakvDelCallback, path, rev)
}

var metakvRecDelCallback = func(args ...interface{}) error {
	logPrefix := "Util::metakvRecDelCallback"

	metakvPath := args[0].(string)

	err := metakv.RecursiveDelete(metakvPath)
	if err != nil {
		logging.Errorf("%s metakv recursive delete failed for path: %s, err: %v", logPrefix, metakvPath, err)
	}
	return err
}

func MetakvRecursiveDelete(dirpath string) error {
	return Retry(NewFixedBackoff(time.Second), &cm.MetakvMaxRetries, metakvRecDelCallback, dirpath)
}

// WriteAppContent fragments the payload and store it to metakv
func WriteAppContent(appsPath, checksumPath, appLocation string, payload []byte, compressPayload bool) error {
	logPrefix := "util::WriteAppContent"

	payload2, err := StripCurlCredentials(appsPath, appLocation, payload)
	if err != nil {
		return err
	}

	payload3, err := MaybeCompress(payload2, compressPayload)
	if err != nil {
		return err
	}

	appsPath += appLocation
	appsPath += "/"
	length := len(payload3)

	checksumPath += appLocation
	fragmentCount := length / MetaKvMaxDocSize()
	if length%MetaKvMaxDocSize() != 0 {
		fragmentCount++
	}

	logging.Infof("%s Function: %s number of fragments: %d payload size: %d app path: %s checksum path: %s",
		logPrefix, appLocation, fragmentCount, length, appsPath, checksumPath)

	for idx := 0; idx < fragmentCount; idx++ {
		currpath := appsPath + strconv.Itoa(int(idx))
		curridx := idx * MetaKvMaxDocSize()
		lastidx := (idx + 1) * MetaKvMaxDocSize()
		if lastidx > length {
			lastidx = length
		}

		fragment := payload3[curridx:lastidx]

		err := MetakvSet(currpath, fragment, nil)
		if err != nil {
			//Delete existing entry from appspath
			logging.Errorf("%s Function: %s metakv set failed for fragments, fragment number: %d, err: %v", logPrefix, appLocation, idx, err)
			if errd := MetakvRecursiveDelete(appsPath); errd != nil {
				logging.Errorf("%s Function: %s metakv recursive delete failed, fragment number: %d, err: %v", logPrefix, appLocation, idx, errd)
				return errd
			}
			return err
		}
	}

	//Compute MD5 hash and update it in metakv
	payloadhash := PayloadHash{}
	if err := payloadhash.Update(payload3, MetaKvMaxDocSize()); err != nil {
		logging.Errorf("%s Function: %s updating payload hash failed err: %v", logPrefix, appLocation, err)
		//Delete existing entry from appspath
		if errd := MetakvRecursiveDelete(appsPath); errd != nil {
			logging.Errorf("%s Function: %s payload hash metakv recursive delete failed, err: %v", logPrefix, appLocation, errd)
			return errd
		}
		return err
	}

	//Marshal payload hash and update it in metakv
	hashdata, err := json.Marshal(&payloadhash)
	if err != nil {
		//Delete existing entry from apps path
		logging.Errorf("%s Function: %s marshal failed, err: %v", logPrefix, appLocation, err)
		if errd := MetakvRecursiveDelete(appsPath); errd != nil {
			logging.Errorf("%s Function: %s unmarshal failed, err: %v", logPrefix, appLocation, errd)
			return errd
		}
		return err
	}

	if err = MetakvSet(checksumPath, hashdata, nil); err != nil {
		//Delete existing entry from apps path
		logging.Errorf("%s Function: %s metakv set failed for checksum, err: %v", logPrefix, appLocation, err)
		if errd := MetakvRecursiveDelete(appsPath); errd != nil {
			logging.Errorf("%s Function: %s checksum metakv recursive delete, err: %v", logPrefix, appLocation, errd)
			return errd
		}
		return err
	}

	return nil
}

// ReadAppContent reads function code
func ReadAppContent(appsPath, checksumPath, appLocation string) ([]byte, error) {
	logPrefix := "util::ReadAppContent"

	checksumPath += appLocation
	var payloadhash PayloadHash
	if hashdata, err := MetakvGet(checksumPath); err != nil {
		logging.Errorf("%s Function: %s metakv get failed for checksum, err: %v", logPrefix, appLocation, err)
		return nil, err
	} else {
		if len(hashdata) == 0 {
			logging.Errorf("%s Function: %s app content doesn't exist or is empty", logPrefix, appLocation)
			return nil, AppNotExist
		}

		if err := json.Unmarshal(hashdata, &payloadhash); err != nil {
			logging.Errorf("%s Function: %s unmarshal failed for checksum", logPrefix, appLocation)
			return nil, err
		}
	}

	//Read fragment data
	var payload []byte
	appsPath += appLocation
	for idx := 0; idx < payloadhash.Fragmentcnt; idx++ {
		path := appsPath + "/" + strconv.Itoa(int(idx))
		data, err := MetakvGet(path)
		if err != nil {
			logging.Errorf("%s Function: %s metakv get failed for fragments, fragment number: %d fragment count: %d, err: %v",
				logPrefix, appLocation, idx, payloadhash.Fragmentcnt, err)
			return nil, err
		}

		if data == nil {
			logging.Errorf("%s Function: %s metakv get data is empty, fragment number: %d fragment count: %d",
				logPrefix, appLocation, idx, payloadhash.Fragmentcnt)
			return nil, errors.New("Reading stale data")
		}

		if fragmenthash, err := ComputeMD5(data); err != nil {
			logging.Errorf("%s Function: %s metakv get MD5 computation failed, fragment number: %d fragment count: %d, err: %v",
				logPrefix, appLocation, idx, payloadhash.Fragmentcnt, err)
			return nil, err
		} else {
			if bytes.Equal(fragmenthash, payloadhash.Fragmenthash[idx]) != true {
				logging.Errorf("%s Function: %s metakv get checksum mismatch, fragment number: %d fragment count: %d",
					logPrefix, appLocation, idx, payloadhash.Fragmentcnt)
				return nil, errors.New("checksum mismatch for payload fragments")
			}
			payload = append(payload, data...)
		}
	}

	payload2, err := MaybeDecompress(payload)
	if err != nil {
		return nil, err
	}

	payload3, mErr := AppendCredentials(appsPath, appLocation, payload2)
	if mErr != nil {
		return nil, mErr
	}

	payload4, lErr := AppendLangCompat(appsPath, appLocation, payload3)
	if lErr != nil {
		return nil, lErr
	}
	return payload4, nil
}

// DeleteAppContent delete handler code
func DeleteAppContent(appPath, checksumPath, appLocation string) error {
	//Delete Checksum path
	logPrefix := "util::DeleteAppContent"
	checksumPath += appLocation
	if err := MetaKvDelete(checksumPath, nil); err != nil {
		logging.Errorf("%s Function: %s metakv delete failed for checksum, err: %v", logPrefix, appLocation, err)
		return err
	}

	//Delete Apps Path
	appPath += appLocation
	appPath += "/"
	if err := MetakvRecursiveDelete(appPath); err != nil {
		logging.Errorf("%s Function: %s metakv recursive delete failed, err: %v", logPrefix, appLocation, err)
		return err
	}

	//Delete Credentials path
	if err := MetaKvDelete(cm.MetakvCredentialsPath+appLocation, nil); err != nil {
		logging.Infof("%s Function: %s failed to delete credentials, err: %v", logPrefix, appLocation, err)
		return err
	}

	return nil
}

// Delete stale app fragments
func DeleteStaleAppContent(appPath, appLocation string) error {
	//Delete Apps Path
	logPrefix := "util::DeleteStaleAppContent"
	appPath += appLocation
	appPath += "/"
	if err := MetakvRecursiveDelete(appPath); err != nil {
		logging.Errorf("%s Function: %s metakv recursive delete failed, err: %v", logPrefix, appLocation, err)
		return err
	}
	return nil
}

func GetHash(appCode string) string {
	hash := md5.New()
	hash.Write([]byte(appCode))
	return fmt.Sprintf("%d-%x", len(appCode), hash.Sum(nil))
}

func MemcachedErrCode(err error) gomemcached.Status {
	status := gomemcached.Status(0xffff)
	if res, ok := err.(*gomemcached.MCResponse); ok {
		status = res.Status
	}
	return status
}

func CompareSlices(s1, s2 []uint16) bool {

	if s1 == nil && s2 == nil {
		return true
	}

	if s1 == nil || s2 == nil {
		return false
	}

	if len(s1) != len(s2) {
		return false
	}

	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}

	return true
}

func CompareStringSlices(s1, s2 []string) bool {

	if s1 == nil && s2 == nil {
		return true
	}

	if s1 == nil || s2 == nil {
		return false
	}

	if len(s1) != len(s2) {
		return false
	}

	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}

	return true
}

func VbsSliceDiff(X, Y []uint16) []uint16 {
	var diff []uint16

	m := make(map[uint16]int)

	for _, y := range Y {
		m[y]++
	}

	for _, x := range X {
		if m[x] > 0 {
			m[x]--
			continue
		}
		diff = append(diff, x)
	}

	n := make(map[uint16]int)

	for _, x := range X {
		n[x]++
	}

	for _, y := range Y {
		if n[y] > 0 {
			n[y]--
			continue
		}
		diff = append(diff, y)
	}

	return diff
}

func Uint16SliceDiff(kv1, kv2 []uint16) []uint16 {
	var diff []uint16

	for _, s1 := range kv1 {
		found := false
		for _, s2 := range kv2 {
			if s1 == s2 {
				found = true
				break
			}
		}

		if !found {
			diff = append(diff, s1)
		}
	}

	return diff
}

func StrSliceDiff(kv1, kv2 []string) []string {
	var diff []string

	for _, s1 := range kv1 {
		found := false
		for _, s2 := range kv2 {
			if s1 == s2 {
				found = true
				break
			}
		}

		if !found {
			diff = append(diff, s1)
		}
	}

	return diff
}

func ConvertBigEndianToUint64(cas []byte) (uint64, error) {
	if len(cas) == 0 {
		return 0, fmt.Errorf("empty cas value")
	}

	// Trim "Ox"
	cas = cas[2:]

	for i := 0; i < len(cas)/2; i += 2 {
		cas[i], cas[len(cas)-i-2] = cas[len(cas)-i-2], cas[i]
		cas[i+1], cas[len(cas)-i-1] = cas[len(cas)-i-1], cas[i+1]
	}

	return strconv.ParseUint(string(cas), 16, 64)
}

func GetLogLevel(logLevel string) logging.LogLevel {
	switch logLevel {
	case "ERROR":
		return logging.Error
	case "INFO":
		return logging.Info
	case "WARNING":
		return logging.Warn
	case "DEBUG":
		return logging.Debug
	case "TRACE":
		return logging.Trace
	default:
		return logging.Info
	}
}

// VbucketNodeAssignment will be used as generic partitioning scheme for vbucket assignment to
// Eventing.Consumer and Eventing.Producer instances
func VbucketNodeAssignment(vbs []uint16, numWorkers int) map[int][]uint16 {
	vbucketsPerWorker := len(vbs) / numWorkers

	var vbNo int

	vbWorkerAssignMap := make(map[int][]uint16)
	if len(vbs) == 0 {
		for i := 0; i < numWorkers; i++ {
			assignedVbs := make([]uint16, 0)
			vbWorkerAssignMap[i] = assignedVbs
		}

		return vbWorkerAssignMap
	}

	vbCountPerWorker := make([]int, numWorkers)
	for i := 0; i < numWorkers; i++ {
		vbCountPerWorker[i] = vbucketsPerWorker
		vbNo += vbucketsPerWorker
	}

	remainingVbs := len(vbs) - vbNo
	if remainingVbs > 0 {
		for i := 0; i < remainingVbs; i++ {
			vbCountPerWorker[i] = vbCountPerWorker[i] + 1
		}
	}

	startVb := vbs[0]
	for i, v := range vbCountPerWorker {
		assignedVbs := make([]uint16, 0)
		for j := 0; j < v; j++ {
			assignedVbs = append(assignedVbs, startVb)
			startVb++
		}
		vbWorkerAssignMap[i] = assignedVbs
	}

	return vbWorkerAssignMap
}

// VbucketDistribution is used by vbucket ownership give up and takeover routines during rebalance
func VbucketDistribution(vbs []uint16, numWorkers int) map[int][]uint16 {
	vbWorkerAssignMap := make(map[int][]uint16)
	for i := 0; i < numWorkers; i++ {
		assignedVbs := make([]uint16, 0)
		vbWorkerAssignMap[i] = assignedVbs
	}

	if len(vbs) == 0 || numWorkers == 0 {
		return vbWorkerAssignMap
	}

	for i := 0; i < len(vbs); {
		for j := 0; j < numWorkers; j++ {
			if i < len(vbs) {
				vbWorkerAssignMap[j] = append(vbWorkerAssignMap[j], vbs[i])
				i++
			} else {
				return vbWorkerAssignMap
			}
		}
	}

	return vbWorkerAssignMap
}

func Condense(vbs []uint16) string {
	if len(vbs) == 0 {
		return "[]"
	}

	startVb := vbs[0]
	res := fmt.Sprintf("[%d", startVb)
	prevVb := startVb

	for i := 1; i < len(vbs); {
		if vbs[i] == startVb+1 {
			startVb++
		} else {

			if prevVb != startVb {
				res = fmt.Sprintf("%s-%d, %d", res, startVb, vbs[i])
			} else {
				res = fmt.Sprintf("%s, %d", res, vbs[i])
			}
			startVb = vbs[i]
			prevVb = startVb
		}

		if i == len(vbs)-1 {
			if prevVb == vbs[i] {
				res = fmt.Sprintf("%s]", res)
				return res
			}

			res = fmt.Sprintf("%s-%d]", res, vbs[i])
			return res
		}

		i++
	}

	return res
}

// VbucketByKey returns doc_id to vbucket mapping
func VbucketByKey(key []byte, numVbuckets int) uint16 {
	return uint16((crc32.ChecksumIEEE(key) >> 16) % uint32(numVbuckets))
}

func StripScheme(endpoint string) string {
	return strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
}

func (dynAuth *DynamicAuthenticator) Credentials(req gocb.AuthCredsRequest) ([]gocb.UserPassPair, error) {
	logPrefix := "DynamicAuthenticator::Credentials"

	GocbCredsRequestCounter++
	strippedEndpoint := StripScheme(req.Endpoint)
	username, password, err := cbauth.GetMemcachedServiceAuth(strippedEndpoint)
	if err != nil {
		logging.Errorf("%s invoked by %s, failed to get auth from cbauth, err: %v", logPrefix, dynAuth.Caller, err)
		return []gocb.UserPassPair{{}}, err
	}

	return []gocb.UserPassPair{{
		Username: username,
		Password: password,
	}}, nil
}

func CheckIfRebalanceOngoing(urlSuffix string, nodeAddrs []string) (bool, error) {
	logPrefix := "util::CheckIfRebalanceOngoing"

	var netClient *Client

	netClient = CheckTLSandGetClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		var endpointURL string
		endpointURL = CheckTLSandReplaceProtocol("http://%s%s", nodeAddr, urlSuffix)

		res, err := netClient.Get(endpointURL)
		if err != nil {
			logging.Errorf("%s Failed to gather rebalance status from url: %rs, err: %v", logPrefix, endpointURL, err)
			return true, err
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("%s Failed to read response body from url: %rs, err: %v", logPrefix, endpointURL, err)
			return true, err
		}

		status, err := strconv.ParseBool(string(buf))
		if err != nil {
			logging.Errorf("%s Failed to interpret rebalance status from url: %rs, err: %v", logPrefix, endpointURL, err)
			return true, err
		}

		logging.Infof("%s Rebalance status from url: %rs status: %v", logPrefix, endpointURL, status)

		if status {
			return true, nil
		}

	}

	return false, nil
}

func CheckIfBootstrapOngoing(urlSuffix string, nodeAddrs []string) (bool, error) {
	logPrefix := "util::CheckIfBootstrapOngoing"

	var netClient *Client
	netClient = CheckTLSandGetClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		var endpointURL string
		endpointURL = CheckTLSandReplaceProtocol("http://%s%s", nodeAddr, urlSuffix)

		res, err := netClient.Get(endpointURL)
		if err != nil {
			logging.Errorf("%s Failed to gather bootstrap status from url: %rs, err: %v", logPrefix, endpointURL, err)
			return true, err
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("%s Failed to read response body from url: %rs, err: %v", logPrefix, endpointURL, err)
			return true, err
		}

		status, err := strconv.ParseBool(string(buf))
		if err != nil {
			logging.Errorf("%s Failed to interpret bootstrap status from url: %rs, err: %v", logPrefix, endpointURL, err)
			return true, err
		}

		logging.Infof("%s Bootstrap status from url: %rs status: %v", logPrefix, endpointURL, status)

		if status {
			return true, nil
		}
	}

	return false, nil
}

func CheckIfAppBootstrapOngoing(urlSuffix string, nodeAddrs []string, query string) (bool, error) {
	logPrefix := "util::CheckIfAppBootstrapOngoing"

	var netClient *Client
	netClient = CheckTLSandGetClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		var endpointURL string
		endpointURL = CheckTLSandReplaceProtocol("http://%s%s?%s", nodeAddr, urlSuffix, query)

		res, err := netClient.Get(endpointURL)
		if err != nil {
			logging.Errorf("%s Failed to gather bootstrap status from url: %rs, err: %v", logPrefix, endpointURL, err)
			return true, err
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("%s Failed to read response body from url: %rs, err: %v", logPrefix, endpointURL, err)
			return true, err
		}

		status, err := strconv.ParseBool(string(buf))
		if err != nil {
			logging.Errorf("%s Failed to interpret bootstrap status from url: %rs, err: %v", logPrefix, endpointURL, err)
			return true, err
		}

		if status {
			logging.Infof("%s Bootstrap status form url: %rs status: %v", logPrefix, endpointURL, status)
			return true, nil
		}
	}

	return false, nil
}

func GetAggPausingApps(urlSuffix string, nodeAddrs []string) (bool, error) {
	logPrefix := "util::GetAggPausingApps"

	var netClient *Client
	netClient = CheckTLSandGetClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		var endpointURL string
		endpointURL = CheckTLSandReplaceProtocol("http://%s%s", nodeAddr, urlSuffix)

		res, err := netClient.Get(endpointURL)
		if err != nil {
			logging.Errorf("%s Failed to gather pausing app list from url: %rs, err: %v", logPrefix, endpointURL, err)
			return false, err
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("%s Failed to read response body from url: %rs, err: %v", logPrefix, endpointURL, err)
			return false, err
		}

		pausingApps := make(map[string]string)
		err = json.Unmarshal(buf, &pausingApps)
		if err != nil {
			logging.Errorf("%s Failed to marshal pausing app list from url: %rs, err: %v", logPrefix, endpointURL, err)
			return false, err
		}

		if len(pausingApps) > 0 {
			return true, fmt.Errorf("Some apps are being paused, node: %s", nodeAddr)
		}
	}

	return false, nil
}

func GetAggBootstrappingApps(urlSuffix string, nodeAddrs []string) (bool, error) {
	logPrefix := "util::GetAggBootstrappingApps"

	var netClient *Client
	netClient = CheckTLSandGetClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		var endpointURL string
		endpointURL = CheckTLSandReplaceProtocol("http://%s%s", nodeAddr, urlSuffix)

		res, err := netClient.Get(endpointURL)
		if err != nil {
			logging.Errorf("%s Failed to gather bootstrapping app list from url: %rs, err: %v", logPrefix, endpointURL, err)
			return false, err
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("%s Failed to read response body from url: %rs, err: %v", logPrefix, endpointURL, err)
			return false, err
		}

		bootstrappingApps := make(map[string]string)
		err = json.Unmarshal(buf, &bootstrappingApps)
		if err != nil {
			logging.Errorf("%s Failed to marshal bootstrapping app list from url: %rs, err: %v", logPrefix, endpointURL, err)
			return false, err
		}

		if len(bootstrappingApps) > 0 {
			return true, fmt.Errorf("Some apps are deploying or resuming, node: %s", nodeAddr)
		}
	}

	return false, nil
}

func GetAggBootstrapStatus(nodeAddr string) (bool, error) {
	logPrefix := "util::GetAggBootstrapStatus"

	var netClient *Client
	var url string

	url = CheckTLSandReplaceProtocol("https://%s/getAggBootstrapStatus", nodeAddr)
	netClient = CheckTLSandGetClient(HTTPRequestTimeout)

	res, err := netClient.Get(url)
	if err != nil {
		logging.Errorf("%s Failed to gather bootstrap status from url: %rs, err: %v", logPrefix, url, err)
		return false, err
	}
	defer res.Body.Close()

	buf, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logging.Errorf("%s Failed to read response body from url: %rs, err: %v", logPrefix, url, err)
		return false, err
	}

	status, err := strconv.ParseBool(string(buf))
	if err != nil {
		logging.Errorf("%s Failed to interpret bootstrap status from url: %rs, err: %v", logPrefix, url, err)
		return false, err
	}

	return status, nil
}

func GetAggBootstrapAppStatus(nodeAddr string, query string, isLocalhost bool) (bool, error) {
	logPrefix := "util::GetAggBootstrapAppStatus"

	var netClient *Client
	var url string

	url = CheckTLSandReplaceProtocol("https://%s/getAggBootstrapAppStatus?%s", nodeAddr, query)
	netClient = CheckTLSandGetClient(HTTPRequestTimeout)

	res, err := netClient.Get(url)
	if err != nil {
		logging.Errorf("%s Failed to gather bootstrap status from url: %rs, err: %v", logPrefix, url, err)
		return false, err
	}
	defer res.Body.Close()

	buf, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logging.Errorf("%s Failed to read response body from url: %rs, err: %v", logPrefix, url, err)
		return false, err
	}

	status, err := strconv.ParseBool(string(buf))
	if err != nil {
		logging.Errorf("%s Failed to interpret bootstrap status from url: %rs, err: %v", logPrefix, url, err)
		return false, err
	}

	return status, nil
}

func GetEventingVersion(urlSuffix string, nodeAddrs []string) ([]string, error) {
	logPrefix := "util::GetEventingVersion"

	var netClient *Client
	netClient = CheckTLSandGetClient(HTTPRequestTimeout)

	versions := make([]string, 0)

	for _, nodeAddr := range nodeAddrs {
		var endpointURL string
		endpointURL = CheckTLSandReplaceProtocol("http://%s%s", nodeAddr, urlSuffix)

		res, err := netClient.Get(endpointURL)
		if err != nil {
			logging.Errorf("%s Failed to gather eventing version from url: %rs, err: %v", logPrefix, endpointURL, err)
			return versions, err
		}
		defer res.Body.Close()

		version, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("%s Failed to read response body from url: %rs, err: %v", logPrefix, endpointURL, err)
			return versions, err
		}

		versions = append(versions, string(version))
	}

	return versions, nil
}

func Contains(needle interface{}, haystack interface{}) bool {
	s := reflect.ValueOf(haystack)

	if s.Kind() != reflect.Slice {
		panic("non-slice type provided")
	}

	for i := 0; i < s.Len(); i++ {
		if s.Index(i).Interface() == needle {
			return true
		}
	}

	return false
}

func ContainsIgnoreCase(needle string, haystack []string) bool {
	for _, item := range haystack {
		if strings.EqualFold(item, needle) {
			return true
		}
	}

	return false
}

func ToStr(value bool) (strValue string) {
	if value {
		strValue = "1"
	} else {
		strValue = "0"
	}

	return
}

func ToStringArray(from interface{}) (to []string) {
	if from == nil {
		return
	}

	fromArray := from.([]interface{})
	to = make([]string, len(fromArray))
	for i := 0; i < len(fromArray); i++ {
		to[i] = fromArray[i].(string)
	}

	return
}

func FloatEquals(a, b float64) bool {
	return math.Abs(a-b) <= EPSILON
}

func DeepCopy(kv map[string]interface{}) (newKv map[string]interface{}) {
	newKv = make(map[string]interface{})
	for k, v := range kv {
		newKv[k] = v
	}

	return
}

func GetAppNameFromPath(path string) (string, error) {
	split := strings.Split(path, "/")
	return getAppNameFromSplitPath(split)
}

func getAppNameFromSplitPath(splitPath []string) (string, error) {
	switch len(splitPath) {
	case 4:
		return splitPath[3], nil
	case 6:
		return fmt.Sprintf("%s/%s/%s", splitPath[3], splitPath[4], splitPath[5]), nil
	}
	return "", fmt.Errorf("Invalid path: %s", splitPath)
}

func GenerateFunctionID() (uint32, error) {
	uuid := make([]byte, 16)
	_, err := rand.Read(uuid)
	if err != nil {
		return 0, err
	}
	return crc32.ChecksumIEEE(uuid), nil
}

func GenerateFunctionInstanceID() (string, error) {
	return randomID(dict)
}

func GenerateRandomNameSuffix() string {
	id, err := randomID(nameDict)
	if err != nil {
		return "000000"
	}

	return id
}

func randomID(dict string) (string, error) {
	uuid := make([]byte, 16)
	_, err := rand.Read(uuid)
	if err != nil {
		return "", err
	}
	genId := crc32.ChecksumIEEE(uuid)
	genIdStr := make([]byte, 0, 8)
	if genId == 0 {
		return "0", nil
	}
	for genId > 0 {
		ch := dict[genId%64]
		genIdStr = append(genIdStr, byte(ch))
		genId /= 64
	}
	return string(genIdStr), nil
}

type GocbLogger struct{}

func (r *GocbLogger) Log(level gocb.LogLevel, offset int, format string, v ...interface{}) error {
	format = "[gocb] " + format
	switch level {
	case gocb.LogError:
		logging.Errorf(format, v...)
	case gocb.LogWarn:
		logging.Warnf(format, v...)
	case gocb.LogInfo:
		logging.Infof(format, v...)
	case gocb.LogDebug:
		logging.Debugf(format, v...)
	case gocb.LogTrace:
		logging.Tracef(format, v...)
	case gocb.LogSched:
		logging.Tracef(format, v...)
	case gocb.LogMaxVerbosity:
		logging.Tracef(format, v...)
	default:
		logging.Tracef(format, v...)
	}
	return nil
}

func KillProcess(pid int) error {
	if pid < 1 {
		return errors.New(fmt.Sprintf("Can not kill %d", pid))
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}

	return process.Kill()
}

func SuperImpose(source, on map[string]interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	for key := range on {
		m[key] = on[key]
	}

	for key := range source {
		m[key] = source[key]
	}

	return m
}

func CPUCount(log bool) int {
	logPrefix := "util::GetCPUCount"

	cpuCount := runtime.GOMAXPROCS(-1)
	if cpuCount == 0 {
		if log {
			logging.Errorf("%s CPU count reported as 0", logPrefix)
		}
		return 3
	}

	return cpuCount
}

func ParseXattrData(xattrPrefix string, data []byte) (body, xattr []byte, err error) {
	length := len(data)
	if length < 4 {
		return nil, nil, fmt.Errorf("empty xattr metadata")
	}
	xattrLen := binary.BigEndian.Uint32(data[0:4])
	body = data[xattrLen+4:]
	if xattrLen == 0 {
		return body, nil, nil
	}
	index := uint32(4)
	delimeter := []byte("\x00")
	for index < xattrLen {
		keyValPairLen := binary.BigEndian.Uint32(data[index : index+4])
		if keyValPairLen == 0 || int(index+keyValPairLen) > length {
			return body, nil, fmt.Errorf("xattr parse error, unexpected xattr data")
		}
		index += 4
		keyValPairData := data[index : index+keyValPairLen]
		keyValPair := bytes.Split(keyValPairData, delimeter)
		if len(keyValPair) != 3 {
			return body, nil, fmt.Errorf("xattr parse error, unexpected number of components")
		}
		xattrKey := string(keyValPair[0])
		if xattrKey == xattrPrefix {
			return body, keyValPair[1], nil
		}
		index += keyValPairLen
	}
	return body, nil, nil
}

func MaybeCompress(payload []byte, compressPayload bool) ([]byte, error) {
	if compressPayload {
		var buf bytes.Buffer
		compressor, err := flate.NewWriter(&buf, flate.BestCompression)
		if err != nil {
			logging.Errorf("%s error in selecting level for flate: err %v", err)
			return nil, err
		}
		if _, err = compressor.Write(payload); err != nil {
			logging.Errorf("%s error in compressing: err %v", err)
			compressor.Close()
			return nil, err
		}
		if err = compressor.Close(); err != nil {
			logging.Errorf("%s error in Flushing to Writer err: %v", err)
			return nil, err
		}

		payload2 := buf.Bytes()
		if len(payload2) < len(payload) {
			return append([]byte{0, 0}, payload2...), nil
		}
		return payload, nil
	} else {
		return payload, nil
	}
}

func MaybeDecompress(payload []byte) ([]byte, error) {
	if payload[0] == byte(0) {
		r := flate.NewReader(bytes.NewReader(payload[2:]))
		defer r.Close()
		payload2, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, err
		}
		return payload2, nil
	}
	return payload, nil
}

func EncodeAppPayload(app *cm.Application) []byte {
	builder := flatbuffers.NewBuilder(0)

	var constantBindings []flatbuffers.UOffsetT

	for i := 0; i < len(app.DeploymentConfig.Constants); i++ {
		ConstantValueEncoded := builder.CreateString(app.DeploymentConfig.Constants[i].Value)
		ConstantLiteralEncoded := builder.CreateString(app.DeploymentConfig.Constants[i].Literal)
		cfg.ConstantStart(builder)
		cfg.ConstantAddValue(builder, ConstantValueEncoded)
		cfg.ConstantAddLiteral(builder, ConstantLiteralEncoded)
		constantBindingEnd := cfg.ConstantEnd(builder)
		constantBindings = append(constantBindings, constantBindingEnd)
	}

	cfg.ConfigStartConstantsVector(builder, len(constantBindings))
	for i := 0; i < len(constantBindings); i++ {
		builder.PrependUOffsetT(constantBindings[i])
	}

	constantsBindingsVector := builder.EndVector(len(constantBindings))

	var curlBindings []flatbuffers.UOffsetT
	for i := 0; i < len(app.DeploymentConfig.Curl); i++ {
		authTypeEncoded := builder.CreateString(app.DeploymentConfig.Curl[i].AuthType)
		hostnameEncoded := builder.CreateString(app.DeploymentConfig.Curl[i].Hostname)
		valueEncoded := builder.CreateString(app.DeploymentConfig.Curl[i].Value)
		passwordEncoded := builder.CreateString(app.DeploymentConfig.Curl[i].Password)
		usernameEncoded := builder.CreateString(app.DeploymentConfig.Curl[i].Username)
		bearerKeyEncoded := builder.CreateString(app.DeploymentConfig.Curl[i].BearerKey)
		cookiesEncoded := byte(0x0)
		if app.DeploymentConfig.Curl[i].AllowCookies {
			cookiesEncoded = byte(0x1)
		}
		validateSSLCertificateEncoded := byte(0x0)
		if app.DeploymentConfig.Curl[i].ValidateSSLCertificate {
			validateSSLCertificateEncoded = byte(0x1)
		}

		cfg.CurlStart(builder)
		cfg.CurlAddAuthType(builder, authTypeEncoded)
		cfg.CurlAddHostname(builder, hostnameEncoded)
		cfg.CurlAddValue(builder, valueEncoded)
		cfg.CurlAddPassword(builder, passwordEncoded)
		cfg.CurlAddUsername(builder, usernameEncoded)
		cfg.CurlAddBearerKey(builder, bearerKeyEncoded)
		cfg.CurlAddAllowCookies(builder, cookiesEncoded)
		cfg.CurlAddValidateSSLCertificate(builder, validateSSLCertificateEncoded)
		curlBindingsEnd := cfg.CurlEnd(builder)

		curlBindings = append(curlBindings, curlBindingsEnd)
	}

	cfg.ConfigStartCurlVector(builder, len(curlBindings))
	for i := 0; i < len(curlBindings); i++ {
		builder.PrependUOffsetT(curlBindings[i])
	}
	curlBindingsVector := builder.EndVector(len(curlBindings))

	var bNames []flatbuffers.UOffsetT
	var bucketAccess []flatbuffers.UOffsetT
	for i := 0; i < len(app.DeploymentConfig.Buckets); i++ {
		alias := builder.CreateString(app.DeploymentConfig.Buckets[i].Alias)
		bName := builder.CreateString(app.DeploymentConfig.Buckets[i].BucketName)
		bAccess := builder.CreateString(app.DeploymentConfig.Buckets[i].Access)
		sName := builder.CreateString(app.DeploymentConfig.Buckets[i].ScopeName)
		cName := builder.CreateString(app.DeploymentConfig.Buckets[i].CollectionName)

		cfg.BucketStart(builder)
		cfg.BucketAddAlias(builder, alias)
		cfg.BucketAddBucketName(builder, bName)
		cfg.BucketAddScopeName(builder, sName)
		cfg.BucketAddCollectionName(builder, cName)
		csBucket := cfg.BucketEnd(builder)

		bNames = append(bNames, csBucket)
		bucketAccess = append(bucketAccess, bAccess)
	}

	cfg.ConfigStartAccessVector(builder, len(bucketAccess))
	for i := 0; i < len(bucketAccess); i++ {
		builder.PrependUOffsetT(bucketAccess[i])
	}
	access := builder.EndVector(len(bucketAccess))

	cfg.DepCfgStartBucketsVector(builder, len(bNames))
	for i := 0; i < len(bNames); i++ {
		builder.PrependUOffsetT(bNames[i])
	}
	buckets := builder.EndVector(len(bNames))

	metaBucket := builder.CreateString(app.DeploymentConfig.MetadataBucket)
	sourceBucket := builder.CreateString(app.DeploymentConfig.SourceBucket)
	metadataCollection := builder.CreateString(app.DeploymentConfig.MetadataCollection)
	metadataScope := builder.CreateString(app.DeploymentConfig.MetadataScope)
	sourceScope := builder.CreateString(app.DeploymentConfig.SourceScope)
	sourceCollection := builder.CreateString(app.DeploymentConfig.SourceCollection)

	cfg.DepCfgStart(builder)
	cfg.DepCfgAddBuckets(builder, buckets)
	cfg.DepCfgAddMetadataBucket(builder, metaBucket)
	cfg.DepCfgAddSourceBucket(builder, sourceBucket)
	cfg.DepCfgAddMetadataCollection(builder, metadataCollection)
	cfg.DepCfgAddSourceCollection(builder, sourceCollection)
	cfg.DepCfgAddSourceScope(builder, sourceScope)
	cfg.DepCfgAddMetadataScope(builder, metadataScope)

	depcfg := cfg.DepCfgEnd(builder)

	appCode := builder.CreateString(app.AppHandlers)
	aName := builder.CreateString(app.Name)
	fiid := builder.CreateString(app.FunctionInstanceID)
	version := builder.CreateString(app.EventingVersion)
	schema := byte(0x0)
	if app.EnforceSchema {
		schema = byte(0x1)
	}

	if app.Metainfo == nil {
		app.Metainfo = make(map[string]interface{})
		app.Metainfo["lifecycle_state"] = ""
	}

	bucketName := builder.CreateString(app.FunctionScope.BucketName)
	scopeName := builder.CreateString(app.FunctionScope.ScopeName)

	cfg.FunctionScopeStart(builder)
	cfg.FunctionScopeAddBucketName(builder, bucketName)
	cfg.FunctionScopeAddScopeName(builder, scopeName)
	funcScope := cfg.FunctionScopeEnd(builder)

	user, domain, uuid := "", "", ""
	if app.Owner != nil {
		uuid = app.Owner.UUID
		user, domain = app.Owner.User, app.Owner.Domain
	}

	flatbufUser := builder.CreateString(user)
	flatbufDomain := builder.CreateString(domain)
	flatbufUuid := builder.CreateString(uuid)

	cfg.OwnerStart(builder)
	cfg.OwnerAddUser(builder, flatbufUser)
	cfg.OwnerAddDomain(builder, flatbufDomain)
	cfg.OwnerAddUuid(builder, flatbufUuid)
	owner := cfg.OwnerEnd(builder)

	lifecycleState := builder.CreateString(app.Metainfo["lifecycle_state"].(string))

	cfg.ConfigStart(builder)
	cfg.ConfigAddAppCode(builder, appCode)
	cfg.ConfigAddAppName(builder, aName)
	cfg.ConfigAddDepCfg(builder, depcfg)
	cfg.ConfigAddVersion(builder, version)
	cfg.ConfigAddHandlerUUID(builder, app.FunctionID)
	cfg.ConfigAddCurl(builder, curlBindingsVector)
	cfg.ConfigAddConstants(builder, constantsBindingsVector)
	cfg.ConfigAddAccess(builder, access)
	cfg.ConfigAddFunctionInstanceID(builder, fiid)
	cfg.ConfigAddEnforceSchema(builder, schema)
	cfg.ConfigAddLifecycleState(builder, lifecycleState)
	cfg.ConfigAddFunctionScope(builder, funcScope)
	cfg.ConfigAddOwner(builder, owner)
	config := cfg.ConfigEnd(builder)

	builder.Finish(config)

	return builder.FinishedBytes()
}

func ParseFunctionPayload(data []byte, fnName string) cm.Application {

	config := cfg.GetRootAsConfig(data, 0)

	var app cm.Application
	app.AppHandlers = string(config.AppCode())
	app.Name = string(config.AppName())
	app.FunctionID = uint32(config.HandlerUUID())
	app.EventingVersion = string(config.Version())
	app.FunctionInstanceID = string(config.FunctionInstanceID())
	if config.EnforceSchema() == byte(0x1) {
		app.EnforceSchema = true
	} else {
		app.EnforceSchema = false
	}
	lifecycleState := string(config.LifecycleState())
	app.Metainfo = make(map[string]interface{})
	app.Metainfo["lifecycle_state"] = lifecycleState

	d := new(cfg.DepCfg)
	depcfg := new(cm.DepCfg)
	dcfg := config.DepCfg(d)

	depcfg.MetadataBucket = string(dcfg.MetadataBucket())
	depcfg.SourceBucket = string(dcfg.SourceBucket())
	depcfg.SourceScope = cm.CheckAndReturnDefaultForScopeOrCollection(string(dcfg.SourceScope()))
	depcfg.SourceCollection = cm.CheckAndReturnDefaultForScopeOrCollection(string(dcfg.SourceCollection()))
	depcfg.MetadataCollection = cm.CheckAndReturnDefaultForScopeOrCollection(string(dcfg.MetadataCollection()))
	depcfg.MetadataScope = cm.CheckAndReturnDefaultForScopeOrCollection(string(dcfg.MetadataScope()))

	var buckets []cm.Bucket
	b := new(cfg.Bucket)
	for i := 0; i < dcfg.BucketsLength(); i++ {

		if dcfg.Buckets(b, i) {
			newBucket := cm.Bucket{
				Alias:          string(b.Alias()),
				BucketName:     string(b.BucketName()),
				Access:         string(config.Access(i)),
				ScopeName:      cm.CheckAndReturnDefaultForScopeOrCollection(string(b.ScopeName())),
				CollectionName: cm.CheckAndReturnDefaultForScopeOrCollection(string(b.CollectionName())),
			}
			buckets = append(buckets, newBucket)
		}
	}

	var curl []cm.Curl
	c := new(cfg.Curl)
	for i := 0; i < config.CurlLength(); i++ {
		if config.Curl(c, i) {
			allowCookies := false
			if c.AllowCookies() == (0x1) {
				allowCookies = true
			}
			validateSSL := false
			if c.ValidateSSLCertificate() == (0x1) {
				validateSSL = true
			}
			newCurl := cm.Curl{
				Hostname:               string(c.Hostname()),
				Value:                  string(c.Value()),
				AuthType:               string(c.AuthType()),
				Username:               string(c.Username()),
				Password:               string(c.Password()),
				BearerKey:              string(c.BearerKey()),
				AllowCookies:           allowCookies,
				ValidateSSLCertificate: validateSSL,
			}
			curl = append(curl, newCurl)
		}
	}

	var constantBindings []cm.Constant
	binding := new(cfg.Constant)
	for i := 0; i < config.ConstantsLength(); i++ {
		if config.Constants(binding, i) {
			newBinding := cm.Constant{
				Value:   string(binding.Value()),
				Literal: string(binding.Literal()),
			}
			constantBindings = append(constantBindings, newBinding)
		}
	}

	depcfg.Buckets = buckets
	depcfg.Curl = curl
	depcfg.Constants = constantBindings
	app.DeploymentConfig = *depcfg

	f := new(cfg.FunctionScope)
	fS := config.FunctionScope(f)
	funcScope := common.FunctionScope{}

	if fS != nil {
		funcScope.BucketName = string(fS.BucketName())
		funcScope.ScopeName = string(fS.ScopeName())
	}

	o := new(cfg.Owner)
	ownerEncrypted := config.Owner(o)
	owner := &common.Owner{}

	if ownerEncrypted != nil {
		owner.UUID = string(ownerEncrypted.Uuid())
		owner.User = string(ownerEncrypted.User())
		owner.Domain = string(ownerEncrypted.Domain())
	}

	app.FunctionScope = funcScope
	app.Owner = owner

	return app
}

func StripCurlCredentials(path, appLocation string, payload []byte) ([]byte, error) {
	logPrefix := "Util::StripCurlCredentials"

	if path == cm.MetakvTempAppsPath {
		var app cm.Application
		if err := json.Unmarshal(payload, &app); err != nil {
			logging.Errorf("%s Function: %s unmarshal failed for data from metakv", logPrefix, appLocation)
			return nil, err
		}

		var creds []cm.Credential
		for i, binding := range app.DeploymentConfig.Curl {
			creds = append(creds, cm.Credential{Username: binding.Username,
				Password: binding.Password, BearerKey: binding.BearerKey})
			app.DeploymentConfig.Curl[i].Username = ""
			app.DeploymentConfig.Curl[i].Password = ""
			app.DeploymentConfig.Curl[i].BearerKey = ""
		}

		data, err := json.MarshalIndent(creds, "", " ")
		if err != nil {
			logging.Errorf("%s Function %s marshal failed for credentials", logPrefix, appLocation)
			return nil, err
		}

		err = MetakvSetSensitive(cm.MetakvCredentialsPath+appLocation, data, nil)
		if err != nil {
			logging.Errorf("%s Function: %s failed to store credentials in credentials path", logPrefix, appLocation)
			return nil, err
		}

		data, err = json.MarshalIndent(app, "", " ")
		if err != nil {
			logging.Errorf("%s Function: %s failed to marshal data", logPrefix, appLocation)
			return nil, err
		}
		return data, nil
	}
	app := ParseFunctionPayload(payload, appLocation)

	var creds []cm.Credential
	for i, binding := range app.DeploymentConfig.Curl {
		creds = append(creds, cm.Credential{Username: binding.Username,
			Password: binding.Password, BearerKey: binding.BearerKey})
		app.DeploymentConfig.Curl[i].Username = ""
		app.DeploymentConfig.Curl[i].Password = ""
		app.DeploymentConfig.Curl[i].BearerKey = ""
	}

	data, err := json.MarshalIndent(creds, "", " ")
	if err != nil {
		logging.Errorf("%s Function %s marshal failed for credentials", logPrefix, appLocation)
		return nil, err
	}

	err = MetakvSetSensitive(cm.MetakvCredentialsPath+appLocation, data, nil)
	if err != nil {
		logging.Errorf("%s Function: %s failed to store credentials in credentials path", logPrefix, appLocation)
		return nil, err
	}

	appContent := EncodeAppPayload(&app)
	return appContent, nil

}

func AppendCredentials(path, appLocation string, payload []byte) ([]byte, error) {
	logPrefix := "Util::AppendCredentials"

	data, err := MetakvGet(cm.MetakvCredentialsPath + appLocation)
	if err != nil {
		logging.Errorf("%s Function: %s metakv get failed for credentials, err: %v",
			logPrefix, appLocation, err)
		return nil, err
	}

	if data == nil {
		return payload, nil
	}

	var creds []cm.Credential
	if err = json.Unmarshal(data, &creds); err != nil {
		logging.Errorf("%s Function: %s unmarshal failed for credentials", logPrefix, appLocation)
		return nil, err
	}

	if path == cm.MetakvTempAppsPath+appLocation {
		var app cm.Application
		if err = json.Unmarshal(payload, &app); err != nil {
			logging.Errorf("%s Function: %s unmarshal failed for data from metakv", logPrefix, appLocation)
			return nil, err
		}

		if len(creds) < len(app.DeploymentConfig.Curl) {
			app.DeploymentConfig.Curl = app.DeploymentConfig.Curl[:len(creds)]
		}

		for i, _ := range app.DeploymentConfig.Curl {
			app.DeploymentConfig.Curl[i].Username = creds[i].Username
			app.DeploymentConfig.Curl[i].Password = creds[i].Password
			app.DeploymentConfig.Curl[i].BearerKey = creds[i].BearerKey
		}

		data, err = json.MarshalIndent(app, "", " ")
		if err != nil {
			logging.Errorf("%s Function: %s failed to marshal data", logPrefix, appLocation)
			return nil, err
		}

		return data, nil
	}

	app := ParseFunctionPayload(payload, appLocation)

	if len(creds) < len(app.DeploymentConfig.Curl) {
		app.DeploymentConfig.Curl = app.DeploymentConfig.Curl[:len(creds)]
	}

	for i, _ := range app.DeploymentConfig.Curl {
		app.DeploymentConfig.Curl[i].Username = creds[i].Username
		app.DeploymentConfig.Curl[i].Password = creds[i].Password
		app.DeploymentConfig.Curl[i].BearerKey = creds[i].BearerKey
	}

	appContent := EncodeAppPayload(&app)
	return appContent, nil

}

func AppendLangCompat(path, appLocation string, payload []byte) ([]byte, error) {
	logPrefix := "Util::AppendLangCompat"

	if path == cm.MetakvTempAppsPath+appLocation {
		var app cm.Application
		if err := json.Unmarshal(payload, &app); err != nil {
			logging.Errorf("%s Function: %s unmarshal failed for data from metakv", logPrefix, appLocation)
			return nil, err
		}

		if _, ok := app.Settings["language_compatibility"]; !ok {
			app.Settings["language_compatibility"] = cm.LanguageCompatibility[0]
		}

		data, mErr := json.MarshalIndent(app, "", " ")
		if mErr != nil {
			logging.Errorf("%s Function: %s failed to marshal data", logPrefix, appLocation)
			return nil, mErr
		}

		return data, nil
	}

	return payload, nil
}

func RoundUpToNearestPowerOf2(number float64) int {
	return 1 << uint16(math.Ceil(math.Log2(number)))
}

const OBOAuthHeader = "cb-on-behalf-of"

func ComposeOBOAuthInfo(user, domain string) (oboAuthInfo string) {

	if user != "" && domain != "" {
		return base64.StdEncoding.EncodeToString([]byte(user + ":" + domain))
	}

	return ""
}

func CheckAndGetBktAndScopeIDs(fG *common.FunctionScope, restPort string) (string, uint32, error) {
	nsServerEndpoint := net.JoinHostPort(Localhost(), restPort)
	cic, err := FetchClusterInfoClient(nsServerEndpoint)
	if err != nil {
		return "", 0, err
	}

	clusterInfo := cic.GetClusterInfoCache()
	clusterInfo.RLock()
	defer clusterInfo.RUnlock()

	bucketUUID, scopeId, _, err := clusterInfo.GetUniqueBSCIds(fG.BucketName, fG.ScopeName, "")
	if err != nil {
		return "", 0, err
	}

	return bucketUUID, scopeId, nil
}
