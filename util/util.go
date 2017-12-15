package util

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/cbauth/metakv"
	cm "github.com/couchbase/eventing/common"
	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/gomemcached"
)

const (
	EventingAdminService = "eventingAdminPort"
	DataService          = "kv"
	MgmtService          = "mgmt"
	HTTPRequestTimeout   = time.Duration(5000) * time.Millisecond
)

type Uint16Slice []uint16

func (s Uint16Slice) Len() int           { return len(s) }
func (s Uint16Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Uint16Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type Config map[string]interface{}

type ConfigHolder struct {
	ptr unsafe.Pointer
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

func listOfVbnos(startVB int, endVB int) []uint16 {
	vbnos := make([]uint16, 0, endVB-startVB)
	for i := startVB; i <= endVB; i++ {
		vbnos = append(vbnos, uint16(i))
	}
	return vbnos
}

func sprintWorkerState(state map[int]map[string]interface{}) string {
	line := ""
	for workerid := range state {
		line += fmt.Sprintf("workerID: %d startVB: %d endVB: %d ",
			workerid, state[workerid]["start_vb"].(int), state[workerid]["end_vb"].(int))
	}
	return strings.TrimRight(line, " ")
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

func NsServerNodesAddresses(auth, hostaddress string) ([]string, error) {
	cinfo, err := FetchNewClusterInfoCache(hostaddress)
	if err != nil {
		return nil, err
	}

	nsServerAddrs := cinfo.GetNodesByServiceType(MgmtService)

	nsServerNodes := []string{}
	for _, nsServerAddr := range nsServerAddrs {
		addr, _ := cinfo.GetServiceAddress(nsServerAddr, MgmtService)
		nsServerNodes = append(nsServerNodes, addr)
	}

	sort.Strings(nsServerNodes)

	return nsServerNodes, nil
}

func KVNodesAddresses(auth, hostaddress string) ([]string, error) {
	cinfo, err := FetchNewClusterInfoCache(hostaddress)
	if err != nil {
		return nil, err
	}

	kvAddrs := cinfo.GetNodesByServiceType(DataService)

	kvNodes := []string{}
	for _, kvAddr := range kvAddrs {
		addr, _ := cinfo.GetServiceAddress(kvAddr, DataService)
		kvNodes = append(kvNodes, addr)
	}

	sort.Strings(kvNodes)

	return kvNodes, nil
}

func EventingNodesAddresses(auth, hostaddress string) ([]string, error) {
	cinfo, err := FetchNewClusterInfoCache(hostaddress)
	if err != nil {
		return nil, err
	}

	eventingAddrs := cinfo.GetNodesByServiceType(EventingAdminService)

	eventingNodes := []string{}
	for _, eventingAddr := range eventingAddrs {
		addr, err := cinfo.GetServiceAddress(eventingAddr, EventingAdminService)
		if err != nil {
			logging.Errorf("UTIL Failed to get eventing node address, err: %v", err)
			continue
		}
		eventingNodes = append(eventingNodes, addr)
	}

	sort.Strings(eventingNodes)

	return eventingNodes, nil
}

func CurrentEventingNodeAddress(auth, hostaddress string) (string, error) {
	cinfo, err := FetchNewClusterInfoCache(hostaddress)
	if err != nil {
		return "", err
	}

	cNodeID := cinfo.GetCurrentNode()
	eventingNode, err := cinfo.GetServiceAddress(cNodeID, EventingAdminService)
	if err != nil {
		logging.Errorf("UTIL Failed to get current eventing node address, err: %v", err)
		return "", err
	}
	return eventingNode, nil
}

func LocalEventingServiceHost(auth, hostaddress string) (string, error) {
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

func KVVbMap(auth, bucket, hostaddress string) (map[uint16]string, error) {
	cinfo, err := FetchNewClusterInfoCache(hostaddress)
	if err != nil {
		return nil, err
	}

	kvAddrs := cinfo.GetNodesByServiceType(DataService)

	kvVbMap := make(map[uint16]string)

	for _, kvAddr := range kvAddrs {
		addr, err := cinfo.GetServiceAddress(kvAddr, DataService)
		if err != nil {
			logging.Errorf("UTIL Failed to get address of KV host: %v, err: %v", kvAddr, err)
			return nil, err
		}

		vbs, err := cinfo.GetVBuckets(kvAddr, bucket)
		if err != nil {
			logging.Errorf("UTIL Failed to get vbuckets for given kv: %v, err: %v", kvAddr, err)
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
	msg := fmt.Sprintf(format, v...)
	values := url.Values{"message": {msg}, "logLevel": {"info"}, "component": {"indexing"}}

	if !strings.HasPrefix(clusterAddr, "http://") {
		clusterAddr = "http://" + clusterAddr
	}
	clusterAddr += "/_log"

	_, err := PostForm(clusterAddr, values)

	return err
}

func GetProcessedPSec(urlSuffix, nodeAddr string) (string, error) {
	netClient := NewClient(HTTPRequestTimeout)

	url := fmt.Sprintf("http://%s%s", nodeAddr, urlSuffix)

	res, err := netClient.Get(url)
	if err != nil {
		logging.Errorf("UTIL Failed to gather events processed/sec stats from url: %s, err: %v", url, err)
		return "", err
	}
	defer res.Body.Close()

	buf, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logging.Errorf("UTIL Failed to read response body from url: %s, err: %v", url, err)
		return "", err
	}

	return string(buf), nil
}

func GetAggProcessedPSec(urlSuffix string, nodeAddrs []string) (string, error) {
	netClient := NewClient(HTTPRequestTimeout)

	var aggPStats cm.EventProcessingStats

	for _, nodeAddr := range nodeAddrs {
		url := fmt.Sprintf("http://%s%s", nodeAddr, urlSuffix)

		res, err := netClient.Get(url)
		if err != nil {
			logging.Errorf("UTIL Failed to gather events processed/sec stats from url: %s, err: %v", url, err)
			continue
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("UTIL Failed to read response body from url: %s, err: %v", url, err)
			continue
		}

		var pStats cm.EventProcessingStats
		err = json.Unmarshal(buf, &pStats)
		if err != nil {
			logging.Errorf("UTIL Failed to unmarshal response body from url: %s, err: %v", url, err)
			continue
		}

		aggPStats.DcpEventsProcessedPSec += pStats.DcpEventsProcessedPSec
		aggPStats.TimerEventsProcessedPSec += pStats.TimerEventsProcessedPSec
	}

	aggPStats.Timestamp = time.Now().Format("2006-01-02T15:04:05Z")

	stats, err := json.Marshal(&aggPStats)
	if err != nil {
		logging.Errorf("UTIL Failed to marshal aggregate processing stats, err: %v", err)
		return "", err
	}

	return string(stats), nil
}

func StopDebugger(urlSuffix, nodeAddr, appName string) {
	url := fmt.Sprintf("http://%s/stopDebugger/?name=%s", nodeAddr, appName)
	netClient := NewClient(HTTPRequestTimeout)

	_, err := netClient.Get(url)
	if err != nil {
		logging.Errorf("UTIL Failed to capture v8 debugger url from url: %s, err: %v", url, err)
		return
	}
	return
}

func GetDebuggerURL(urlSuffix, nodeAddr, appName string) string {
	if nodeAddr == "" {
		logging.Verbosef("UTIL Debugger host not found. Debugger not started")
		return ""
	}

	url := fmt.Sprintf("http://%s/%s/?name=%s", nodeAddr, urlSuffix, appName)

	netClient := NewClient(HTTPRequestTimeout)

	res, err := netClient.Get(url)
	if err != nil {
		logging.Errorf("UTIL Failed to capture v8 debugger url from url: %s, err: %v", url, err)
		return ""
	}

	buf, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logging.Errorf("UTIL Failed to read v8 debugger url response from url: %s, err: %v", url, err)
		return ""
	}

	return string(buf)
}

func GetNodeUUIDs(urlSuffix string, nodeAddrs []string) (map[string]string, error) {
	addrUUIDMap := make(map[string]string)

	netClient := NewClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		url := fmt.Sprintf("http://%s%s", nodeAddr, urlSuffix)

		res, err := netClient.Get(url)
		if err != nil {
			logging.Errorf("UTIL Failed to fetch node uuid from url: %s, err: %v", url, err)
			return nil, err
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("UTIL Failed to read response body from url: %s, err: %v", url, err)
			return nil, err
		}

		addrUUIDMap[string(buf)] = nodeAddr
	}
	return addrUUIDMap, nil
}

func GetEventProcessingStats(urlSuffix string, nodeAddrs []string) (map[string]int64, error) {
	pStats := make(map[string]int64)

	netClient := NewClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		url := fmt.Sprintf("http://%s%s", nodeAddr, urlSuffix)

		res, err := netClient.Get(url)
		if err != nil {
			logging.Errorf("UTIL Failed to gather event processing stats from url: %s, err: %v", url, err)
			return nil, err
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("UTIL Failed to read response body for event processing stats from url: %s, err: %v", url, err)
			return nil, err
		}

		var nodePStats map[string]int64
		err = json.Unmarshal(buf, &nodePStats)
		if err != nil {
			logging.Errorf("UTIL Failed to unmarshal event processing stats from url: %s, err: %v", url, err)
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

func GetProgress(urlSuffix string, nodeAddrs []string) (*cm.RebalanceProgress, error) {
	aggProgress := &cm.RebalanceProgress{}

	netClient := NewClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		url := fmt.Sprintf("http://%s%s", nodeAddr, urlSuffix)

		res, err := netClient.Get(url)
		if err != nil {
			logging.Errorf("UTIL Failed to gather task status from url: %s, err: %v", url, err)
			continue
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("UTIL Failed to read response body from url: %s, err: %v", url, err)
			continue
		}

		var progress cm.RebalanceProgress
		err = json.Unmarshal(buf, &progress)
		if err != nil {
			logging.Errorf("UTIL Failed to unmarshal progress from url: %s, err: %v", url, err)
			continue
		}

		aggProgress.VbsRemainingToShuffle += progress.VbsRemainingToShuffle
		aggProgress.VbsOwnedPerPlan += progress.VbsOwnedPerPlan
	}

	return aggProgress, nil
}

func GetAggTimerHostPortAddrs(appName, eventingAdminPort, urlSuffix string) (map[string]map[string]string, error) {
	netClient := NewClient(HTTPRequestTimeout)

	url := fmt.Sprintf("http://127.0.0.1:%s/%s?name=%s", eventingAdminPort, urlSuffix, appName)

	res, err := netClient.Get(url)
	if err != nil {
		logging.Errorf("UTIL Failed to capture aggregate timer host port addrs from url: %s, err: %v", url, err)
		return nil, err
	}

	buf, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logging.Errorf("UTIL Failed to read response from url: %s, err: %v", url, err)
		return nil, err
	}

	hostPortAddrs := make(map[string]map[string]string)
	err = json.Unmarshal(buf, &hostPortAddrs)
	if err != nil {
		return nil, err
	}

	return hostPortAddrs, nil
}

func GetTimerHostPortAddrs(urlSuffix string, nodeAddrs []string) (string, error) {
	hostPortAddrs := make(map[string]map[string]string)

	netClient := NewClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		url := fmt.Sprintf("http://%s%s", nodeAddr, urlSuffix)

		res, err := netClient.Get(url)
		if err != nil {
			logging.Errorf("UTIL Failed to gather timer host port addrs from url: %s, err: %v", url, err)
			return "", err
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("UTIL Failed to read response body from url: %s, err: %v", url, err)
			return "", err
		}

		var addrs map[string]string
		err = json.Unmarshal(buf, &addrs)
		if err != nil {
			logging.Errorf("UTIL Failed to unmarshal timer host port addrs from url: %s, err: %v", url, err)
			return "", nil
		}

		hostPortAddrs[nodeAddr] = make(map[string]string)
		hostPortAddrs[nodeAddr] = addrs
	}

	buf, err := json.Marshal(&hostPortAddrs)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func GetDeployedApps(urlSuffix string, nodeAddrs []string) (map[string]map[string]string, error) {
	deployedApps := make(map[string]map[string]string)

	netClient := NewClient(HTTPRequestTimeout)

	for _, nodeAddr := range nodeAddrs {
		url := fmt.Sprintf("http://%s%s", nodeAddr, urlSuffix)

		res, err := netClient.Get(url)
		if err != nil {
			logging.Errorf("UTIL Failed to get deployed apps from url: %s, err: %v", url, err)
			return nil, err
		}
		defer res.Body.Close()

		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logging.Errorf("UTIL Failed to read response body from url: %s, err: %v", url, err)
			return nil, err
		}

		var locallyDeployedApps map[string]string
		err = json.Unmarshal(buf, &locallyDeployedApps)
		if err != nil {
			logging.Errorf("UTIL Failed to unmarshal deployed apps from url: %s, err: %v", url, err)
			return nil, err
		}

		deployedApps[nodeAddr] = make(map[string]string)
		deployedApps[nodeAddr] = locallyDeployedApps
	}

	return deployedApps, nil
}

func ListChildren(path string) []string {
	entries, err := metakv.ListAllChildren(path)
	if err != nil {
		logging.Errorf("UTIL Failed to fetch deployed app list from metakv, err: %v", err)
		return nil
	}

	var children []string
	for _, entry := range entries {
		splitRes := strings.Split(entry.Path, "/")
		child := splitRes[len(splitRes)-1]
		children = append(children, child)
	}

	return children
}

func MetakvGet(path string) ([]byte, error) {
	data, _, err := metakv.Get(path)
	if err != nil {
		return nil, err
	}
	return data, err
}

func MetakvSet(path string, value []byte, rev interface{}) error {
	return metakv.Set(path, value, rev)
}

func MetaKvDelete(path string, rev interface{}) error {
	return metakv.Delete(path, rev)
}

func RecursiveDelete(dirpath string) error {
	return metakv.RecursiveDelete(dirpath)
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

func CompareSlices(s1, s2 []string) bool {

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

func SliceDifferences(kv1, kv2 []string) []string {
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
