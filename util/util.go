package util

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/cbauth/metakv"
	cm "github.com/couchbase/eventing/common"
	"github.com/couchbase/gomemcached"
	"github.com/couchbase/indexing/secondary/common"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/logging"
)

const (
	EventingAdminService = "eventingAdminPort"
	DataService          = "kv"
	MgmtService          = "mgmt"
	HTTPRequestTimeout   = time.Duration(1000) * time.Millisecond
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

func SprintDCPCounts(counts map[mcd.CommandCode]int) (string, int, time.Time) {
	line := ""
	ops := 0
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

func SprintV8Counts(counts map[string]int) string {
	line := ""
	for k, v := range counts {
		line += fmt.Sprintf("%s:%v ", k, v)
	}
	return strings.TrimRight(line, " ")
}

func NsServerNodesAddresses(auth, hostaddress string) ([]string, error) {
	cinfo, err := ClusterInfoCache(auth, hostaddress)
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
	cinfo, err := ClusterInfoCache(auth, hostaddress)
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
	cinfo, err := ClusterInfoCache(auth, hostaddress)
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
	cinfo, err := ClusterInfoCache(auth, hostaddress)
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
	cinfo, err := ClusterInfoCache(auth, hostaddress)
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
	cinfo, err := ClusterInfoCache(auth, hostaddress)
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
			logging.Errorf("UTIL Failed to get vbuckets for given kv common.NodeId, err: %v", err)
			return nil, err
		}

		for i := 0; i < len(vbs); i++ {
			kvVbMap[uint16(vbs[i])] = addr
		}
	}

	return kvVbMap, nil
}

func ClusterInfoCache(auth, hostaddress string) (*common.ClusterInfoCache, error) {
	clusterURL := fmt.Sprintf("http://%s@%s", auth, hostaddress)

	cinfo, err := common.NewClusterInfoCache(clusterURL, "default")
	if err != nil {
		return nil, err
	}

	if err := cinfo.Fetch(); err != nil {
		return nil, err
	}

	return cinfo, nil
}

func GetProcessedPSec(urlSuffix, nodeAddr string) (int, error) {
	netClient := &http.Client{
		Timeout: HTTPRequestTimeout,
	}

	url := fmt.Sprintf("http://%s%s", nodeAddr, urlSuffix)

	res, err := netClient.Get(url)
	if err != nil {
		logging.Errorf("UTIL Failed to gather events processed/sec stats from url: %s, err: %v", url, err)
		return 0, err
	}
	defer res.Body.Close()

	buf, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logging.Errorf("UTIL Failed to read response body from url: %s, err: %v", url, err)
		return 0, err
	}

	pSec, err := strconv.Atoi(string(buf))
	if err != nil {
		logging.Errorf("UTIL Failed to convert events processed stats to int from url: %s, err: %v", url, err)
		return 0, err
	}

	return pSec, nil
}

func GetProgress(urlSuffix string, nodeAddrs []string) *cm.RebalanceProgress {
	aggProgress := &cm.RebalanceProgress{}

	netClient := &http.Client{
		Timeout: HTTPRequestTimeout,
	}

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

		aggProgress.VbsCurrentlyOwned += progress.VbsCurrentlyOwned
		aggProgress.VbsOwnedPerPlan += progress.VbsOwnedPerPlan
	}

	return aggProgress
}

func GetTimerHostPortAddrs(urlSuffix string, nodeAddrs []string) (string, error) {
	hostPortAddrs := make(map[string][]string)

	netClient := &http.Client{
		Timeout: HTTPRequestTimeout,
	}

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

		var addrs []string
		err = json.Unmarshal(buf, &addrs)
		if err != nil {
			logging.Errorf("UTIL Failed to unmarshal progress from url: %s, err: %v", url, err)
			continue
		}

		hostPortAddrs[nodeAddr] = addrs
	}

	buf, err := json.Marshal(&hostPortAddrs)
	if err != nil {
		return "", err
	}

	return string(buf), nil
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
	// Trim "Ox"
	cas = cas[2:]

	for i := 0; i < len(cas)/2; i += 2 {
		cas[i], cas[len(cas)-i-2] = cas[len(cas)-i-2], cas[i]
		cas[i+1], cas[len(cas)-i-1] = cas[len(cas)-i-1], cas[i+1]
	}

	return strconv.ParseUint(string(cas), 16, 64)
}
