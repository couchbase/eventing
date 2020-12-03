package supervisor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/gen/flatbuf/cfg"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

func (s *SuperSupervisor) assignVbucketsToOwn(addrs []string, currNodeAddr string) {
	logPrefix := "SuperSupervisor::assignVbucketsToOwn"

	if len(addrs) <= 0 {
		logging.Fatalf("%s Unexpected count of eventing nodes reported, count: %d", logPrefix, len(addrs))
		return
	}

	sort.Strings(addrs)

	vbucketsPerNode := numVbuckets / len(addrs)
	var vbNo int
	var startVb uint16

	vbCountPerNode := make([]int, len(addrs))
	for i := 0; i < len(addrs); i++ {
		vbCountPerNode[i] = vbucketsPerNode
		vbNo += vbucketsPerNode
	}

	remainingVbs := numVbuckets - vbNo
	if remainingVbs > 0 {
		for i := 0; i < remainingVbs; i++ {
			vbCountPerNode[i] = vbCountPerNode[i] + 1
		}
	}

	var currNodeIndex int
	for i, v := range addrs {
		if v == currNodeAddr {
			currNodeIndex = i
		}
	}

	for i := 0; i < currNodeIndex; i++ {
		for j := 0; j < vbCountPerNode[i]; j++ {
			startVb++
		}
	}

	assignedVbs := make([]uint16, 0)

	for i := 0; i < vbCountPerNode[currNodeIndex]; i++ {
		assignedVbs = append(assignedVbs, startVb)
		startVb++
	}

	s.vbucketsToOwn = make([]uint16, 0)
	for _, vb := range assignedVbs {
		s.vbucketsToOwn = append(s.vbucketsToOwn, vb)
	}

	logging.Infof("%s [%d] currNodeAddr: %rs vbucketsToOwn len: %v dump: %v",
		logPrefix, s.runningFnsCount(), currNodeAddr, len(s.vbucketsToOwn), util.Condense(s.vbucketsToOwn))
}

func (s *SuperSupervisor) getStatuses(data []byte) (bool, bool, bool, map[string]interface{}, error) {
	logPrefix := "SuperSupervisor::getStatuses"

	settings := make(map[string]interface{})
	err := json.Unmarshal(data, &settings)
	if err != nil {
		logging.Errorf("%s [%d] Failed to unmarshal settings", logPrefix, s.runningFnsCount())
		return false, false, false, settings, err
	}

	val, ok := settings["processing_status"]
	if !ok {
		logging.Errorf("%s [%d] Missing processing_status", logPrefix, s.runningFnsCount())
		return false, false, false, settings, fmt.Errorf("missing processing_status")
	}

	pStatus, ok := val.(bool)
	if !ok {
		logging.Errorf("%s [%d] Supplied processing_status unexpected", logPrefix, s.runningFnsCount())
		return false, false, false, settings, fmt.Errorf("non boolean processing_status")
	}

	val, ok = settings["deployment_status"]
	if !ok {
		logging.Errorf("%s [%d] Missing deployment_status", logPrefix, s.runningFnsCount())
		return false, false, false, settings, fmt.Errorf("missing deployment_status")
	}

	dStatus, ok := val.(bool)
	if !ok {
		logging.Errorf("%s [%d] Supplied deployment_status unexpected", logPrefix, s.runningFnsCount())
		return false, false, false, settings, fmt.Errorf("non boolean deployment_status")
	}

	val, ok = settings["cleanup_timers"]
	if !ok {
		logging.Infof("%s [%d] Missing cleanup_timers", logPrefix, s.runningFnsCount())
		return pStatus, dStatus, false, settings, nil
	}

	cTimers, ok := val.(bool)
	if !ok {
		logging.Infof("%s [%d] Supplied cleanup_timers unexpected", logPrefix, s.runningFnsCount())
		return pStatus, dStatus, false, settings, nil
	}

	return pStatus, dStatus, cTimers, settings, nil
}

func (s *SuperSupervisor) getSourceAndMetaBucketNodeCount(appName string) (int, int, error) {
	source, meta, err := s.getSourceAndMetaBucket(appName)
	if err != nil {
		return 0, 0, err
	}
	hostAddress := net.JoinHostPort(util.Localhost(), s.restPort)
	sourceNodeCount := util.CountActiveKVNodes(source, hostAddress)
	metaNodeCount := util.CountActiveKVNodes(meta, hostAddress)
	return sourceNodeCount, metaNodeCount, nil
}

func (s *SuperSupervisor) getSourceAndMetaBucket(appName string) (source string, meta string, err error) {
	logPrefix := "SuperSupervisor::getSourceAndMetaBucket"
	var appData []byte
	err = util.Retry(util.NewFixedBackoff(time.Second), nil, metakvAppCallback, s, MetakvAppsPath, MetakvChecksumPath, appName, &appData)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s] Exiting due to timeout", logPrefix, appName)
		return
	}
	config := cfg.GetRootAsConfig(appData, 0)
	depcfg := config.DepCfg(new(cfg.DepCfg))
	source = string(depcfg.SourceBucket())
	meta = string(depcfg.MetadataBucket())
	return
}

func printMemoryStats() {
	stats := memoryStats()
	buf, err := json.Marshal(&stats)
	if err == nil {
		logging.Infof(string(buf))
	}
}

func memoryStats() map[string]interface{} {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	stats := make(map[string]interface{})
	stats["allocated"] = mem.Alloc
	stats["last_gc_cycle"] = time.Unix(0, int64(mem.LastGC)).String()
	stats["heap_allocated"] = mem.HeapAlloc
	stats["heap_idle"] = mem.HeapIdle
	stats["heap_in_use"] = mem.HeapInuse
	stats["heap_objects"] = mem.HeapObjects
	stats["heap_os_related"] = mem.HeapReleased
	stats["heap_system"] = mem.HeapSys
	stats["next_gc_cycle"] = parseNano(mem.NextGC)
	stats["num_gc"] = mem.NumGC
	stats["memory_allocations"] = mem.Mallocs
	stats["memory_frees"] = mem.Frees
	stats["stack_cache_in_use"] = mem.MCacheInuse
	stats["stack_in_use"] = mem.StackInuse
	stats["stack_span_in_use"] = mem.MSpanInuse
	stats["stack_system"] = mem.StackSys
	stats["total_allocated"] = mem.TotalAlloc

	return stats
}

func parseNano(n uint64) string {
	var suffix string

	switch {
	case n > 1e9:
		n /= 1e9
		suffix = "s"
	case n > 1e6:
		n /= 1e6
		suffix = "ms"
	case n > 1e3:
		n /= 1e3
		suffix = "us"
	default:
		suffix = "ns"
	}

	return strconv.Itoa(int(n)) + suffix
}

func (s *SuperSupervisor) watchBucketWithLock(bucketName, appName string) error {
	logPrefix := "Supervisor::watchBucketWithLock"
	bucketWatch, ok := s.buckets[bucketName]
	if !ok {
		bucketWatch = &bucketWatchStruct{}
		err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &s.retryCount, commonConnectBucketOpCallback, &bucketWatch.b, bucketName, s.restPort)
		if err != nil {
			logging.Errorf("%s: Could not connect to bucket %s err: %v", logPrefix, bucketName, err)
			return err
		}
		bucketWatch.apps = make(map[string]struct{})
		s.buckets[bucketName] = bucketWatch
	}
	bucketWatch.apps[appName] = struct{}{}

	return nil
}

func (s *SuperSupervisor) bucketRefresh() error {
	s.bucketsRWMutex.Lock()
	defer s.bucketsRWMutex.Unlock()
	for bucketName := range s.buckets {
		if err := s.buckets[bucketName].Refresh(s.retryCount, s.restPort); err != nil {
			return err
		}
	}
	return nil
}

func (s *SuperSupervisor) watchBucketChanges() {
	config := s.getConfig()
	s.servicesNotifierRetryTm = 6000
	if tm, ok := config["service_notifier_timeout"]; ok {
		retryTm, tOk := tm.(float64)
		if tOk {
			s.servicesNotifierRetryTm = uint(retryTm)
		}
	}

	selfRestart := func() {
		time.Sleep(time.Duration(s.servicesNotifierRetryTm) * time.Millisecond)
		go s.watchBucketChanges()
	}

	hostPortAddr := net.JoinHostPort(util.Localhost(), s.restPort)
	clusterAuthURL, err := util.ClusterAuthUrl(hostPortAddr)
	if err != nil {
		logging.Errorf("WatchBucketChanges ClusterAuthUrl(): %v\n", err)
		selfRestart()
		return
	}

	scn, err := util.NewServicesChangeNotifier(clusterAuthURL, "default")
	if err != nil {
		logging.Errorf("ClusterInfoClient NewServicesChangeNotifier(): %v\n", err)
		selfRestart()
		return
	}
	defer scn.Close()

	ticker := time.NewTicker(time.Duration(s.servicesNotifierRetryTm) * time.Millisecond)
	defer ticker.Stop()

	// For observing node services config
	ch := scn.GetNotifyCh()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				selfRestart()
				return
			} else if err := s.bucketRefresh(); err != nil {
				logging.Errorf("s.bucketRefresh(): %v\n", err)
				selfRestart()
				return
			}
		case <-ticker.C:
			if err := s.bucketRefresh(); err != nil {
				logging.Errorf("s.bucketRefresh(): %v\n", err)
				selfRestart()
				return
			}
		case <-s.finch:
			return
		}
	}
}

func (s *SuperSupervisor) getConfig() (c common.Config) {

	data, err := util.MetakvGet(common.MetakvConfigPath)
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

func (bw *bucketWatchStruct) Refresh(retryCount int64, restPort string) error {
	logPrefix := "bucketWatchStruct:Refresh"
	err := bw.b.Refresh()

	if err != nil {
		if strings.Contains(err.Error(), "Bucket uuid does not match") {
			logging.Errorf("%s Bucket: %s out of sync. refreshing...", logPrefix, bw.b.Name)
			err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &retryCount, commonConnectBucketOpCallback, &bw.b, bw.b.Name, restPort)
			if err != nil {
				logging.Errorf("%s: Could not connect to bucket %s err: %v", logPrefix, bw.b.Name, err)
			}
		}
	}
	return err
}

func (bw *bucketWatchStruct) Close() {
	bw.b.Close()
}
