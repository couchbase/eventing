package supervisor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/collections"
	"github.com/couchbase/eventing/dcp"
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

func (s *SuperSupervisor) getStatuses(data []byte) (bool, bool, map[string]interface{}, error) {
	logPrefix := "SuperSupervisor::getStatuses"

	settings := make(map[string]interface{})
	err := json.Unmarshal(data, &settings)
	if err != nil {
		logging.Errorf("%s [%d] Failed to unmarshal settings", logPrefix, s.runningFnsCount())
		return false, false, settings, err
	}

	val, ok := settings["processing_status"]
	if !ok {
		logging.Errorf("%s [%d] Missing processing_status", logPrefix, s.runningFnsCount())
		return false, false, settings, fmt.Errorf("missing processing_status")
	}

	pStatus, ok := val.(bool)
	if !ok {
		logging.Errorf("%s [%d] Supplied processing_status unexpected", logPrefix, s.runningFnsCount())
		return false, false, settings, fmt.Errorf("non boolean processing_status")
	}

	val, ok = settings["deployment_status"]
	if !ok {
		logging.Errorf("%s [%d] Missing deployment_status", logPrefix, s.runningFnsCount())
		return false, false, settings, fmt.Errorf("missing deployment_status")
	}

	dStatus, ok := val.(bool)
	if !ok {
		logging.Errorf("%s [%d] Supplied deployment_status unexpected", logPrefix, s.runningFnsCount())
		return false, false, settings, fmt.Errorf("non boolean deployment_status")
	}

	return pStatus, dStatus, settings, nil
}

func (s *SuperSupervisor) checkSourceAndMetadataKeyspaceExist(appName string) (bool, bool, error) {
	source, meta, err := s.getSourceAndMetaKeyspace(appName)
	if err != nil {
		return false, false, err
	}

	hostAddress := net.JoinHostPort(util.Localhost(), s.restPort)
	sourceExist := util.CheckKeyspaceExist(source.BucketName, source.ScopeName, source.CollectionName, hostAddress)
	metaDataExist := util.CheckKeyspaceExist(meta.BucketName, meta.ScopeName, meta.CollectionName, hostAddress)
	return sourceExist, metaDataExist, nil
}

func (s *SuperSupervisor) getSourceAndMetaKeyspace(appName string) (*common.Keyspace, *common.Keyspace, error) {
	var appData []byte
	err := util.Retry(util.NewFixedBackoff(time.Second), nil, metakvAppCallback, s, MetakvAppsPath, MetakvChecksumPath, appName, &appData)
	if err == common.ErrRetryTimeout {
		return nil, nil, err
	}
	config := cfg.GetRootAsConfig(appData, 0)
	depcfg := config.DepCfg(new(cfg.DepCfg))
	source := &common.Keyspace{
		BucketName:     string(depcfg.SourceBucket()),
		ScopeName:      common.CheckAndReturnDefaultForScopeOrCollection(string(depcfg.SourceScope())),
		CollectionName: common.CheckAndReturnDefaultForScopeOrCollection(string(depcfg.SourceCollection())),
	}

	meta := &common.Keyspace{
		BucketName:     string(depcfg.MetadataBucket()),
		ScopeName:      common.CheckAndReturnDefaultForScopeOrCollection(string(depcfg.MetadataScope())),
		CollectionName: common.CheckAndReturnDefaultForScopeOrCollection(string(depcfg.MetadataCollection())),
	}

	return source, meta, nil
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
		// forcefully update the manifest of the Bucket
		bucketWatch.RefreshBucketManifestOnUIDChange("", s.restPort)
		s.scn.RunObserveCollectionManifestChanges(bucketName)
		bucketWatch.apps = make(map[string]struct{})
		s.buckets[bucketName] = bucketWatch
	}
	bucketWatch.apps[appName] = struct{}{}

	return nil
}

func (s *SuperSupervisor) bucketRefresh(np *couchbase.Pool) ([]string, error) {
	s.bucketsRWMutex.Lock()
	defer s.bucketsRWMutex.Unlock()
	deletedBuckets := make([]string, 0)
	var nHash string
	if np != nil && (atomic.LoadInt32(&s.fetchBucketInfoOnURIHashChangeOnly) == 1) {
		nHash, _ = np.GetBucketURLVersionHash()
	}

	for bucketName := range s.buckets {
		if err := s.buckets[bucketName].Refresh(s.retryCount, s.restPort, np, nHash); err != nil {
			if err == NoBucket {
				deletedBuckets = append(deletedBuckets, bucketName)
			} else {
				return nil, err
			}
		}
	}
	return deletedBuckets, nil
}

func (s *SuperSupervisor) FetchBucketManifestInfo(bucketName string, muid string) (bool, error) {
	s.bucketsRWMutex.Lock()
	defer s.bucketsRWMutex.Unlock()

	bucketWatch, ok := s.buckets[bucketName]
	if !ok {
		return false, nil
	}

	return bucketWatch.RefreshBucketManifestOnUIDChange(muid, s.restPort)
}

func (s *SuperSupervisor) undeployFunctionsOnDeletedBkts(deletedBuckets []string) {
	logPrefix := "SuperSupervisor::undeployFunctionsOnDeletedBkts"
	logging.Infof("%s Undeploying functions due to bucket delete: %v", logPrefix, deletedBuckets)

	for _, bucketName := range deletedBuckets {
		appNames, ok := s.getAppsWatchingBucket(bucketName)
		if !ok {
			continue
		}

		for _, appName := range appNames {
			p, ok := s.runningProducers[appName]
			if !ok {
				util.Retry(util.NewExponentialBackoff(), &s.retryCount, undeployFunctionCallback, s, appName)
				continue
			}
			skipMetadataCleanup := (p.MetadataBucket() == bucketName)
			p.UndeployHandler(skipMetadataCleanup)
		}
	}
}

func (s *SuperSupervisor) checkDeletedCid(bucketName string) {
	logPrefix := "SuperSupervisor::checkDeletedCid"
	appNames, ok := s.getAppsWatchingBucket(bucketName)
	if !ok {
		return
	}

	for _, appName := range appNames {
		p, ok := s.runningProducers[appName]
		if !ok {
			// possible that app didn't get spawned yet
			logging.Infof("%s Undeploying %s Reason: Not in running producer", logPrefix, appName)
			util.Retry(util.NewExponentialBackoff(), &s.retryCount, undeployFunctionCallback, s, appName)
			continue
		}

		mCid := p.GetMetadataCid()
		if mCid == math.MaxUint32 {
			continue
		}
		cid, err := s.GetCollectionID(p.MetadataBucket(), p.MetadataScope(), p.MetadataCollection())
		if err != nil || cid != mCid {
			logging.Infof("%s Undeploying %s Reason: metadata collection delete err: %v", logPrefix, appName, err)
			p.UndeployHandler(true)
			continue
		}

		sCid := p.GetSourceCid()
		if sCid == math.MaxUint32 {
			continue
		}
		cid, err = s.GetCollectionID(p.SourceBucket(), p.SourceScope(), p.SourceCollection())
		if err != nil || cid != sCid {
			logging.Infof("%s Undeploying %s Reason: source collection delete err: %v", logPrefix, appName, err)
			p.UndeployHandler(false)
		}
	}
}

func (s *SuperSupervisor) watchBucketChanges() {
	config := s.getConfig()
	s.servicesNotifierRetryTm = 5
	if tm, ok := config["service_notifier_timeout"]; ok {
		retryTm, tOk := tm.(float64)
		if tOk {
			s.servicesNotifierRetryTm = uint(retryTm)
		}
	}

	delChannel := make(chan []string, 5)
	go func(delChannel <-chan []string) {
		for {
			select {
			case delBuckets, ok := <-delChannel:
				if !ok {
					return
				}
				s.undeployFunctionsOnDeletedBkts(delBuckets)
			}
		}
	}(delChannel)

	selfRestart := func() {
		close(delChannel)
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

	s.scn, err = util.NewServicesChangeNotifier(clusterAuthURL, "default")
	if err != nil {
		logging.Errorf("ClusterInfoClient NewServicesChangeNotifier(): %v\n", err)
		selfRestart()
		return
	}
	defer s.scn.Close()

	ticker := time.NewTicker(time.Duration(s.servicesNotifierRetryTm) * time.Minute)
	defer ticker.Stop()

	// For observing node services config
	ch := s.scn.GetNotifyCh()
	for {
		select {
		case notif, ok := <-ch:
			if !ok {
				selfRestart()
				return
			}
			if notif.Type == util.CollectionManifestChangeNotification {
				bucket := (notif.Msg).(*couchbase.Bucket)
				changed, err := s.FetchBucketManifestInfo(bucket.Name, bucket.CollectionManifestUID)
				if err != nil {
					logging.Errorf("cic.cinfo.FetchManifestInfo(): %v\n", err)
					selfRestart()
					return
				}
				if changed {
					s.checkDeletedCid(bucket.Name)
				}
			}

			if notif.Type != util.PoolChangeNotification {
				continue
			}

			np := notif.Msg.(*couchbase.Pool)
			deletedBuckets, err := s.bucketRefresh(np)
			if err != nil {
				logging.Errorf("Refresh bucket Error")
				selfRestart()
				return
			}

			if len(deletedBuckets) != 0 {
				delChannel <- deletedBuckets
			}

		case <-ticker.C:
			deletedBuckets, err := s.bucketRefresh(nil)
			if err != nil {
				logging.Errorf("Refresh bucket Error")
				selfRestart()
				return
			}

			if len(deletedBuckets) != 0 {
				delChannel <- deletedBuckets
			}

		case <-s.finch:
			close(delChannel)
			return
		}
	}
}

func (s *SuperSupervisor) OptimiseBucketLoading(optimise bool) {
	if optimise {
		atomic.StoreInt32(&s.fetchBucketInfoOnURIHashChangeOnly, 1)
		return
	}
	s.bucketRefresh(nil)
	atomic.StoreInt32(&s.fetchBucketInfoOnURIHashChangeOnly, 0)
	return
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

// returns apps which are watching bucket. If bucket not being watched then 2nd argument will be false
func (s *SuperSupervisor) getAppsWatchingBucket(bucketName string) ([]string, bool) {
	s.bucketsRWMutex.RLock()
	defer s.bucketsRWMutex.RUnlock()
	bucketWatch, ok := s.buckets[bucketName]
	if !ok {
		return nil, false
	}
	return bucketWatch.AppNames(), true
}

func (bw *bucketWatchStruct) Refresh(retryCount int64, restPort string, np *couchbase.Pool, nHash string) error {
	logPrefix := "bucketWatchStruct:Refresh"
	oHash, err := bw.b.GetBucketURLVersionHash()
	if err != nil {
		logging.Errorf("%s Get bucket version hash error: %v", logPrefix, err)
		return nil
	}
	if nHash == oHash {
		return nil
	}

	err = bw.b.RefreshWithTerseBucket()
	if err != nil {
		if strings.Contains(err.Error(), "HTTP error 404 Object Not Found") {
			return NoBucket
		}
		if strings.Contains(err.Error(), "Bucket uuid does not match") {
			return NoBucket
		}
	}

	if np != nil {
		bw.b.SetBucketUri(np.BucketURL["uri"])
	}

	bw.RefreshBucketManifestOnUIDChange("", restPort)
	return err
}

func (bw *bucketWatchStruct) RefreshBucketManifestOnUIDChange(muid, restPort string) (bool, error) {
	hostAddress := net.JoinHostPort(util.Localhost(), restPort)
	cic, err := util.FetchClusterInfoClient(hostAddress)
	if err != nil {
		return false, err
	}
	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	version := cinfo.GetNodeCompatVersion("")
	cinfo.RUnlock()

	if version < collections.COLLECTION_SUPPORTED_VERSION {
		return false, nil
	}

	if muid != "" && bw.b.Manifest.UID == muid {
		return false, nil
	}
	return true, bw.b.RefreshBucketManifest()
}

func (bw *bucketWatchStruct) Close() {
	bw.b.Close()
}

func (bw *bucketWatchStruct) AppNames() []string {
	appNames := make([]string, 0, len(bw.apps))
	for appName := range bw.apps {
		appNames = append(appNames, appName)
	}
	return appNames
}

func (bw *bucketWatchStruct) GetManifestId() string {
	if bw.b.Manifest == nil {
		return "0"
	}
	return bw.b.Manifest.UID
}
