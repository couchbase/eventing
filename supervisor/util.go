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
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
	couchbase "github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/gen/flatbuf/cfg"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"gopkg.in/couchbase/gocb.v1"
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
				continue
			}
			skipMetadataCleanup := (p.MetadataBucket() == bucketName)
			s.StopProducer(appName, skipMetadataCleanup, true)
		}
	}
}

func (s *SuperSupervisor) watchBucketChanges() {
	logPrefix := "SuperSupervisor::watchBucketChanges"

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

	deletedBuckets, err := s.bucketRefresh(nil)
	if err != nil {
		logging.Errorf("%s Error in bucket Refresh: %v", logPrefix, err)
		selfRestart()
		return
	}

	if len(deletedBuckets) != 0 {
		delChannel <- deletedBuckets
	}

	hostPortAddr := net.JoinHostPort(util.Localhost(), s.restPort)
	s.hostport = hostPortAddr
	s.pool = "default"
	scn, err := util.NewServicesChangeNotifier(s.hostport, s.pool)
	if err != nil {
		logging.Errorf("ClusterInfoClient NewServicesChangeNotifier(): %v\n", err)
		selfRestart()
		return
	}
	s.serviceMgr.NotifySupervisorWaitCh()
	defer scn.Close()

	ticker := time.NewTicker(time.Duration(s.servicesNotifierRetryTm) * time.Minute)
	defer ticker.Stop()

	// For observing node services config
	ch := scn.GetNotifyCh()
	for {
		select {
		case notif, ok := <-ch:
			if !ok {
				logging.Errorf("%s ServicesChangeNotifier channel closed. Restarting..", logPrefix)
				selfRestart()
				return
			}

			if notif.Type == util.EncryptionLevelChangeNotification {
				for {
					_, refreshErr := s.bucketRefresh(nil)
					if refreshErr != nil && strings.Contains(strings.ToLower(refreshErr.Error()), "connection refused") {
						logging.Errorf("%s Error in bucket refresh while processing encryption level change: %v. Retrying...", logPrefix, err)
					} else if refreshErr != nil {
						selfRestart()
						return
					} else {
						break
					}
					time.Sleep(time.Second)
				}
			}

			// Process only PoolChangeNotification as any change to
			// ClusterMembership is reflected only in PoolChangeNotification
			if notif.Type != util.PoolChangeNotification {
				continue
			}

			np := notif.Msg.(*couchbase.Pool)
			deletedBuckets, err := s.bucketRefresh(np)
			if err != nil {
				logging.Errorf("%s Error in bucket Refresh: %v", logPrefix, err)
				selfRestart()
				return
			}

			if len(deletedBuckets) != 0 {
				delChannel <- deletedBuckets
			}

		case <-ticker.C:
			deletedBuckets, err := s.bucketRefresh(nil)
			if err != nil {
				logging.Errorf("%s Error in bucket Refresh: %v", logPrefix, err)
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

func (s *SuperSupervisor) watchBucketWithGocb(bucketName, appName string) error {
	err := s.watchBucket(bucketName, appName)
	if err != nil {
		return err
	}
	err = s.gocbGlobalConfigHandle.maybeRegistergocbBucket(bucketName, appName, s.GetSecuritySetting())
	if err != nil {
		s.unwatchBucket(bucketName, appName)
	}
	return err
}

func (s *SuperSupervisor) unwatchBucketWithGocb(bucketName, appName string) {
	s.unwatchBucket(bucketName, appName)
	s.gocbGlobalConfigHandle.maybeUnregistergocbBucket(bucketName, appName)
}

func (s *SuperSupervisor) watchBucket(bucketName, appName string) error {
	s.bucketsRWMutex.Lock()
	defer s.bucketsRWMutex.Unlock()
	return s.watchBucketWithLock(bucketName, appName)
}

// UnwatchBucket removes the bucket from supervisor
func (s *SuperSupervisor) unwatchBucket(bucketName, appName string) {
	s.bucketsRWMutex.Lock()
	defer s.bucketsRWMutex.Unlock()
	if bucketWatch, ok := s.buckets[bucketName]; ok {
		if _, ok := bucketWatch.apps[appName]; ok {
			delete(bucketWatch.apps, appName)
			if len(bucketWatch.apps) == 0 {
				bucketWatch.Close()
				delete(s.buckets, bucketName)
			}
		}
	}
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

	return err
}

func (bw *bucketWatchStruct) Close() {
	bw.b.Close()
}

func (bw *bucketWatchStruct) AppNames() []string {
	appNames := make([]string, len(bw.apps))
	for appName := range bw.apps {
		appNames = append(appNames, appName)
	}
	return appNames
}

func initgocbGlobalConfig(retryCount int64, restPort string) (*gocbGlobalConfig, error) {
	config := &gocbGlobalConfig{
		appEncryptionMap: make(map[string]bool),
		nsServerPort:     restPort,
		retrycount:       retryCount,
	}
	return config, nil
}

/*
	This function checks encryptionEnabled flag and inserts app <-> bucket mapping
	to appropriate cluster instance. Note: For pause-resume use-case without change in encryption level
	this is a noop
*/
func (config *gocbGlobalConfig) maybeRegistergocbBucket(bucketName, appName string, setting *common.SecuritySetting) error {
	var err error
	logPrefix := "gocbGlobalConfig::maybeRegistergocbBucket"
	config.Lock()
	defer config.Unlock()

	encryptionEnabled := setting != nil && setting.EncryptData == true

	val, found := config.appEncryptionMap[appName]
	if found && val == encryptionEnabled { // resume with no encryption change. noop
		return nil
	}

	if encryptionEnabled == true {
		if config.encryptedgocbPool == nil {
			config.encryptedgocbPool, err = initGoCbPool(config.retrycount, config.nsServerPort, setting)
			if err != nil {
				return fmt.Errorf("Could not create encrypted gocb cluster object. Cause: %v", err)
			}
			logging.Infof("%v Successfully created an encrypted gocb cluster", logPrefix)
		}
		if err = config.encryptedgocbPool.insertBucket(bucketName, appName, config.retrycount, config.nsServerPort); err != nil {
			// failure doesn't add the handle. No need to call gocbpool.remove
			logging.Errorf("%v Could not create an encrypted gocb handle. Cause: %v", logPrefix, err)
			return err
		}
	} else {
		if config.plaingocbPool == nil {
			config.plaingocbPool, err = initGoCbPool(config.retrycount, config.nsServerPort, setting)
			if err != nil {
				return fmt.Errorf("Could not create plain gocb cluster object. Cause: %v", err)
			}
			logging.Infof("%v Successfully created a plain gocb cluster", logPrefix)
		}
		if err = config.plaingocbPool.insertBucket(bucketName, appName, config.retrycount, config.nsServerPort); err != nil {
			logging.Errorf("%v Could not create an plain gocb handle. Cause: %v", logPrefix, err)
			return err
		}
	}

	var poolEmpty bool
	config.appEncryptionMap[appName] = encryptionEnabled
	if found && val != encryptionEnabled { // A resume where encryption level changed.
		if val == true {
			poolEmpty, err = config.encryptedgocbPool.removeBucket(bucketName, appName)
			if poolEmpty {
				config.encryptedgocbPool = nil
			}
			if err != nil {
				// Will err out only if bucket or app wasn't present just log
				logging.Infof("%v Inserted a new plain gocb handle but could not remove existing encrypted handle. Cause: %v", logPrefix, err)
				return err
			}
		} else {
			poolEmpty, err = config.plaingocbPool.removeBucket(bucketName, appName)
			if poolEmpty {
				config.plaingocbPool = nil
			}
			if err != nil {
				logging.Infof("%v Inserted a new encrypted gocb handle but could not remove existing plain handle. Cause: %v", logPrefix, err)
				return err
			}
		}
	}
	return nil
}

func (config *gocbGlobalConfig) maybeUnregistergocbBucket(bucketName, appName string) error {
	logPrefix := "gocbGlobalConfig::maybeUnregistergocbBucket"
	config.Lock()
	defer config.Unlock()
	val, found := config.appEncryptionMap[appName] // if insertion failed this might've given older entry
	if !found {
		// either no entry or registration failed earlier
		return fmt.Errorf("App %s is not registered", appName)
	}
	if val == true {
		poolEmpty, err := config.encryptedgocbPool.removeBucket(bucketName, appName)
		if poolEmpty {
			config.encryptedgocbPool = nil
		}
		if err != nil {
			logging.Infof("%v Couldn't remove existing encrypted gocb handle. Cause: %v", logPrefix, err)
			return err
		}
	} else {
		poolEmpty, err := config.plaingocbPool.removeBucket(bucketName, appName)
		if poolEmpty {
			config.plaingocbPool = nil
		}
		if err != nil {
			logging.Infof("%v Couldn't remove existing plain gocb handle. Cause: %v", logPrefix, err)
			return err
		}
	}
	delete(config.appEncryptionMap, appName)
	return nil
}

func (config *gocbGlobalConfig) getBucket(bucketName, appName string) (*gocb.Bucket, error) {
	config.RLock()
	defer config.RUnlock()
	encrypted, found := config.appEncryptionMap[appName]
	if !found {
		return nil, fmt.Errorf("App: %s is not registered by any of the member pools", appName)
	}
	if encrypted {
		if config.encryptedgocbPool == nil {
			return nil, fmt.Errorf("Encrypted gocb pool hasn't been initialized yet")
		} else {
			return config.encryptedgocbPool.getBucket(bucketName, appName)
		}
	} else {
		if config.plaingocbPool == nil {
			return nil, fmt.Errorf("Plain gocb pool hasn't been initialized yet")
		} else {
			return config.plaingocbPool.getBucket(bucketName, appName)
		}
	}
}

func initGoCbPool(retryCount int64, restPort string, setting *common.SecuritySetting) (*gocbPool, error) {
	pool := &gocbPool{
		bucketHandle: make(map[string]*gocbBucketInstance),
	}
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &retryCount, gocbConnectCluster, &pool.cluster, restPort, setting)
	return pool, err
}

func (pool *gocbPool) insertBucket(bucketName, label string, retryCount int64, restPort string) error {
	pool.Lock()
	defer pool.Unlock()
	bucket, ok := pool.bucketHandle[bucketName]
	if !ok {
		var err error
		bucket, err = initGocbBucketHandle(bucketName, pool.cluster, retryCount, restPort)
		if err != nil {
			return err
		}
		pool.bucketHandle[bucketName] = bucket
	}
	bucket.insert(label)
	return nil
}

func (pool *gocbPool) removeBucket(bucketName, label string) (bool, error) {
	pool.Lock()
	defer pool.Unlock()
	var poolEmpty bool

	bucket, ok := pool.bucketHandle[bucketName]
	if !ok {
		return poolEmpty, fmt.Errorf("Bucket: %s is not registered by any app", bucketName)
	}

	count, err := bucket.remove(label)
	if err != nil {
		return poolEmpty, err
	}

	if count == 0 {
		delete(pool.bucketHandle, bucketName)
		if len(pool.bucketHandle) == 0 {
			pool.cluster.Close()
			poolEmpty = true
		}
	}
	return poolEmpty, nil
}

func (pool *gocbPool) getBucket(bucketName, label string) (*gocb.Bucket, error) {
	pool.RLock()
	defer pool.RUnlock()

	bucket, ok := pool.bucketHandle[bucketName]
	if !ok {
		return nil, fmt.Errorf("Bucket: %s is not registered by any app", bucketName)
	}
	return bucket.get(label)
}

func initGocbBucketHandle(bucketName string, cluster *gocb.Cluster, retryCount int64, restPort string) (*gocbBucketInstance, error) {
	bucketHandle := &gocbBucketInstance{
		apps: make(map[string]struct{}),
	}
	bucketNotExist := false
	hostPortAddr := net.JoinHostPort(util.Localhost(), restPort)
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &retryCount,
		gocbConnectBucket, &bucketHandle.bucketHandle,
		cluster, bucketName, hostPortAddr, &bucketNotExist)

	if bucketNotExist {
		return nil, fmt.Errorf("Bucket %s doesn't exist", bucketName)
	}
	return bucketHandle, err
}

func (bucket *gocbBucketInstance) insert(label string) {
	bucket.apps[label] = struct{}{}
}

func (bucket *gocbBucketInstance) get(label string) (*gocb.Bucket, error) {
	_, ok := bucket.apps[label]
	if !ok {
		return nil, fmt.Errorf("App %s is not registered", label)
	}
	return bucket.bucketHandle, nil
}

func (bucket *gocbBucketInstance) remove(label string) (int, error) {
	_, err := bucket.get(label)
	if err != nil {
		return len(bucket.apps), err
	}

	delete(bucket.apps, label)
	return len(bucket.apps), nil
}
