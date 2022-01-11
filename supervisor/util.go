package supervisor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/collections"
	couchbase "github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/gen/flatbuf/cfg"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/rbac"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb/v2"
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

func (s *SuperSupervisor) isDeployable(appName string) error {
	depCfg, funcScope, owner, err := s.getFuncDetails(appName)
	if err != nil {
		return err
	}

	hostAddress := net.JoinHostPort(util.Localhost(), s.restPort)
	sourceExist := util.CheckKeyspaceExist(depCfg.SourceBucket, depCfg.SourceScope,
		depCfg.SourceCollection, hostAddress)
	if !sourceExist {
		return fmt.Errorf("Source Keyspace doesn't exist")
	}

	metaExist := util.CheckKeyspaceExist(depCfg.MetadataBucket, depCfg.MetadataScope,
		depCfg.MetadataCollection, hostAddress)
	if !metaExist {
		return fmt.Errorf("Meta Keyspace doesn't exist")
	}

	if (funcScope.BucketName != "" || funcScope.ScopeName != "") && (funcScope.BucketName != "*" || funcScope.ScopeName != "*") {
		_, _, err = util.CheckAndGetBktAndScopeIDs(funcScope, s.restPort)
		if err == couchbase.ErrBucketNotFound || err == collections.SCOPE_NOT_FOUND {
			return fmt.Errorf("Function Scope doesn't exist: %v", err)
		}
	}

	if owner.User == "" && owner.Domain == "" {
		return nil
	}

	srcKeyspace := &common.Keyspace{
		BucketName:     depCfg.SourceBucket,
		ScopeName:      depCfg.SourceScope,
		CollectionName: depCfg.SourceCollection,
	}

	metadataKeyspace := &common.Keyspace{
		BucketName:     depCfg.MetadataBucket,
		ScopeName:      depCfg.MetadataScope,
		CollectionName: depCfg.MetadataCollection,
	}
	permissions := rbac.HandlerBucketPermissions(srcKeyspace, metadataKeyspace)
	permissions = append(permissions, rbac.HandlerManagePermissions(funcScope.ToKeyspace())...)
	_, err = rbac.HasPermissions(owner, permissions, true)
	// Return error only if its confirmed ErrAuthorisation or user deleted error
	// For temp error producer will undeploy it based on the error message
	if err == rbac.ErrAuthorisation || err == rbac.ErrUserDeleted {
		return err
	}
	return nil
}

func (s *SuperSupervisor) getFuncDetails(appName string) (*common.DepCfg, *common.FunctionScope, *common.Owner, error) {
	var appData []byte
	err := util.Retry(util.NewFixedBackoff(time.Second), nil, metakvAppCallback, s, MetakvAppsPath, MetakvChecksumPath, appName, &appData)
	if err == common.ErrRetryTimeout {
		return nil, nil, nil, err
	}
	config := cfg.GetRootAsConfig(appData, 0)
	depcfg := config.DepCfg(new(cfg.DepCfg))

	depCfg := &common.DepCfg{
		SourceBucket:       string(depcfg.SourceBucket()),
		SourceScope:        common.CheckAndReturnDefaultForScopeOrCollection(string(depcfg.SourceScope())),
		SourceCollection:   common.CheckAndReturnDefaultForScopeOrCollection(string(depcfg.SourceCollection())),
		MetadataBucket:     string(depcfg.MetadataBucket()),
		MetadataScope:      common.CheckAndReturnDefaultForScopeOrCollection(string(depcfg.MetadataScope())),
		MetadataCollection: common.CheckAndReturnDefaultForScopeOrCollection(string(depcfg.MetadataCollection())),
	}

	f := new(cfg.FunctionScope)
	fS := config.FunctionScope(f)
	funcScope := &common.FunctionScope{}

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

	return depCfg, funcScope, owner, nil
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
		bucketWatch.refreshBucketManifestOnUIDChange("", s.restPort)
		s.scn.RunObserveCollectionManifestChanges(bucketName)
		bucketWatch.apps = make(map[string]int)
		s.buckets[bucketName] = bucketWatch
	}
	bucketWatch.apps[appName]++

	return nil
}

func (s *SuperSupervisor) refreshBuckets() ([]string, error) {
	s.bucketsRWMutex.Lock()
	defer s.bucketsRWMutex.Unlock()
	deletedBuckets := make([]string, 0)

	for bucketName := range s.buckets {
		if err := s.buckets[bucketName].Refresh(s.retryCount, s.restPort); err != nil {
			if err == NoBucket {
				deletedBuckets = append(deletedBuckets, bucketName)
			} else {
				return nil, err
			}
		}
	}
	return deletedBuckets, nil
}

func (s *SuperSupervisor) getDeletedBucketFromPool(pool *couchbase.Pool) []string {
	s.bucketsRWMutex.Lock()
	defer s.bucketsRWMutex.Unlock()

	deletedBuckets := make([]string, 0, len(s.buckets))
	currentClusterBuckets := make(map[string]struct{})
	for _, bucketInfo := range pool.BucketList {
		currentClusterBuckets[bucketInfo.BucketName] = struct{}{}
	}

	for bucketName := range s.buckets {
		if _, ok := currentClusterBuckets[bucketName]; !ok {
			deletedBuckets = append(deletedBuckets, bucketName)
		}
	}
	return deletedBuckets
}

func (s *SuperSupervisor) RefreshBucketAndManifestInfo(bucket *couchbase.Bucket) (bool, error) {
	s.bucketsRWMutex.Lock()
	defer s.bucketsRWMutex.Unlock()

	bucketName := bucket.Name
	bucketWatch, ok := s.buckets[bucketName]
	if !ok {
		return false, nil
	}

	return bucketWatch.RefreshBucketAndManifestOnUIDChange(bucket, s.restPort)
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
		_, cid, err := s.GetScopeAndCollectionID(p.MetadataBucket(), p.MetadataScope(), p.MetadataCollection())
		if err != nil || cid != mCid {
			logging.Infof("%s Undeploying %s Reason: metadata collection delete err: %v", logPrefix, appName, err)
			p.UndeployHandler(true)
			continue
		}

		sCid := p.GetSourceCid()
		if sCid == math.MaxUint32 {
			continue
		}
		_, cid, err = s.GetScopeAndCollectionID(p.SourceBucket(), p.SourceScope(), p.SourceCollection())
		if err != nil || cid != sCid {
			logging.Infof("%s Undeploying %s Reason: source collection delete err: %v", logPrefix, appName, err)
			p.UndeployHandler(false)
		}

		_, sid := p.GetFuncScopeDetails()
		if sid == math.MaxUint32 {
			continue
		}

		currentSid, _, err := s.GetScopeAndCollectionID(p.FunctionManageBucket(), p.FunctionManageScope(), "")
		if err != nil || sid != currentSid {
			logging.Infof("%s Undeploying %s Reason: function manage scope delete err: %v", logPrefix, appName, err)
			p.UndeployHandler(false)
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
		if s.scn != nil {
			s.scn.Close()
		}
		close(delChannel)
		time.Sleep(time.Duration(s.servicesNotifierRetryTm) * time.Millisecond)
		go s.watchBucketChanges()
	}

	hostPortAddr := net.JoinHostPort(util.Localhost(), s.restPort)
	deletedBuckets, err := s.refreshBuckets()
	if err != nil {
		logging.Errorf("%s Error in bucket Refresh: %v", logPrefix, err)
		selfRestart()
		return
	}

	if len(deletedBuckets) != 0 {
		delChannel <- deletedBuckets
	}

	s.hostport = hostPortAddr
	s.pool = "default"
	s.scn, err = util.NewServicesChangeNotifier(s.hostport, s.pool)
	if err != nil {
		logging.Errorf("ClusterInfoClient NewServicesChangeNotifier(): %v\n", err)
		selfRestart()
		return
	}

	s.serviceMgr.NotifySupervisorWaitCh()

	s.bucketsRWMutex.Lock()
	for bucketName, _ := range s.buckets {
		s.scn.RunObserveCollectionManifestChanges(bucketName)
	}
	s.bucketsRWMutex.Unlock()

	ticker := time.NewTicker(time.Duration(s.servicesNotifierRetryTm) * time.Minute)
	defer ticker.Stop()

	// For observing node services config
	ch := s.scn.GetNotifyCh()
	for {
		select {
		case notif, ok := <-ch:
			if !ok {
				logging.Errorf("%s ServicesChangeNotifier channel closed. Restarting..", logPrefix)
				selfRestart()
				return
			}

			switch notif.Type {
			case util.CollectionManifestChangeNotification:
				bucket := (notif.Msg).(*couchbase.Bucket)
				changed, err := s.RefreshBucketAndManifestInfo(bucket)
				if err != nil {
					logging.Errorf("%s RefreshBucketAndManifestInfo(): %v\n", logPrefix, err)
					selfRestart()
					return
				}
				if changed {
					s.checkDeletedCid(bucket.Name)
				}

			case util.EncryptionLevelChangeNotification:
				for {
					_, refreshErr := s.refreshBuckets()
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
			case util.PoolChangeNotification:
				np := notif.Msg.(*couchbase.Pool)
				deletedBuckets := s.getDeletedBucketFromPool(np)
				if len(deletedBuckets) != 0 {
					delChannel <- deletedBuckets
				}
			default:
			}

		case <-ticker.C:
			deletedBuckets, err := s.refreshBuckets()
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
	err = s.gocbGlobalConfigHandle.maybeRegistergocbBucket(bucketName, appName, s.GetSecuritySetting(), s)
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
	// Function bucket can be nil
	if bucketName == "" || bucketName == "*" {
		return nil
	}
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
			bucketWatch.apps[appName]--
			if bucketWatch.apps[appName] <= 0 {
				delete(bucketWatch.apps, appName)
				if len(bucketWatch.apps) == 0 {
					bucketWatch.Close()
					s.scn.StopObserveCollectionManifestChanges(bucketName)
					delete(s.buckets, bucketName)
				}
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

func (s *SuperSupervisor) setinitLifecycleEncryptData() {
	if securitySetting := s.GetSecuritySetting(); securitySetting != nil {
		s.initEncryptDataMutex.Lock()
		s.initLifecycleEncryptData = securitySetting.EncryptData
		s.initEncryptDataMutex.Unlock()
	}
}

// Forceful refresh all the buckets
func (bw *bucketWatchStruct) Refresh(retryCount int64, restPort string) error {
	err := bw.b.RefreshWithTerseBucket()
	if err != nil {
		if strings.Contains(err.Error(), "HTTP error 404 Object Not Found") {
			return NoBucket
		}
		if strings.Contains(err.Error(), "Bucket uuid does not match") {
			return NoBucket
		}
	}

	bw.refreshBucketManifestOnUIDChange("", restPort)
	return err
}

// refresh both manifest and bucket
func (bw *bucketWatchStruct) RefreshBucketAndManifestOnUIDChange(bucket *couchbase.Bucket, restPort string) (bool, error) {
	bw.replaceBucket(bucket)
	muid := bucket.CollectionManifestUID
	return bw.refreshBucketManifestOnUIDChange(muid, restPort)
}

func (bw *bucketWatchStruct) replaceBucket(bucket *couchbase.Bucket) {
	bw.b.RefreshFromBucket(bucket)
}

func (bw *bucketWatchStruct) refreshBucketManifestOnUIDChange(muid, restPort string) (bool, error) {
	if muid != "" && bw.b.Manifest != nil && bw.b.Manifest.UID == muid {
		return false, nil
	}

	hostAddress := net.JoinHostPort(util.Localhost(), restPort)
	cic, err := util.FetchClusterInfoClient(hostAddress)
	if err != nil {
		return false, err
	}
	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	version, _ := cinfo.GetNodeCompatVersion()
	cinfo.RUnlock()

	if version < collections.COLLECTION_SUPPORTED_VERSION {
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
func (config *gocbGlobalConfig) maybeRegistergocbBucket(bucketName, appName string, setting *common.SecuritySetting, supervisor *SuperSupervisor) error {
	var err error
	logPrefix := "gocbGlobalConfig::maybeRegistergocbBucket"
	defer func() {
		if r := recover(); r != nil {
			// Recover from a possible panic in gocb cluster.Close() if socket on other side is already closed.
			trace := debug.Stack()
			logging.Errorf("%s Recovered from panic, stack trace: %rm", logPrefix, string(trace))
		}
	}()
	config.Lock()
	defer config.Unlock()

	encryptionEnabled := setting != nil && setting.EncryptData == true

	val, found := config.appEncryptionMap[appName]
	if found && val == encryptionEnabled { // resume with no encryption change. noop

		return nil
	}

	if encryptionEnabled == true {
		if config.encryptedgocbPool == nil {
			config.encryptedgocbPool, err = initGoCbPool(config.retrycount, config.nsServerPort, setting, supervisor)
			if err != nil {
				return fmt.Errorf("Could not create encrypted gocb cluster object. Cause: %v", err)
			}
			logging.Infof("%v Successfully created an encrypted gocb cluster", logPrefix)
		}
		if err = config.encryptedgocbPool.insertBucket(bucketName, appName, config.retrycount, config.nsServerPort, supervisor); err != nil {
			// failure doesn't add the handle. No need to call gocbpool.remove. However, there might be an unused cluster object
			logging.Errorf("%v Could not create an encrypted gocb handle. Cause: %v", logPrefix, err)
			if config.encryptedgocbPool != nil && len(config.encryptedgocbPool.bucketHandle) == 0 {
				config.encryptedgocbPool.cluster.Close(nil)
				config.encryptedgocbPool = nil
			}
			return err
		}
	} else {
		if config.plaingocbPool == nil {
			config.plaingocbPool, err = initGoCbPool(config.retrycount, config.nsServerPort, setting, supervisor)
			if err != nil {
				return fmt.Errorf("Could not create plain gocb cluster object. Cause: %v", err)
			}
			logging.Infof("%v Successfully created a plain gocb cluster", logPrefix)
		}
		if err = config.plaingocbPool.insertBucket(bucketName, appName, config.retrycount, config.nsServerPort, supervisor); err != nil {
			logging.Errorf("%v Could not create an plain gocb handle. Cause: %v", logPrefix, err)
			if config.plaingocbPool != nil && len(config.plaingocbPool.bucketHandle) == 0 {
				config.plaingocbPool.cluster.Close(nil)
				config.plaingocbPool = nil
			}
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

func (config *gocbGlobalConfig) getBucketCollection(bucketName, scopeName, collectionName, appName string) (*gocb.Collection, error) {
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
			return config.encryptedgocbPool.getBucketCollection(bucketName, scopeName, collectionName, appName)
		}
	} else {
		if config.plaingocbPool == nil {
			return nil, fmt.Errorf("Plain gocb pool hasn't been initialized yet")
		} else {
			return config.plaingocbPool.getBucketCollection(bucketName, scopeName, collectionName, appName)
		}
	}
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

func initGoCbPool(retryCount int64, restPort string, setting *common.SecuritySetting, supervisor *SuperSupervisor) (*gocbPool, error) {
	pool := &gocbPool{
		bucketHandle: make(map[string]*gocbBucketInstance),
	}
	var operr error
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &retryCount, gocbConnectCluster, supervisor, &pool.cluster, restPort, setting, &operr)
	if operr == common.ErrEncryptionLevelChanged {
		return pool, operr
	}
	return pool, err
}

func (pool *gocbPool) insertBucket(bucketName, label string, retryCount int64, restPort string, supervisor *SuperSupervisor) error {
	pool.Lock()
	defer pool.Unlock()
	bucket, ok := pool.bucketHandle[bucketName]
	if !ok {
		var err error
		bucket, err = initGocbBucketHandle(bucketName, pool.cluster, retryCount, restPort, supervisor)
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
			pool.cluster.Close(nil)
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

func (pool *gocbPool) getBucketCollection(bucketName, scopeName, collectionName, label string) (*gocb.Collection, error) {
	pool.RLock()
	defer pool.RUnlock()
	bucket, ok := pool.bucketHandle[bucketName]
	if !ok {
		return nil, fmt.Errorf("Bucket: %s is not registered by any app", bucketName)
	}

	return bucket.getCollection(scopeName, collectionName, label)
}

func initGocbBucketHandle(bucketName string, cluster *gocb.Cluster, retryCount int64, restPort string, supervisor *SuperSupervisor) (*gocbBucketInstance, error) {
	bucketHandle := &gocbBucketInstance{
		apps: make(map[string]struct{}),
	}
	bucketNotExist := false
	hostPortAddr := net.JoinHostPort(util.Localhost(), restPort)
	var operr error
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &retryCount,
		gocbConnectBucket, supervisor, &bucketHandle.bucketHandle,
		cluster, bucketName, hostPortAddr, &bucketNotExist, &operr)
	if operr == common.ErrEncryptionLevelChanged {
		return nil, operr
	}
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

func (bucket *gocbBucketInstance) getCollection(scopeName, collectionName, label string) (*gocb.Collection, error) {
	bucketHandle, err := bucket.get(label)
	if err != nil {
		return nil, err
	}
	scope := bucketHandle.Scope(scopeName)
	return scope.Collection(collectionName), nil
}
