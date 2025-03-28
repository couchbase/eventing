package supervisor

import (
	"fmt"
	mcd "github.com/couchbase/eventing/dcp/transport"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
	couchbase "github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb/v2"
)

// ClearEventStats flushes event processing stats
func (s *SuperSupervisor) ClearEventStats() []string {
	appNames := make([]string, 0)
	for appName, p := range s.runningFns() {
		appNames = append(appNames, appName)
		p.ClearEventStats()
	}
	return appNames
}

// DeployedAppList returns list of deployed lambdas running under super_supervisor
func (s *SuperSupervisor) DeployedAppList() []string {
	appList := make([]string, 0)

	for app := range s.runningFns() {
		appList = append(appList, app)
	}

	return appList
}

// GetEventProcessingStats returns dcp/timer event processing stats
func (s *SuperSupervisor) GetEventProcessingStats(appName string) map[string]uint64 {
	if p, ok := s.runningFns()[appName]; ok {
		return p.GetEventProcessingStats()
	}
	return nil
}

// GetAppCode returns handler code for requested appname
func (s *SuperSupervisor) GetAppCode(appName string) string {
	logPrefix := "SuperSupervisor::GetAppCode"

	logging.Infof("%s [%d] Function: %s request function code", logPrefix, s.runningFnsCount(), appName)
	if p, ok := s.runningFns()[appName]; ok {
		return p.GetAppCode()
	}
	return ""
}

// GetDebuggerURL returns the v8 debugger url for supplied appname
func (s *SuperSupervisor) GetDebuggerURL(appName string) (string, error) {
	logPrefix := "SuperSupervisor::GetDebuggerURL"

	logging.Debugf("%s [%d] Function: %s request for debugger URL", logPrefix, s.runningFnsCount(), appName)
	if p, ok := s.runningFns()[appName]; ok {
		return p.GetDebuggerURL()
	}

	return "", fmt.Errorf("App %s not running", appName)
}

// GetDeployedApps returns list of deployed apps and their last deployment time
func (s *SuperSupervisor) GetDeployedApps() map[string]string {
	s.appListRWMutex.RLock()
	defer s.appListRWMutex.RUnlock()

	deployedApps := make(map[string]string)
	for app, timeStamp := range s.deployedApps {
		deployedApps[app] = timeStamp
	}

	return deployedApps
}

func (s *SuperSupervisor) GetUndeployedApps() []string {
	s.appListRWMutex.RLock()
	defer s.appListRWMutex.RUnlock()

	undeployedApps := make([]string, 0)
	for appName, dStatus := range s.appDeploymentStatus {
		if pStatus, pOk := s.appProcessingStatus[appName]; pOk && !dStatus && !pStatus {
			undeployedApps = append(undeployedApps, appName)
		}
	}

	return undeployedApps
}

// GetLatencyStats dumps stats from cpp world
func (s *SuperSupervisor) GetLatencyStats(appName string) common.StatsData {
	if p, ok := s.runningFns()[appName]; ok {
		return p.GetLatencyStats()
	}
	return nil
}

func (s *SuperSupervisor) GetCurlLatencyStats(appName string) common.StatsData {
	if p, ok := s.runningFns()[appName]; ok {
		return p.GetCurlLatencyStats()
	}
	return nil
}

func (s *SuperSupervisor) GetInsight(appName string) *common.Insight {
	logPrefix := "SuperSupervisor::GetInsight"
	if p, ok := s.runningFns()[appName]; ok {
		if insight := p.GetInsight(); insight != nil {
			logging.Debugf("%s [%d] Function: %s insight is %ru", logPrefix, s.runningFnsCount(), appName, insight)
			return insight
		}
	}
	return common.NewInsight() // empty if error
}

// GetLocallyDeployedApps returns list of deployed apps and their last deployment time
func (s *SuperSupervisor) GetLocallyDeployedApps() map[string]string {
	s.appListRWMutex.RLock()
	defer s.appListRWMutex.RUnlock()

	locallyDeployedApps := make(map[string]string)
	for app, timeStamp := range s.locallyDeployedApps {
		locallyDeployedApps[app] = timeStamp
	}

	return locallyDeployedApps
}

// GetExecutionStats returns aggregated failure stats from Eventing.Producer instance
func (s *SuperSupervisor) GetExecutionStats(appName string) map[string]interface{} {
	if p, ok := s.runningFns()[appName]; ok {
		return p.GetExecutionStats()
	}
	return nil
}

// GetFailureStats returns aggregated failure stats from Eventing.Producer instance
func (s *SuperSupervisor) GetFailureStats(appName string) map[string]interface{} {
	if p, ok := s.runningFns()[appName]; ok {
		return p.GetFailureStats()
	}
	return nil
}

// GetLcbExceptionsStats returns libcouchbase exception stats from CPP workers
func (s *SuperSupervisor) GetLcbExceptionsStats(appName string) map[string]uint64 {
	p, ok := s.runningFns()[appName]
	if ok {
		return p.GetLcbExceptionsStats()
	}
	return nil
}

// GetSeqsProcessed returns vbucket specific sequence nos processed so far
func (s *SuperSupervisor) GetSeqsProcessed(appName string) map[int]int64 {
	if p, ok := s.runningFns()[appName]; ok {
		return p.GetSeqsProcessed()
	}
	return nil
}

// Gets the cluster url and pool supervisor is observing ns_server changes on.
func (s *SuperSupervisor) GetRegisteredPool() string {
	return s.hostport + "-" + s.pool
}

// RestPort returns ns_server port(typically 8091/9000)
func (s *SuperSupervisor) RestPort() string {
	return s.restPort
}

func (s *SuperSupervisor) ResetCounters(appName string) error {
	p, ok := s.runningFns()[appName]
	if ok {
		p.ResetCounters()
		return nil
	}
	return fmt.Errorf("No function named %v registered by supervisor")
}

// SignalStopDebugger stops V8 Debugger for a specific deployed lambda
func (s *SuperSupervisor) SignalStopDebugger(appName string) error {
	p, ok := s.runningFns()[appName]
	if !ok {
		return fmt.Errorf("Producer %s not running", appName)
	}
	err := p.SignalStopDebugger()
	if err != nil {
		return err
	}
	return nil
}

// GetAppCompositeState returns current state of app
func (s *SuperSupervisor) GetAppCompositeState(appName string) int8 {
	s.appRWMutex.RLock()
	defer s.appRWMutex.RUnlock()

	dStatus := s.appDeploymentStatus[appName]
	pStatus := s.appProcessingStatus[appName]
	return common.GetCompositeState(dStatus, pStatus)
}

// GetDcpEventsRemainingToProcess returns remaining dcp events to process
func (s *SuperSupervisor) GetDcpEventsRemainingToProcess(appName string) uint64 {
	logPrefix := "SuperSupervisor::GetDcpEventsRemainingToProcess"

	p, ok := s.runningFns()[appName]
	if ok {
		return p.GetDcpEventsRemainingToProcess()
	}
	logging.Errorf("%s [%d] Function: %s request didn't go through as Eventing.Producer instance isn't alive",
		logPrefix, s.runningFnsCount(), appName)
	return 0
}

// VbDcpEventsRemainingToProcess returns remaining dcp events to process
func (s *SuperSupervisor) VbDcpEventsRemainingToProcess(appName string) map[int]int64 {
	logPrefix := "SuperSupervisor::VbDcpEventsRemainingToProcess"

	p, ok := s.runningFns()[appName]
	if ok {
		return p.VbDcpEventsRemainingToProcess()
	}
	logging.Errorf("%s [%d] Function: %s request didn't go through as Eventing.Producer instance isn't alive",
		logPrefix, s.runningFnsCount(), appName)
	return nil
}

// GetEventingConsumerPids returns map of Eventing.Consumer worker name and it's os pid
func (s *SuperSupervisor) GetEventingConsumerPids(appName string) map[string]int {
	logPrefix := "SuperSupervisor::GetEventingConsumerPids"

	p, ok := s.runningFns()[appName]
	if ok {
		return p.GetEventingConsumerPids()
	}
	logging.Errorf("%s [%d] Function: %s request didn't go through as Eventing.Producer instance isn't alive",
		logPrefix, s.runningFnsCount(), appName)
	return nil
}

// Last ditch effort to kill all consumers
func (s *SuperSupervisor) KillAllConsumers() {
	for _, p := range s.runningProducers {
		p.KillAllConsumers()
	}
}

// InternalVbDistributionStats returns internal state of vbucket ownership distribution on local eventing node
func (s *SuperSupervisor) InternalVbDistributionStats(appName string) map[string]string {
	p, ok := s.runningFns()[appName]
	if ok {
		return p.InternalVbDistributionStats()
	}

	return nil
}

// VbDistributionStatsFromMetadata returns vbucket distribution across eventing nodes from metadata bucket
func (s *SuperSupervisor) VbDistributionStatsFromMetadata(appName string) map[string]map[string]string {
	p, ok := s.runningFns()[appName]
	if ok {
		return p.VbDistributionStatsFromMetadata()
	}

	return nil
}

// PlannerStats returns vbucket distribution as per planner running on local eventing
// node for a given app
func (s *SuperSupervisor) PlannerStats(appName string) []*common.PlannerNodeVbMapping {
	p, ok := s.runningFns()[appName]
	if ok {
		return p.PlannerStats()
	}

	return nil
}

// RebalanceTaskProgress reports vbuckets remaining to be transferred as per planner
// during the course of rebalance
func (s *SuperSupervisor) RebalanceTaskProgress(appName string) (*common.RebalanceProgress, error) {
	p, ok := s.runningFns()[appName]
	if ok {
		return p.RebalanceTaskProgress(), nil
	}

	_, err := s.isFnRunningFromPrimary(appName)
	if err != nil {
		return nil, err
	}

	progress := &common.RebalanceProgress{}

	// report rebalance progress for yet-to-bootstrap-apps only if node is still part of cluster
	if s.checkIfNodeInCluster() {
		progress.VbsRemainingToShuffle = 1
	}

	return progress, nil
}

// TimerDebugStats captures timer related stats to assist in debugging mismtaches during rebalance
func (s *SuperSupervisor) TimerDebugStats(appName string) (map[int]map[string]interface{}, error) {
	p, ok := s.runningFns()[appName]
	if ok {
		return p.TimerDebugStats(), nil
	}

	return nil, fmt.Errorf("Eventing.Producer isn't alive")
}

// BootstrapAppStatus reports back status of bootstrap for a particular app on current node
func (s *SuperSupervisor) BootstrapAppStatus(appName string) bool {
	logPrefix := "SuperSupervisor::BootstrapAppStatus"

	producer, ok := s.runningFns()[appName]

	if ok {
		bootstrapStatus := producer.BootstrapStatus()
		if bootstrapStatus {
			logging.Infof("%s [%d] Bootstrap status from %s: %#v", logPrefix, s.runningFnsCount(), appName, bootstrapStatus)
		}
		return bootstrapStatus
	}

	return false
}

// BootstrapStatus reports back status of bootstrap for all running apps on current node
func (s *SuperSupervisor) BootstrapStatus() bool {
	logPrefix := "SuperSupervisor::BootstrapStatus"

	bootstrapStatuses := make(map[string]bool)
	for appName, p := range s.runningFns() {
		bootstrapStatuses[appName] = p.BootstrapStatus()
	}

	logging.Infof("%s [%d] Bootstrap status from all running applications: %#v", logPrefix, s.runningFnsCount(), bootstrapStatuses)

	for _, bootstrapStatus := range bootstrapStatuses {
		if bootstrapStatus {
			return bootstrapStatus
		}
	}

	return false
}

// RebalanceStatus reports back status of rebalance for all running apps on current node
func (s *SuperSupervisor) RebalanceStatus() bool {
	logPrefix := "SuperSupervisor::RebalanceStatus"

	if atomic.LoadInt32(&s.isRebalanceOngoing) == 1 {
		return true
	}

	rebalanceStatuses := make(map[string]bool)
	for appName, p := range s.runningFns() {
		rebalanceStatuses[appName] = p.RebalanceStatus()
	}

	logging.Infof("%s [%d] Rebalance status from all running applications: %#v",
		logPrefix, s.runningFnsCount(), rebalanceStatuses)

	for _, rebStatus := range rebalanceStatuses {
		if rebStatus {
			return rebStatus
		}
	}

	return false
}

// PausingAppList returns list of apps which are being Paused
func (s *SuperSupervisor) PausingAppList() map[string]string {
	logPrefix := "SuperSupervisor::PausingAppList"

	pausingApps := make(map[string]string)

	s.appListRWMutex.RLock()
	defer s.appListRWMutex.RUnlock()

	for appName, ts := range s.pausingApps {
		pausingApps[appName] = ts
	}

	if len(pausingApps) != 0 {
		logging.Infof("%s [%d] pausingApps: %+v", logPrefix, s.runningFnsCount(), pausingApps)
	}

	return pausingApps
}

// BootstrapAppList returns list of apps undergoing bootstrap
func (s *SuperSupervisor) BootstrapAppList() map[string]string {
	logPrefix := "SuperSupervisor::BootstrapAppList"

	bootstrappingApps := make(map[string]string)

	s.appListRWMutex.RLock()
	defer s.appListRWMutex.RUnlock()

	for appName, ts := range s.bootstrappingApps {
		bootstrappingApps[appName] = ts
	}

	if len(bootstrappingApps) > 0 {
		logging.Infof("%s [%d] bootstrappingApps: %+v", logPrefix, s.runningFnsCount(), bootstrappingApps)
	}

	return bootstrappingApps
}

// VbSeqnoStats returns seq no stats, which can be useful in figuring out missed events during rebalance
func (s *SuperSupervisor) VbSeqnoStats(appName string) (map[int][]map[string]interface{}, error) {
	p, ok := s.runningFns()[appName]
	if ok {
		return p.VbSeqnoStats(), nil
	}

	return nil, fmt.Errorf("Eventing.Producer isn't alive")
}

// RemoveProducerToken takes out appName from supervision tree
func (s *SuperSupervisor) RemoveProducerToken(appName string) {
	if p, exists := s.runningFns()[appName]; exists {
		s.stopAndDeleteProducer(p)
	}
}

// CheckpointBlobDump returns state of metadata blobs stored in Couchbase bucket
func (s *SuperSupervisor) CheckpointBlobDump(appName string) (interface{}, error) {
	p, ok := s.runningFns()[appName]
	if ok {
		return p.CheckpointBlobDump(), nil
	}

	return nil, fmt.Errorf("Eventing.Producer isn't alive")
}

// StopProducer tries to gracefully stop running producer instance for a function
func (s *SuperSupervisor) StopProducer(appName string, msg common.UndeployAction) {
	logPrefix := "SuperSupervisor::StopProducer"

	if swapped := s.checkAndSwapStatus(appName, false, false); !swapped {
		// Some other routine must have undeployed this app
		return
	}

	logging.Infof("%s [%d] Function: %s stopping running producer instance, %s",
		logPrefix, s.runningFnsCount(), appName, msg)

	if p, ok := s.runningFns()[appName]; ok {
		if p.GetCursorAware() {
			sourceKeyspace := common.KeyspaceName{
				Bucket:     p.SourceBucket(),
				Scope:      p.SourceScope(),
				Collection: p.SourceCollection(),
			}
			fiid := p.GetFunctionInstanceId()
			s.cursorRegistry.Unregister(sourceKeyspace, fiid)
			logging.Infof("%s [%d] Attempting to unregister function: %s with cursorId: %s and keyspace: %v",
				logPrefix, s.runningFnsCount(), appName, fiid, sourceKeyspace)
		}
	}

	s.deleteFromLocallyDeployedApps(appName)

	s.removePauseTimestampDoc(appName)

	s.cleanupProducer(appName, msg)
	s.deleteFromDeployedApps(appName)

	s.metadataHandleMutex.Lock()
	defer s.metadataHandleMutex.Unlock()
	delete(s.appToMetadataHandle, appName)
}

func (s *SuperSupervisor) addToDeployedApps(appName string) {
	logPrefix := "SuperSupervisor::addToDeployedApps"
	s.appListRWMutex.Lock()
	defer s.appListRWMutex.Unlock()
	logging.Infof("%s [%d] Function: %s adding to deployed apps map", logPrefix, s.runningFnsCount(), appName)
	s.deployedApps[appName] = time.Now().String()
}

func (s *SuperSupervisor) addToLocallyDeployedApps(appName string) {
	logPrefix := "SuperSupervisor::addToLocallyDeployedApps"
	s.appListRWMutex.Lock()
	defer s.appListRWMutex.Unlock()
	logging.Infof("%s [%d] Function: %s adding to locally deployed apps map", logPrefix, s.runningFnsCount(), appName)
	s.locallyDeployedApps[appName] = time.Now().String()
}

func (s *SuperSupervisor) deleteFromDeployedApps(appName string) {
	logPrefix := "SuperSupervisor::deleteFromDeployedApps"
	s.appListRWMutex.Lock()
	defer s.appListRWMutex.Unlock()
	logging.Infof("%s [%d] Function: %s deleting from deployed apps map", logPrefix, s.runningFnsCount(), appName)
	delete(s.deployedApps, appName)
}

func (s *SuperSupervisor) deleteFromLocallyDeployedApps(appName string) {
	logPrefix := "SuperSupervisor::deleteFromLocallyDeployedApps"
	s.appListRWMutex.Lock()
	defer s.appListRWMutex.Unlock()
	logging.Infof("%s [%d] Function: %s deleting from locally deployed apps map", logPrefix, s.runningFnsCount(), appName)
	delete(s.locallyDeployedApps, appName)
}

// GetMetaStoreStats returns metastore related stats from all running functions on current node
func (s *SuperSupervisor) GetMetaStoreStats(appName string) map[string]uint64 {
	stats := make(map[string]uint64)
	if p, ok := s.runningFns()[appName]; ok {
		stats = p.GetMetaStoreStats()
	}
	return stats
}

// WriteDebuggerToken signals running function to write debug token
func (s *SuperSupervisor) WriteDebuggerToken(appName, token string, hostnames []string) {
	logPrefix := "SuperSupervisor::WriteDebuggerToken"

	p, exists := s.runningFns()[appName]
	if !exists {
		logging.Errorf("%s [%d] Function %s not found", logPrefix, s.runningFnsCount(), appName)
		return
	}
	p.WriteDebuggerToken(token, hostnames)
}

// WriteDebuggerURL signals running function to write debug url
func (s *SuperSupervisor) WriteDebuggerURL(appName, url string) {
	logPrefix := "SuperSupervisor::WriteDebuggerURL"

	p, exists := s.runningFns()[appName]
	if !exists {
		logging.Errorf("%s [%d] Function %s not found", logPrefix, s.runningFnsCount(), appName)
		return
	}
	p.WriteDebuggerURL(url)
}

func (s *SuperSupervisor) runningFnsCount() int {
	s.runningProducersRWMutex.RLock()
	defer s.runningProducersRWMutex.RUnlock()
	return len(s.runningProducers)
}

func (s *SuperSupervisor) runningFns() map[string]common.EventingProducer {
	runningFns := make(map[string]common.EventingProducer)

	s.runningProducersRWMutex.RLock()
	defer s.runningProducersRWMutex.RUnlock()
	for k, v := range s.runningProducers {
		runningFns[k] = v
	}

	return runningFns
}

func (s *SuperSupervisor) deleteFromRunningProducers(appName string) {
	logPrefix := "SuperSupervisor::deleteFromRunningProducers"

	s.runningProducersRWMutex.Lock()
	delete(s.runningProducers, appName)
	s.runningProducersRWMutex.Unlock()

	logging.Infof("%s [%d] Function: %s deleted from running functions", logPrefix, s.runningFnsCount(), appName)
}

func (s *SuperSupervisor) addToRunningProducers(appName string, p common.EventingProducer) {
	logPrefix := "SuperSupervisor::addToRunningProducers"

	s.runningProducersRWMutex.Lock()
	s.runningProducers[appName] = p
	s.runningProducersRWMutex.Unlock()

	logging.Infof("%s [%d] Function: %s added to running functions", logPrefix, s.runningFnsCount(), appName)
}

// SpanBlobDump returns state of timer span blobs stored in metadata bucket
func (s *SuperSupervisor) SpanBlobDump(appName string) (interface{}, error) {
	p, ok := s.runningFns()[appName]
	if ok {
		return p.SpanBlobDump(), nil
	}

	return nil, fmt.Errorf("Eventing.Producer isn't alive")
}

func (s *SuperSupervisor) addToCleanupApps(appName string) {
	logPrefix := "SuperSupervisor::addToCleanupApps"

	s.Lock()
	defer s.Unlock()

	_, ok := s.cleanedUpAppMap[appName]
	if !ok {
		s.cleanedUpAppMap[appName] = struct{}{}
		logging.Infof("%s [%d] Function: %s added", logPrefix, s.runningFnsCount(), appName)
	}
}

func (s *SuperSupervisor) deleteFromCleanupApps(appName string) {
	logPrefix := "SuperSupervisor::deleteFromCleanupApps"

	s.Lock()
	defer s.Unlock()
	delete(s.cleanedUpAppMap, appName)

	logging.Infof("%s [%d] Function: %s deleted", logPrefix, s.runningFnsCount(), appName)
}

// DcpFeedBoundary returns feed boundary used for vb dcp streams
func (s *SuperSupervisor) DcpFeedBoundary(fnName string) (string, error) {
	p, ok := s.runningFns()[fnName]
	if ok {
		return p.DcpFeedBoundary(), nil
	}

	return "", fmt.Errorf("Eventing.Producer isn't alive")
}

// GetAppLog returns tail of app log
func (s *SuperSupervisor) GetAppLog(fnName string, sz int64) []string {
	p, ok := s.runningFns()[fnName]
	if !ok {
		logging.Infof("Could not find app %v to tail", fnName)
		return nil
	}
	return p.GetAppLog(sz)

}

// GetBucket returns the bucket to the caller
func (s *SuperSupervisor) GetBucket(bucketName, appName string) (*couchbase.Bucket, error) {
	s.bucketsRWMutex.Lock()
	defer s.bucketsRWMutex.Unlock()
	if bucketWatch, ok := s.buckets[bucketName]; ok {
		if _, ok := bucketWatch.apps[appName]; !ok {
			return nil, fmt.Errorf("Function: %s is no longer watching bucket: %s", appName, bucketName)
		}
		return bucketWatch.b, nil
	}

	return nil, fmt.Errorf("Function: %s requested bucket: %s is not in watch list", appName, bucketName)
}

func (s *SuperSupervisor) GetGocbHandle(bucketName, appName string) (*gocb.Bucket, error) {
	return s.gocbGlobalConfigHandle.getBucket(bucketName, appName)
}

func (s *SuperSupervisor) CheckAndSwitchgocbBucket(bucketName, appName string, setting *common.SecuritySetting) error {
	return s.gocbGlobalConfigHandle.maybeRegistergocbBucket(bucketName, appName, setting, s)
}

func (s *SuperSupervisor) GetCurrentManifestId(bucketName string) (string, error) {
	s.bucketsRWMutex.Lock()
	defer s.bucketsRWMutex.Unlock()
	bucketWatch, ok := s.buckets[bucketName]
	if !ok {
		return "0", common.BucketNotWatched
	}
	return bucketWatch.GetManifestId(), nil
}

// Empty collectionName returns collection id as 0
// Caller of this functions should take care of it
func (s *SuperSupervisor) GetKeyspaceID(bucketName, scopeName, collectionName string) (keyspaceID common.KeyspaceID, err error) {
	s.bucketsRWMutex.RLock()
	defer s.bucketsRWMutex.RUnlock()
	bucketWatch, ok := s.buckets[bucketName]
	if !ok {
		err = common.BucketNotWatched
		return
	}

	keyspaceID.Bid = bucketWatch.b.UUID
	manifest := bucketWatch.b.Manifest
	if manifest == nil {
		return
	}

	if scopeName == "*" && collectionName == "*" {
		keyspaceID.StreamType = common.STREAM_BUCKET
		return
	}

	if collectionName == "*" {
		collectionName = ""
		keyspaceID.StreamType = common.STREAM_SCOPE
	} else {
		keyspaceID.StreamType = common.STREAM_COLLECTION
	}

	sid, cid, err := manifest.GetScopeAndCollectionID(scopeName, collectionName)
	if err != nil {
		return keyspaceID, err
	}

	keyspaceID.Sid = sid
	keyspaceID.Cid = cid
	return keyspaceID, nil
}

func (s *SuperSupervisor) GetMetadataHandle(bucketName, scopeName, collectionName, appName string) (*gocb.Collection, error) {
	return s.gocbGlobalConfigHandle.getBucketCollection(bucketName, scopeName, collectionName, appName)
}

func (s *SuperSupervisor) IncWorkerRespawnedCount() {
	atomic.AddUint32(&s.workerRespawnedCount, 1)
}

func (s *SuperSupervisor) WorkerRespawnedCount() uint32 {
	return atomic.LoadUint32(&s.workerRespawnedCount)
}

func (s *SuperSupervisor) CheckLifeCycleOpsDuringRebalance() bool {
	return s.serviceMgr.CheckLifeCycleOpsDuringRebalance()
}

// SetSecuritySetting Sets the new security settings and returns whether reload is required or not
func (s *SuperSupervisor) SetSecuritySetting(setting *common.SecuritySetting) bool {
	defer func() {
		s.securityMutex.Unlock()
		if setting != nil {
			// Push encryption change notifications to all consumers of all producers
			s.UpdateEncryptionLevel(setting.DisableNonSSLPorts, setting.EncryptData)
		}
	}()
	s.securityMutex.Lock()
	if s.securitySetting != nil {
		// TODO: 7.0.1 Change return value based on EncryptData and DisableNonSSLPorts since both can change
		if s.securitySetting.EncryptData == false && setting.EncryptData == false {
			s.securitySetting = setting
			return false
		}
		s.securitySetting = setting
		return true
	}
	s.securitySetting = setting
	return false
}

func (s *SuperSupervisor) GetSecuritySetting() *common.SecuritySetting {
	s.securityMutex.RLock()
	defer s.securityMutex.RUnlock()
	return s.securitySetting
}

func (s *SuperSupervisor) EncryptionChangedDuringLifecycle() bool {
	s.initEncryptDataMutex.RLock()
	initencryptData := s.initLifecycleEncryptData
	defer s.initEncryptDataMutex.RUnlock()

	currentencryptData := false
	if securitySetting := s.GetSecuritySetting(); securitySetting != nil {
		currentencryptData = securitySetting.EncryptData
	}

	if initencryptData != currentencryptData {
		return true
	}
	return false
}

func (s *SuperSupervisor) GetGocbSubscribedApps(encryptionEnabled bool) map[string]struct{} {
	apps := make(map[string]struct{})
	s.gocbGlobalConfigHandle.RLock()
	defer s.gocbGlobalConfigHandle.RUnlock()
	for appName, appencrypted := range s.gocbGlobalConfigHandle.appEncryptionMap {
		if appencrypted == encryptionEnabled {
			apps[appName] = struct{}{}
		}
	}
	return apps
}

func (s *SuperSupervisor) GetBSCSnapshot() (map[string]map[string][]string, error) {
	hostAddress := net.JoinHostPort(util.Localhost(), s.restPort)
	cic, err := util.FetchClusterInfoClient(hostAddress)
	if err != nil {
		return nil, err
	}
	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	snapshot := make(map[string]map[string][]string)
	buckets := cinfo.GetBuckets()
	for _, bucketName := range buckets {
		scopeList := cinfo.GetScopes(bucketName)
		if scopeList == nil {
			snapshot[bucketName] = make(map[string][]string)
			continue
		}
		snapshot[bucketName] = scopeList
	}
	return snapshot, nil
}

func (s *SuperSupervisor) GetNumVbucketsForBucket(bucketName string) int {
	hostAddress := net.JoinHostPort(util.Localhost(), s.restPort)
	cic, err := util.FetchClusterInfoClient(hostAddress)
	if err != nil {
		return 1024
	}
	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	return cinfo.GetNumVbucketsForBucket(bucketName)
}

func (s *SuperSupervisor) UpdateEncryptionLevel(enforceTLS, encryptOn bool) {
	for _, p := range s.runningFns() {
		p.UpdateEncryptionLevel(enforceTLS, encryptOn)
	}
}

func (s *SuperSupervisor) GetSystemMemoryQuota() float64 {
	return s.systemMemLimit * memQuotaThreshold
}

func (s *SuperSupervisor) ReadOnDeployDoc(appName string) (nodeLeader, restPort, onDeployStatus string) {
	return s.readOnDeployDoc(appName)
}

func (s *SuperSupervisor) RemoveOnDeployLeader(appName string) {
	s.removeOnDeployLeader(appName)
}

func (s *SuperSupervisor) WritePauseTimestamp(appName string, timestamp time.Time) {
	s.writePauseTimestamp(appName, timestamp)
}

func (s *SuperSupervisor) RemovePauseTimestampDoc(appName string) {
	s.removePauseTimestampDoc(appName)
}

func (s *SuperSupervisor) PublishOnDeployStatus(appName string, stat string) {
	const logPrefix string = "SuperSupervisor::PublishOnDeployStatus"

	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}
	mutateIn := []gocb.MutateInSpec{gocb.UpsertSpec("on_deploy_status", stat, upsertOptions)}

	s.metadataHandleMutex.RLock()
	defer s.metadataHandleMutex.RUnlock()

	metadataHandle, ok := s.appToMetadataHandle[appName]
	if !ok {
		logging.Errorf("%s [%s] Failed to fetch metadata handle", logPrefix, s.uuid)
		return
	}

	_, err := metadataHandle.MutateIn(appName+"::onDeployLeader", mutateIn, &gocb.MutateInOptions{PreserveExpiry: true})
	if err != nil {
		logging.Errorf("%s Could not update OnDeploy status: %v", logPrefix, err)
	}
}

func (s *SuperSupervisor) WriteOnDeployMsgBuffer(appName, msg string) {
	s.onDeployMsgBuffer[appName] = append(s.onDeployMsgBuffer[appName], msg)
}

func (s *SuperSupervisor) GetOnDeployMsgBuffer(appName string) []string {
	return s.onDeployMsgBuffer[appName]
}

func (s *SuperSupervisor) ClearOnDeployMsgBuffer(appName string) {
	s.onDeployMsgBuffer[appName] = make([]string, 0)
}

func (s *SuperSupervisor) GetOnDeployStatus(appName string) common.OnDeployState {
	if val, ok := s.onDeployStatus[appName]; ok {
		return val
	}
	return common.PENDING
}

func (s *SuperSupervisor) GetPreviousOnDeployStatus(appName string) common.OnDeployState {
	if val, ok := s.prevOnDeployStatus[appName]; ok {
		return val
	}
	return common.PENDING
}

func (s *SuperSupervisor) UpdateFailedOnDeployStatus(appName string) {
	s.updateFailedOnDeployStatus(appName)
}

// CleanupOnDeployTimers removes the timer related documents from the metadata bucket of the eventing function during OnDeploy failure
func (s *SuperSupervisor) CleanupOnDeployTimers(appName string, skipCheckpointBlobs bool) error {
	const logPrefix string = "SuperSupervisor::CleanupOnDeployTimers"

	_, metadataKeyspace, _, _ := s.getSourceMetaAndFunctionKeySpaces(appName)

	hostAddress := net.JoinHostPort(util.Localhost(), s.gocbGlobalConfigHandle.nsServerPort)

	metaBucketNodeCount := util.CountActiveKVNodes(metadataKeyspace.BucketName, hostAddress)
	if metaBucketNodeCount == 0 {
		logging.Infof("%s [%s] MetaBucketNodeCount: %d exiting",
			logPrefix, appName, metaBucketNodeCount)
		return nil
	}

	eventingNodeAddr, err := util.CurrentEventingNodeAddress(hostAddress)
	if err != nil {
		logging.Errorf("%s [%s] Failed to get address for current eventing node, err: %v",
			logPrefix, appName, err)
		return err
	}

	numVbuckets := s.GetNumVbucketsForBucket(metadataKeyspace.BucketName)
	vbsToCleanup := make([]uint16, 0, numVbuckets)
	for i := 0; i < numVbuckets; i++ {
		vbsToCleanup = append(vbsToCleanup, uint16(i))
	}

	logging.Infof("%s [%s] Eventing node: %s vbs to cleanup len: %d dump: %s",
		logPrefix, appName, eventingNodeAddr, len(vbsToCleanup), util.Condense(vbsToCleanup))

	undeployRoutineCount := util.CPUCount(true)
	vbsDistribution := util.VbucketNodeAssignment(vbsToCleanup, undeployRoutineCount)

	s.CheckAndSwitchgocbBucket(metadataKeyspace.BucketName, appName, s.GetSecuritySetting())
	err = s.updateMetadataHandle(appName, metadataKeyspace)
	if err != nil {
		logging.Warnf("%s [%s] Failed to refresh meta data handle during cleanup, using the existing handle. Err: %v", logPrefix, appName, err)
	}

	var undeployWG sync.WaitGroup
	undeployWG.Add(undeployRoutineCount)

	for i := 0; i < undeployRoutineCount; i++ {
		go s.CleanupOnDeployTimersImpl(i, vbsDistribution[i], &undeployWG, skipCheckpointBlobs, appName)
	}

	undeployWG.Wait()
	return nil
}

func (s *SuperSupervisor) CleanupOnDeployTimersImpl(id int, vbsToCleanup []uint16, undeployWG *sync.WaitGroup, skipCheckpointBlobs bool, appName string) error {
	const logPrefix string = "SuperSupervisor::CleanupOnDeployTimersImpl"
	defer undeployWG.Done()

	sort.Sort(util.Uint16Slice(vbsToCleanup))
	logging.Infof("%s [%s:id_%d] vbs to cleanup len: %d dump: %s",
		logPrefix, appName, id, len(vbsToCleanup), util.Condense(vbsToCleanup))

	_, metadataKeyspace, _, _ := s.getSourceMetaAndFunctionKeySpaces(appName)
	metaKeyspaceID, err := s.GetKeyspaceID(metadataKeyspace.BucketName, metadataKeyspace.ScopeName, metadataKeyspace.CollectionName)
	if err != nil {
		logging.Errorf("%s [%s] metadata bucket, scope or collection not found %v", logPrefix, appName, err)
		return err
	}
	identity, _ := common.GetIdentityFromLocation(appName)
	functionID, err := s.getFunctionId(identity)
	if err == util.AppNotExist {
		return err
	}

	err = util.Retry(util.NewFixedBackoff(time.Second), &s.retryCount, getKVNodesAddressesOpCallback, s, metadataKeyspace.BucketName, appName)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:id_%d] Exiting due to timeout", logPrefix, appName, id)
		return err
	}

	b, err := s.GetBucket(metadataKeyspace.BucketName, appName)
	if err != nil {
		logging.Errorf("%s appName: %s Error in getting metadata bucket %s err: %s", logPrefix, appName, metadataKeyspace.BucketName, err)
		return err
	}

	dcpConfig, _ := s.getDCPconfig(appName)

	var dcpFeed *couchbase.DcpFeed
	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &s.retryCount, initCleanStaleTimersCallback, s, &b, &dcpFeed, id, appName, dcpConfig)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:id_%d] Exiting due to timeout", logPrefix, appName, id)
		return err
	}

	logging.Infof("%s [%s:id_%d] Started up dcpfeed to cleanup artifacts from metadata bucket: %s",
		logPrefix, appName, id, metadataKeyspace.BucketName)

	nsServerHostPort := net.JoinHostPort(util.Localhost(), s.gocbGlobalConfigHandle.nsServerPort)
	var vbSeqnos []uint64
	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &s.retryCount, util.GetSeqnos, nsServerHostPort,
		// No need to protect metaKeyspaceID since its updated only once
		"default", metadataKeyspace.BucketName, metaKeyspaceID, &vbSeqnos, false)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:id_%d] Exiting due to timeout", logPrefix, appName, id)
		return err
	}

	vbSeqNos := make(map[uint16]uint64)
	for vb, seqNo := range vbSeqnos {
		vbSeqNos[uint16(vb)] = seqNo
	}

	cleanupVbs := make(map[uint16]bool)
	for _, vb := range vbsToCleanup {
		cleanupVbs[vb] = true
	}

	vbs := make([]uint16, 0)
	for vb := range vbSeqNos {
		if cleanupVbs[vb] {
			vbs = append(vbs, vb)
		}
	}

	sort.Sort(util.Uint16Slice(vbs))

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	rw := &sync.RWMutex{}
	receivedVbSeqNos := make(map[uint16]uint64)

	go func(b *couchbase.Bucket, dcpFeed *couchbase.DcpFeed, wg *sync.WaitGroup,
		receivedVbSeqNos map[uint16]uint64, rw *sync.RWMutex) {
		defer wg.Done()

		prefix := common.NewKey(s.getUserPrefix(appName), fmt.Sprint(functionID), "").GetPrefix()
		var operr error
		for {
			select {
			case e, ok := <-dcpFeed.C:
				if ok == false {
					logging.Infof("%s [%s:id_%d] Exiting metadata cleanup routine, mutations till high vb seqnos received",
						logPrefix, appName, id)
					return
				}

				rw.Lock()
				receivedVbSeqNos[e.VBucket] = e.Seqno
				rw.Unlock()

				switch e.Opcode {
				case mcd.DCP_MUTATION:
					docID := string(e.Key)
					if strings.HasPrefix(docID, prefix) {
						if skipCheckpointBlobs && strings.Contains(docID, "::vb::") {
							continue
						}
						err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &s.retryCount, deleteOpCallback, s, docID, &operr, appName, metadataKeyspace.BucketName)
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:id_%d] Exiting due to timeout",
								logPrefix, appName, id)
							return
						} else if operr == common.ErrEncryptionLevelChanged {
							continue
						}
					}

				case mcd.DCP_STREAMREQ:
					if e.Status != mcd.SUCCESS {
						logging.Infof("%s [%s:id_%d] vb: %d STREAMREQ wasn't successful. feed: %s status: %v",
							logPrefix, appName, id, e.VBucket, dcpFeed.GetName(), e.Status)

						rw.Lock()
						// Setting it to high value to bail out routine waiting for
						// received_seq_no >= high_seq_no_at_feed_spawn
						receivedVbSeqNos[e.VBucket] = uint64(0xFFFFFFFFFFFFFFFF)
						rw.Unlock()
					}

				case mcd.DCP_STREAMEND:
					logging.Infof("%s [%s:id_%d] vb: %d got STREAMEND, feed: %s",
						logPrefix, appName, id, e.VBucket, dcpFeed.GetName())

					rw.Lock()
					receivedVbSeqNos[e.VBucket] = uint64(0xFFFFFFFFFFFFFFFF)
					rw.Unlock()

				case mcd.DCP_SYSTEM_EVENT, mcd.DCP_SEQNO_ADVANCED:

				}
			}
		}
	}(b, dcpFeed, &wg, receivedVbSeqNos, rw)

	var flogs couchbase.FailoverLog
	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &s.retryCount, getFailoverLogOpCallback, appName, &b, &flogs, vbs, dcpConfig)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:id_%d] Exiting due to timeout", logPrefix, appName, id)
		dcpFeed.Close()
		return err
	}
	keyspaceExist := true

	logging.Infof("%s [%s:id_%d] Going to start DCP streams from metadata bucket: %s, vbs len: %d dump: %s",
		logPrefix, appName, id, metadataKeyspace.BucketName, len(vbs), util.Condense(vbs))

	for vb, flog := range flogs {
		if !cleanupVbs[vb] {
			continue
		}

		vbuuid, _, _ := flog.Latest()

		logging.Debugf("%s [%s:id_%d] vb: %d starting DCP feed",
			logPrefix, appName, id, vb)

		util.Retry(util.NewFixedBackoff(time.Second), &s.retryCount, openDcpStreamFromZero, dcpFeed, vb, vbuuid, s, id, vbSeqNos[vb], &keyspaceExist, metadataKeyspace, appName)
		if !keyspaceExist {
			// No need to update receivedVbSeqNos since metadata keyspace is already deleted.
			logging.Infof("%s [%s:id_%d] vb: %d Exiting cleanup routine due to keyspace delete",
				logPrefix, appName, id, vb)
			dcpFeed.Close()
			return nil
		}
	}

	ticker := time.NewTicker(10 * time.Second)

	wg.Add(1)

	go func(wg *sync.WaitGroup, dcpFeed *couchbase.DcpFeed) {
		defer wg.Done()

		for {
			select {
			case <-ticker.C:
				receivedVbs := make([]uint16, 0)
				rw.RLock()
				for vb := range receivedVbSeqNos {
					receivedVbs = append(receivedVbs, vb)
				}
				rw.RUnlock()

				sort.Sort(util.Uint16Slice(receivedVbs))
				sort.Sort(util.Uint16Slice(vbs))

				if len(receivedVbs) < len(vbs) {
					logging.Infof("%s [%s:id_%d] Received vbs len: %d dump: %s vbs to cleanup len: %d dump: %s",
						logPrefix, appName, id, len(receivedVbs), util.Condense(receivedVbs),
						len(vbs), util.Condense(vbs))
					continue
				}

				receivedAllMutations := true

				rw.RLock()
				for vb, seqNo := range vbSeqNos {
					if !cleanupVbs[vb] {
						continue
					}

					if receivedVbSeqNos[vb] < seqNo {
						receivedAllMutations = false
						break
					}
				}
				rw.RUnlock()

				if !receivedAllMutations {
					continue
				}

				dcpFeed.Close()
				logging.Infof("%s [%s:id_%d] Closed dcpFeed spawned for cleaning up metadata bucket artifacts",
					logPrefix, appName, id)

				ticker.Stop()
				return
			}
		}
	}(&wg, dcpFeed)

	return nil
}
