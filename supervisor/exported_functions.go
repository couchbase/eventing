package supervisor

import (
	"fmt"
	"net"
	"time"
	"sync/atomic"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/timers"
	"github.com/couchbase/eventing/util"
)

// ClearEventStats flushes event processing stats
func (s *SuperSupervisor) ClearEventStats() {
	for _, p := range s.runningFns() {
		p.ClearEventStats()
	}
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

	return "", nil
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

// RestPort returns ns_server port(typically 8091/9000)
func (s *SuperSupervisor) RestPort() string {
	return s.restPort
}

// SignalStopDebugger stops V8 Debugger for a specific deployed lambda
func (s *SuperSupervisor) SignalStopDebugger(appName string) error {
	logPrefix := "SuperSupervisor::SignalStopDebugger"

	p, ok := s.runningFns()[appName]
	if ok {
		err := p.SignalStopDebugger()
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%d] Exiting due to timeout", logPrefix, s.runningFnsCount())
			return err
		}
	} else {
		logging.Errorf("%s [%d] Function: %s request didn't go through as Eventing.Producer instance isn't alive",
			logPrefix, s.runningFnsCount(), appName)
	}

	return nil
}

// GetAppState returns current state of app
func (s *SuperSupervisor) GetAppState(appName string) int8 {
	s.appRWMutex.RLock()
	defer s.appRWMutex.RUnlock()

	switch s.appDeploymentStatus[appName] {
	case true:
		switch s.appProcessingStatus[appName] {
		case true:
			return common.AppStateEnabled
		case false:
			return common.AppStatePaused
		}
	case false:
		switch s.appProcessingStatus[appName] {
		case true:
			return common.AppStateUnexpected
		case false:
			return common.AppStateUndeployed
		}
	}
	return common.AppState
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
		if producer.BootstrapStatus() {
			logging.Infof("%s [%d] Bootstrap status from %s: %#v", logPrefix, s.runningFnsCount(), appName, producer.BootstrapStatus())
		}
		return producer.BootstrapStatus()
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
func (s *SuperSupervisor) StopProducer(appName string, skipMetaCleanup bool, updateMetakv bool) {
	logPrefix := "SuperSupervisor::StopProducer"

	s.appRWMutex.Lock()
	s.appDeploymentStatus[appName] = false
	s.appProcessingStatus[appName] = false
	s.appRWMutex.Unlock()

	logging.Infof("%s [%d] Function: %s stopping running producer instance, skipMetaCleanup: %t, updateMetakv: %t",
		logPrefix, s.runningFnsCount(), appName, skipMetaCleanup, updateMetakv)

	s.deleteFromLocallyDeployedApps(appName)

	s.CleanupProducer(appName, skipMetaCleanup, updateMetakv)
	s.deleteFromDeployedApps(appName)
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
	for stat, counter := range timers.PoolStats() {
		stats[stat] = counter
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

func (s *SuperSupervisor) watchBucketWithLock(bucketName string) error {
	if _, ok := s.buckets[bucketName]; !ok {
		hostPortAddr := net.JoinHostPort(util.Localhost(), s.restPort)
		b, err := util.ConnectBucket(hostPortAddr, "default", bucketName)
		if err != nil {
			logging.Errorf("Could not connect to bucket %s", bucketName)
			return err
		} else {
			s.buckets[bucketName] = b
		}
	}
	s.bucketsCount[bucketName]++
	return nil
}

// UnwatchBucket removes the bucket from supervisor
func (s *SuperSupervisor) UnwatchBucket(bucketName string) {
	s.bucketsRWMutex.Lock()
	defer s.bucketsRWMutex.Unlock()
	if _, ok := s.buckets[bucketName]; ok {
		s.bucketsCount[bucketName]--
		if s.bucketsCount[bucketName] == 0 {
			delete(s.buckets, bucketName)
		}
	}
}

// GetBucket returns the bucket to the caller
func (s *SuperSupervisor) GetBucket(bucketName string) (*couchbase.Bucket, error) {
	s.bucketsRWMutex.Lock()
	defer s.bucketsRWMutex.Unlock()
	if _, ok := s.buckets[bucketName]; ok {
		return s.buckets[bucketName], nil
	}

	if err := s.watchBucketWithLock(bucketName); err != nil {
		return nil, err
	}

	if err := s.buckets[bucketName].Refresh(); err != nil {
		return nil, err
	}
	return s.buckets[bucketName], nil
}
