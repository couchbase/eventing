package producer

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

// Auth returns username:password combination for the cluster
func (p *Producer) Auth() string {
	p.RLock()
	defer p.RUnlock()
	return p.auth
}

// CfgData returns deployment descriptor content
func (p *Producer) CfgData() string {
	p.RLock()
	defer p.RUnlock()
	return p.cfgData
}

// ClearEventStats flushes event processing stats
func (p *Producer) ClearEventStats() {
	p.RLock()
	defer p.RUnlock()
	for _, c := range p.runningConsumers {
		c.ClearEventStats()
	}
}

// GetLatencyStats returns latency stats for event handlers from from cpp world
func (p *Producer) GetLatencyStats() map[string]uint64 {
	p.RLock()
	defer p.RUnlock()
	latencyStats := make(map[string]uint64)
	for _, c := range p.runningConsumers {
		clStats := c.GetLatencyStats()
		for k, v := range clStats {
			if _, ok := latencyStats[k]; !ok {
				latencyStats[k] = 0
			}
			latencyStats[k] += v
		}
	}
	return latencyStats
}

// GetExecutionStats returns execution stats aggregated from Eventing.Consumer instances
func (p *Producer) GetExecutionStats() map[string]interface{} {
	executionStats := make(map[string]interface{})
	executionStats["timestamp"] = make(map[int]string)
	p.RLock()
	defer p.RUnlock()
	for _, c := range p.runningConsumers {
		for k, v := range c.GetExecutionStats() {
			if k == "timestamp" {
				executionStats["timestamp"].(map[int]string)[c.Pid()] = v.(string)
				continue
			}

			if _, ok := executionStats[k]; !ok {
				executionStats[k] = float64(0)
			}

			executionStats[k] = executionStats[k].(float64) + v.(float64)
		}
	}

	return executionStats
}

// GetFailureStats returns failure stats aggregated from Eventing.Consumer instances
func (p *Producer) GetFailureStats() map[string]interface{} {
	failureStats := make(map[string]interface{})
	failureStats["timestamp"] = make(map[int]string)
	p.RLock()
	defer p.RUnlock()
	for _, c := range p.runningConsumers {
		for k, v := range c.GetFailureStats() {
			if k == "timestamp" {
				failureStats["timestamp"].(map[int]string)[c.Pid()] = v.(string)
				continue
			}

			if _, ok := failureStats[k]; !ok {
				failureStats[k] = float64(0)
			}

			failureStats[k] = failureStats[k].(float64) + v.(float64)
		}
	}

	return failureStats
}

// GetLcbExceptionsStats returns libcouchbase exception stats from CPP workers
func (p *Producer) GetLcbExceptionsStats() map[string]uint64 {
	exceptionStats := make(map[string]uint64)
	p.RLock()
	defer p.RUnlock()
	for _, c := range p.runningConsumers {
		leStats := c.GetLcbExceptionsStats()
		for k, v := range leStats {
			if _, ok := exceptionStats[k]; !ok {
				exceptionStats[k] = 0
			}
			exceptionStats[k] += v
		}
	}
	return exceptionStats
}

// GetAppCode returns handler code for the current app
func (p *Producer) GetAppCode() string {
	return p.app.AppCode
}

// GetEventProcessingStats exposes dcp/timer processing stats
func (p *Producer) GetEventProcessingStats() map[string]uint64 {
	aggStats := make(map[string]uint64)
	p.RLock()
	defer p.RUnlock()
	for _, consumer := range p.runningConsumers {
		stats := consumer.GetEventProcessingStats()
		for stat, value := range stats {
			if _, ok := aggStats[stat]; !ok {
				aggStats[stat] = 0
			}
			aggStats[stat] += value
		}
	}

	return aggStats
}

// GetHandlerCode returns handler code to assist V8 Debugger
func (p *Producer) GetHandlerCode() string {
	logPrefix := "Producer::GetHandlerCode"
	p.RLock()
	defer p.RUnlock()
	if len(p.runningConsumers) > 0 {
		return p.runningConsumers[0].GetHandlerCode()
	}
	logging.Errorf("%s [%s:%d] No active Eventing.Consumer instances running", logPrefix, p.appName, p.LenRunningConsumers())
	return ""
}

// GetNsServerPort return rest port for ns_server
func (p *Producer) GetNsServerPort() string {
	return p.nsServerPort
}

// GetSourceMap return source map to assist V8 Debugger
func (p *Producer) GetSourceMap() string {
	logPrefix := "Producer::GetSourceMap"
	p.RLock()
	defer p.RUnlock()
	if len(p.runningConsumers) > 0 {
		return p.runningConsumers[0].GetSourceMap()
	}
	logging.Errorf("%s [%s:%d] No active Eventing.Consumer instances running", logPrefix, p.appName, p.LenRunningConsumers())
	return ""
}

// IsEventingNodeAlive verifies if a hostPortAddr combination is an active eventing node
func (p *Producer) IsEventingNodeAlive(eventingHostPortAddr, nodeUUID string) bool {
	eventingNodeAddrs := (*[]string)(atomic.LoadPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&p.eventingNodeAddrs))))
	if eventingNodeAddrs != nil {
		for _, v := range *eventingNodeAddrs {
			if v == eventingHostPortAddr {
				return true
			}
		}
	}

	// To assist in the case of hostname update
	if util.Contains(nodeUUID, p.eventingNodeUUIDs) || util.Contains(nodeUUID, p.eventingNodeUUIDs) {
		return true
	}

	return false
}

// KvHostPorts returns host:port combination for kv service
func (p *Producer) KvHostPorts() []string {
	p.RLock()
	defer p.RUnlock()
	return p.kvHostPorts
}

// LenRunningConsumers returns the number of actively running consumers for a given app's producer
func (p *Producer) LenRunningConsumers() int {
	return len(p.runningConsumers)
}

// MetadataBucket return metadata bucket for event handler
func (p *Producer) MetadataBucket() string {
	return p.metadatabucket
}

// NotifyInit notifies the supervisor about producer initialisation
func (p *Producer) NotifyInit() {
	<-p.notifyInitCh
}

// NsServerHostPort returns host:port combination for ns_server instance
func (p *Producer) NsServerHostPort() string {
	return p.nsServerHostPort
}

// NsServerNodeCount returns count of currently active ns_server nodes in the cluster
func (p *Producer) NsServerNodeCount() int {
	nsServerNodeAddrs := (*[]string)(atomic.LoadPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&p.nsServerNodeAddrs))))
	if nsServerNodeAddrs != nil {
		return len(*nsServerNodeAddrs)
	}
	return 0
}

// SetRetryCount changes the retry count for early bail out from retry ops
func (p *Producer) SetRetryCount(retryCount int64) {
	p.retryCount = retryCount
}

// SignalBootstrapFinish is leveraged by EventingSuperSup instance to
// check if app handler has finished bootstrapping
func (p *Producer) SignalBootstrapFinish() {
	logPrefix := "Producer::SignalBootstrapFinish"

	runningConsumers := make([]common.EventingConsumer, 0)

	logging.Infof("%s [%s:%d] Got request to signal bootstrap status", logPrefix, p.appName, p.LenRunningConsumers())
	<-p.bootstrapFinishCh

	p.RLock()
	for _, c := range p.runningConsumers {
		runningConsumers = append(runningConsumers, c)
	}
	p.RUnlock()

	for _, c := range runningConsumers {
		if c == nil {
			continue
		}
		c.SignalBootstrapFinish()
	}
}

// SignalCheckpointBlobCleanup cleans up eventing app related blobs from metadata bucket
func (p *Producer) SignalCheckpointBlobCleanup() error {
	logPrefix := "Producer::SignalCheckpointBlobCleanup"

	for vb := 0; vb < p.numVbuckets; vb++ {
		vbKey := fmt.Sprintf("%s::vb::%d", p.appName, vb)
		err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, deleteOpCallback,
			p, p.AddMetadataPrefix(vbKey))
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
			return common.ErrRetryTimeout
		}
	}

	dFlagKey := fmt.Sprintf("%s::%s", p.appName, startDebuggerFlag)
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, deleteOpCallback,
		p, p.AddMetadataPrefix(dFlagKey))
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return common.ErrRetryTimeout
	}

	dInstAddrKey := fmt.Sprintf("%s::%s", p.appName, debuggerInstanceAddr)
	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, deleteOpCallback,
		p, p.AddMetadataPrefix(dInstAddrKey))
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return common.ErrRetryTimeout
	}

	logging.Infof("%s [%s:%d] Purged all owned checkpoint & debugger blobs from metadata bucket: %s",
		logPrefix, p.appName, p.LenRunningConsumers(), p.metadataBucketHandle.Name())
	return nil
}

// PauseProducer pauses the execution of Eventing.Producer and corresponding Eventing.Consumer instances
func (p *Producer) PauseProducer() {
	p.pauseProducerCh <- struct{}{}
}

// StopProducer cleans up resource handles
func (p *Producer) StopProducer() {
	logPrefix := "Producer::StopProducer"

	if p.stopProducerCh != nil {
		p.stopProducerCh <- struct{}{}
	}

	logging.Infof("%s [%s:%d] Signalled Producer::Serve to exit",
		logPrefix, p.appName, p.LenRunningConsumers())

	if p.metadataBucketHandle != nil {
		p.metadataBucketHandle.Close()
	}

	logging.Infof("%s [%s:%d] Closed metadata bucket handle",
		logPrefix, p.appName, p.LenRunningConsumers())

	if p.workerSupervisor != nil {
		p.workerSupervisor.Stop()
	}

	logging.Infof("%s [%s:%d] Stopped supervisor tree",
		logPrefix, p.appName, p.LenRunningConsumers())
}

// GetDcpEventsRemainingToProcess returns remaining dcp events to process
func (p *Producer) GetDcpEventsRemainingToProcess() uint64 {
	var remainingEvents uint64
	p.RLock()
	defer p.RUnlock()
	for _, consumer := range p.runningConsumers {
		remainingEvents += consumer.DcpEventsRemainingToProcess()
	}

	return remainingEvents
}

// VbDcpEventsRemainingToProcess returns remaining dcp events to process per vbucket
func (p *Producer) VbDcpEventsRemainingToProcess() map[int]int64 {
	vbDcpEventsRemaining := make(map[int]int64)

	for _, consumer := range p.runningConsumers {
		eventsRemaining := consumer.VbDcpEventsRemainingToProcess()
		for vb, count := range eventsRemaining {

			if _, ok := vbDcpEventsRemaining[vb]; !ok {
				vbDcpEventsRemaining[vb] = 0
			}

			vbDcpEventsRemaining[vb] += count
		}
	}

	return vbDcpEventsRemaining
}

// GetEventingConsumerPids returns map of Eventing.Consumer worker name and it's os pid
func (p *Producer) GetEventingConsumerPids() map[string]int {
	workerPidMapping := make(map[string]int)
	p.RLock()
	defer p.RUnlock()
	for _, consumer := range p.runningConsumers {
		workerPidMapping[consumer.ConsumerName()] = consumer.Pid()
	}
	return workerPidMapping
}

// WriteAppLog dumps the application specific log message to configured file
func (p *Producer) WriteAppLog(log string) {
	ts := time.Now().Format("2006-01-02T15:04:05.000-07:00")
	fmt.Fprintf(p.appLogWriter, "%s [INFO] %s\n", ts, log)
}

// InternalVbDistributionStats returns internal state of vbucket ownership distribution on local eventing node
func (p *Producer) InternalVbDistributionStats() map[string]string {
	distributionStats := make(map[string]string)
	p.RLock()
	defer p.RUnlock()
	for _, consumer := range p.runningConsumers {
		distributionStats[consumer.ConsumerName()] = util.Condense(consumer.InternalVbDistributionStats())
	}
	return distributionStats
}

// VbDistributionStatsFromMetadata dumps the state of vbucket distribution per metadata bucket
func (p *Producer) VbDistributionStatsFromMetadata() map[string]map[string]string {
	p.vbEventingNodeRWMutex.RLock()
	defer p.vbEventingNodeRWMutex.RUnlock()

	vbEventingNodeMap := make(map[string]map[string]string)
	for node, nodeMap := range p.vbEventingNodeMap {

		if _, ok := vbEventingNodeMap[node]; !ok {
			vbEventingNodeMap[node] = make(map[string]string)
		}

		for workerID, vbsOwned := range nodeMap {
			vbEventingNodeMap[node][workerID] = vbsOwned
		}
	}
	return vbEventingNodeMap
}

func (p *Producer) vbDistributionStats() error {
	logPrefix := "Producer::vbDistributionStats"
	vbNodeMap := make(map[string]map[string][]uint16)
	vbBlob := make(map[string]interface{})

	for vb := 0; vb < p.numVbuckets; vb++ {
		vbKey := fmt.Sprintf("%s::vb::%d", p.appName, vb)
		err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, getOpCallback,
			p, p.AddMetadataPrefix(vbKey), &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
			return common.ErrRetryTimeout
		}

		if val, ok := vbBlob["current_vb_owner"]; !ok || val == "" {
			continue
		}
		currentOwner := vbBlob["current_vb_owner"].(string)

		if val, ok := vbBlob["assigned_worker"]; !ok || val == "" {
			continue
		}
		workerID := vbBlob["assigned_worker"].(string)

		if _, ok := vbNodeMap[currentOwner]; !ok {
			vbNodeMap[currentOwner] = make(map[string][]uint16)
		}

		if _, cOk := vbNodeMap[currentOwner]; cOk {
			if _, wOk := vbNodeMap[currentOwner][workerID]; !wOk {
				vbNodeMap[currentOwner][workerID] = make([]uint16, 0)
			}
		}

		vbNodeMap[currentOwner][workerID] = append(
			vbNodeMap[currentOwner][workerID], uint16(vb))
	}

	p.vbEventingNodeRWMutex.Lock()
	defer p.vbEventingNodeRWMutex.Unlock()
	// Concise representation of vb mapping for eventing nodes
	p.vbEventingNodeMap = make(map[string]map[string]string)

	for node, nodeMap := range vbNodeMap {
		if _, ok := p.vbEventingNodeMap[node]; !ok {
			p.vbEventingNodeMap[node] = make(map[string]string)
		}

		for workerID, vbsOwned := range nodeMap {
			p.vbEventingNodeMap[node][workerID] = util.Condense(vbsOwned)
		}
	}

	return nil
}

// PlannerStats returns vbucket distribution as per planner running on local eventing
// node for a given app
func (p *Producer) PlannerStats() []*common.PlannerNodeVbMapping {
	p.plannerNodeMappingsRWMutex.RLock()
	defer p.plannerNodeMappingsRWMutex.RUnlock()

	plannerNodeMappings := make([]*common.PlannerNodeVbMapping, 0)

	for _, mapping := range p.plannerNodeMappings {
		plannerNodeMappings = append(plannerNodeMappings, mapping)
	}

	return plannerNodeMappings
}

func (p *Producer) getSeqsProcessed() error {
	logPrefix := "Producer::getSeqsProcessed"
	vbBlob := make(map[string]interface{})

	for vb := 0; vb < p.numVbuckets; vb++ {
		vbKey := fmt.Sprintf("%s::vb::%d", p.appName, vb)
		err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, getOpCallback,
			p, p.AddMetadataPrefix(vbKey), &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
			return common.ErrRetryTimeout
		}

		p.seqsNoProcessedRWMutex.Lock()
		if _, ok := vbBlob["last_processed_seq_no"]; ok {
			p.seqsNoProcessed[vb] = int64(vbBlob["last_processed_seq_no"].(float64))
		}
		p.seqsNoProcessedRWMutex.Unlock()
	}

	return nil
}

// GetSeqsProcessed returns vbucket specific sequence nos processed so far
func (p *Producer) GetSeqsProcessed() map[int]int64 {
	p.seqsNoProcessedRWMutex.RLock()
	defer p.seqsNoProcessedRWMutex.RUnlock()

	seqNoProcessed := make(map[int]int64)
	for k, v := range p.seqsNoProcessed {
		seqNoProcessed[k] = v
	}

	return seqNoProcessed
}

// RebalanceTaskProgress reports vbuckets remaining to be transferred as per planner
// during the course of rebalance
func (p *Producer) RebalanceTaskProgress() *common.RebalanceProgress {
	p.RLock()
	defer p.RUnlock()

	producerLevelProgress := &common.RebalanceProgress{}

	for _, consumer := range p.runningConsumers {
		consumerProgress := consumer.RebalanceTaskProgress()

		producerLevelProgress.CloseStreamVbsLen += consumerProgress.CloseStreamVbsLen
		producerLevelProgress.StreamReqVbsLen += consumerProgress.StreamReqVbsLen

		producerLevelProgress.VbsRemainingToShuffle += consumerProgress.VbsRemainingToShuffle
		producerLevelProgress.VbsOwnedPerPlan += consumerProgress.VbsOwnedPerPlan
	}

	return producerLevelProgress
}

// CleanupMetadataBucket clears up all application related artifacts from
// metadata bucket post undeploy
func (p *Producer) CleanupMetadataBucket() error {
	logPrefix := "Producer::CleanupMetadataBucket"

	err := util.Retry(util.NewFixedBackoff(time.Second), &p.retryCount, getKVNodesAddressesOpCallback, p, p.metadatabucket)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return common.ErrRetryTimeout
	}

	kvNodeAddrs := p.getKvNodeAddrs()

	var b *couchbase.Bucket
	var dcpFeed *couchbase.DcpFeed
	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, commonConnectBucketOpCallback, p, &b)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return common.ErrRetryTimeout
	}

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, startFeedCallback, p, &b, &dcpFeed, kvNodeAddrs)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return common.ErrRetryTimeout
	}

	logging.Infof("%s [%s:%d] Started up dcpFeed to cleanup artifacts from metadata bucket: %v",
		logPrefix, p.appName, p.LenRunningConsumers(), p.metadatabucket)

	var vbSeqNos map[uint16]uint64
	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, dcpGetSeqNosCallback, p, &dcpFeed, &vbSeqNos)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return common.ErrRetryTimeout
	}

	vbs := make([]uint16, 0)
	for vb := range vbSeqNos {
		vbs = append(vbs, vb)
	}

	sort.Sort(util.Uint16Slice(vbs))

	var wg sync.WaitGroup
	wg.Add(1)

	rw := &sync.RWMutex{}
	receivedVbSeqNos := make(map[uint16]uint64)

	go func(b *couchbase.Bucket, dcpFeed *couchbase.DcpFeed, wg *sync.WaitGroup,
		receivedVbSeqNos map[uint16]uint64, rw *sync.RWMutex) {

		defer wg.Done()

		prefix := p.AddMetadataPrefix(p.appName)
		for {
			select {
			case e, ok := <-dcpFeed.C:
				if ok == false {
					logging.Infof("%s [%s:%d] Exiting timer cleanup routine, mutations till high vb seqnos received",
						logPrefix, p.appName, p.LenRunningConsumers())
					return
				}

				rw.Lock()
				receivedVbSeqNos[e.VBucket] = e.Seqno
				rw.Unlock()

				switch e.Opcode {
				case mcd.DCP_MUTATION:
					docID := string(e.Key)
					if strings.HasPrefix(docID, prefix.Raw()) {
						err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval),
							&p.retryCount, deleteOpCallback, p,
							p.AddMetadataPrefix(p.appName+strings.TrimPrefix(docID, prefix.Raw())))
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:%d] Exiting due to timeout",
								logPrefix, p.appName, p.LenRunningConsumers())
							return
						}
					}
				}
			}
		}
	}(b, dcpFeed, &wg, receivedVbSeqNos, rw)

	var flogs couchbase.FailoverLog
	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, getFailoverLogOpCallback, p, &b, &flogs, vbs)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
		return common.ErrRetryTimeout
	}

	start, snapStart, snapEnd := uint64(0), uint64(0), uint64(0xFFFFFFFFFFFFFFFF)
	flags := uint32(0)
	end := uint64(0xFFFFFFFFFFFFFFFF)

	logging.Infof("%s [%s:%d] Going to start DCP streams from metadata bucket: %v, vbs len: %v dump: %v",
		logPrefix, p.appName, p.LenRunningConsumers(), p.metadatabucket, len(vbs), util.Condense(vbs))

	for vb, flog := range flogs {
		vbuuid, _, _ := flog.Latest()

		logging.Debugf("%s [%s:%d] vb: %v starting DCP feed",
			logPrefix, p.appName, p.LenRunningConsumers(), vb)

		opaque := uint16(vb)
		err := dcpFeed.DcpRequestStream(vb, opaque, flags, vbuuid, start, end, snapStart, snapEnd)
		if err != nil {
			logging.Errorf("%s [%s:%d] Failed to stream vb: %v",
				logPrefix, p.appName, p.LenRunningConsumers(), vb)
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

				if len(receivedVbs) < len(vbs) {
					logging.Debugf("%s [%s:%d] len of receivedVbs: %v len of vbs: %v",
						logPrefix, p.appName, p.LenRunningConsumers(), len(receivedVbs), len(vbs))
					continue
				}

				receivedAllMutations := true

				rw.RLock()
				for vb, seqNo := range vbSeqNos {
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
				logging.Infof("%s [%s:%d] Closed dcpFeed spawned for cleaning up metadata bucket artifacts",
					logPrefix, p.appName, p.LenRunningConsumers())

				ticker.Stop()
				return
			}
		}
	}(&wg, dcpFeed)

	wg.Wait()
	return nil
}

// UpdateMemoryQuota allows tuning of memory quota for Eventing
func (p *Producer) UpdateMemoryQuota(quota int64) {
	logPrefix := "Producer::UpdateMemoryQuota"

	logging.Infof("%s [%s:%d] Updating eventing memory quota to %d MB",
		logPrefix, p.appName, p.LenRunningConsumers(), quota)

	p.MemoryQuota = quota // in MB
}

// TimerDebugStats captures timer related stats to assist in debugging mismtaches during rebalance
func (p *Producer) TimerDebugStats() map[int]map[string]interface{} {
	aggStats := make(map[int]map[string]interface{})
	p.RLock()
	defer p.RUnlock()
	for _, consumer := range p.runningConsumers {
		workerStats := consumer.TimerDebugStats()

		for vb, stats := range workerStats {
			if _, ok := aggStats[vb]; !ok {
				aggStats[vb] = stats
			} else {

				deletedDuringCleanupCounter := aggStats[vb]["deleted_during_cleanup_counter"].(uint64) + stats["deleted_during_cleanup_counter"].(uint64)
				removedDuringRebalanceCounter := aggStats[vb]["removed_during_rebalance_counter"].(uint64) + stats["removed_during_rebalance_counter"].(uint64)
				sentToWorkerCounter := aggStats[vb]["sent_to_worker_counter"].(uint64) + stats["sent_to_worker_counter"].(uint64)
				timerCreateCounter := aggStats[vb]["timer_create_counter"].(uint64) + stats["timer_create_counter"].(uint64)
				timersInPastCounter := aggStats[vb]["timers_in_past_counter"].(uint64) + stats["timers_in_past_counter"].(uint64)
				timersInPastFromBackfill := aggStats[vb]["timers_in_past_from_backfill_counter"].(uint64) + stats["timers_in_past_from_backfill_counter"].(uint64)
				timersCreatedFromBackfill := aggStats[vb]["timers_recreated_from_dcp_backfill"].(uint64) + stats["timers_recreated_from_dcp_backfill"].(uint64)

				aggStats[vb]["deleted_during_cleanup_counter"] = deletedDuringCleanupCounter
				aggStats[vb]["removed_during_rebalance_counter"] = removedDuringRebalanceCounter
				aggStats[vb]["sent_to_worker_counter"] = sentToWorkerCounter
				aggStats[vb]["timer_create_counter"] = timerCreateCounter
				aggStats[vb]["timers_in_past_counter"] = timersInPastCounter
				aggStats[vb]["timers_in_past_from_backfill_counter"] = timersInPastFromBackfill
				aggStats[vb]["timers_recreated_from_dcp_backfill"] = timersCreatedFromBackfill
			}
		}
	}

	return aggStats
}

// StopRunningConsumers stops all running instances of Eventing.Consumer
func (p *Producer) StopRunningConsumers() {
	logPrefix := "Producer::StopRunningConsumers"
	p.Lock()
	defer p.Unlock()
	logging.Infof("%s [%s:%d] Stopping running instances of Eventing.Consumer",
		logPrefix, p.appName, p.LenRunningConsumers())

	for _, eventingConsumer := range p.runningConsumers {
		p.workerSupervisor.Remove(p.consumerSupervisorTokenMap[eventingConsumer])
		delete(p.consumerSupervisorTokenMap, eventingConsumer)
	}
	p.runningConsumers = nil
}

// RebalanceStatus returns state of rebalance for all running consumer instances
func (p *Producer) RebalanceStatus() bool {
	logPrefix := "Producer::RebalanceStatus"
	p.RLock()
	defer p.RUnlock()
	consumerRebStatuses := make(map[string]bool)
	for _, c := range p.runningConsumers {
		consumerRebStatuses[c.ConsumerName()] = c.RebalanceStatus()
	}

	logging.Infof("%s [%s:%d] Rebalance status from all running consumer instances: %#v",
		logPrefix, p.appName, p.LenRunningConsumers(), consumerRebStatuses)

	for _, rebStatus := range consumerRebStatuses {
		if rebStatus {
			return rebStatus
		}
	}

	return false
}

// VbSeqnoStats returns seq no stats, which can be useful in figuring out missed events during rebalance
func (p *Producer) VbSeqnoStats() map[int][]map[string]interface{} {
	seqnoStats := make(map[int][]map[string]interface{})
	p.RLock()
	defer p.RUnlock()
	for _, consumer := range p.runningConsumers {
		workerStats := consumer.VbSeqnoStats()

		for vb, stats := range workerStats {
			if len(stats) == 0 {
				continue
			}

			if _, ok := seqnoStats[vb]; !ok {
				seqnoStats[vb] = make([]map[string]interface{}, 0)
			}
			seqnoStats[vb] = append(seqnoStats[vb], stats)
		}
	}

	return seqnoStats
}

// CleanupUDSs clears up UDS created for communication between Go and eventing-consumer
func (p *Producer) CleanupUDSs() {
	if p.processConfig.IPCType == "af_unix" {
		p.RLock()
		defer p.RUnlock()
		for _, c := range p.runningConsumers {
			udsSockPath := fmt.Sprintf("%s/%s_%s.sock", os.TempDir(), p.nsServerHostPort, c.ConsumerName())
			feedbackSockPath := fmt.Sprintf("%s/feedback_%s_%s.sock", os.TempDir(), p.nsServerHostPort, c.ConsumerName())

			os.Remove(udsSockPath)
			os.Remove(feedbackSockPath)
		}
	}
}

// RemoveConsumerToken removes specified worker from supervisor tree
func (p *Producer) RemoveConsumerToken(workerName string) {
	p.workerNameConsumerMapRWMutex.RLock()
	defer p.workerNameConsumerMapRWMutex.RUnlock()

	if c, exists := p.workerNameConsumerMap[workerName]; exists {
		p.workerSupervisor.Remove(p.consumerSupervisorTokenMap[c])
		delete(p.consumerSupervisorTokenMap, c)
	}
}

// IsPlannerRunning returns planner execution status
func (p *Producer) IsPlannerRunning() bool {
	return p.isPlannerRunning
}

// CheckpointBlobDump returns state of metadata blobs stored in Couchbase bucket
func (p *Producer) CheckpointBlobDump() map[string]interface{} {
	logPrefix := "Producer::CheckpointBlobDump"

	checkpointBlobDumps := make(map[string]interface{})

	if p.metadataBucketHandle == nil {
		return checkpointBlobDumps
	}

	for vb := 0; vb < p.numVbuckets; vb++ {
		vbBlob := make(map[string]interface{})
		vbKey := fmt.Sprintf("%s::vb::%d", p.appName, vb)
		err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, getOpCallback, p, p.AddMetadataPrefix(vbKey), &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%d] Exiting due to timeout", logPrefix, p.appName, p.LenRunningConsumers())
			return nil
		}

		checkpointBlobDumps[vbKey] = vbBlob
	}
	return checkpointBlobDumps
}

// AddMetadataPrefix prepends user prefix and handler UUID to namespacing
// within metadata bucket
func (p *Producer) AddMetadataPrefix(key string) common.Key {
	return common.NewKey(p.app.UserPrefix, strconv.Itoa(int(p.app.HandlerUUID)), key)
}

// GetVbOwner returns assigned eventing nodes and worker for a vbucket
func (p *Producer) GetVbOwner(vb uint16) (string, string, error) {
	if info, ok := p.vbMapping[vb]; ok {
		return info.ownerNode, info.assignedWorker, nil
	}

	return "", "", fmt.Errorf("owner not found")
}
