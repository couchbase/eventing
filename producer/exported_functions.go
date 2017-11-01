package producer

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/eventing/common"
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
	return p.cfgData
}

// ClearEventStats flushes event processing stats
func (p *Producer) ClearEventStats() {
	for _, c := range p.runningConsumers {
		c.ClearEventStats()
	}
}

// GetLatencyStats returns latency stats for event handlers from from cpp world
func (p *Producer) GetLatencyStats() map[string]uint64 {
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

// GetFailureStats returns failure stats aggregated from Eventing.Consumer instances
func (p *Producer) GetFailureStats() map[string]uint64 {
	failureStats := make(map[string]uint64)
	for _, c := range p.runningConsumers {
		cfStats := c.GetFailureStats()
		for k, v := range cfStats {
			if _, ok := failureStats[k]; !ok {
				failureStats[k] = 0
			}
			failureStats[k] += v
		}
	}
	return failureStats
}

// GetAppCode returns handler code for the current app
func (p *Producer) GetAppCode() string {
	return p.app.AppCode
}

// GetEventProcessingStats exposes dcp/timer processing stats
func (p *Producer) GetEventProcessingStats() map[string]uint64 {
	aggStats := make(map[string]uint64)
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
	if len(p.runningConsumers) > 0 {
		return p.runningConsumers[0].GetHandlerCode()
	}
	logging.Errorf("PRDR[%s:%d] No active Eventing.Consumer instances running", p.appName, p.LenRunningConsumers())
	return ""
}

// GetNsServerPort return rest port for ns_server
func (p *Producer) GetNsServerPort() string {
	p.RLock()
	defer p.RUnlock()
	return p.nsServerPort
}

// GetSeqsProcessed returns vbucket specific sequence nos processed so far
func (p *Producer) GetSeqsProcessed() map[int]int64 {
	if len(p.runningConsumers) > 0 {
		return p.runningConsumers[0].GetSeqsProcessed()
	}
	logging.Errorf("PRDR[%s:%d] No active Eventing.Consumer instances running", p.appName, p.LenRunningConsumers())
	return nil
}

// GetSourceMap return source map to assist V8 Debugger
func (p *Producer) GetSourceMap() string {
	if len(p.runningConsumers) > 0 {
		return p.runningConsumers[0].GetSourceMap()
	}
	logging.Errorf("PRDR[%s:%d] No active Eventing.Consumer instances running", p.appName, p.LenRunningConsumers())
	return ""
}

// IsEventingNodeAlive verifies if a hostPortAddr combination is an active eventing node
func (p *Producer) IsEventingNodeAlive(eventingHostPortAddr string) bool {
	eventingNodeAddrs := (*[]string)(atomic.LoadPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&p.eventingNodeAddrs))))
	if eventingNodeAddrs != nil {
		for _, v := range *eventingNodeAddrs {
			if v == eventingHostPortAddr {
				return true
			}
		}
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
	p.RLock()
	defer p.RUnlock()
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

// SignalBootstrapFinish is leveraged by EventingSuperSup instance to
// check if app handler has finished bootstrapping
func (p *Producer) SignalBootstrapFinish() {
	runningConsumers := make([]common.EventingConsumer, 0)

	logging.Infof("PRDR[%s:%d] Got request to signal bootstrap status", p.appName, p.LenRunningConsumers())
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
func (p *Producer) SignalCheckpointBlobCleanup() {

	for vb := 0; vb < numVbuckets; vb++ {
		vbKey := fmt.Sprintf("%s_vb_%d", p.appName, vb)
		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), deleteOpCallback, p, vbKey)
	}

	dFlagKey := fmt.Sprintf("%s::%s", p.appName, startDebuggerFlag)
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), deleteOpCallback, p, dFlagKey)

	dInstAddrKey := fmt.Sprintf("%s::%s", p.appName, debuggerInstanceAddr)
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), deleteOpCallback, p, dInstAddrKey)

	logging.Infof("PRDR[%s:%d] Purged all owned checkpoint & debugger blobs from metadata bucket: %s",
		p.appName, p.LenRunningConsumers(), p.metadataBucketHandle.Name())
}

// VbEventingNodeAssignMap returns the vbucket to evening node mapping
func (p *Producer) VbEventingNodeAssignMap() map[uint16]string {
	p.RLock()
	defer p.RUnlock()
	return p.vbEventingNodeAssignMap
}

// WorkerVbMap returns mapping of active consumers to vbuckets they should handle as per static planner
func (p *Producer) WorkerVbMap() map[string][]uint16 {
	p.RLock()
	defer p.RUnlock()
	return p.workerVbucketMap
}

// RbacUser returns the rbac user supplied as part of app settings
func (p *Producer) RbacUser() string {
	return p.rbacUser
}

// RbacPass returns the rbac password supplied as part of app settings
func (p *Producer) RbacPass() string {
	return p.rbacPass
}

// PauseProducer pauses the execution of Eventing.Producer and corresponding Eventing.Consumer instances
func (p *Producer) PauseProducer() {
	p.pauseProducerCh <- struct{}{}
}

// StopProducer cleans up resource handles
func (p *Producer) StopProducer() {
	p.stopProducerCh <- struct{}{}
	p.metadataBucketHandle.Close()
	p.workerSupervisor.Stop()
}

// GetDcpEventsRemainingToProcess returns remaining dcp events to process
func (p *Producer) GetDcpEventsRemainingToProcess() uint64 {
	var remainingEvents uint64

	for _, consumer := range p.runningConsumers {
		remainingEvents += consumer.DcpEventsRemainingToProcess()
	}

	return remainingEvents
}

// GetEventingConsumerPids returns map of Eventing.Consumer worker name and it's os pid
func (p *Producer) GetEventingConsumerPids() map[string]int {
	workerPidMapping := make(map[string]int)

	for _, consumer := range p.runningConsumers {
		workerPidMapping[consumer.ConsumerName()] = consumer.Pid()
	}

	return workerPidMapping
}
