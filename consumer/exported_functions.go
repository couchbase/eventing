package consumer

import (
	"fmt"
	"net"
	"sync/atomic"
	"unsafe"

	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

// ClearEventStats flushes event processing stats
func (c *Consumer) ClearEventStats() {
	c.Lock()
	c.dcpMessagesProcessed = make(map[mcd.CommandCode]uint64)
	c.v8WorkerMessagesProcessed = make(map[string]uint64)
	c.doctimerMessagesProcessed = 0
	c.crontimerMessagesProcessed = 0
	c.plasmaDeleteCounter = 0
	c.plasmaInsertCounter = 0
	c.plasmaLookupCounter = 0
	c.Unlock()
}

// ConsumerName returns consumer name e.q <event_handler_name>_worker_1
func (c *Consumer) ConsumerName() string {
	return c.workerName
}

// EventingNodeUUIDs return list of known eventing node uuids
func (c *Consumer) EventingNodeUUIDs() []string {
	return c.eventingNodeUUIDs
}

// GetEventProcessingStats exposes dcp/timer processing stats
func (c *Consumer) GetEventProcessingStats() map[string]uint64 {
	stats := make(map[string]uint64)
	c.RLock()
	for opcode, value := range c.dcpMessagesProcessed {
		stats[mcd.CommandNames[opcode]] = value
	}
	if c.doctimerMessagesProcessed > 0 {
		stats["DOC_TIMER_EVENTS"] = c.doctimerMessagesProcessed
	}

	if c.crontimerMessagesProcessed > 0 {
		stats["CRON_TIMER_EVENTS"] = c.crontimerMessagesProcessed
	}

	c.RUnlock()

	return stats
}

// GetHandlerCode returns handler code to assist V8 debugger
func (c *Consumer) GetHandlerCode() string {
	return c.handlerCode
}

// GetSeqsProcessed returns vbucket specific sequence nos processed so far
func (c *Consumer) GetSeqsProcessed() map[int]int64 {
	seqNoProcessed := make(map[int]int64)

	var seqNo int64
	subdocPath := "last_processed_seq_no"

	for vb := 0; vb < numVbuckets; vb++ {
		vbKey := fmt.Sprintf("%s_vb_%d", c.app.AppName, vb)
		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getMetaOpCallback, c, vbKey, &seqNo, subdocPath)
		seqNoProcessed[vb] = seqNo
	}

	return seqNoProcessed
}

// GetSourceMap returns source map to assist V8 debugger
func (c *Consumer) GetSourceMap() string {
	return c.sourceMap
}

// HostPortAddr returns the HostPortAddr combination of current eventing node
// e.g. 127.0.0.1:25000
func (c *Consumer) HostPortAddr() string {
	hostPortAddr := (*string)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&c.hostPortAddr))))
	if hostPortAddr != nil {
		return *hostPortAddr
	}
	return ""
}

// NodeUUID returns UUID that's supplied by ns_server from command line
func (c *Consumer) NodeUUID() string {
	return c.uuid
}

// SetConnHandle sets the tcp connection handle for CPP V8 worker
func (c *Consumer) SetConnHandle(conn net.Conn) {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	c.conn = conn
	logging.Infof("V8CR[%s:%s:%s:%d] Setting conn handle: %v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.conn)
}

// SignalBootstrapFinish is leveraged by Eventing.Producer instance to know
// if corresponding Eventing.Consumer instance has finished bootstrap
func (c *Consumer) SignalBootstrapFinish() {
	logging.Infof("V8CR[%s:%s:%s:%d] Got request to signal bootstrap status",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())

	<-c.signalBootstrapFinishCh
}

// SignalConnected notifies consumer routine when CPP V8 worker has connected to
// tcp listener instance
func (c *Consumer) SignalConnected() {
	c.signalConnectedCh <- struct{}{}
}

// TimerTransferHostPortAddr returns hostport combination for RPC server handling transfer of
// timer related plasma files during rebalance
func (c *Consumer) TimerTransferHostPortAddr() string {
	if c.timerTransferHandle == nil {
		return ""
	}

	return c.timerTransferHandle.Addr
}

// UpdateEventingNodesUUIDs is called by producer instance to notify about
// updated list of node uuids
func (c *Consumer) UpdateEventingNodesUUIDs(uuids []string) {
	c.eventingNodeUUIDs = uuids
}

// GetLatencyStats returns latency stats for event handlers from from cpp world
func (c *Consumer) GetLatencyStats() map[string]uint64 {
	c.sendGetLatencyStats(false)
	return c.latencyStats
}
