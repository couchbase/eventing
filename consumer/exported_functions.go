package consumer

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/eventing/common"
	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/timers"
	"github.com/couchbase/eventing/util"
	"github.com/google/flatbuffers/go"
)

// ClearEventStats flushes event processing stats
func (c *Consumer) ClearEventStats() {
	c.msgProcessedRWMutex.Lock()
	defer c.msgProcessedRWMutex.Unlock()

	c.dcpMessagesProcessed = make(map[mcd.CommandCode]uint64)
	c.v8WorkerMessagesProcessed = make(map[string]uint64)

	c.adhocTimerResponsesRecieved = 0
	c.aggMessagesSentCounter = 0
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
	c.msgProcessedRWMutex.RLock()
	defer c.msgProcessedRWMutex.RUnlock()

	stats := make(map[string]uint64)

	for opcode, value := range c.dcpMessagesProcessed {
		stats[strings.ToLower(mcd.CommandNames[opcode])] = value
	}

	if c.adhocTimerResponsesRecieved > 0 {
		stats["adhoc_timer_response_received"] = c.adhocTimerResponsesRecieved
	}

	if c.cppQueueSizes != nil {
		stats["agg_timer_feedback_queue_size"] = uint64(c.cppQueueSizes.DocTimerQueueSize)
		stats["agg_queue_memory"] = uint64(c.cppQueueSizes.AggQueueMemory)
		stats["agg_queue_size"] = uint64(c.cppQueueSizes.AggQueueSize)
	}

	stats["agg_timer_feedback_queue_cap"] = uint64(c.feedbackQueueCap)
	stats["agg_queue_memory_cap"] = uint64(c.workerQueueMemCap)
	stats["agg_queue_size_cap"] = uint64(c.workerQueueCap)

	if c.aggMessagesSentCounter > 0 {
		stats["agg_messages_sent_to_worker"] = c.aggMessagesSentCounter
	}

	if c.dcpDeletionCounter > 0 {
		stats["dcp_deletion_sent_to_worker"] = c.dcpDeletionCounter
	}

	if c.dcpMutationCounter > 0 {
		stats["dcp_mutation_sent_to_worker"] = c.dcpMutationCounter
	}

	if c.dcpXattrParseError > 0 {
		stats["dcp_xattr_parse_error_counter"] = c.dcpXattrParseError
	}

	if c.suppressedDCPDeletionCounter > 0 {
		stats["dcp_deletion_suppressed_counter"] = c.suppressedDCPDeletionCounter
	}

	if c.suppressedDCPMutationCounter > 0 {
		stats["dcp_mutation_suppressed_counter"] = c.suppressedDCPMutationCounter
	}

	if c.dcpCloseStreamCounter > 0 {
		stats["dcp_stream_close_counter"] = c.dcpCloseStreamCounter
	}

	if c.dcpCloseStreamErrCounter > 0 {
		stats["dcp_stream_close_err_counter"] = c.dcpCloseStreamErrCounter
	}

	if c.dcpStreamReqCounter > 0 {
		stats["dcp_stream_req_counter"] = c.dcpStreamReqCounter
	}

	if c.dcpStreamReqErrCounter > 0 {
		stats["dcp_stream_req_err_counter"] = c.dcpStreamReqErrCounter
	}

	if c.timerResponsesRecieved > 0 {
		stats["timer_responses_received"] = c.timerResponsesRecieved
	}

	if c.timerMessagesProcessed > 0 {
		stats["timer_events"] = c.timerMessagesProcessed
	}

	if c.errorParsingTimerResponses > 0 {
		stats["error_parsing_timer_response"] = c.errorParsingTimerResponses
	}

	if c.isBootstrapping {
		stats["is_bootstrapping"] = 1
	}

	if c.isRebalanceOngoing {
		stats["is_rebalance_ongoing"] = 1
	}

	vbsRemainingToCloseStream := c.getVbRemainingToCloseStream()
	if len(vbsRemainingToCloseStream) > 0 {
		stats["reb_vb_remaining_to_close_stream"] = uint64(len(vbsRemainingToCloseStream))
	}

	vbsRemainingToGiveUp := c.getVbRemainingToGiveUp()
	if len(vbsRemainingToGiveUp) > 0 {
		stats["reb_vb_remaining_to_give_up"] = uint64(len(vbsRemainingToGiveUp))
	}

	vbsRemainingToOwn := c.getVbRemainingToOwn()
	if len(vbsRemainingToOwn) > 0 {
		stats["reb_vb_remaining_to_own"] = uint64(len(vbsRemainingToOwn))
	}

	vbsRemainingToStreamReq := c.getVbRemainingToStreamReq()
	if len(vbsRemainingToStreamReq) > 0 {
		stats["reb_vb_remaining_to_stream_req"] = uint64(len(vbsRemainingToStreamReq))
	}

	if c.vbsStateUpdateRunning {
		stats["vbs_state_update_running"] = 1
	}

	if _, ok := c.v8WorkerMessagesProcessed["debug_start"]; ok {
		if c.v8WorkerMessagesProcessed["debug_start"] > 0 {
			stats["debug_start"] = c.v8WorkerMessagesProcessed["debug_start"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["debug_stop"]; ok {
		if c.v8WorkerMessagesProcessed["debug_stop"] > 0 {
			stats["debug_stop"] = c.v8WorkerMessagesProcessed["debug_stop"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["execution_stats"]; ok {
		if c.v8WorkerMessagesProcessed["execution_stats"] > 0 {
			stats["execution_stats"] = c.v8WorkerMessagesProcessed["execution_stats"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["failure_stats"]; ok {
		if c.v8WorkerMessagesProcessed["failure_stats"] > 0 {
			stats["failure_stats"] = c.v8WorkerMessagesProcessed["failure_stats"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["handler_code"]; ok {
		if c.v8WorkerMessagesProcessed["handler_code"] > 0 {
			stats["handler_code"] = c.v8WorkerMessagesProcessed["handler_code"]

		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["latency_stats"]; ok {
		if c.v8WorkerMessagesProcessed["latency_stats"] > 0 {
			stats["latency_stats"] = c.v8WorkerMessagesProcessed["latency_stats"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["lcb_exception_stats"]; ok {
		if c.v8WorkerMessagesProcessed["lcb_exception_stats"] > 0 {
			stats["lcb_exception_stats"] = c.v8WorkerMessagesProcessed["lcb_exception_stats"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["log_level"]; ok {
		if c.v8WorkerMessagesProcessed["log_level"] > 0 {
			stats["log_level"] = c.v8WorkerMessagesProcessed["log_level"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["source_map"]; ok {
		if c.v8WorkerMessagesProcessed["source_map"] > 0 {
			stats["source_map"] = c.v8WorkerMessagesProcessed["source_map"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["timer_context_size"]; ok {
		if c.v8WorkerMessagesProcessed["timer_context_size"] > 0 {
			stats["timer_context_size"] = c.v8WorkerMessagesProcessed["timer_context_size"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["thr_count"]; ok {
		if c.v8WorkerMessagesProcessed["thr_count"] > 0 {
			stats["thr_count"] = c.v8WorkerMessagesProcessed["thr_count"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["thr_map"]; ok {
		if c.v8WorkerMessagesProcessed["thr_map"] > 0 {
			stats["thr_map"] = c.v8WorkerMessagesProcessed["thr_map"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["v8_compile"]; ok {
		if c.v8WorkerMessagesProcessed["v8_compile"] > 0 {
			stats["v8_compile"] = c.v8WorkerMessagesProcessed["v8_compile"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["v8_init"]; ok {
		if c.v8WorkerMessagesProcessed["v8_init"] > 0 {
			stats["v8_init"] = c.v8WorkerMessagesProcessed["v8_init"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["v8_load"]; ok {
		if c.v8WorkerMessagesProcessed["v8_load"] > 0 {
			stats["v8_load"] = c.v8WorkerMessagesProcessed["v8_load"]
		}
	}

	return stats
}

// GetMetaStoreStats exposes timer store related stat counters
func (c *Consumer) GetMetaStoreStats() map[string]uint64 {
	stats := make(map[string]uint64)

	if !c.usingTimer {
		return nil
	}

	stats["metastore_deletes"] = atomic.LoadUint64(&c.metastoreDeleteCounter)
	stats["metastore_delete_err"] = atomic.LoadUint64(&c.metastoreDeleteErrCounter)
	stats["metastore_not_found"] = atomic.LoadUint64(&c.metastoreNotFoundErrCounter)
	stats["metastore_scan"] = atomic.LoadUint64(&c.metastoreScanCounter)
	stats["metastore_scan_due"] = atomic.LoadUint64(&c.metastoreScanDueCounter)
	stats["metastore_scan_err"] = atomic.LoadUint64(&c.metastoreScanErrCounter)
	stats["metastore_set"] = atomic.LoadUint64(&c.metastoreSetCounter)
	stats["metastore_set_err"] = atomic.LoadUint64(&c.metastoreSetErrCounter)

	for _, vb := range c.getCurrentlyOwnedVbs() {
		store, found := timers.Fetch(c.producer.GetMetadataPrefix(), int(vb))
		if !found {
			atomic.AddUint64(&c.metastoreNotFoundErrCounter, 1)
			continue
		}

		storeStats := store.Stats()
		for stat, counter := range storeStats {
			if _, ok := stats[stat]; !ok {
				stats[stat] = 0
			}
			stats[stat] += counter
		}
	}

	return stats
}

// GetHandlerCode returns handler code to assist V8 debugger
func (c *Consumer) GetHandlerCode() string {
	return c.handlerCode
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
	logPrefix := "Consumer::SetConnHandle"

	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	c.conn = conn
	logging.Infof("%s [%s:%s:%d] Setting conn handle: %rs",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.conn)

	c.sockReader = bufio.NewReader(c.conn)
	go c.readMessageLoop()

	c.socketWriteLoopStopCh <- struct{}{}
	<-c.socketWriteLoopStopAckCh
	go c.sendMessageLoop()
}

// SetFeedbackConnHandle initialised the socket connect for data channel from eventing-consumer
func (c *Consumer) SetFeedbackConnHandle(conn net.Conn) {
	logPrefix := "Consumer::SetFeedbackConnHandle"

	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	c.feedbackConn = conn
	logging.Infof("%s [%s:%s:%d] Setting feedback conn handle: %rs",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.feedbackConn)

	c.sockFeedbackReader = bufio.NewReader(c.feedbackConn)

	go c.feedbackReadMessageLoop(c.sockFeedbackReader)
}

// SignalBootstrapFinish is leveraged by Eventing.Producer instance to know
// if corresponding Eventing.Consumer instance has finished bootstrap
func (c *Consumer) SignalBootstrapFinish() {
	logPrefix := "Consumer::SignalBootstrapFinish"

	logging.Infof("%s [%s:%s:%d] Got request to signal bootstrap status",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	<-c.signalBootstrapFinishCh
}

// SignalConnected notifies consumer routine when CPP V8 worker has connected to
// tcp listener instance
func (c *Consumer) SignalConnected() {
	c.signalConnectedCh <- struct{}{}
}

// SignalFeedbackConnected notifies consumer routine when CPP V8 worker has connected to
// data channel
func (c *Consumer) SignalFeedbackConnected() {
	c.signalFeedbackConnectedCh <- struct{}{}
}

// UpdateEventingNodesUUIDs is called by producer instance to notify about
// updated list of node uuids
func (c *Consumer) UpdateEventingNodesUUIDs(keepNodes, ejectNodes []string) {
	c.ejectNodesUUIDs = ejectNodes
	c.eventingNodeUUIDs = keepNodes

	// Reset the flag before a rebalance is about to start off
	c.timerQueuesAreDrained = false
}

// GetLatencyStats returns latency stats for event handlers from from cpp world
func (c *Consumer) GetLatencyStats() map[string]uint64 {
	c.statsRWMutex.RLock()
	defer c.statsRWMutex.RUnlock()

	latencyStats := make(map[string]uint64)

	for k, v := range c.latencyStats {
		latencyStats[k] = v
	}

	return latencyStats
}

func (c *Consumer) GetCurlLatencyStats() map[string]uint64 {
	c.statsRWMutex.RLock()
	defer c.statsRWMutex.RUnlock()

	latencyStats := make(map[string]uint64)

	for k, v := range c.curlLatencyStats {
		latencyStats[k] = v
	}

	return latencyStats
}

// GetExecutionStats returns OnUpdate/OnDelete success/failure stats for event handlers from cpp world
func (c *Consumer) GetExecutionStats() map[string]interface{} {
	c.statsRWMutex.RLock()
	defer c.statsRWMutex.RUnlock()

	executionStats := make(map[string]interface{})

	for k, v := range c.executionStats {
		executionStats[k] = v
	}

	return executionStats
}

// GetFailureStats returns failure stats for event handlers from cpp world
func (c *Consumer) GetFailureStats() map[string]interface{} {
	c.statsRWMutex.RLock()
	defer c.statsRWMutex.RUnlock()
	failureStats := make(map[string]interface{})

	for k, v := range c.failureStats {
		failureStats[k] = v
	}

	return failureStats
}

// Pid returns the process id of CPP V8 worker
func (c *Consumer) Pid() int {
	pid, ok := c.osPid.Load().(int)
	if ok {
		return pid
	}
	return 0
}

// GetLcbExceptionsStats returns libcouchbase exception stats from CPP workers
func (c *Consumer) GetLcbExceptionsStats() map[string]uint64 {
	c.statsRWMutex.RLock()
	defer c.statsRWMutex.RUnlock()
	lcbExceptionStats := make(map[string]uint64)

	for k, v := range c.lcbExceptionStats {
		lcbExceptionStats[k] = v
	}

	return lcbExceptionStats
}

// SpawnCompilationWorker bring up a CPP worker to compile the user supplied handler code
func (c *Consumer) SpawnCompilationWorker(appCode, appContent, appName, eventingPort string, handlerHeaders, handlerFooters []string) (*common.CompileStatus, error) {
	logPrefix := "Consumer::SpawnCompilationWorker"

	listener, err := net.Listen("tcp", net.JoinHostPort(util.Localhost(), "0"))
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Compilation worker: Failed to listen on tcp port, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
		return nil, err
	}

	connectedCh := make(chan struct{}, 1)
	c.initConsumer(appName)

	go func(listener net.Listener, connectedCh chan struct{}) {

		var err error
		c.conn, err = listener.Accept()
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Compilation worker: Error on accept, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
			return
		}

		logging.Infof("%s [%s:%s:%d] Compilation worker: got connection: %rs",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.conn)

		connectedCh <- struct{}{}
	}(listener, connectedCh)

	_, c.tcpPort, err = net.SplitHostPort(listener.Addr().String())
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to parse address, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
	}

	var pid int
	go func() {
		user, key := util.LocalKey()
		cmd := exec.Command(
			"eventing-consumer",
			appName,
			"af_inet",
			c.tcpPort,
			c.tcpPort,
			fmt.Sprintf("worker_%s", appName),
			"1",
			"1",
			os.TempDir(),
			util.GetIPMode(),
			"true",
			"validate") // this parameter is not read, for tagging

		cmd.Env = append(os.Environ(),
			fmt.Sprintf("CBEVT_CALLBACK_USR=%s", user),
			fmt.Sprintf("CBEVT_CALLBACK_KEY=%s", key))

		outPipe, err := cmd.StdoutPipe()
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to open stdout pipe, err: %v",
				appName, c.workerName, c.tcpPort, c.Pid(), err)
			return
		}

		errPipe, err := cmd.StderrPipe()
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to open stderr pipe, err: %v",
				appName, c.workerName, c.tcpPort, c.Pid(), err)
			return
		}

		inPipe, err := cmd.StdinPipe()
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to open stdin pipe, err: %v",
				appName, c.workerName, c.tcpPort, c.Pid(), err)
			return
		}

		defer inPipe.Close()

		err = cmd.Start()
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to spawn compilation worker, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
			return
		}
		pid = cmd.Process.Pid
		logging.Infof("%s [%s:%s:%d] compilation worker launched",
			logPrefix, c.workerName, c.tcpPort, pid)

		bufErr := bufio.NewReader(errPipe)
		go func(bufErr *bufio.Reader) {
			defer errPipe.Close()
			for {
				msg, _, err := bufErr.ReadLine()
				if err != nil {
					logging.Warnf("%s [%s:%s:%d] Failed to read from stderr pipe, err: %v",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
					return
				}

				logging.Infof("%s %s", logPrefix, string(msg))
			}
		}(bufErr)

		bufOut := bufio.NewReader(outPipe)
		go func(bufOut *bufio.Reader) {
			defer outPipe.Close()
			for {
				msg, _, err := bufOut.ReadLine()
				if err != nil {
					logging.Warnf("%s [%s:%s:%d] Failed to read from stdout pipe, err: %v",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
					return
				}

				logging.Infof("%s %s", logPrefix, string(msg))
			}
		}(bufOut)

		err = cmd.Wait()

		logging.Infof("%s [%s:%s:%d] compilation worker exited with status %v",
			logPrefix, c.workerName, c.tcpPort, pid, err)

	}()
	<-connectedCh
	c.sockReader = bufio.NewReader(c.conn)

	c.sendWorkerThrCount(1, false)
	logging.Infof("%s [%s:%s:%d] Handler headers %v", logPrefix, c.workerName, c.tcpPort, pid, c.handlerHeaders)
	logging.Infof("%s [%s:%s:%d] Handler footers %v", logPrefix, c.workerName, c.tcpPort, pid, c.handlerFooters)

	c.handlerHeaders = handlerHeaders
	c.handlerFooters = handlerFooters
	// Framing bare minimum V8 worker init payload
	payload, pBuilder := c.makeV8InitPayload(appName, c.debuggerPort, util.Localhost(), "", eventingPort, "",
		"", appContent, 5, 10, 10*1000, true, 1024)

	c.sendInitV8Worker(payload, false, pBuilder)

	c.sendCompileRequest(appCode)

	go c.readMessageLoop()

	for c.compileInfo == nil {
		time.Sleep(1 * time.Second)
	}

	c.conn.Close()
	listener.Close()

	err = util.KillProcess(pid)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Unable to kill C++ worker spawned for compilation, err: %v",
			logPrefix, c.workerName, c.tcpPort, pid, err)
	}

	logging.Infof("%s [%s:%s:%d] compilation status %#v",
		logPrefix, c.workerName, c.tcpPort, pid, c.compileInfo)

	return c.compileInfo, nil
}

func (c *Consumer) initConsumer(appName string) {
	c.executionTimeout = 10000
	c.lcbInstCapacity = 1
	c.socketWriteBatchSize = 1
	c.cppWorkerThrCount = 1
	c.ipcType = "af_inet"

	c.connMutex = &sync.RWMutex{}
	c.msgProcessedRWMutex = &sync.RWMutex{}
	c.sendMsgBufferRWMutex = &sync.RWMutex{}
	c.app = &common.AppConfig{AppName: appName}
	c.socketTimeout = 1 * time.Second

	c.v8WorkerMessagesProcessed = make(map[string]uint64)

	c.builderPool = &sync.Pool{
		New: func() interface{} {
			return flatbuffers.NewBuilder(0)
		},
	}
}

// InternalVbDistributionStats returns internal state of vbucket ownership distribution on local eventing node
func (c *Consumer) InternalVbDistributionStats() []uint16 {
	activeDcpStreams := make([]uint16, 0)

	for vb := 0; vb < c.numVbuckets; vb++ {
		dcpStreamStatus := c.vbProcessingStats.getVbStat(uint16(vb), "dcp_stream_status").(string)
		if dcpStreamStatus == dcpStreamRunning {
			activeDcpStreams = append(activeDcpStreams, uint16(vb))
		}
	}

	return activeDcpStreams
}

// TimerDebugStats captures timer related stats to assist in debugging mismatches during rebalance
func (c *Consumer) TimerDebugStats() map[int]map[string]interface{} {
	stats := make(map[int]map[string]interface{})

	for vb := 0; vb < c.numVbuckets; vb++ {
		if _, ok := stats[vb]; !ok {
			stats[vb] = make(map[string]interface{})

			stats[vb]["assigned_worker"] = c.vbProcessingStats.getVbStat(uint16(vb), "assigned_worker")
			stats[vb]["currently_processed_doc_id_timer"] = c.vbProcessingStats.getVbStat(uint16(vb), "currently_processed_doc_id_timer")
			stats[vb]["deleted_during_cleanup_counter"] = c.vbProcessingStats.getVbStat(uint16(vb), "deleted_during_cleanup_counter")
			stats[vb]["last_processed_doc_id_timer_event"] = c.vbProcessingStats.getVbStat(uint16(vb), "last_processed_doc_id_timer_event")
			stats[vb]["next_doc_id_timer_to_process"] = c.vbProcessingStats.getVbStat(uint16(vb), "next_doc_id_timer_to_process")
			stats[vb]["node_uuid"] = c.vbProcessingStats.getVbStat(uint16(vb), "node_uuid")
			stats[vb]["removed_during_rebalance_counter"] = c.vbProcessingStats.getVbStat(uint16(vb), "removed_during_rebalance_counter")
			stats[vb]["sent_to_worker_counter"] = c.vbProcessingStats.getVbStat(uint16(vb), "sent_to_worker_counter")
			stats[vb]["timer_create_counter"] = c.vbProcessingStats.getVbStat(uint16(vb), "timer_create_counter")
			stats[vb]["timers_in_past_counter"] = c.vbProcessingStats.getVbStat(uint16(vb), "timers_in_past_counter")
			stats[vb]["timers_in_past_from_backfill_counter"] = c.vbProcessingStats.getVbStat(uint16(vb), "timers_in_past_from_backfill_counter")
			stats[vb]["timers_recreated_from_dcp_backfill"] = c.vbProcessingStats.getVbStat(uint16(vb), "timers_recreated_from_dcp_backfill")
		}
	}

	return stats
}

// RebalanceStatus returns state of rebalance for consumer instance
func (c *Consumer) RebalanceStatus() bool {
	return c.isRebalanceOngoing
}

// SetRebalanceStatus update rebalance status for consumer instance
func (c *Consumer) SetRebalanceStatus(status bool) {
	logPrefix := "Consumer::SetRebalanceStatus"

	c.isRebalanceOngoing = status
	logging.Infof("%s [%s:%s:%d] Updated isRebalanceOngoing to %t",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), status)
}

// VbSeqnoStats returns seq no stats, which can be useful in figuring out missed events during rebalance
func (c *Consumer) VbSeqnoStats() map[int]map[string]interface{} {
	seqnoStats := make(map[int]map[string]interface{})

	for vb := 0; vb < c.numVbuckets; vb++ {
		if _, ok := seqnoStats[vb]; !ok {
			seqnoStats[vb] = make(map[string]interface{})

			everOwnedVb := c.vbProcessingStats.getVbStat(uint16(vb), "ever_owned_vb").(bool)
			if !everOwnedVb {
				continue
			}

			seqnoStats[vb]["host_name"] = c.vbProcessingStats.getVbStat(uint16(vb), "host_name")
			seqnoStats[vb]["last_checkpointed_seq_no"] = c.vbProcessingStats.getVbStat(uint16(vb), "last_checkpointed_seq_no")
			seqnoStats[vb]["node_uuid"] = c.vbProcessingStats.getVbStat(uint16(vb), "node_uuid")
			seqnoStats[vb]["start_seq_no"] = c.vbProcessingStats.getVbStat(uint16(vb), "start_seq_no")
			seqnoStats[vb]["seq_no_at_stream_end"] = c.vbProcessingStats.getVbStat(uint16(vb), "seq_no_at_stream_end")
			seqnoStats[vb]["seq_no_after_close_stream"] = c.vbProcessingStats.getVbStat(uint16(vb), "seq_no_after_close_stream")
			seqnoStats[vb]["timestamp"] = c.vbProcessingStats.getVbStat(uint16(vb), "timestamp")
			seqnoStats[vb]["worker_name"] = c.vbProcessingStats.getVbStat(uint16(vb), "worker_name")
		}
	}

	return seqnoStats
}

// Index returns the index of consumer among all consumers designated
// for specific handler on an eventing node
func (c *Consumer) Index() int {
	return c.index
}

// VbEventingNodeAssignMapUpdate captures updated node to vbucket assignment
func (c *Consumer) VbEventingNodeAssignMapUpdate(vbEventingNodeAssignMap map[uint16]string) {
	c.vbEventingNodeAssignRWMutex.Lock()
	defer c.vbEventingNodeAssignRWMutex.Unlock()

	c.vbEventingNodeAssignMap = make(map[uint16]string)

	for vb, node := range vbEventingNodeAssignMap {
		c.vbEventingNodeAssignMap[vb] = node
	}
}

// WorkerVbMapUpdate captures updated mapping of active consumers to vbuckets they should handle as per static planner
func (c *Consumer) WorkerVbMapUpdate(workerVbucketMap map[string][]uint16) {
	c.workerVbucketMapRWMutex.Lock()
	defer c.workerVbucketMapRWMutex.Unlock()

	c.workerVbucketMap = make(map[string][]uint16)

	for workerName, assignedVbs := range workerVbucketMap {
		c.workerVbucketMap[workerName] = assignedVbs
	}
}

func (c *Consumer) getAssignedVbs(workerName string) ([]uint16, error) {
	c.workerVbucketMapRWMutex.RLock()
	defer c.workerVbucketMapRWMutex.RUnlock()

	if _, ok := c.workerVbucketMap[workerName]; ok {
		return c.workerVbucketMap[workerName], nil
	}

	return nil, fmt.Errorf("worker not found")
}

// UpdateWorkerQueueMemCap revises the memory cap for cpp worker, dcp and timer queues
func (c *Consumer) UpdateWorkerQueueMemCap(quota int64) {
	logPrefix := "Consumer::updateWorkerQueueMemCap"

	prevWorkerMemCap := c.workerQueueMemCap
	prevDCPFeedMemCap := c.aggDCPFeedMemCap

	var divisor int64

	if c.app.UsingTimer {
		divisor = 5
		c.timerQueueMemCap = uint64((quota / divisor) * 1024 * 1024)
	} else {
		divisor = 2
	}

	c.workerQueueMemCap = (quota / divisor) * 1024 * 1024
	c.aggDCPFeedMemCap = (quota / divisor) * 1024 * 1024

	logging.Infof("%s [%s:%s:%d] Updated memory quota: %d MB previous worker quota: %d MB dcp feed quota: %d MB",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.workerQueueMemCap/(1024*1024),
		prevWorkerMemCap/(1024*1024), prevDCPFeedMemCap/(1024*1024))
}

// ResetBootstrapDone to unset bootstrap flag
func (c *Consumer) ResetBootstrapDone() {
	logPrefix := "Consumer::ResetBootstrapDone"

	logging.Infof("%s [%s:%s:%d] Current ResetBootstrapDone flag: %t", logPrefix, c.workerName, c.tcpPort, c.Pid(), c.resetBootstrapDone)
	c.resetBootstrapDone = true
	logging.Infof("%s [%s:%s:%d] Updated ResetBootstrapDone flag to: %t", logPrefix, c.workerName, c.tcpPort, c.Pid(), c.resetBootstrapDone)
}
