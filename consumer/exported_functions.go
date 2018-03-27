package consumer

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/eventing/common"
	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/google/flatbuffers/go"
)

// ClearEventStats flushes event processing stats
func (c *Consumer) ClearEventStats() {
	c.msgProcessedRWMutex.Lock()
	c.dcpMessagesProcessed = make(map[mcd.CommandCode]uint64)
	c.v8WorkerMessagesProcessed = make(map[string]uint64)
	c.doctimerMessagesProcessed = 0
	c.crontimerMessagesProcessed = 0
	c.plasmaDeleteCounter = 0
	c.plasmaInsertCounter = 0
	c.plasmaLookupCounter = 0
	c.timersInPastCounter = 0
	c.msgProcessedRWMutex.Unlock()
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
	c.msgProcessedRWMutex.RLock()
	for opcode, value := range c.dcpMessagesProcessed {
		stats[mcd.CommandNames[opcode]] = value
	}
	if c.doctimerMessagesProcessed > 0 {
		stats["DOC_TIMER_EVENTS"] = c.doctimerMessagesProcessed
	}

	if c.crontimerMessagesProcessed > 0 {
		stats["CRON_TIMER_EVENTS"] = c.crontimerMessagesProcessed
	}

	if c.timersInPastCounter > 0 {
		stats["TIMERS_IN_PAST"] = c.timersInPastCounter
	}

	if c.timersInPastFromBackfill > 0 {
		stats["TIMERS_IN_PAST_FROM_BACKFILL"] = c.timersInPastFromBackfill
	}

	if c.timersRecreatedFromDCPBackfill > 0 {
		stats["TIMERS_RECREATED_FROM_DCP_BACKFILL"] = c.timersRecreatedFromDCPBackfill
	}

	if c.plasmaDeleteCounter > 0 {
		stats["PLASMA_DELETE_COUNTER"] = c.plasmaDeleteCounter
	}

	if c.plasmaInsertCounter > 0 {
		stats["PLASMA_INSERT_COUNTER"] = c.plasmaInsertCounter
	}

	if c.plasmaLookupCounter > 0 {
		stats["PLASMA_LOOKUP_COUNTER"] = c.plasmaLookupCounter
	}

	if c.dcpMutationCounter > 0 {
		stats["DCP_MUTATION_SENT_TO_WORKER"] = c.dcpMutationCounter
	}

	if c.dcpDeletionCounter > 0 {
		stats["DCP_DELETION_SENT_TO_WORKER"] = c.dcpDeletionCounter
	}

	if c.aggMessagesSentCounter > 0 {
		stats["AGG_MESSAGES_SENT_TO_WORKER"] = c.aggMessagesSentCounter
	}

	if c.doctimerResponsesRecieved > 0 {
		stats["DOC_TIMER_RESPONSES_RECEIVED"] = c.doctimerResponsesRecieved
	}

	if c.errorParsingDocTimerResponses > 0 {
		stats["ERROR_PARSING_DOC_TIMER_RESPONSES"] = c.errorParsingDocTimerResponses
	}

	if c.adhocDoctimerResponsesRecieved > 0 {
		stats["ADHOC_DOC_TIMER_RESPONSES_RECEIVED"] = c.adhocDoctimerResponsesRecieved
	}

	if _, ok := c.v8WorkerMessagesProcessed["LOG_LEVEL"]; ok {
		if c.v8WorkerMessagesProcessed["LOG_LEVEL"] > 0 {
			stats["LOG_LEVEL"] = c.v8WorkerMessagesProcessed["LOG_LEVEL"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["THR_COUNT"]; ok {
		if c.v8WorkerMessagesProcessed["THR_COUNT"] > 0 {
			stats["THR_COUNT"] = c.v8WorkerMessagesProcessed["THR_COUNT"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["THR_MAP"]; ok {
		if c.v8WorkerMessagesProcessed["THR_MAP"] > 0 {
			stats["THR_MAP"] = c.v8WorkerMessagesProcessed["THR_MAP"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["DEBUG_START"]; ok {
		if c.v8WorkerMessagesProcessed["DEBUG_START"] > 0 {
			stats["DEBUG_START"] = c.v8WorkerMessagesProcessed["DEBUG_START"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["DEBUG_STOP"]; ok {
		if c.v8WorkerMessagesProcessed["DEBUG_STOP"] > 0 {
			stats["DEBUG_STOP"] = c.v8WorkerMessagesProcessed["DEBUG_STOP"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["V8_INIT"]; ok {
		if c.v8WorkerMessagesProcessed["V8_INIT"] > 0 {
			stats["V8_INIT"] = c.v8WorkerMessagesProcessed["V8_INIT"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["V8_COMPILE"]; ok {
		if c.v8WorkerMessagesProcessed["V8_COMPILE"] > 0 {
			stats["V8_COMPILE"] = c.v8WorkerMessagesProcessed["V8_COMPILE"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["V8_LOAD"]; ok {
		if c.v8WorkerMessagesProcessed["V8_LOAD"] > 0 {
			stats["V8_LOAD"] = c.v8WorkerMessagesProcessed["V8_LOAD"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["LATENCY_STATS"]; ok {
		if c.v8WorkerMessagesProcessed["LATENCY_STATS"] > 0 {
			stats["LATENCY_STATS"] = c.v8WorkerMessagesProcessed["LATENCY_STATS"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["FAILURE_STATS"]; ok {
		if c.v8WorkerMessagesProcessed["FAILURE_STATS"] > 0 {
			stats["FAILURE_STATS"] = c.v8WorkerMessagesProcessed["FAILURE_STATS"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["EXECUTION_STATS"]; ok {
		if c.v8WorkerMessagesProcessed["EXECUTION_STATS"] > 0 {
			stats["EXECUTION_STATS"] = c.v8WorkerMessagesProcessed["EXECUTION_STATS"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["LCB_EXCEPTION_STATS"]; ok {
		if c.v8WorkerMessagesProcessed["LCB_EXCEPTION_STATS"] > 0 {
			stats["LCB_EXCEPTION_STATS"] = c.v8WorkerMessagesProcessed["LCB_EXCEPTION_STATS"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["SOURCE_MAP"]; ok {
		if c.v8WorkerMessagesProcessed["SOURCE_MAP"] > 0 {
			stats["SOURCE_MAP"] = c.v8WorkerMessagesProcessed["SOURCE_MAP"]
		}
	}

	if _, ok := c.v8WorkerMessagesProcessed["HANDLER_CODE"]; ok {
		if c.v8WorkerMessagesProcessed["HANDLER_CODE"] > 0 {
			stats["HANDLER_CODE"] = c.v8WorkerMessagesProcessed["HANDLER_CODE"]

		}
	}

	c.msgProcessedRWMutex.RUnlock()

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

	go c.feedbackReadMessageLoop()
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
func (c *Consumer) UpdateEventingNodesUUIDs(uuids []string) {
	c.eventingNodeUUIDs = uuids
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
func (c *Consumer) SpawnCompilationWorker(appCode, appContent, appName, eventingPort string) (*common.CompileStatus, error) {
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

		defer outPipe.Close()

		err = cmd.Start()
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to spawn compilation worker, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
			return
		}
		pid = cmd.Process.Pid
		logging.Infof("%s [%s:%s:%d] compilation worker launched",
			logPrefix, c.workerName, c.tcpPort, pid)

		bufOut := bufio.NewReader(outPipe)

		go func(bufOut *bufio.Reader) {
			for {
				msg, _, err := bufOut.ReadLine()
				if err != nil {
					logging.Warnf("%s [%s:%s:%d] Failed to read from stdout pipe, err: %v",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
					return
				}

				logging.Infof("%s", string(msg))
			}
		}(bufOut)

		err = cmd.Wait()

		logging.Infof("%s [%s:%s:%d] compilation worker exited with status %v",
			logPrefix, c.workerName, c.tcpPort, pid, err)

	}()
	<-connectedCh
	c.sockReader = bufio.NewReader(c.conn)

	c.sendWorkerThrCount(1, false)

	// Framing bare minimum V8 worker init payload
	// TODO : Remove rbac user once RBAC issue is resolved
	payload, pBuilder := c.makeV8InitPayload(appName, util.Localhost(), "", eventingPort, "",
		"", appContent, 5, 10, 1, 30, 10*1000, true, true, 500)

	c.sendInitV8Worker(payload, false, pBuilder)

	c.sendCompileRequest(appCode)

	go c.readMessageLoop()

	for c.compileInfo == nil {
		time.Sleep(1 * time.Second)
	}

	c.conn.Close()
	listener.Close()

	if pid > 1 {
		ps, err := os.FindProcess(pid)
		if err == nil {
			ps.Kill()
		}
	}

	logging.Infof("%s [%s:%s:%d] compilation status %v",
		logPrefix, c.workerName, c.tcpPort, pid, c.compileInfo)

	return c.compileInfo, nil
}

func (c *Consumer) initConsumer(appName string) {
	c.executionTimeout = 10000
	c.lcbInstCapacity = 1
	c.socketWriteBatchSize = 1
	c.cppWorkerThrCount = 1
	c.curlTimeout = 1000
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

// TimerDebugStats captures timer related stats to assist in debugging mismtaches during rebalance
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
