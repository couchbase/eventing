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
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	c.conn = conn
	logging.Infof("CREF[%s:%s:%s:%d] Setting conn handle: %v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.conn)

	c.sockReader = bufio.NewReader(c.conn)
	go c.readMessageLoop()

	c.socketWriteLoopStopCh <- struct{}{}
	<-c.socketWriteLoopStopAckCh
	go c.sendMessageLoop()
}

// SignalBootstrapFinish is leveraged by Eventing.Producer instance to know
// if corresponding Eventing.Consumer instance has finished bootstrap
func (c *Consumer) SignalBootstrapFinish() {
	logging.Infof("CREF[%s:%s:%s:%d] Got request to signal bootstrap status",
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
	c.statsRWMutex.RLock()
	defer c.statsRWMutex.RUnlock()

	latencyStats := make(map[string]uint64)

	for k, v := range c.latencyStats {
		latencyStats[k] = v
	}

	return latencyStats
}

// GetExecutionStats returns OnUpdate/OnDelete success/failure stats for event handlers from cpp world
func (c *Consumer) GetExecutionStats() map[string]uint64 {
	c.sendGetExecutionStats(false)
	c.statsRWMutex.RLock()
	defer c.statsRWMutex.RUnlock()

	executionStats := make(map[string]uint64)

	for k, v := range c.executionStats {
		executionStats[k] = v
	}

	return executionStats
}

// GetFailureStats returns failure stats for event handlers from cpp world
func (c *Consumer) GetFailureStats() map[string]uint64 {
	c.sendGetFailureStats(false)
	c.statsRWMutex.RLock()
	defer c.statsRWMutex.RUnlock()
	failureStats := make(map[string]uint64)

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
	c.sendGetLcbExceptionStats(false)
	c.statsRWMutex.RLock()
	defer c.statsRWMutex.RUnlock()
	lcbExceptionStats := make(map[string]uint64)

	for k, v := range c.lcbExceptionStats {
		lcbExceptionStats[k] = v
	}

	return lcbExceptionStats
}

// SpawnCompilationWorker bring up a CPP worker to compile the user supplied handler code
func (c *Consumer) SpawnCompilationWorker(appCode, appContent, appName string) (*common.CompileStatus, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		logging.Errorf("CREF[%s:%s:%s:%d] Compilation worker: Failed to listen on tcp port, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
		return nil, err
	}

	connectedCh := make(chan struct{}, 1)
	c.initConsumer(appName)

	go func(listener net.Listener, connectedCh chan struct{}) {

		var err error
		c.conn, err = listener.Accept()
		if err != nil {
			logging.Errorf("CREF[%s:%s:%s:%d] Compilation worker: Error on accept, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
			return
		}

		logging.Infof("CREF[%s:%s:%s:%d] Compilation worker: got connection: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.conn)

		connectedCh <- struct{}{}
	}(listener, connectedCh)

	c.tcpPort = strings.Split(listener.Addr().String(), ":")[1]
	var pid int

	go func(pid int, appName string) {
		cmd := exec.Command("eventing-consumer", appName, "af_inet", c.tcpPort,
			fmt.Sprintf("worker_%s", appName), "1", "8096")

		err := cmd.Start()
		if err != nil {
			logging.Errorf("CREF[%s:%s:%s:%d] Failed to spawn compilation worker, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
		} else {
			pid = cmd.Process.Pid
			logging.Infof("CREF[%s:%s:%s:%d] compilation worker launched",
				c.app.AppName, c.workerName, c.tcpPort, pid)
		}

		cmd.Wait()

	}(pid, appName)
	<-connectedCh
	c.sockReader = bufio.NewReader(c.conn)

	c.sendWorkerThrCount(1, false)

	// Framing bare minimum V8 worker init payload
	payload, pBuilder := c.makeV8InitPayload(appName, "127.0.0.1", "", "",
		"", appContent, 5, 10,
		1, 30, 10*1000, true, true, 500)

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
