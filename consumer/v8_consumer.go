package consumer

import (
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/timer_transfer"
	"github.com/couchbase/eventing/util"
	cblib "github.com/couchbase/go-couchbase"
	"github.com/couchbase/indexing/secondary/dcp"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/plasma"
)

// NewConsumer called by producer to create consumer handle
func NewConsumer(streamBoundary common.DcpStreamBoundary, cleanupTimers, enableRecursiveMutation bool,
	executionTimeout, index, lcbInstCapacity, skipTimerThreshold, sockWriteBatchSize, timerProcessingPoolSize int,
	vbOwnershipGiveUpRoutineCount, vbOwnershipTakeoverRoutineCount int,
	bucket, eventingAdminPort, eventingDir, logLevel, tcpPort, uuid string,
	eventingNodeUUIDs []string, vbnos []uint16, app *common.AppConfig,
	p common.EventingProducer, s common.EventingSuperSup, vbPlasmaStoreMap map[uint16]*plasma.Plasma,
	socketTimeout time.Duration) *Consumer {

	var b *couchbase.Bucket
	consumer := &Consumer{
		app:                                app,
		aggDCPFeed:                         make(chan *memcached.DcpEvent, dcpGenChanSize),
		bucket:                             bucket,
		cbBucket:                           b,
		checkpointInterval:                 checkpointInterval,
		cleanupTimers:                      cleanupTimers,
		clusterStateChangeNotifCh:          make(chan struct{}, ClusterChangeNotifChBufSize),
		dcpFeedCancelChs:                   make([]chan struct{}, 0),
		dcpFeedVbMap:                       make(map[*couchbase.DcpFeed][]uint16),
		dcpStreamBoundary:                  streamBoundary,
		debuggerStarted:                    false,
		docTimerEntryCh:                    make(chan *byTimerEntry, timerChanSize),
		enableRecursiveMutation:            enableRecursiveMutation,
		eventingAdminPort:                  eventingAdminPort,
		eventingDir:                        eventingDir,
		eventingNodeUUIDs:                  eventingNodeUUIDs,
		executionTimeout:                   executionTimeout,
		gracefulShutdownChan:               make(chan struct{}, 1),
		hostDcpFeedRWMutex:                 &sync.RWMutex{},
		kvHostDcpFeedMap:                   make(map[string]*couchbase.DcpFeed),
		lcbInstCapacity:                    lcbInstCapacity,
		logLevel:                           logLevel,
		nonDocTimerEntryCh:                 make(chan string, timerChanSize),
		nonDocTimerStopCh:                  make(chan struct{}, 1),
		opsTimestamp:                       time.Now(),
		persistAllTicker:                   time.NewTicker(persistAllTickInterval),
		plasmaReaderRWMutex:                &sync.RWMutex{},
		plasmaStoreRWMutex:                 &sync.RWMutex{},
		producer:                           p,
		restartVbDcpStreamTicker:           time.NewTicker(restartVbDcpStreamTickInterval),
		sendMsgCounter:                     0,
		sendMsgToDebugger:                  false,
		signalConnectedCh:                  make(chan struct{}, 1),
		signalDebugBlobDebugStopCh:         make(chan struct{}, 1),
		signalInstBlobCasOpFinishCh:        make(chan struct{}, 1),
		signalSettingsChangeCh:             make(chan struct{}, 1),
		signalPlasmaClosedCh:               make(chan uint16, numVbuckets),
		signalPlasmaTransferFinishCh:       make(chan *plasmaStoreMsg, numVbuckets),
		signalProcessTimerPlasmaCloseAckCh: make(chan uint16, numVbuckets),
		signalStartDebuggerCh:              make(chan struct{}, 1),
		signalStopDebuggerCh:               make(chan struct{}, 1),
		signalStopDebuggerRoutineCh:        make(chan struct{}, 1),
		signalStoreTimerPlasmaCloseAckCh:   make(chan uint16, numVbuckets),
		signalStoreTimerPlasmaCloseCh:      make(chan uint16, numVbuckets),
		signalUpdateDebuggerInstBlobCh:     make(chan struct{}, 1),
		skipTimerThreshold:                 skipTimerThreshold,
		socketTimeout:                      socketTimeout,
		socketWriteBatchSize:               sockWriteBatchSize,
		statsTicker:                        time.NewTicker(statsTickInterval),
		stopControlRoutineCh:               make(chan struct{}),
		stopPlasmaPersistCh:                make(chan struct{}, 1),
		stopVbOwnerGiveupCh:                make(chan struct{}, 1),
		stopVbOwnerTakeoverCh:              make(chan struct{}, 1),
		superSup:                           s,
		tcpPort:                            tcpPort,
		timerRWMutex:                       &sync.RWMutex{},
		timerProcessingTickInterval:        timerProcessingTickInterval,
		timerProcessingWorkerCount:         timerProcessingPoolSize,
		timerProcessingVbsWorkerMap:        make(map[uint16]*timerProcessingWorker),
		timerProcessingRunningWorkers:      make([]*timerProcessingWorker, 0),
		timerProcessingWorkerSignalCh:      make(map[*timerProcessingWorker]chan struct{}),
		uuid:         uuid,
		vbDcpFeedMap: make(map[uint16]*couchbase.DcpFeed),
		vbFlogChan:   make(chan *vbFlogEntry),
		vbnos:        vbnos,
		vbOwnershipGiveUpRoutineCount:   vbOwnershipGiveUpRoutineCount,
		vbOwnershipTakeoverRoutineCount: vbOwnershipTakeoverRoutineCount,
		vbPlasmaReader:                  make(map[uint16]*plasma.Writer),
		vbPlasmaWriter:                  make(map[uint16]*plasma.Writer),
		vbProcessingStats:               newVbProcessingStats(app.AppName),
		vbsRemainingToGiveUp:            make([]uint16, 0),
		vbsRemainingToOwn:               make([]uint16, 0),
		vbsRemainingToRestream:          make([]uint16, 0),
		workerName:                      fmt.Sprintf("worker_%s_%d", app.AppName, index),
		writeBatchSeqnoMap:              make(map[uint16]uint64),
	}

	consumer.vbPlasmaStoreMap = make(map[uint16]*plasma.Plasma)

	for vb, store := range vbPlasmaStoreMap {
		consumer.vbPlasmaStoreMap[vb] = store
	}

	return consumer
}

// Serve acts as init routine for consumer handle
func (c *Consumer) Serve() {
	c.stopConsumerCh = make(chan struct{}, 1)
	c.stopCheckpointingCh = make(chan struct{}, 1)

	c.dcpMessagesProcessed = make(map[mcd.CommandCode]uint64)
	c.v8WorkerMessagesProcessed = make(map[string]uint64)

	c.consumerSup = suptree.NewSimple(c.workerName)
	go c.consumerSup.ServeBackground()

	c.timerTransferHandle = timer.NewTimerTransfer(c, c.app.AppName, c.eventingDir,
		c.HostPortAddr(), c.workerName)
	c.timerTransferSupToken = c.consumerSup.Add(c.timerTransferHandle)

	c.initCBBucketConnHandle()

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), commonConnectBucketOpCallback, c, &c.cbBucket)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), gocbConnectBucketCallback, c)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), gocbConnectMetaBucketCallback, c)

	var flogs couchbase.FailoverLog
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getFailoverLogOpCallback, c, &flogs, dcpConfig)

	sort.Sort(util.Uint16Slice(c.vbnos))
	logging.Infof("V8CR[%s:%s:%s:%d] vbnos len: %d",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), len(c.vbnos))

	util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

	logging.Infof("V8CR[%s:%s:%s:%d] Spawning worker corresponding to producer, node addr: %v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.HostPortAddr())

	var feedName couchbase.DcpFeedName

	kvHostPorts := c.producer.KvHostPorts()
	for _, kvHostPort := range kvHostPorts {
		feedName = couchbase.DcpFeedName("eventing:" + c.HostPortAddr() + "_" + kvHostPort + "_" + c.workerName)

		c.hostDcpFeedRWMutex.Lock()
		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), startDCPFeedOpCallback, c, feedName, dcpConfig, kvHostPort)

		cancelCh := make(chan struct{}, 1)
		c.dcpFeedCancelChs = append(c.dcpFeedCancelChs, cancelCh)

		c.addToAggChan(c.kvHostDcpFeedMap[kvHostPort], cancelCh)
		c.hostDcpFeedRWMutex.Unlock()
	}

	c.client = newClient(c, c.app.AppName, c.tcpPort, c.workerName)
	c.clientSupToken = c.consumerSup.Add(c.client)

	c.startDcp(dcpConfig, flogs)

	// Initialises timer processing worker instances
	c.vbTimerProcessingWorkerAssign(true)

	go c.plasmaPersistAll()

	// doc_id timer events
	for _, r := range c.timerProcessingRunningWorkers {
		go r.processTimerEvents()
	}

	// non doc_id timer events
	go c.processNonDocTimerEvents()

	// V8 Debugger polling routine
	go c.pollForDebuggerStart()

	c.controlRoutine()

	logging.Debugf("V8CR[%s:%s:%s:%d] Exiting consumer init routine",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())
}

// HandleV8Worker sets up CPP V8 worker post it's bootstrap
func (c *Consumer) HandleV8Worker() {
	<-c.signalConnectedCh

	logging.SetLogLevel(util.GetLogLevel(c.logLevel))
	c.sendLogLevel(c.logLevel, false)

	util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

	var currHostAddr string
	h := c.HostPortAddr()
	if h != "" {
		currHostAddr = strings.Split(h, ":")[0]
	} else {
		currHostAddr = "127.0.0.1"
	}

	payload := makeV8InitPayload(c.app.AppName, currHostAddr, c.producer.KvHostPorts()[0], c.producer.CfgData(),
		c.producer.RbacUser(), c.producer.RbacPass(), c.lcbInstCapacity, c.executionTimeout, c.enableRecursiveMutation)
	logging.Debugf("V8CR[%s:%s:%s:%d] V8 worker init enable_recursive_mutation flag: %v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.enableRecursiveMutation)

	c.sendInitV8Worker(payload, false)

	c.sendLoadV8Worker(c.app.AppCode, false)

	go c.doLastSeqNoCheckpoint()

	go c.processEvents()

}

// Stop acts terminate routine for consumer handle
func (c *Consumer) Stop() {
	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("V8CR[%s:%s:%s:%d] Consumer stop routine, panic and recover, %v stack trace: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
		}
	}()

	logging.Infof("V8CR[%s:%s:%s:%d] Gracefully shutting down consumer routine",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())

	c.cbBucket.Close()
	c.gocbBucket.Close()
	c.gocbMetaBucket.Close()

	c.producer.CleanupDeadConsumer(c)

	c.consumerSup.Remove(c.timerTransferSupToken)
	c.consumerSup.Remove(c.clientSupToken)
	c.consumerSup.Stop()

	c.checkpointTicker.Stop()
	c.restartVbDcpStreamTicker.Stop()
	c.statsTicker.Stop()
	c.persistAllTicker.Stop()

	c.conn.Close()
	c.debugConn.Close()
	c.debugListener.Close()

	for k := range c.timerProcessingWorkerSignalCh {
		k.stopCh <- struct{}{}
	}

	c.nonDocTimerStopCh <- struct{}{}
	c.stopControlRoutineCh <- struct{}{}
	c.stopPlasmaPersistCh <- struct{}{}
	c.signalStopDebuggerRoutineCh <- struct{}{}

	for _, dcpFeed := range c.kvHostDcpFeedMap {
		dcpFeed.Close()
	}

	for _, cancelCh := range c.dcpFeedCancelChs {
		cancelCh <- struct{}{}
	}

	close(c.aggDCPFeed)
}

// Implement fmt.Stringer interface to allow better debugging
// if C++ V8 worker crashes
func (c *Consumer) String() string {
	countMsg, _, _ := util.SprintDCPCounts(c.dcpMessagesProcessed)
	return fmt.Sprintf("consumer => app: %s name: %v tcpPort: %s ospid: %d"+
		" dcpEventProcessed: %s v8EventProcessed: %s", c.app.AppName, c.ConsumerName(),
		c.tcpPort, c.Pid(), countMsg, util.SprintV8Counts(c.v8WorkerMessagesProcessed))
}

// SignalConnected notifies consumer routine when CPP V8 worker has connected to
// tcp listener instance
func (c *Consumer) SignalConnected() {
	c.signalConnectedCh <- struct{}{}
}

// SetConnHandle sets the tcp connection handle for CPP V8 worker
func (c *Consumer) SetConnHandle(conn net.Conn) {
	c.Lock()
	defer c.Unlock()
	c.conn = conn
}

// ClearEventStats flushes event processing stats
func (c *Consumer) ClearEventStats() {
	c.Lock()
	c.dcpMessagesProcessed = make(map[mcd.CommandCode]uint64)
	c.v8WorkerMessagesProcessed = make(map[string]uint64)
	c.timerMessagesProcessed = 0
	c.Unlock()
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

// Pid returns the process id of CPP V8 worker
func (c *Consumer) Pid() int {
	pid, ok := c.osPid.Load().(int)
	if ok {
		return pid
	}
	return 0
}

// ConsumerName returns consumer name e.q <event_handler_name>_worker_1
func (c *Consumer) ConsumerName() string {
	return c.workerName
}

// NodeUUID returns UUID that's supplied by ns_server from command line
func (c *Consumer) NodeUUID() string {
	return c.uuid
}

// TimerTransferHostPortAddr returns hostport combination for RPC server handling transfer of
// timer related plasma files during rebalance
func (c *Consumer) TimerTransferHostPortAddr() string {
	if c.timerTransferHandle == nil {
		return ""
	}

	return c.timerTransferHandle.Addr
}

// NotifyClusterChange is called by producer handle to signify each
// consumer instance about StartTopologyChange rpc call from cbauth service.Manager
func (c *Consumer) NotifyClusterChange() {
	logging.Infof("V8CR[%s:%s:%s:%d] Got notification about cluster state change",
		c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid())

	if !c.isRebalanceOngoing {
		c.clusterStateChangeNotifCh <- struct{}{}
	} else {
		logging.Infof("V8CR[%s:%s:%s:%d] Skipping cluster state change notification to control routine because another rebalance is in ongoing",
			c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid())
	}
}

// UpdateEventingNodesUUIDs is called by producer instance to notify about
// updated list of node uuids
func (c *Consumer) UpdateEventingNodesUUIDs(uuids []string) {
	c.eventingNodeUUIDs = uuids
}

// EventingNodeUUIDs return list of known eventing node uuids
func (c *Consumer) EventingNodeUUIDs() []string {
	return c.eventingNodeUUIDs
}

// NotifyRebalanceStop is called by producer to signal stopping of
// rebalance operation
func (c *Consumer) NotifyRebalanceStop() {
	logging.Infof("V8CR[%s:%s:%s:%d] Got notification about rebalance stop",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())

	for i := 0; i < c.vbOwnershipGiveUpRoutineCount; i++ {
		c.stopVbOwnerGiveupCh <- struct{}{}
	}

	for i := 0; i < c.vbOwnershipTakeoverRoutineCount; i++ {
		c.stopVbOwnerTakeoverCh <- struct{}{}
	}
}

// NotifySettingsChange signals consumer instance of settings update
func (c *Consumer) NotifySettingsChange() {
	logging.Infof("V8CR[%s:%s:%s:%d] Got notification about application settings update",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())

	c.signalSettingsChangeCh <- struct{}{}
}

func (c *Consumer) initCBBucketConnHandle() {
	metadataBucket := c.producer.MetadataBucket()
	connStr := fmt.Sprintf("http://127.0.0.1:" + c.producer.GetNsServerPort())

	var conn cblib.Client
	var pool cblib.Pool

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), connectBucketOpCallback, c, &conn, connStr)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), poolGetBucketOpCallback, c, &conn, &pool, "default")

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), cbGetBucketOpCallback, c, &pool, metadataBucket)
}

// SignalPlasmaClosed is used by producer instance to signal message from SuperSupervisor
// to under consumer about Closed plasma store instance
func (c *Consumer) SignalPlasmaClosed(vb uint16) {
	logging.Infof("V8CR[%s:%s:%s:%d] vb: %v got signal from parent producer about plasma store instance close",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb)
	c.signalPlasmaClosedCh <- vb
}

// SignalPlasmaTransferFinish is called by parent producer instance to signal consumer
// about timer data transfer completion during rebalance
func (c *Consumer) SignalPlasmaTransferFinish(vb uint16, store *plasma.Plasma) {
	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("V8CR[%s:%s:%s:%d] vb: %v SignalPlasmaTransferFinish: panic and recover, %v, stack trace: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb, r, string(trace))
		}
	}()

	logging.Infof("V8CR[%s:%s:%s:%d] vb: %v got signal from parent producer about plasma timer data transfer finish",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb)
	c.signalPlasmaTransferFinishCh <- &plasmaStoreMsg{vb, store}
}

// SignalCheckpointBlobCleanup called by parent producer instance to signal consumer to clear
// up all checkpoint metadata blobs from metadata bucket. Kicked off at the time of app/lambda
// purge request
func (c *Consumer) SignalCheckpointBlobCleanup() {
	c.stopCheckpointingCh <- struct{}{}

	for vb := range c.vbProcessingStats {
		vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vb)))

		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), deleteOpCallback, c, vbKey)
	}

	logging.Infof("V8CR[%s:%s:%s:%d] Purged all owned checkpoint blobs from metadata bucket: %s",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.metadataBucketHandle.Name)
}

// SignalStopDebugger signal C++ V8 consumer to stop Debugger Agent
func (c *Consumer) SignalStopDebugger() {
	logging.Infof("V8CR[%s:%s:%s:%d] Got signal to stop V8 Debugger Agent",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())

	c.signalStopDebuggerCh <- struct{}{}

	c.stopDebuggerServer()

	// Reset the debugger instance blob
	dInstAddrKey := fmt.Sprintf("%s::%s", c.app.AppName, debuggerInstanceAddr)
	dInstAddrBlob := &common.DebuggerInstanceAddrBlob{}
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), setOpCallback, c, dInstAddrKey, dInstAddrBlob)

	cwd, err := os.Getwd()
	if err != nil {
		logging.Infof("V8CR[%s:%s:%s:%d] Failed to get current working dir, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
		return
	}

	frontendURLFilePath := fmt.Sprintf("%s/%s_frontend.url", cwd, c.app.AppName)
	err = os.Remove(frontendURLFilePath)
	if err != nil {
		logging.Infof("V8CR[%s:%s:%s:%d] Failed to remove frontend.url file, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
	}
}
