package consumer

import (
	"fmt"
	"hash/crc32"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/timer_transfer"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/plasma"
)

// NewConsumer called by producer to create consumer handle
func NewConsumer(streamBoundary common.DcpStreamBoundary, cleanupTimers, enableRecursiveMutation bool,
	executionTimeout, index, lcbInstCapacity, skipTimerThreshold, sockWriteBatchSize, timerProcessingPoolSize int,
	cppWorkerThrCount, vbOwnershipGiveUpRoutineCount, vbOwnershipTakeoverRoutineCount int,
	bucket, eventingAdminPort, eventingDir, logLevel, ipcType, tcpPort, uuid string,
	eventingNodeUUIDs []string, vbnos []uint16, app *common.AppConfig,
	p common.EventingProducer, s common.EventingSuperSup, vbPlasmaStore *plasma.Plasma,
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
		cppThrPartitionMap:                 make(map[int][]uint16),
		cppWorkerThrCount:                  cppWorkerThrCount,
		crcTable:                           crc32.MakeTable(crc32.Castagnoli),
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
		ipcType:                            ipcType,
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
		signalBootstrapFinishCh:            make(chan struct{}, 1),
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
		vbPlasmaStore:                   vbPlasmaStore,
		vbPlasmaReader:                  make(map[uint16]*plasma.Writer),
		vbPlasmaWriter:                  make(map[uint16]*plasma.Writer),
		vbProcessingStats:               newVbProcessingStats(app.AppName),
		vbsRemainingToGiveUp:            make([]uint16, 0),
		vbsRemainingToOwn:               make([]uint16, 0),
		vbsRemainingToRestream:          make([]uint16, 0),
		workerName:                      fmt.Sprintf("worker_%s_%d", app.AppName, index),
		writeBatchSeqnoMap:              make(map[uint16]uint64),
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

	c.cppWorkerThrPartitionMap()

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

	c.client = newClient(c, c.app.AppName, c.tcpPort, c.workerName, c.eventingAdminPort)
	c.clientSupToken = c.consumerSup.Add(c.client)

	c.startDcp(dcpConfig, flogs)

	// Initialises timer processing worker instances
	c.vbTimerProcessingWorkerAssign(true)

	// go c.plasmaPersistAll()

	// doc_id timer events
	for _, r := range c.timerProcessingRunningWorkers {
		go r.processTimerEvents()
	}

	// non doc_id timer events
	go c.processNonDocTimerEvents()

	// V8 Debugger polling routine
	go c.pollForDebuggerStart()

	c.signalBootstrapFinishCh <- struct{}{}

	c.controlRoutine()

	logging.Debugf("V8CR[%s:%s:%s:%d] Exiting consumer init routine",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())
}

// HandleV8Worker sets up CPP V8 worker post it's bootstrap
func (c *Consumer) HandleV8Worker() {
	<-c.signalConnectedCh

	logging.SetLogLevel(util.GetLogLevel(c.logLevel))
	c.sendLogLevel(c.logLevel, false)
	c.sendWorkerThrMap(nil, false)
	c.sendWorkerThrCount(0, false)

	util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

	var currHost string
	h := c.HostPortAddr()
	if h != "" {
		currHost = strings.Split(h, ":")[0]
	} else {
		currHost = "127.0.0.1"
	}

	var user, password string
	util.Retry(util.NewFixedBackoff(time.Second), getMemcachedServiceAuth, c.producer.KvHostPorts()[0], &user, &password)

	payload := makeV8InitPayload(c.app.AppName, currHost, c.eventingDir, c.eventingAdminPort,
		c.producer.KvHostPorts()[0], c.producer.CfgData(), user, password, c.lcbInstCapacity,
		c.executionTimeout, c.enableRecursiveMutation)
	logging.Debugf("V8CR[%s:%s:%s:%d] V8 worker init enable_recursive_mutation flag: %v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.enableRecursiveMutation)

	c.sendInitV8Worker(payload, false)

	c.sendLoadV8Worker(c.app.AppCode, false)

	c.sendGetSourceMap(false)
	c.sendGetHandlerCode(false)

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
	if c.debugClient != nil {
		c.debugConn.Close()
		c.debugListener.Close()
	}

	for k := range c.timerProcessingWorkerSignalCh {
		k.stopCh <- struct{}{}
	}

	c.nonDocTimerStopCh <- struct{}{}
	c.stopControlRoutineCh <- struct{}{}
	c.stopPlasmaPersistCh <- struct{}{}
	c.signalStopDebuggerRoutineCh <- struct{}{}

	for _, cancelCh := range c.dcpFeedCancelChs {
		cancelCh <- struct{}{}
	}

	for _, dcpFeed := range c.kvHostDcpFeedMap {
		dcpFeed.Close()
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

// Pid returns the process id of CPP V8 worker
func (c *Consumer) Pid() int {
	pid, ok := c.osPid.Load().(int)
	if ok {
		return pid
	}
	return 0
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

	frontendURLFilePath := fmt.Sprintf("%s/%s_frontend.url", c.eventingDir, c.app.AppName)
	err := os.Remove(frontendURLFilePath)
	if err != nil {
		logging.Infof("V8CR[%s:%s:%s:%d] Failed to remove frontend.url file, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
	}
}
