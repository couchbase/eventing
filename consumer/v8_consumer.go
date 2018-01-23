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
	"github.com/google/flatbuffers/go"
)

// NewConsumer called by producer to create consumer handle
func NewConsumer(streamBoundary common.DcpStreamBoundary, cleanupTimers, enableRecursiveMutation bool,
	executionTimeout, index, lcbInstCapacity, skipTimerThreshold, sockWriteBatchSize int,
	cronTimersPerDoc, timerProcessingPoolSize, cppWorkerThrCount, vbOwnershipGiveUpRoutineCount int,
	curlTimeout int64, vbOwnershipTakeoverRoutineCount, xattrEntryPruneThreshold int, workerQueueCap int64,
	bucket, eventingAdminPort, eventingDir, logLevel, ipcType, tcpPort, uuid string,
	eventingNodeUUIDs []string, vbnos []uint16, app *common.AppConfig, dcpConfig map[string]interface{},
	p common.EventingProducer, s common.EventingSuperSup, vbPlasmaStore *plasma.Plasma,
	socketTimeout time.Duration, diagDir string, numVbuckets, fuzzOffset int) *Consumer {

	var b *couchbase.Bucket
	consumer := &Consumer{
		addCronTimerStopCh:              make(chan struct{}, 1),
		app:                             app,
		aggDCPFeed:                      make(chan *memcached.DcpEvent, dcpConfig["dataChanSize"].(int)),
		bucket:                          bucket,
		cbBucket:                        b,
		checkpointInterval:              checkpointInterval,
		cleanupTimers:                   cleanupTimers,
		clusterStateChangeNotifCh:       make(chan struct{}, ClusterChangeNotifChBufSize),
		cppThrPartitionMap:              make(map[int][]uint16),
		cppWorkerThrCount:               cppWorkerThrCount,
		crcTable:                        crc32.MakeTable(crc32.Castagnoli),
		cronTimersPerDoc:                cronTimersPerDoc,
		curlTimeout:                     curlTimeout,
		connMutex:                       &sync.RWMutex{},
		dcpConfig:                       dcpConfig,
		dcpFeedCancelChs:                make([]chan struct{}, 0),
		dcpFeedVbMap:                    make(map[*couchbase.DcpFeed][]uint16),
		dcpStreamBoundary:               streamBoundary,
		debuggerStarted:                 false,
		diagDir:                         diagDir,
		docTimerEntryCh:                 make(chan *byTimer, dcpConfig["genChanSize"].(int)),
		enableRecursiveMutation:         enableRecursiveMutation,
		eventingAdminPort:               eventingAdminPort,
		eventingDir:                     eventingDir,
		eventingNodeUUIDs:               eventingNodeUUIDs,
		executionTimeout:                executionTimeout,
		fuzzOffset:                      fuzzOffset,
		gracefulShutdownChan:            make(chan struct{}, 1),
		ipcType:                         ipcType,
		hostDcpFeedRWMutex:              &sync.RWMutex{},
		kvHostDcpFeedMap:                make(map[string]*couchbase.DcpFeed),
		lcbInstCapacity:                 lcbInstCapacity,
		logLevel:                        logLevel,
		msgProcessedRWMutex:             &sync.RWMutex{},
		cleanupCronTimerCh:              make(chan *cronTimerToCleanup, dcpConfig["genChanSize"].(int)),
		cleanupCronTimerStopCh:          make(chan struct{}, 1),
		cronTimerEntryCh:                make(chan *timerMsg, dcpConfig["genChanSize"].(int)),
		cronTimerStopCh:                 make(chan struct{}, 1),
		numVbuckets:                     numVbuckets,
		opsTimestamp:                    time.Now(),
		plasmaStoreCh:                   make(chan *plasmaStoreEntry, dcpConfig["genChanSize"].(int)),
		plasmaStoreStopCh:               make(chan struct{}, 1),
		producer:                        p,
		restartVbDcpStreamTicker:        time.NewTicker(restartVbDcpStreamTickInterval),
		sendMsgBufferRWMutex:            &sync.RWMutex{},
		sendMsgCounter:                  0,
		sendMsgToDebugger:               false,
		signalBootstrapFinishCh:         make(chan struct{}, 1),
		signalConnectedCh:               make(chan struct{}, 1),
		signalDebugBlobDebugStopCh:      make(chan struct{}, 1),
		signalInstBlobCasOpFinishCh:     make(chan struct{}, 1),
		signalSettingsChangeCh:          make(chan struct{}, 1),
		signalStartDebuggerCh:           make(chan struct{}, 1),
		signalStopDebuggerCh:            make(chan struct{}, 1),
		signalStopDebuggerRoutineCh:     make(chan struct{}, 1),
		signalUpdateDebuggerInstBlobCh:  make(chan struct{}, 1),
		skipTimerThreshold:              skipTimerThreshold,
		socketTimeout:                   socketTimeout,
		socketWriteBatchSize:            sockWriteBatchSize,
		socketWriteLoopStopAckCh:        make(chan struct{}, 1),
		socketWriteLoopStopCh:           make(chan struct{}, 1),
		socketWriteTicker:               time.NewTicker(socketWriteTimerInterval),
		statsRWMutex:                    &sync.RWMutex{},
		statsTicker:                     time.NewTicker(statsTickInterval),
		stopControlRoutineCh:            make(chan struct{}, 1),
		stopVbOwnerGiveupCh:             make(chan struct{}, vbOwnershipGiveUpRoutineCount),
		stopVbOwnerTakeoverCh:           make(chan struct{}, vbOwnershipTakeoverRoutineCount),
		superSup:                        s,
		tcpPort:                         tcpPort,
		timerCleanupStopCh:              make(chan struct{}, 1),
		timerProcessingRWMutex:          &sync.RWMutex{},
		timerRWMutex:                    &sync.RWMutex{},
		timerProcessingTickInterval:     timerProcessingTickInterval,
		timerProcessingWorkerCount:      timerProcessingPoolSize,
		timerProcessingVbsWorkerMap:     make(map[uint16]*timerProcessingWorker),
		timerProcessingRunningWorkers:   make([]*timerProcessingWorker, 0),
		timerProcessingWorkerSignalCh:   make(map[*timerProcessingWorker]chan struct{}),
		updateStatsTicker:               time.NewTicker(updateCPPStatsTickInterval),
		uuid:                            uuid,
		vbDcpFeedMap:                    make(map[uint16]*couchbase.DcpFeed),
		vbFlogChan:                      make(chan *vbFlogEntry),
		vbnos:                           vbnos,
		updateStatsStopCh:               make(chan struct{}, 1),
		vbDcpEventsRemaining:            make(map[int]int64),
		vbOwnershipGiveUpRoutineCount:   vbOwnershipGiveUpRoutineCount,
		vbOwnershipTakeoverRoutineCount: vbOwnershipTakeoverRoutineCount,
		vbPlasmaStore:                   vbPlasmaStore,
		vbProcessingStats:               newVbProcessingStats(app.AppName, uint16(numVbuckets)),
		vbsRemainingToGiveUp:            make([]uint16, 0),
		vbsRemainingToOwn:               make([]uint16, 0),
		vbsRemainingToRestream:          make([]uint16, 0),
		vbsStreamClosed:                 make(map[uint16]bool),
		vbsStreamClosedRWMutex:          &sync.RWMutex{},
		vbStreamRequested:               make(map[uint16]struct{}),
		vbsStreamRRWMutex:               &sync.RWMutex{},
		workerName:                      fmt.Sprintf("worker_%s_%d", app.AppName, index),
		workerQueueCap:                  workerQueueCap,
		xattrEntryPruneThreshold:        xattrEntryPruneThreshold,
	}

	consumer.builderPool = &sync.Pool{
		New: func() interface{} {
			return flatbuffers.NewBuilder(0)
		},
	}

	return consumer
}

// Serve acts as init routine for consumer handle
func (c *Consumer) Serve() {
	// Insert an entry to sendMessage loop control channel to signify a normal bootstrap
	c.socketWriteLoopStopAckCh <- struct{}{}

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

	util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getKvNodesFromVbMap, c)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), commonConnectBucketOpCallback, c, &c.cbBucket)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), gocbConnectBucketCallback, c)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), gocbConnectMetaBucketCallback, c)

	var flogs couchbase.FailoverLog
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getFailoverLogOpCallback, c, &flogs)

	sort.Sort(util.Uint16Slice(c.vbnos))
	logging.Infof("V8CR[%s:%s:%s:%d] vbnos len: %d",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), len(c.vbnos))

	util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

	logging.Infof("V8CR[%s:%s:%s:%d] Spawning worker corresponding to producer, node addr: %v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.HostPortAddr())

	var feedName couchbase.DcpFeedName

	util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getKvNodesFromVbMap, c)
	kvHostPorts := c.kvNodes
	for _, kvHostPort := range kvHostPorts {
		feedName = couchbase.DcpFeedName("eventing:" + c.HostPortAddr() + "_" + kvHostPort + "_" + c.workerName)

		c.hostDcpFeedRWMutex.Lock()
		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), startDCPFeedOpCallback, c, feedName, kvHostPort)

		cancelCh := make(chan struct{}, 1)
		c.dcpFeedCancelChs = append(c.dcpFeedCancelChs, cancelCh)

		c.addToAggChan(c.kvHostDcpFeedMap[kvHostPort], cancelCh)
		c.hostDcpFeedRWMutex.Unlock()
	}

	c.client = newClient(c, c.app.AppName, c.tcpPort, c.workerName, c.eventingAdminPort)
	c.clientSupToken = c.consumerSup.Add(c.client)

	c.cronCurrTimer = fmt.Sprintf("%s::%s", c.app.AppName, time.Now().UTC().Format(time.RFC3339))
	c.cronNextTimer = fmt.Sprintf("%s::%s", c.app.AppName, time.Now().UTC().Add(time.Second).Format(time.RFC3339))

	c.docCurrTimer = time.Now().UTC().Format(time.RFC3339)
	c.docNextTimer = time.Now().UTC().Add(time.Second).Format(time.RFC3339)

	c.startDcp(flogs)

	// Initialises timer processing worker instances
	c.vbTimerProcessingWorkerAssign(true)

	// doc_id timer events
	for _, r := range c.timerProcessingRunningWorkers {
		go r.processTimerEvents(c.docCurrTimer, c.docNextTimer)
	}

	go c.cleanupProcessesedDocTimers()

	go c.processCronTimerEvents(c.cronCurrTimer, c.cronNextTimer)

	go c.addCronTimersToCleanup()

	go c.cleanupProcessedCronTimers()

	go c.updateWorkerStats()

	go c.doLastSeqNoCheckpoint()

	// V8 Debugger polling routine
	go c.pollForDebuggerStart()

	c.signalBootstrapFinishCh <- struct{}{}

	c.controlRoutine()

	logging.Debugf("V8CR[%s:%s:%s:%d] Exiting consumer init routine",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())
}

// HandleV8Worker sets up CPP V8 worker post its bootstrap
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

	// TODO : Remove rbac user once RBAC issue is resolved
	payload, pBuilder := c.makeV8InitPayload(c.producer.RbacUser(), c.producer.RbacPass(), c.app.AppName, currHost, c.eventingDir, c.eventingAdminPort,
		c.kvNodes[0], c.producer.CfgData(), c.lcbInstCapacity,
		c.cronTimersPerDoc, c.executionTimeout, c.fuzzOffset, int(c.checkpointInterval.Nanoseconds()/(1000*1000)),
		c.enableRecursiveMutation, false, c.curlTimeout)
	logging.Debugf("V8CR[%s:%s:%s:%d] V8 worker init enable_recursive_mutation flag: %v",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.enableRecursiveMutation)

	c.sendInitV8Worker(payload, false, pBuilder)

	c.sendLoadV8Worker(c.app.AppCode, false)

	c.sendGetSourceMap(false)
	c.sendGetHandlerCode(false)

	go c.storeTimerEventLoop()

	go c.processEvents()

}

// Stop acts terminate routine for consumer handle
func (c *Consumer) Stop() {
	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("V8CR[%s:%s:%s:%d] Consumer stop routine, recover %v stack trace: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
		}
	}()

	logging.Infof("V8CR[%s:%s:%s:%d] Gracefully shutting down consumer routine",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())

	for k := range c.timerProcessingWorkerSignalCh {
		k.stopCh <- struct{}{}
	}

	c.cbBucket.Close()
	c.gocbBucket.Close()
	c.gocbMetaBucket.Close()

	c.consumerSup.Remove(c.timerTransferSupToken)
	c.consumerSup.Remove(c.clientSupToken)
	c.consumerSup.Stop()

	c.checkpointTicker.Stop()
	c.restartVbDcpStreamTicker.Stop()
	c.statsTicker.Stop()

	c.addCronTimerStopCh <- struct{}{}
	c.cleanupCronTimerStopCh <- struct{}{}
	c.socketWriteLoopStopCh <- struct{}{}
	<-c.socketWriteLoopStopAckCh
	c.socketWriteTicker.Stop()
	c.timerCleanupStopCh <- struct{}{}

	c.updateStatsTicker.Stop()
	c.updateStatsStopCh <- struct{}{}

	c.plasmaStoreStopCh <- struct{}{}
	c.stopCheckpointingCh <- struct{}{}
	c.cronTimerStopCh <- struct{}{}
	c.stopControlRoutineCh <- struct{}{}
	c.stopConsumerCh <- struct{}{}
	c.signalStopDebuggerRoutineCh <- struct{}{}

	for _, cancelCh := range c.dcpFeedCancelChs {
		cancelCh <- struct{}{}
	}

	for _, dcpFeed := range c.kvHostDcpFeedMap {
		dcpFeed.Close()
	}

	close(c.aggDCPFeed)

	if c.conn != nil {
		c.conn.Close()
	}

	if c.debugClient != nil {
		c.debugConn.Close()
		c.debugListener.Close()
	}
}

// Implement fmt.Stringer interface to allow better debugging
// if C++ V8 worker crashes
func (c *Consumer) String() string {
	countMsg, _, _ := util.SprintDCPCounts(c.dcpMessagesProcessed)
	return fmt.Sprintf("consumer => app: %s name: %v tcpPort: %s ospid: %d"+
		" dcpEventProcessed: %s v8EventProcessed: %s", c.app.AppName, c.ConsumerName(),
		c.tcpPort, c.Pid(), countMsg, util.SprintV8Counts(c.v8WorkerMessagesProcessed))
}

// NotifyClusterChange is called by producer handle to signify each
// consumer instance about StartTopologyChange rpc call from cbauth service.Manager
func (c *Consumer) NotifyClusterChange() {
	logging.Infof("V8CR[%s:%s:%s:%d] Got notification about cluster state change",
		c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid())

	c.clusterStateChangeNotifCh <- struct{}{}
}

// NotifyRebalanceStop is called by producer to signal stopping of
// rebalance operation
func (c *Consumer) NotifyRebalanceStop() {
	logging.Infof("V8CR[%s:%s:%s:%d] Got notification about rebalance stop",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())

	c.isRebalanceOngoing = false

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

func (c *Consumer) getBuilder() *flatbuffers.Builder {
	return c.builderPool.Get().(*flatbuffers.Builder)
}

func (c *Consumer) putBuilder(b *flatbuffers.Builder) {
	b.Reset()
	c.builderPool.Put(b)
}
