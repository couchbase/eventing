package consumer

import (
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/plasma"
	"github.com/google/flatbuffers/go"
)

// NewConsumer called by producer to create consumer handle
func NewConsumer(hConfig *common.HandlerConfig, pConfig *common.ProcessConfig, rConfig *common.RebalanceConfig,
	index int, uuid string, eventingNodeUUIDs []string, vbnos []uint16, app *common.AppConfig,
	dcpConfig map[string]interface{}, p common.EventingProducer, s common.EventingSuperSup, vbPlasmaStore *plasma.Plasma,
	iteratorRefreshCounter, numVbuckets int) *Consumer {

	var b *couchbase.Bucket
	consumer := &Consumer{
		addCronTimerStopCh:              make(chan struct{}, 1),
		app:                             app,
		aggDCPFeed:                      make(chan *memcached.DcpEvent, dcpConfig["dataChanSize"].(int)),
		aggDCPFeedMemCap:                hConfig.AggDCPFeedMemCap,
		breakpadOn:                      pConfig.BreakpadOn,
		bucket:                          hConfig.SourceBucket,
		cbBucket:                        b,
		checkpointInterval:              time.Duration(hConfig.CheckpointInterval) * time.Millisecond,
		cleanupCronTimerCh:              make(chan *cronTimerToCleanup, dcpConfig["genChanSize"].(int)),
		cleanupCronTimerStopCh:          make(chan struct{}, 1),
		cleanupTimers:                   hConfig.CleanupTimers,
		clusterStateChangeNotifCh:       make(chan struct{}, ClusterChangeNotifChBufSize),
		connMutex:                       &sync.RWMutex{},
		cppThrPartitionMap:              make(map[int][]uint16),
		cppWorkerThrCount:               hConfig.CPPWorkerThrCount,
		crcTable:                        crc32.MakeTable(crc32.Castagnoli),
		cronTimerEntryCh:                make(chan *timerMsg, dcpConfig["genChanSize"].(int)),
		cronTimersPerDoc:                hConfig.CronTimersPerDoc,
		cronTimerStopCh:                 make(chan struct{}, 1),
		curlTimeout:                     hConfig.CurlTimeout,
		dcpConfig:                       dcpConfig,
		dcpFeedCancelChs:                make([]chan struct{}, 0),
		dcpFeedVbMap:                    make(map[*couchbase.DcpFeed][]uint16),
		dcpStreamBoundary:               hConfig.StreamBoundary,
		debuggerStarted:                 false,
		diagDir:                         pConfig.DiagDir,
		docTimerEntryCh:                 make(chan *byTimer, dcpConfig["genChanSize"].(int)),
		docTimerProcessingStopCh:        make(chan struct{}, 1),
		enableRecursiveMutation:         hConfig.EnableRecursiveMutation,
		eventingAdminPort:               pConfig.EventingPort,
		eventingSSLPort:                 pConfig.EventingSSLPort,
		eventingDir:                     pConfig.EventingDir,
		eventingNodeUUIDs:               eventingNodeUUIDs,
		executionTimeout:                hConfig.ExecutionTimeout,
		feedbackQueueCap:                hConfig.FeedbackQueueCap,
		feedbackReadBufferSize:          hConfig.FeedbackReadBufferSize,
		feedbackTCPPort:                 pConfig.FeedbackSockIdentifier,
		feedbackWriteBatchSize:          hConfig.FeedbackBatchSize,
		fuzzOffset:                      hConfig.FuzzOffset,
		gracefulShutdownChan:            make(chan struct{}, 1),
		index:                           index,
		ipcType:                         pConfig.IPCType,
		iteratorRefreshCounter:          iteratorRefreshCounter,
		hostDcpFeedRWMutex:              &sync.RWMutex{},
		kvHostDcpFeedMap:                make(map[string]*couchbase.DcpFeed),
		lcbInstCapacity:                 hConfig.LcbInstCapacity,
		logLevel:                        hConfig.LogLevel,
		msgProcessedRWMutex:             &sync.RWMutex{},
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
		signalFeedbackConnectedCh:       make(chan struct{}, 1),
		signalInstBlobCasOpFinishCh:     make(chan struct{}, 1),
		signalSettingsChangeCh:          make(chan struct{}, 1),
		signalStartDebuggerCh:           make(chan struct{}, 1),
		signalStopDebuggerCh:            make(chan struct{}, 1),
		signalStopDebuggerRoutineCh:     make(chan struct{}, 1),
		signalUpdateDebuggerInstBlobCh:  make(chan struct{}, 1),
		skipTimerThreshold:              hConfig.SkipTimerThreshold,
		socketTimeout:                   time.Duration(hConfig.SocketTimeout) * time.Second,
		socketWriteBatchSize:            hConfig.SocketWriteBatchSize,
		socketWriteLoopStopAckCh:        make(chan struct{}, 1),
		socketWriteLoopStopCh:           make(chan struct{}, 1),
		socketWriteTicker:               time.NewTicker(socketWriteTimerInterval),
		statsRWMutex:                    &sync.RWMutex{},
		statsTickDuration:               time.Duration(hConfig.StatsLogInterval) * time.Millisecond,
		stopControlRoutineCh:            make(chan struct{}, 1),
		stopHandleFailoverLogCh:         make(chan struct{}, 1),
		stopVbOwnerGiveupCh:             make(chan struct{}, rConfig.VBOwnershipGiveUpRoutineCount),
		stopVbOwnerTakeoverCh:           make(chan struct{}, rConfig.VBOwnershipTakeoverRoutineCount),
		superSup:                        s,
		tcpPort:                         pConfig.SockIdentifier,
		timerCleanupStopCh:              make(chan struct{}, 1),
		timerProcessingTickInterval:     time.Duration(hConfig.TimerProcessingTickInterval) * time.Millisecond,
		updateStatsTicker:               time.NewTicker(updateCPPStatsTickInterval),
		uuid:                            uuid,
		vbDcpFeedMap:                    make(map[uint16]*couchbase.DcpFeed),
		vbFlogChan:                      make(chan *vbFlogEntry),
		vbnos:                           vbnos,
		updateStatsStopCh:               make(chan struct{}, 1),
		vbDcpEventsRemaining:            make(map[int]int64),
		vbOwnershipGiveUpRoutineCount:   rConfig.VBOwnershipGiveUpRoutineCount,
		vbOwnershipTakeoverRoutineCount: rConfig.VBOwnershipTakeoverRoutineCount,
		vbPlasmaStore:                   vbPlasmaStore,
		vbsRemainingToClose:             make([]uint16, 0),
		vbsRemainingToGiveUp:            make([]uint16, 0),
		vbsRemainingToOwn:               make([]uint16, 0),
		vbsRemainingToRestream:          make([]uint16, 0),
		vbsStreamClosed:                 make(map[uint16]bool),
		vbsStreamClosedRWMutex:          &sync.RWMutex{},
		vbStreamRequested:               make(map[uint16]struct{}),
		vbsStreamRRWMutex:               &sync.RWMutex{},
		workerName:                      fmt.Sprintf("worker_%s_%d", app.AppName, index),
		workerQueueCap:                  hConfig.WorkerQueueCap,
		workerQueueMemCap:               hConfig.WorkerQueueMemCap,
		xattrEntryPruneThreshold:        hConfig.XattrEntryPruneThreshold,
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
	logPrefix := "Consumer::Serve"

	c.cronCurrTimer = time.Now().Add(-time.Second * 10).UTC().Format(time.RFC3339)
	c.cronNextTimer = time.Now().Add(-time.Second * 10).UTC().Format(time.RFC3339)

	c.docCurrTimer = time.Now().UTC().Format(time.RFC3339)
	c.docNextTimer = time.Now().UTC().Add(time.Second).Format(time.RFC3339)

	c.vbProcessingStats = newVbProcessingStats(c.app.AppName, uint16(c.numVbuckets), c.NodeUUID(), c.workerName)
	c.statsTicker = time.NewTicker(c.statsTickDuration)

	// Insert an entry to sendMessage loop control channel to signify a normal bootstrap
	c.socketWriteLoopStopAckCh <- struct{}{}

	c.stopConsumerCh = make(chan struct{}, 1)
	c.stopCheckpointingCh = make(chan struct{}, 1)

	c.dcpMessagesProcessed = make(map[mcd.CommandCode]uint64)
	c.v8WorkerMessagesProcessed = make(map[string]uint64)

	c.consumerSup = suptree.NewSimple(c.workerName)
	go c.consumerSup.ServeBackground()

	c.cppWorkerThrPartitionMap()

	util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getKvNodesFromVbMap, c)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), commonConnectBucketOpCallback, c, &c.cbBucket)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), gocbConnectBucketCallback, c)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), gocbConnectMetaBucketCallback, c)

	var flogs couchbase.FailoverLog
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getFailoverLogOpCallback, c, &flogs)

	go c.handleFailoverLog()

	sort.Sort(util.Uint16Slice(c.vbnos))
	logging.Infof("%s [%s:%s:%d] vbnos len: %d dump: %s",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), len(c.vbnos), util.Condense(c.vbnos))

	util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

	logging.Infof("%s [%s:%s:%d] Spawning worker corresponding to producer, node addr: %rs",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.HostPortAddr())

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

	c.client = newClient(c, c.app.AppName, c.tcpPort, c.feedbackTCPPort, c.workerName, c.eventingAdminPort)
	c.clientSupToken = c.consumerSup.Add(c.client)

	c.doCleanupForPreviouslyOwnedVbs()

	c.startDcp(flogs)

	logging.Infof("%s [%s:%s:%d] docCurrTimer: %s docNextTimer: %v cronCurrTimer: %v cronNextTimer: %v",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.docCurrTimer, c.docNextTimer, c.cronCurrTimer, c.cronNextTimer)

	// doc_id timer events
	go c.processDocTimerEvents()

	go c.cleanupProcessedDocTimers()

	go c.processCronTimerEvents()

	go c.addCronTimersToCleanup()

	go c.cleanupProcessedCronTimers()

	go c.updateWorkerStats()

	go c.doLastSeqNoCheckpoint()

	// V8 Debugger polling routine
	go c.pollForDebuggerStart()

	c.signalBootstrapFinishCh <- struct{}{}

	c.controlRoutine()

	logging.Debugf("%s [%s:%s:%d] Exiting consumer init routine",
		logPrefix, c.workerName, c.tcpPort, c.Pid())
}

// HandleV8Worker sets up CPP V8 worker post its bootstrap
func (c *Consumer) HandleV8Worker() {
	logPrefix := "Consumer::HandleV8Worker"

	<-c.signalConnectedCh
	<-c.signalFeedbackConnectedCh

	logging.SetLogLevel(util.GetLogLevel(c.logLevel))
	c.sendLogLevel(c.logLevel, false)
	c.sendWorkerThrMap(nil, false)
	c.sendWorkerThrCount(0, false)

	util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

	currHost := util.Localhost()
	h := c.HostPortAddr()
	if h != "" {
		var err error
		currHost, _, err = net.SplitHostPort(h)
		if err != nil {
			logging.Errorf("%s Unable to split hostport %rs: %v", logPrefix, h, err)
		}
	}

	payload, pBuilder := c.makeV8InitPayload(c.app.AppName, currHost, c.eventingDir, c.eventingAdminPort, c.eventingSSLPort,
		c.kvNodes[0], c.producer.CfgData(), c.lcbInstCapacity,
		c.cronTimersPerDoc, c.executionTimeout, c.fuzzOffset, int(c.checkpointInterval.Nanoseconds()/(1000*1000)),
		c.enableRecursiveMutation, false, c.curlTimeout)
	logging.Infof("%s [%s:%s:%d] V8 worker init enable_recursive_mutation flag: %t",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.enableRecursiveMutation)

	c.sendInitV8Worker(payload, false, pBuilder)

	c.sendLoadV8Worker(c.app.AppCode, false)

	c.sendGetSourceMap(false)
	c.sendGetHandlerCode(false)

	c.workerExited = false

	go c.storeDocTimerEventLoop()

	go c.processEvents()
}

// Stop acts terminate routine for consumer handle
func (c *Consumer) Stop() {
	logPrefix := "Consumer::Stop"

	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("%s [%s:%s:%d] Consumer stop routine, recover %rm stack trace: %rm",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
		}
	}()

	logging.Infof("%s [%s:%s:%d] Gracefully shutting down consumer routine",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.docTimerProcessingStopCh <- struct{}{}

	c.cbBucket.Close()
	c.gocbBucket.Close()
	c.gocbMetaBucket.Close()
	logging.Infof("%s [%s:%s:%d] Issued close for go-couchbase and gocb handles",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.consumerSup.Remove(c.clientSupToken)
	logging.Infof("%s [%s:%s:%d] Requested to remove supervision of eventing-consumer",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.checkpointTicker.Stop()
	c.restartVbDcpStreamTicker.Stop()
	c.statsTicker.Stop()
	logging.Infof("%s [%s:%s:%d] Stopped checkpoint, restart vb dcp stream and stats tickers",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.addCronTimerStopCh <- struct{}{}
	c.cleanupCronTimerStopCh <- struct{}{}
	c.socketWriteLoopStopCh <- struct{}{}
	<-c.socketWriteLoopStopAckCh
	c.socketWriteTicker.Stop()
	c.timerCleanupStopCh <- struct{}{}
	logging.Infof("%s [%s:%s:%d] Sent signal over channel to stop cron, doc routines",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.updateStatsTicker.Stop()
	c.updateStatsStopCh <- struct{}{}
	logging.Infof("%s [%s:%s:%d] Sent signal to stop cpp worker stat collection routine",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.plasmaStoreStopCh <- struct{}{}
	c.stopCheckpointingCh <- struct{}{}
	c.cronTimerStopCh <- struct{}{}
	c.stopControlRoutineCh <- struct{}{}
	c.stopConsumerCh <- struct{}{}
	c.signalStopDebuggerRoutineCh <- struct{}{}
	logging.Infof("%s [%s:%s:%d] Sent signal over channel to stop plasma store, checkpointing, cron timer processing routines",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	for _, dcpFeed := range c.kvHostDcpFeedMap {
		dcpFeed.Close()
	}
	logging.Infof("%s [%s:%s:%d] Closed all dcpfeed handles",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	for _, cancelCh := range c.dcpFeedCancelChs {
		cancelCh <- struct{}{}
	}
	logging.Infof("%s [%s:%s:%d] Sent signal over channel to stop dcp event forwarding routine",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.stopHandleFailoverLogCh <- struct{}{}

	close(c.aggDCPFeed)
	logging.Infof("%s [%s:%s:%d] Closing up aggDcpFeed channel",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	if c.conn != nil {
		c.conn.Close()
	}

	if c.debugClient != nil {
		c.debugConn.Close()
		c.debugListener.Close()
	}

	c.consumerSup.Stop()
	logging.Infof("%s [%s:%s:%d] Requested to stop supervisor for Eventing.Consumer",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	logging.Infof("%s [%s:%s:%d] Exiting Eventing.Consumer Stop routine",
		logPrefix, c.workerName, c.tcpPort, c.Pid())
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
	logPrefix := "Consumer::NotifyClusterChange"

	logging.Infof("%s [%s:%s:%d] Got notification about cluster state change",
		logPrefix, c.ConsumerName(), c.tcpPort, c.Pid())

	c.clusterStateChangeNotifCh <- struct{}{}
}

// NotifyRebalanceStop is called by producer to signal stopping of
// rebalance operation
func (c *Consumer) NotifyRebalanceStop() {
	logPrefix := "Consumer::NotifyRebalanceStop"

	logging.Infof("%s [%s:%s:%d] Got notification about rebalance stop",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.isRebalanceOngoing = false
	logging.Infof("%s [%s:%s:%d] Updated isRebalanceOngoing to %v",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.isRebalanceOngoing)

	for i := 0; i < c.vbOwnershipGiveUpRoutineCount; i++ {
		c.stopVbOwnerGiveupCh <- struct{}{}
	}

	for i := 0; i < c.vbOwnershipTakeoverRoutineCount; i++ {
		c.stopVbOwnerTakeoverCh <- struct{}{}
	}
}

// NotifySettingsChange signals consumer instance of settings update
func (c *Consumer) NotifySettingsChange() {
	logPrefix := "Consumer::NotifySettingsChange"

	logging.Infof("%s [%s:%s:%d] Got notification about application settings update",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.signalSettingsChangeCh <- struct{}{}
}

// SignalStopDebugger signal C++ V8 consumer to stop Debugger Agent
func (c *Consumer) SignalStopDebugger() {
	logPrefix := "Consumer::SignalStopDebugger"

	logging.Infof("%s [%s:%s:%d] Got signal to stop V8 Debugger Agent",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.signalStopDebuggerCh <- struct{}{}

	c.stopDebuggerServer()

	// Reset the debugger instance blob
	dInstAddrKey := fmt.Sprintf("%s::%s", c.app.AppName, debuggerInstanceAddr)
	dInstAddrBlob := &common.DebuggerInstanceAddrBlob{}
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), setOpCallback, c, dInstAddrKey, dInstAddrBlob)

	frontendURLFilePath := fmt.Sprintf("%s/%s_frontend.url", c.eventingDir, c.app.AppName)
	err := os.Remove(frontendURLFilePath)
	if err != nil {
		logging.Infof("%s [%s:%s:%d] Failed to remove frontend.url file, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
	}
}

func (c *Consumer) getBuilder() *flatbuffers.Builder {
	return c.builderPool.Get().(*flatbuffers.Builder)
}

func (c *Consumer) putBuilder(b *flatbuffers.Builder) {
	b.Reset()
	c.builderPool.Put(b)
}
