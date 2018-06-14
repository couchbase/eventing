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
	iteratorRefreshCounter, numVbuckets int, retryCount *int64, vbEventingNodeAssignMap map[uint16]string,
	workerVbucketMap map[string][]uint16) *Consumer {

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
		idleCheckpointInterval:          time.Duration(hConfig.IdleCheckpointInterval) * time.Millisecond,
		cleanupCronTimerCh:              make(chan *cronTimerToCleanup, dcpConfig["genChanSize"].(int)),
		cleanupCronTimerStopCh:          make(chan struct{}, 1),
		cleanupTimers:                   hConfig.CleanupTimers,
		clusterStateChangeNotifCh:       make(chan struct{}, ClusterChangeNotifChBufSize),
		connMutex:                       &sync.RWMutex{},
		controlRoutineWg:                &sync.WaitGroup{},
		cppThrPartitionMap:              make(map[int][]uint16),
		cppWorkerThrCount:               hConfig.CPPWorkerThrCount,
		crcTable:                        crc32.MakeTable(crc32.Castagnoli),
		cronTimerEntryCh:                make(chan *timerMsg, dcpConfig["genChanSize"].(int)),
		cronTimersPerDoc:                hConfig.CronTimersPerDoc,
		cronTimerStopCh:                 make(chan struct{}, 1),
		curlTimeout:                     hConfig.CurlTimeout,
		dcpConfig:                       dcpConfig,
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
		filterVbEvents:                  make(map[uint16]struct{}),
		filterVbEventsRWMutex:           &sync.RWMutex{},
		fuzzOffset:                      hConfig.FuzzOffset,
		gracefulShutdownChan:            make(chan struct{}, 1),
		handlerFooters:                  hConfig.HandlerFooters,
		handlerHeaders:                  hConfig.HandlerHeaders,
		index:                           index,
		ipcType:                         pConfig.IPCType,
		inflightDcpStreams:              make(map[uint16]struct{}),
		inflightDcpStreamsRWMutex:       &sync.RWMutex{},
		iteratorRefreshCounter:          iteratorRefreshCounter,
		hostDcpFeedRWMutex:              &sync.RWMutex{},
		kvHostDcpFeedMap:                make(map[string]*couchbase.DcpFeed),
		kvNodesRWMutex:                  &sync.RWMutex{},
		lcbInstCapacity:                 hConfig.LcbInstCapacity,
		logLevel:                        hConfig.LogLevel,
		msgProcessedRWMutex:             &sync.RWMutex{},
		numVbuckets:                     numVbuckets,
		opsTimestamp:                    time.Now(),
		plasmaStoreCh:                   make(chan *plasmaStoreEntry, dcpConfig["genChanSize"].(int)),
		plasmaStoreStopCh:               make(chan struct{}, 1),
		producer:                        p,
		reqStreamCh:                     make(chan *streamRequestInfo, numVbuckets*10),
		reqStreamResponseCh:             make(chan uint16, numVbuckets*10),
		restartVbDcpStreamTicker:        time.NewTicker(restartVbDcpStreamTickInterval),
		retryCount:                      retryCount,
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
		stopReqStreamProcessCh:          make(chan struct{}, 1),
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
		usingDocTimer:                   hConfig.UsingDocTimer,
		vbDcpEventsRemaining:            make(map[int]int64),
		vbEventingNodeAssignMap:         vbEventingNodeAssignMap,
		vbEventingNodeAssignRWMutex:     &sync.RWMutex{},
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
		vbProcessingStats:               newVbProcessingStats(app.AppName, uint16(numVbuckets), uuid, fmt.Sprintf("worker_%s_%d", app.AppName, index)),
		workerQueueCap:                  hConfig.WorkerQueueCap,
		workerQueueMemCap:               hConfig.WorkerQueueMemCap,
		workerVbucketMap:                workerVbucketMap,
		workerVbucketMapRWMutex:         &sync.RWMutex{},
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

	defer func() {
		if *c.retryCount >= 0 {
			c.signalBootstrapFinishCh <- struct{}{}
			c.producer.RemoveConsumerToken(c.workerName)
		}
	}()

	c.isBootstrapping = true

	c.cronCurrTimer = time.Now().Add(-time.Second * 10).UTC().Format(time.RFC3339)
	c.cronNextTimer = time.Now().Add(-time.Second * 10).UTC().Format(time.RFC3339)

	c.docCurrTimer = time.Now().UTC().Format(time.RFC3339)
	c.docNextTimer = time.Now().UTC().Add(time.Second).Format(time.RFC3339)

	c.statsTicker = time.NewTicker(c.statsTickDuration)
	c.backupVbStats = newVbBackupStats(uint16(c.numVbuckets))

	// Insert an entry to sendMessage loop control channel to signify a normal bootstrap
	c.socketWriteLoopStopAckCh <- struct{}{}

	c.stopConsumerCh = make(chan struct{}, 1)
	c.stopCheckpointingCh = make(chan struct{}, 1)

	c.dcpMessagesProcessed = make(map[mcd.CommandCode]uint64)
	c.v8WorkerMessagesProcessed = make(map[string]uint64)

	c.consumerSup = suptree.NewSimple(c.workerName)
	go c.consumerSup.ServeBackground()

	c.cppWorkerThrPartitionMap()

	err := util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getKvNodesFromVbMap, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, commonConnectBucketOpCallback, c, &c.cbBucket)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, gocbConnectBucketCallback, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, gocbConnectMetaBucketCallback, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	var flogs couchbase.FailoverLog
	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getFailoverLogOpCallback, c, &flogs)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	go c.handleFailoverLog()
	go c.processReqStreamMessages()

	sort.Sort(util.Uint16Slice(c.vbnos))
	logging.Infof("%s [%s:%s:%d] using doc_timer: %t vbnos len: %d dump: %s",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.usingDocTimer, len(c.vbnos), util.Condense(c.vbnos))

	err = util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getEventingNodeAddrOpCallback, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	logging.Infof("%s [%s:%s:%d] Spawning worker corresponding to producer, node addr: %rs",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.HostPortAddr())

	var feedName couchbase.DcpFeedName

	err = util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getKvNodesFromVbMap, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	for _, kvHostPort := range c.getKvNodes() {
		feedName = couchbase.NewDcpFeedName(c.HostPortAddr() + "_" + kvHostPort + "_" + c.workerName)

		c.hostDcpFeedRWMutex.Lock()
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, startDCPFeedOpCallback, c, feedName, kvHostPort)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}

		logging.Infof("%s [%s:%s:%d] vbKvAddr: %s Spawned aggChan routine",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), kvHostPort)

		c.addToAggChan(c.kvHostDcpFeedMap[kvHostPort])
		c.hostDcpFeedRWMutex.Unlock()
	}

	c.client = newClient(c, c.app.AppName, c.tcpPort, c.feedbackTCPPort, c.workerName, c.eventingAdminPort)
	c.clientSupToken = c.consumerSup.Add(c.client)

	err = c.doCleanupForPreviouslyOwnedVbs()
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	c.controlRoutineWg.Add(1)
	go c.controlRoutine()

	err = c.startDcp(flogs)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}
	c.isBootstrapping = false

	logging.Infof("%s [%s:%s:%d] vbsStateUpdateRunning: %t docCurrTimer: %s docNextTimer: %v cronCurrTimer: %v cronNextTimer: %v",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.vbsStateUpdateRunning, c.docCurrTimer, c.docNextTimer, c.cronCurrTimer, c.cronNextTimer)

	if !c.vbsStateUpdateRunning {
		logging.Infof("%s [%s:%s:%d] Kicking off vbsStateUpdate routine",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		go c.vbsStateUpdate()
	}

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

	c.controlRoutineWg.Wait()

	logging.Infof("%s [%s:%s:%d] Exiting consumer init routine",
		logPrefix, c.workerName, c.tcpPort, c.Pid())
}

// HandleV8Worker sets up CPP V8 worker post its bootstrap
func (c *Consumer) HandleV8Worker() error {
	logPrefix := "Consumer::HandleV8Worker"

	<-c.signalConnectedCh
	<-c.signalFeedbackConnectedCh

	logging.SetLogLevel(util.GetLogLevel(c.logLevel))
	c.sendLogLevel(c.logLevel, false)
	c.sendWorkerThrMap(nil, false)
	c.sendWorkerThrCount(0, false)

	err := util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getEventingNodeAddrOpCallback, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return common.ErrRetryTimeout
	}

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
		c.getKvNodes()[0], c.producer.CfgData(), c.lcbInstCapacity,
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
	return nil
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

	c.isTerminateRunning = true

	logging.Infof("%s [%s:%s:%d] Gracefully shutting down consumer routine",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	if c.docTimerProcessingStopCh != nil {
		c.docTimerProcessingStopCh <- struct{}{}
	}

	if c.cbBucket != nil {
		c.cbBucket.Close()
	}

	if c.gocbBucket != nil {
		c.gocbBucket.Close()
	}

	if c.gocbMetaBucket != nil {
		c.gocbMetaBucket.Close()
	}

	logging.Infof("%s [%s:%s:%d] Issued close for go-couchbase and gocb handles",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	if c.consumerSup != nil {
		c.consumerSup.Remove(c.clientSupToken)
	}

	logging.Infof("%s [%s:%s:%d] Requested to remove supervision of eventing-consumer",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	if c.checkpointTicker != nil {
		c.checkpointTicker.Stop()
	}

	if c.restartVbDcpStreamTicker != nil {
		c.restartVbDcpStreamTicker.Stop()
	}

	if c.statsTicker != nil {
		c.statsTicker.Stop()
	}

	logging.Infof("%s [%s:%s:%d] Stopped checkpoint, restart vb dcp stream and stats tickers",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	if c.addCronTimerStopCh != nil {
		c.addCronTimerStopCh <- struct{}{}
	}

	if c.cleanupCronTimerStopCh != nil {
		c.cleanupCronTimerStopCh <- struct{}{}
	}

	if c.socketWriteLoopStopCh != nil {
		c.socketWriteLoopStopCh <- struct{}{}
	}

	if c.socketWriteLoopStopAckCh != nil {
		<-c.socketWriteLoopStopAckCh
	}

	if c.socketWriteTicker != nil {
		c.socketWriteTicker.Stop()
	}

	if c.stopReqStreamProcessCh != nil {
		c.stopReqStreamProcessCh <- struct{}{}
	}

	if c.timerCleanupStopCh != nil {
		c.timerCleanupStopCh <- struct{}{}
	}

	logging.Infof("%s [%s:%s:%d] Sent signal over channel to stop cron, doc routines",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	if c.updateStatsTicker != nil {
		c.updateStatsTicker.Stop()
	}

	if c.updateStatsStopCh != nil {
		c.updateStatsStopCh <- struct{}{}
	}

	logging.Infof("%s [%s:%s:%d] Sent signal to stop cpp worker stat collection routine",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	if c.plasmaStoreStopCh != nil {
		c.plasmaStoreStopCh <- struct{}{}
	}

	if c.stopCheckpointingCh != nil {
		c.stopCheckpointingCh <- struct{}{}
	}

	if c.cronTimerStopCh != nil {
		c.cronTimerStopCh <- struct{}{}
	}

	if c.stopControlRoutineCh != nil {
		c.stopControlRoutineCh <- struct{}{}
	}

	if c.stopConsumerCh != nil {
		c.stopConsumerCh <- struct{}{}
	}

	if c.signalStopDebuggerRoutineCh != nil {
		c.signalStopDebuggerRoutineCh <- struct{}{}
	}

	logging.Infof("%s [%s:%s:%d] Sent signal over channel to stop plasma store, checkpointing, cron timer processing routines",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.dcpFeedsClosed = true

	if c.kvHostDcpFeedMap != nil {
		for _, dcpFeed := range c.kvHostDcpFeedMap {
			if dcpFeed != nil {
				dcpFeed.Close()
			}
		}
	}

	logging.Infof("%s [%s:%s:%d] Closed all dcpfeed handles",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	if c.stopHandleFailoverLogCh != nil {
		c.stopHandleFailoverLogCh <- struct{}{}
	}

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

	if c.consumerSup != nil {
		c.consumerSup.Stop()
	}

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
func (c *Consumer) SignalStopDebugger() error {
	logPrefix := "Consumer::SignalStopDebugger"

	logging.Infof("%s [%s:%s:%d] Got signal to stop V8 Debugger Agent",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.signalStopDebuggerCh <- struct{}{}

	c.stopDebuggerServer()

	// Reset the debugger instance blob
	dInstAddrKey := fmt.Sprintf("%s::%s", c.app.AppName, debuggerInstanceAddr)
	dInstAddrBlob := &common.DebuggerInstanceAddrBlobVer{
		common.DebuggerInstanceAddrBlob{},
		util.EventingVer(),
	}
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, setOpCallback, c, dInstAddrKey, dInstAddrBlob)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return common.ErrRetryTimeout
	}

	frontendURLFilePath := fmt.Sprintf("%s/%s_frontend.url", c.eventingDir, c.app.AppName)
	err = os.Remove(frontendURLFilePath)
	if err != nil {
		logging.Infof("%s [%s:%s:%d] Failed to remove frontend.url file, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
	}

	return nil
}

func (c *Consumer) getBuilder() *flatbuffers.Builder {
	return c.builderPool.Get().(*flatbuffers.Builder)
}

func (c *Consumer) putBuilder(b *flatbuffers.Builder) {
	b.Reset()
	c.builderPool.Put(b)
}

func (c *Consumer) getKvNodes() []string {
	c.kvNodesRWMutex.Lock()
	defer c.kvNodesRWMutex.Unlock()

	kvNodes := make([]string, len(c.kvNodes))
	copy(kvNodes, c.kvNodes)

	return kvNodes
}
