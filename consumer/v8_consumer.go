package consumer

import (
	"fmt"
	"hash/crc32"
	"net"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
	couchbase "github.com/couchbase/eventing/dcp"
	mcd "github.com/couchbase/eventing/dcp/transport"
	memcached "github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/goutils/systemeventlog"
	flatbuffers "github.com/google/flatbuffers/go"
)

// Note: Should be a multiple of number of dcpFeeds which we might not know during initialising consumer
// Hence, assuming 8 KV dcpFeeds for an average of 8 KV nodes.
const (
	AggChanSizeMultiplier = 8
)

// NewConsumer called by producer to create consumer handle
func NewConsumer(hConfig *common.HandlerConfig, pConfig *common.ProcessConfig, rConfig *common.RebalanceConfig,
	index int, uuid, nsServerPort string, eventingNodeUUIDs []string, vbnos []uint16, app *common.AppConfig,
	dcpConfig map[string]interface{}, p common.EventingProducer, s common.EventingSuperSup,
	cursorRegistry common.CursorRegistryMgr, numVbuckets int, retryCount *int64,
	vbEventingNodeAssignMap map[uint16]string, workerVbucketMap map[string][]uint16, featureMatrix uint32) *Consumer {

	var b *couchbase.Bucket
	consumer := &Consumer{
		n1qlPrepareAll:                  hConfig.N1qlPrepareAll,
		isPausing:                       false,
		languageCompatibility:           hConfig.LanguageCompatibility,
		app:                             app,
		aggDCPFeed:                      make(chan *memcached.DcpEvent, (AggChanSizeMultiplier * dcpConfig["dataChanSize"].(int))),
		aggDCPFeedMemCap:                hConfig.AggDCPFeedMemCap,
		breakpadOn:                      pConfig.BreakpadOn,
		sourceKeyspace:                  hConfig.SourceKeyspace,
		bucketCacheSize:                 hConfig.BucketCacheSize,
		bucketCacheAge:                  hConfig.BucketCacheAge,
		cbBucket:                        b,
		checkpointInterval:              time.Duration(hConfig.CheckpointInterval) * time.Millisecond,
		idleCheckpointInterval:          time.Duration(hConfig.IdleCheckpointInterval) * time.Millisecond,
		clusterStateChangeNotifCh:       make(chan struct{}, ClusterChangeNotifChBufSize),
		connMutex:                       &sync.RWMutex{},
		controlRoutineWg:                &sync.WaitGroup{},
		cppThrPartitionMap:              make(map[int][]uint16),
		cppWorkerThrCount:               hConfig.CPPWorkerThrCount,
		crcTable:                        crc32.MakeTable(crc32.Castagnoli),
		dcpConfig:                       dcpConfig,
		dcpFeedVbMap:                    make(map[*couchbase.DcpFeed][]uint16),
		dcpStreamBoundary:               hConfig.StreamBoundary,
		diagDir:                         pConfig.DiagDir,
		debuggerPort:                    pConfig.DebuggerPort,
		eventingAdminPort:               pConfig.EventingPort,
		eventingSSLPort:                 pConfig.EventingSSLPort,
		eventingDir:                     pConfig.EventingDir,
		eventingNodeUUIDs:               eventingNodeUUIDs,
		executeTimerRoutineCount:        hConfig.ExecuteTimerRoutineCount,
		executionTimeout:                hConfig.ExecutionTimeout,
		cursorCheckpointTimeout:         hConfig.CursorCheckpointTimeout,
		lcbRetryCount:                   hConfig.LcbRetryCount,
		lcbTimeout:                      hConfig.LcbTimeout,
		feedbackQueueCap:                hConfig.FeedbackQueueCap,
		feedbackReadBufferSize:          hConfig.FeedbackReadBufferSize,
		feedbackTCPPort:                 pConfig.FeedbackSockIdentifier,
		feedbackWriteBatchSize:          hConfig.FeedbackBatchSize,
		filterVbEvents:                  make(map[uint16]struct{}),
		filterVbEventsRWMutex:           &sync.RWMutex{},
		filterDataCh:                    make(chan *vbSeqNo, numVbuckets),
		initCPPWorkerCh:                 make(chan struct{}),
		gracefulShutdownChan:            make(chan struct{}, 1),
		gocbMetaHandleMutex:             &sync.RWMutex{},
		handlerFooters:                  hConfig.HandlerFooters,
		handlerHeaders:                  hConfig.HandlerHeaders,
		index:                           index,
		ipcType:                         pConfig.IPCType,
		inflightDcpStreams:              make(map[uint16]struct{}),
		inflightDcpStreamsRWMutex:       &sync.RWMutex{},
		hostDcpFeedRWMutex:              &sync.RWMutex{},
		insight:                         make(chan *common.Insight),
		kvHostDcpFeedMap:                make(map[string]*couchbase.DcpFeed),
		lcbInstCapacity:                 hConfig.LcbInstCapacity,
		n1qlConsistency:                 hConfig.N1qlConsistency,
		logLevel:                        hConfig.LogLevel,
		msgProcessedRWMutex:             &sync.RWMutex{},
		nsServerPort:                    nsServerPort,
		numVbuckets:                     numVbuckets,
		numTimerPartitions:              hConfig.NumTimerPartitions,
		curlMaxAllowedRespSize:          hConfig.CurlMaxAllowedRespSize,
		opsTimestamp:                    time.Now(),
		producer:                        p,
		reqStreamCh:                     make(chan *streamRequestInfo, numVbuckets*10),
		restartVbDcpStreamTicker:        time.NewTicker(restartVbDcpStreamTickInterval),
		retryCount:                      retryCount,
		sendMsgBufferRWMutex:            &sync.RWMutex{},
		sendMsgCounter:                  0,
		signalBootstrapFinishCh:         make(chan struct{}, 1),
		signalConnectedCh:               make(chan struct{}, 1),
		signalFeedbackConnectedCh:       make(chan struct{}, 1),
		signalSettingsChangeCh:          make(chan struct{}, 1),
		socketWriteBatchSize:            hConfig.SocketWriteBatchSize,
		socketWriteLoopStopAckCh:        make(chan struct{}, 1),
		socketWriteLoopStopCh:           make(chan struct{}, 1),
		socketWriteTicker:               time.NewTicker(socketWriteTimerInterval),
		statsRWMutex:                    &sync.RWMutex{},
		baseexecutionStats:              make(map[string]float64),
		basefailureStats:                make(map[string]float64),
		baselcbExceptionStats:           make(map[string]uint64),
		statsTickDuration:               time.Duration(hConfig.StatsLogInterval) * time.Millisecond,
		streamReqRWMutex:                &sync.RWMutex{},
		stopVbOwnerTakeoverCh:           make(chan struct{}),
		stopConsumerCh:                  make(chan struct{}),
		superSup:                        s,
		cursorRegistry:                  cursorRegistry,
		tcpPort:                         pConfig.SockIdentifier,
		allowTransactionMutations:       hConfig.AllowTransactionMutations,
		allowSyncDocuments:              hConfig.AllowSyncDocuments,
		cursorAware:                     hConfig.CursorAware,
		timerContextSize:                hConfig.TimerContextSize,
		updateStatsTicker:               time.NewTicker(updateCPPStatsTickInterval),
		loadStatsTicker:                 time.NewTicker(updateCPPStatsTickInterval),
		uuid:                            uuid,
		vbDcpFeedMap:                    make(map[uint16]*couchbase.DcpFeed),
		vbEnqueuedForStreamReq:          make(map[uint16]struct{}),
		vbEnqueuedForStreamReqRWMutex:   &sync.RWMutex{},
		vbFlogChan:                      make(chan *vbFlogEntry, 1024),
		vbnos:                           vbnos,
		vbDcpEventsRemaining:            make(map[int]int64),
		vbEventingNodeAssignMap:         vbEventingNodeAssignMap,
		vbEventingNodeAssignRWMutex:     &sync.RWMutex{},
		vbOwnershipGiveUpRoutineCount:   rConfig.VBOwnershipGiveUpRoutineCount,
		vbOwnershipTakeoverRoutineCount: rConfig.VBOwnershipTakeoverRoutineCount,
		vbsRemainingToCleanup:           make([]uint16, 0),
		vbsRemainingToClose:             make([]uint16, 0),
		vbsRemainingToGiveUp:            make([]uint16, 0),
		vbsRemainingToOwn:               make([]uint16, 0),
		vbsRemainingToRestream:          make([]uint16, 0),
		vbsStreamClosed:                 make(map[uint16]bool),
		vbsStreamClosedRWMutex:          &sync.RWMutex{},
		vbStreamRequested:               make(map[uint16]uint64),
		vbsStreamRRWMutex:               &sync.RWMutex{},
		workerName:                      fmt.Sprintf("worker_%s_%d", app.AppLocation, index),
		vbProcessingStats:               newVbProcessingStats(app.AppLocation, uint16(numVbuckets), uuid, fmt.Sprintf("worker_%s_%d", app.AppLocation, index)),
		workerCount:                     len(workerVbucketMap),
		workerQueueCap:                  hConfig.WorkerQueueCap,
		workerQueueMemCap:               hConfig.WorkerQueueMemCap,
		workerRespMainLoopThreshold:     hConfig.WorkerResponseTimeout,
		workerVbucketMap:                workerVbucketMap,
		workerVbucketMapRWMutex:         &sync.RWMutex{},
		respawnInvoked:                  0,
		featureMatrix:                   featureMatrix,
	}

	consumer.functionInstanceId = util.GetFunctionInstanceId(app.FunctionID, app.FunctionInstanceID)
	consumer.dcpStatsLogger = NewDcpStatsLog(5*time.Minute, consumer.workerName, consumer.stopConsumerCh)
	consumer.srcKeyspaceID, _ = p.GetSourceKeyspaceID()
	consumer.cidToKeyspaceCache = initCidToCol(hConfig.SourceKeyspace.BucketName, nsServerPort)
	consumer.binaryDocAllowed = consumer.checkBinaryDocAllowed()
	consumer.builderPool = &sync.Pool{
		New: func() interface{} {
			return flatbuffers.NewBuilder(0)
		},
	}
	for _, stat := range executionStatstoReset {
		consumer.baseexecutionStats[stat] = float64(0)
	}
	for _, stat := range failureStatstoReset {
		consumer.basefailureStats[stat] = float64(0)
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
	logging.Infof("%s [%s:%s:%d] Bootstrapping status: %t", logPrefix, c.workerName, c.tcpPort, c.Pid(), c.isBootstrapping)

	c.statsTicker = time.NewTicker(c.statsTickDuration)
	c.backupVbStats = newVbBackupStats(uint16(c.numVbuckets))

	// Insert an entry to sendMessage loop control channel to signify a normal bootstrap
	c.socketWriteLoopStopAckCh <- struct{}{}

	c.dcpMessagesProcessed = make(map[mcd.CommandCode]uint64)
	c.v8WorkerMessagesProcessed = make(map[string]uint64)

	c.consumerSup = suptree.NewSimple(c.workerName)
	c.consumerSup.ServeBackground(c.workerName)

	c.cppWorkerThrPartitionMap()

	var err error
	c.cbBucket, err = c.superSup.GetBucket(c.sourceKeyspace.BucketName, c.app.AppLocation)
	if err != nil {
		return
	}

	err = c.updategocbMetaHandle()
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
		return
	}

	go c.handleFailoverLog()
	go c.processReqStreamMessages()

	sort.Sort(util.Uint16Slice(c.vbnos))
	logging.Infof("%s [%s:%s:%d] using timer: %t vbnos len: %d dump: %s memory quota for worker and dcp queues each: %d MB",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.producer.UsingTimer(), len(c.vbnos), util.Condense(c.vbnos),
		c.workerQueueMemCap/(1024*1024))

	err = util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getEventingNodeAddrOpCallback, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	logging.Infof("%s [%s:%s:%d] Spawning worker corresponding to producer, node addr: %rs",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.HostPortAddr())

	if atomic.LoadUint32(&c.isTerminateRunning) == 0 {
		c.client = newClient(c, c.app.AppLocation, c.tcpPort, c.feedbackTCPPort, c.workerName, c.eventingAdminPort)
		c.clientSupToken = c.consumerSup.Add(c.client)
	}

checkIfPlannerRunning:
	if c.producer.IsPlannerRunning() {
		time.Sleep(time.Second)
		goto checkIfPlannerRunning
	}

	err = c.doCleanupForPreviouslyOwnedVbs()
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	// Ensure cpp worker is running and initialised before starting with any stream processing
	<-c.initCPPWorkerCh

	c.controlRoutineWg.Add(1)
	go c.controlRoutine()

	go c.updateWorkerStats()

	err = c.startDcp()
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}
	c.isBootstrapping = false
	logging.Infof("%s [%s:%s:%d] Bootstrapping status: %t", logPrefix, c.workerName, c.tcpPort, c.Pid(), c.isBootstrapping)

	c.signalBootstrapFinishCh <- struct{}{}

	logging.Infof("%s [%s:%s:%d] vbsStateUpdateRunning: %t",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.vbsStateUpdateRunning)

	if !c.vbsStateUpdateRunning && atomic.LoadUint32(&c.isTerminateRunning) == 0 {
		logging.Infof("%s [%s:%s:%d] Kicking off vbsStateUpdate routine",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		go c.vbsStateUpdate()
	}

	go c.doLastSeqNoCheckpoint()

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
	c.sendWorkerMemQuota(c.aggDCPFeedMemCap * int64(2))
	err := util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getEventingNodeAddrOpCallback, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
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

	payload, pBuilder := c.makeV8InitPayload(c.app.AppName, c.app.FunctionScope, c.debuggerPort, currHost,
		c.eventingDir, c.eventingAdminPort, c.eventingSSLPort,
		c.producer.CfgData(), c.lcbInstCapacity, c.executionTimeout, c.cursorCheckpointTimeout,
		int(c.checkpointInterval.Nanoseconds()/(1000*1000)), false, c.timerContextSize,
		c.producer.UsingTimer(), c.producer.SrcMutation())

	c.sendInitV8Worker(payload, false, pBuilder)
	if c.cursorAware {
		c.sendTrackerV8Worker(true)
	}

	c.sendLoadV8Worker(c.app.ParsedAppCode, false)

	c.sendFeatureMatrix(atomic.LoadUint32(&c.featureMatrix), false)

	c.workerExited = false

	c.SendAssignedVbs()
	c.initCPPWorkerCh <- struct{}{}
	go c.processDCPEvents()
	go c.processFilterEvents()
	go c.processStatsEvents()
	go c.loadStatsFromConsumer()
	return nil
}

// Stop acts terminate routine for consumer handle
func (c *Consumer) Stop(context string) {
	logPrefix := "Consumer::Stop"

	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("%s [%s:%s:%d] Consumer stop routine, recover %rm stack trace: %rm",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
		}
	}()

	atomic.StoreUint32(&c.isTerminateRunning, 1)

	logging.Infof("%s [%s:%s:%d] Gracefully shutting down consumer routine",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	err := c.RemoveSupervisorToken()
	if err != nil {
		logging.Errorf("%v", err)
	} else {
		logging.Infof("%s [%s:%s:%d] Requested to remove supervision of eventing-consumer",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
	}

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

	if c.socketWriteLoopStopCh != nil {
		c.socketWriteLoopStopCh <- struct{}{}
	}

	if c.socketWriteLoopStopAckCh != nil {
		<-c.socketWriteLoopStopAckCh
	}

	if c.socketWriteTicker != nil {
		c.socketWriteTicker.Stop()
	}

	logging.Infof("%s [%s:%s:%d] Sent signal over channel to stop timer routines",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	if c.updateStatsTicker != nil {
		c.updateStatsTicker.Stop()
	}

	if c.loadStatsTicker != nil {
		c.loadStatsTicker.Stop()
	}

	logging.Infof("%s [%s:%s:%d] Sent signal to stop cpp worker stat collection routine",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	logging.Infof("%s [%s:%s:%d] Sent signal over channel to stop checkpointing routine",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.dcpFeedsClosed = true

	func() {
		c.hostDcpFeedRWMutex.RLock()
		defer c.hostDcpFeedRWMutex.RUnlock()

		if c.kvHostDcpFeedMap != nil {
			for _, dcpFeed := range c.kvHostDcpFeedMap {
				if dcpFeed != nil {
					dcpFeed.Close()
				}
			}
		}
	}()

	logging.Infof("%s [%s:%s:%d] Closed all dcpfeed handles", logPrefix, c.workerName, c.tcpPort, c.Pid())

	close(c.stopConsumerCh)

	if c.conn != nil {
		c.conn.Close()
	}

	if c.debugConn != nil {
		c.debugConn.Close()
	}

	if c.debugListener != nil {
		c.debugListener.Close()
	}

	if c.consumerSup != nil {
		c.consumerSup.Stop(c.workerName)
	}

	logging.Infof("%s [%s:%s:%d] Requested to stop supervisor for Eventing.Consumer. Exiting Consumer::Stop",
		logPrefix, c.workerName, c.tcpPort, c.Pid())
}

func (c *Consumer) String() string {
	c.msgProcessedRWMutex.RLock()
	defer c.msgProcessedRWMutex.RUnlock()

	countMsg, _, _ := util.SprintDCPCounts(c.dcpMessagesProcessed)
	return fmt.Sprintf("consumer => app: %s name: %v tcpPort: %s ospid: %d"+
		" dcpEventProcessed: %s v8EventProcessed: %s", c.app.AppLocation, c.ConsumerName(),
		c.tcpPort, c.Pid(), countMsg, util.SprintV8Counts(c.v8WorkerMessagesProcessed))
}

// NotifyClusterChange is called by producer handle to signify each
// consumer instance about StartTopologyChange rpc call from cbauth service.Manager
func (c *Consumer) NotifyClusterChange() {
	logPrefix := "Consumer::NotifyClusterChange"

	vbsRemainingToCloseStream := c.getVbRemainingToCloseStream()
	vbsRemainingToStreamReq := c.getVbRemainingToStreamReq()

	if len(vbsRemainingToCloseStream) == 0 && len(vbsRemainingToStreamReq) == 0 {
		logging.Infof("%s [%s:%s:%d] Got notification about cluster state change, nothing to be done",
			logPrefix, c.ConsumerName(), c.tcpPort, c.Pid())
		return
	}

	c.isRebalanceOngoing = true
	logging.Infof("%s [%s:%s:%d] Got notification about cluster state change, updated isRebalanceOngoing to %v",
		logPrefix, c.ConsumerName(), c.tcpPort, c.Pid(), c.isRebalanceOngoing)

	c.clusterStateChangeNotifCh <- struct{}{}
}

// NotifyRebalanceStop is called by producer to signal stopping of
// rebalance operation
func (c *Consumer) NotifyRebalanceStop() {
	logPrefix := "Consumer::NotifyRebalanceStop"

	logging.Infof("%s [%s:%s:%d] Got notification about rebalance stop",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.isRebalanceOngoing = false
	logging.Infof("%s [%s:%s:%d] Updated isRebalanceOngoing to %t",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.isRebalanceOngoing)

	if c.vbsStateUpdateRunning {
		close(c.stopVbOwnerTakeoverCh)
	}
}

// NotifySettingsChange signals consumer instance of settings update
func (c *Consumer) NotifySettingsChange() {
	logPrefix := "Consumer::NotifySettingsChange"

	logging.Infof("%s [%s:%s:%d] Got notification about application settings update",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.signalSettingsChangeCh <- struct{}{}
}

// SignalStopDebugger signal C++ consumer to stop debugger
func (c *Consumer) SignalStopDebugger() error {
	logPrefix := "Consumer::SignalStopDebugger"

	logging.Infof("%s [%s:%s:%d] Got signal to stop debugger",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	c.stopDebugger()
	return nil
}

func (c *Consumer) getBuilder() *flatbuffers.Builder {
	return c.builderPool.Get().(*flatbuffers.Builder)
}

func (c *Consumer) putBuilder(b *flatbuffers.Builder) {
	b.Reset()
	c.builderPool.Put(b)
}

func (c *Consumer) getManifestUID(bucketName string) (string, error) {
	return c.superSup.GetCurrentManifestId(bucketName)
}

func (c *Consumer) updategocbMetaHandle() error {
	var err error
	c.gocbMetaHandleMutex.Lock()
	defer c.gocbMetaHandleMutex.Unlock()
	c.gocbMetaHandle, err = c.superSup.GetMetadataHandle(c.producer.MetadataBucket(), c.producer.MetadataScope(), c.producer.MetadataCollection(), c.app.AppLocation)
	return err
}

func (c *Consumer) encryptionChangedDuringLifecycle() bool {
	if (c.isBootstrapping || c.isPausing) && c.superSup.EncryptionChangedDuringLifecycle() {
		return true
	}
	return false
}

func (c *Consumer) resetExecutionStats() {
	c.statsRWMutex.Lock()
	defer c.statsRWMutex.Unlock()

	if c.executionStats != nil {
		for k, _ := range c.baseexecutionStats {
			if _, found := c.executionStats[k]; found {
				val, ok := c.executionStats[k].(float64)
				if !ok {
					continue
				}
				c.baseexecutionStats[k] = val
			}
		}
	}
}

func (c *Consumer) resetFailureStats() {
	c.statsRWMutex.Lock()
	defer c.statsRWMutex.Unlock()

	if c.failureStats != nil {
		for k, _ := range c.basefailureStats {
			if _, found := c.failureStats[k]; found {
				val, ok := c.failureStats[k].(float64)
				if !ok {
					continue
				}
				c.basefailureStats[k] = val
			}
		}
	}
}

func (c *Consumer) resetlcbExceptionStats() {
	c.statsRWMutex.Lock()
	defer c.statsRWMutex.Unlock()

	if c.lcbExceptionStats != nil {
		for k, baseval := range c.lcbExceptionStats {
			c.baselcbExceptionStats[k] = baseval
		}
	}
}

func (c *Consumer) killAndRespawn() {
	if atomic.LoadUint32(&c.isTerminateRunning) == 1 || !atomic.CompareAndSwapUint32(&c.respawnInvoked, 0, 1) {
		return
	}

	extraAttributes := map[string]interface{}{"workerName": c.workerName, "pid": c.Pid()}
	util.LogSystemEvent(util.EVENTID_CONSUMER_CRASH, systemeventlog.SEError, extraAttributes)
	c.producer.KillAndRespawnEventingConsumer(c)
}

func (c *Consumer) updateDcpProcessedMsgs(code mcd.CommandCode) {
	c.msgProcessedRWMutex.Lock()
	if _, ok := c.dcpMessagesProcessed[code]; !ok {
		c.dcpMessagesProcessed[code] = 0
	}
	c.dcpMessagesProcessed[code]++
	c.msgProcessedRWMutex.Unlock()
}
