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
	"github.com/couchbase/eventing/dcp"
	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/timers"
	"github.com/couchbase/eventing/util"
	"github.com/google/flatbuffers/go"
)

// NewConsumer called by producer to create consumer handle
func NewConsumer(hConfig *common.HandlerConfig, pConfig *common.ProcessConfig, rConfig *common.RebalanceConfig,
	index int, uuid, nsServerPort string, eventingNodeUUIDs []string, vbnos []uint16, app *common.AppConfig,
	dcpConfig map[string]interface{}, p common.EventingProducer, s common.EventingSuperSup,
	numVbuckets int, retryCount *int64, vbEventingNodeAssignMap map[uint16]string,
	workerVbucketMap map[string][]uint16) *Consumer {

	var b *couchbase.Bucket
	consumer := &Consumer{
		app:                             app,
		aggDCPFeed:                      make(chan *memcached.DcpEvent, dcpConfig["dataChanSize"].(int)),
		aggDCPFeedMemCap:                hConfig.AggDCPFeedMemCap,
		breakpadOn:                      pConfig.BreakpadOn,
		bucket:                          hConfig.SourceBucket,
		cbBucket:                        b,
		cbBucketRWMutex:                 &sync.RWMutex{},
		checkpointInterval:              time.Duration(hConfig.CheckpointInterval) * time.Millisecond,
		idleCheckpointInterval:          time.Duration(hConfig.IdleCheckpointInterval) * time.Millisecond,
		cleanupTimers:                   hConfig.CleanupTimers,
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
		fireTimerQueue:                  util.NewBoundedQueue(hConfig.TimerQueueSize, hConfig.TimerQueueMemCap),
		debuggerPort:                    pConfig.DebuggerPort,
		eventingAdminPort:               pConfig.EventingPort,
		eventingSSLPort:                 pConfig.EventingSSLPort,
		eventingDir:                     pConfig.EventingDir,
		eventingNodeUUIDs:               eventingNodeUUIDs,
		executeTimerRoutineCount:        hConfig.ExecuteTimerRoutineCount,
		executionTimeout:                hConfig.ExecutionTimeout,
		feedbackQueueCap:                hConfig.FeedbackQueueCap,
		feedbackReadBufferSize:          hConfig.FeedbackReadBufferSize,
		feedbackTCPPort:                 pConfig.FeedbackSockIdentifier,
		feedbackWriteBatchSize:          hConfig.FeedbackBatchSize,
		filterVbEvents:                  make(map[uint16]struct{}),
		filterVbEventsRWMutex:           &sync.RWMutex{},
		filterDataCh:                    make(chan *vbSeqNo, numVbuckets),
		gracefulShutdownChan:            make(chan struct{}, 1),
		handlerFooters:                  hConfig.HandlerFooters,
		handlerHeaders:                  hConfig.HandlerHeaders,
		index:                           index,
		ipcType:                         pConfig.IPCType,
		inflightDcpStreams:              make(map[uint16]struct{}),
		inflightDcpStreamsRWMutex:       &sync.RWMutex{},
		hostDcpFeedRWMutex:              &sync.RWMutex{},
		kvHostDcpFeedMap:                make(map[string]*couchbase.DcpFeed),
		kvNodesRWMutex:                  &sync.RWMutex{},
		lcbInstCapacity:                 hConfig.LcbInstCapacity,
		logLevel:                        hConfig.LogLevel,
		msgProcessedRWMutex:             &sync.RWMutex{},
		numVbuckets:                     numVbuckets,
		opsTimestamp:                    time.Now(),
		createTimerQueue:                util.NewBoundedQueue(hConfig.TimerQueueSize, hConfig.TimerQueueMemCap),
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
		socketTimeout:                   time.Duration(hConfig.SocketTimeout) * time.Second,
		socketWriteBatchSize:            hConfig.SocketWriteBatchSize,
		socketWriteLoopStopAckCh:        make(chan struct{}, 1),
		socketWriteLoopStopCh:           make(chan struct{}, 1),
		socketWriteTicker:               time.NewTicker(socketWriteTimerInterval),
		statsRWMutex:                    &sync.RWMutex{},
		statsTickDuration:               time.Duration(hConfig.StatsLogInterval) * time.Millisecond,
		streamReqRWMutex:                &sync.RWMutex{},
		stopVbOwnerTakeoverCh:           make(chan struct{}),
		stopConsumerCh:                  make(chan struct{}),
		superSup:                        s,
		tcpPort:                         pConfig.SockIdentifier,
		timerContextSize:                hConfig.TimerContextSize,
		timerQueueSize:                  hConfig.TimerQueueSize,
		timerQueueMemCap:                hConfig.TimerQueueMemCap,
		timerStorageChanSize:            hConfig.TimerStorageChanSize,
		timerStorageMetaChsRWMutex:      &sync.RWMutex{},
		timerStorageRoutineCount:        hConfig.TimerStorageRoutineCount,
		timerStorageQueues:              make([]*util.BoundedQueue, hConfig.TimerStorageRoutineCount),
		updateStatsTicker:               time.NewTicker(updateCPPStatsTickInterval),
		usingTimer:                      hConfig.UsingTimer,
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
		vbStreamRequested:               make(map[uint16]struct{}),
		vbsStreamRRWMutex:               &sync.RWMutex{},
		workerName:                      fmt.Sprintf("worker_%s_%d", app.AppName, index),
		vbProcessingStats:               newVbProcessingStats(app.AppName, uint16(numVbuckets), uuid, fmt.Sprintf("worker_%s_%d", app.AppName, index)),
		workerCount:                     len(workerVbucketMap),
		workerQueueCap:                  hConfig.WorkerQueueCap,
		workerQueueMemCap:               hConfig.WorkerQueueMemCap,
		workerRespMainLoopThreshold:     hConfig.WorkerResponseTimeout,
		workerVbucketMap:                workerVbucketMap,
		workerVbucketMapRWMutex:         &sync.RWMutex{},
		nsServerPort:                    nsServerPort,
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
	logging.Infof("%s [%s:%s:%d] using timer: %t vbnos len: %d dump: %s memory quota for worker and dcp queues each: %d MB",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.usingTimer, len(c.vbnos), util.Condense(c.vbnos),
		c.workerQueueMemCap/(1024*1024))

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
		if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
			continue
		}

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

	if atomic.LoadUint32(&c.isTerminateRunning) == 0 {
		c.client = newClient(c, c.app.AppName, c.tcpPort, c.feedbackTCPPort, c.workerName, c.eventingAdminPort)
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

	c.controlRoutineWg.Add(1)
	go c.controlRoutine()

	if c.usingTimer {
		go c.scanTimers()
	}

	go c.updateWorkerStats()

	err = c.startDcp(flogs)
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

	payload, pBuilder := c.makeV8InitPayload(c.app.AppName, c.debuggerPort, currHost,
		c.eventingDir, c.eventingAdminPort, c.eventingSSLPort, c.getKvNodes()[0],
		c.producer.CfgData(), c.lcbInstCapacity, c.executionTimeout,
		int(c.checkpointInterval.Nanoseconds()/(1000*1000)), false, c.timerContextSize)

	c.sendInitV8Worker(payload, false, pBuilder)

	c.sendLoadV8Worker(c.app.AppCode, false)

	c.workerExited = false

	if c.usingTimer {
		go c.routeTimers()
		go c.processTimerEvents()
	}

	go c.processEvents()
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

	if c.usingTimer {
		vbsOwned := c.getCurrentlyOwnedVbs()
		sort.Sort(util.Uint16Slice(vbsOwned))

		logging.Infof("%s [%s:%s:%d] Currently owned vbs len: %d dump: %s",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), len(vbsOwned), util.Condense(vbsOwned))

		for _, vb := range vbsOwned {
			store, found := timers.Fetch(c.producer.GetMetadataPrefix(), int(vb))

			if found {
				store.Free()
			}
		}
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

	if c.socketWriteLoopStopCh != nil {
		c.socketWriteLoopStopCh <- struct{}{}
	}

	if c.socketWriteLoopStopAckCh != nil {
		<-c.socketWriteLoopStopAckCh
	}

	if c.socketWriteTicker != nil {
		c.socketWriteTicker.Stop()
	}

	if c.createTimerQueue != nil {
		c.createTimerQueue.Close()
	}

	c.timerStorageMetaChsRWMutex.Lock()
	for _, q := range c.timerStorageQueues {
		if q != nil {
			q.Close()
		}
	}
	c.timerStorageMetaChsRWMutex.Unlock()

	if c.fireTimerQueue != nil {
		c.fireTimerQueue.Close()
	}

	logging.Infof("%s [%s:%s:%d] Sent signal over channel to stop timer routines",
		logPrefix, c.workerName, c.tcpPort, c.Pid())

	// Closing bucket feed handle after sending message on stopReqStreamProcessCh
	if c.cbBucket != nil {
		c.cbBucket.Close()
	}

	if c.updateStatsTicker != nil {
		c.updateStatsTicker.Stop()
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

func (c *Consumer) getKvNodes() []string {
	c.kvNodesRWMutex.Lock()
	defer c.kvNodesRWMutex.Unlock()

	kvNodes := make([]string, len(c.kvNodes))
	copy(kvNodes, c.kvNodes)

	return kvNodes
}
