package consumer

import (
	"fmt"
	"net"
	"runtime/debug"
	"sort"
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
	"github.com/couchbase/nitro/plasma"
)

// NewConsumer called by producer to create consumer handle
func NewConsumer(streamBoundary common.DcpStreamBoundary, cleanupTimers bool, skipTimerThreshold int,
	eventingAdminPort, eventingDir string, p common.EventingProducer, app *common.AppConfig, vbnos []uint16,
	bucket, logLevel, tcpPort, uuid string, eventingNodeUUIDs []string,
	sockWriteBatchSize, timerProcessingPoolSize, workerID int) *Consumer {

	var b *couchbase.Bucket
	consumer := &Consumer{
		app:                                app,
		aggDCPFeed:                         make(chan *memcached.DcpEvent, dcpGenChanSize),
		bucket:                             bucket,
		cbBucket:                           b,
		checkpointInterval:                 checkpointInterval,
		cleanupTimers:                      cleanupTimers,
		clusterStateChangeNotifCh:          make(chan bool, ClusterChangeNotifChBufSize),
		dcpBootstrapCh:                     make(chan bool, 10),
		dcpFeedCancelChs:                   make([]chan bool, 0),
		dcpFeedVbMap:                       make(map[*couchbase.DcpFeed][]uint16),
		dcpStreamBoundary:                  streamBoundary,
		docTimerEntryCh:                    make(chan *byTimerEntry, timerChanSize),
		eventingAdminPort:                  eventingAdminPort,
		eventingDir:                        eventingDir,
		eventingNodeUUIDs:                  eventingNodeUUIDs,
		gracefulShutdownChan:               make(chan bool, 1),
		kvHostDcpFeedMap:                   make(map[string]*couchbase.DcpFeed),
		logLevel:                           logLevel,
		nonDocTimerEntryCh:                 make(chan string, timerChanSize),
		nonDocTimerStopCh:                  make(chan bool, 1),
		opsTimestamp:                       time.Now(),
		persistAllTicker:                   time.NewTicker(persistAllTickInterval),
		plasmaStoreRWMutex:                 &sync.RWMutex{},
		producer:                           p,
		restartVbDcpStreamTicker:           time.NewTicker(restartVbDcpStreamTickInterval),
		sendMsgCounter:                     0,
		signalConnectedCh:                  make(chan bool, 1),
		signalSettingsChangeCh:             make(chan bool, 1),
		signalProcessTimerPlasmaCloseAckCh: make(chan uint16),
		signalStoreTimerPlasmaCloseAckCh:   make(chan uint16),
		signalStoreTimerPlasmaCloseCh:      make(chan uint16),
		skipTimerThreshold:                 skipTimerThreshold,
		socketWriteBatchSize:               sockWriteBatchSize,
		statsTicker:                        time.NewTicker(statsTickInterval),
		stopControlRoutineCh:               make(chan bool),
		stopPlasmaPersistCh:                make(chan bool, 1),
		stopVbOwnerGiveupCh:                make(chan bool, 1),
		stopVbOwnerTakeoverCh:              make(chan bool, 1),
		tcpPort:                            tcpPort,
		timerRWMutex:                       &sync.RWMutex{},
		timerProcessingTickInterval:        timerProcessingTickInterval,
		timerProcessingWorkerCount:         timerProcessingPoolSize,
		timerProcessingVbsWorkerMap:        make(map[uint16]*timerProcessingWorker),
		timerProcessingRunningWorkers:      make([]*timerProcessingWorker, 0),
		timerProcessingWorkerSignalCh:      make(map[*timerProcessingWorker]chan bool),
		uuid:                   uuid,
		vbDcpFeedMap:           make(map[uint16]*couchbase.DcpFeed),
		vbFlogChan:             make(chan *vbFlogEntry),
		vbnos:                  vbnos,
		vbPlasmaReader:         make(map[uint16]*plasma.Writer),
		vbPlasmaWriter:         make(map[uint16]*plasma.Writer),
		vbPlasmaStoreMap:       make(map[uint16]*plasma.Plasma),
		vbProcessingStats:      newVbProcessingStats(),
		vbsRemainingToGiveUp:   make([]uint16, 0),
		vbsRemainingToOwn:      make([]uint16, 0),
		vbsRemainingToRestream: make([]uint16, 0),
		workerName:             fmt.Sprintf("worker_%s_%d", app.AppName, workerID),
		writeBatchSeqnoMap:     make(map[uint16]uint64),
	}
	return consumer
}

// Serve acts as init routine for consumer handle
func (c *Consumer) Serve() {
	c.stopConsumerCh = make(chan bool, 1)
	c.stopCheckpointingCh = make(chan bool, 1)

	c.dcpMessagesProcessed = make(map[mcd.CommandCode]uint64)
	c.v8WorkerMessagesProcessed = make(map[string]uint64)

	c.consumerSup = suptree.NewSimple(c.workerName)
	go c.consumerSup.ServeBackground()

	c.initCBBucketConnHandle()

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), commonConnectBucketOpCallback, c, &c.cbBucket)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), gocbConnectBucketCallback, c)

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
		util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), startDCPFeedOpCallback, c, feedName, dcpConfig, kvHostPort)

		cancelCh := make(chan bool, 1)
		c.dcpFeedCancelChs = append(c.dcpFeedCancelChs, cancelCh)
		c.addToAggChan(c.kvHostDcpFeedMap[kvHostPort], cancelCh)
	}

	c.client = newClient(c, c.app.AppName, c.tcpPort, c.workerName)
	c.clientSupToken = c.consumerSup.Add(c.client)

	c.timerTransferHandle = timer.NewTimerTransfer(c, c.app.AppName, fmt.Sprintf("%s/%s/", c.eventingDir, c.app.AppName),
		c.HostPortAddr(), c.workerName)
	c.timerTransferSupToken = c.consumerSup.Add(c.timerTransferHandle)

	go c.startDcp(dcpConfig, flogs)

	for i := 0; i < len(c.vbnos); i++ {
		<-c.dcpBootstrapCh
	}

	// Initialises timer processing worker instances
	c.vbTimerProcessingWorkerAssign(true)

	go c.plasmaPersistAll()

	// doc_id timer events
	for _, r := range c.timerProcessingRunningWorkers {
		go r.processTimerEvents()
	}

	// non doc_id timer events
	go c.processNonDocTimerEvents()

	c.controlRoutine()

	logging.Debugf("V8CR[%s:%s:%s:%d] Exiting consumer init routine",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())
}

// HandleV8Worker sets up CPP V8 worker post it's bootstrap
func (c *Consumer) HandleV8Worker() {
	<-c.signalConnectedCh

	logging.SetLogLevel(util.GetLogLevel(c.logLevel))
	c.sendLogLevel(c.logLevel)

	payload := makeV8InitPayload(c.app.AppName, c.producer.KvHostPorts()[0], c.producer.CfgData(),
		c.producer.RbacUser(), c.producer.RbacPass())
	c.sendInitV8Worker(payload)

	c.sendLoadV8Worker(c.app.AppCode)

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

	c.plasmaStoreRWMutex.RLock()
	for _, store := range c.vbPlasmaStoreMap {
		store.Close()
	}
	c.plasmaStoreRWMutex.RUnlock()

	close(c.dcpBootstrapCh)

	c.cbBucket.Close()
	c.gocbBucket.Close()

	c.producer.CleanupDeadConsumer(c)

	c.consumerSup.Remove(c.timerTransferSupToken)
	c.consumerSup.Remove(c.clientSupToken)
	c.consumerSup.Stop()

	c.checkpointTicker.Stop()
	c.restartVbDcpStreamTicker.Stop()
	c.statsTicker.Stop()
	c.persistAllTicker.Stop()

	for k := range c.timerProcessingWorkerSignalCh {
		k.stopCh <- true
	}

	c.nonDocTimerStopCh <- true
	c.stopControlRoutineCh <- true
	c.stopPlasmaPersistCh <- true

	for _, dcpFeed := range c.kvHostDcpFeedMap {
		dcpFeed.Close()
	}

	for _, cancelCh := range c.dcpFeedCancelChs {
		cancelCh <- true
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
	c.signalConnectedCh <- true
}

// SetConnHandle sets the tcp connection handle for CPP V8 worker
func (c *Consumer) SetConnHandle(conn net.Conn) {
	c.Lock()
	defer c.Unlock()
	c.conn = conn
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
	return c.timerTransferHandle.Addr
}

// NotifyClusterChange is called by producer handle to signify each
// consumer instance about StartTopologyChange rpc call from cbauth service.Manager
func (c *Consumer) NotifyClusterChange() {
	logging.Infof("V8CR[%s:%s:%s:%d] Got notification about cluster state change",
		c.app.AppName, c.ConsumerName(), c.tcpPort, c.Pid())
	c.clusterStateChangeNotifCh <- true
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

	c.stopVbOwnerGiveupCh <- true
	c.stopVbOwnerTakeoverCh <- true
}

// NotifySettingsChange signals consumer instance of settings update
func (c *Consumer) NotifySettingsChange() {
	logging.Infof("V8CR[%s:%s:%s:%d] Got notification about application settings update",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())

	c.signalSettingsChangeCh <- true
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
