package consumer

import (
	"fmt"
	"math/rand"
	"net"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
	cblib "github.com/couchbase/go-couchbase"
	"github.com/couchbase/indexing/secondary/dcp"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
)

// NewConsumer called by producer to create consumer handle
func NewConsumer(streamBoundary common.DcpStreamBoundary, p common.EventingProducer, app *common.AppConfig,
	vbnos []uint16, bucket, logLevel, tcpPort, uuid string, sockWriteBatchSize, workerID int) *Consumer {
	var b *couchbase.Bucket
	consumer := &Consumer{
		app:                       app,
		aggDCPFeed:                make(chan *memcached.DcpEvent, dcpGenChanSize),
		bucket:                    bucket,
		cbBucket:                  b,
		clusterStateChangeNotifCh: make(chan bool, ClusterChangeNotifChBufSize),
		dcpFeedCancelChs:          make([]chan bool, 0),
		dcpFeedVbMap:              make(map[*couchbase.DcpFeed][]uint16),
		dcpStreamBoundary:         streamBoundary,
		gracefulShutdownChan:      make(chan bool, 1),
		kvHostDcpFeedMap:          make(map[string]*couchbase.DcpFeed),
		logLevel:                  logLevel,
		producer:                  p,
		restartVbDcpStreamTicker:  time.NewTicker(restartVbDcpStreamTickInterval),
		sendMsgCounter:            0,
		signalConnectedCh:         make(chan bool, 1),
		socketWriteBatchSize:      sockWriteBatchSize,
		statsTicker:               time.NewTicker(statsTickInterval),
		stopControlRoutineCh:      make(chan bool),
		tcpPort:                   tcpPort,
		uuid:                      uuid,
		vbDcpFeedMap:              make(map[uint16]*couchbase.DcpFeed),
		vbFlogChan:                make(chan *vbFlogEntry),
		vbnos:                     vbnos,
		vbProcessingStats:         newVbProcessingStats(),
		vbsRemainingToGiveUp:      make([]uint16, 0),
		vbsRemainingToOwn:         make([]uint16, 0),
		vbsRemainingToRestream:    make([]uint16, 0),
		workerName:                fmt.Sprintf("worker_%s_%d", app.AppName, workerID),
		writeBatchSeqnoMap:        make(map[uint16]uint64),
	}
	return consumer
}

// Serve acts as init routine for consumer handle
func (c *Consumer) Serve() {
	c.stopConsumerCh = make(chan bool, 1)
	c.stopCheckpointingCh = make(chan bool)

	c.dcpMessagesProcessed = make(map[mcd.CommandCode]int)
	c.v8WorkerMessagesProcessed = make(map[string]int)

	c.consumerSup = suptree.NewSimple(c.workerName)
	go c.consumerSup.ServeBackground()

	c.initCBBucketConnHandle()

	util.Retry(util.NewFixedBackoff(time.Second), commonConnectBucketOpCallback, c, &c.cbBucket)

	var flogs couchbase.FailoverLog
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getFailoverLogOpCallback, c, &flogs, dcpConfig)

	sort.Sort(util.Uint16Slice(c.vbnos))
	logging.Infof("V8CR[%s:%s:%s:%d] vbnos len: %d",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid(), len(c.vbnos))

	logging.Infof("V8CR[%s:%s:%s:%d] Spawning worker corresponding to producer",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())

	rand.Seed(time.Now().UnixNano())
	util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

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

	c.startDcp(dcpConfig, flogs)

	c.controlRoutine()
}

// HandleV8Worker sets up CPP V8 worker post it's bootstrap
func (c *Consumer) HandleV8Worker() {
	<-c.signalConnectedCh

	c.sendLogLevel(c.logLevel)

	payload := makeV8InitPayload(c.app.AppName, c.producer.KvHostPorts()[0], c.producer.CfgData())
	c.sendInitV8Worker(payload)

	c.sendLoadV8Worker(c.app.AppCode)

	go c.doLastSeqNoCheckpoint()

	go c.doDCPEventProcess()

}

// Stop acts terminate routine for consumer handle
func (c *Consumer) Stop() {
	logging.Infof("V8CR[%s:%s:%s:%d] Gracefully shutting down consumer routine",
		c.app.AppName, c.workerName, c.tcpPort, c.Pid())

	c.producer.CleanupDeadConsumer(c)

	c.consumerSup.Remove(c.clientSupToken)
	c.consumerSup.Stop()

	c.checkpointTicker.Stop()
	c.restartVbDcpStreamTicker.Stop()
	c.statsTicker.Stop()

	c.stopCheckpointingCh <- true
	c.stopControlRoutineCh <- true
	c.gracefulShutdownChan <- true

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
	return fmt.Sprintf("consumer => app: %s name: %v tcpPort: %s ospid: %d"+
		" dcpEventProcessed: %s v8EventProcessed: %s", c.app.AppName, c.ConsumerName(),
		c.tcpPort, c.Pid(), util.SprintDCPCounts(c.dcpMessagesProcessed),
		util.SprintV8Counts(c.v8WorkerMessagesProcessed))
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

// NotifyClusterChange is called by producer handle to signify each
// consumer instance about StartTopologyChange rpc call from cbauth service.Manager
func (c *Consumer) NotifyClusterChange() {
	logging.Infof("Consumer: %s got message that cluster state has changed", c.ConsumerName())
	c.clusterStateChangeNotifCh <- true
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
