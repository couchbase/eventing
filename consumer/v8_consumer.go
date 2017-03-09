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
	vbnos []uint16, bucket, logLevel, tcpPort, uuid string, workerID int) *Consumer {
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
		signalConnectedCh:         make(chan bool),
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
	}
	return consumer
}

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
	logging.Infof("V8CR[%s:%s:%s:%d] vbnos len: %d vbnos dump: %#v",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid, len(c.vbnos), c.vbnos)

	logging.Infof("V8CR[%s:%s:%s:%d] Spawning worker corresponding to producer",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid)

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

	go c.startDcp(dcpConfig, flogs)

	c.client = newClient(c.app.AppName, c.tcpPort, c.workerName)
	c.clientSupToken = c.consumerSup.Add(c.client)

	// Wait for net.Conn to be initialised
	<-c.signalConnectedCh

	c.sendLogLevel(c.logLevel)

	initMeta := makeV8InitMetadata(c.app.AppName, c.producer.KvHostPorts()[0], c.producer.CfgData())
	c.sendInitV8Worker(string(initMeta))
	resp := c.readMessage()
	logging.Infof("V8CR[%s:%s:%s:%d] Response from worker for init call: %s",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid, resp.res)

	c.sendLoadV8Worker(c.app.AppCode)
	resp = c.readMessage()
	logging.Infof("V8CR[%s:%s:%s:%d] Response from worker for app load call: %s",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid, resp.res)

	go c.doLastSeqNoCheckpoint()
	go c.controlRoutine()

	c.doDCPEventProcess()
}

func (c *Consumer) Stop() {
	logging.Infof("V8CR[%s:%s:%s:%d] Gracefully shutting down consumer routine",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid)

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
	return fmt.Sprintf("consumer => app: %s tcpPort: %s ospid: %d"+
		" dcpEventProcessed: %s v8EventProcessed: %s", c.app.AppName, c.tcpPort,
		c.osPid, util.SprintDCPCounts(c.dcpMessagesProcessed),
		util.SprintV8Counts(c.v8WorkerMessagesProcessed))
}

func (c *Consumer) SignalConnected() {
	c.signalConnectedCh <- true
}

func (c *Consumer) SetConnHandle(conn net.Conn) {
	c.conn = conn
}

func (c *Consumer) HostPortAddr() string {
	hostPortAddr := (*string)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&c.hostPortAddr))))
	if hostPortAddr != nil {
		return *hostPortAddr
	}
	return ""
}

func (c *Consumer) ConsumerName() string {
	return c.workerName
}

func (c *Consumer) NodeUUID() string {
	return c.uuid
}

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
