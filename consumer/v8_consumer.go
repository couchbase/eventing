package consumer

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
	cblib "github.com/couchbase/go-couchbase"
	"github.com/couchbase/indexing/secondary/dcp"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/logging"
)

func New(p common.EventingProducer, app *common.AppConfig, vbnos []uint16, bucket, tcpPort, uuid string, workerID int) *Consumer {
	var b *couchbase.Bucket
	consumer := &Consumer{
		app:                       app,
		bucket:                    bucket,
		cbBucket:                  b,
		controlRoutineTicker:      time.NewTicker(ControlRoutineTickInterval),
		clusterStateChangeNotifCh: make(chan bool, ClusterChangeNotifChBufSize),
		gracefulShutdownChan:      make(chan bool, 1),
		producer:                  p,
		signalConnectedCh:         make(chan bool),
		statsTicker:               time.NewTicker(StatsTickInterval),
		stopControlRoutineCh:      make(chan bool),
		tcpPort:                   tcpPort,
		uuid:                      uuid,
		vbFlogChan:                make(chan *vbFlogEntry),
		vbnos:                     vbnos,
		vbProcessingStats:         newVbProcessingStats(),
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

	dcpConfig := map[string]interface{}{
		"genChanSize":    DcpGenChanSize,
		"dataChanSize":   DcpDataChanSize,
		"numConnections": DcpNumConnections,
	}

	util.Retry(util.NewFixedBackoff(time.Second), commonConnectBucketOpCallback, c, &c.cbBucket)

	var flogs couchbase.FailoverLog
	util.Retry(util.NewFixedBackoff(BucketOpRetryInterval), getFailoverLogOpCallback, c, &flogs, dcpConfig)

	logging.Infof("V8CR[%s:%s:%s:%d] vbnos len: %d vbnos dump: %#v",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid, len(c.vbnos), c.vbnos)

	logging.Infof("V8CR[%s:%s:%s:%d] Spawning worker corresponding to producer",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid)

	rand.Seed(time.Now().UnixNano())
	feedName := couchbase.DcpFeedName("eventing:" + c.workerName + "_" + strconv.Itoa(rand.Int()))
	util.Retry(util.NewFixedBackoff(BucketOpRetryInterval), startDCPFeedOpCallback, c, feedName, dcpConfig)

	go c.startDcp(dcpConfig, flogs)

	c.client = newClient(c.app.AppName, c.tcpPort, c.workerName)
	c.clientSupToken = c.consumerSup.Add(c.client)

	// Wait for net.Conn to be initialised
	<-c.signalConnectedCh

	initMeta := MakeV8InitMetadata(c.app.AppName, c.producer.KvHostPort()[0], c.producer.CfgData())
	c.sendInitV8Worker(string(initMeta))
	res := c.readMessage()
	logging.Infof("V8CR[%s:%s:%s:%d] Response from worker for init call: %s",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid, res.response)

	c.sendLoadV8Worker(c.app.AppCode)
	res = c.readMessage()
	logging.Infof("V8CR[%s:%s:%s:%d] Response from worker for app load call: %s",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid, res.response)

	go c.doLastSeqNoCheckpoint()
	go c.controlRoutine()
	go c.vbsStateUpdate()

	c.doDCPEventProcess()
}

func (c *Consumer) Stop() {
	logging.Infof("V8CR[%s:%s:%s:%d] Gracefully shutting down consumer routine",
		c.app.AppName, c.workerName, c.tcpPort, c.osPid)

	c.producer.CleanupDeadConsumer(c)

	c.consumerSup.Remove(c.clientSupToken)
	c.consumerSup.Stop()

	c.statsTicker.Stop()
	c.stopControlRoutineCh <- true
	c.stopCheckpointingCh <- true
	c.gracefulShutdownChan <- true
	c.dcpFeed.Close()
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

	util.Retry(util.NewFixedBackoff(BucketOpRetryInterval), connectBucketOpCallback, c, &conn, connStr)

	util.Retry(util.NewFixedBackoff(BucketOpRetryInterval), poolGetBucketOpCallback, c, &conn, &pool, "default")

	util.Retry(util.NewFixedBackoff(BucketOpRetryInterval), cbGetBucketOpCallback, c, &pool, metadataBucket)
}

func (c *Consumer) VbProcessingStats() map[uint16]map[string]interface{} {
	vbstats := make(map[uint16]map[string]interface{})
	for vbno := range c.vbProcessingStats {
		if _, ok := vbstats[vbno]; !ok {
			vbstats[vbno] = make(map[string]interface{})
		}
		assignedWorker := c.vbProcessingStats.getVbStat(vbno, "assigned_worker")
		owner := c.vbProcessingStats.getVbStat(vbno, "current_vb_owner")
		streamStatus := c.vbProcessingStats.getVbStat(vbno, "dcp_stream_status")
		seqNo := c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no")
		uuid := c.vbProcessingStats.getVbStat(vbno, "node_uuid")

		vbstats[vbno]["assigned_worker"] = assignedWorker
		vbstats[vbno]["current_vb_owner"] = owner
		vbstats[vbno]["node_uuid"] = uuid
		vbstats[vbno]["stream_status"] = streamStatus
		vbstats[vbno]["seq_no"] = seqNo
	}

	return vbstats
}
