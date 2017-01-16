package producer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os/exec"
	"strconv"
	"time"

	cbbucket "github.com/couchbase/go-couchbase"
	"github.com/couchbase/indexing/secondary/dcp"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/logging"
)

func createConsumer(p *Producer, app *appConfig) *Consumer {
	consumer := &Consumer{
		producer: p,
		app:      app,
	}
	return consumer
}

func (c *Consumer) Serve() {
	c.stopConsumerCh = make(chan bool, 1)
	c.stopCheckpointingCh = make(chan bool)
	c.stopVbTakeoverCh = make(chan bool)

	c.dcpMessagesProcessed = make(map[mcd.CommandCode]int)
	c.v8WorkerMessagesProcessed = make(map[string]int)

	c.initCBBucketConnHandle()

	dcpConfig := map[string]interface{}{
		"genChanSize":    DcpGenChanSize,
		"dataChanSize":   DcpDataChanSize,
		"numConnections": DcpNumConnections,
	}

	var flogs couchbase.FailoverLog
	Retry(NewFixedBackoff(BucketOpRetryInterval), getFailoverLogOpCallback, c, &flogs, dcpConfig)

	logging.Infof("V8CR[%s:%s:%s:%d] vbnos len: %d vbnos dump: %#v",
		c.producer.AppName, c.workerName, c.tcpPort, c.osPid, len(c.vbnos), c.vbnos)

	logging.Infof("V8CR[%s:%s:%s:%d] Spawning worker corresponding to producer",
		c.producer.AppName, c.workerName, c.tcpPort, c.osPid)

	c.cmd = exec.Command("client", c.app.AppName, c.tcpPort,
		time.Now().UTC().Format("2006-01-02T15:04:05.000000000-0700"))

	err := c.cmd.Start()
	if err != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Failed to spawn worker, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)
	} else {
		c.osPid = c.cmd.Process.Pid
		logging.Infof("V8CR[%s:%s:%s:%d] c++ worker launched",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid)
	}

	rand.Seed(time.Now().UnixNano())
	feedName := couchbase.DcpFeedName("eventing:" + c.workerName + "_" + strconv.Itoa(rand.Int()))
	Retry(NewFixedBackoff(BucketOpRetryInterval), startDCPFeedOpCallback, c, feedName, dcpConfig)

	go c.startDcp(dcpConfig, flogs)

	go func(c *Consumer) {
		c.cmd.Wait()
	}(c)

	// Wait for net.Conn to be initialised
	<-c.signalConnectedCh

	c.sendInitV8Worker(c.app.AppName)
	res := c.readMessage()
	logging.Infof("V8CR[%s:%s:%s:%d] Response from worker for init call: %s",
		c.producer.AppName, c.workerName, c.tcpPort, c.osPid, res.response)

	c.sendLoadV8Worker(c.app.AppCode)
	res = c.readMessage()
	logging.Infof("V8CR[%s:%s:%s:%d] Response from worker for app load call: %s",
		c.producer.AppName, c.workerName, c.tcpPort, c.osPid, res.response)

	go c.doLastSeqNoCheckpoint()
	go c.doVbucketTakeover()

	c.doDCPEventProcess()
}

func (c *Consumer) doDCPEventProcess() {
	for {
		select {
		case e, ok := <-c.dcpFeed.C:
			if ok == false {
				logging.Infof("V8CR[%s:%s:%s:%d] Closing DCP feed for bucket %q",
					c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.producer.bucket)

				c.stopCheckpointingCh <- true
				c.stopVbTakeoverCh <- true
				c.producer.cleanupDeadConsumer(c)
				return
			}

			if _, ok := c.dcpMessagesProcessed[e.Opcode]; !ok {
				c.dcpMessagesProcessed[e.Opcode] = 0
			}
			c.dcpMessagesProcessed[e.Opcode]++

			switch e.Opcode {
			case mcd.DCP_MUTATION:
				metadata := fmt.Sprintf("{\"cas\": %d, \"flag\": %d,"+
					" \"partition\": %d, \"seq\": %d, \"ttl\": %d}",
					e.Cas, e.Flags, e.VBucket, e.Seqno, e.Expiry)

				dcpHeader := MakeDcpMutationHeader(metadata)

				dcpPayload := MakeDcpMutationPayload(e.Key, e.Value)
				msg := &Message{
					Header:  dcpHeader,
					Payload: dcpPayload,
				}

				c.vbProcessingStats.updateVbStat(e.VBucket, "last_processed_seq_no", e.Seqno)

				if err := c.sendMessage(msg); err != nil {
					c.stopCheckpointingCh <- true
					c.stopVbTakeoverCh <- true
					c.producer.cleanupDeadConsumer(c)
					return
				}
				if resp := c.readMessage(); resp.err != nil {
					c.stopCheckpointingCh <- true
					c.stopVbTakeoverCh <- true
					c.producer.cleanupDeadConsumer(c)
					return
				}
			case mcd.DCP_STREAMREQ:

				if e.Status == mcd.SUCCESS {

					c.vbProcessingStats.updateVbStat(e.VBucket, "current_vb_owner", c.getHostPortAddr())
					c.vbProcessingStats.updateVbStat(e.VBucket, "dcp_stream_status", DcpStreamRunning)
					c.vbProcessingStats.updateVbStat(e.VBucket, "last_processed_seq_no", uint64(0))
					c.vbProcessingStats.updateVbStat(e.VBucket, "assigned_worker", c.workerName)

					vbFlog := &vbFlogEntry{streamReqRetry: false, statusCode: e.Status}

					var vbBlob vbucketKVBlob
					var cas uint64

					vbKey := fmt.Sprintf("%s_vb_%s", c.producer.AppName, strconv.Itoa(int(e.VBucket)))

					Retry(NewFixedBackoff(BucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

					vbuuid, seqNo, err := e.FailoverLog.Latest()
					if err != nil {
						logging.Errorf("V8CR[%s:%s:%s:%d] Failure to get latest failover log vb: %d err: %v, not updating metadata",
							c.producer.AppName, c.workerName, c.tcpPort, c.osPid, e.VBucket, err)
						c.vbFlogChan <- vbFlog
						continue
					}

					// Update metadata with latest vbuuid and rolback seq no.
					vbBlob.LastSeqNoProcessed = seqNo
					vbBlob.VBuuid = vbuuid
					vbBlob.AssignedWorker = c.workerName

					Retry(NewFixedBackoff(BucketOpRetryInterval), casOpCallback, c, vbKey, &vbBlob, &cas)

					c.vbFlogChan <- vbFlog
					continue
				}

				if e.Status == mcd.KEY_EEXISTS || e.Status == mcd.NOT_MY_VBUCKET {
					vbFlog := &vbFlogEntry{streamReqRetry: false, statusCode: e.Status}
					c.vbFlogChan <- vbFlog
					continue
				}

				if e.Status == mcd.EINVAL || e.Status == mcd.ROLLBACK || e.Status == mcd.ENOMEM {
					vbFlog := &vbFlogEntry{
						seqNo:          e.Seqno,
						streamReqRetry: true,
						statusCode:     e.Status,
						vb:             e.VBucket,
						flog:           e.FailoverLog,
					}
					c.vbFlogChan <- vbFlog
				}
			case mcd.DCP_STREAMEND:
				// Cleanup entry for vb for which stream_end has been received from vbPorcessingStats
				// which will allow vbTakeOver background routine to start up new stream from
				// new KV node, where the vbucket has been migrated

				//Store the latest state of vbucket processing stats in the metadata bucket
				vbKey := fmt.Sprintf("%s_vb_%s", c.producer.AppName, strconv.Itoa(int(e.VBucket)))
				var vbBlob vbucketKVBlob
				var cas uint64

				Retry(NewFixedBackoff(BucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

				vbBlob.CurrentVBOwner = ""
				vbBlob.DCPStreamStatus = DcpStreamStopped

				vbBlob.LastSeqNoProcessed = c.vbProcessingStats.getVbStat(e.VBucket, "last_processed_seq_no").(uint64)

				c.vbProcessingStats.updateVbStat(e.VBucket, "current_vb_owner", vbBlob.CurrentVBOwner)
				c.vbProcessingStats.updateVbStat(e.VBucket, "dcp_stream_status", vbBlob.DCPStreamStatus)

				Retry(NewFixedBackoff(BucketOpRetryInterval), casOpCallback, c, vbKey, &vbBlob, &cas)
			default:
			}

		case <-c.statsTicker.C:
			c.RLock()
			logging.Infof("V8CR[%s:%s:%s:%d] DCP events processed: %s V8 events processed: %s",
				c.producer.AppName, c.workerName, c.tcpPort, c.osPid, sprintDCPCounts(c.dcpMessagesProcessed), sprintV8Counts(c.v8WorkerMessagesProcessed))
			c.RUnlock()
		case <-c.stopConsumerCh:
			logging.Errorf("V8CR[%s:%s:%s:%d] Socket belonging to V8 consumer died",
				c.producer.AppName, c.workerName, c.tcpPort, c.osPid)
			c.stopCheckpointingCh <- true
			c.stopVbTakeoverCh <- true
			c.producer.cleanupDeadConsumer(c)
			return
		case <-c.gracefulShutdownChan:
			return
		}
	}
}

func (c *Consumer) Stop() {
	logging.Infof("V8CR[%s:%s:%s:%d] Gracefully shutting down consumer routine\n",
		c.producer.AppName, c.workerName, c.tcpPort, c.osPid)

	c.producer.cleanupDeadConsumer(c)

	c.cmd.Process.Kill()

	c.statsTicker.Stop()
	c.stopCheckpointingCh <- true
	c.stopVbTakeoverCh <- true
	c.gracefulShutdownChan <- true
	c.dcpFeed.Close()
}

// Implement fmt.Stringer interface to allow better debugging
// if C++ V8 worker crashes
func (c *Consumer) String() string {
	return fmt.Sprintf("consumer => app: %s tcpPort: %s ospid: %d"+
		" dcpEventProcessed: %s v8EventProcessed: %s", c.app.AppName, c.tcpPort,
		c.osPid, sprintDCPCounts(c.dcpMessagesProcessed),
		sprintV8Counts(c.v8WorkerMessagesProcessed))
}

func (c *Consumer) startDcp(dcpConfig map[string]interface{},
	flogs couchbase.FailoverLog) {
	logging.Infof("V8CR[%s:%s:%s:%d] no. of vbs owned: %d vbnos owned: %#v",
		c.producer.AppName, c.workerName, c.tcpPort, c.osPid, len(c.vbnos), c.vbnos)

	Retry(NewFixedBackoff(ClusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

	for vbno, flog := range flogs {
		vbuuid, _, _ := flog.Latest()

		vbKey := fmt.Sprintf("%s_vb_%s", c.producer.AppName, strconv.Itoa(int(vbno)))
		var vbBlob vbucketKVBlob
		var cas, start uint64
		var isNoEnt bool

		Retry(NewFixedBackoff(BucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, true, &isNoEnt)
		if isNoEnt {

			// Storing vbuuid in metadata bucket, will be required for start
			// stream later on
			vbBlob.VBuuid = vbuuid
			vbBlob.VBId = vbno
			vbBlob.AssignedWorker = c.workerName

			c.vbProcessingStats.updateVbStat(vbno, "assigned_worker", c.workerName)

			Retry(NewFixedBackoff(BucketOpRetryInterval), setOpCallback, c, vbKey, &vbBlob)

			start = uint64(0)
			c.dcpRequestStreamHandle(vbno, &vbBlob, start)
		} else {
			// vbucket might be owned by another eventing node

			if vbBlob.CurrentVBOwner == c.getHostPortAddr() {
				start = vbBlob.LastSeqNoProcessed

				c.dcpRequestStreamHandle(vbno, &vbBlob, start)
			}
		}
	}
}

func (c *Consumer) dcpRequestStreamHandle(vbno uint16, vbBlob *vbucketKVBlob, start uint64) {

	opaque, flags := uint16(vbno), uint32(0)
	end := uint64(0xFFFFFFFFFFFFFFFF)

	snapStart, snapEnd := start, start

	logging.Infof("V8CR[%s:%s:%s:%d] vb: %d DCP stream start vbuuid: %d startSeq: %d snapshotStart: %d snapshotEnd: %d",
		c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbno, vbBlob.VBuuid, start, snapStart, snapEnd)

	c.dcpFeed.DcpRequestStream(vbno, opaque, flags, vbBlob.VBuuid, start, end, snapStart, snapEnd)

loop:
	vbFlog := <-c.vbFlogChan

	if !vbFlog.streamReqRetry && vbFlog.statusCode == mcd.SUCCESS {
		logging.Infof("V8CR[%s:%s:%s:%d] vb: %d DCP Stream created", c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbno)

		return
	}

	if vbFlog.streamReqRetry && vbFlog.vb == vbno {

		if vbFlog.statusCode == mcd.ROLLBACK {
			logging.Infof("V8CR[%s:%s:%s:%d] vb: %d vbuuid: %d Rollback requested by DCP, previous startseq: %d rollback startseq: %d",
				c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbno, vbBlob.VBuuid, start, vbFlog.seqNo)
			start, snapStart, snapEnd = vbFlog.seqNo, vbFlog.seqNo, vbFlog.seqNo
		}

		logging.Infof("V8CR[%s:%s:%s:%d] Retrying DCP stream start vb: %d vbuuid: %d startSeq: %d snapshotStart: %d snapshotEnd: %d",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbno, vbBlob.VBuuid, start, snapStart, snapEnd)
		c.dcpFeed.DcpRequestStream(vbno, opaque, flags, vbBlob.VBuuid, start, end, snapStart, snapEnd)
		goto loop
	}
}

func (c *Consumer) initCBBucketConnHandle() {
	config := c.app.Depcfg.(map[string]interface{})
	metadataBucket := config["metadata_bucket"].(string)
	connStr := fmt.Sprintf("http://127.0.0.1:" + c.producer.NsServerPort)

	var conn cbbucket.Client
	var pool cbbucket.Pool

	Retry(NewFixedBackoff(BucketOpRetryInterval), connectBucketOpCallback, c, &conn, connStr)

	Retry(NewFixedBackoff(BucketOpRetryInterval), poolGetBucketOpCallback, c, &conn, &pool, "default")

	Retry(NewFixedBackoff(BucketOpRetryInterval), cbGetBucketOpCallback, c, &pool, metadataBucket)
}

func (c *Consumer) doLastSeqNoCheckpoint() {
	c.checkpointTicker = time.NewTicker(CheckPointInterval * time.Second)

	for {
		select {
		case <-c.checkpointTicker.C:

			Retry(NewFixedBackoff(ClusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

			var vbBlob vbucketKVBlob

			for vbno, _ := range c.vbProcessingStats {

				// only checkpoint stats for vbuckets that the consumer instance owns
				if c.getHostPortAddr() == c.vbProcessingStats.getVbStat(vbno, "current_vb_owner") {
					vbKey := fmt.Sprintf("%s_vb_%s", c.producer.AppName, strconv.Itoa(int(vbno)))

					var cas uint64
					var isNoEnt bool

					// Case 1: Metadata blob doesn't exist probably the app is deployed for the first time.
					Retry(NewFixedBackoff(BucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, true, &isNoEnt)
					if isNoEnt {

						vbBlob.CurrentVBOwner = c.getHostPortAddr()
						vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
						vbBlob.VBId = vbno

						c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)
						vbBlob.LastSeqNoProcessed = c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no").(uint64)
						vbBlob.DCPStreamStatus = c.vbProcessingStats.getVbStat(vbno, "dcp_stream_status").(string)

						logging.Infof("V8CR[%s:%s:%s:%d] vb: %d Creating the initial metadata blob entry",
							c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbno)

						Retry(NewFixedBackoff(BucketOpRetryInterval), casOpCallback, c, vbKey, &vbBlob, &cas)
						continue
					}

					// Case 2: Metadata blob exists and mentioned vb stream owner is the current owner, this would
					// be the case during steady state eventing cluster
					if (c.getHostPortAddr() == vbBlob.CurrentVBOwner || vbBlob.CurrentVBOwner == "") && vbBlob.NewVBOwner == "" {

						vbBlob.CurrentVBOwner = c.getHostPortAddr()
						vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
						vbBlob.VBId = vbno

						c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)
						vbBlob.LastSeqNoProcessed = c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no").(uint64)
						vbBlob.DCPStreamStatus = c.vbProcessingStats.getVbStat(vbno, "dcp_stream_status").(string)

						Retry(NewFixedBackoff(BucketOpRetryInterval), casOpCallback, c, vbKey, &vbBlob, &cas)
						continue
					}

					// Case 3: current vb owner notices, a new node is requesting ownership of the vbucket
					// closes dcp stream for it after updating last processed seq no
					if c.getHostPortAddr() == vbBlob.CurrentVBOwner && vbBlob.NewVBOwner != "" {

						vbBlob.DCPStreamStatus = DcpStreamStopped
						vbBlob.CurrentVBOwner = ""
						vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)

						c.vbProcessingStats.updateVbStat(vbno, "current_vb_owner", vbBlob.CurrentVBOwner)

						vbBlob.LastSeqNoProcessed = c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no").(uint64)

						Retry(NewFixedBackoff(BucketOpRetryInterval), casOpCallback, c, vbKey, &vbBlob, &cas)

						logging.Infof("V8CR[%s:%s:%s:%d] vb: %d Closing dcp stream from node: %s as node: %s is requesting it's ownership. Node as per producer: %s",
							c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbno, c.getHostPortAddr(), vbBlob.NewVBOwner, c.producer.vbEventingNodeAssignMap[vbno])

						// TODO: Retry loop for dcp close stream as it could fail
						// Additional check needed to verify if vbBlob.NewOwner is the expected owner
						// as per the vbEventingNodesAssignMap
						c.dcpFeed.DcpCloseStream(vbno, vbno)
						continue
					}
				}
			}

		case <-c.stopCheckpointingCh:
			return
		}
	}
}

func (c *Consumer) sendInitV8Worker(appName string) {

	header := MakeV8InitOpcodeHeader(appName)
	var payload []byte

	msg := &Message{
		Header:  header,
		Payload: payload,
	}

	if _, ok := c.v8WorkerMessagesProcessed["V8_INIT"]; !ok {
		c.v8WorkerMessagesProcessed["V8_INIT"] = 0
	}
	c.v8WorkerMessagesProcessed["V8_INIT"]++

	c.sendMessage(msg)
}

func (c *Consumer) sendLoadV8Worker(appCode string) {

	header := MakeV8LoadOpcodeHeader(appCode)
	var payload []byte

	msg := &Message{
		Header:  header,
		Payload: payload,
	}

	if _, ok := c.v8WorkerMessagesProcessed["V8_LOAD"]; !ok {
		c.v8WorkerMessagesProcessed["V8_LOAD"] = 0
	}
	c.v8WorkerMessagesProcessed["V8_LOAD"]++

	c.sendMessage(msg)
}

func (c *Consumer) sendMessage(msg *Message) error {

	// Protocol encoding format:
	//<headerSize><payloadSize><Header><Payload>
	var buffer bytes.Buffer

	event := ReadHeader(msg.Header)
	if event == int8(DcpEvent) {
		ReadPayload(msg.Payload)
	}
	err := binary.Write(&buffer, binary.LittleEndian, uint32(len(msg.Header)))
	if err != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Failure while writing header size, err : %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)
	}

	err = binary.Write(&buffer, binary.LittleEndian, uint32(len(msg.Payload)))
	if err != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Failure while writing payload size, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)
	}

	err = binary.Write(&buffer, binary.LittleEndian, msg.Header)
	if err != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Failure while writing encoded header, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)
	}

	err = binary.Write(&buffer, binary.LittleEndian, msg.Payload)
	if err != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Failure while writing encoded payload, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)
	}

	err = binary.Write(c.conn, binary.LittleEndian, buffer.Bytes())
	if err != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Write to downstream socket failed, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)
		c.stopConsumerCh <- true
		c.conn.Close()
	}

	return err
}

func (c *Consumer) readMessage() *Response {
	var result *Response
	msg, err := bufio.NewReader(c.conn).ReadSlice('\n')
	if err != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Read from client socket failed, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)

		c.stopConsumerCh <- true
		c.conn.Close()

		result = &Response{
			response: "",
			err:      err,
		}
	} else {
		result = &Response{
			response: string(msg),
			err:      err,
		}
	}
	return result
}

func (c *Consumer) getHostPortAddr() string {
	c.RLock()
	defer c.RUnlock()
	return c.hostPortAddr
}

func (c *Consumer) getWorkerName() string {
	c.RLock()
	defer c.RUnlock()
	return c.workerName
}
