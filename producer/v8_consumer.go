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

	c.dcpMessagesProcessed = make(map[mcd.CommandCode]int)
	c.v8WorkerMessagesProcessed = make(map[string]int)

	c.initCBBucketConnHandle()

	dcpConfig := map[string]interface{}{
		"genChanSize":    DCP_GEN_CHAN_SIZE,
		"dataChanSize":   DCP_DATA_CHAN_SIZE,
		"numConnections": DCP_NUM_CONNECTIONS,
	}

	flogs, err := c.cbBucket.GetFailoverLogs(0xABCD, c.vbnos, dcpConfig)
	sleepDuration := time.Duration(1)
	for err != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Failed to get failover logs, retrying after %d secs, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, int(sleepDuration), err)

		time.Sleep(sleepDuration * time.Second)
		c.cbBucket.Refresh()
		flogs, err = c.cbBucket.GetFailoverLogs(0xABCD, c.vbnos, dcpConfig)

		if sleepDuration < BACKOFF_THRESHOLD {
			sleepDuration = sleepDuration * 2
		}
	}

	c.cbBucket.Refresh()
	rand.Seed(time.Now().UnixNano())
	c.dcpFeed, err = c.cbBucket.StartDcpFeedOver(
		couchbase.NewDcpFeedName("eventing_"+c.workerName+"_"+strconv.Itoa(rand.Int())),
		uint32(0), c.producer.kvHostPort, 0xABCD, dcpConfig)
	sleepDuration = time.Duration(1)
	for err != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Failed to start dcp feed, retrying after %d secs, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, int(sleepDuration), err)

		time.Sleep(sleepDuration * time.Second)

		c.dcpFeed, err = c.cbBucket.StartDcpFeedOver(
			couchbase.NewDcpFeedName("eventing_"+c.workerName+"_"+strconv.Itoa(rand.Int())),
			uint32(0), c.producer.kvHostPort, 0xABCD, dcpConfig)

		if sleepDuration < BACKOFF_THRESHOLD {
			sleepDuration = sleepDuration * 2
		}
	}

	go c.startDcp(dcpConfig, flogs)

	logging.Infof("V8CR[%s:%s:%s:%d] Spawning worker corresponding to producer",
		c.producer.AppName, c.workerName, c.tcpPort, c.osPid)

	cmd := exec.Command("client", c.app.AppName, c.tcpPort,
		time.Now().UTC().Format("2006-01-02T15:04:05-0700"))

	err = cmd.Start()
	if err != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Failed to spawn worker, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, err)
	} else {
		c.osPid = cmd.Process.Pid
		logging.Infof("V8CR[%s:%s:%s:%d] c++ worker launched",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid)
	}

	go func(c *Consumer) {
		cmd.Wait()
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

	for {
		select {
		case e, ok := <-c.dcpFeed.C:
			if ok == false {
				logging.Infof("V8CR[%s:%s:%s:%d] Closing DCP feed for bucket %q",
					c.producer.AppName, c.workerName, c.tcpPort, c.osPid, c.producer.bucket)
				c.stopCheckpointingCh <- true
				c.producer.cleanupDeadConsumer(c)
				return
			}
			if e.Opcode == mcd.DCP_MUTATION {
				metadata := fmt.Sprintf("{\"cas\": %d, \"flag\": %d,"+
					" \"partition\": %d, \"seq\": %d, \"ttl\": %d}",
					e.Cas, e.Flags, e.VBucket, e.Seqno, e.Expiry)

				dcpHeader := MakeDcpMutationHeader(metadata)

				dcpPayload := MakeDcpMutationPayload(e.Key, e.Value)
				msg := &Message{
					Header:  dcpHeader,
					Payload: dcpPayload,
				}

				if _, ok := c.dcpMessagesProcessed[e.Opcode]; !ok {
					c.dcpMessagesProcessed[e.Opcode] = 0
				}

				if _, ok := c.vbProcessingStats[e.VBucket]; !ok {
					c.vbProcessingStats[e.VBucket] = make(map[string]interface{})
					c.vbProcessingStats[e.VBucket]["last_processed_seq_no"] = 0
					c.vbProcessingStats[e.VBucket]["dcp_stream_status"] = "running"
				}

				c.dcpMessagesProcessed[e.Opcode]++
				c.vbProcessingStats[e.VBucket]["last_processed_seq_no"] = e.Seqno

				if err := c.sendMessage(msg); err != nil {
					c.stopCheckpointingCh <- true
					c.producer.cleanupDeadConsumer(c)
					return
				}
				if resp := c.readMessage(); resp.err != nil {
					c.stopCheckpointingCh <- true
					c.producer.cleanupDeadConsumer(c)
					return
				}
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

	c.statsTicker.Stop()
	c.stopCheckpointingCh <- true
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

	end := uint64(0xFFFFFFFFFFFFFFFF)
	for vbno, flog := range flogs {
		x := flog[len(flog)-1] // map[uint16][][2]uint64
		opaque, flags, vbuuid := uint16(vbno), uint32(0), x[0]

		vbKey := fmt.Sprintf("%s_vb_%s", c.producer.AppName, strconv.Itoa(int(vbno)))
		var vbBlob vbucketKVBlob

		// TODO: More error handling
		gErr := c.metadataBucketHandle.Get(vbKey, &vbBlob)
		if gErr != nil {
			if gErr != nil {
				logging.Errorf("V8CR[%s:%s:%s:%d] Key: %s Bucket fetch failed, err: %v",
					c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbKey, gErr)
			}

			// Storing vbuuid in metadata bucket, will be required for start
			// stream later on
			vbBlob.VBuuid = vbuuid
			sErr := c.metadataBucketHandle.Set(vbKey, 0, &vbBlob)
			if sErr != nil {
				logging.Errorf("V8CR[%s:%s:%s:%d] Key: %s Bucket set failed, err: %v",
					c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbKey, sErr)
			}

			start, snapStart, snapEnd := uint64(0), uint64(0), uint64(0)
			fErr := c.dcpFeed.DcpRequestStream(
				vbno, opaque, flags, vbuuid, start, end, snapStart, snapEnd)
			if fErr != nil {
				logging.Errorf("V8CR[%s:%s:%s:%d] vb : %d Stream request failed, err: %v",
					c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbno, fErr)
			} else {
				logging.Infof("V8CR[%s:%s:%s:%d] vb : %d DCP Stream created",
					c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbno)
			}
		} else {
			start, snapStart, snapEnd := vbBlob.LastSeqNoProcessed,
				vbBlob.LastSeqNoProcessed, vbBlob.LastSeqNoProcessed

			logging.Infof("V8CR[%s:%s:%s:%d] DCP stream start vb: %d vbuuid: %d startSeq: %d",
				c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbno, vbBlob.VBuuid, start)

			fErr := c.dcpFeed.DcpRequestStream(
				vbno, opaque, flags, vbBlob.VBuuid, start, end, snapStart, snapEnd)
			if fErr != nil {
				logging.Errorf("V8CR[%s:%s:%s:%d] Stream req failed for vb: %d, err: %v",
					c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbno, fErr)
			}
		}
	}
}

func (c *Consumer) initCBBucketConnHandle() {
	config := c.app.Depcfg.(map[string]interface{})
	metadataBucket := config["metadata_bucket"].(string)
	connStr := fmt.Sprintf("http://" + c.producer.nsServerHostPort)

	conn, cErr := cbbucket.Connect(connStr)
	if cErr != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Failed to bootstrap conn to source cluster, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, cErr)
	}

	pool, pErr := conn.GetPool("default")
	if pErr != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Failed to get pool info, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, pErr)
	}

	var gbErr error
	c.metadataBucketHandle, gbErr = pool.GetBucket(metadataBucket)

	sleepDuration := time.Duration(1)
	for gbErr != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Bucket: %s missing, retrying after %d secs, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, metadataBucket, int(sleepDuration), gbErr)
		time.Sleep(sleepDuration * time.Second)

		conn, cErr = cbbucket.Connect(connStr)
		if cErr != nil {
			logging.Errorf("V8CR[%s:%s:%s:%d] Failed to bootstrap conn to source cluster, err: %v",
				c.producer.AppName, c.workerName, c.tcpPort, c.osPid, cErr)
		}

		pool, pErr = conn.GetPool("default")
		if pErr != nil {
			logging.Errorf("V8CR[%s:%s:%s:%d] Failed to get pool info, err: %v",
				c.producer.AppName, c.workerName, c.tcpPort, c.osPid, pErr)
		}

		c.metadataBucketHandle, gbErr = pool.GetBucket(metadataBucket)

		if sleepDuration < BACKOFF_THRESHOLD {
			sleepDuration = sleepDuration * 2
		}
	}
}

func (c *Consumer) doLastSeqNoCheckpoint() {
	c.checkpointTicker = time.NewTicker(CHECKPOINT_INTERVAL * time.Second)

	var err error

	// Leveraging ClusterInfoCache from secondary indexes
	hostAddress := fmt.Sprintf("127.0.0.1:%s", c.producer.NsServerPort)
	c.hostPortAddr, err = getCurrentEventingNodeAddress(c.producer.auth, hostAddress)

	sleepDuration := time.Duration(1)
	for err != nil {
		logging.Errorf("V8CR[%s:%s:%s:%d] Failed to grab routable interface, retrying after %d secs, err: %v",
			c.producer.AppName, c.workerName, c.tcpPort, c.osPid, int(sleepDuration), err)

		time.Sleep(sleepDuration * time.Second)
		c.hostPortAddr, err = getCurrentEventingNodeAddress(c.producer.auth, hostAddress)

		if sleepDuration < BACKOFF_THRESHOLD {
			sleepDuration = sleepDuration * 2
		}
	}

	for {
		select {
		case <-c.checkpointTicker.C:
			var vbBlob vbucketKVBlob
			c.RLock()
			for vbno, v := range c.vbProcessingStats {
				vbKey := fmt.Sprintf("%s_vb_%s", c.producer.AppName, strconv.Itoa(int(vbno)))

				var cas uint64
				gErr := c.metadataBucketHandle.Gets(vbKey, &vbBlob, &cas)
				if gErr != nil {
					logging.Errorf("V8CR[%s:%s:%s:%d] Bucket fetch failed for key: %s, err: %v",
						c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbKey, gErr)

					vbBlob.CurrentVBOwner = c.hostPortAddr
					vbBlob.LastSeqNoProcessed = v["last_processed_seq_no"].(uint64)
					vbBlob.DCPStreamStatus = v["dcp_stream_status"].(string)
					vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
					vbBlob.VBId = vbno

					// TODO: Helper function retries operation till it succeeds. Do we need another helper that retries
					// for finite attempts but then it would break the OOAO contract by more margin
					c.casWithRetry(vbKey, cas, &vbBlob, v, vbno)
					continue
				}

				if (c.hostPortAddr == vbBlob.CurrentVBOwner || vbBlob.CurrentVBOwner == "") &&
					vbBlob.NewVBOwner == "" {
					if vbBlob.CurrentVBOwner == "" {
						vbBlob.CurrentVBOwner = c.hostPortAddr
						vbBlob.DCPStreamStatus = v["dcp_stream_status"].(string)
						vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
						vbBlob.VBId = vbno
					}

					vbBlob.LastSeqNoProcessed = v["last_processed_seq_no"].(uint64)
					sErr := c.metadataBucketHandle.Set(vbKey, 0, &vbBlob)
					if sErr != nil {
						logging.Errorf("V8CR[%s:%s:%s:%d] Bucket set failed for key: %s, err: %v",
							c.producer.AppName, c.workerName, c.tcpPort, c.osPid, vbKey, sErr)
					}
				}
			}
			c.RUnlock()
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
