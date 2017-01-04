package producer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"time"

	cbbucket "github.com/couchbase/go-couchbase"
	"github.com/couchbase/indexing/secondary/dcp"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

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

	go c.startDcp(dcpConfig)

	var err error
	c.dcpFeed, err = c.cbBucket.StartDcpFeedOver(
		couchbase.NewDcpFeedName("eventing_"+c.workerName),
		uint32(0), c.producer.kvHostPort, 0xABCD, dcpConfig)
	sleepDuration := time.Duration(1)
	for err != nil {
		catchErr(fmt.Sprintf("Failed to start dcp feed, retrying after %d secs",
			int(sleepDuration)), err)

		time.Sleep(sleepDuration * time.Second)

		c.dcpFeed, err = c.cbBucket.StartDcpFeedOver(
			couchbase.NewDcpFeedName("eventing_"+c.workerName),
			uint32(0), c.producer.kvHostPort, 0xABCD, dcpConfig)

		if sleepDuration < BACKOFF_THRESHOLD {
			sleepDuration = sleepDuration * 2
		}
	}

	log.Printf("Spawning worker corresponding to producer running"+
		" on port %s\n", c.tcpPort)
	cmd := exec.Command("client", c.app.AppName, c.tcpPort,
		time.Now().UTC().Format("2006-01-02T15:04:05-0700"))

	err = cmd.Start()
	if err != nil {
		log.Fatal("Failed while trying to spawn worker for app:%s"+
			" with err: %s", c.app.AppName, err.Error())
	} else {
		c.osPid = cmd.Process.Pid
		log.Printf("pid of process launched: %d\n", c.osPid)
	}

	go func(c *Consumer) {
		cmd.Wait()
	}(c)

	// Wait for net.Conn to be initialised
	<-c.signalConnectedCh

	c.sendInitV8Worker(c.app.AppName)
	res := c.readMessage()
	log.Printf("Response from worker for init call: %s\n", res.response)

	c.sendLoadV8Worker(c.app.AppCode)
	res = c.readMessage()
	log.Printf("Response from worker for app load call: %s\n", res.response)

	go c.doLastSeqNoCheckpoint()

	for {
		select {
		case e, ok := <-c.dcpFeed.C:
			if ok == false {
				log.Printf("Closing DCP feed for bucket %q\n", c.producer.bucket)
				c.stopCheckpointingCh <- true
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
					c.vbProcessingStats[e.VBucket] = make(map[string]uint64)
					c.vbProcessingStats[e.VBucket]["last_processed_seq_no"] = 0
				}

				c.Lock()
				c.dcpMessagesProcessed[e.Opcode]++
				c.vbProcessingStats[e.VBucket]["last_processed_seq_no"] = e.Seqno
				c.Unlock()

				if err := c.sendMessage(msg); err != nil {
					c.stopCheckpointingCh <- true
					return
				}
				if resp := c.readMessage(); resp.err != nil {
					c.stopCheckpointingCh <- true
					return
				}
			}
		case <-c.statsTicker.C:
			c.RLock()
			log.Printf("Consumer: %s DCP opcode processing counter: %s\n",
				c.workerName, sprintDCPCounts(c.dcpMessagesProcessed))
			log.Printf("Consumer: %s V8 opcode processing counter: %s\n",
				c.workerName, sprintV8Counts(c.v8WorkerMessagesProcessed))
			c.RUnlock()
		case <-c.stopConsumerCh:
			log.Printf("Socket belonging to V8 consumer died\n")
			c.stopCheckpointingCh <- true
			return
		}
	}
}

func (c *Consumer) Stop() {
}

// Implement fmt.Stringer interface to allow better debugging
// if C++ V8 worker crashes
func (c *Consumer) String() string {
	return fmt.Sprintf("consumer => app: %s tcpPort: %s ospid: %d"+
		" dcpEventProcessed: %s v8EventProcessed: %s", c.app.AppName, c.tcpPort,
		c.osPid, sprintDCPCounts(c.dcpMessagesProcessed),
		sprintV8Counts(c.v8WorkerMessagesProcessed))
}

func (c *Consumer) startDcp(dcpConfig map[string]interface{}) {
	flogs, err := c.cbBucket.GetFailoverLogs(0xABCD, c.vbnos, dcpConfig)
	sleepDuration := time.Duration(1)
	for err != nil {
		catchErr(
			fmt.Sprintf("Failed to get dcp failover logs, retrying after %d secs",
				int(sleepDuration)), err)

		time.Sleep(sleepDuration * time.Second)
		flogs, err = c.cbBucket.GetFailoverLogs(0xABCD, c.vbnos, dcpConfig)

		if sleepDuration < BACKOFF_THRESHOLD {
			sleepDuration = sleepDuration * 2
		}
	}

	end := uint64(0xFFFFFFFFFFFFFFFF)
	for vbno, flog := range flogs {
		x := flog[len(flog)-1] // map[uint16][][2]uint64
		opaque, flags, vbuuid := uint16(vbno), uint32(0), x[0]

		vbKey := "vb_" + strconv.Itoa(int(vbno))
		var vbBlob vbucketKVBlob

		// TODO: More error handling
		err := c.metadataBucketHandle.Get(vbKey, &vbBlob)
		if err != nil {
			catchErr(
				fmt.Sprintf("Bucket fetch failed for key: %s, "+
					"creating stream from start seq #0", vbKey), err)
			start, snapStart, snapEnd := uint64(0), uint64(0), uint64(0)
			fErr := c.dcpFeed.DcpRequestStream(
				vbno, opaque, flags, vbuuid, start, end, snapStart, snapEnd)
			if fErr != nil {
				catchErr(fmt.Sprintf("stream-req for %v failed", vbno), fErr)
			} else {
				log.Printf("Consumer: %s vb: %d dcp stream created\n",
					c.workerName, vbno)
			}
		} else {
			start, snapStart, snapEnd := vbBlob.LastSeqNoProcessed,
				vbBlob.LastSeqNoProcessed, vbBlob.LastSeqNoProcessed
			log.Printf("Consumer: %s vb: %d starting DCP stream from seq #: %d\n",
				c.workerName, vbno, start)

			fErr := c.dcpFeed.DcpRequestStream(
				vbno, opaque, flags, vbuuid, start, end, snapStart, snapEnd)
			catchErr(fmt.Sprintf("stream-req for %v failed", vbno), fErr)
		}
	}
}

func (c *Consumer) initCBBucketConnHandle() {
	config := c.app.Depcfg.(map[string]interface{})
	metadataBucket := config["metadata_bucket"].(string)
	connStr := fmt.Sprintf("http://" + c.producer.nsServerHostPort)

	conn, cErr := cbbucket.Connect(connStr)
	catchErr("Failed to bootstrap conn to source cluster", cErr)

	pool, pErr := conn.GetPool("default")
	catchErr("Failed to get pool info", pErr)

	var gbErr error
	c.metadataBucketHandle, gbErr = pool.GetBucket(metadataBucket)

	sleepDuration := time.Duration(1)
	for gbErr != nil {
		catchErr(
			fmt.Sprintf("Bucket: %s missing, retrying after %d seconds, err",
				metadataBucket, int(sleepDuration)), gbErr)
		time.Sleep(sleepDuration * time.Second)

		conn, cErr = cbbucket.Connect(connStr)
		catchErr("Failed to bootstrap conn to source cluster", cErr)

		pool, pErr = conn.GetPool("default")
		catchErr("Failed to get pool info", pErr)

		c.metadataBucketHandle, gbErr = pool.GetBucket(metadataBucket)

		if sleepDuration < BACKOFF_THRESHOLD {
			sleepDuration = sleepDuration * 2
		}
	}
}

func (c *Consumer) doLastSeqNoCheckpoint() {
	c.checkpointTicker = time.NewTicker(CHECKPOINT_INTERVAL * time.Second)

	// Leveraging ClusterInfoCache from secondary indexes
	ipAddr, err := getLocalEventingServiceHost(c.producer.auth, c.producer.nsServerHostPort)
	if err != nil {
		catchErr("Failed to grab routable network interface", err)
		return
	}
	for {
		select {
		case <-c.checkpointTicker.C:
			var vbBlob vbucketKVBlob
			c.RLock()
			for k, v := range c.vbProcessingStats {
				vbKey := "vb_" + strconv.Itoa(int(k))
				err := c.metadataBucketHandle.Get(vbKey, &vbBlob)
				if err != nil {
					catchErr(fmt.Sprintf("Bucket fetch failed for key: %s", vbKey), err)
					vbBlob.CurrentVBOwner = ipAddr
					vbBlob.LastSeqNoProcessed = v["last_processed_seq_no"]
					err := c.metadataBucketHandle.Set(vbKey, 0, &vbBlob)
					catchErr(
						fmt.Sprintf("Bucket set operation failed for key: %s", vbKey), err)
					continue
				}

				if (ipAddr == vbBlob.CurrentVBOwner || vbBlob.CurrentVBOwner == "") &&
					vbBlob.NewVBOwner == "" {
					if vbBlob.CurrentVBOwner == "" {
						vbBlob.CurrentVBOwner = ipAddr
					}
					vbBlob.LastSeqNoProcessed = v["last_processed_seq_no"]
					err := c.metadataBucketHandle.Set(vbKey, 0, &vbBlob)
					catchErr(
						fmt.Sprintf("Bucket set operation failed for key: %s", vbKey), err)
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
	catchErr("Failure hile writing header size", err)

	err = binary.Write(&buffer, binary.LittleEndian, uint32(len(msg.Payload)))
	catchErr("Failure while writing payload size", err)

	err = binary.Write(&buffer, binary.LittleEndian, msg.Header)
	catchErr("Failure while writing encoded header", err)

	err = binary.Write(&buffer, binary.LittleEndian, msg.Payload)
	catchErr("Failure while writing encoded payload", err)

	err = binary.Write(c.conn, binary.LittleEndian, buffer.Bytes())
	c.catchDeadConnection("Write to consumer socket", err)

	return err
}

func (c *Consumer) readMessage() *Response {
	var result *Response
	msg, err := bufio.NewReader(c.conn).ReadSlice('\n')
	if err != nil {
		log.Printf("Read from client socket failed, err: %s\n", err.Error())

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

func (c *Consumer) catchDeadConnection(context string, err error) {
	if err != nil {
		log.Printf("Write to downstream socket failed while %s, err: %s\n",
			context, err.Error())

		c.stopConsumerCh <- true
		c.conn.Close()
	}
}
