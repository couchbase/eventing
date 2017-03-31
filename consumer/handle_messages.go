package consumer

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
)

func (c *Consumer) sendLogLevel(logLevel string) error {
	header := makeLogLevelHeader(logLevel)

	msg := &message{
		Header: header,
	}

	return c.sendMessage(msg, 0, 0, false)
}

func (c *Consumer) sendInitV8Worker(payload []byte) error {

	header := makeV8InitOpcodeHeader()

	msg := &message{
		Header:  header,
		Payload: payload,
	}

	if _, ok := c.v8WorkerMessagesProcessed["V8_INIT"]; !ok {
		c.v8WorkerMessagesProcessed["V8_INIT"] = 0
	}
	c.v8WorkerMessagesProcessed["V8_INIT"]++

	return c.sendMessage(msg, 0, 0, false)
}

func (c *Consumer) sendLoadV8Worker(appCode string) error {

	header := makeV8LoadOpcodeHeader(appCode)

	msg := &message{
		Header: header,
	}

	if _, ok := c.v8WorkerMessagesProcessed["V8_LOAD"]; !ok {
		c.v8WorkerMessagesProcessed["V8_LOAD"] = 0
	}
	c.v8WorkerMessagesProcessed["V8_LOAD"]++

	return c.sendMessage(msg, 0, 0, false)
}

func (c *Consumer) sendDcpEvent(e *memcached.DcpEvent) {
	m := dcpMetadata{
		Cas:     e.Cas,
		DocID:   string(e.Key),
		Expiry:  e.Expiry,
		Flag:    e.Flags,
		Vbucket: e.VBucket,
		SeqNo:   e.Seqno,
	}

	metadata, err := json.Marshal(&m)
	if err != nil {
		logging.Errorf("CRHM[%s:%s:%s:%d] key: %v failed to marshal metadata",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), string(e.Key))
		return
	}

	var dcpHeader []byte
	if e.Opcode == mcd.DCP_MUTATION {
		dcpHeader = makeDcpMutationHeader(string(metadata))
	}

	if e.Opcode == mcd.DCP_DELETION {
		dcpHeader = makeDcpDeletionHeader(string(metadata))
	}

	dcpPayload := makeDcpPayload(e.Key, e.Value)
	msg := &message{
		Header:  dcpHeader,
		Payload: dcpPayload,
	}

	if err := c.sendMessage(msg, e.VBucket, e.Seqno, true); err != nil {
		return
	}

}

func (c *Consumer) sendMessage(msg *message, vb uint16, seqno uint64, shouldCheckpoint bool) error {

	// Protocol encoding format:
	//<headerSize><payloadSize><Header><Payload>

	// For debugging
	// event := ReadHeader(msg.Header)
	// if event == int8(DcpEvent) {
	// 	ReadPayload(msg.Payload)
	// }

	err := binary.Write(&c.sendMsgBuffer, binary.LittleEndian, uint32(len(msg.Header)))
	if err != nil {
		logging.Errorf("CRHM[%s:%s:%s:%d] Failure while writing header size, err : %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
		return err
	}

	err = binary.Write(&c.sendMsgBuffer, binary.LittleEndian, uint32(len(msg.Payload)))
	if err != nil {
		logging.Errorf("CRHM[%s:%s:%s:%d] Failure while writing payload size, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
		return err
	}

	err = binary.Write(&c.sendMsgBuffer, binary.LittleEndian, msg.Header)
	if err != nil {
		logging.Errorf("CRHM[%s:%s:%s:%d] Failure while writing encoded header, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
		return err
	}

	err = binary.Write(&c.sendMsgBuffer, binary.LittleEndian, msg.Payload)
	if err != nil {
		logging.Errorf("CRHM[%s:%s:%s:%d] Failure while writing encoded payload, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
		return err
	}

	c.sendMsgCounter++
	if shouldCheckpoint {
		if _, ok := c.writeBatchSeqnoMap[vb]; !ok {
			c.writeBatchSeqnoMap[vb] = seqno
		}
		c.writeBatchSeqnoMap[vb] = seqno
	}

	if c.sendMsgCounter >= c.socketWriteBatchSize {
		logging.Infof("CRHM[%s:%s:%s:%d] SendMsgCounter: %v, batch size: %v buffer len: %v cap: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.sendMsgCounter, c.socketWriteBatchSize,
			c.sendMsgBuffer.Len(), c.sendMsgBuffer.Cap())

		c.conn.SetWriteDeadline(time.Now().Add(WriteDeadline))

		err = binary.Write(c.conn, binary.LittleEndian, c.sendMsgBuffer.Bytes())
		if err != nil {
			logging.Errorf("CRHM[%s:%s:%s:%d] Write to downstream socket failed, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
			c.stopConsumerCh <- true
			c.stopCheckpointingCh <- true
			c.gracefulShutdownChan <- true
			c.conn.Close()
			return err
		}

		// Reset the sendMessage buffer and message counter
		c.sendMsgBuffer.Reset()
		c.sendMsgCounter = 0

		if err := c.readMessage(); err != nil {
			c.stopCheckpointingCh <- true
			c.gracefulShutdownChan <- true
		}

		for vb, seqno := range c.writeBatchSeqnoMap {
			c.vbProcessingStats.updateVbStat(vb, "last_processed_seq_no", seqno)
		}

		c.writeBatchSeqnoMap = make(map[uint16]uint64)
	}

	return nil
}

func (c *Consumer) readMessage() error {
	c.conn.SetReadDeadline(time.Now().Add(ReadDeadline))
	msg, err := bufio.NewReader(c.conn).ReadBytes('\r')
	if err != nil {
		logging.Errorf("CRHM[%s:%s:%s:%d] Read from client socket failed, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)

		c.stopConsumerCh <- true
		c.conn.Close()
	} else {
		if len(msg) > 1 {
			fmt.Println(string(msg))
		}
	}
	return err
}
