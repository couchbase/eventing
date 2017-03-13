package consumer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/couchbase/eventing/flatbuf/worker_response"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
)

func (c *Consumer) sendLogLevel(logLevel string) error {
	header := makeLogLevelHeader(logLevel)

	msg := &message{
		Header: header,
	}

	return c.sendMessage(msg)
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

	return c.sendMessage(msg)
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

	return c.sendMessage(msg)
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
			c.app.AppName, c.workerName, c.tcpPort, c.osPid, string(e.Key))
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

	c.vbProcessingStats.updateVbStat(e.VBucket, "last_processed_seq_no", e.Seqno)

	if err := c.sendMessage(msg); err != nil {
		c.stopCheckpointingCh <- true
		c.gracefulShutdownChan <- true
		return
	}

	if resp := c.readMessage(); resp.err != nil {
		c.stopCheckpointingCh <- true
		c.gracefulShutdownChan <- true
		return
	}
}

func (c *Consumer) sendMessage(msg *message) error {

	// Protocol encoding format:
	//<headerSize><payloadSize><Header><Payload>
	var buffer bytes.Buffer

	// For debugging
	// event := ReadHeader(msg.Header)
	// if event == int8(DcpEvent) {
	// 	ReadPayload(msg.Payload)
	// }

	err := binary.Write(&buffer, binary.LittleEndian, uint32(len(msg.Header)))
	if err != nil {
		logging.Errorf("CRHM[%s:%s:%s:%d] Failure while writing header size, err : %v",
			c.app.AppName, c.workerName, c.tcpPort, c.osPid, err)
	}

	err = binary.Write(&buffer, binary.LittleEndian, uint32(len(msg.Payload)))
	if err != nil {
		logging.Errorf("CRHM[%s:%s:%s:%d] Failure while writing payload size, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.osPid, err)
	}

	err = binary.Write(&buffer, binary.LittleEndian, msg.Header)
	if err != nil {
		logging.Errorf("CRHM[%s:%s:%s:%d] Failure while writing encoded header, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.osPid, err)
	}

	err = binary.Write(&buffer, binary.LittleEndian, msg.Payload)
	if err != nil {
		logging.Errorf("CRHM[%s:%s:%s:%d] Failure while writing encoded payload, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.osPid, err)
	}

	c.conn.SetWriteDeadline(time.Now().Add(WriteDeadline))
	err = binary.Write(c.conn, binary.LittleEndian, buffer.Bytes())
	if err != nil {
		logging.Errorf("CRHM[%s:%s:%s:%d] Write to downstream socket failed, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.osPid, err)
		c.stopConsumerCh <- true
		c.conn.Close()
	}

	return err
}

func (c *Consumer) readMessage() *response {
	var result *response
	c.conn.SetReadDeadline(time.Now().Add(ReadDeadline))
	msg, err := bufio.NewReader(c.conn).ReadSlice('\n')
	if err != nil {
		logging.Errorf("CRHM[%s:%s:%s:%d] Read from client socket failed, err: %v",
			c.app.AppName, c.workerName, c.tcpPort, c.osPid, err)

		c.stopConsumerCh <- true
		c.conn.Close()

		result = &response{
			res: "",
			err: err,
		}
	} else {
		decodedRes := worker_response.GetRootAsMessage(msg, 0)
		logEntry := string(decodedRes.LogEntry())

		result = &response{
			logEntry: logEntry,
			err:      err,
		}
		fmt.Printf(result.logEntry)
	}
	return result
}
