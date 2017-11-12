package consumer

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

func (c *Consumer) sendLogLevel(logLevel string, sendToDebugger bool) {
	header := c.makeLogLevelHeader(logLevel)

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
	}

	c.msgToCppWorkerCh <- m
}

func (c *Consumer) sendWorkerThrCount(thrCount int, sendToDebugger bool) {
	var header []byte
	if sendToDebugger {
		header = c.makeThrCountHeader(strconv.Itoa(thrCount))
	} else {
		header = c.makeThrCountHeader(strconv.Itoa(c.cppWorkerThrCount))
	}

	if _, ok := c.v8WorkerMessagesProcessed["THR_COUNT"]; !ok {
		c.v8WorkerMessagesProcessed["THR_COUNT"] = 0
	}
	c.v8WorkerMessagesProcessed["THR_COUNT"]++

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
	}

	c.msgToCppWorkerCh <- m
}

func (c *Consumer) sendWorkerThrMap(thrPartitionMap map[int][]uint16, sendToDebugger bool) {
	header := c.makeThrMapHeader()

	var payload []byte
	if sendToDebugger {
		payload = c.makeThrMapPayload(thrPartitionMap, cppWorkerPartitionCount)
	} else {
		payload = c.makeThrMapPayload(c.cppThrPartitionMap, cppWorkerPartitionCount)
	}

	if _, ok := c.v8WorkerMessagesProcessed["THR_MAP"]; !ok {
		c.v8WorkerMessagesProcessed["THR_MAP"] = 0
	}
	c.v8WorkerMessagesProcessed["THR_MAP"]++

	m := &msgToTransmit{
		msg: &message{
			Header:  header,
			Payload: payload,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
	}

	c.msgToCppWorkerCh <- m
}

func (c *Consumer) sendDebuggerStart() {

	header := c.makeV8DebuggerStartHeader()

	if _, ok := c.v8WorkerMessagesProcessed["DEBUG_START"]; !ok {
		c.v8WorkerMessagesProcessed["DEBUG_START"] = 0
	}
	c.v8WorkerMessagesProcessed["DEBUG_START"]++

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: true,
		prioritize:     true,
	}

	c.msgToCppWorkerCh <- m
}

func (c *Consumer) sendDebuggerStop() {

	header := c.makeV8DebuggerStopHeader()

	if _, ok := c.v8WorkerMessagesProcessed["DEBUG_STOP"]; !ok {
		c.v8WorkerMessagesProcessed["DEBUG_STOP"] = 0
	}
	c.v8WorkerMessagesProcessed["DEBUG_STOP"]++

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: true,
		prioritize:     true,
	}

	c.msgToCppWorkerCh <- m
}

func (c *Consumer) sendInitV8Worker(payload []byte, sendToDebugger bool) {

	header := c.makeV8InitOpcodeHeader()

	if _, ok := c.v8WorkerMessagesProcessed["V8_INIT"]; !ok {
		c.v8WorkerMessagesProcessed["V8_INIT"] = 0
	}
	c.v8WorkerMessagesProcessed["V8_INIT"]++

	m := &msgToTransmit{
		msg: &message{
			Header:  header,
			Payload: payload,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
	}

	c.msgToCppWorkerCh <- m
}

func (c *Consumer) sendLoadV8Worker(appCode string, sendToDebugger bool) {

	header := c.makeV8LoadOpcodeHeader(appCode)

	if _, ok := c.v8WorkerMessagesProcessed["V8_LOAD"]; !ok {
		c.v8WorkerMessagesProcessed["V8_LOAD"] = 0
	}
	c.v8WorkerMessagesProcessed["V8_LOAD"]++

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
	}

	c.msgToCppWorkerCh <- m
}

func (c *Consumer) sendGetLatencyStats(sendToDebugger bool) {
	header := c.makeHeader(v8WorkerEvent, v8WorkerLatencyStats, 0, "")

	if _, ok := c.v8WorkerMessagesProcessed["LATENCY_STATS"]; !ok {
		c.v8WorkerMessagesProcessed["LATENCY_STATS"] = 0
	}
	c.v8WorkerMessagesProcessed["LATENCY_STATS"]++

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
	}

	c.msgToCppWorkerCh <- m
}

func (c *Consumer) sendGetFailureStats(sendToDebugger bool) {
	header := c.makeHeader(v8WorkerEvent, v8WorkerFailureStats, 0, "")

	if _, ok := c.v8WorkerMessagesProcessed["FAILURE_STATS"]; !ok {
		c.v8WorkerMessagesProcessed["FAILURE_STATS"] = 0
	}
	c.v8WorkerMessagesProcessed["FAILURE_STATS"]++

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
	}

	c.msgToCppWorkerCh <- m
}

func (c *Consumer) sendGetExecutionStats(sendToDebugger bool) {
	header := c.makeHeader(v8WorkerEvent, v8WorkerExecutionStats, 0, "")

	if _, ok := c.v8WorkerMessagesProcessed["EXECUTION_STATS"]; !ok {
		c.v8WorkerMessagesProcessed["EXECUTION_STATS"] = 0
	}
	c.v8WorkerMessagesProcessed["EXECUTION_STATS"]++

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
	}

	c.msgToCppWorkerCh <- m
}

func (c *Consumer) sendGetSourceMap(sendToDebugger bool) {
	header := c.makeHeader(v8WorkerEvent, v8WorkerSourceMap, 0, "")

	if _, ok := c.v8WorkerMessagesProcessed["SOURCE_MAP"]; !ok {
		c.v8WorkerMessagesProcessed["SOURCE_MAP"] = 0
	}
	c.v8WorkerMessagesProcessed["SOURCE_MAP"]++

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
	}

	c.msgToCppWorkerCh <- m
}

func (c *Consumer) sendGetHandlerCode(sendToDebugger bool) {
	header := c.makeHeader(v8WorkerEvent, v8WorkerHandlerCode, 0, "")

	if _, ok := c.v8WorkerMessagesProcessed["HANDLER_CODE"]; !ok {
		c.v8WorkerMessagesProcessed["HANDLER_CODE"] = 0
	}
	c.v8WorkerMessagesProcessed["HANDLER_CODE"]++

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
	}

	c.msgToCppWorkerCh <- m
}

func (c *Consumer) sendDocTimerEvent(e *byTimerEntry, sendToDebugger bool) {
	partition := int16(util.VbucketByKey([]byte(e.DocID), cppWorkerPartitionCount))
	timerHeader := c.makeDocTimerEventHeader(partition)
	timerPayload := c.makeDocTimerPayload(e.DocID, e.CallbackFn)

	m := &msgToTransmit{
		msg: &message{
			Header:  timerHeader,
			Payload: timerPayload,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     false,
	}

	c.msgToCppWorkerCh <- m

}

func (c *Consumer) sendNonDocTimerEvent(payload string, sendToDebugger bool) {
	partition := int16(util.VbucketByKey([]byte(payload), cppWorkerPartitionCount))
	timerHeader := c.makeNonDocTimerEventHeader(partition)
	timerPayload := c.makeNonDocTimerPayload(payload)

	m := &msgToTransmit{
		msg: &message{
			Header:  timerHeader,
			Payload: timerPayload,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     false,
	}

	c.msgToCppWorkerCh <- m
}

func (c *Consumer) sendDcpEvent(e *memcached.DcpEvent, sendToDebugger bool) {

	if sendToDebugger {
	checkDebuggerStarted:
		if !c.debuggerStarted {
			time.Sleep(retryInterval)
			goto checkDebuggerStarted
		}
	}

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

	partition := int16(util.VbucketByKey(e.Key, cppWorkerPartitionCount))

	var dcpHeader []byte
	if e.Opcode == mcd.DCP_MUTATION {
		dcpHeader = c.makeDcpMutationHeader(partition, string(metadata))
	}

	if e.Opcode == mcd.DCP_DELETION {
		dcpHeader = c.makeDcpDeletionHeader(partition, string(metadata))
	}

	dcpPayload := c.makeDcpPayload(e.Key, e.Value)

	msg := &msgToTransmit{
		msg: &message{
			Header:  dcpHeader,
			Payload: dcpPayload,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     false,
	}

	c.msgToCppWorkerCh <- msg
}

func (c *Consumer) sendMessageLoop() {

	// Flush any entry in stop channel. Entry could have come in as part of bootstrap
	select {
	case <-c.socketWriteLoopStopCh:
	default:
	}
	c.socketWriteLoopStopAckCh = make(chan struct{}, 1)

	for {
		select {
		case m := <-c.msgToCppWorkerCh:
			c.sendMessage(m.msg, m.sendToDebugger, m.prioritize)
		case <-c.socketWriteTicker.C:
			if c.sendMsgCounter > 0 {
				c.conn.SetWriteDeadline(time.Now().Add(c.socketTimeout))

				err := binary.Write(c.conn, binary.LittleEndian, c.sendMsgBuffer.Bytes())
				if err != nil {
					logging.Errorf("CRHM[%s:%s:%s:%d] Write to downstream socket failed, err: %v",
						c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
					c.client.Stop()
				}

				// Reset the sendMessage buffer and message counter
				c.sendMsgBuffer.Reset()
				c.sendMsgCounter = 0
			}
		case <-c.socketWriteLoopStopCh:
			c.socketWriteLoopStopAckCh <- struct{}{}
			return
		}
	}
}

func (c *Consumer) sendMessage(msg *message, sendToDebugger bool, prioritise bool) error {
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

	if c.sendMsgCounter >= c.socketWriteBatchSize || prioritise || sendToDebugger {
		c.connMutex.Lock()
		defer c.connMutex.Unlock()

		if !sendToDebugger && c.conn != nil {
			c.conn.SetWriteDeadline(time.Now().Add(c.socketTimeout))

			err = binary.Write(c.conn, binary.LittleEndian, c.sendMsgBuffer.Bytes())
			if err != nil {
				logging.Errorf("CRHM[%s:%s:%s:%d] Write to downstream socket failed, err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
				c.client.Stop()
				return err
			}
		} else if c.debugConn != nil {
			err = binary.Write(c.debugConn, binary.LittleEndian, c.sendMsgBuffer.Bytes())
			if err != nil {
				logging.Errorf("CRHM[%s:%s:%s:%d] Write to debug enabled worker socket failed, err: %v",
					c.app.AppName, c.workerName, c.debugTCPPort, c.Pid(), err)
				c.debugConn.Close()
				return err
			}
			c.sendMsgToDebugger = false
		}

		// Reset the sendMessage buffer and message counter
		c.sendMsgBuffer.Reset()
		c.sendMsgCounter = 0
	}

	return nil
}

func (c *Consumer) readMessageLoop() {

	for {
		buffer := make([]byte, 4096)
		bytesRead, err := c.sockReader.Read(buffer)

		if err == io.EOF || bytesRead == 0 {
			fmt.Printf("Exiting for loop, which is reading from socket, err: %v\n", err)
			c.client.Stop()
			break
		}
		if err != nil {
			logging.Errorf("CRHM[%s:%s:%s:%d] Read from client socket failed, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
			c.client.Stop()
			return
		}

		if bytesRead < len(buffer) {
			buffer = buffer[:bytesRead]
		}

		if bytesRead >= headerFragmentSize || c.readMsgBuffer.Len() >= headerFragmentSize {

		parseMessage:
			if c.readMsgBuffer.Len() > 0 {
				buffer = append(c.readMsgBuffer.Bytes(), buffer...)
				c.readMsgBuffer.Reset()
			}

			headerSize := binary.LittleEndian.Uint32(buffer[:headerFragmentSize])

			if len(buffer) >= int(headerFragmentSize+headerSize) {

				c.parseWorkerResponse(buffer[headerFragmentSize : headerFragmentSize+headerSize])
				buffer = buffer[headerFragmentSize+headerSize:]

				c.readMsgBuffer.Write(buffer)

				if c.readMsgBuffer.Len() > headerFragmentSize {
					buffer = buffer[:0]
					goto parseMessage
				}
			} else {
				c.readMsgBuffer.Write(buffer)
				buffer = buffer[:0]
			}

		} else {
			c.readMsgBuffer.Write(buffer)
			buffer = buffer[:0]
		}
	}
}
