package consumer

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"strconv"
	"time"

	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

func (c *Consumer) sendLogLevel(logLevel string, sendToDebugger bool) error {
	header := makeLogLevelHeader(logLevel)

	msg := &message{
		Header: header,
	}

	return c.sendMessage(msg, 0, 0, false, sendToDebugger, true)
}

func (c *Consumer) sendWorkerThrCount(thrCount int, sendToDebugger bool) error {
	var header []byte
	if sendToDebugger {
		header = makeThrCountHeader(strconv.Itoa(thrCount))
	} else {
		header = makeThrCountHeader(strconv.Itoa(c.cppWorkerThrCount))
	}

	msg := &message{
		Header: header,
	}

	if _, ok := c.v8WorkerMessagesProcessed["THR_COUNT"]; !ok {
		c.v8WorkerMessagesProcessed["THR_COUNT"] = 0
	}
	c.v8WorkerMessagesProcessed["THR_COUNT"]++

	return c.sendMessage(msg, 0, 0, false, sendToDebugger, true)
}

func (c *Consumer) sendWorkerThrMap(thrPartitionMap map[int][]uint16, sendToDebugger bool) error {
	header := makeThrMapHeader()

	var payload []byte
	if sendToDebugger {
		payload = makeThrMapPayload(thrPartitionMap, cppWorkerPartitionCount)
	} else {
		payload = makeThrMapPayload(c.cppThrPartitionMap, cppWorkerPartitionCount)
	}

	msg := &message{
		Header:  header,
		Payload: payload,
	}

	if _, ok := c.v8WorkerMessagesProcessed["THR_MAP"]; !ok {
		c.v8WorkerMessagesProcessed["THR_MAP"] = 0
	}
	c.v8WorkerMessagesProcessed["THR_MAP"]++

	return c.sendMessage(msg, 0, 0, false, sendToDebugger, true)
}

func (c *Consumer) sendDebuggerStart() error {

	header := makeV8DebuggerStartHeader()

	msg := &message{
		Header: header,
	}

	if _, ok := c.v8WorkerMessagesProcessed["DEBUG_START"]; !ok {
		c.v8WorkerMessagesProcessed["DEBUG_START"] = 0
	}
	c.v8WorkerMessagesProcessed["DEBUG_START"]++

	return c.sendMessage(msg, 0, 0, false, true, true)
}

func (c *Consumer) sendDebuggerStop() error {

	header := makeV8DebuggerStopHeader()

	msg := &message{
		Header: header,
	}

	if _, ok := c.v8WorkerMessagesProcessed["DEBUG_STOP"]; !ok {
		c.v8WorkerMessagesProcessed["DEBUG_STOP"] = 0
	}
	c.v8WorkerMessagesProcessed["DEBUG_STOP"]++

	return c.sendMessage(msg, 0, 0, false, true, true)
}

func (c *Consumer) sendInitV8Worker(payload []byte, sendToDebugger bool) error {

	header := makeV8InitOpcodeHeader()

	msg := &message{
		Header:  header,
		Payload: payload,
	}

	if _, ok := c.v8WorkerMessagesProcessed["V8_INIT"]; !ok {
		c.v8WorkerMessagesProcessed["V8_INIT"] = 0
	}
	c.v8WorkerMessagesProcessed["V8_INIT"]++

	return c.sendMessage(msg, 0, 0, false, sendToDebugger, true)
}

func (c *Consumer) sendLoadV8Worker(appCode string, sendToDebugger bool) error {

	header := makeV8LoadOpcodeHeader(appCode)

	msg := &message{
		Header: header,
	}

	if _, ok := c.v8WorkerMessagesProcessed["V8_LOAD"]; !ok {
		c.v8WorkerMessagesProcessed["V8_LOAD"] = 0
	}
	c.v8WorkerMessagesProcessed["V8_LOAD"]++

	return c.sendMessage(msg, 0, 0, false, sendToDebugger, true)
}

func (c *Consumer) sendGetLatencyStats(sendToDebugger bool) error {
	header := makeHeader(v8WorkerEvent, v8WorkerLatencyStats, 0, "")

	msg := &message{
		Header: header,
	}

	if _, ok := c.v8WorkerMessagesProcessed["LATENCY_STATS"]; !ok {
		c.v8WorkerMessagesProcessed["LATENCY_STATS"] = 0
	}
	c.v8WorkerMessagesProcessed["LATENCY_STATS"]++

	return c.sendMessage(msg, 0, 0, false, sendToDebugger, true)
}

func (c *Consumer) sendGetFailureStats(sendToDebugger bool) error {
	header := makeHeader(v8WorkerEvent, v8WorkerFailureStats, 0, "")

	msg := &message{
		Header: header,
	}

	if _, ok := c.v8WorkerMessagesProcessed["FAILURE_STATS"]; !ok {
		c.v8WorkerMessagesProcessed["FAILURE_STATS"] = 0
	}
	c.v8WorkerMessagesProcessed["FAILURE_STATS"]++

	return c.sendMessage(msg, 0, 0, false, sendToDebugger, true)
}

func (c *Consumer) sendGetSourceMap(sendToDebugger bool) error {
	header := makeHeader(v8WorkerEvent, v8WorkerSourceMap, 0, "")

	msg := &message{
		Header: header,
	}

	if _, ok := c.v8WorkerMessagesProcessed["SOURCE_MAP"]; !ok {
		c.v8WorkerMessagesProcessed["SOURCE_MAP"] = 0
	}
	c.v8WorkerMessagesProcessed["SOURCE_MAP"]++

	return c.sendMessage(msg, 0, 0, false, sendToDebugger, true)
}

func (c *Consumer) sendGetHandlerCode(sendToDebugger bool) error {
	header := makeHeader(v8WorkerEvent, v8WorkerHandlerCode, 0, "")

	msg := &message{
		Header: header,
	}

	if _, ok := c.v8WorkerMessagesProcessed["HANDLER_CODE"]; !ok {
		c.v8WorkerMessagesProcessed["HANDLER_CODE"] = 0
	}
	c.v8WorkerMessagesProcessed["HANDLER_CODE"]++

	return c.sendMessage(msg, 0, 0, false, sendToDebugger, true)
}

func (c *Consumer) sendDocTimerEvent(e *byTimerEntry, sendToDebugger bool) {
	partition := int16(util.VbucketByKey([]byte(e.DocID), cppWorkerPartitionCount))
	timerHeader := makeDocTimerEventHeader(partition)
	timerPayload := makeDocTimerPayload(e.DocID, e.CallbackFn)

	msg := &message{
		Header:  timerHeader,
		Payload: timerPayload,
	}

	util.Retry(util.NewFixedBackoff(5*time.Second), sendMsgCallback, c, msg, uint16(0), uint64(0), false, sendToDebugger, false)
}

func (c *Consumer) sendNonDocTimerEvent(payload string, sendToDebugger bool) {
	partition := int16(util.VbucketByKey([]byte(payload), cppWorkerPartitionCount))
	timerHeader := makeNonDocTimerEventHeader(partition)
	timerPayload := makeNonDocTimerPayload(payload)

	msg := &message{
		Header:  timerHeader,
		Payload: timerPayload,
	}

	util.Retry(util.NewFixedBackoff(5*time.Second), sendMsgCallback, c, msg, uint16(0), uint64(0), false, sendToDebugger, false)
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
		dcpHeader = makeDcpMutationHeader(partition, string(metadata))
	}

	if e.Opcode == mcd.DCP_DELETION {
		dcpHeader = makeDcpDeletionHeader(partition, string(metadata))
	}

	dcpPayload := makeDcpPayload(e.Key, e.Value)
	msg := &message{
		Header:  dcpHeader,
		Payload: dcpPayload,
	}

	util.Retry(util.NewFixedBackoff(5*time.Second), sendMsgCallback, c, msg, e.VBucket, e.Seqno, true, sendToDebugger, false)
}

var sendMsgCallback = func(args ...interface{}) error {
	c := args[0].(*Consumer)
	msg := args[1].(*message)
	vb := args[2].(uint16)
	seqno := args[3].(uint64)
	shouldCheckpoint := args[4].(bool)
	sendToDebugger := args[5].(bool)
	prioritise := args[6].(bool)

	err := c.sendMessage(msg, vb, seqno, shouldCheckpoint, sendToDebugger, prioritise)
	return err
}

func (c *Consumer) sendMessage(msg *message, vb uint16, seqno uint64, shouldCheckpoint bool, sendToDebugger bool, prioritise bool) error {
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
		c.Lock()
		if _, ok := c.writeBatchSeqnoMap[vb]; !ok {
			c.writeBatchSeqnoMap[vb] = seqno
		}
		c.writeBatchSeqnoMap[vb] = seqno
		c.Unlock()
	}

	if c.sendMsgCounter >= c.socketWriteBatchSize || prioritise {
		c.connMutex.Lock()
		defer c.connMutex.Unlock()

		if !sendToDebugger && c.conn != nil {
			c.conn.SetWriteDeadline(time.Now().Add(c.socketTimeout))

			err = binary.Write(c.conn, binary.LittleEndian, c.sendMsgBuffer.Bytes())
			if err != nil {
				logging.Errorf("CRHM[%s:%s:%s:%d] Write to downstream socket failed, err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
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

		var err error
		if !sendToDebugger && c.conn != nil {
			if err = c.readMessage(sendToDebugger); err != nil {
				logging.Errorf("CRHM[%s:%s:%s:%d] Read message: Closing conn: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.conn)
				c.client.Stop()
				return err
			}
		} else {
			err = c.readMessage(sendToDebugger)
		}

		if sendToDebugger && err == nil {
			c.sendMsgToDebugger = true
		}

		c.RLock()
		if len(c.writeBatchSeqnoMap) > 0 {
			logging.Tracef("CRHM[%s:%s:%s:%d] WriteBatchSeqNo dump: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), c.writeBatchSeqnoMap)
		}

		for vb, seqno := range c.writeBatchSeqnoMap {
			c.vbProcessingStats.updateVbStat(vb, "last_processed_seq_no", seqno)
		}
		c.RUnlock()

		c.Lock()
		c.writeBatchSeqnoMap = make(map[uint16]uint64)
		c.Unlock()
	}

	return nil
}

func (c *Consumer) readMessage(readFromDebugger bool) error {
	if !readFromDebugger && c.conn != nil {
		c.conn.SetReadDeadline(time.Now().Add(c.socketTimeout))

		msg, err := bufio.NewReader(c.conn).ReadBytes('\r')
		if err != nil {
			logging.Errorf("CRHM[%s:%s:%s:%d] Read from client socket failed, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
		} else {
			if len(msg) > 1 {
				c.parseWorkerResponse(msg[:len(msg)-1], 0)
			}
		}
		return err
	}

	if c.debugConn != nil {
		msg, err := bufio.NewReader(c.debugConn).ReadBytes('\r')
		if err != nil {
			logging.Errorf("CRHM[%s:%s:%s:%d] Read from debug enabled worker socket failed, err: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
			c.sendMsgToDebugger = false
		} else {
			if len(msg) > 1 {
				c.parseWorkerResponse(msg[:len(msg)-1], 0)
			}
		}
		return err
	}

	return nil
}
