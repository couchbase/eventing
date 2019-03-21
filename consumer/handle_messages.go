package consumer

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"time"

	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/google/flatbuffers/go"
)

func (c *Consumer) sendLogLevel(logLevel string, sendToDebugger bool) {
	header, hBuilder := c.makeLogLevelHeader(logLevel)

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["log_level"]; !ok {
		c.v8WorkerMessagesProcessed["log_level"] = 0
	}
	c.v8WorkerMessagesProcessed["log_level"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) sendTimerContextSize(timerContextSize int64, sendToDebugger bool) {
	logPrefix := "Consumer::sendTimerContextSize"

	header, hBuilder := c.makeTimerContextSizeHeader(fmt.Sprintf("%d", timerContextSize))

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["timer_context_size"]; !ok {
		c.v8WorkerMessagesProcessed["timer_context_size"] = 0
	}
	c.v8WorkerMessagesProcessed["timer_context_size"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	logging.Infof("%s [%s:%s:%d] Sending timer context size: %d",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), timerContextSize)

	c.sendMessage(m)
}

func (c *Consumer) sendWorkerThrCount(thrCount int, sendToDebugger bool) {
	var header []byte
	var hBuilder *flatbuffers.Builder
	if sendToDebugger {
		header, hBuilder = c.makeThrCountHeader(strconv.Itoa(thrCount))
	} else {
		header, hBuilder = c.makeThrCountHeader(strconv.Itoa(c.cppWorkerThrCount))
	}

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["thr_count"]; !ok {
		c.v8WorkerMessagesProcessed["thr_count"] = 0
	}
	c.v8WorkerMessagesProcessed["thr_count"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) sendWorkerThrMap(thrPartitionMap map[int][]uint16, sendToDebugger bool) {
	header, hBuilder := c.makeThrMapHeader()

	var payload []byte
	var pBuilder *flatbuffers.Builder
	if sendToDebugger {
		payload, pBuilder = c.makeThrMapPayload(thrPartitionMap, cppWorkerPartitionCount)
	} else {
		payload, pBuilder = c.makeThrMapPayload(c.cppThrPartitionMap, cppWorkerPartitionCount)
	}

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["thr_map"]; !ok {
		c.v8WorkerMessagesProcessed["thr_map"] = 0
	}
	c.v8WorkerMessagesProcessed["thr_map"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header:  header,
			Payload: payload,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
		headerBuilder:  hBuilder,
		payloadBuilder: pBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) sendDebuggerStart() {

	header, hBuilder := c.makeV8DebuggerStartHeader()

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["debug_start"]; !ok {
		c.v8WorkerMessagesProcessed["debug_start"] = 0
	}
	c.v8WorkerMessagesProcessed["debug_start"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: true,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) sendDebuggerStop() {

	header, hBuilder := c.makeV8DebuggerStopHeader()

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["debug_stop"]; !ok {
		c.v8WorkerMessagesProcessed["debug_stop"] = 0
	}
	c.v8WorkerMessagesProcessed["debug_stop"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: true,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) sendInitV8Worker(payload []byte, sendToDebugger bool, pBuilder *flatbuffers.Builder) {

	header, hBuilder := c.makeV8InitOpcodeHeader()

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["v8_init"]; !ok {
		c.v8WorkerMessagesProcessed["v8_init"] = 0
	}
	c.v8WorkerMessagesProcessed["v8_init"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header:  header,
			Payload: payload,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
		headerBuilder:  hBuilder,
		payloadBuilder: pBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) sendCompileRequest(appCode string) {
	header, hBuilder := c.makeV8CompileOpcodeHeader(appCode)

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["v8_compile"]; !ok {
		c.v8WorkerMessagesProcessed["v8_compile"] = 0
	}
	c.v8WorkerMessagesProcessed["v8_compile"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: false,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) sendLoadV8Worker(appCode string, sendToDebugger bool) {

	header, hBuilder := c.makeV8LoadOpcodeHeader(appCode)

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["v8_load"]; !ok {
		c.v8WorkerMessagesProcessed["v8_load"] = 0
	}
	c.v8WorkerMessagesProcessed["v8_load"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) sendGetLatencyStats(sendToDebugger bool) {
	header, hBuilder := c.makeHeader(v8WorkerEvent, v8WorkerLatencyStats, 0, "")

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["latency_stats"]; !ok {
		c.v8WorkerMessagesProcessed["latency_stats"] = 0
	}
	c.v8WorkerMessagesProcessed["latency_stats"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) refershCurlLatencyStats(sendToDebugger bool) {
	header, hBuilder := c.makeHeader(v8WorkerEvent, v8WorkerCurlLatencyStats, 0, "")

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["curl_latency_stats"]; !ok {
		c.v8WorkerMessagesProcessed["curl_latency_stats"] = 0
	}
	c.v8WorkerMessagesProcessed["curl_latency_stats"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) sendGetFailureStats(sendToDebugger bool) {
	header, hBuilder := c.makeHeader(v8WorkerEvent, v8WorkerFailureStats, 0, "")

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["failure_stats"]; !ok {
		c.v8WorkerMessagesProcessed["failure_stats"] = 0
	}
	c.v8WorkerMessagesProcessed["failure_stats"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) sendGetExecutionStats(sendToDebugger bool) {
	header, hBuilder := c.makeHeader(v8WorkerEvent, v8WorkerExecutionStats, 0, "")

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["execution_stats"]; !ok {
		c.v8WorkerMessagesProcessed["execution_stats"] = 0
	}
	c.v8WorkerMessagesProcessed["execution_stats"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) sendGetLcbExceptionStats(sendToDebugger bool) {
	header, hBuilder := c.makeHeader(v8WorkerEvent, v8WorkerLcbExceptions, 0, "")

	c.msgProcessedRWMutex.Lock()
	if _, ok := c.v8WorkerMessagesProcessed["lcb_exception_stats"]; !ok {
		c.v8WorkerMessagesProcessed["lcb_exception_stats"] = 0
	}
	c.v8WorkerMessagesProcessed["lcb_exception_stats"]++
	c.msgProcessedRWMutex.Unlock()

	m := &msgToTransmit{
		msg: &message{
			Header: header,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) sendTimerEvent(e *timerContext, sendToDebugger bool) {
	cppPartition := util.VbucketByKey([]byte(e.reference), cppWorkerPartitionCount)
	timerHeader, hBuilder := c.makeTimerEventHeader(int16(cppPartition))
	timerPayload, pBuilder := c.makeTimerPayload(e)

	m := &msgToTransmit{
		msg: &message{
			Header:  timerHeader,
			Payload: timerPayload,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     false,
		headerBuilder:  hBuilder,
		payloadBuilder: pBuilder,
	}

	c.sendMessage(m)
}

func (c *Consumer) sendDcpEvent(e *memcached.DcpEvent, sendToDebugger bool) {
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
		logging.Errorf("CRHM[%s:%s:%s:%d] key: %ru failed to marshal metadata",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), string(e.Key))
		return
	}

	partition := int16(util.VbucketByKey(e.Key, cppWorkerPartitionCount))

	var dcpHeader []byte
	var hBuilder *flatbuffers.Builder
	if e.Opcode == mcd.DCP_MUTATION {
		dcpHeader, hBuilder = c.makeDcpMutationHeader(partition, string(metadata))
	}

	if e.Opcode == mcd.DCP_DELETION {
		dcpHeader, hBuilder = c.makeDcpDeletionHeader(partition, string(metadata))
	}

	dcpPayload, pBuilder := c.makeDcpPayload(e.Key, e.Value)

	msg := &msgToTransmit{
		msg: &message{
			Header:  dcpHeader,
			Payload: dcpPayload,
		},
		sendToDebugger: sendToDebugger,
		prioritize:     false,
		headerBuilder:  hBuilder,
		payloadBuilder: pBuilder,
	}

	c.sendMessage(msg)
}

func (c *Consumer) sendVbFilterData(vb uint16, seqNo uint64, skipAck bool) {
	logPrefix := "Consumer::sendVbFilterData"

	data := vbSeqNo{
		SeqNo:   seqNo,
		Vbucket: vb,
	}

	if skipAck {
		data.SkipAck = 1
	}

	metadata, err := json.Marshal(&data)
	if err != nil {
		logging.Errorf("[%s:%s:%s:%d] Failed to marshal metadata",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid())
		return
	}

	filterHeader, hBuilder := c.makeVbFilterHeader(int16(vb), string(metadata))

	msg := &msgToTransmit{
		msg: &message{
			Header: filterHeader,
		},
		sendToDebugger: false,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	c.sendMessage(msg)
	logging.Infof("%s [%s:%s:%d] vb: %d seqNo: %d sending filter data to C++",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, seqNo)
}

func (c *Consumer) sendUpdateProcessedSeqNo(vb uint16, seqNo uint64) {
	logPrefix := "Consumer::sendUpdateProcessedSeqNo"

	data := vbSeqNo{
		SeqNo:   seqNo,
		Vbucket: vb,
	}

	metadata, err := json.Marshal(&data)
	if err != nil {
		logging.Errorf("[%s:%s:%s:%d] vb: %d failed to marshal ",
			c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vb)
		return
	}

	updateSeqNoHeader, hBuilder := c.makeProcessedSeqNoHeader(int16(vb), string(metadata))

	msg := &msgToTransmit{
		msg: &message{
			Header: updateSeqNoHeader,
		},
		sendToDebugger: false,
		prioritize:     true,
		headerBuilder:  hBuilder,
	}

	c.sendMessage(msg)
	logging.Infof("%s [%s:%s:%d] vb: %d seqNo: %d sending update seqno data to C++",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, seqNo)
}

func (c *Consumer) sendMessageLoop() {
	logPrefix := "Consumer::sendMessageLoop"

	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("%s [%s:%s:%d] sendMessageLoop recover, %rm stack trace: %rm",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
		}
	}()

	// Flush any entry in stop channel. Entry could have come in as part of bootstrap
	select {
	case <-c.socketWriteLoopStopCh:
	default:
	}
	c.socketWriteLoopStopAckCh = make(chan struct{}, 1)

	for {
		select {
		case <-c.socketWriteTicker.C:
			if c.sendMsgCounter > 0 && c.conn != nil {
				if atomic.LoadUint32(&c.isTerminateRunning) == 1 || c.stoppingConsumer {
					c.socketWriteLoopStopAckCh <- struct{}{}
					return
				}

				c.conn.SetWriteDeadline(time.Now().Add(c.socketTimeout))

				func() {
					c.sendMsgBufferRWMutex.Lock()
					defer c.sendMsgBufferRWMutex.Unlock()

					if c.conn == nil {
						logging.Infof("%s [%s:%s:%d] stoppingConsumer: %t connection socket closed, bailing out",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), c.stoppingConsumer)
						c.socketWriteLoopStopAckCh <- struct{}{}
						return
					}

					_, err := c.sendMsgBuffer.WriteTo(c.conn)
					if err != nil {
						logging.Errorf("%s [%s:%s:%d] stoppingConsumer: %t write to downstream socket failed, err: %v",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), c.stoppingConsumer, err)

						if atomic.LoadUint32(&c.isTerminateRunning) == 1 || c.stoppingConsumer {
							c.socketWriteLoopStopAckCh <- struct{}{}
							return
						}

						c.stoppingConsumer = true
						c.producer.KillAndRespawnEventingConsumer(c)
					}

					// Reset the sendMessage buffer and message counter
					c.sendMsgBuffer.Reset()
					c.aggMessagesSentCounter += c.sendMsgCounter
					c.sendMsgCounter = 0
				}()
			}
		case <-c.socketWriteLoopStopCh:
			logging.Infof("%s [%s:%s:%d] Exiting send message routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			c.socketWriteLoopStopAckCh <- struct{}{}
			return
		}
	}
}

func (c *Consumer) sendMessage(m *msgToTransmit) error {
	logPrefix := "Consumer::sendMessage"

	defer func() {
		if m.headerBuilder != nil {
			c.putBuilder(m.headerBuilder)
		}
		if m.payloadBuilder != nil {
			c.putBuilder(m.payloadBuilder)
		}
	}()

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 || c.stoppingConsumer {
		return fmt.Errorf("Eventing.Consumer instance is terminating")
	}

	// Protocol encoding format:
	//<headerSize><payloadSize><Header><Payload>

	c.sendMsgBufferRWMutex.Lock()
	defer c.sendMsgBufferRWMutex.Unlock()
	err := binary.Write(&c.sendMsgBuffer, binary.LittleEndian, uint32(len(m.msg.Header)))
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failure while writing header size, err : %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
		return err
	}

	err = binary.Write(&c.sendMsgBuffer, binary.LittleEndian, uint32(len(m.msg.Payload)))
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failure while writing payload size, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
		return err
	}

	err = binary.Write(&c.sendMsgBuffer, binary.LittleEndian, m.msg.Header)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failure while writing encoded header, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
		return err
	}

	err = binary.Write(&c.sendMsgBuffer, binary.LittleEndian, m.msg.Payload)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failure while writing encoded payload, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
		return err
	}

	c.sendMsgCounter++

	if c.sendMsgCounter >= uint64(c.socketWriteBatchSize) || m.prioritize || m.sendToDebugger {
		c.connMutex.Lock()
		defer c.connMutex.Unlock()

		if !m.sendToDebugger && c.conn != nil {
			c.conn.SetWriteDeadline(time.Now().Add(c.socketTimeout))

			_, err := c.sendMsgBuffer.WriteTo(c.conn)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] stoppingConsumer: %t write to downstream socket failed, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), c.stoppingConsumer, err)

				if c.stoppingConsumer {
					return fmt.Errorf("consumer is already getting respawned")
				}

				c.stoppingConsumer = true
				c.producer.KillAndRespawnEventingConsumer(c)
				return err
			}
		} else if c.debugConn != nil {
			_, err := c.sendMsgBuffer.WriteTo(c.debugConn)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Write to debug enabled worker socket failed, err: %v",
					logPrefix, c.workerName, c.debugTCPPort, c.Pid(), err)
				c.debugConn.Close()
				return err
			}
		}

		// Reset the sendMessage buffer and message counter
		c.aggMessagesSentCounter += c.sendMsgCounter
		c.sendMsgBuffer.Reset()
		c.sendMsgCounter = 0
	}

	return nil
}

func (c *Consumer) feedbackReadMessageLoop(feedbackReader *bufio.Reader) {
	logPrefix := "Consumer::feedbackReadMessageLoop"

	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("%s [%s:%s:%d] Recover, %rm stack trace: %rm",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
		}
	}()

	for {
		if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
			return
		}

		buffer := make([]byte, c.feedbackReadBufferSize)
		bytesRead, err := feedbackReader.Read(buffer)

		if err == io.EOF || bytesRead == 0 {
			break
		}

		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Read from client socket failed, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
			return
		}

		c.adhocTimerResponsesRecieved++

		if bytesRead < len(buffer) {
			buffer = buffer[:bytesRead]
		}

		if bytesRead >= headerFragmentSize || c.feedbackReadMsgBuffer.Len() >= headerFragmentSize {

		parseFeedbackMessage:
			if c.feedbackReadMsgBuffer.Len() > 0 {
				buffer = append(c.feedbackReadMsgBuffer.Bytes(), buffer...)
				c.feedbackReadMsgBuffer.Reset()
			}

			headerSize := binary.LittleEndian.Uint32(buffer[:headerFragmentSize])

			if len(buffer) >= int(headerFragmentSize+headerSize) {

				c.parseWorkerResponse(buffer[headerFragmentSize : headerFragmentSize+headerSize])
				buffer = buffer[headerFragmentSize+headerSize:]

				c.feedbackReadMsgBuffer.Write(buffer)

				if c.feedbackReadMsgBuffer.Len() > headerFragmentSize {
					buffer = buffer[:0]
					goto parseFeedbackMessage
				}
			} else {
				c.feedbackReadMsgBuffer.Write(buffer)
				buffer = buffer[:0]
			}

		} else {
			c.feedbackReadMsgBuffer.Write(buffer)
			buffer = buffer[:0]
		}
	}
}

func (c *Consumer) readMessageLoop() {
	logPrefix := "Consumer::readMessageLoop"

	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("%s [%s:%s:%d] readMessageLoop recover, %rm stack trace: %rm",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
		}
	}()

	for {
		buffer := make([]byte, 4096)
		bytesRead, err := c.sockReader.Read(buffer)

		if err == io.EOF || bytesRead == 0 {
			break
		}

		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Read from client socket failed, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
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
