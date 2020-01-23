package consumer

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/gen/flatbuf/header"
	"github.com/couchbase/eventing/gen/flatbuf/payload"
	"github.com/couchbase/eventing/gen/flatbuf/response"
	"github.com/couchbase/eventing/logging"
	flatbuffers "github.com/google/flatbuffers/go"
)

const (
	eventType int8 = iota
	dcpEvent
	v8WorkerEvent
	appWorkerSetting
	timerEvent
	debuggerEvent
	filterEvent
	reservedEvent
	pauseConsumer
)

const (
	debuggerOpcode int8 = iota
	startDebug
	stopDebug
)

const (
	timerOpcode int8 = iota
	timer
)

const (
	filterOpcode int8 = iota
	vbFilter
	processedSeqNo
)

const (
	v8WorkerOpcode int8 = iota
	v8WorkerDispose
	v8WorkerInit
	v8WorkerLoad
	v8WorkerTerminate
	v8WorkerUnused1
	v8WorkerUnused2
	v8WorkerLatencyStats
	v8WorkerFailureStats
	v8WorkerExecutionStats
	v8WorkerCompile
	v8WorkerLcbExceptions
	v8WorkerCurlLatencyStats
	v8WorkerInsight
)

const (
	dcpOpcode int8 = iota
	dcpDeletion
	dcpMutation
)

const (
	appWorkerSettingsOpcode int8 = iota
	logLevel
	workerThreadCount
	workerThreadPartitionMap
	timerContextSize
	vbMap
	workerThreadMemQuota
)

// message and opcode types for interpreting messages from C++ To Go
const (
	respMsgType int8 = iota
	respV8WorkerConfig
	docTimerResponse
	bucketOpsResponse
	bucketOpsFilterAck
	pauseAck
)

const (
	respV8WorkerConfigOpcode int8 = iota
	Unused3
	Unused4
	appLogMessage
	sysLogMessage
	latencyStats
	failureStats
	executionStats
	compileInfo
	queueSize
	lcbExceptions
	curlLatencyStats
	insight
)

const (
	docTimerResponseOpcode int8 = iota
)

const (
	bucketOpsResponseOpcode int8 = iota
)

const (
	bucketOpsFilterAckOpCode int8 = iota
)

type message struct {
	Header  []byte
	Payload []byte
}

func (c *Consumer) makeTimerEventHeader(partition int16) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(timerEvent, timer, partition, "")
}

func (c *Consumer) makeDcpMutationHeader(partition int16, mutationMeta string) ([]byte, *flatbuffers.Builder) {
	return c.makeDcpHeader(dcpMutation, partition, mutationMeta)
}

func (c *Consumer) makeDcpDeletionHeader(partition int16, deletionMeta string) ([]byte, *flatbuffers.Builder) {
	return c.makeDcpHeader(dcpDeletion, partition, deletionMeta)
}

func (c *Consumer) makeDcpHeader(opcode int8, partition int16, meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(dcpEvent, opcode, partition, meta)
}

func (c *Consumer) filterEventHeader(opcode int8, partition int16, meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(filterEvent, opcode, partition, meta)
}

func (c *Consumer) makeVbFilterHeader(partition int16, meta string) ([]byte, *flatbuffers.Builder) {
	return c.filterEventHeader(vbFilter, partition, meta)
}

func (c *Consumer) makePauseConsumerHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(pauseConsumer, 0, 0, "")
}
func (c *Consumer) makeProcessedSeqNoHeader(partition int16, meta string) ([]byte, *flatbuffers.Builder) {
	return c.filterEventHeader(processedSeqNo, partition, meta)
}

func (c *Consumer) makeV8DebuggerStartHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeV8DebuggerHeader(startDebug, "")
}

func (c *Consumer) makeV8DebuggerStopHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeV8DebuggerHeader(stopDebug, "")
}

func (c *Consumer) makeV8DebuggerHeader(opcode int8, meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(debuggerEvent, opcode, 0, meta)
}

func (c *Consumer) makeV8InitOpcodeHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeV8EventHeader(v8WorkerInit, "")
}

func (c *Consumer) makeV8CompileOpcodeHeader(appCode string) ([]byte, *flatbuffers.Builder) {
	return c.makeV8EventHeader(v8WorkerCompile, appCode)
}

func (c *Consumer) makeV8LoadOpcodeHeader(appCode string) ([]byte, *flatbuffers.Builder) {
	return c.makeV8EventHeader(v8WorkerLoad, appCode)
}

func (c *Consumer) makeV8EventHeader(opcode int8, meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(v8WorkerEvent, opcode, 0, meta)
}

func (c *Consumer) makeLogLevelHeader(meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(appWorkerSetting, logLevel, 0, meta)
}

func (c *Consumer) makeTimerContextSizeHeader(meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(appWorkerSetting, timerContextSize, 0, meta)
}

func (c *Consumer) makeThrCountHeader(meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(appWorkerSetting, workerThreadCount, 0, meta)
}

func (c *Consumer) makeThrMapHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(appWorkerSetting, workerThreadPartitionMap, 0, "")
}

func (c *Consumer) makeVbMapHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(appWorkerSetting, vbMap, 0, "")
}

func (c *Consumer) makeHeader(event int8, opcode int8, partition int16, meta string) (encodedHeader []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()

	metadata := builder.CreateString(meta)

	header.HeaderStart(builder)

	header.HeaderAddEvent(builder, event)
	header.HeaderAddOpcode(builder, opcode)
	header.HeaderAddPartition(builder, partition)
	header.HeaderAddMetadata(builder, metadata)

	headerPos := header.HeaderEnd(builder)
	builder.Finish(headerPos)

	encodedHeader = builder.FinishedBytes()
	return
}

func (c *Consumer) createHandlerHeaders(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	for i := len(c.handlerHeaders) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(builder.CreateString(c.handlerHeaders[i]))
	}

	payload.PayloadStartHandlerHeadersVector(builder, len(c.handlerHeaders))
	return builder.EndVector(len(c.handlerHeaders))
}

func (c *Consumer) createHandlerFooters(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	for i := len(c.handlerFooters) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(builder.CreateString(c.handlerFooters[i]))
	}

	payload.PayloadStartHandlerFootersVector(builder, len(c.handlerFooters))
	return builder.EndVector(len(c.handlerFooters))
}

func (c *Consumer) makeThrMapPayload(thrMap map[int][]uint16, partitionCount int) (encodedPayload []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()

	tMaps := make([]flatbuffers.UOffsetT, 0)

	for i := 0; i < len(thrMap); i++ {
		payload.VbsThreadMapStartPartitionsVector(builder, len(thrMap[i]))

		for j := 0; j < len(thrMap[i]); j++ {
			builder.PrependUint16(thrMap[i][j])
		}
		partitions := builder.EndVector(len(thrMap[i]))

		payload.VbsThreadMapStart(builder)
		payload.VbsThreadMapAddPartitions(builder, partitions)
		payload.VbsThreadMapAddThreadID(builder, int16(i))

		tMaps = append(tMaps, payload.VbsThreadMapEnd(builder))
	}

	payload.PayloadStartThrMapVector(builder, len(tMaps))
	for i := len(tMaps) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(tMaps[i])
	}

	resMap := builder.EndVector(len(tMaps))

	payload.PayloadStart(builder)
	payload.PayloadAddThrMap(builder, resMap)
	payload.PayloadAddPartitionCount(builder, int16(partitionCount))
	payloadPos := payload.PayloadEnd(builder)
	builder.Finish(payloadPos)

	encodedPayload = builder.FinishedBytes()
	return
}

func (c *Consumer) makeVbMapPayload(assgnedVbs []uint16) (encodedPayload []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()
	payload.PayloadStartVbMapVector(builder, len(assgnedVbs))
	for i := len(assgnedVbs) - 1; i >= 0; i-- {
		builder.PrependUint16(assgnedVbs[i])
	}
	pos := builder.EndVector(len(assgnedVbs))
	payload.PayloadStart(builder)
	payload.PayloadAddVbMap(builder, pos)
	payloadPos := payload.PayloadEnd(builder)
	builder.Finish(payloadPos)
	encodedPayload = builder.FinishedBytes()
	return
}

func (c *Consumer) makeDcpPayload(key, value []byte) (encodedPayload []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()

	keyPos := builder.CreateByteString(key)
	valPos := builder.CreateByteString(value)

	payload.PayloadStart(builder)

	payload.PayloadAddKey(builder, keyPos)
	payload.PayloadAddValue(builder, valPos)

	payloadPos := payload.PayloadEnd(builder)
	builder.Finish(payloadPos)

	encodedPayload = builder.FinishedBytes()
	return
}

func (c *Consumer) makeV8InitPayload(appName, debuggerPort, currHost, eventingDir, eventingPort,
	eventingSSLPort, depCfg string, capacity, executionTimeout, checkpointInterval int,
	skipLcbBootstrap bool, timerContextSize int64) (encodedPayload []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()

	app := builder.CreateString(appName)
	dp := builder.CreateString(debuggerPort)
	ch := builder.CreateString(currHost)
	ed := builder.CreateString(eventingDir)
	ep := builder.CreateString(eventingPort)
	esp := builder.CreateString(eventingSSLPort)
	dcfg := builder.CreateString(depCfg)
	fiid := builder.CreateString(c.app.FunctionInstanceID)
	handlerHeaders := c.createHandlerHeaders(builder)
	handlerFooters := c.createHandlerFooters(builder)
	n1qlConsistency := builder.CreateString(c.n1qlConsistency)
	languageCompatibility := builder.CreateString(c.languageCompatibility)

	lcb := make([]byte, 1)
	flatbuffers.WriteBool(lcb, skipLcbBootstrap)

	usingTimer := make([]byte, 1)
	flatbuffers.WriteBool(usingTimer, c.usingTimer)

	payload.PayloadStart(builder)

	payload.PayloadAddAppName(builder, app)
	payload.PayloadAddDebuggerPort(builder, dp)
	payload.PayloadAddCurrHost(builder, ch)
	payload.PayloadAddEventingDir(builder, ed)
	payload.PayloadAddCurrEventingPort(builder, ep)
	payload.PayloadAddCurrEventingSslport(builder, esp)
	payload.PayloadAddDepcfg(builder, dcfg)
	payload.PayloadAddLcbInstCapacity(builder, int32(capacity))
	payload.PayloadAddLanguageCompatibility(builder, languageCompatibility)
	payload.PayloadAddExecutionTimeout(builder, int32(executionTimeout))
	payload.PayloadAddCheckpointInterval(builder, int32(checkpointInterval))
	payload.PayloadAddTimerContextSize(builder, timerContextSize)
	payload.PayloadAddFunctionInstanceId(builder, fiid)
	payload.PayloadAddSkipLcbBootstrap(builder, lcb[0])
	payload.PayloadAddUsingTimer(builder, usingTimer[0])
	payload.PayloadAddHandlerHeaders(builder, handlerHeaders)
	payload.PayloadAddHandlerFooters(builder, handlerFooters)
	payload.PayloadAddN1qlConsistency(builder, n1qlConsistency)
	payload.PayloadAddLcbRetryCount(builder, int32(c.lcbRetryCount))
	msgPos := payload.PayloadEnd(builder)
	builder.Finish(msgPos)

	encodedPayload = builder.FinishedBytes()
	return
}

func readHeader(buf []byte) int8 {
	headerPos := header.GetRootAsHeader(buf, 0)

	event := headerPos.Event()
	opcode := headerPos.Opcode()
	metadata := string(headerPos.Metadata())

	logging.Infof(" ReadHeader => event: %d opcode: %d meta: %ru\n",
		event, opcode, metadata)
	return event
}

func readPayload(buf []byte) {
	payloadPos := payload.GetRootAsPayload(buf, 0)

	key := string(payloadPos.Key())
	val := string(payloadPos.Value())

	logging.Infof("ReadPayload => key: %ru val: %ru\n", key, val)
}

func (c *Consumer) parseWorkerResponse(msg []byte) {
	r := response.GetRootAsResponse(msg, 0)

	msgType := r.MsgType()
	opcode := r.Opcode()
	message := string(r.Msg())

	c.routeResponse(msgType, opcode, message)
}

func (c *Consumer) routeResponse(msgType, opcode int8, msg string) {
	logPrefix := "Consumer::routeResponse"

	switch msgType {
	case respV8WorkerConfig:
		switch opcode {

		case latencyStats:
			c.workerRespMainLoopTs.Store(time.Now())

			deltas := make(common.StatsData)
			err := json.Unmarshal([]byte(msg), &deltas)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal latency stats, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
			c.producer.AppendLatencyStats(deltas)

		case curlLatencyStats:
			c.workerRespMainLoopTs.Store(time.Now())

			deltas := make(common.StatsData)
			err := json.Unmarshal([]byte(msg), &deltas)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal curl latency stats, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
			c.producer.AppendCurlLatencyStats(deltas)

		case insight:
			c.workerRespMainLoopTs.Store(time.Now())
			logging.Debugf("%s [%s:%s:%d] Received insight: %v", logPrefix, c.workerName, c.tcpPort, c.Pid(), msg)
			insight := common.NewInsight()
			err := json.Unmarshal([]byte(msg), insight)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal insight data, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
			c.insight <- insight

		case failureStats:
			c.workerRespMainLoopTs.Store(time.Now())

			c.statsRWMutex.Lock()
			defer c.statsRWMutex.Unlock()
			err := json.Unmarshal([]byte(msg), &c.failureStats)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal failure stats, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		case executionStats:
			c.workerRespMainLoopTs.Store(time.Now())

			c.statsRWMutex.Lock()
			defer c.statsRWMutex.Unlock()
			err := json.Unmarshal([]byte(msg), &c.executionStats)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal execution stats, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			} else {
				if val, ok := c.executionStats["timer_create_counter"]; ok {
					c.timerResponsesRecieved = uint64(val.(float64))
				}
				if val, ok := c.executionStats["timer_msg_counter"]; ok {
					c.timerMessagesProcessed = uint64(val.(float64))
				}
			}
		case compileInfo:
			err := json.Unmarshal([]byte(msg), &c.compileInfo)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal compilation stats, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		case queueSize:
			c.workerRespMainLoopTs.Store(time.Now())

			err := json.Unmarshal([]byte(msg), &c.cppQueueSizes)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal cpp queue sizes, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		case lcbExceptions:
			c.workerRespMainLoopTs.Store(time.Now())

			c.statsRWMutex.Lock()
			defer c.statsRWMutex.Unlock()
			err := json.Unmarshal([]byte(msg), &c.lcbExceptionStats)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal lcb exception stats, msg: %v err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		}

	case bucketOpsResponse:
		data := strings.Split(msg, "::")
		if len(data) != 2 {
			logging.Errorf("%s [%s:%s:%d] Invalid bucket ops message received: %s",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), msg)
			return
		}

		vbStr, seqNoStr := data[0], data[1]
		vb, err := strconv.ParseUint(vbStr, 10, 16)
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to convert vbStr: %s to uint64, msg: %s err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vbStr, msg, err)
			return
		}
		seqNo, err := strconv.ParseUint(seqNoStr, 10, 64)
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to convert seqNoStr: %s to int64, msg: %s err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), seqNoStr, msg, err)
			return
		}
		prevSeqNo := c.vbProcessingStats.getVbStat(uint16(vb), "last_processed_seq_no").(uint64)
		if seqNo > prevSeqNo {
			c.vbProcessingStats.updateVbStat(uint16(vb), "last_processed_seq_no", seqNo)
			logging.Tracef("%s [%s:%s:%d] vb: %d Updating last_processed_seq_no to seqNo: %d",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, seqNo)
		}
	case bucketOpsFilterAck:
		var ack vbSeqNo
		err := json.Unmarshal([]byte(msg), &ack)
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to unmarshal filter ack, msg: %v err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			return
		}

		logging.Infof("%s [%s:%s:%d] vb: %d seqNo: %d skip_ack: %d received filter ack from C++",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), ack.Vbucket, ack.SeqNo, ack.SkipAck)

		if ack.SkipAck == 0 {
			c.filterDataCh <- &ack
		}

	case pauseAck:
		var acks []vbSeqNo
		if err := json.Unmarshal([]byte(msg), &acks); err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to unmarshal pause ack, msg: %v err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), msg, err)
			return
		}

		for _, ack := range acks {
			c.filterDataCh <- &ack
		}
	default:
		logging.Infof("%s [%s:%s:%d] Unknown message %s",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), msg)
	}
}
