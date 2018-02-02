package consumer

import (
	"encoding/json"
	"github.com/couchbase/eventing/gen/flatbuf/header"
	"github.com/couchbase/eventing/gen/flatbuf/payload"
	"github.com/couchbase/eventing/gen/flatbuf/response"
	"github.com/couchbase/eventing/logging"
	"github.com/google/flatbuffers/go"
)

const (
	eventType int8 = iota
	dcpEvent
	httpEvent
	v8WorkerEvent
	appWorkerSetting
	timerEvent
	debuggerEvent
)

const (
	debuggerOpcode int8 = iota
	startDebug
	stopDebug
)

const (
	timerOpcode int8 = iota
	docTimer
	cronTimer
)

const (
	v8WorkerOpcode int8 = iota
	v8WorkerDispose
	v8WorkerInit
	v8WorkerLoad
	v8WorkerTerminate
	v8WorkerSourceMap
	v8WorkerHandlerCode
	v8WorkerLatencyStats
	v8WorkerFailureStats
	v8WorkerExecutionStats
	v8WorkerCompile
	v8WorkerLcbExceptions
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
)

// message and opcode types for interpreting messages from C++ To Go
const (
	respMsgType int8 = iota
	respV8WorkerConfig
)

const (
	respV8WorkerConfigOpcode int8 = iota
	sourceMap
	handlerCode
	appLogMessage
	sysLogMessage
	latencyStats
	failureStats
	executionStats
	compileInfo
	queueSize
	lcbExceptions
)

type message struct {
	Header  []byte
	Payload []byte
}

func (c *Consumer) makeDocTimerEventHeader(partition int16) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(timerEvent, docTimer, partition, "")
}

func (c *Consumer) makeCronTimerEventHeader(partition int16) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(timerEvent, cronTimer, partition, "")
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

func (c *Consumer) makeThrCountHeader(meta string) ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(appWorkerSetting, workerThreadCount, 0, meta)
}

func (c *Consumer) makeThrMapHeader() ([]byte, *flatbuffers.Builder) {
	return c.makeHeader(appWorkerSetting, workerThreadPartitionMap, 0, "")
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

func (c *Consumer) makeDocTimerPayload(e *byTimer) (encodedPayload []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()

	callbackFnPos := builder.CreateString(e.entry.CallbackFn)
	docIDPos := builder.CreateString(e.entry.DocID)
	docIDTsPos := builder.CreateString(e.meta.timestamp)

	payload.PayloadStart(builder)

	payload.PayloadAddCallbackFn(builder, callbackFnPos)
	payload.PayloadAddDocId(builder, docIDPos)
	payload.PayloadAddTimerTs(builder, docIDTsPos)
	payload.PayloadAddTimerPartition(builder, e.meta.partition)

	payloadPos := payload.PayloadEnd(builder)
	builder.Finish(payloadPos)

	encodedPayload = builder.FinishedBytes()
	return
}

func (c *Consumer) makeCronTimerPayload(e *timerMsg) (encodedPayload []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()

	pPos := builder.CreateString(e.payload)
	tPos := builder.CreateString(e.timestamp)

	payload.PayloadStart(builder)

	payload.PayloadAddDocIdsCallbackFns(builder, pPos)
	payload.PayloadAddTimerTs(builder, tPos)
	payload.PayloadAddTimerPartition(builder, e.partition)

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

func (c *Consumer) makeV8InitPayload(appName, currHost, eventingDir, eventingPort, kvHostPort, depCfg string,
	capacity, cronTimerPerDoc, executionTimeout, fuzzOffset, checkpointInterval int, enableRecursiveMutation, skipLcbBootstrap bool,
	curlTimeout int64) (encodedPayload []byte, builder *flatbuffers.Builder) {
	builder = c.getBuilder()

	app := builder.CreateString(appName)
	ch := builder.CreateString(currHost)
	ed := builder.CreateString(eventingDir)
	ep := builder.CreateString(eventingPort)
	dcfg := builder.CreateString(depCfg)
	khp := builder.CreateString(kvHostPort)

	rec := make([]byte, 1)
	flatbuffers.WriteBool(rec, enableRecursiveMutation)

	lcb := make([]byte, 1)
	flatbuffers.WriteBool(lcb, skipLcbBootstrap)

	payload.PayloadStart(builder)

	payload.PayloadAddAppName(builder, app)
	payload.PayloadAddCurrHost(builder, ch)
	payload.PayloadAddEventingDir(builder, ed)
	payload.PayloadAddCurrEventingPort(builder, ep)
	payload.PayloadAddDepcfg(builder, dcfg)
	payload.PayloadAddKvHostPort(builder, khp)
	payload.PayloadAddLcbInstCapacity(builder, int32(capacity))
	payload.PayloadAddCronTimersPerDoc(builder, int32(cronTimerPerDoc))
	payload.PayloadAddExecutionTimeout(builder, int32(executionTimeout))
	payload.PayloadAddFuzzOffset(builder, int32(fuzzOffset))
	payload.PayloadAddCheckpointInterval(builder, int32(checkpointInterval))
	payload.PayloadAddCurlTimeout(builder, curlTimeout)
	payload.PayloadAddEnableRecursiveMutation(builder, rec[0])
	payload.PayloadAddSkipLcbBootstrap(builder, lcb[0])

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

	logging.Infof(" ReadHeader => event: %d opcode: %d meta: %r\n",
		event, opcode, metadata)
	return event
}

func readPayload(buf []byte) {
	payloadPos := payload.GetRootAsPayload(buf, 0)

	key := string(payloadPos.Key())
	val := string(payloadPos.Value())

	logging.Infof("ReadPayload => key: %r val: %r\n", key, val)
}

func (c *Consumer) parseWorkerResponse(msg []byte) {
	r := response.GetRootAsResponse(msg, 0)

	msgType := r.MsgType()
	opcode := r.Opcode()
	message := string(r.Msg())

	c.routeResponse(msgType, opcode, message)
}

func (c *Consumer) routeResponse(msgType, opcode int8, msg string) {

	switch msgType {
	case respV8WorkerConfig:
		switch opcode {
		case sourceMap:
			c.sourceMap = msg
		case handlerCode:
			c.handlerCode = msg
		case latencyStats:
			c.statsRWMutex.Lock()
			defer c.statsRWMutex.Unlock()
			err := json.Unmarshal([]byte(msg), &c.latencyStats)
			if err != nil {
				logging.Errorf("CRDP[%s:%s:%s:%d] Failed to unmarshal latency stats, msg: %r err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		case failureStats:
			c.statsRWMutex.Lock()
			defer c.statsRWMutex.Unlock()
			err := json.Unmarshal([]byte(msg), &c.failureStats)
			if err != nil {
				logging.Errorf("CRDP[%s:%s:%s:%d] Failed to unmarshal failure stats, msg: %r err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		case executionStats:
			c.statsRWMutex.Lock()
			defer c.statsRWMutex.Unlock()
			err := json.Unmarshal([]byte(msg), &c.executionStats)
			if err != nil {
				logging.Errorf("CRDP[%s:%s:%s:%d] Failed to unmarshal execution stats, msg: %r err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		case compileInfo:
			err := json.Unmarshal([]byte(msg), &c.compileInfo)
			if err != nil {
				logging.Errorf("CRDP[%s:%s:%s:%d] Failed to unmarshal compilation stats, msg: %r err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		case queueSize:
			err := json.Unmarshal([]byte(msg), &c.cppWorkerAggQueueSize)
			if err != nil {
				logging.Errorf("CRDP[%s:%s:%s:%d] Failed to unmarshal agg queue size, msg: %r err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		case lcbExceptions:
			c.statsRWMutex.Lock()
			defer c.statsRWMutex.Unlock()
			err := json.Unmarshal([]byte(msg), &c.lcbExceptionStats)
			if err != nil {
				logging.Errorf("CRDP[%s:%s:%s:%d] Failed to unmarshal lcb exception stats, msg: %r err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		}
	}
}
