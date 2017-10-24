package consumer

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"

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
	nonDocTimer
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
	logMessage
	latencyStats
)

type message struct {
	Header  []byte
	Payload []byte
}

func makeDocTimerEventHeader(partition int16) []byte {
	return makeHeader(timerEvent, docTimer, partition, "")
}

func makeNonDocTimerEventHeader(partition int16) []byte {
	return makeHeader(timerEvent, nonDocTimer, partition, "")
}

func makeDcpMutationHeader(partition int16, mutationMeta string) []byte {
	return makeDcpHeader(dcpMutation, partition, mutationMeta)
}

func makeDcpDeletionHeader(partition int16, deletionMeta string) []byte {
	return makeDcpHeader(dcpDeletion, partition, deletionMeta)
}

func makeDcpHeader(opcode int8, partition int16, meta string) []byte {
	return makeHeader(dcpEvent, opcode, partition, meta)
}

func makeV8DebuggerStartHeader() []byte {
	return makeV8DebuggerHeader(startDebug, "")
}

func makeV8DebuggerStopHeader() []byte {
	return makeV8DebuggerHeader(stopDebug, "")
}

func makeV8DebuggerHeader(opcode int8, meta string) []byte {
	return makeHeader(debuggerEvent, opcode, 0, meta)
}

func makeV8InitOpcodeHeader() []byte {
	return makeV8EventHeader(v8WorkerInit, "")
}

func makeV8LoadOpcodeHeader(appCode string) []byte {
	return makeV8EventHeader(v8WorkerLoad, appCode)
}

func makeV8EventHeader(opcode int8, meta string) []byte {
	return makeHeader(v8WorkerEvent, opcode, 0, meta)
}

func makeLogLevelHeader(meta string) []byte {
	return makeHeader(appWorkerSetting, logLevel, 0, meta)
}

func makeThrCountHeader(meta string) []byte {
	return makeHeader(appWorkerSetting, workerThreadCount, 0, meta)
}

func makeThrMapHeader() []byte {
	return makeHeader(appWorkerSetting, workerThreadPartitionMap, 0, "")
}

func makeHeader(event int8, opcode int8, partition int16, meta string) (encodedHeader []byte) {
	logging.Infof("makeHeader event: %v opcode: %v", event, opcode)

	builder := flatbuffers.NewBuilder(0)
	builder.Reset()
	metadata := builder.CreateString(meta)

	header.HeaderStart(builder)

	header.HeaderAddEvent(builder, event)
	header.HeaderAddOpcode(builder, opcode)
	header.HeaderAddPartition(builder, partition)
	header.HeaderAddMetadata(builder, metadata)

	headerPos := header.HeaderEnd(builder)
	builder.Finish(headerPos)
	encodedHeader = builder.Bytes[builder.Head():]
	return builder.Bytes[builder.Head():]
}

func makeThrMapPayload(thrMap map[int][]uint16, partitionCount int) []byte {
	builder := flatbuffers.NewBuilder(0)
	builder.Reset()

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

	return builder.Bytes[builder.Head():]
}

func makeDocTimerPayload(docID, callbackFn string) []byte {
	builder := flatbuffers.NewBuilder(0)
	builder.Reset()
	docIDPos := builder.CreateString(docID)
	callbackFnPos := builder.CreateString(callbackFn)

	payload.PayloadStart(builder)

	payload.PayloadAddDocId(builder, docIDPos)
	payload.PayloadAddCallbackFn(builder, callbackFnPos)

	payloadPos := payload.PayloadEnd(builder)
	builder.Finish(payloadPos)
	return builder.Bytes[builder.Head():]
}

func makeNonDocTimerPayload(data string) []byte {
	builder := flatbuffers.NewBuilder(0)
	builder.Reset()
	pPos := builder.CreateString(data)

	payload.PayloadStart(builder)

	payload.PayloadAddDocIdsCallbackFns(builder, pPos)

	payloadPos := payload.PayloadEnd(builder)
	builder.Finish(payloadPos)
	return builder.Bytes[builder.Head():]
}

func makeDcpPayload(key, value []byte) []byte {
	builder := flatbuffers.NewBuilder(0)
	builder.Reset()
	keyPos := builder.CreateByteString(key)
	valPos := builder.CreateByteString(value)

	payload.PayloadStart(builder)

	payload.PayloadAddKey(builder, keyPos)
	payload.PayloadAddValue(builder, valPos)

	payloadPos := payload.PayloadEnd(builder)
	builder.Finish(payloadPos)
	return builder.Bytes[builder.Head():]
}

func makeV8InitPayload(appName, currHost, eventingDir, eventingPort, kvHostPort, depCfg, rbacUser, rbacPass string,
	capacity, executionTimeout int, enableRecursiveMutation bool) []byte {
	builder := flatbuffers.NewBuilder(0)
	builder.Reset()

	app := builder.CreateString(appName)
	ch := builder.CreateString(currHost)
	ed := builder.CreateString(eventingDir)
	ep := builder.CreateString(eventingPort)
	dcfg := builder.CreateString(depCfg)
	khp := builder.CreateString(kvHostPort)
	rUser := builder.CreateString(rbacUser)
	rPass := builder.CreateString(rbacPass)

	buf := make([]byte, 1)
	flatbuffers.WriteBool(buf, enableRecursiveMutation)

	payload.PayloadStart(builder)

	payload.PayloadAddAppName(builder, app)
	payload.PayloadAddCurrHost(builder, ch)
	payload.PayloadAddEventingDir(builder, ed)
	payload.PayloadAddCurrEventingPort(builder, ep)
	payload.PayloadAddDepcfg(builder, dcfg)
	payload.PayloadAddKvHostPort(builder, khp)
	payload.PayloadAddRbacUser(builder, rUser)
	payload.PayloadAddRbacPass(builder, rPass)
	payload.PayloadAddLcbInstCapacity(builder, int32(capacity))
	payload.PayloadAddExecutionTimeout(builder, int32(executionTimeout))
	payload.PayloadAddEnableRecursiveMutation(builder, buf[0])

	msgPos := payload.PayloadEnd(builder)
	builder.Finish(msgPos)
	return builder.Bytes[builder.Head():]
}

func readHeader(buf []byte) int8 {
	headerPos := header.GetRootAsHeader(buf, 0)

	event := headerPos.Event()
	opcode := headerPos.Opcode()
	metadata := string(headerPos.Metadata())

	log.Printf(" ReadHeader => event: %d opcode: %d meta: %s\n",
		event, opcode, metadata)
	return event
}

func readPayload(buf []byte) {
	payloadPos := payload.GetRootAsPayload(buf, 0)

	key := string(payloadPos.Key())
	val := string(payloadPos.Value())

	log.Printf("ReadPayload => key: %s val: %s\n", key, val)
}

func (c *Consumer) parseWorkerResponse(m []byte, start int) {
	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("CRDP[%s:%s:%s:%d] parseWorkerResponse: panic and recover, %v stack trace: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
		}
	}()

	msg := m[start:]

	if len(msg) > headerFragmentSize {
		size := binary.LittleEndian.Uint32(msg[0:headerFragmentSize])

		if len(msg) >= int(headerFragmentSize+size) && size > 0 {

			r := response.GetRootAsResponse(msg[headerFragmentSize:headerFragmentSize+size], 0)

			msgType := r.MsgType()
			opcode := r.Opcode()
			message := string(r.Msg())

			c.routeResponse(msgType, opcode, message)

			if len(msg) > 2*headerFragmentSize+int(size) {
				c.parseWorkerResponse(msg, int(size)+headerFragmentSize)
			}
		}
	}
}

func (c *Consumer) routeResponse(msgType, opcode int8, msg string) {
	switch msgType {
	case respV8WorkerConfig:
		switch opcode {
		case sourceMap:
			c.sourceMap = msg
		case handlerCode:
			c.handlerCode = msg
		case logMessage:
			fmt.Printf("%s", msg)
		case latencyStats:
			err := json.Unmarshal([]byte(msg), &c.latencyStats)
			if err != nil {
				logging.Errorf("CRDP[%s:%s:%s:%d] Failed to unmarshal latency stats, msg: %s err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), msg, err)
			}
		}
	}
}
