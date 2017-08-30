package consumer

import (
	"encoding/binary"
	"fmt"
	"log"
	"runtime/debug"

	"github.com/couchbase/eventing/flatbuf/header"
	"github.com/couchbase/eventing/flatbuf/payload"
	"github.com/couchbase/eventing/flatbuf/response"
	"github.com/couchbase/indexing/secondary/logging"
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
)

const (
	dcpOpcode int8 = iota
	dcpDeletion
	dcpMutation
)

const (
	appWorkerSettingsOpcode int8 = iota
	logLevel
)

// message and opcode types for interpreting messages from C++ To Go
const (
	respMsgType int8 = iota
	respV8WorkerConfig
)

const (
	respV8WorkerConfigOpcode int8 = iota
	sourceMap
	logMessage
)

type message struct {
	Header  []byte
	Payload []byte
}

func makeDocTimerEventHeader() []byte {
	return makeHeader(timerEvent, docTimer, "")
}

func makeNonDocTimerEventHeader() []byte {
	return makeHeader(timerEvent, nonDocTimer, "")
}

func makeDcpMutationHeader(mutationMeta string) []byte {
	return makeDcpHeader(dcpMutation, mutationMeta)
}

func makeDcpDeletionHeader(deletionMeta string) []byte {
	return makeDcpHeader(dcpDeletion, deletionMeta)
}

func makeDcpHeader(opcode int8, meta string) []byte {
	return makeHeader(dcpEvent, opcode, meta)
}

func makeV8DebuggerStartHeader() []byte {
	return makeV8DebuggerHeader(startDebug, "")
}

func makeV8DebuggerStopHeader() []byte {
	return makeV8DebuggerHeader(stopDebug, "")
}

func makeV8DebuggerHeader(opcode int8, meta string) []byte {
	return makeHeader(debuggerEvent, opcode, meta)
}

func makeV8InitOpcodeHeader() []byte {
	return makeV8EventHeader(v8WorkerInit, "")
}

func makeV8LoadOpcodeHeader(appCode string) []byte {
	return makeV8EventHeader(v8WorkerLoad, appCode)
}

func makeV8EventHeader(opcode int8, meta string) []byte {
	return makeHeader(v8WorkerEvent, opcode, meta)
}

func makeLogLevelHeader(meta string) []byte {
	return makeHeader(appWorkerSetting, logLevel, meta)
}

func makeHeader(event int8, opcode int8, meta string) (encodedHeader []byte) {
	builder := flatbuffers.NewBuilder(0)
	builder.Reset()
	metadata := builder.CreateString(meta)

	header.HeaderStart(builder)

	header.HeaderAddEvent(builder, event)
	header.HeaderAddOpcode(builder, opcode)
	header.HeaderAddMetadata(builder, metadata)

	headerPos := header.HeaderEnd(builder)
	builder.Finish(headerPos)
	encodedHeader = builder.Bytes[builder.Head():]
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

func makeV8InitPayload(appName, currHostAddr, kvHostPort, depCfg, rbacUser, rbacPass string,
	capacity, executionTimeout int, enableRecursiveMutation bool) []byte {
	builder := flatbuffers.NewBuilder(0)
	builder.Reset()

	app := builder.CreateString(appName)
	chp := builder.CreateString(currHostAddr)
	dcfg := builder.CreateString(depCfg)
	khp := builder.CreateString(kvHostPort)
	rUser := builder.CreateString(rbacUser)
	rPass := builder.CreateString(rbacPass)

	buf := make([]byte, 1)
	flatbuffers.WriteBool(buf, enableRecursiveMutation)

	payload.PayloadStart(builder)

	payload.PayloadAddAppName(builder, app)
	payload.PayloadAddCurrHostPort(builder, chp)
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

	size := binary.LittleEndian.Uint32(msg[0:headerFragmentSize])
	if len(msg) >= int(size+headerFragmentSize) {

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

func (c *Consumer) routeResponse(msgType, opcode int8, msg string) {
	switch msgType {
	case respV8WorkerConfig:
		switch opcode {
		case sourceMap:
		case logMessage:
			fmt.Printf("%s", msg)
		}
	}
}
