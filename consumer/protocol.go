package consumer

import (
	"log"

	"github.com/couchbase/eventing/flatbuf/header"
	"github.com/couchbase/eventing/flatbuf/payload"
	"github.com/google/flatbuffers/go"
)

const (
	eventType int8 = iota
	dcpEvent
	httpEvent
	v8DebugEvent
	v8WorkerEvent
	appWorkerSetting
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

type message struct {
	Header  []byte
	Payload []byte

	ResChan chan response
}

type response struct {
	res      string
	logEntry string
	err      error
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

func makeV8InitPayload(appName, kvHostPort, depCfg string) []byte {
	builder := flatbuffers.NewBuilder(0)
	builder.Reset()

	app := builder.CreateString(appName)
	dcfg := builder.CreateString(depCfg)
	khp := builder.CreateString(kvHostPort)

	payload.PayloadStart(builder)

	payload.PayloadAddAppName(builder, app)
	payload.PayloadAddDepcfg(builder, dcfg)
	payload.PayloadAddKvHostPort(builder, khp)

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
