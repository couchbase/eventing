package processManager

import (
	"encoding/binary"
	"encoding/json"
	"strings"
	"sync"

	"github.com/couchbase/eventing/application"
	header "github.com/couchbase/eventing/gen/flatbuf/header_v2"
	"github.com/couchbase/eventing/process_manager/communicator"
	flatbuffers "github.com/google/flatbuffers/go"
)

type Command uint8

const (
	// Initialise the new handler
	InitEvent Command = iota

	// Functions dynamic settings
	HandlerDynamicSettings

	// Vb related settings
	VbSettings

	// pause/resume/stop
	LifeCycleChange

	// For statistics
	StatsEvent

	// Dcp event
	DcpEvent

	// GlobalConfig change
	GlobalConfigChange

	// InternalComm change
	InternalComm
)

type extraValue struct {
	datatype uint8
	vb       uint16
	expiry   uint32
	cid      uint32
	connSeq  uint32
	vbuuid   uint64
	seq      uint64

	cursorDetailsPresent bool
	rootCas              uint64
	staleIds             []string
	cas                  uint64
	key                  []byte
	scope                string
	collection           string
}

type flatbufMessageBuilder struct {
	builderPool     *sync.Pool
	commMessagePool *sync.Pool
}

func newflatbufMessageBuilder() *flatbufMessageBuilder {
	return &flatbufMessageBuilder{
		builderPool: &sync.Pool{
			New: func() interface{} {
				return flatbuffers.NewBuilder(8)
			},
		},

		commMessagePool: &sync.Pool{
			New: func() interface{} {
				return &communicator.CommMessage{}
			},
		},
	}
}

func (mb *flatbufMessageBuilder) putBuilder(commMsg *communicator.CommMessage, builder *flatbuffers.Builder) {
	builder.Reset()
	mb.builderPool.Put(builder)

	commMsg.Reset()
	mb.commMessagePool.Put(commMsg)
}

func getPayloadMeta(eventCode Command, opcode uint8, idBytesLen uint8, payloadByteLen uint32) uint64 {
	payloadMeta := uint64(0)
	payloadMeta = payloadMeta | uint64(eventCode)<<56
	payloadMeta = payloadMeta | uint64(opcode)<<48
	payloadMeta = payloadMeta | uint64(idBytesLen)<<40
	payloadMeta = payloadMeta | uint64(payloadByteLen)
	return payloadMeta
}

func getMsgsSize(metadata uint64) (uint8, uint32) {
	idSize := uint8((metadata & (0xff << 40)) >> 40)
	msgSize := uint32(metadata & (0xffffffff))

	return idSize, msgSize
}

func (mb *flatbufMessageBuilder) buildMessage(builder *flatbuffers.Builder, eventCode Command, opcode uint8,
	idBytes, extras, meta, xattr, value []byte) (*communicator.CommMessage, *flatbuffers.Builder) {

	if builder == nil {
		builder = mb.builderPool.Get().(*flatbuffers.Builder)
	}

	extrasOffset := builder.CreateByteString(extras)
	metaOffset := builder.CreateByteString(meta)
	valueOffset := builder.CreateByteString(value)
	xattrOffset := builder.CreateByteString(xattr)

	header.HeaderV2Start(builder)

	header.HeaderV2AddExtras(builder, extrasOffset)
	header.HeaderV2AddMeta(builder, metaOffset)
	header.HeaderV2AddValue(builder, valueOffset)
	header.HeaderV2AddXattr(builder, xattrOffset)
	headerPos := header.HeaderV2End(builder)

	builder.Finish(headerPos)

	encodedHeader := builder.FinishedBytes()

	idSize := uint8(len(idBytes))
	msgSize := uint32(len(encodedHeader))
	metadata := getPayloadMeta(eventCode, opcode, idSize, msgSize)

	comm := mb.commMessagePool.Get().(*communicator.CommMessage)
	comm.Metadata = metadata
	comm.Identifier = idBytes
	comm.Msg = encodedHeader

	return comm, builder
}

// For InitEvent
const (
	CompileHandler uint8 = iota
	InitHandler
	DebugHandlerStart
	DebugHandlerStop
)

func (mb *flatbufMessageBuilder) InitMessage(opcode uint8, handlerID []byte, value interface{}) (*communicator.CommMessage, *flatbuffers.Builder) {
	var val []byte

	switch opcode {
	case InitHandler:
		v := value.(*application.FunctionDetails)
		val, _ = json.Marshal(v)

	case CompileHandler:
		code := value.(string)
		val = []byte(code)

	case DebugHandlerStart:
		val, _ = json.Marshal(value)

	case DebugHandlerStop:
		// Just send the handlerID
	}

	return mb.buildMessage(nil, InitEvent, opcode, handlerID, nil, nil, nil, val)
}

// For HandlerDynamicSettings
const (
	LogLevelChange uint8 = iota
	TimeContextSize
)

func (mb *flatbufMessageBuilder) DynamicSettingMessage(opcode uint8, handlerID []byte, value interface{}) (*communicator.CommMessage, *flatbuffers.Builder) {
	var val []byte

	switch opcode {
	case LogLevelChange:
		logLevel := value.(string)
		val = []byte(logLevel)

	case TimeContextSize:
		contextSize := value.(uint32)
		val = make([]byte, 4)
		binary.BigEndian.PutUint32(val, contextSize)
	}

	return mb.buildMessage(nil, HandlerDynamicSettings, opcode, handlerID, nil, nil, nil, val)
}

// For VbSettings
const (
	VbMap uint8 = iota
	VbAddChanges
	FilterVb
)

func (mb *flatbufMessageBuilder) VbSettingsMessage(opcode uint8, handlerID []byte, key, value interface{}) (*communicator.CommMessage, *flatbuffers.Builder) {
	var extras []byte

	switch opcode {
	case VbMap:
		extras = value.([]byte)

	case FilterVb:
		extras = make([]byte, 2)
		binary.BigEndian.PutUint16(extras[0:], key.(uint16))

	case VbAddChanges:
		extras = make([]byte, 18)
		binary.BigEndian.PutUint16(extras[0:], key.(uint16))
		vbInfo := value.([]uint64)
		binary.BigEndian.PutUint64(extras[2:], vbInfo[0])
		binary.BigEndian.PutUint64(extras[10:], vbInfo[1])
	}

	return mb.buildMessage(nil, VbSettings, opcode, handlerID, extras, nil, nil, nil)
}

// For LifeCycleChange
const (
	// maybe init also comes in this category
	Pause uint8 = iota
	Resume
	Stop
)

func (mb *flatbufMessageBuilder) LifeCycleChangeMessage(opcode uint8, handlerID []byte) (*communicator.CommMessage, *flatbuffers.Builder) {
	return mb.buildMessage(nil, LifeCycleChange, opcode, handlerID, nil, nil, nil, nil)
}

// For StatsEvent
const (
	LatencyStats uint8 = iota
	Insight
	FailureStats
	ExecutionStats
	LcbExceptions
	CurlLatencyStats
	ProcessedEvents
	StatsAckBytes
	AllStats
)

// Empty handlername gives statistics of all handler
// maybe handler uuid should be used
func (mb *flatbufMessageBuilder) StatisticsMessage(opcode uint8, handlerID []byte) (*communicator.CommMessage, *flatbuffers.Builder) {
	return mb.buildMessage(nil, StatsEvent, opcode, handlerID, nil, nil, nil, nil)
}

// For DcpEvent
const (
	DcpMutation uint8 = iota
	DcpDeletion
	DcpNoOp
	DcpCollectionDelete
)

const (
	workerIdByte = 1
	vbByte       = 2
	cidByte      = 4
	vbuuidByte   = 8
	seqNumByte   = 8
	totalBytes   = workerIdByte + vbByte + cidByte + vbuuidByte + seqNumByte

	extrasDefaultLength = 5
	extrasWithCursor    = 30
)

func (mb *flatbufMessageBuilder) DcpMessage(opcode uint8, workerID uint8, handlerID []byte, extraV extraValue, meta, xattr, value []byte) (*communicator.CommMessage, *flatbuffers.Builder) {
	// TODO: In future for maintanibilty use flatbuffer to encode and send
	idBytes := make([]byte, 0, len(handlerID)+totalBytes)
	idBytes = append(idBytes, handlerID...)
	idBytes = append(idBytes, byte(workerID))
	idBytes = binary.BigEndian.AppendUint16(idBytes, extraV.vb)
	idBytes = binary.BigEndian.AppendUint32(idBytes, extraV.cid)
	idBytes = binary.BigEndian.AppendUint64(idBytes, extraV.vbuuid)
	idBytes = binary.BigEndian.AppendUint64(idBytes, extraV.seq)

	totalPossibleExtraLength := extrasDefaultLength
	if extraV.cursorDetailsPresent {
		// Not taking account of stale cursors
		totalPossibleExtraLength += extrasWithCursor + len(extraV.scope) + len(extraV.collection)
	}
	extras := make([]byte, 0, totalPossibleExtraLength)
	extras = append(extras, extraV.datatype)
	extras = binary.BigEndian.AppendUint32(extras, extraV.expiry)
	if extraV.cursorDetailsPresent {
		extras = binary.BigEndian.AppendUint64(extras, extraV.rootCas)
		extras = binary.BigEndian.AppendUint64(extras, extraV.cas)
		extras = binary.BigEndian.AppendUint32(extras, uint32(len(extraV.key)))
		extras = append(extras, extraV.key...)
		staleIdString := strings.Join(extraV.staleIds, ",")
		extras = binary.BigEndian.AppendUint32(extras, uint32(len(staleIdString)))
		extras = append(extras, []byte(staleIdString)...)
		extras = binary.BigEndian.AppendUint16(extras, uint16(len(extraV.scope)))
		extras = append(extras, []byte(extraV.scope)...)
		extras = binary.BigEndian.AppendUint16(extras, uint16(len(extraV.collection)))
		extras = append(extras, []byte(extraV.collection)...)
	}

	return mb.buildMessage(nil, DcpEvent, opcode, idBytes, extras, meta, xattr, value)
}

// GlobalConfigChange
const (
	FeatureMatrix uint8 = iota
)

func (mb *flatbufMessageBuilder) GlobalConfigMessage(opcode uint8, handlerID []byte, value interface{}) (*communicator.CommMessage, *flatbuffers.Builder) {
	var val []byte

	switch opcode {
	case FeatureMatrix:
		featureMatrix := value.(uint32)
		val = make([]byte, 4)
		binary.BigEndian.PutUint32(val, featureMatrix)
	}

	return mb.buildMessage(nil, GlobalConfigChange, opcode, handlerID, nil, nil, nil, val)
}

const (
	AckBytes uint8 = iota
)

// Parsing message from server
func (mb *flatbufMessageBuilder) ParseMessage(metadata uint64, idSize uint8, msg []byte) *ResponseMessage {
	resMsg := &ResponseMessage{}

	resMsg.Event = Command((metadata & (0xff << 56)) >> 56)
	resMsg.Opcode = uint8((metadata & (0xff << 48)) >> 48)
	if resMsg.Event == DcpEvent {
		resMsg.HandlerID = string(msg[:idSize-totalBytes])
	} else {
		resMsg.HandlerID = string(msg[:idSize])
	}
	msg = msg[idSize:]

	res := header.GetRootAsHeaderV2(msg, 0)
	resMsg.Extras = make([]byte, len(res.Extras()))
	copy(resMsg.Extras, res.Extras())
	resMsg.Meta = make([]byte, len(res.Meta()))
	copy(resMsg.Meta, res.Meta())
	resMsg.Value = make([]byte, len(res.Value()))
	copy(resMsg.Value, res.Value())

	return resMsg
}
