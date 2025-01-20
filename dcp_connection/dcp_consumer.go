package dcpConn

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/notifier"
)

var (
	ErrDecodingComponentID = errors.New("error decoding keyspaceComponentId")
)

const (
	systemXattrByte = byte('_')
)

type DcpConsumer interface {
	Wait() error

	StartStreamReq(sr *StreamReq) error
	PauseStreamReq(sr *StreamReq)
	StopStreamReq(sr *StreamReq) *StreamReq

	GetSeqNumber(collectionID string) map[uint16]uint64
	GetFailoverLog(vbs []uint16) (map[uint16]FailoverLog, error)

	TlsSettingsChange(config *notifier.TlsConfig)

	GetRuntimeStats() common.StatsInterface
	CloseDcpConsumer() []*DcpEvent
}

type Mode uint8

const (
	InfoMode Mode = iota
	StreamRequestMode
	MixedMode
)

type Config struct {
	Mode       Mode   `json:"mode"`
	ClientName string `json:"client_name"`
	BucketName string `json:"bucket_name"`
	KvAddress  string `json:"kv_address"`

	DcpConfig map[ConfigKey]interface{} `json:"dcp_config"`
}

func (c Config) String() string {
	return fmt.Sprintf("{ clientName: %s, bucketName: %s, kvAddress: %s, mode: %v }", c.ClientName, c.BucketName, c.KvAddress, c.Mode)
}

var (
	ErrInvalidRequest  = errors.New("invalid Request")
	ErrAlreadyInflight = errors.New("stream request is already in flight")
)

type RequestType int8

const (
	Request_Collections RequestType = iota
	Request_Scope
	Request_Bucket
)

type StreamReq struct {
	ID      uint16 `json:"-"`
	Version uint32 `json:"-"`

	// Status gives whats the status of the current request
	Status status `json:"status"`

	RequestType   RequestType `json:"-"`
	CollectionIDs []string    `json:"-"` //array of collction ids
	ScopeID       string      `json:"-"` // scope id
	ManifestUID   string      `json:"-"` //manifest id

	Vbno             uint16      `json:"vb_no"`
	Flags            uint32      `json:"flags"`
	StartSeq         uint64      `json:"seq_num_received"`
	EndSeq           uint64      `json:"end_seq_no"`
	Vbuuid           uint64      `json:"vb_uuid"`
	FailoverLog      FailoverLog `json:"-"`
	failoverLogIndex int

	// Internal field to check if request is already made on this
	// connection or not
	opaque  uint32
	running bool

	LastStreamSuccessTime   time.Time `json:"stream_success_time"`
	LastStreamRequestedTime time.Time `json:"stream_requested_time"`
}

func (sr *StreamReq) Copy() *StreamReq {
	newSR := &StreamReq{
		ID:      sr.ID,
		Version: sr.Version,

		Status: sr.Status,

		RequestType:   sr.RequestType,
		CollectionIDs: sr.CollectionIDs,
		ScopeID:       sr.ScopeID,
		ManifestUID:   sr.ManifestUID,

		Vbno:             sr.Vbno,
		Flags:            sr.Flags,
		StartSeq:         sr.StartSeq,
		EndSeq:           sr.EndSeq,
		Vbuuid:           sr.Vbuuid,
		FailoverLog:      sr.FailoverLog,
		failoverLogIndex: sr.failoverLogIndex,

		opaque:  sr.opaque,
		running: sr.running,

		LastStreamSuccessTime:   sr.LastStreamSuccessTime,
		LastStreamRequestedTime: sr.LastStreamRequestedTime,
	}

	return newSR
}

func (sr *StreamReq) String() string {
	return fmt.Sprintf("{ \"id\": %d, \"version\": %d, \"status\": %d, \"requestType\": %d, \"collection_id\": %v, \"scope_id\": %s, \"vb_no\": %d, \"start_seq\": %d, \"end_seq\": %d, \"vbuuid\": %d, \"running\": %t, \"lastStreamSuccessTime\": %v, \"lastStreamRequtedTime\": %v }", sr.ID, sr.Version, sr.Status, sr.RequestType, sr.CollectionIDs, sr.ScopeID, sr.Vbno, sr.StartSeq, sr.EndSeq, sr.Vbuuid, sr.running, sr.LastStreamSuccessTime, sr.LastStreamRequestedTime)
}

type valueType uint8

type xattrVal struct {
	body []byte
}

func (x xattrVal) MarshalJSON() ([]byte, error) {
	return x.body, nil
}

func (x xattrVal) Bytes() []byte {
	return x.body
}

type DcpEvent struct {
	Opcode   resCommandCode
	Datatype dcpDatatype
	Vbno     uint16
	ID       uint16
	Version  uint32

	Vbuuid       uint64
	Key, Value   []byte
	Cas          uint64
	ScopeID      uint32
	CollectionID uint32
	Seqno        uint64
	Status       status
	Type         valueType
	Expiry       uint32

	EventType   collectionEvent
	ManifestUID string
	FailoverLog FailoverLog

	Keyspace    *common.MarshalledData[application.Keyspace]
	SystemXattr map[string]xattrVal
	UserXattr   map[string]xattrVal
	SrRequest   *StreamReq

	opaque uint32
}

type FailoverLog [][2]uint64

func (failoverLog FailoverLog) Pop(seq uint64) (FailoverLog, uint64, uint64) {
	if failoverLog == nil {
		return nil, uint64(0), uint64(0)
	}

	_, index := GetVbUUID(seq, failoverLog)
	if index == 0 {
		return nil, uint64(0), uint64(0)
	}

	fLog := failoverLog[index-1]
	uuid, seq := fLog[0], fLog[1]
	return failoverLog[:index], uuid, seq
}

/*
func (failoverLog1 FailoverLog) Equal(failoverLog2 FailoverLog) bool {
	if len(failoverLog1) != len(failoverLog2) {
		return false
	}

	for index := 0; index < len(failoverLog1); index++ {
		failoverLog1[index][0], failoverLog2[index][0]

	}

	return true
}
*/

func GetVbUUID(seqNo uint64, failoverLog FailoverLog) (uint64, int) {
	for index, fLog := range failoverLog {
		vbuuid, failoverSeqNo := fLog[0], fLog[1]
		if failoverSeqNo <= seqNo {
			return vbuuid, index
		}
	}
	return 0, len(failoverLog) - 1
}

func (event *DcpEvent) Reset() {
	event.Key = event.Key[:0]
	event.Value = event.Value[:0]
	event.ManifestUID = ""
	event.Opcode = DCP_NO_RES
	event.EventType = EVENT_UNKNOWN
	event.Status = SUCCESS
	event.SrRequest = nil
	event.Keyspace = nil
	event.Expiry = 0
	event.FailoverLog = nil
}

type resCommandCode uint8

const (
	DCP_NO_RES        = resCommandCode(0x00)
	DCP_SEQ_NUM       = resCommandCode(0x48)
	DCP_OPEN_CONN     = resCommandCode(0x50) // Open a DCP connection with a name
	DCP_SELECT_BUCKET = resCommandCode(0x89) // Select bucket
	DCP_ADDSTREAM     = resCommandCode(0x51) // DCP stream addition
	DCP_BUFFERACK     = resCommandCode(0x5d) // DCP Buffer Acknowledgement
	DCP_CONTROL       = resCommandCode(0x5e) // Set flow control params
	DCP_NOOP          = resCommandCode(0x5c) // dcp noop
	DCP_FLUSH         = resCommandCode(0x5a)

	DCP_SNAPSHOT_MARKER = resCommandCode(0x56) //snapshot marker

	DCP_DELETION   = resCommandCode(0x58)
	DCP_EXPIRATION = resCommandCode(0x59)
	DCP_MUTATION   = resCommandCode(0x57)

	DCP_FAILOVER   = resCommandCode(0x54)
	DCP_SEQ_NUMBER = resCommandCode(0x48)

	DCP_STREAMREQ   = resCommandCode(0x53)
	DCP_STREAM_END  = resCommandCode(0x55)
	DCP_CLOSESTREAM = resCommandCode(0x52)

	DCP_SYSTEM_EVENT = resCommandCode(0x5F)
	DCP_ADV_SEQNUM   = resCommandCode(0x64)

	DCP_HELO = resCommandCode(0x1F)

	DCP_SASL_AUTH  = resCommandCode(0x21)
	SASL_AUTH_LIST = resCommandCode(0x20)
)

type collectionEvent uint32

const (
	COLLECTION_CREATE  = collectionEvent(0x00) // Collection has been created
	COLLECTION_DROP    = collectionEvent(0x01) // Collection has been dropped
	COLLECTION_FLUSH   = collectionEvent(0x02) // Collection has been flushed
	SCOPE_CREATE       = collectionEvent(0x03) // Scope has been created
	SCOPE_DROP         = collectionEvent(0x04) // Scope has been dropped
	COLLECTION_CHANGED = collectionEvent(0x05) // Collection has changed

	OSO_SNAPSHOT_START = collectionEvent(0x06) // OSO snapshot start
	OSO_SNAPSHOT_END   = collectionEvent(0x07) // OSO snapshot end

	EVENT_UNKNOWN = collectionEvent(0xFF)
)

type status uint16

const (
	// Can add more status later
	SUCCESS         = status(0x00)
	KEY_ENOENT      = status(0x01)
	FORCED_CLOSED   = status(0x01) // for stream end status
	STATE_CHANGED   = status(0x02)
	KEY_EEXISTS     = status(0x02)
	E2BIG           = status(0x03)
	EINVAL          = status(0x04)
	NOT_STORED      = status(0x05)
	DELTA_BADVAL    = status(0x06)
	FILTER_DELETED  = status(0x07) // Filter is deleted
	NOT_MY_VBUCKET  = status(0x07)
	LOST_PRIVILAGE  = status(0x08)
	ERANGE          = status(0x22)
	ROLLBACK        = status(0x23)
	UNKNOWN_COMMAND = status(0x81)
	ENOMEM          = status(0x82)
	TMPFAIL         = status(0x86)
	UNKNOWN         = status(0xFF)
)

type ConfigKey int

const (
	// KeyOnly specifies whether to open connection with key only
	KeyOnly ConfigKey = iota
	IncludeXattr
)

type heloCommand uint16

const (
	FEATURE_COLLECTIONS = heloCommand(0x12)
	FEATURE_XERROR      = heloCommand(0x07)
)

type dcpDatatype uint8

const (
	RAW    = 0x00
	JSON   = 0x01
	SNAPPY = 0x02
	XATTR  = 0x04
)

func GetHexToUint32(keyspaceComponentHexId string) (uint32, error) {
	keyspaceComponentId, err := strconv.ParseUint(keyspaceComponentHexId, 16, 32)
	if err != nil {
		return 0, ErrDecodingComponentID
	}
	return uint32(keyspaceComponentId), nil
}
