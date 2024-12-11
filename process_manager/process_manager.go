package processManager

import (
	"bytes"
	"context"

	checkpointManager "github.com/couchbase/eventing/checkpoint_manager"
	"github.com/couchbase/eventing/common"
	dcpMessage "github.com/couchbase/eventing/dcp_connection"
)

type AppLogFunction func(id string, msg string)

type ProcessConfig struct {
	Username string
	Password string
	Address  string

	NsServerPort string

	// ipv4/ipv6
	IPMode          common.IpMode
	BreakpadOn      bool
	DebuggerPort    string
	DiagDir         string
	EventingDir     string
	EventingPort    string
	EventingSSLPort string
	ExecPath        string
	CertPath        string
	ClientCertPath  string
	ClientKeyPath   string

	// Id used to create tcp socket
	ID string

	AppLogCallback AppLogFunction
}

func (p ProcessConfig) String() string {
	return p.ID
}

type ResponseMessage struct {
	Event     Command
	Opcode    uint8
	HandlerID string

	Extras []byte
	Meta   []byte
	Value  []byte
}

// ProcessManager will spawn c++ process
// This will also take care of sending messages to c++ side
// Response for the process is sent in the response channel
type ProcessManager interface {
	// Start will start the process. response channel should be used
	// to get response
	// CHECK: if this should return error or not
	// or process manager should takecare of the errors internally
	Start() (<-chan *ResponseMessage, error)
	StartWithContext(ctx context.Context) (<-chan *ResponseMessage, error)

	InitEvent(version uint32, opcode uint8, handlerID []byte, value interface{})

	VbSettings(version uint32, opcode uint8, handlerID []byte, key interface{}, value interface{})

	LifeCycleOp(version uint32, opcode uint8, handlerID []byte)

	SendControlMessage(version uint32, cmd Command, opcode uint8, handlerID []byte, key, value interface{})

	GetStats(version uint32, opcode uint8, handlerID []byte)

	WriteDcpMessage(version uint32, buffer *bytes.Buffer, opcode uint8, workerID uint8,
		instanceID []byte, msg *dcpMessage.DcpEvent, internalInfo *checkpointManager.ParsedInternalDetails) int32

	FlushMessage(version uint32, buffer *bytes.Buffer)

	GetProcessVersion() uint32

	StopProcess()
}
