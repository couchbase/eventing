package communicator

import (
	"bytes"
	"errors"
	"io"
	"net"

	"github.com/couchbase/eventing/common"
)

var (
	// ErrNotInit is returned by the function when connection is not initialised
	ErrNotInit = errors.New("Connection not initialised")
)

// ConnSettings contains all the details of the connection
type ConnSettings struct {
	ServerPort string
	ID         string
	IPMode     common.IpMode

	// Socket details
	IpcType            string
	SockIdentifier     string
	FeedbackIdentifier string
}

// ConnFunction is the function that is used to create listner interface
type ConnFunction func(*ConnSettings) (net.Listener, net.Listener, error)

type CommMessage struct {
	Metadata   uint64
	Identifier []byte
	Msg        []byte
}

func (c *CommMessage) Reset() {
	c.Metadata = 0
	c.Identifier = c.Identifier[:0]
	c.Msg = c.Msg[:0]
}

// Comm provides function which is used to send and receive message
type Comm interface {
	// Start the connection with given ConnSettings.ID
	Start() error

	// Wait for connection to establish
	Wait() error

	// Connection type and identifier
	Details() ConnSettings

	// Close the connection with process
	Close()

	// Receive response type from process
	ReceiveResponse(readBuffer []byte, size uint32) ([]byte, error)

	// Receive stdout message from process
	ReceiveStdoutMsg(readBuffer []byte, size uint32) ([]byte, error)

	// Receive Stderr message from process
	ReceiveStdErrMsg(readBuffer []byte, sep byte) ([]byte, error)

	// Set stdout and stderr
	SetStdOutErrBuffer(readerOut, readerErr io.Reader)

	// Write message to buffer
	Write(msg *CommMessage) error

	// Write the buffer message
	WriteToBuffer(buffer *bytes.Buffer, msg *CommMessage) error

	// ResetBuffer resets the buffer
	ResetBuffer()

	// FlushMessageImmediately flushes the message immediately
	FlushMessageImmediately(msg *CommMessage) error

	// FlushMessage sends cached message
	FlushMessage() error

	// FlushMessageImmediatelyForBuffer sends all the message in the buffer
	FlushMessageImmediatelyForBuffer(buffer *bytes.Buffer) error
}
