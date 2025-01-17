package communicator

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
)

type connType uint8

const (
	SOCKET connType = iota
)

type stats struct {
	Settings    ConnSettings `json:"conn_settings"`           // Connection settings
	MsgInBuffer uint64       `json:"msg_remaining_in_buffer"` // Number of messages remaining in the buffer
	NumFlushed  uint64       `json:"num_flushed"`             // Number of times flush message was called
}

func (s *stats) Copy() *stats {
	return &stats{
		Settings:    s.Settings,
		MsgInBuffer: atomic.LoadUint64(&s.MsgInBuffer),
		NumFlushed:  atomic.LoadUint64(&s.NumFlushed),
	}
}

type communicator struct {
	sync.Mutex

	settings  ConnSettings
	msgBuffer *bytes.Buffer

	conn         io.ReadWriteCloser
	feedbackConn io.ReadWriteCloser

	socketReader       io.Reader
	sockFeedbackReader io.Reader
	bufOut             io.Reader
	bufErr             io.Reader

	listener        net.Listener
	feedbackListner net.Listener

	feedbackAcceptCh chan []interface{}
	acceptCh         chan []interface{}

	connFunction ConnFunction

	stat *stats
}

func NewCommunicator(conn connType, settings ConnSettings) *communicator {
	connFunc := dummyConnect
	switch conn {
	case SOCKET:
		connFunc = socketConnect
	}

	return NewCommunicatorWithConnFunc(connFunc, settings)
}

func NewCommunicatorWithConnFunc(connFunction ConnFunction, settings ConnSettings) *communicator {
	c := &communicator{
		settings: settings,

		acceptCh:         make(chan []interface{}, 1),
		feedbackAcceptCh: make(chan []interface{}, 1),

		msgBuffer: &bytes.Buffer{},

		conn:         dummyConn(0),
		feedbackConn: dummyConn(0),

		bufOut:             dummyConn(0),
		bufErr:             dummyConn(0),
		socketReader:       dummyConn(0),
		sockFeedbackReader: dummyConn(0),

		connFunction: connFunction,

		stat: &stats{Settings: settings},
	}

	return c
}

func (c *communicator) GetRuntimeStats() common.StatsInterface {
	return common.NewMarshalledData(c.stat.Copy())
}

func (c *communicator) GetBufferStats() uint64 {
	return atomic.LoadUint64(&c.stat.MsgInBuffer)
}

func waitForConnAccept(listener net.Listener, acceptCh chan<- []interface{}) {
	conn, err := listener.Accept()
	acceptCh <- []interface{}{conn, err}
}

func (c *communicator) SetStdOutErrBuffer(bufOut, bufErr io.Reader) {
	c.bufOut = bufOut
	c.bufErr = bufErr
}

func (c *communicator) Start() error {
	listener, feedbackListner, err := c.connFunction(&c.settings)
	c.stat.Settings = c.settings
	c.stat.MsgInBuffer = 0
	c.stat.NumFlushed = 0
	if err != nil {
		return err
	}
	c.listener = listener
	c.feedbackListner = feedbackListner

	go waitForConnAccept(c.listener, c.acceptCh)
	go waitForConnAccept(c.feedbackListner, c.feedbackAcceptCh)

	return nil
}

func (c *communicator) Wait() error {
	t := time.NewTicker(6 * time.Second)
	defer t.Stop()

	for i := 0; i != 2; i++ {
		select {
		case accepted := <-c.acceptCh:
			err, ok := accepted[1].(error)
			if ok && err != nil {
				return fmt.Errorf("error accepting normal connection: %v", err)
			}

			c.Lock()
			c.conn = accepted[0].(net.Conn)
			c.socketReader = bufio.NewReader(c.conn)
			c.Unlock()

		case accepted := <-c.feedbackAcceptCh:
			err, ok := accepted[1].(error)
			if ok && err != nil {
				return fmt.Errorf("error accepting feedback connection: %v", err)
			}

			c.Lock()
			c.feedbackConn = accepted[0].(net.Conn)
			c.sockFeedbackReader = bufio.NewReader(c.feedbackConn)
			c.Unlock()

		case <-t.C:
			return fmt.Errorf("timeout in accepting connection")
		}
	}

	return nil
}

// Connection type and identifier
func (c *communicator) Details() ConnSettings {
	return c.settings
}

// Close the connection with process
func (c *communicator) Close() {
	c.Lock()
	defer c.Unlock()

	c.conn.Close()
	c.feedbackConn.Close()
	c.conn = dummyConn(0)
	c.feedbackConn = dummyConn(0)

	c.bufOut = dummyConn(0)
	c.bufErr = dummyConn(0)
	c.socketReader = dummyConn(0)
	c.sockFeedbackReader = dummyConn(0)

	c.msgBuffer.Reset()

	c.listener.Close()
	c.feedbackListner.Close()
}

func (c *communicator) ReceiveResponse(readBuffer []byte, size uint32) ([]byte, error) {
	return c.receiveMessage(c.sockFeedbackReader, readBuffer, size)
}

func (c *communicator) ReceiveStdoutMsg(readBuffer []byte, size uint32) ([]byte, error) {
	return c.receiveMessage(c.bufOut, readBuffer, size)
}

func (c *communicator) ReceiveStdErrMsg(readBuffer []byte, sep byte) ([]byte, error) {
	index := 0

	for {
		if cap(readBuffer) <= index {
			readBuffer = append(readBuffer, byte(0))
		} else {
			readBuffer = readBuffer[:index+1]
		}

		n, err := c.bufErr.Read(readBuffer[index : index+1])
		if err != nil {
			return readBuffer, err
		}

		if n == 0 {
			continue
		}

		if readBuffer[index] == sep {
			return readBuffer, nil
		}

		index++
	}
}

// Receive message from process
func (c *communicator) receiveMessage(buf io.Reader, readBuffer []byte, size uint32) ([]byte, error) {
	if uint32(cap(readBuffer)) < size {
		readBuffer = make([]byte, 0, size)
	}
	readBuffer = readBuffer[:size]

	index := 0
	for size > 0 {
		n, err := buf.Read(readBuffer[index:])
		if err != nil {
			return readBuffer, err
		}

		index = index + n
		size = size - uint32(n)
	}
	return readBuffer, nil
}

// Write message to buffer
func (c *communicator) Write(msg *CommMessage) (uint64, error) {
	c.Lock()
	defer c.Unlock()

	err := c.write(c.msgBuffer, msg)
	if err != nil {
		return 0, err
	}

	remaining := atomic.AddUint64(&c.stat.MsgInBuffer, 1)
	return remaining, nil
}

func (c *communicator) WriteToBuffer(buffer *bytes.Buffer, msg *CommMessage) error {
	return c.write(buffer, msg)
}

// CHECK: The problem with this is half sent message from the cache. Do we need this?
// If temp error then in the next cycle we can flush the remaining message
// But with send immediately the issue will be half sent message and we can't use flush immediately
func (c *communicator) FlushMessageImmediately(msg *CommMessage) error {
	buffer := bytes.NewBuffer(make([]byte, 0, 8+len(msg.Identifier)+len(msg.Msg)))
	err := c.write(buffer, msg)
	if err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()

	return c.flushMessageWithLock(buffer)
}

func (c *communicator) ResetBuffer() {
	c.Lock()
	defer c.Unlock()

	c.msgBuffer.Reset()
}

func (c *communicator) FlushMessageImmediatelyForBuffer(buffer *bytes.Buffer) error {
	c.Lock()
	defer c.Unlock()

	return c.flushMessageWithLock(buffer)
}

// FlushMessage sends the message to socket
func (c *communicator) FlushMessage() error {
	c.Lock()
	defer c.Unlock()

	err := c.flushMessageWithLock(c.msgBuffer)
	if err != nil {
		return err
	}
	atomic.StoreUint64(&c.stat.MsgInBuffer, 0)
	return nil
}

// Write the message to local buffer
func (c *communicator) write(buffer *bytes.Buffer, msg *CommMessage) error {
	err := binary.Write(buffer, binary.BigEndian, msg.Metadata)
	if err != nil {
		return err
	}

	err = binary.Write(buffer, binary.BigEndian, msg.Identifier)
	if err != nil {
		return err
	}

	return binary.Write(buffer, binary.BigEndian, msg.Msg)
}

// Flush given message to socket
func (c *communicator) flushMessageWithLock(msgBuffer *bytes.Buffer) error {
	err := io.ErrShortWrite

	for ; errors.Is(err, io.ErrShortWrite); _, err = msgBuffer.WriteTo(c.conn) {
	}

	atomic.AddUint64(&c.stat.NumFlushed, 1)
	return err
}
