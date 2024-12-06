package dcpConn

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/authenticator"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
)

var (
	ErrConnClosed          = errors.New("connection closed. Retry request")
	ErrFetchingSeqNum      = errors.New("error fetching seq number")
	ErrFetchingFailoverLog = errors.New("error fetching FailoverLog")
	ErrTimeout             = errors.New("request timeout")

	errNotInitialised = errors.New("connection not initialised")
)

const (
	seqFetchInterval = 100 * time.Millisecond
)

// Constant values for starting request
const (
	connBufferSize                   = 20 * common.MiB
	setNoopInterval                  = time.Duration(240) // In seconds
	enableNoop                       = "true"
	enableStreamID                   = "true"
	sendStreamEndOnClientCloseStream = "true"
	enableExpiryOpcode               = "true"
	backfillOrder                    = "round-robin"
)

const (
	requestThreshold = 2048
)

const (
	recvChannelSize        = 100
	maxInternalCommandWait = 10 * time.Second
	unackedBytesLimit      = 0.5
)

const (
	randomVB      = uint16(1024)
	failoverLogID = uint32(math.MaxUint32)
)

type client struct {
	sync.Mutex

	config       Config
	tlsConfig    *notifier.TlsConfig
	tcpConn      io.ReadWriteCloser
	unackedBytes uint32
	hdrBytes     []byte
	sendChannel  chan<- *DcpEvent
	dcpEventPool *sync.Pool

	requestManager *reqManager
	requestChannel chan command
	recvChannel    chan *memcachedCommand

	connVersion          *atomic.Uint32
	closed               *atomic.Bool
	requestRoutineClosed *atomic.Bool
	genServerClosed      *atomic.Bool
	pendingDcpEvents     []*DcpEvent
	close                func()

	// internal request response like GetSeqNumber
	seqMap                 atomic.Value
	seqMapChannel          chan map[uint16]uint64
	seqConnected           chan struct{}
	internalCommandChannel map[uint32]map[uint16][]chan interface{}
}

type dummyRWC struct {
}

func (*dummyRWC) Read(_ []byte) (int, error) {
	return 0, errNotInitialised
}

func (*dummyRWC) Write(_ []byte) (int, error) {
	return 0, errNotInitialised
}

func (*dummyRWC) Close() error {
	return nil
}

var defaultConfig = map[ConfigKey]interface{}{IncludeXattr: true}

func GetDcpConsumer(config Config, tlsConfig *notifier.TlsConfig, sendChannel chan<- *DcpEvent, dcpEventPool *sync.Pool) DcpConsumer {
	return GetDcpConsumerWithContext(context.Background(), config, tlsConfig, sendChannel, dcpEventPool)
}

func GetDcpConsumerWithContext(ctx context.Context, config Config, tlsConfig *notifier.TlsConfig, sendChannel chan<- *DcpEvent, dcpEventPool *sync.Pool) DcpConsumer {
	logPrefix := "dcpconnection::DcpConnection"

	if config.DcpConfig == nil {
		config.DcpConfig = defaultConfig
	}

	c := &client{
		config:               config,
		tlsConfig:            tlsConfig,
		tcpConn:              &dummyRWC{},
		connVersion:          &atomic.Uint32{},
		closed:               &atomic.Bool{},
		sendChannel:          sendChannel,
		hdrBytes:             make([]byte, hdr_len),
		dcpEventPool:         dcpEventPool,
		requestRoutineClosed: &atomic.Bool{},
		genServerClosed:      &atomic.Bool{},
		pendingDcpEvents:     make([]*DcpEvent, 0),
	}

	c.requestRoutineClosed.Store(false)
	c.genServerClosed.Store(false)
	c.closed.Store(false)

	ctx, close := context.WithCancel(ctx)
	c.close = close

	switch config.Mode {
	case InfoMode:
		c.internalCommandChannel = make(map[uint32]map[uint16][]chan interface{})
		c.internalCommandChannel[failoverLogID] = make(map[uint16][]chan interface{})
		c.seqMapChannel = make(chan map[uint16]uint64, 1)
		c.seqConnected = make(chan struct{}, 1)
		c.seqMap.Store(make(map[uint16]uint64))
		go c.requestInternalCommands(ctx)

	case StreamRequestMode:
		c.requestChannel = make(chan command, requestThreshold)
		c.requestManager = newRequestManager(c.requestChannel)
		go c.requestRoutine(ctx)

	case MixedMode:
		c.internalCommandChannel = make(map[uint32]map[uint16][]chan interface{})
		c.internalCommandChannel[failoverLogID] = make(map[uint16][]chan interface{})
		c.seqMapChannel = make(chan map[uint16]uint64, 1)
		c.seqConnected = make(chan struct{}, 1)
		c.seqMap.Store(make(map[uint16]uint64))
		c.requestChannel = make(chan command, requestThreshold)
		c.requestManager = newRequestManager(c.requestChannel)
		go c.requestInternalCommands(ctx)
		go c.requestRoutine(ctx)
	}

	c.recvChannel = make(chan *memcachedCommand, recvChannelSize)
	logging.Infof("%s created DCP connection on config: %s", logPrefix, config)
	go c.receiveWorker(ctx)
	go c.genServer(ctx)

	return c
}

func (c *client) TlsSettingsChange(config *notifier.TlsConfig) {
	copyConfig := config.Copy()

	c.Lock()
	defer c.Unlock()

	c.tlsConfig = copyConfig
	c.tcpConn.Close()
	c.unackedBytes = 0
	c.connVersion.Add(1)
	// No need to close the connection. Anyway if its connected then its fine
	// eventually connection will close and new connection will be tried with the updated settings
}

func (c *client) StartStreamReq(sr *StreamReq) error {
	if c.closed.Load() {
		return ErrConnClosed
	}

	sr.opaque = composeOpaqueValue(sr)
	ok := c.requestManager.initRequest(sr)
	if !ok {
		return ErrConnClosed
	}
	return nil
}

func (c *client) PauseStreamReq(sr *StreamReq) {
	sr.opaque = composeOpaqueValue(sr)
	c.requestManager.closeRequest(sr.opaque, false)
}

func (c *client) StopStreamReq(sr *StreamReq) *StreamReq {
	sr.opaque = composeOpaqueValue(sr)
	c.requestManager.closeRequest(sr.opaque, true)
	return nil
}

func (c *client) Wait() error {
	if c.config.Mode != InfoMode {
		return nil
	}

	t := time.NewTicker(maxInternalCommandWait)
	defer t.Stop()

	select {
	case <-t.C:
		return ErrTimeout

	case <-c.seqConnected:
		c.seqConnected <- struct{}{}
	}

	return nil
}

func (c *client) GetSeqNumber(collectionID string) map[uint16]uint64 {
	return c.seqMap.Load().(map[uint16]uint64)
}

func (c *client) GetFailoverLog(vbs []uint16) (failoverLogMap map[uint16]FailoverLog, err error) {
	failoverLogMap = make(map[uint16]FailoverLog)
	failoverChan := make(chan interface{}, len(vbs))

	c.Lock()
	vbMap := c.internalCommandChannel[failoverLogID]
	for _, vb := range vbs {
		recvChannel, ok := vbMap[vb]
		vbMap[vb] = append(recvChannel, failoverChan)
		if ok {
			continue
		}

		opaque := composeOpaque(vb, vb)
		c.failoverlog(opaque, vb)
	}
	c.Unlock()

	t := time.NewTicker(maxInternalCommandWait)
	defer t.Stop()

	for i := 0; i < len(vbs); i++ {
		select {
		case fLogInterfaceArray, ok := <-failoverChan:
			if !ok {
				err = ErrFetchingFailoverLog
				break
			}

			fLogInterface := fLogInterfaceArray.([]interface{})
			vb := fLogInterface[0].(uint16)
			log := fLogInterface[1].(FailoverLog)
			if _, ok := failoverLogMap[vb]; ok {
				i--
				continue
			}
			failoverLogMap[vb] = log

			c.Lock()
			delete(vbMap, vb)
			c.Unlock()

		case <-t.C:
			err = ErrTimeout
			return
		}
	}

	if err != nil {
		c.Lock()
		for _, vb := range vbs {
			delete(vbMap, vb)
		}
		c.Unlock()
	}

	return
}

// Manager doesn't care what all requests ownersip we have
func (c *client) CloseDcpConsumer() []*DcpEvent {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	logPrefix := fmt.Sprintf("dcpConnection::CloseDcpConnection[%s:%v]", c.config.ClientName, c.config.Mode)
	logging.Infof("%s closing dcp connection: %s", logPrefix, c.config)
	c.Lock()
	c.tcpConn.Close()
	c.unackedBytes = 0
	c.connVersion.Add(1)
	c.Unlock()

	c.close()
	// wait for request routine to get closed
	for !c.requestRoutineClosed.Load() {
		time.Sleep(5 * time.Millisecond)
	}

	pendingEvents := c.pendingDcpEvents
	c.pendingDcpEvents = make([]*DcpEvent, 0)
	switch c.config.Mode {
	case StreamRequestMode:
		allRequests := c.requestManager.closeAllRequest(true)
		for _, req := range allRequests {
			dcpMsg := c.createStreamEnd(req)
			pendingEvents = append(pendingEvents, dcpMsg)
		}
	}
	return pendingEvents
}

// Internal functions
func (c *client) requestInternalCommands(ctx context.Context) {
	t := time.NewTicker(seqFetchInterval)
	requested := false
	connected := false

	defer func() {
		t.Stop()
	}()

	for {
		select {
		case <-t.C:
			if requested {
				continue
			}
			c.Lock()
			err := c.highSeqno(bucketSeq, bucketSeq)
			c.Unlock()
			if err != nil {
				continue
			}
			requested = true

		case seqMap := <-c.seqMapChannel:
			requested = false
			if seqMap == nil {
				continue
			}

			if !connected {
				c.seqConnected <- struct{}{}
				connected = true
			}
			c.seqMap.Store(seqMap)

		case <-ctx.Done():
			return
		}
	}
}

func (c *client) requestRoutine(ctx context.Context) {
	logPrefix := fmt.Sprintf("dcpConsumer::requestRoutine[%s]", c.config)
	for {
		select {
		case cmd := <-c.requestChannel:
			req, err := c.handleDcpRequest(cmd)
			if err != nil {
				logging.Errorf("%s handler dcp request error: %v, err: %v for vb: %d. request: %v", logPrefix, cmd, err, cmd.vbno, req)
				time.Sleep(1 * time.Second)
			}
			if err != nil && req != nil {
				// error in creating request
				// generate error message and put it back into genserver response
				mcCommand := &memcachedCommand{}
				mcCommand.createErrorMemcachedCommand(req)
				select {
				case c.recvChannel <- mcCommand:
				case <-ctx.Done():
					return
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

func (c *client) receiveWorker(ctx context.Context) {
	logPrefix := fmt.Sprintf("dcpConnection::receiveWorker[%s:%v]", c.config.ClientName, c.config.Mode)

	var err error
	defer func() {
		select {
		case <-ctx.Done():
			c.requestRoutineClosed.Store(true)
			return
		default:
		}

		c.Lock()
		c.tcpConn = &dummyRWC{}
		c.connVersion.Add(1)
		c.Unlock()

		time.Sleep(time.Second)
		go c.receiveWorker(ctx)
	}()

	if err := c.initConnection(ctx); err != nil {
		logging.Errorf("%s initConnection error: %v", logPrefix, err)
		return
	}
	logging.Infof("%s connection successfully started version: %d", logPrefix, c.connVersion.Load())

	for {
		mcCommand := &memcachedCommand{}
		_, err = mcCommand.receive(c.tcpConn, c.hdrBytes)
		if err != nil {
			logging.Errorf("%s error receiving from socket: %v", logPrefix, err)
			return
		}

		if mcCommand.resCommand == DCP_NOOP {
			c.noopResponse(mcCommand)
			continue
		}

		mcCommand.connVersion = c.connVersion.Load()
		select {
		case c.recvChannel <- mcCommand:
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) genServer(ctx context.Context) {
	logPrefix := fmt.Sprintf("dcpConnection::genServer[%s:%v]", c.config.ClientName, c.config.Mode)

	defer func() {
		c.genServerClosed.Store(true)
		logging.Infof("%s Closing genserver...", logPrefix)
	}()

	for {
		select {
		case msg := <-c.recvChannel:
			dcpEvent, send := c.handlePacket(msg)
			if send {
				select {
				case c.sendChannel <- dcpEvent:
				case <-ctx.Done():
					c.pendingDcpEvents = append(c.pendingDcpEvents, dcpEvent)
					return
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

func (c *client) handleDcpRequest(cmd command) (req *StreamReq, err error) {
	c.Lock()
	defer c.Unlock()

	switch cmd.command {
	case stream_close:
		// Caller asked to close it so request won't be there
		// no need update state to closing
		err = c.streamClose(cmd.id, cmd.opaque, cmd.vbno)

	case stream_request:
		req = c.requestManager.readyRequest(cmd.opaque)
		if req == nil {
			return nil, nil
		}
		err = c.streamRequest(req)
		if err != nil {
			return req, err
		}

	case mixed_request:
		// Send the bulk request at once
	}

	return nil, err
}

func (c *client) handlePacket(res *memcachedCommand) (msg *DcpEvent, send bool) {
	sendAck := false // only for mutations it needs to keep the sendAck to true

	switch res.resCommand {
	case DCP_BUFFERACK:

	case DCP_STREAMREQ:
		msg, send = c.handleStreamRequest(res)

	case DCP_MUTATION, DCP_DELETION,
		DCP_EXPIRATION:
		sendAck = true
		msg = c.handleDcpMessages(res)
		send = c.requestManager.mutationNote(msg)

	case DCP_STREAM_END:
		msg, send = c.handleStreamEnd(res)
		sendAck = true

	case DCP_SNAPSHOT_MARKER:
		sendAck = true

	case DCP_CLOSESTREAM:
		// dcp will send streamend for this request so no need to send this message

	case DCP_SYSTEM_EVENT:
		sendAck = true
		send = true
		msg = c.handleSystemEvent(res)
		send = c.requestManager.mutationNote(msg)

	case DCP_ADV_SEQNUM:
		sendAck = true
		msg = c.handleAdvSeqNumber(res)
		send = c.requestManager.mutationNote(msg)

	case DCP_SEQ_NUMBER:
		c.handleSeqNumber(res)

	case DCP_FAILOVER:
		c.handleFailoverLog(res)

	default:
	}

	c.sendBufferAck(sendAck, res.size, res.connVersion)
	return
}

func (c *client) initConnection(parent context.Context) (err error) {
	switch c.config.Mode {
	case StreamRequestMode:
		allRequests := c.requestManager.closeAllRequest(false)
		for index, req := range allRequests {
			mcCommand := &memcachedCommand{}
			mcCommand.createErrorMemcachedCommand(req)

			select {
			case c.recvChannel <- mcCommand:
			case <-parent.Done():
				// First wait till genserver gets closed. This will make sure we have inorder execution of events
				for !c.genServerClosed.Load() {
					time.Sleep(5 * time.Millisecond)
				}

				// First wait till genserver gets closed. This will make sure we have inorder execution of events
				done := false
				for !done {
					select {
					case cmd := <-c.recvChannel:
						if cmd.resCommand == DCP_STREAMREQ && cmd.vbucket == uint16(UNKNOWN) {
							c.pendingDcpEvents = append(c.pendingDcpEvents, c.createStreamEnd(cmd.req))
						}
					default:
						done = true
					}
				}

				// closing signal. Just start pushing into final pendingMutations
				for _, req := range allRequests[index:] {
					c.pendingDcpEvents = append(c.pendingDcpEvents, c.createStreamEnd(req))
				}
				return ErrConnClosed
			}
		}
	}

	conn, err := c.startConnection(parent)
	if err != nil {
		return
	}

	c.Lock()
	if c.closed.Load() {
		c.Unlock()
		conn.Close()
		return ErrConnClosed
	}
	// Created new connection
	c.unackedBytes = 0
	c.tcpConn = conn
	c.connVersion.Add(1)
	c.Unlock()

	switch c.config.Mode {
	case InfoMode:
		// open up the seq map channel
		select {
		case c.seqMapChannel <- nil:
			c.requestInternalCommand()
		case <-parent.Done():
			return ErrConnClosed
		}
	}
	return
}

func (c *client) requestInternalCommand() {
	if c.config.Mode != InfoMode {
		return
	}

	c.Lock()
	defer c.Unlock()

	// Request failover request again
	failoverRequestMap := c.internalCommandChannel[failoverLogID]
	for vb := range failoverRequestMap {
		opaque := composeOpaque(vb, vb)
		c.failoverlog(opaque, vb)
	}
}

// start the connection and
func (c *client) startConnection(parent context.Context) (conn net.Conn, err error) {
	c.Lock()
	// All new requests are rejected
	c.tcpConn = &dummyRWC{}
	tlsConfig := c.tlsConfig
	c.Unlock()

	defer func() {
		if err != nil && conn != nil {
			conn.Close()
		}
	}()

	conn, err = makeConn(parent, c.config.KvAddress, tlsConfig)
	if err != nil {
		return
	}

	userName, password, _ := authenticator.GetMemcachedServiceAuth(c.config.KvAddress)
	if err = c.saslAuth(conn, userName, password); err != nil {
		return
	}

	if err = c.selectBucket(conn); err != nil {
		return
	}

	flags := uint32(0x00)
	noValue, ok := c.config.DcpConfig[KeyOnly].(bool)
	if ok && noValue {
		flags = flags | noValueFlag
	}

	includeXattr, ok := c.config.DcpConfig[IncludeXattr].(bool)
	if ok && includeXattr {
		flags = flags | includeXATTRFlag
	}

	if err = c.openConnection(conn, flags); err != nil {
		return
	}

	controlRequest := map[string]string{
		"connection_buffer_size":                 strconv.Itoa(connBufferSize),
		"enable_noop":                            enableNoop,
		"set_noop_interval":                      strconv.Itoa(int(setNoopInterval)),
		"enable_stream_id":                       enableStreamID,
		"enable_expiry_opcode":                   enableExpiryOpcode,
		"send_stream_end_on_client_close_stream": sendStreamEndOnClientCloseStream,
		"backfill_order":                         backfillOrder,
	}
	if _, err = c.controlRequest(conn, controlRequest); err != nil {
		return
	}

	heloCommand := []heloCommand{FEATURE_COLLECTIONS, FEATURE_XERROR}
	if err = c.heloCommand(conn, heloCommand); err != nil {
		return
	}
	return
}

func makeConn(parent context.Context, address string, tlsConfig *notifier.TlsConfig) (conn net.Conn, err error) {
	ctx, _ := context.WithCancel(parent)

	if tlsConfig.EncryptData {
		var t tls.Dialer
		t.Config = &tls.Config{RootCAs: tlsConfig.Config.RootCAs.Clone()}
		conn, err = t.DialContext(ctx, "tcp", address)
	} else {
		var d net.Dialer
		conn, err = d.DialContext(ctx, "tcp", address)
	}

	if err != nil {
		err = fmt.Errorf("error opening connection: %v", err)
		return
	}

	return
}

// TODO: Replace bytes with Flow control bytes
func (c *client) sendBufferAck(sendAck bool, bytes uint32, version uint32) {
	if sendAck {
		c.Lock()
		defer c.Unlock()

		if version != c.connVersion.Load() {
			return
		}

		c.unackedBytes = c.unackedBytes + bytes
		if float64(c.unackedBytes)-(float64(connBufferSize)*unackedBytesLimit) > 0 {
			if err := c.bufferAck(c.unackedBytes); err != nil {
				return
			}
			c.unackedBytes = 0
		}
	}
}

func (c *client) getDcpMsgFilled(res *memcachedCommand) (dcpMsg *DcpEvent) {
	msg := c.dcpEventPool.Get()
	dcpMsg = msg.(*DcpEvent)

	dcpMsg.Opcode = res.resCommand
	dcpMsg.Status = status(res.vbucket)
	dcpMsg.opaque = res.opaque
	dcpMsg.Vbno = vbFromOpaque(res.opaque)
	dcpMsg.ID = idFromOpaque(res.opaque)
	dcpMsg.Cas = res.cas

	return
}

func (c *client) handleStreamRequest(res *memcachedCommand) (dcpMsg *DcpEvent, send bool) {
	dcpMsg = c.getDcpMsgFilled(res)
	send = true

	switch dcpMsg.Status {
	case ROLLBACK:
		dcpMsg.Seqno = binary.BigEndian.Uint64(res.body)
		req := c.requestManager.rollbackReqNote(dcpMsg)
		if req == nil {
			return dcpMsg, false
		}
		dcpMsg.fillDcpstreamEnd(req)

	case KEY_EEXISTS:
		send = false
		return

	case SUCCESS:
		var err error
		dcpMsg.FailoverLog, err = parseFailoverLog(res.body)
		if err != nil {
			req, send := c.requestManager.doneRequest(dcpMsg)
			dcpMsg.fillDcpstreamEnd(req)
			return dcpMsg, send
		}
		send = c.requestManager.runningReq(dcpMsg)

	case NOT_MY_VBUCKET, TMPFAIL, ENOMEM:
		req, sendReq := c.requestManager.doneRequest(dcpMsg)
		if !sendReq {
			return dcpMsg, false
		}
		dcpMsg.fillDcpstreamEnd(req)

	case UNKNOWN:
		c.requestManager.doneRequest(dcpMsg)
		dcpMsg.fillDcpstreamEnd(res.req)

	default:
		req, sendReq := c.requestManager.doneRequest(dcpMsg)
		if !sendReq {
			return dcpMsg, false
		}
		dcpMsg.fillDcpstreamEnd(req)
	}
	return
}

func (c *client) handleDcpMessages(res *memcachedCommand) (dcpMsg *DcpEvent) {
	dcpMsg = c.getDcpMsgFilled(res)
	dcpMsg.Datatype = dcpDatatype(res.dataType)
	dcpMsg.Seqno = binary.BigEndian.Uint64(res.extras[:8])

	key, cid := LEB128Dec(res.key)

	if dcpMsg.Opcode == DCP_MUTATION {
		dcpMsg.Expiry = binary.BigEndian.Uint32(res.extras[20:])
	}

	if cap(dcpMsg.Key) > len(key) {
		dcpMsg.Key = dcpMsg.Key[:len(key)]
	} else {
		dcpMsg.Key = make([]byte, len(key))
	}
	copy(dcpMsg.Key, key)
	dcpMsg.CollectionID = cid

	pos := 0
	bodyLength := len(res.body)

	switch {
	case (dcpMsg.Datatype & XATTR) == XATTR:
		xattrLen := int(binary.BigEndian.Uint32(res.body))
		pos = 4
		dcpMsg.SystemXattr = make(map[string]xattrVal)
		dcpMsg.UserXattr = make(map[string]xattrVal)

		for pos < xattrLen {
			pairLen := int(binary.BigEndian.Uint32(res.body[pos:]))
			pos = pos + 4
			binaryPair := res.body[pos : pos+pairLen]
			pos = pos + pairLen
			kvPair := bytes.Split(binaryPair, []byte{0x00})
			if kvPair[0][0] == systemXattrByte {
				dcpMsg.SystemXattr[string(kvPair[0])] = xattrVal{body: kvPair[1]}
				continue
			}
			dcpMsg.UserXattr[string(kvPair[0])] = xattrVal{body: kvPair[1]}
		}
		bodyLength = bodyLength - (4 + xattrLen)
	default:
	}

	if cap(dcpMsg.Value) > bodyLength {
		dcpMsg.Value = dcpMsg.Value[:bodyLength]
	} else {
		dcpMsg.Value = make([]byte, bodyLength)
	}
	copy(dcpMsg.Value, res.body[pos:])
	return
}

func (c *client) handleSystemEvent(res *memcachedCommand) (dcpMsg *DcpEvent) {
	dcpMsg = c.getDcpMsgFilled(res)

	extras := res.extras
	dcpMsg.Seqno = binary.BigEndian.Uint64(extras[0:])

	uid := binary.BigEndian.Uint64(res.body[0:]) //8 byte Manifest UID
	uidstr := strconv.FormatUint(uid, 16)        //convert to base 16 encoded string
	dcpMsg.ManifestUID = uidstr

	systemEventType := collectionEvent(binary.BigEndian.Uint32(extras[8:12]))
	dcpMsg.EventType = systemEventType

	switch systemEventType {
	case COLLECTION_CREATE:
		dcpMsg.ScopeID = binary.BigEndian.Uint32(res.body[8:12])
		dcpMsg.CollectionID = binary.BigEndian.Uint32(res.body[12:16])

	case COLLECTION_DROP, COLLECTION_FLUSH:
		dcpMsg.ScopeID = binary.BigEndian.Uint32(res.body[8:12])
		dcpMsg.CollectionID = binary.BigEndian.Uint32(res.body[12:16])

	case SCOPE_CREATE, SCOPE_DROP:
		dcpMsg.ScopeID = binary.BigEndian.Uint32(res.body[8:12])

	case COLLECTION_CHANGED:
		dcpMsg.CollectionID = binary.BigEndian.Uint32(res.body[8:12])

	default:
	}

	return
}

func (c *client) handleAdvSeqNumber(res *memcachedCommand) (dcpMsg *DcpEvent) {
	dcpMsg = c.getDcpMsgFilled(res)
	dcpMsg.Seqno = binary.BigEndian.Uint64(res.extras)
	return
}

func (c *client) handleStreamEnd(res *memcachedCommand) (dcpMsg *DcpEvent, send bool) {
	dcpMsg = c.getDcpMsgFilled(res)
	dcpMsg.Status = status(binary.BigEndian.Uint32(res.extras))
	_, send = c.requestManager.doneRequest(dcpMsg)
	return
}

func (c *client) handleFailoverLog(res *memcachedCommand) {
	body := res.body
	vb := vbFromOpaque(res.opaque)

	var fLog FailoverLog
	var err error
	errResponse := (status(res.vbucket) != SUCCESS)

	if !errResponse {
		fLog, err = parseFailoverLog(body)
		errResponse = (err != nil)
	}

	c.Lock()
	defer c.Unlock()
	vbMap, ok := c.internalCommandChannel[failoverLogID]
	if !ok {
		return
	}

	recvChannels := vbMap[vb]
	for _, recvChan := range recvChannels {
		if errResponse {
			close(recvChan)
			continue
		}

		recvChan <- []interface{}{vb, fLog}
	}
	delete(vbMap, vb)
}

func (c *client) handleSeqNumber(res *memcachedCommand) {
	body := res.body

	seqMap, err := parseGetSeqnos(body)
	if err != nil {
		c.seqMapChannel <- nil
		return
	}
	c.seqMapChannel <- seqMap
}

func (c *client) createStreamEnd(req *StreamReq) *DcpEvent {
	msg := c.dcpEventPool.Get()
	dcpMsg := msg.(*DcpEvent)
	dcpMsg.Status = FORCED_CLOSED
	dcpMsg.fillDcpstreamEnd(req)
	dcpMsg.Vbno = req.Vbno
	dcpMsg.SrRequest = req

	return dcpMsg
}

func (dcpMsg *DcpEvent) fillDcpstreamEnd(req *StreamReq) {
	dcpMsg.Opcode = DCP_STREAM_END
	dcpMsg.ID = req.ID
	dcpMsg.Version = req.Version
	dcpMsg.SrRequest = req
	dcpMsg.Seqno = req.StartSeq
}

// Internal function to parse different responses
func parseFailoverLog(body []byte) (FailoverLog, error) {
	log := make(FailoverLog, len(body)/16)
	if len(body)%16 != 0 {
		return nil, fmt.Errorf("invalid body length %v, in failover-log", len(body))
	}

	for i, j := 0, 0; i < len(body); i += 16 {
		vuuid := binary.BigEndian.Uint64(body[i : i+8])
		seqno := binary.BigEndian.Uint64(body[i+8 : i+16])
		log[j] = [2]uint64{vuuid, seqno}
		j++
	}
	return log, nil
}

// parse vbno,seqno from response body for get-seqnos.
func parseGetSeqnos(body []byte) (map[uint16]uint64, error) {
	if len(body)%10 != 0 {
		return nil, fmt.Errorf("invalid body length %v, in get-seqnos", len(body))
	}
	seqnos := make(map[uint16]uint64)
	for i := 0; i < len(body); i += 10 {
		vbno := binary.BigEndian.Uint16(body[i : i+2])
		seqno := binary.BigEndian.Uint64(body[i+2 : i+10])
		seqnos[vbno] = seqno
	}
	return seqnos, nil
}

// Decodes the encoded value according to LEB128 uint32 scheme
// Returns the decoded key as byte stream, collectionID as uint32 value
func LEB128Dec(data []byte) ([]byte, uint32) {
	if len(data) == 0 {
		return data, 0
	}

	cid := (uint32)(data[0] & 0x7f)
	end := 1
	if data[0]&0x80 == 0x80 {
		shift := 7
		for end = 1; end < len(data); end++ {
			cid |= ((uint32)(data[end]&0x7f) << (uint32)(shift))
			if data[end]&0x80 == 0 {
				break
			}
			shift += 7
		}
		end++
	}
	return data[end:], cid
}

// These function will create the opaque to identify the request
// Top 16 bits will contain the id and bottom 16 bits will have the
// vbucket number
func composeOpaqueValue(sr *StreamReq) uint32 {
	return composeOpaque(sr.ID, sr.Vbno)
}

func composeOpaque(id, vbno uint16) uint32 {
	return (uint32(id) << 16) | uint32(vbno)
}

func idFromOpaque(opaque uint32) uint16 {
	return uint16(opaque >> 16)
}

func vbFromOpaque(opaque uint32) uint16 {
	return uint16((opaque & 0xFFFF))
}
