package dcpConn

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"strings"
)

const (
	opaqueNoop    = 0x00000005
	opaqueOpen    = 0x00000002
	opaqueAck     = 0x00000000
	opaqueControl = 0x00000001
)

const (
	openConnFlag      = uint32(0x1)
	includeXATTRFlag  = uint32(0x4)
	noValueFlag       = uint32(0x8)
	includeDeleteTime = uint32(0x20)
)

const (
	streamIDFlexLength = byte(0x22)
)

const (
	bucketSeq = uint32(math.MaxUint32 - 1)
)

// Caller should make sure clientName is under 256 bytes
func (c *client) openConnection(conn net.Conn, flags uint32) error {
	rq := &memcachedCommand{
		msgType: req_magic,
		opcode:  dcp_open,
		key:     []byte(c.config.ClientName),
		opaque:  opaqueOpen,
	}

	rq.extras = make([]byte, 8)
	flags = flags | openConnFlag | includeDeleteTime
	binary.BigEndian.PutUint32(rq.extras[4:], flags) // we are consumer

	if _, err := rq.transmit(conn); err != nil {
		return err
	}

	_, err := rq.receive(conn, c.hdrBytes)
	if err != nil {
		return err
	}

	if rq.resCommand != DCP_OPEN_CONN {
		return fmt.Errorf("expected DCP_OPEN command. Got: %v", rq.resCommand)
	}

	if status(rq.vbucket) != SUCCESS {
		return fmt.Errorf("expected Success for dcp open. Got: %v", rq.vbucket)
	}

	return nil
}

func (c *client) authList(conn net.Conn) (string, error) {
	rq := &memcachedCommand{
		msgType: req_magic,
		opcode:  dcp_sasl_auth_list,
	}

	if _, err := rq.transmit(conn); err != nil {
		return "", err
	}

	_, err := rq.receive(conn, c.hdrBytes)
	if err != nil {
		return "", err
	}

	if rq.resCommand != SASL_AUTH_LIST {
		return "", fmt.Errorf("expected SASL_AUTH_LIST. Got: %v", rq.resCommand)
	}

	if status(rq.vbucket) != SUCCESS {
		return "", fmt.Errorf("expected success for authlist. Got: %v", status(rq.vbucket))
	}

	return string(rq.body), nil
}

func (c *client) saslAuth(conn net.Conn, user, passwd string) error {
	authMech, err := c.authList(conn)
	if err != nil {
		return err
	}

	// If plain then send the plain authentication
	if strings.Index(authMech, "PLAIN") == -1 {
		return fmt.Errorf("auth mechanism PLAIN not supported. Supported: %s", authMech)
	}

	rq := &memcachedCommand{
		msgType: req_magic,
		opcode:  dcp_sasl_auth,
		key:     []byte("PLAIN"),
		body:    []byte(fmt.Sprintf("\x00%s\x00%s", user, passwd)),
	}

	if _, err := rq.transmit(conn); err != nil {
		return err
	}

	if _, err = rq.receive(conn, c.hdrBytes); err != nil {
		return err
	}

	if rq.resCommand != DCP_SASL_AUTH {
		return fmt.Errorf("expected DCP_SASL_AUTH. Got: %v", rq.resCommand)
	}

	if status(rq.vbucket) != SUCCESS {
		return fmt.Errorf("expected success for sasl auth. Got: %v", status(rq.vbucket))
	}

	return nil
}

func (c *client) selectBucket(conn net.Conn) error {
	rq := &memcachedCommand{
		msgType: req_magic,
		opcode:  select_bucket,
		key:     []byte(c.config.BucketName),
	}

	if _, err := rq.transmit(conn); err != nil {
		return err
	}

	_, err := rq.receive(conn, c.hdrBytes)
	if err != nil {
		return err
	}

	if rq.resCommand != DCP_SELECT_BUCKET {
		return fmt.Errorf("expected DCP_SELECT_BUCKET command. Got: %v", rq.resCommand)
	}

	if status(rq.vbucket) != SUCCESS {
		// TODO: Check for status code. If any error then return errBucketNotFound
		return fmt.Errorf("expected Success for select bucket. Got: %v", rq.vbucket)
	}

	return nil
}

func (c *client) heloCommand(conn net.Conn, flags []heloCommand) error {
	rq := &memcachedCommand{
		msgType: req_magic,
		opcode:  dcp_helo,
		key:     []byte(fmt.Sprintf("Dcp hello message from: %s", c.config.ClientName)),
	}

	rq.body = make([]byte, len(flags)*2)
	for index, flag := range flags {
		binary.BigEndian.PutUint16(rq.body[index*2:], uint16(flag))
	}

	if _, err := rq.transmit(conn); err != nil {
		return err
	}

	rq.body = rq.body[:0]
	rq.key = rq.key[:0]

	_, err := rq.receive(conn, c.hdrBytes)
	if err != nil {
		return err
	}

	if rq.resCommand != DCP_HELO {
		return fmt.Errorf("expected DCP_SELECT_BUCKET command. Got: %v", rq.resCommand)
	}

	if status(rq.vbucket) != SUCCESS {
		return fmt.Errorf("expected Success for helo command. Got: %v", rq.vbucket)
	}

	// Need to check the body cause all the features might not be enabled by the dcp
	return nil
}

// 2nd return value returns error if there is any error while sending or receiving from socket
// Caller should restart the connection if such things happen
func (c *client) controlRequest(conn net.Conn, request map[string]string) (map[string]error, error) {
	rq := &memcachedCommand{
		msgType: req_magic,
		opcode:  dcp_control,
		opaque:  opaqueControl,
	}

	res := &memcachedCommand{}

	notAllowed := make(map[string]error)
	for key, value := range request {
		rq.key = []byte(key)
		rq.body = []byte(value)

		if _, err := rq.transmit(conn); err != nil {
			notAllowed[key] = err
			return nil, err
		}

		if _, err := res.receive(conn, c.hdrBytes); err != nil {
			notAllowed[key] = err
			return nil, err
		}

		if res.resCommand != DCP_CONTROL {
			notAllowed[key] = fmt.Errorf("expected DCP_CONTROL. Got: %v", res.resCommand)
		}

		if status(res.vbucket) != SUCCESS {
			notAllowed[key] = fmt.Errorf("expected SUCCESS for control request. Got: %v", status(res.vbucket))
			continue
		}
		res.reset()
	}
	return notAllowed, nil
}

type streamRequestValue struct {
	UID      string   `json:"uid,omitempty"`
	ScopeID  string   `json:"scope,omitempty"`
	ColIDs   []string `json:"collections,omitempty"`
	StreamID uint16   `json:"sid,omitempty"`
}

func (sr StreamReq) getStreamRequestValue() ([]byte, error) {
	srv := streamRequestValue{
		StreamID: sr.ID,
		UID:      sr.ManifestUID,
	}

	if sr.ManifestUID == "" {
		srv.UID = "0"
	}

	switch sr.RequestType {
	case Request_Bucket:

	case Request_Scope:
		srv.ScopeID = sr.ScopeID

	case Request_Collections:
		srv.ColIDs = sr.CollectionIDs
	}
	return json.Marshal(srv)
}

const (
	ActiveOnly          = uint32(0x10)
	TillLatest          = uint32(0x04)
	IgnorePurgeRollback = uint32(0x80)
)

// Non blocking calls
func (c *client) streamRequest(sr *StreamReq) error {
	rq := &memcachedCommand{
		msgType: req_magic,
		opcode:  dcp_streamreq,
		vbucket: sr.Vbno,
		opaque:  sr.opaque,
	}
	rq.extras = make([]byte, 48) // #Extras
	binary.BigEndian.PutUint32(rq.extras, sr.Flags|ActiveOnly|IgnorePurgeRollback)
	binary.BigEndian.PutUint64(rq.extras[8:], sr.StartSeq)
	binary.BigEndian.PutUint64(rq.extras[16:], sr.EndSeq)
	binary.BigEndian.PutUint64(rq.extras[24:], sr.Vbuuid)
	binary.BigEndian.PutUint64(rq.extras[32:], sr.StartSeq)
	binary.BigEndian.PutUint64(rq.extras[40:], sr.StartSeq)

	var err error
	rq.body, err = sr.getStreamRequestValue()
	if err != nil {
		return fmt.Errorf("error creating request value: %v", err)
	}

	if _, err = rq.transmit(c.tcpConn); err != nil {
		return err
	}
	return nil
}

// If streamID is not specified then it will error out
func (c *client) streamClose(streamID uint16, opaque uint32, vbno uint16) error {
	rq := &memcachedCommand{
		msgType: frame_extras,
		opcode:  dcp_closestream,
		vbucket: vbno,
		opaque:  opaque,
		fExtras: make([]byte, 3),
	}

	rq.fExtras[0] = streamIDFlexLength
	binary.BigEndian.PutUint16(rq.fExtras[1:], streamID)

	if _, err := rq.transmit(c.tcpConn); err != nil {
		return err
	}

	return nil
}

func (c *client) failoverlog(opaque uint32, vbno uint16) error {
	rq := &memcachedCommand{
		msgType: req_magic,
		opcode:  dcp_failoverlog,
		vbucket: vbno,
		opaque:  opaque,
	}

	if _, err := rq.transmit(c.tcpConn); err != nil {
		return err
	}

	return nil
}

// fetches high seq number for the active vb
func (c *client) highSeqno(opaque uint32, collectionID uint32) error {
	rq := &memcachedCommand{
		msgType: req_magic,
		opcode:  dcp_seqnum,
		opaque:  opaque,
	}

	if collectionID != bucketSeq {
		rq.extras = make([]byte, 8)
	} else {
		rq.extras = make([]byte, 4)
	}

	binary.BigEndian.PutUint32(rq.extras[0:4], 1) // Only active vbuckets
	if collectionID != bucketSeq {
		binary.BigEndian.PutUint32(rq.extras[4:], collectionID)
	}

	if _, err := rq.transmit(c.tcpConn); err != nil {
		return err
	}

	return nil
}

// Add commands for get/set/delete/subdoc inserts
func (c *client) GetKey(req *memcachedCommand) error {
	req.msgType = res_magic
	req.opcode = dcp_get_key

	c.Lock()
	defer c.Unlock()

	if _, err := req.transmit(c.tcpConn); err != nil {
		return err
	}

	return nil
}

func (c *client) SetKey(req *memcachedCommand) error {
	req.msgType = res_magic
	req.opcode = dcp_set_key

	c.Lock()
	defer c.Unlock()

	if _, err := req.transmit(c.tcpConn); err != nil {
		return err
	}

	return nil
}

func (c *client) DeleteKey(req *memcachedCommand) error {
	req.msgType = res_magic
	req.opcode = dcp_delete_key

	c.Lock()
	defer c.Unlock()

	if _, err := req.transmit(c.tcpConn); err != nil {
		return err
	}

	return nil
}

// Internal callbacks
// Need to hold the lock while sending it over network
func (c *client) bufferAck(totalBytes uint32) error {
	rq := &memcachedCommand{
		msgType: req_magic,
		opcode:  dcp_bufferack,
		opaque:  opaqueAck,
	}

	rq.extras = make([]byte, 4)
	binary.BigEndian.PutUint32(rq.extras[:4], uint32(totalBytes))

	if _, err := rq.transmit(c.tcpConn); err != nil {
		return err
	}

	return nil
}

func (c *client) noopResponse(req *memcachedCommand) error {
	req.msgType = res_magic
	req.opcode = dcp_noop

	c.Lock()
	defer c.Unlock()
	if _, err := req.transmit(c.tcpConn); err != nil {
		return err
	}

	return nil
}
