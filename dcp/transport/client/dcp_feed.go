// go implementation of dcp client.
// See https://github.com/couchbaselabs/cbupr/blob/master/transport-spec.md

package memcached

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/eventing/common/collections"
	"github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/logging"
	"io"
	"strconv"
	"sync/atomic"
	"time"
)

const dcpMutationExtraLen = 16
const bufferAckThreshold = 0.1
const opaqueOpen = 0xBEAF0001
const opaqueFailover = 0xDEADBEEF
const opaqueGetseqno = 0xDEADBEEF
const openConnFlag = uint32(0x1)
const bufferAckPeriod = 20
const includeDeleteTime = uint32(0x20)
const dcpSeqnoAdvExtrasLen = 8

var kvSeparator = []byte{0x00}

var TransactionMutationPrefix = []byte("_txn:")

const SystemXattrPrefix = "_"

const IncludeXATTRs = uint32(4)

// error codes
var ErrorInvalidLog = errors.New("couchbase.errorInvalidLog")

// ErrorConnection
var ErrorConnection = errors.New("dcp.connection")

// ErrorInvalidFeed
var ErrorInvalidFeed = errors.New("dcp.invalidFeed")

// ErrorEnableCollections
var ErrorEnableCollections = errors.New("dcp.EnableCollections")
var ErrorCollectionsNotEnabled = errors.New("dcp.ErrorCollectionsNotEnabled")

// DcpFeed represents an DCP feed. A feed contains a connection to a single
// host and multiple vBuckets
type DcpFeed struct {
	conn      *Client // connection to DCP producer
	name      string
	outch     chan<- *DcpEvent      // Exported channel for receiving DCP events
	vbstreams map[uint16]*DcpStream // vb->stream mapping
	// genserver
	reqch     chan []interface{}
	finch     chan bool
	logPrefix string
	// stats
	toAckBytes         uint32    // bytes client has read
	maxAckBytes        uint32    // Max buffer control ack bytes
	lastAckTime        time.Time // last time when BufferAck was sent
	stats              DcpStats  // Stats for dcp client
	dcplatency         *Average
	enableReadDeadline int32 // 0 => Read deadline is disabled in doReceive, 1 => enabled

	collectionAware bool // Check if all kv nodes are above version 7
}

// NewDcpFeed creates a new DCP Feed.
func NewDcpFeed(
	mc *Client, name string, outch chan<- *DcpEvent,
	opaque uint16, config map[string]interface{}) (*DcpFeed, error) {

	genChanSize := config["genChanSize"].(int)
	dataChanSize := config["dataChanSize"].(int)
	feed := &DcpFeed{
		name:      name,
		outch:     outch,
		vbstreams: make(map[uint16]*DcpStream),
		reqch:     make(chan []interface{}, genChanSize),
		finch:     make(chan bool),
		// TODO: would be nice to add host-addr as part of prefix.
		logPrefix:  fmt.Sprintf("DCPT[%s]", name),
		dcplatency: &Average{},
	}

	feed.collectionAware = config["collectionAware"].(bool)
	mc.Hijack()
	feed.conn = mc
	rcvch := make(chan []interface{}, dataChanSize)
	feed.lastAckTime = time.Now()
	go feed.genServer(opaque, feed.reqch, feed.finch, rcvch, config)
	go feed.doReceive(rcvch, feed.finch, mc)
	logging.Infof("%v ##%x feed started ... collectionAware: %v", feed.logPrefix, opaque, feed.collectionAware)
	return feed, nil
}

func (feed *DcpFeed) Name() string {
	return feed.name
}

// DcpOpen to connect with a DCP producer.
// Name: name of te DCP connection
// sequence: sequence number for the connection
// bufsize: max size of the application
func (feed *DcpFeed) DcpOpen(
	name string, sequence, flags, bufsize uint32, opaque uint16) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{
		dfCmdOpen, name, sequence, flags, bufsize, opaque, respch,
	}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	return opError(err, resp, 0)
}

// DcpGetFailoverLog for given list of vbuckets.
func (feed *DcpFeed) DcpGetFailoverLog(
	opaque uint16, vblist []uint16) (map[uint16]*FailoverLog, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{dfCmdGetFailoverlog, opaque, vblist, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	if err = opError(err, resp, 1); err != nil {
		return nil, err
	}
	return resp[0].(map[uint16]*FailoverLog), err
}

// DcpGetSeqnos for vbuckets hosted by this node.
func (feed *DcpFeed) DcpGetSeqnos() (map[uint16]uint64, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{dfCmdGetSeqnos, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	if err = opError(err, resp, 1); err != nil {
		return nil, err
	}
	return resp[0].(map[uint16]uint64), nil
}

// DcpRequestStream for a single vbucket.
func (feed *DcpFeed) DcpRequestStream(vbno, opaqueMSB uint16, flags uint32,
	vuuid, startSequence, endSequence, snapStart, snapEnd uint64,
	manifestUID, scopeId, collectionId string) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{
		dfCmdRequestStream, vbno, opaqueMSB, flags, vuuid,
		startSequence, endSequence, snapStart, snapEnd,
		manifestUID, scopeId, collectionId,
		respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	return opError(err, resp, 0)
}

// CloseStream for specified vbucket.
func (feed *DcpFeed) CloseStream(vbno, opaqueMSB uint16) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{dfCmdCloseStream, vbno, opaqueMSB, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	return opError(err, resp, 0)
}

// Close this DcpFeed.
func (feed *DcpFeed) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{dfCmdClose, respch}
	_, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	return err
}

const (
	dfCmdOpen byte = iota + 1
	dfCmdGetFailoverlog
	dfCmdGetSeqnos
	dfCmdRequestStream
	dfCmdCloseStream
	dfCmdClose
)

func (feed *DcpFeed) genServer(
	opaque uint16, reqch chan []interface{}, finch chan bool,
	rcvch chan []interface{},
	config map[string]interface{}) {

	defer func() { // panic safe
		if r := recover(); r != nil {
			logging.Errorf("%v ##%x crashed: %v\n", feed.logPrefix, opaque, r)
			logging.Errorf("%s", logging.StackTrace())
			feed.sendStreamEnd(feed.outch)
		}
		close(feed.finch)
		feed.conn.Close()
		logging.Infof("%v ##%x ... stopped\n", feed.logPrefix, opaque)
	}()

	latencyTick := int64(60 * 1000) // in milli-seconds
	if val, ok := config["latencyTick"]; ok && val != nil {
		latencyTick = int64(val.(int)) // in milli-seconds
	}
	latencyTm := time.NewTicker(time.Duration(latencyTick) * time.Millisecond)
	defer func() {
		latencyTm.Stop()
	}()

loop:
	for {
		select {
		case <-latencyTm.C:

			fmsg := "%v dcp latency stats %v\n"
			logging.Debugf(fmsg, feed.logPrefix, feed.dcplatency)
			fmsg = "%v dcp stats %v\n"
			logging.Debugf(fmsg, feed.logPrefix, feed.stats.String(feed))

		case msg := <-reqch:
			if feed.handleControlRequest(msg, rcvch, nil) == "break" {
				break loop
			}

		case resp, ok := <-rcvch:
			if !ok {
				feed.sendStreamEnd(feed.outch)
				break loop
			}
			pkt, bytes := resp[0].(*transport.MCRequest), resp[1].(int)
			feed.handlePacket(pkt, bytes, reqch, rcvch)
		}
	}
}

func (feed *DcpFeed) handleControlRequest(
	msg []interface{}, rcvch chan []interface{}, event *DcpEvent) string {

	prefix := feed.logPrefix
	cmd := msg[0].(byte)
	switch cmd {
	case dfCmdOpen:
		name, sequence := msg[1].(string), msg[2].(uint32)
		flags := msg[3].(uint32)
		bufsize, opaque := msg[4].(uint32), msg[5].(uint16)
		respch := msg[6].(chan []interface{})
		err := feed.doDcpOpen(name, sequence, flags, bufsize, opaque, rcvch)
		respch <- []interface{}{err}

	case dfCmdGetFailoverlog:
		opaque := msg[1].(uint16)
		vblist, respch := msg[2].([]uint16), msg[3].(chan []interface{})
		if len(feed.vbstreams) > 0 {
			fmsg := "%v %##x active streams in doDcpGetFailoverLog"
			logging.Errorf(fmsg, prefix, opaque)
			respch <- []interface{}{nil, ErrorInvalidFeed}
		}
		flog, err := feed.doDcpGetFailoverLog(opaque, vblist, rcvch)
		respch <- []interface{}{flog, err}

	case dfCmdGetSeqnos:
		respch := msg[1].(chan []interface{})
		seqnos, err := feed.doDcpGetSeqnos(rcvch)
		respch <- []interface{}{seqnos, err}

	case dfCmdRequestStream:
		vbno, opaqueMSB := msg[1].(uint16), msg[2].(uint16)
		flags, vuuid := msg[3].(uint32), msg[4].(uint64)
		startSequence, endSequence := msg[5].(uint64), msg[6].(uint64)
		snapStart, snapEnd := msg[7].(uint64), msg[8].(uint64)

		manifestUID := msg[9].(string)
		scopeId := msg[10].(string)
		collectionId := msg[11].(string)

		respch := msg[12].(chan []interface{})
		err := feed.doDcpRequestStream(
			vbno, opaqueMSB, flags, vuuid,
			startSequence, endSequence, snapStart, snapEnd,
			manifestUID, scopeId, collectionId)
		respch <- []interface{}{err}

	case dfCmdCloseStream:
		vbno, opaqueMSB := msg[1].(uint16), msg[2].(uint16)
		respch := msg[3].(chan []interface{})
		err := feed.doDcpCloseStream(vbno, opaqueMSB)
		respch <- []interface{}{err}

	case dfCmdClose:
		feed.sendStreamEnd(feed.outch)
		respch := msg[1].(chan []interface{})
		if event != nil && event.Opcode == transport.DCP_STREAMEND {
			feed.outch <- event
		}
		respch <- []interface{}{nil}
		return "break"
	}
	return "ok"
}

func (feed *DcpFeed) isClosed() bool {
	select {
	case <-feed.finch:
		return true
	default:
	}
	return false
}

func (feed *DcpFeed) handlePacket(
	pkt *transport.MCRequest, bytes int,
	reqch, rcvch chan []interface{}) string {

	var event *DcpEvent
	feed.stats.TotalBytes += uint64(bytes)
	res := &transport.MCResponse{
		Opcode: pkt.Opcode,
		Cas:    pkt.Cas,
		Opaque: pkt.Opaque,
		Status: transport.Status(pkt.VBucket),
		Extras: pkt.Extras,
		Key:    pkt.Key,
		Body:   pkt.Body,
	}
	vb := vbOpaque(pkt.Opaque)

	sendAck := false
	prefix := feed.logPrefix

	stream := feed.vbstreams[vb]
	if stream == nil {
		fmsg := "%v spurious %v for %d: %ru\n"
		arg1 := logging.TagUD(pkt)
		logging.Fatalf(fmsg, prefix, pkt.Opcode, vb, fmt.Sprintf("%#v", arg1))
		return "ok" // yeah it not _my_ mistake...
	}

	defer func() { feed.dcplatency.Add(computeLatency(stream)) }()

	stream.LastSeen = time.Now().UnixNano()
	switch pkt.Opcode {
	case transport.DCP_STREAMREQ:
		event = newDcpEvent(pkt, stream)
		feed.handleStreamRequest(res, vb, stream, event)
		feed.stats.TotalStreamReq++

	case transport.DCP_MUTATION, transport.DCP_DELETION,
		transport.DCP_EXPIRATION:
		event = newDcpEvent(pkt, stream)
		stream.Seqno = event.Seqno
		feed.stats.TotalMutation++
		sendAck = true

	case transport.DCP_STREAMEND:
		event = newDcpEvent(pkt, stream)
		sendAck = true
		delete(feed.vbstreams, vb)
		fmsg := "%v ##%x DCP_STREAMEND for vb %d\n"
		logging.Debugf(fmsg, prefix, stream.AppOpaque, vb)
		feed.stats.TotalStreamEnd++

	case transport.DCP_SNAPSHOT:
		event = newDcpEvent(pkt, stream)
		event.SnapstartSeq = binary.BigEndian.Uint64(pkt.Extras[0:8])
		event.SnapendSeq = binary.BigEndian.Uint64(pkt.Extras[8:16])
		event.SnapshotType = binary.BigEndian.Uint32(pkt.Extras[16:20])
		stream.Snapstart = event.SnapstartSeq
		stream.Snapend = event.SnapendSeq
		feed.stats.TotalSnapShot++
		sendAck = true
		if (stream.Snapend - stream.Snapstart) > 50000 {
			fmsg := "%v ##%x DCP_SNAPSHOT for vb %d snapshot {%v,%v}\n"
			logging.Infof(
				fmsg, prefix, stream.AppOpaque, vb, stream.Snapstart,
				stream.Snapend,
			)
		}
		fmsg := "%v ##%x DCP_SNAPSHOT for vb %d\n"
		logging.Debugf(fmsg, prefix, stream.AppOpaque, vb)

	case transport.DCP_FLUSH:
		event = newDcpEvent(pkt, stream) // special processing ?

	case transport.DCP_CLOSESTREAM:
		// since send_stream_end_on_client_close_stream is set in the
		// control message. we will ignore the close_stream and wait for
		// stream_end instead.
		event = newDcpEvent(pkt, stream)
		if event.Opaque != stream.CloseOpaque {
			fmsg := "%v ##%x DCP_CLOSESTREAM mismatch in opaque %v != %v\n"
			logging.Fatalf(
				fmsg, prefix, stream.AppOpaque, event.Opaque, stream.CloseOpaque,
			)
		}
		event = nil // IMPORTANT: make sure to nil event.
		fmsg := "%v ##%x DCP_CLOSESTREAM for vb %d\n"
		logging.Infof(fmsg, prefix, stream.AppOpaque, vb)
		feed.stats.TotalCloseStream++

	case transport.DCP_CONTROL, transport.DCP_BUFFERACK:
		if res.Status != transport.SUCCESS {
			fmsg := "%v ##%x opcode %v received status %v\n"
			logging.Errorf(
				fmsg, prefix, stream.AppOpaque, pkt.Opcode, res.Status,
			)
		}

	case transport.DCP_ADDSTREAM:
		fmsg := "%v ##%x opcode DCP_ADDSTREAM not implemented\n"
		logging.Fatalf(fmsg, prefix, stream.AppOpaque)

	case transport.DCP_SYSTEM_EVENT:
		event = newDcpEvent(pkt, stream)
		feed.handleDCPSystemEvent(pkt, event)
		stream.Seqno = event.Seqno
		sendAck = true

	case transport.DCP_SEQNO_ADVANCED:
		event = newDcpEvent(pkt, stream)
		sendAck = true
		if len(pkt.Extras) == dcpSeqnoAdvExtrasLen {
			event.Seqno = binary.BigEndian.Uint64(pkt.Extras)
		} else {
			fmsg := "%v ##%x DCP_SEQNO_ADVANCED for vb %d. Expected extras len: %v, received: %v\n"
			logging.Fatalf(fmsg, prefix, stream.AppOpaque, vb, dcpSeqnoAdvExtrasLen, len(pkt.Extras))
		}

	default:
		fmsg := "%v opcode %v not known for vbucket %d\n"
		logging.Warnf(fmsg, prefix, pkt.Opcode, vb)
	}

	if event != nil {
		feed.outch <- event
	}
	feed.sendBufferAck(sendAck, uint32(bytes))
	return "ok"
}

func (feed *DcpFeed) handleDCPSystemEvent(pkt *transport.MCRequest, dcpEvent *DcpEvent) {
	extras := pkt.Extras
	dcpEvent.Seqno = binary.BigEndian.Uint64(extras[0:8])
	uid := binary.BigEndian.Uint64(pkt.Body[0:8]) //8 byte Manifest UID
	uidstr := strconv.FormatUint(uid, 16)         //convert to base 16 encoded string
	dcpEvent.ManifestUID = []byte(uidstr)

	systemEventType := transport.CollectionEvent(binary.BigEndian.Uint32(extras[8:12]))
	dcpEvent.EventType = systemEventType

	switch systemEventType {

	case transport.COLLECTION_CREATE:
		dcpEvent.ScopeID = binary.BigEndian.Uint32(pkt.Body[8:12])
		dcpEvent.CollectionID = binary.BigEndian.Uint32(pkt.Body[12:16])

	case transport.COLLECTION_DROP, transport.COLLECTION_FLUSH:
		dcpEvent.ScopeID = binary.BigEndian.Uint32(pkt.Body[8:12])
		dcpEvent.CollectionID = binary.BigEndian.Uint32(pkt.Body[12:16])

	case transport.SCOPE_CREATE, transport.SCOPE_DROP:
		dcpEvent.ScopeID = binary.BigEndian.Uint32(pkt.Body[8:12])

	case transport.COLLECTION_CHANGED:
		dcpEvent.CollectionID = binary.BigEndian.Uint32(pkt.Body[8:12])

	default:
	}

}

func (feed *DcpFeed) doDcpGetFailoverLog(
	opaque uint16,
	vblist []uint16,
	rcvch chan []interface{}) (map[uint16]*FailoverLog, error) {

	// Enable read deadline at function exit
	// As this method is called only once (i.e. at the start of DCP feed),
	// it is ok to set the value of readDeadline to "1" and not reset it later
	defer func() {
		atomic.StoreInt32(&feed.enableReadDeadline, 1)
		feed.conn.SetMcdMutationReadDeadline()
	}()

	rq := &transport.MCRequest{
		Opcode: transport.DCP_FAILOVERLOG,
		Opaque: opaqueFailover,
	}
	failoverLogs := make(map[uint16]*FailoverLog)
	for _, vBucket := range vblist {
		rq.VBucket = vBucket

		var msg []interface{}
		err1 := func() error {
			feed.conn.SetMcdConnectionDeadline()
			defer feed.conn.ResetMcdConnectionDeadline()

			if err := feed.conn.Transmit(rq); err != nil {
				fmsg := "%v ##%x doDcpGetFailoverLog.Transmit(): %v"
				logging.Errorf(fmsg, feed.logPrefix, opaque, err)
				return err
			}

			var ok bool
			msg, ok = <-rcvch
			if !ok {
				fmsg := "%v ##%x doDcpGetFailoverLog.rcvch closed"
				logging.Errorf(fmsg, feed.logPrefix, opaque)
				return ErrorConnection
			}

			return nil
		}()

		if err1 != nil {
			return nil, err1
		}

		pkt := msg[0].(*transport.MCRequest)
		req := &transport.MCResponse{
			Opcode: pkt.Opcode,
			Cas:    pkt.Cas,
			Opaque: pkt.Opaque,
			Status: transport.Status(pkt.VBucket),
			Extras: pkt.Extras,
			Key:    pkt.Key,
			Body:   pkt.Body,
		}
		if req.Opcode != transport.DCP_FAILOVERLOG {
			fmsg := "%v ##%x for failover log request unexpected #opcode %v"
			logging.Errorf(fmsg, feed.logPrefix, opaque, req.Opcode)
			return nil, ErrorInvalidFeed

		} else if req.Status != transport.SUCCESS {
			fmsg := "%v ##%x for failover log request unexpected #status %v"
			logging.Errorf(fmsg, feed.logPrefix, opaque, req.Status)
			return nil, ErrorInvalidFeed
		}
		flog, err := parseFailoverLog(req.Body)
		if err != nil {
			fmsg := "%v ##%x parse failover logs for vb %d"
			logging.Errorf(fmsg, feed.logPrefix, opaque, vBucket)
			return nil, ErrorInvalidFeed
		}
		failoverLogs[vBucket] = flog
	}
	return failoverLogs, nil
}

func (feed *DcpFeed) doDcpGetSeqnos(
	rcvch chan []interface{}) (map[uint16]uint64, error) {

	// Enable read deadline at function exit
	// As this method is called only once (i.e. at the start of DCP feed),
	// it is ok to set the value of readDeadline to "1" and not reset it later
	defer func() {
		atomic.StoreInt32(&feed.enableReadDeadline, 1)
		feed.conn.SetMcdMutationReadDeadline()
	}()

	rq := &transport.MCRequest{
		Opcode: transport.DCP_GET_SEQNO,
		Opaque: opaqueGetseqno,
	}

	rq.Extras = make([]byte, 4)
	binary.BigEndian.PutUint32(rq.Extras, 1) // Only active vbuckets

	feed.conn.SetMcdConnectionDeadline()
	defer feed.conn.ResetMcdConnectionDeadline()

	if err := feed.conn.Transmit(rq); err != nil {
		fmsg := "%v ##%x doDcpGetSeqnos.Transmit(): %v"
		logging.Errorf(fmsg, feed.logPrefix, rq.Opaque, err)
		return nil, err
	}
	msg, ok := <-rcvch
	if !ok {
		fmsg := "%v ##%x doDcpGetSeqnos.rcvch closed"
		logging.Errorf(fmsg, feed.logPrefix, rq.Opaque)
		return nil, ErrorConnection
	}
	pkt := msg[0].(*transport.MCRequest)
	req := &transport.MCResponse{
		Opcode: pkt.Opcode,
		Cas:    pkt.Cas,
		Opaque: pkt.Opaque,
		Status: transport.Status(pkt.VBucket),
		Extras: pkt.Extras,
		Key:    pkt.Key,
		Body:   pkt.Body,
	}
	if req.Opcode != transport.DCP_GET_SEQNO {
		fmsg := "%v ##%x for get-seqno request unexpected #opcode %v"
		logging.Errorf(fmsg, feed.logPrefix, req.Opaque, req.Opcode)
		return nil, ErrorInvalidFeed

	} else if req.Status != transport.SUCCESS {
		fmsg := "%v ##%x for get-seqno request unexpected #status %v"
		logging.Errorf(fmsg, feed.logPrefix, req.Opaque, req.Status)
		return nil, ErrorInvalidFeed
	}
	seqnos, err := parseGetSeqnos(req.Body)
	if err != nil {
		fmsg := "%v ##%x parsing get-seqnos: %v"
		logging.Errorf(fmsg, feed.logPrefix, req.Opaque, err)
		return nil, ErrorInvalidFeed
	}
	return seqnos, nil
}

func (feed *DcpFeed) doDcpOpen(
	name string, sequence, flags, bufsize uint32,
	opaque uint16,
	rcvch chan []interface{}) error {

	prefix := feed.logPrefix

	// Enable read deadline at function exit
	// As this method is called only once (i.e. at the start of DCP feed),
	// it is ok to set the value of readDeadline to "1" and not reset it later
	defer func() {
		atomic.StoreInt32(&feed.enableReadDeadline, 1)
		feed.conn.SetMcdMutationReadDeadline()
	}()

	feed.conn.SetMcdConnectionDeadline()
	defer feed.conn.ResetMcdConnectionDeadline()

	if feed.collectionAware {
		if err := feed.enableCollections(rcvch); err != nil {
			return err
		}
	}

	rq := &transport.MCRequest{
		Opcode: transport.DCP_OPEN,
		Key:    []byte(name),
		Opaque: opaqueOpen,
	}
	rq.Extras = make([]byte, 8)
	flags = flags | openConnFlag | includeDeleteTime
	binary.BigEndian.PutUint32(rq.Extras[:4], sequence)
	binary.BigEndian.PutUint32(rq.Extras[4:], flags) // we are consumer

	if err := feed.conn.Transmit(rq); err != nil {
		return err
	}
	msg, ok := <-rcvch
	if !ok {
		logging.Errorf("%v ##%x doDcpOpen.rcvch closed", prefix, opaque)
		return ErrorConnection
	}
	pkt := msg[0].(*transport.MCRequest)
	req := &transport.MCResponse{
		Opcode: pkt.Opcode,
		Cas:    pkt.Cas,
		Opaque: pkt.Opaque,
		Status: transport.Status(pkt.VBucket),
		Extras: pkt.Extras,
		Key:    pkt.Key,
		Body:   pkt.Body,
	}
	if req.Opcode != transport.DCP_OPEN {
		logging.Errorf("%v ##%x unexpected #%v", prefix, opaque, req.Opcode)
		return ErrorConnection
	} else if rq.Opaque != req.Opaque {
		fmsg := "%v ##%x opaque mismatch, %v != %v"
		logging.Errorf(fmsg, prefix, opaque, req.Opaque, req.Opaque)
		return ErrorConnection
	} else if req.Status != transport.SUCCESS {
		fmsg := "%v ##%x doDcpOpen response status %v"
		logging.Errorf(fmsg, prefix, opaque, req.Status)
		return ErrorConnection
	}

	// send a DCP control message to set the window size for
	// this connection
	if bufsize > 0 {
		if err := feed.doControlRequest(opaque, "connection_buffer_size", []byte(strconv.Itoa(int(bufsize))), rcvch); err != nil {
			return err
		}
		feed.maxAckBytes = uint32(bufferAckThreshold * float32(bufsize))
	}

	// send a DCP control message to enable_noop
	if true /*enable_noop*/ {
		if err := feed.doControlRequest(opaque, "enable_noop", []byte("true"), rcvch); err != nil {
			return err
		}
	}

	// send a DCP control message to set_noop_interval
	if true /*set_noop_interval*/ {
		if err := feed.doControlRequest(opaque, "set_noop_interval", []byte("120"), rcvch); err != nil {
			return err
		}
	}

	if true /*send_stream_end_on_client_close_stream*/ {
		if err := feed.doControlRequest(opaque, "send_stream_end_on_client_close_stream", []byte("true"), rcvch); err != nil {
			return err
		}
	}

	// Send Dcp control message to enable expiry code
	if true {
		if err := feed.doControlRequest(opaque, "enable_expiry_opcode", []byte("true"), rcvch); err != nil {
			return err
		}
	}
	return nil
}

func (feed *DcpFeed) doControlRequest(opaque uint16, key string, value []byte, rcvch chan []interface{}) error {
	prefix := feed.logPrefix

	rq := &transport.MCRequest{
		Opcode: transport.DCP_CONTROL,
		Key:    []byte(key),
		Body:   value,
	}
	if err := feed.conn.Transmit(rq); err != nil {
		fmsg := "%v ##%x doDcpOpen.Transmit(%s): %v"
		logging.Errorf(fmsg, prefix, key, opaque, err)
		return err
	}
	logging.Debugf("%v ##%x sending %s", prefix, opaque, key)
	msg, ok := <-rcvch
	if !ok {
		fmsg := "%v ##%x doDcpOpen.rcvch (%s) closed"
		logging.Errorf(fmsg, prefix, opaque, key)
		return ErrorConnection
	}
	pkt := msg[0].(*transport.MCRequest)
	opcode, status := pkt.Opcode, transport.Status(pkt.VBucket)
	if opcode != transport.DCP_CONTROL {
		fmsg := "%v ##%x DCP_CONTROL (%s) != #%v"
		logging.Errorf(fmsg, prefix, opaque, key, opcode)
		return ErrorConnection
	} else if status != transport.SUCCESS {
		fmsg := "%v ##%x doDcpOpen (%s) response status %v"
		logging.Errorf(fmsg, prefix, opaque, key, status)
		return ErrorConnection
	}
	fmsg := "%v ##%x received response %s"
	logging.Debugf(fmsg, prefix, opaque, key)
	return nil
}

func (feed *DcpFeed) doDcpRequestStream(
	vbno, opaqueMSB uint16, flags uint32,
	vuuid, startSequence, endSequence, snapStart, snapEnd uint64,
	manifestUID, scopeId, collectionId string) error {

	rq := &transport.MCRequest{
		Opcode:  transport.DCP_STREAMREQ,
		VBucket: vbno,
		Opaque:  composeOpaque(vbno, opaqueMSB),
	}
	rq.Extras = make([]byte, 48) // #Extras
	binary.BigEndian.PutUint32(rq.Extras[:4], flags)
	binary.BigEndian.PutUint32(rq.Extras[4:8], uint32(0))
	binary.BigEndian.PutUint64(rq.Extras[8:16], startSequence)
	binary.BigEndian.PutUint64(rq.Extras[16:24], endSequence)
	binary.BigEndian.PutUint64(rq.Extras[24:32], vuuid)
	binary.BigEndian.PutUint64(rq.Extras[32:40], snapStart)
	binary.BigEndian.PutUint64(rq.Extras[40:48], snapEnd)

	prefix := feed.logPrefix

	requestValue := &StreamRequestValue{}
	if feed.collectionAware {
		if collectionId != "" {
			requestValue.CollectionIDs = []string{collectionId}
		} else if scopeId != "" {
			requestValue.ScopeID = scopeId
		}

		requestValue.ManifestUID = manifestUID
		body, _ := json.Marshal(requestValue)
		rq.Body = body
	}

	feed.conn.SetMcdConnectionWriteDeadline()
	defer feed.conn.ResetMcdConnectionWriteDeadline()

	if err := feed.conn.Transmit(rq); err != nil {
		fmsg := "%v ##%x doDcpRequestStream.Transmit(): %v"
		logging.Errorf(fmsg, prefix, opaqueMSB, err)
		return err
	}
	stream := &DcpStream{
		AppOpaque:        opaqueMSB,
		Vbucket:          vbno,
		Vbuuid:           vuuid,
		StartSeq:         startSequence,
		EndSeq:           endSequence,
		collectionsAware: feed.collectionAware,
	}
	feed.vbstreams[vbno] = stream
	return nil
}

func (feed *DcpFeed) doDcpCloseStream(vbno, opaqueMSB uint16) error {
	prefix := feed.logPrefix
	stream, ok := feed.vbstreams[vbno]
	if !ok || stream == nil {
		fmsg := "%v ##%x stream for vb %d is not active"
		logging.Warnf(fmsg, prefix, opaqueMSB, vbno)
		return nil // TODO: should we return error here ?
	}
	stream.CloseOpaque = opaqueMSB
	rq := &transport.MCRequest{
		Opcode:  transport.DCP_CLOSESTREAM,
		VBucket: vbno,
		Opaque:  composeOpaque(vbno, opaqueMSB),
	}
	if err := feed.conn.Transmit(rq); err != nil {
		fmsg := "%v ##%x (##%x) doDcpCloseStream.Transmit(): %v"
		logging.Errorf(fmsg, prefix, opaqueMSB, stream.AppOpaque, err)
		return err
	}
	return nil
}

func (feed *DcpFeed) enableCollections(rcvch chan []interface{}) error {
	prefix := feed.logPrefix

	rq := &transport.MCRequest{
		Opcode: transport.HELO,
		Key:    []byte(feed.name),
		Body:   []byte{0x00, transport.FEATURE_COLLECTIONS},
	}
	if err := feed.conn.Transmit(rq); err != nil {
		fmsg := "%v doDcpOpen.Transmit DCP_HELO (feature_collections): %v"
		logging.Errorf(fmsg, prefix, err)
		return err
	}
	msg, ok := <-rcvch
	if !ok {
		fmsg := "%v doDcpOpen.rcvch (feature_collections) closed"
		logging.Errorf(fmsg, prefix)
		return ErrorConnection
	}

	pkt := msg[0].(*transport.MCRequest)
	opcode, body := pkt.Opcode, pkt.Body
	if opcode != transport.HELO {
		fmsg := "%v DCP_HELO (feature_collections) opcode = %v. Expecting opcode = 0x1f"
		logging.Errorf(fmsg, prefix, opcode)
		return ErrorEnableCollections
	} else if (len(body) != 2) || (body[0] != 0x00 && body[1] != transport.FEATURE_COLLECTIONS) {
		fmsg := "%v DCP_HELO (feature_collections) body = %v. Expecting body = 0x0012"
		logging.Errorf(fmsg, prefix, opcode)
		return ErrorCollectionsNotEnabled
	}

	fmsg := "%v received response for DCP_HELO (feature_collections)"
	logging.Infof(fmsg, prefix)
	return nil
}

// generate stream end responses for all active vb streams
func (feed *DcpFeed) sendStreamEnd(outch chan<- *DcpEvent) {
	if feed.vbstreams != nil {
		for vb, stream := range feed.vbstreams {
			dcpEvent := &DcpEvent{
				VBucket: vb,
				VBuuid:  stream.Vbuuid,
				Opcode:  transport.DCP_STREAMEND,
				Opaque:  stream.AppOpaque,
				Ctime:   time.Now().UnixNano(),
			}
			outch <- dcpEvent
		}
		feed.vbstreams = nil
	}
}

func (feed *DcpFeed) handleStreamRequest(
	res *transport.MCResponse, vb uint16, stream *DcpStream, event *DcpEvent) {

	prefix := feed.logPrefix
	switch {
	case res.Status == transport.ROLLBACK && len(res.Body) != 8:
		event.Status, event.Seqno = res.Status, 0
		fmsg := "%v ##%x STREAMREQ(%v) invalid rollback: %v\n"
		arg1 := logging.TagUD(res.Body)
		logging.Errorf(fmsg, prefix, stream.AppOpaque, vb, arg1)
		delete(feed.vbstreams, vb)

	case res.Status == transport.ROLLBACK:
		rollback := binary.BigEndian.Uint64(res.Body)
		event.Status, event.Seqno = res.Status, rollback
		fmsg := "%v ##%x STREAMREQ(%v) with rollback %d\n"
		logging.Debugf(fmsg, prefix, stream.AppOpaque, vb, rollback)
		delete(feed.vbstreams, vb)

	case res.Status == transport.SUCCESS:
		event.Status, event.Seqno = res.Status, stream.StartSeq
		flog, err := parseFailoverLog(res.Body[:])
		if err != nil {
			fmsg := "%v ##%x STREAMREQ(%v) parseFailoverLog: %v\n"
			logging.Errorf(fmsg, prefix, stream.AppOpaque, vb, err)
		}
		event.FailoverLog = flog
		stream.connected = true
		fmsg := "%v ##%x STREAMREQ(%d) successful\n"
		logging.Debugf(fmsg, prefix, stream.AppOpaque, vb)

	default:
		event.Status = res.Status
		event.VBucket = vb
		fmsg := "%v ##%x STREAMREQ(%v) unexpected status: %v\n"
		logging.Debugf(fmsg, prefix, stream.AppOpaque, vb, res.Status)
		delete(feed.vbstreams, vb)
	}
	return
}

// Send buffer ack
func (feed *DcpFeed) sendBufferAck(sendAck bool, bytes uint32) {
	prefix := feed.logPrefix
	if sendAck {
		totalBytes := feed.toAckBytes + bytes
		if totalBytes > feed.maxAckBytes || time.Since(feed.lastAckTime).Seconds() > bufferAckPeriod {
			bufferAck := &transport.MCRequest{
				Opcode: transport.DCP_BUFFERACK,
			}
			bufferAck.Extras = make([]byte, 4)
			binary.BigEndian.PutUint32(bufferAck.Extras[:4], uint32(totalBytes))
			feed.stats.TotalBufferAckSent++
			func() {
				// Timeout here will unblock genServer() thread after some time.
				feed.conn.SetMcdConnectionWriteDeadline()
				defer feed.conn.ResetMcdConnectionWriteDeadline()

				if err := feed.conn.Transmit(bufferAck); err != nil {
					logging.Errorf("%v buffer-ack Transmit(): %v, lastAckTime: %v", prefix, err, feed.lastAckTime.UnixNano())
				} else {
					// Reset the counters only on a successful BufferAck
					feed.toAckBytes = 0
					feed.lastAckTime = time.Now()
					feed.stats.LastAckTime = feed.lastAckTime.UnixNano()
					logging.Tracef("%v buffer-ack %v, lastAckTime: %v\n", prefix, totalBytes, feed.lastAckTime.UnixNano())
				}
			}()
		} else {
			feed.toAckBytes += bytes
		}
	}
}

func composeOpaque(vbno, opaqueMSB uint16) uint32 {
	return (uint32(opaqueMSB) << 16) | uint32(vbno)
}

func appOpaque(opq32 uint32) uint16 {
	return uint16((opq32 & 0xFFFF0000) >> 16)
}

func vbOpaque(opq32 uint32) uint16 {
	return uint16(opq32 & 0xFFFF)
}

type StreamRequestValue struct {
	ManifestUID   string   `json:"uid,omitempty"`
	CollectionIDs []string `json:"collections,omitempty"`
	ScopeID       string   `json:"scope,omitempty"`
}

// DcpStream is per stream data structure over an DCP Connection.
type DcpStream struct {
	AppOpaque        uint16
	CloseOpaque      uint16
	Vbucket          uint16 // Vbucket id
	Vbuuid           uint64 // vbucket uuid
	Seqno            uint64
	StartSeq         uint64 // start sequence number
	EndSeq           uint64 // end sequence number
	Snapstart        uint64
	Snapend          uint64
	LastSeen         int64 // UnixNano value of last seen
	connected        bool
	collectionsAware bool
}

// DcpEvent memcached events for DCP streams.
type DcpEvent struct {
	Opcode       transport.CommandCode // Type of event
	Status       transport.Status      // Response status
	Datatype     uint8                 // Datatype per binary protocol
	VBucket      uint16                // VBucket this event applies to
	Opaque       uint16                // 16 MSB of opaque
	VBuuid       uint64                // This field is set by downstream
	Key, Value   []byte                // Item key/value
	OldValue     []byte                // TODO: TBD: old document value
	Cas          uint64                // CAS value of the item
	CollectionID uint32                // Collection Id
	ScopeID      uint32
	Xattr        map[string]XattrVal // Xattr map

	EventType transport.CollectionEvent // For DCP_SYSTEM_EVENT, DCP_OSO_SNAPSHOT types
	// meta fields
	Seqno uint64 // seqno. of the mutation, doubles as rollback-seqno
	// https://issues.couchbase.com/browse/MB-15333,
	RevSeqno uint64
	Flags    uint32 // Item flags
	Expiry   uint32 // Item expiration time
	LockTime uint32
	Nru      byte
	// snapshots
	SnapstartSeq uint64 // start sequence number of this snapshot
	SnapendSeq   uint64 // End sequence number of the snapshot
	SnapshotType uint32 // 0: disk 1: memory
	// collections
	ManifestUID []byte // For DCP_SYSTEM_EVENT
	// failoverlog
	FailoverLog *FailoverLog // Failover log containing vvuid and sequnce number
	Error       error        // Error value in case of a failure
	// stats
	Ctime int64
}

// Include the XATTR in the request
type XattrVal struct {
	body []byte
}

func (x XattrVal) MarshalJSON() ([]byte, error) {
	return x.body, nil
}

func (x XattrVal) Bytes() []byte {
	return x.body
}

func newDcpEvent(rq *transport.MCRequest, stream *DcpStream) *DcpEvent {
	event := &DcpEvent{
		Cas:      rq.Cas,
		Datatype: rq.Datatype,
		Opcode:   rq.Opcode,
		VBucket:  stream.Vbucket,
		VBuuid:   stream.Vbuuid,
		Ctime:    time.Now().UnixNano(),
		Xattr:    make(map[string]XattrVal),
	}

	docId := rq.Key
	if stream.collectionsAware {
		switch event.Opcode {
		case transport.DCP_MUTATION, transport.DCP_DELETION, transport.DCP_EXPIRATION:
			docId, event.CollectionID = collections.LEB128Dec(rq.Key)
		}
	}

	event.Key = make([]byte, len(docId))
	copy(event.Key, docId)
	event.Value = make([]byte, len(rq.Body))
	copy(event.Value, rq.Body)

	if rq.Datatype&uint8(IncludeXATTRs) == uint8(IncludeXATTRs) {
		xattrsLen := int(binary.BigEndian.Uint32(event.Value))
		pos := 4

		for pos < xattrsLen {
			pairLen := int(binary.BigEndian.Uint32(event.Value[pos:]))
			pos = pos + 4
			binaryPair := event.Value[pos : pos+pairLen]
			pos = pos + pairLen
			kvPair := bytes.Split(binaryPair, kvSeparator)
			event.Xattr[string(kvPair[0])] = XattrVal{body: kvPair[1]}
		}

		event.Value = event.Value[pos:]
	}

	// 16 LSBits are used by client library to encode vbucket number.
	// 16 MSBits are left for application to multiplex on opaque value.
	event.Opaque = appOpaque(rq.Opaque)

	if len(rq.Extras) >= tapMutationExtraLen {
		event.Seqno = binary.BigEndian.Uint64(rq.Extras[:8])
		switch event.Opcode {
		case transport.DCP_MUTATION:
			event.RevSeqno = binary.BigEndian.Uint64(rq.Extras[8:])
			event.Flags = binary.BigEndian.Uint32(rq.Extras[16:])
			event.Expiry = binary.BigEndian.Uint32(rq.Extras[20:])
			event.LockTime = binary.BigEndian.Uint32(rq.Extras[24:])
			event.Nru = rq.Extras[30]

		case transport.DCP_DELETION:
			event.RevSeqno = binary.BigEndian.Uint64(rq.Extras[8:])

		case transport.DCP_EXPIRATION:
			event.RevSeqno = binary.BigEndian.Uint64(rq.Extras[8:])
		}

	} else if len(rq.Extras) >= tapMutationExtraLen &&
		event.Opcode == transport.DCP_SNAPSHOT {

		event.SnapstartSeq = binary.BigEndian.Uint64(rq.Extras[:8])
		event.SnapendSeq = binary.BigEndian.Uint64(rq.Extras[8:16])
		event.SnapshotType = binary.BigEndian.Uint32(rq.Extras[16:20])
	}

	return event
}

func (event *DcpEvent) String() string {
	name := transport.CommandNames[event.Opcode]
	if name == "" {
		name = fmt.Sprintf("#%d", event.Opcode)
	}
	return name
}

// DcpStats on mutations/snapshots/buff-acks.
type DcpStats struct {
	TotalBufferAckSent uint64
	TotalBytes         uint64
	TotalCloseStream   uint64
	TotalMutation      uint64
	TotalSnapShot      uint64
	TotalStreamReq     uint64
	TotalStreamEnd     uint64
	LastAckTime        int64
}

func (stats *DcpStats) String(feed *DcpFeed) string {
	return fmt.Sprintf(
		"bytes: %v buffacks: %v toAckBytes: %v streamreqs: %v "+
			"snapshots: %v mutations: %v streamends: %v closestreams: %v"+
			"lastAckTime: %v",
		stats.TotalBytes, stats.TotalBufferAckSent, feed.toAckBytes,
		stats.TotalStreamReq, stats.TotalSnapShot, stats.TotalMutation,
		stats.TotalStreamEnd, stats.TotalCloseStream, stats.LastAckTime,
	)
}

// FailoverLog containing vvuid and sequnce number
type FailoverLog [][2]uint64

const (
	MaxFailoverLogEntries = 20
)

// Kind of start of the vbucket
func InitFailoverLog() FailoverLog {
	return FailoverLog{[2]uint64{uint64(0), uint64(0)}}
}

// Latest will return the recent vbuuid and its high-seqno.
func (flogp *FailoverLog) Latest() (vbuuid, seqno uint64, err error) {
	if flogp != nil {
		flog := *flogp
		if len(flog) == 0 {
			return vbuuid, seqno, nil
		}

		latest := flog[0]
		return latest[0], latest[1], nil
	}
	return vbuuid, seqno, ErrorInvalidLog
}

// Trim failover log to hold MaxFailoverLogEntries entries
// Caller should make sure that flog is not nill
func (flog *FailoverLog) trimFailoverLog() {
	// This is to make sure flog always contain atleast one entry
	if len(*flog) == 0 {
		*flog = InitFailoverLog()
		return
	}

	if len(*flog) <= MaxFailoverLogEntries {
		return
	}

	tmpFlog := *flog
	log := make(FailoverLog, MaxFailoverLogEntries)
	for i := 0; i < MaxFailoverLogEntries-1; i++ {
		log[i] = tmpFlog[i]
	}
	*flog = log
}

func (flogp *FailoverLog) PopAndGetLatest() (vbuuid, seqno uint64, err error) {
	if flogp == nil {
		return vbuuid, seqno, ErrorInvalidLog
	}

	if len(*flogp) == 0 {
		*flogp = InitFailoverLog()
	} else {
		*flogp = (*flogp)[1:]
	}

	flogp.trimFailoverLog()
	return flogp.Latest()
}

func (flogp *FailoverLog) PopTillSeqNo(desiredSeqNo uint64) (vbuuid, seqno uint64, err error) {
	if flogp == nil {
		return vbuuid, seqno, ErrorInvalidLog
	}

	tmpFlog := *flogp
	entry := 0
	for ; entry < len(tmpFlog) && tmpFlog[entry][1] > desiredSeqNo; entry++ {
	}

	log := make(FailoverLog, 0, len(tmpFlog)-entry)
	for ; entry < len(tmpFlog); entry++ {
		log = append(log, tmpFlog[entry])
	}

	*flogp = log
	flogp.trimFailoverLog()
	return flogp.Latest()
}

// failsafeOp can be used by gen-server implementors to avoid infinitely
// blocked API calls.
func failsafeOp(
	reqch, respch chan []interface{},
	cmd []interface{},
	finch chan bool) ([]interface{}, error) {

	select {
	case reqch <- cmd:
		if respch != nil {
			select {
			case resp := <-respch:
				return resp, nil
			case <-finch:
				return nil, ErrorConnection
			}
		}
	case <-finch:
		return nil, ErrorConnection
	}
	return nil, nil
}

// opError suppliments FailsafeOp used by gen-servers.
func opError(err error, vals []interface{}, idx int) error {
	if err != nil {
		return err
	} else if vals[idx] == nil {
		return nil
	}
	return vals[idx].(error)
}

// parse failover log fields from response body.
func parseFailoverLog(body []byte) (*FailoverLog, error) {
	if len(body)%16 != 0 {
		fmsg := "invalid body length %v, in failover-log\n"
		err := fmt.Errorf(fmsg, len(body))
		return nil, err
	}
	log := make(FailoverLog, len(body)/16)
	for i, j := 0, 0; i < len(body); i += 16 {
		vuuid := binary.BigEndian.Uint64(body[i : i+8])
		seqno := binary.BigEndian.Uint64(body[i+8 : i+16])
		log[j] = [2]uint64{vuuid, seqno}
		j++
	}
	log.trimFailoverLog()
	return &log, nil
}

// parse vbno,seqno from response body for get-seqnos.
func parseGetSeqnos(body []byte) (map[uint16]uint64, error) {
	if len(body)%10 != 0 {
		fmsg := "invalid body length %v, in get-seqnos\n"
		err := fmt.Errorf(fmsg, len(body))
		return nil, err
	}
	seqnos := make(map[uint16]uint64)
	for i := 0; i < len(body); i += 10 {
		vbno := binary.BigEndian.Uint16(body[i : i+2])
		seqno := binary.BigEndian.Uint64(body[i+2 : i+10])
		seqnos[vbno] = seqno
	}
	return seqnos, nil
}

func computeLatency(stream *DcpStream) int64 {
	now := time.Now().UnixNano()
	strm_seqno := stream.Seqno
	if stream.Snapend == 0 || strm_seqno == stream.Snapend {
		return 0
	}
	delta := now - stream.LastSeen
	stream.LastSeen = now
	return delta
}

// receive loop
func (feed *DcpFeed) doReceive(
	rcvch chan []interface{}, finch chan bool, conn *Client) {
	defer close(rcvch)

	var headerBuf [transport.HDR_LEN]byte
	var duration time.Duration
	var start time.Time
	var blocked bool

	epoc := time.Now()
	tick := time.NewTicker(time.Second * 5) // log every 5 second, if blocked.
	defer func() {
		tick.Stop()
	}()

	var bytes int
	var err error
	// Cached version of feed.readDeadline
	// Used inorder to prevent atomic access to feed.readDeadline in every iteration of "loop"
	var enableReadDeadline bool

	prefix := feed.logPrefix
loop:
	for {
		pkt := transport.MCRequest{} // always a new instance.

		// Enable read deadline only when doDcpOpen or doDcpGetFailoverLog or doDcpGetDeqnos
		// finish execution as these methods require a different read deadline
		if !enableReadDeadline {
			enableReadDeadline = (atomic.LoadInt32(&feed.enableReadDeadline) == 1)
			bytes, err = pkt.Receive(conn.conn, headerBuf[:])
		} else {
			// In cases where client misses TCP notificatons like connection closure,
			// the dcp_feed would continuously wait for data
			// In such scenarios, the read deadline would terminate the blocking read
			// calls thereby unblocking the dcp_feed. This is only a safe-guard measure
			// to handle highly unlikely scenarios like missing TCP notifications. In
			// those cases, an "i/o timeout" error will be returned to the reader.
			conn.SetMcdMutationReadDeadline()
			bytes, err = pkt.Receive(conn.conn, headerBuf[:])
			// Resetting the read deadline here is unnecessary because doReceive() is the
			// only thread reading data from conn after read deadline is enabled.
			// If reset does not happen here, then the next SetMcdMutationReadDeadline
			// would update the deadline
			// conn.ResetMcdMutationReadDeadline()
		}

		// Immediately respond to NOOP and listen for next message.
		// NOOPs are not accounted for buffer-ack.
		if pkt.Opcode == transport.DCP_NOOP {
			fmsg := "%v Opaque: %d received NOOP from producer ...\n"
			logging.Tracef(fmsg, prefix, pkt.Opaque)

			noop := &transport.MCResponse{
				Opcode: transport.DCP_NOOP, Opaque: pkt.Opaque,
			}
			func() {
				// Timeout here will unblock doReceive() thread after some time.
				conn.SetMcdConnectionDeadline()
				defer conn.ResetMcdConnectionDeadline()

				if err := conn.TransmitResponse(noop); err != nil {
					logging.Errorf("%v NOOP.Transmit(): %v", feed.logPrefix, err)
				} else {
					fmsg := "%v responded to NOOP ok ...\n"
					logging.Tracef(fmsg, feed.logPrefix)
				}
			}()
			continue loop
		}

		if err != nil && err == io.EOF {
			logging.Infof("%v EOF received\n", feed.logPrefix)
			break loop

		} else if feed.isClosed() {
			logging.Infof("%v doReceive(): connection closed\n", feed.logPrefix)
			break loop

		} else if err != nil {
			logging.Errorf("%v doReceive(): %v\n", feed.logPrefix, err)
			break loop
		}
		if len(rcvch) == cap(rcvch) {
			start, blocked = time.Now(), true
		}
		select {
		case rcvch <- []interface{}{&pkt, bytes}:
		case <-finch:
			break loop
		}
		if blocked {
			blockedTs := time.Since(start)
			duration += blockedTs
			blocked = false
			select {
			case <-tick.C:
				percent := float64(duration) / float64(time.Since(epoc))
				fmsg := "%v DCP-socket -> eventing blocked %v (%f%%)"
				logging.Infof(fmsg, feed.logPrefix, blockedTs, percent)
			default:
			}
		}
	}
}
