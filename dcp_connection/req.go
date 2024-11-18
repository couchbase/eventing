package dcpConn

import (
	"encoding/binary"
	"fmt"
	"io"
	// "log"
)

type internalCommandCode uint8

const (
	dcp_no_request     = internalCommandCode(0x00)
	dcp_seqnum         = internalCommandCode(0x48)
	dcp_open           = internalCommandCode(0x50) // Open a DCP connection with a name
	select_bucket      = internalCommandCode(0x89) // Select bucket
	dcp_addstream      = internalCommandCode(0x51) // DCP stream addition
	dcp_closestream    = internalCommandCode(0x52) // Sent by eBucketMigrator to DCP Consumer
	dcp_failoverlog    = internalCommandCode(0x54) // Request failover logs
	dcp_streamreq      = internalCommandCode(0x53) // Stream request from consumer to producer
	dcp_bufferack      = internalCommandCode(0x5d) // DCP Buffer Acknowledgement
	dcp_control        = internalCommandCode(0x5e) // Set flow control params
	dcp_noop           = internalCommandCode(0x5c) // dcp noop
	dcp_helo           = internalCommandCode(0x1f) // dcp helo msg
	dcp_sasl_auth      = internalCommandCode(0x21) // sasl auth
	dcp_sasl_auth_list = internalCommandCode(0x20) // sasl list

	dcp_get_key         = internalCommandCode(0x0c) // get single key
	dcp_get_key_quietly = internalCommandCode(0x0d) // get key quietly

	dcp_set_key     = internalCommandCode(0x01) // set single key
	dcp_set_quietly = internalCommandCode(0x11) // set key quietly

	dcp_delete_key         = internalCommandCode(0x04) // delete single key
	dcp_delete_key_quietly = internalCommandCode(0x14) // delete key quietly
)

const hdr_len = 24

type magic uint8

const (
	// Default to request magic
	default_magic    = magic(0x00)
	req_magic        = magic(0x80)
	res_magic        = magic(0x81)
	frame_extras     = magic(0x08)
	frame_extras_res = magic(0x18)
)

type memcachedCommand struct {
	connVersion uint32
	msgType     magic
	opcode      internalCommandCode
	resCommand  resCommandCode
	dataType    uint8
	vbucket     uint16 // this field holds the vbucket number in the request and the KV response code in the response
	opaque      uint32
	cas         uint64

	fExtras, key, body, extras []byte
	size                       uint32

	req *StreamReq
}

func (req *memcachedCommand) reset() {
	req.msgType = default_magic
	req.connVersion = 0
	req.opcode = dcp_no_request
	req.resCommand = DCP_NO_RES
	req.dataType = 0
	req.vbucket = 0
	req.opaque = 0
	req.cas = 0
	req.fExtras = req.fExtras[:0]
	req.key = req.key[:0]
	req.body = req.body[:0]
	req.size = 0
}

func (req *memcachedCommand) createErrorMemcachedCommand(res *StreamReq) {
	req.vbucket = uint16(UNKNOWN)
	req.resCommand = DCP_STREAMREQ
	req.opaque = res.opaque
	req.req = res
}

func (req *memcachedCommand) fillBytes(withBody bool) (data []byte) {
	fExtraLength, keyLength, bodyLength, extraLength := uint32(len(req.fExtras)), uint16(len(req.key)), uint32(len(req.body)), uint8(len(req.extras))
	totalBodyLength := fExtraLength + uint32(keyLength) + bodyLength + uint32(extraLength)

	data = make([]byte, 0, totalBodyLength+hdr_len)
	magicVal := req.msgType
	if magicVal == default_magic {
		magicVal = req_magic
	}

	data = append(data, byte(magicVal))

	data = append(data, byte(req.opcode))

	switch magicVal {
	case req_magic, res_magic:
		data = binary.BigEndian.AppendUint16(data, keyLength)

	case frame_extras, frame_extras_res:
		data = append(data, byte(fExtraLength), byte(keyLength))
	}

	// 4
	data = append(data, byte(extraLength))

	// data[pos] = 0
	data = append(data, byte(0x0))

	data = binary.BigEndian.AppendUint16(data, req.vbucket)

	// 8
	data = binary.BigEndian.AppendUint32(data, totalBodyLength)

	// 12
	data = binary.BigEndian.AppendUint32(data, req.opaque)

	// 16
	data = binary.BigEndian.AppendUint64(data, req.cas)

	if fExtraLength > 0 {
		data = append(data, req.fExtras...)
	}

	if extraLength > 0 {
		data = append(data, req.extras...)
	}

	if keyLength > 0 {
		data = append(data, req.key...)
	}

	if withBody && bodyLength > 0 {
		data = append(data, req.body...)
	}

	return
}

func (req *memcachedCommand) Bytes() []byte {
	return req.fillBytes(true)
}

func (req *memcachedCommand) HeaderBytes() []byte {
	return req.fillBytes(false)
}

func (req *memcachedCommand) transmit(w io.Writer) (n int, err error) {
	if len(req.body) < 128 {
		n, err = w.Write(req.Bytes())
	} else {
		n, err = w.Write(req.HeaderBytes())
		if err == nil {
			m := 0
			m, err = w.Write(req.body)
			n += m
		}
	}
	if err != nil {
		return n, fmt.Errorf("error transmitting bytes: %v", err)
	}
	return
}

func (req *memcachedCommand) receive(r io.Reader, hdrBytes []byte) (int, error) {
	if len(hdrBytes) < hdr_len {
		hdrBytes = make([]byte, 24)
	}

	n, err := io.ReadFull(r, hdrBytes)
	if err != nil {
		return n, fmt.Errorf("error receiving bytes: %v", err)
	}

	req.msgType = magic(hdrBytes[0])
	req.resCommand = resCommandCode(hdrBytes[1])

	klen, flen := 0, 0
	switch req.msgType {
	case req_magic, res_magic:
		klen = int(binary.BigEndian.Uint16(hdrBytes[2:]))

	case frame_extras, frame_extras_res:
		flen = 3
		// TODO: For delete op flexible frame length is 0
		// flen = int(hdrBytes[2])
		klen = int(hdrBytes[3])

	default:
		return n, fmt.Errorf("bad magic: 0x%02x", hdrBytes[0])
	}

	elen := int(hdrBytes[4])
	req.dataType = uint8(hdrBytes[5])

	req.vbucket = binary.BigEndian.Uint16(hdrBytes[6:])
	bodyLen := int(binary.BigEndian.Uint32(hdrBytes[8:]))

	req.opaque = binary.BigEndian.Uint32(hdrBytes[12:])
	req.cas = binary.BigEndian.Uint64(hdrBytes[16:])

	buf := make([]byte, bodyLen)
	m, err := io.ReadFull(r, buf)
	n += m
	if err != nil {
		req.size = uint32(n)
		return n, fmt.Errorf("error receiving bytes: %v", err)
	}
	// This fExtras contains extra info like streamID
	// https://github.com/couchbase/kv_engine/blob/master/docs/BinaryProtocol.md#request-header-with-flexible-framing-extras
	req.fExtras = buf[:flen]
	req.extras = buf[flen : flen+elen]
	req.key = buf[flen+elen : flen+elen+klen]
	req.body = buf[flen+elen+klen:]
	req.size = uint32(n)
	return n, nil
}
