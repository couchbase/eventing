package transport

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/couchbase/eventing/logging"
)

// MCRequest is memcached Request
type MCRequest struct {
	// The command being issued
	Opcode CommandCode
	// The CAS (if applicable, or 0)
	Cas uint64
	// Datatype as per binary protocol
	Datatype uint8
	// An opaque value to be returned with this request
	Opaque uint32
	// The vbucket to which this command belongs
	VBucket uint16
	// Command extras, key, and body
	Extras, Key, Body []byte
}

// Size gives the number of bytes this request requires.
func (req *MCRequest) Size() int {
	return HDR_LEN + len(req.Extras) + len(req.Key) + len(req.Body)
}

// A debugging string representation of this request
func (req MCRequest) String() string {
	return fmt.Sprintf("{MCRequest opcode=%s, bodylen=%d, key='%s'}",
		req.Opcode, len(req.Body), req.Key)
}

func (req *MCRequest) fillHeaderBytes(data []byte) int {

	pos := 0
	data[pos] = REQ_MAGIC
	pos++
	data[pos] = byte(req.Opcode)
	pos++
	binary.BigEndian.PutUint16(data[pos:pos+2],
		uint16(len(req.Key)))
	pos += 2

	// 4
	data[pos] = byte(len(req.Extras))
	pos++
	// data[pos] = 0
	pos++
	binary.BigEndian.PutUint16(data[pos:pos+2], req.VBucket)
	pos += 2

	// 8
	binary.BigEndian.PutUint32(data[pos:pos+4],
		uint32(len(req.Body)+len(req.Key)+len(req.Extras)))
	pos += 4

	// 12
	binary.BigEndian.PutUint32(data[pos:pos+4], req.Opaque)
	pos += 4

	// 16
	if req.Cas != 0 {
		binary.BigEndian.PutUint64(data[pos:pos+8], req.Cas)
	}
	pos += 8

	if len(req.Extras) > 0 {
		copy(data[pos:pos+len(req.Extras)], req.Extras)
		pos += len(req.Extras)
	}

	if len(req.Key) > 0 {
		copy(data[pos:pos+len(req.Key)], req.Key)
		pos += len(req.Key)
	}
	return pos
}

// HeaderBytes will return the wire representation of the request header
// (with the extras and key).
func (req *MCRequest) HeaderBytes() []byte {
	data := make([]byte, HDR_LEN+len(req.Extras)+len(req.Key))

	req.fillHeaderBytes(data)

	return data
}

// Bytes will return the wire representation of this request.
func (req *MCRequest) Bytes() []byte {
	data := make([]byte, req.Size())

	pos := req.fillHeaderBytes(data)

	if len(req.Body) > 0 {
		copy(data[pos:pos+len(req.Body)], req.Body)
	}

	return data
}

// Transmit will send this request message across a writer.
func (req *MCRequest) Transmit(w io.Writer) (n int, err error) {
	if len(req.Body) < 128 {
		n, err = w.Write(req.Bytes())
	} else {
		n, err = w.Write(req.HeaderBytes())
		if err == nil {
			m := 0
			m, err = w.Write(req.Body)
			n += m
		}
	}
	return
}

// Receive will fill this MCRequest with the data from a reader.
func (req *MCRequest) Receive(r io.Reader, hdrBytes []byte) (int, error) {
	if len(hdrBytes) < HDR_LEN {
		hdrBytes = []byte{
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0}
	}
	n, err := io.ReadFull(r, hdrBytes)
	if err != nil {
		return n, err
	}

	if hdrBytes[0] != RES_MAGIC && hdrBytes[0] != REQ_MAGIC {
		return n, fmt.Errorf("bad magic: 0x%02x", hdrBytes[0])
	}

	klen := int(binary.BigEndian.Uint16(hdrBytes[2:]))
	elen := int(hdrBytes[4])

	req.Datatype = uint8(hdrBytes[5])
	req.Opcode = CommandCode(hdrBytes[1])
	// Vbucket at 6:7
	req.VBucket = binary.BigEndian.Uint16(hdrBytes[6:])
	bodyLen := int(binary.BigEndian.Uint32(hdrBytes[8:]) -
		uint32(klen) - uint32(elen))
	req.Opaque = binary.BigEndian.Uint32(hdrBytes[12:])
	req.Cas = binary.BigEndian.Uint64(hdrBytes[16:])

	buf := make([]byte, klen+elen+bodyLen)
	m, err := io.ReadFull(r, buf)
	n += m
	if err == nil {
		if req.Opcode >= TAP_MUTATION &&
			req.Opcode <= TAP_CHECKPOINT_END &&
			len(buf) > 1 {
			// In these commands there is "engine private"
			// data at the end of the extras.  The first 2
			// bytes of extra data give its length.
			elen += int(binary.BigEndian.Uint16(buf))
		}
		req.Extras = buf[0:elen]
		req.Key = buf[elen : klen+elen]
		req.Body = buf[klen+elen:]
		if isSnapEndOpen(req) {
			fmsg := "open snapshot %rm hdrBytes:%v buf:%ru"
			arg1 := logging.TagUD(hdrBytes)
			arg2 := logging.TagUD(buf)
			logging.Errorf(fmsg, fmt.Sprintf("%#v", req), arg1, arg2)
		}
	}

	return n, err
}

func isSnapEndOpen(req *MCRequest) bool {
	if req.Opcode == DCP_SNAPSHOT {
		snapend := binary.BigEndian.Uint64(req.Extras[8:16])
		if snapend == 0xFFFFFFFFFFFFFFFF {
			return true
		}
	}
	return false
}
