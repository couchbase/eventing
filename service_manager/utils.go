package servicemanager

import (
	"encoding/binary"

	"github.com/couchbase/cbauth/service"
)

func decodeRev(b service.Revision) uint64 {
	return binary.BigEndian.Uint64(b)
}

func encodeRev(rev uint64) service.Revision {
	ext := make(service.Revision, 8)
	binary.BigEndian.PutUint64(ext, rev)

	return ext
}
