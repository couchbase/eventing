package dcpManager

import (
	"github.com/couchbase/eventing/common"
	dcpConn "github.com/couchbase/eventing/dcp_connection"
)

// DcpManager manages dcp connection
// It will send the dcp messages to the receiver
// It will manage all the connection till caller calls
// close dcp
type DcpManager interface {
	DoneDcpEvent(event *dcpConn.DcpEvent)
	StartStreamReq(sr *dcpConn.StreamReq) error
	PauseStreamReq(sr *dcpConn.StreamReq)
	CloseRequest(sr *dcpConn.StreamReq) (*dcpConn.StreamReq, error)
	GetFailoverLog(vbs []uint16) (map[uint16]dcpConn.FailoverLog, error)
	GetSeqNumber(vbs []uint16, collectionID string) (map[uint16]uint64, error)

	RegisterID(id uint16, sendChannel chan<- *dcpConn.DcpEvent)
	DeregisterID(id uint16)

	GetRuntimeStats() common.StatsInterface
	CloseManager()
	CloseConditional() bool
}
