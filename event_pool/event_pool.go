package eventPool

import (
	"github.com/couchbase/eventing/application"
	checkpointManager "github.com/couchbase/eventing/checkpoint_manager"
	dcpConn "github.com/couchbase/eventing/dcp_connection"
	dcpManager "github.com/couchbase/eventing/dcp_manager"
	"github.com/couchbase/gocb/v2"
)

type DcpManagerType uint8

const (
	CommonConn DcpManagerType = iota
	DedicatedConn
)

type SeqNumerInterface interface {
	RegisterID(id uint16, sendChannel chan<- *dcpConn.DcpEvent)
	GetSeqNumber(vbs []uint16, collectionID string) (map[uint16]uint64, error)
	GetFailoverLog(vbs []uint16) (map[uint16]dcpConn.FailoverLog, error)
	DeregisterID(id uint16)

	CloseManager()
	CloseConditional() bool
}

type ManagerPool interface {
	GetDcpManagerPool(dcpManagerType DcpManagerType, identifier string, bucketName string, sendChannel chan<- *dcpConn.DcpEvent) dcpManager.DcpManager
	GetSeqManager(bucketName string) SeqNumerInterface
	GetCheckpointManager(appId uint32, interruptCallback checkpointManager.InterruptFunction, appLocation application.AppLocation, keyspace application.Keyspace) checkpointManager.Checkpoint

	TlsSettingsChanged(gocbCluster *gocb.Cluster)
	CloseConditional()
	ClosePool()
}
