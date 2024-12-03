package vbhandler

import (
	"bytes"

	"github.com/couchbase/eventing/application"
	checkpointManager "github.com/couchbase/eventing/checkpoint_manager"
	"github.com/couchbase/eventing/common"
	dcpMessage "github.com/couchbase/eventing/dcp_connection"
	eventPool "github.com/couchbase/eventing/event_pool"
	serverConfig "github.com/couchbase/eventing/server_config"
)

type StatsHandler interface {
	IncrementProcessingStats(key string)
}

type Ownership interface {
	GetVbMap(keyspaceInfo *application.KeyspaceInfo, id uint16, numVb uint16, appLocation application.AppLocation) (string, []uint16, error)
}

type FilterInterface interface {
	CheckAndGetEventsInternalDetails(msg *dcpMessage.DcpEvent) (*checkpointManager.ParsedInternalDetails, bool)
	IsTrapEvent() (RuntimeSystem, bool)
}

type RuntimeSystem interface {
	GetProcessVersion() uint32
	WriteDcpMessage(version uint32, buffer *bytes.Buffer, opcode uint8, workerID uint8,
		instanceID []byte, msg *dcpMessage.DcpEvent, internalInfo *checkpointManager.ParsedInternalDetails) int32

	FlushMessage(version uint32, buffer *bytes.Buffer)
	VbSettings(version uint32, opcode uint8, handlerID []byte, key interface{}, value interface{})
}

type Config struct {
	Version       uint32
	FuncID        uint16
	TenantID      string
	AppLocation   application.AppLocation
	ConfiguredVbs uint16
	InstanceID    []byte
	DcpType       serverConfig.DeploymentMode

	HandlerSettings application.HandlerSettings
	MetaInfo        application.MetaInfo

	RuntimeSystem     RuntimeSystem
	OwnershipRoutine  Ownership
	CheckpointManager checkpointManager.Checkpoint
	Pool              eventPool.ManagerPool
	StatsHandler      StatsHandler
	Filter            FilterInterface
}

type VbHandler interface {
	// GetHighSeqNum returns the high seq number of the owned vbs
	GetHighSeqNum() map[uint16]uint64

	// NotifyOwnershipChange notifies the new vb map. Returns newly added and closed vbs
	NotifyOwnershipChange() (vbMapVersion string, newVbs []uint16, closedVbs []uint16, notFullyOwned []uint16, err error)

	// VbHandlerSnapshot returns the still to be owned and closed vbs
	VbHandlerSnapshot(appProgress *common.AppRebalanceProgress)

	// AddVb will add it to the requester map. Returns how many remained to own
	// NotifyOwnershipChange should be called before AddVb
	AddVb(vb uint16, vbBlob *checkpointManager.VbBlob) int

	// CloseVb will close the requester and filter out any mutations related to this vb
	// Returns how many remained to be closed
	// NotifyOwnershipChange should be called before CloseVb
	CloseVb(vb uint16) int

	// AckMessages notes how many msgs and there bytes are processed by RuntimeSystem
	AckMessages(value []byte) (int, int)

	// Close closes all the vbs and returns how many ownership is closed
	Close() []uint16
}

var (
	DummyVbHandler = dummy(0)
)

type dummy uint8

func NewDummyVbHandler() VbHandler {
	return DummyVbHandler
}

func (_ dummy) GetHighSeqNum() map[uint16]uint64 {
	return nil
}

func (_ dummy) NotifyOwnershipChange() (vbMapVersion string, newVbs []uint16, closedVbs []uint16, notFullyOwned []uint16, err error) {
	return "", nil, nil, nil, nil
}

func (_ dummy) VbHandlerSnapshot(appProgress *common.AppRebalanceProgress) {
}

func (_ dummy) AddVb(vb uint16, vbBlob *checkpointManager.VbBlob) int {
	return 0
}

func (_ dummy) CloseVb(vb uint16) int {
	return 0
}

func (_ dummy) AckMessages(value []byte) (int, int) {
	return 0, 0
}

func (_ dummy) Close() []uint16 {
	return nil
}
