package checkpointManager

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	dcpConn "github.com/couchbase/eventing/dcp_connection"
	"github.com/couchbase/gocb/v2"
)

type ParsedInternalDetails struct {
	AllCursorIds   []string
	StaleCursorIds []string
	RootCas        uint64
	Scope          string
	Collection     string
}

type checkpointField int8

const (
	Checkpoint_Vbuuid checkpointField = iota
	Checkpoint_ManifestID
	Checkpoint_FailoverLog
	Checkpoint_ProcessedSeqNum
	Checkpoint_SeqNum
)

var (
	ErrDocumentNotFound = errors.New("document not found")
)

type CheckpointConfig struct {
	AppLocation  application.AppLocation
	Keyspace     application.Keyspace
	AppInstance  string
	AppID        uint32
	LocalAddress string
	KvPort       string

	OwnerNodeUUID string
}

func (c CheckpointConfig) String() string {
	return fmt.Sprintf("{ \"appLocation\": %s, \"checkpoint_for\": %s, \"appId\": %d }", c.AppLocation, c.Keyspace, c.AppID)
}

type VbBlob struct {
	NodeUUID string `json:"node_uuid"`

	Vbuuid          uint64              `json:"vb_uuid"`
	ManifestID      string              `json:"manifest_id"`
	FailoverLog     dcpConn.FailoverLog `json:"failover_log"`
	ProcessedSeqNum uint64              `json:"last_processed_seq_no"`
}

func getDummyVbBlob() *VbBlob {
	return &VbBlob{}
}

func (blob *VbBlob) Copy() VbBlob {
	newVbBlob := *blob
	log := make(dcpConn.FailoverLog, len(blob.FailoverLog))
	for index, flog := range blob.FailoverLog {
		log[index] = [2]uint64{flog[0], flog[1]}
	}
	newVbBlob.FailoverLog = log
	return newVbBlob
}

func (blob *VbBlob) String() string {
	marshalledBytes, _ := json.Marshal(blob)
	return string(marshalledBytes)
}

type OwnMsg uint8

const (
	OWNERSHIP_OBTAINED OwnMsg = iota
	OWNERSHIP_CLOSED
)

type InterruptFunction func(msg OwnMsg, vb uint16, vbBlob *VbBlob)

type BucketCheckpoint interface {
	GetCheckpointManager(appId uint32, appInstanceID string, interruptCallback InterruptFunction, appLocation application.AppLocation, keyspace application.Keyspace) Checkpoint
	TlsSettingChange(gocbCluster *gocb.Cluster)
	CloseBucketManager()
}

type leaderType uint8

const (
	DebuggerLeader leaderType = iota
	OnDeployLeader
)

type Checkpoint interface {
	OwnershipSnapshot(*common.AppRebalanceProgress)

	OwnVbCheckpoint(vb uint16)
	UpdateVal(vb uint16, field checkpointField, value interface{})

	StopCheckpoint(vb uint16)
	CloseCheckpointManager()

	DeleteCheckpointBlob(vb uint16) error
	DeleteKeys(deleteKeys []string)

	SyncUpsertCheckpoint(vb uint16, vbBlob *VbBlob) error

	TlsSettingChange(*gocb.Bucket)
	WaitTillAllGiveUp(vbs uint16)

	GetAllCheckpoints(appId uint32) (*gocb.ScanResult, error)
	GetRuntimeStats() common.StatsInterface

	TryTobeLeader(leaderType leaderType, seq uint32) (bool, error)
	PollUntilOnDeployCompletes() *OnDeployCheckpoint
	PublishOnDeployStatus(status OnDeployState) *OnDeployCheckpoint
}

type dummyCheckpointManager struct{}

var (
	dummyCheckpointMgrStruct = &dummyCheckpointManager{}
)

func NewDummyCheckpointManager() Checkpoint {
	return dummyCheckpointMgrStruct
}

func (d *dummyCheckpointManager) OwnershipSnapshot(progress *common.AppRebalanceProgress) {
}

func (d *dummyCheckpointManager) OwnVbCheckpoint(vb uint16) {
}

func (d *dummyCheckpointManager) GetCheckpoint(vb uint16) (VbBlob, error) {
	return VbBlob{}, nil
}

func (d *dummyCheckpointManager) UpdateVal(vb uint16, field checkpointField, value interface{}) {
}

func (d *dummyCheckpointManager) StopCheckpoint(vb uint16) {
}

func (d *dummyCheckpointManager) CloseCheckpointManager() {
}

func (d *dummyCheckpointManager) DeleteCheckpointBlob(vb uint16) error {
	return nil
}

func (d *dummyCheckpointManager) DeleteKeys(deleteKeys []string) {
}

func (d *dummyCheckpointManager) SyncUpsertCheckpoint(vb uint16, vbBlob *VbBlob) error {
	return nil
}

func (d *dummyCheckpointManager) TlsSettingChange(*gocb.Bucket) {
}

func (d *dummyCheckpointManager) WaitTillAllGiveUp(vbs uint16) {
}

func (d *dummyCheckpointManager) GetKeyPrefix() string {
	return ""
}

func (d *dummyCheckpointManager) GetAllCheckpoints(appId uint32) (*gocb.ScanResult, error) {
	return nil, fmt.Errorf("dummyCheckpointManager.GetAllCheckpoints not implemented")
}

func (d *dummyCheckpointManager) GetRuntimeStats() common.StatsInterface {
	return nil
}

func (d *dummyCheckpointManager) TryTobeLeader(leaderType leaderType, seq uint32) (bool, error) {
	return false, nil
}

func (d *dummyCheckpointManager) ReadOnDeployCheckpoint() (*OnDeployCheckpoint, error) {
	return &OnDeployCheckpoint{Status: FinishedOnDeploy}, nil
}

func (d *dummyCheckpointManager) PollUntilOnDeployCompletes() *OnDeployCheckpoint {
	return &OnDeployCheckpoint{Status: FinishedOnDeploy}
}

func (d *dummyCheckpointManager) PublishOnDeployStatus(status OnDeployState) *OnDeployCheckpoint {
	return nil
}
