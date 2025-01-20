package checkpointManager

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/couchbase/cbauth/metakv"
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

type CheckpointConfig struct {
	Applocation  application.AppLocation
	Keyspace     application.Keyspace
	AppID        uint32
	LocalAddress string
	KvPort       string

	OwnerNodeUUID string
}

func (c CheckpointConfig) String() string {
	return fmt.Sprintf("{ \"appLocation\": %s, \"checkpoint_for\": %s, \"appId\": %d }", c.Applocation, c.Keyspace, c.AppID)
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
	GetCheckpointManager(appId uint32, interruptCallback InterruptFunction, appLocation application.AppLocation, keyspace application.Keyspace) Checkpoint
	TlsSettingChange(gocbCluster *gocb.Cluster)
}

type leaderType uint8

const (
	DebuggerLeader leaderType = iota
)

type Checkpoint interface {
	OwnershipSnapshot(*common.AppRebalanceProgress)

	OwnVbCheckpoint(vb uint16)
	GetCheckpoint(vb uint16) (VbBlob, error)
	UpdateVal(vb uint16, field checkpointField, value interface{})
	StopCheckpoint(vb uint16)
	CloseCheckpointManager()

	DeleteCheckpointBlob(vb uint16) error
	DeleteKeys(deleteKeys []string)

	SyncUpsertCheckpoint(vb uint16, vbBlob *VbBlob) error

	TlsSettingChange(*gocb.Bucket)
	WaitTillAllGiveUp(vbs uint16)
	GetKeyPrefix() string

	GetTimerCheckpoints(appId uint32) (*gocb.ScanResult, error)
	GetRuntimeStats() common.StatsInterface

	TryTobeLeader(leaderType leaderType) (bool, error)

	ReadOnDeployCheckpoint() (nodeLeader string, seq uint32, onDeployStatus string)
	WriteOnDeployCheckpoint(nodeUUID string, seq uint32, appLocation application.AppLocation) bool
	PollUntilOnDeployCompletes()
	PublishOnDeployStatus(status string) error
}

const (
	checkpointPrefixTemplate  = "eventing::%d::"
	debuggerKeyTemplate       = checkpointPrefixTemplate + "debugger"
	onDeployLeaderKeyTemplate = "%s::onDeployLeader"
)

type debuggerCheckpoint struct {
	Token         string `json:"token"`
	LeaderElected bool   `json:"leaderElected"`
	Url           string `json:"url"`
}

type onDeployCheckpoint struct {
	NodeUUID       string `json:"node_uuid"`
	Seq            uint32 `json:"seq"`
	OnDeployStatus string `json:"on_deploy_status"`
}

func WriteDebuggerCheckpoint(collectionHandle *gocb.Collection, id uint32) (string, error) {
	token, err := common.RandomID()
	if err != nil {
		return "", err
	}

	dCheckpoint := debuggerCheckpoint{
		Token: token,
	}

	key := fmt.Sprintf(debuggerKeyTemplate, id)
	upsertOption := &gocb.UpsertOptions{
		Timeout: opsTimeout,
	}
	_, err = collectionHandle.Upsert(key, dCheckpoint, upsertOption)
	if err != nil {
		return "", err
	}
	return token, nil
}

func DeleteDebuggerCheckpoint(collectionHandle *gocb.Collection, id uint32) error {
	removeOption := &gocb.RemoveOptions{
		Timeout: opsTimeout,
	}

	key := fmt.Sprintf(debuggerKeyTemplate, id)
	_, err := collectionHandle.Remove(key, removeOption)
	if err != nil {
		return err
	}
	return nil
}

func GetDebuggerURL(collectionHandle *gocb.Collection, id uint32) (string, error) {
	result, err := getDebuggerCheckpoint(collectionHandle, id)
	if err != nil {
		return "", err
	}

	var checkpoint debuggerCheckpoint
	err = result.Content(&checkpoint)
	if err != nil {
		return "", err
	}

	return checkpoint.Url, nil
}

func WriteDebuggerUrl(collectionHandle *gocb.Collection, id uint32, url string) error {
	result, err := getDebuggerCheckpoint(collectionHandle, id)
	if err != nil {
		return err
	}

	var checkpoint debuggerCheckpoint
	err = result.Content(&checkpoint)
	if err != nil {
		return err
	}
	checkpoint.Url = url

	key := fmt.Sprintf(debuggerKeyTemplate, id)
	replaceOptions := &gocb.ReplaceOptions{Cas: result.Result.Cas(),
		Expiry: 0}
	_, err = collectionHandle.Replace(key, checkpoint, replaceOptions)
	return err
}

func getDebuggerCheckpoint(collectionHandle *gocb.Collection, id uint32) (*gocb.GetResult, error) {
	prefix := fmt.Sprintf(debuggerKeyTemplate, id)
	return getCheckpointWithPrefix(collectionHandle, prefix)
}

func getCheckpointWithPrefix(collectionHandle *gocb.Collection, key string) (*gocb.GetResult, error) {
	getOption := &gocb.GetOptions{
		Timeout: opsTimeout,
	}

	result, err := collectionHandle.Get(key, getOption)
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return nil, gocb.ErrDocumentNotFound
	}

	return result, err
}

func SetDebuggerCallback(appLocation application.AppLocation, value []byte) error {
	debuggerPath := fmt.Sprintf(common.EventingDebuggerPathTemplate, appLocation)
	return metakv.Set(debuggerPath, value, nil)
}

func DeleteDebuggerCallback(appLocation application.AppLocation) error {
	debuggerPath := fmt.Sprintf(common.EventingDebuggerPathTemplate, appLocation)
	return metakv.Delete(debuggerPath, nil)
}

func GetCheckpointKeyTemplate(appId uint32) string {
	return getCheckpointKeyTemplate(appId)
}

func getCheckpointKeyTemplate(appId uint32) string {
	return fmt.Sprintf(checkpointBlobTemplate, appId)
}

func DeleteOnDeployCheckpoint(appLocation application.AppLocation, collectionHandle *gocb.Collection) error {
	return deleteOnDeployCheckpoint(appLocation, collectionHandle)
}

func deleteOnDeployCheckpoint(appLocation application.AppLocation, collectionHandle *gocb.Collection) error {
	key := fmt.Sprintf(onDeployLeaderKeyTemplate, appLocation)
	_, err := collectionHandle.Remove(key, nil)
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return nil
	}
	return err
}

func ReadOnDeployCheckpoint(appLocation application.AppLocation, collectionHandle *gocb.Collection) (string, uint32, string, error) {
	return readOnDeployCheckpoint(appLocation, collectionHandle)
}

func readOnDeployCheckpoint(appLocation application.AppLocation, collectionHandle *gocb.Collection) (nodeLeader string, seq uint32, onDeployStatus string, err error) {
	var res *gocb.GetResult

	onDeployStatus = common.PENDING.String()

	key := fmt.Sprintf(onDeployLeaderKeyTemplate, appLocation)
	res, err = collectionHandle.Get(key, &gocb.GetOptions{})
	if err != nil {
		return
	}

	var checkpoint onDeployCheckpoint
	err = res.Content(&checkpoint)
	if err != nil {
		return
	}

	nodeLeader = checkpoint.NodeUUID
	seq = checkpoint.Seq
	onDeployStatus = checkpoint.OnDeployStatus
	return
}
