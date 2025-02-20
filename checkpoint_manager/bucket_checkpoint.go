package checkpointManager

import (
	"sync"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/notifier"
	"github.com/couchbase/gocb/v2"
)

type bucketCheckpoint struct {
	sync.RWMutex

	clusterSettings *common.ClusterSettings
	observer        notifier.Observer
	bucketName      string
	broadcaster     common.Broadcaster

	bucket *common.AtomicTypes[*gocb.Bucket]

	checkpoints map[uint32]Checkpoint
	id          uint32
}

func NewBucketCheckpointManager(clusterSettings *common.ClusterSettings, bucketName string, gocbCluster *gocb.Cluster, observer notifier.Observer, broadcaster common.Broadcaster) BucketCheckpoint {
	bCheckpoint := &bucketCheckpoint{
		clusterSettings: clusterSettings,
		observer:        observer,
		bucketName:      bucketName,
		broadcaster:     broadcaster,
		checkpoints:     make(map[uint32]Checkpoint),
	}

	bucket, _ := GetBucketObject(gocbCluster, observer, bucketName)
	bCheckpoint.bucket = common.NewAtomicTypes(bucket)
	return bCheckpoint
}

func (bCheckpoint *bucketCheckpoint) GetCheckpointManager(appID uint32, interruptCallback InterruptFunction, appLocation application.AppLocation, keyspace application.Keyspace) Checkpoint {
	cc := CheckpointConfig{
		AppLocation:   appLocation,
		Keyspace:      keyspace,
		AppID:         appID,
		LocalAddress:  bCheckpoint.clusterSettings.LocalAddress,
		KvPort:        bCheckpoint.clusterSettings.KvPort,
		OwnerNodeUUID: bCheckpoint.clusterSettings.UUID,
	}

	checkpointManager := NewCheckpointManagerForKeyspace(cc, interruptCallback, bCheckpoint.bucket.Load(), bCheckpoint.observer, bCheckpoint.broadcaster)
	cw := &checkpointWrapper{
		checkpoint: checkpointManager,
	}

	bCheckpoint.Lock()
	bCheckpoint.id++
	cw.checkpointMgrIdCloser = bCheckpoint.closeID(bCheckpoint.id)
	bCheckpoint.checkpoints[bCheckpoint.id] = cw
	bCheckpoint.Unlock()

	// This will protect against the case where the bucket is closed and tls changes made
	cw.TlsSettingChange(bCheckpoint.bucket.Load())
	return cw
}

func (bCheckpoint *bucketCheckpoint) TlsSettingChange(gocbCluster *gocb.Cluster) {
	bucket, _ := GetBucketObject(gocbCluster, bCheckpoint.observer, bCheckpoint.bucketName)

	bCheckpoint.bucket.Store(bucket)
	bCheckpoint.Lock()
	checkpoints := make([]Checkpoint, 0, len(bCheckpoint.checkpoints))
	for _, checkpoint := range bCheckpoint.checkpoints {
		checkpoints = append(checkpoints, checkpoint)
	}
	bCheckpoint.Unlock()

	for _, checkpoint := range checkpoints {
		checkpoint.TlsSettingChange(bCheckpoint.bucket.Load())
	}
}

func (bCheckpoint *bucketCheckpoint) CloseBucketManager() {
	bCheckpoint.Lock()
	checkpoints := make([]Checkpoint, 0, len(bCheckpoint.checkpoints))
	for _, checkpoint := range bCheckpoint.checkpoints {
		checkpoints = append(checkpoints, checkpoint)
	}
	bCheckpoint.Unlock()

	for _, checkpoint := range checkpoints {
		checkpoint.CloseCheckpointManager()
	}
}

func (bCheckpoint *bucketCheckpoint) closeID(id uint32) func() {
	return func() {
		bCheckpoint.Lock()
		defer bCheckpoint.Unlock()

		delete(bCheckpoint.checkpoints, id)
	}
}

type checkpointWrapper struct {
	checkpoint Checkpoint

	checkpointMgrIdCloser func()
}

func (cw *checkpointWrapper) OwnershipSnapshot(snapshot *common.AppRebalanceProgress) {
	cw.checkpoint.OwnershipSnapshot(snapshot)
}

func (cw *checkpointWrapper) OwnVbCheckpoint(vb uint16) {
	cw.checkpoint.OwnVbCheckpoint(vb)
}

func (cw *checkpointWrapper) UpdateVal(vb uint16, field checkpointField, value interface{}) {
	cw.checkpoint.UpdateVal(vb, field, value)
}

func (cw *checkpointWrapper) StopCheckpoint(vb uint16) {
	cw.checkpoint.StopCheckpoint(vb)
}

func (cw *checkpointWrapper) WaitTillAllGiveUp(numVbs uint16) {
	cw.checkpoint.WaitTillAllGiveUp(numVbs)
}

func (cw *checkpointWrapper) CloseCheckpointManager() {
	cw.checkpointMgrIdCloser()
	cw.checkpoint.CloseCheckpointManager()
}

func (cw *checkpointWrapper) DeleteCheckpointBlob(vb uint16) error {
	return cw.checkpoint.DeleteCheckpointBlob(vb)
}

func (cw *checkpointWrapper) SyncUpsertCheckpoint(vb uint16, vbBlob *VbBlob) error {
	return cw.checkpoint.SyncUpsertCheckpoint(vb, vbBlob)
}

func (cw *checkpointWrapper) TlsSettingChange(bucket *gocb.Bucket) {
	cw.checkpoint.TlsSettingChange(bucket)
}

func (cw *checkpointWrapper) GetKeyPrefix() string {
	return cw.checkpoint.GetKeyPrefix()
}

func (cw *checkpointWrapper) GetTimerCheckpoints(appId uint32) (*gocb.ScanResult, error) {
	return cw.checkpoint.GetTimerCheckpoints(appId)
}

func (cw *checkpointWrapper) GetRuntimeStats() common.StatsInterface {
	return cw.checkpoint.GetRuntimeStats()
}

func (cw *checkpointWrapper) DeleteKeys(deleteKeys []string) {
	cw.checkpoint.DeleteKeys(deleteKeys)
}

func (cw *checkpointWrapper) TryTobeLeader(lType leaderType, seq uint32) (bool, error) {
	return cw.checkpoint.TryTobeLeader(lType, seq)
}

func (cw *checkpointWrapper) PollUntilOnDeployCompletes() *OnDeployCheckpoint {
	return cw.checkpoint.PollUntilOnDeployCompletes()
}

func (cw *checkpointWrapper) PublishOnDeployStatus(status OnDeployState) *OnDeployCheckpoint {
	return cw.checkpoint.PublishOnDeployStatus(status)
}
