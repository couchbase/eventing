package checkpointManager

import (
	"sync"
	"sync/atomic"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/notifier"
	"github.com/couchbase/gocb/v2"
)

type bucketCheckpoint struct {
	sync.Mutex

	bucket          *gocb.Bucket
	clusterSettings *common.ClusterSettings
	observer        notifier.Observer
	bucketName      string
	checkpoints     map[uint32]Checkpoint
	keyspaceExist   bool
	broadcaster     common.Broadcaster
	id              uint32
}

func NewBucketCheckpointManager(clusterSettings *common.ClusterSettings, bucketName string, gocbCluster *gocb.Cluster, observer notifier.Observer, broadcaster common.Broadcaster) BucketCheckpoint {
	bCheckpoint := &bucketCheckpoint{
		clusterSettings: clusterSettings,
		observer:        observer,
		bucketName:      bucketName,
		broadcaster:     broadcaster,
		checkpoints:     make(map[uint32]Checkpoint),
	}

	var err error
	bCheckpoint.bucket, err = GetBucketObject(gocbCluster, observer, bucketName)
	bCheckpoint.keyspaceExist = ((err == nil) || (err != errKeyspaceNotFound))

	return bCheckpoint
}

func (bCheckpoint *bucketCheckpoint) GetCheckpointManager(appID uint32, interruptCallback InterruptFunction, appLocation application.AppLocation, keyspace application.Keyspace) Checkpoint {
	cc := CheckpointConfig{
		Applocation:   appLocation,
		Keyspace:      keyspace,
		AppID:         appID,
		LocalAddress:  bCheckpoint.clusterSettings.LocalAddress,
		KvPort:        bCheckpoint.clusterSettings.KvPort,
		OwnerNodeUUID: bCheckpoint.clusterSettings.UUID,
	}

	checkpointManager := NewCheckpointManagerForKeyspace(cc, interruptCallback, bCheckpoint.bucket, bCheckpoint.observer, bCheckpoint.broadcaster, bCheckpoint.keyspaceExist)
	cw := &checkpointWrapper{
		checkpoint:    checkpointManager,
		bucketManager: bCheckpoint.CloseID,
	}
	cw.id = atomic.AddUint32(&bCheckpoint.id, uint32(1))
	bCheckpoint.checkpoints[cw.id] = cw
	return cw
}

func (bCheckpoint *bucketCheckpoint) TlsSettingChange(gocbCluster *gocb.Cluster) {
	bCheckpoint.Lock()
	defer bCheckpoint.Unlock()

	var err error
	bCheckpoint.bucket, err = GetBucketObject(gocbCluster, bCheckpoint.observer, bCheckpoint.bucketName)
	bCheckpoint.keyspaceExist = ((err == nil) || (err != errKeyspaceNotFound))
	for _, checkpoint := range bCheckpoint.checkpoints {
		checkpoint.TlsSettingChange(bCheckpoint.bucket)
	}
}

func (bCheckpoint *bucketCheckpoint) CloseID(id uint32) {
	bCheckpoint.Lock()
	defer bCheckpoint.Unlock()

	delete(bCheckpoint.checkpoints, id)
}

type checkpointWrapper struct {
	checkpoint    Checkpoint
	id            uint32
	bucketManager func(uint32)
}

func (cw *checkpointWrapper) OwnershipSnapshot(snapshot *common.AppRebalanceProgress) {
	cw.checkpoint.OwnershipSnapshot(snapshot)
}

func (cw *checkpointWrapper) OwnVbCheckpoint(vb uint16) {
	cw.checkpoint.OwnVbCheckpoint(vb)
}

func (cw *checkpointWrapper) GetCheckpoint(vb uint16) (VbBlob, error) {
	return cw.checkpoint.GetCheckpoint(vb)
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
	id := cw.id
	cw.bucketManager(id)

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

func (cw *checkpointWrapper) DeleteKeys(deleteKeys []string) {
	cw.checkpoint.DeleteKeys(deleteKeys)
}

func (cw *checkpointWrapper) TryTobeLeader(lType leaderType) (bool, error) {
	return cw.checkpoint.TryTobeLeader(lType)
}
