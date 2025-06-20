package checkpointManager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/utils"
	dcpConn "github.com/couchbase/eventing/dcp_connection"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
	pc "github.com/couchbase/eventing/point_connection"
	"github.com/couchbase/gocb/v2"
)

const (
	ownershipCheckInterval = 1 * time.Second
	onDeployStatusCheck    = 1 * time.Second
	failoverCheck          = 1 * time.Minute
	updateInterval         = 20 * time.Second
)

const (
	checkpointBlobTemplate     = "eventing::%d:"
	startKeyCheckpointTemplate = checkpointBlobTemplate + ":"
	endKeyCheckpointTemplate   = checkpointBlobTemplate + "z"
)

var (
	errKeyspaceNotFound = errors.New("keyspace not found")
	errOwnerNotGivenUp  = errors.New("some other node owns this")
)

var (
	upsertOptions = &gocb.UpsertSpecOptions{CreatePath: true}
)

type collectionHandle interface {
	GetCollectionHandle() *gocb.Collection
	GetGocbMutateIn() []gocb.MutateInSpec
}

// This will persist the checkpoint blob
// and keep it sync with the metadata holder
type vbBlobInternal struct {
	sync.RWMutex

	key               string
	collectionHandler collectionHandle
	checkpointConfig  *CheckpointConfig
	observer          notifier.Observer

	// These have single writer single reader
	vbBlob     *VbBlob
	dirty      bool
	dirtyField []interface{}
	ownedTime  time.Time
}

func (vbi *vbBlobInternal) MarshalJSON() ([]byte, error) {
	vbBlob, _ := json.Marshal(vbi.vbBlob)
	return []byte(fmt.Sprintf("{ \"key\": \"%s\", \"vb_blob\": %s, \"owned_time\": \"%s\" }", vbi.key, vbBlob, vbi.ownedTime)), nil
}

func (vbi *vbBlobInternal) Copy() *vbBlobInternal {
	vbBlob := vbi.vbBlob.Copy()
	return &vbBlobInternal{
		key:               vbi.key,
		vbBlob:            &vbBlob,
		ownedTime:         vbi.ownedTime,
		collectionHandler: vbi.collectionHandler,
		checkpointConfig:  vbi.checkpointConfig,
		observer:          vbi.observer,
	}
}

// Get the ownership of the blob
func NewVbBlobInternal(key string, observer notifier.Observer, checkpointConfig *CheckpointConfig, collectionHandler collectionHandle, forced bool) (*vbBlobInternal, string, error) {
	vbblob := &vbBlobInternal{
		key:               key,
		checkpointConfig:  checkpointConfig,
		vbBlob:            nil,
		collectionHandler: collectionHandler,
		observer:          observer,
	}

	nodeUUID, err := vbblob.syncFromServerAndOwnTheKey(forced)
	if errors.Is(err, errOwnerNotGivenUp) {
		return nil, nodeUUID, err
	}

	if err != nil {
		return nil, nodeUUID, err
	}

	vbblob.dirtyField = make([]interface{}, 4)
	return vbblob, nodeUUID, nil
}

func (vbi *vbBlobInternal) unsetVbBlob() *VbBlob {
	vbi.Lock()
	defer vbi.Unlock()

	return vbi.unsetVbBlobLocked()
}

func (vbi *vbBlobInternal) close() (*VbBlob, error) {
	gocbMutateIn := vbi.collectionHandler.GetGocbMutateIn()

	// Capacity of the slice will be enough to put all so new list won't be created
	gocbMutateIn = append(gocbMutateIn, gocb.UpsertSpec("node_uuid", "", upsertOptions))
	gocbMutateIn, _ = vbi.getDirtyUpdates(gocbMutateIn)

	err := subdocUpdate(vbi.collectionHandler.GetCollectionHandle(), vbi.observer, vbi.checkpointConfig.Keyspace, vbi.key, gocbMutateIn)
	if errors.Is(err, ErrDocumentNotFound) {
		return vbi.unsetVbBlobLocked(), nil
	}

	return nil, err
}

func (vbi *vbBlobInternal) unsetVbBlobLocked() *VbBlob {
	vbi.dirty = false
	vbBlob := vbi.vbBlob
	vbi.vbBlob = nil
	vbi.dirtyField = make([]interface{}, 4)
	return vbBlob
}

func (vbi *vbBlobInternal) syncCheckpointBlob() error {
	dirty := false
	gocbMutateIn := vbi.collectionHandler.GetGocbMutateIn()
	gocbMutateIn, dirty = vbi.getDirtyUpdates(gocbMutateIn)
	if !dirty {
		return nil
	}

	err := subdocUpdate(vbi.collectionHandler.GetCollectionHandle(), vbi.observer, vbi.checkpointConfig.Keyspace, vbi.key, gocbMutateIn)
	if err != nil {
		return err
	}

	vbi.Lock()
	for index := range vbi.dirtyField {
		vbi.dirtyField[index] = nil
	}

	vbi.dirty = false
	vbi.Unlock()
	return nil
}

func (vbi *vbBlobInternal) getDirtyUpdates(mutateIn []gocb.MutateInSpec) ([]gocb.MutateInSpec, bool) {
	vbi.RLock()
	defer vbi.RUnlock()

	if !vbi.dirty {
		return mutateIn, false
	}

	uuid, ok := vbi.dirtyField[0].(uint64)
	if ok {
		mutateIn = append(mutateIn, gocb.UpsertSpec("vb_uuid", uuid, upsertOptions))
	}

	manifestID, ok := vbi.dirtyField[1].(string)
	if ok {
		mutateIn = append(mutateIn, gocb.UpsertSpec("manifest_id", manifestID, upsertOptions))
	}

	flog, ok := vbi.dirtyField[2].(dcpConn.FailoverLog)
	if ok {
		mutateIn = append(mutateIn, gocb.UpsertSpec("failover_log", flog, upsertOptions))
	}

	seqNo, ok := vbi.dirtyField[3].(uint64)
	if ok {
		mutateIn = append(mutateIn, gocb.UpsertSpec("last_processed_seq_no", seqNo, upsertOptions))
	}

	return mutateIn, true
}

func (vbi *vbBlobInternal) updateCheckpointBlob(field checkpointField, value interface{}) {
	vbi.Lock()
	defer vbi.Unlock()

	if vbi.vbBlob == nil {
		return
	}

	switch field {
	case Checkpoint_ManifestID:
		if vbi.vbBlob.ManifestID == value.(string) {
			return
		}
		vbi.vbBlob.ManifestID = value.(string)
		vbi.dirtyField[int(Checkpoint_ManifestID)] = value

	case Checkpoint_FailoverLog:
		vbi.vbBlob.FailoverLog = value.(dcpConn.FailoverLog)
		vbi.dirtyField[int(Checkpoint_FailoverLog)] = value

	case Checkpoint_SeqNum:
		vbinfo := value.([]uint64)
		if vbi.vbBlob.ProcessedSeqNum == vbinfo[0] && vbi.vbBlob.Vbuuid == vbinfo[1] {
			return
		}
		vbi.dirtyField[int(Checkpoint_ProcessedSeqNum)] = vbinfo[0]
		vbi.vbBlob.ProcessedSeqNum = vbinfo[0]
		vbi.dirtyField[int(Checkpoint_Vbuuid)] = vbinfo[1]
		vbi.vbBlob.Vbuuid = vbinfo[1]

	default:
		return
	}

	vbi.dirty = true
}

func (vbi *vbBlobInternal) getVbBlob() VbBlob {
	vbi.RLock()
	defer vbi.RUnlock()

	return vbi.vbBlob.Copy()
}

// sync from server only if this is the owner otherwise ignore
// and make the owner
func (vbi *vbBlobInternal) syncFromServerAndOwnTheKey(forced bool) (string, error) {
	vbBlob := &VbBlob{}
	_, err := get(vbi.collectionHandler.GetCollectionHandle(), vbi.observer, vbi.checkpointConfig.Keyspace, vbi.key, vbBlob)
	if errors.Is(err, ErrDocumentNotFound) {
		vbi.vbBlob = &VbBlob{
			NodeUUID: vbi.checkpointConfig.OwnerNodeUUID,
		}

		err := vbi.createCheckpointBlob()
		if err != nil {
			vbi.vbBlob = nil
			return "", fmt.Errorf("error in creating checkpoint: %v", err)
		}

		return vbi.checkpointConfig.OwnerNodeUUID, nil
	}

	if err != nil {
		return "", fmt.Errorf("error for get op: %v", err)
	}

	if !forced && (vbBlob.NodeUUID != "") && (vbBlob.NodeUUID != vbi.checkpointConfig.OwnerNodeUUID) {
		return vbBlob.NodeUUID, errOwnerNotGivenUp
	}

	vbi.vbBlob = vbBlob
	if vbBlob.NodeUUID == vbi.checkpointConfig.OwnerNodeUUID {
		return vbBlob.NodeUUID, nil
	}

	gocbMutateIn := vbi.collectionHandler.GetGocbMutateIn()
	gocbMutateIn = append(gocbMutateIn, gocb.UpsertSpec("node_uuid", vbi.checkpointConfig.OwnerNodeUUID, upsertOptions))
	err = subdocUpdate(vbi.collectionHandler.GetCollectionHandle(), vbi.observer, vbi.checkpointConfig.Keyspace, vbi.key, gocbMutateIn)
	if err != nil {
		vbi.vbBlob = nil
		return "", fmt.Errorf("error updating node_uuid: %v", err)
	}

	vbi.ownedTime = time.Now()
	return vbi.checkpointConfig.OwnerNodeUUID, nil
}

func (vbi *vbBlobInternal) createCheckpointBlob() error {
	upsertOption := &gocb.UpsertOptions{
		Timeout: opsTimeout,
	}

	vbi.Lock()
	vbBlob := vbi.vbBlob
	if vbi.vbBlob != nil {
		vbi.dirty = false
		for index, _ := range vbi.dirtyField {
			vbi.dirtyField[index] = nil
		}
	}
	vbi.Unlock()

	_, err := vbi.collectionHandler.GetCollectionHandle().Upsert(vbi.key, vbBlob, upsertOption)
	return err
}

type stats struct {
	VbsToOwn   []uint16                   `json:"vbs_to_own"`
	VbsToClose []uint16                   `json:"vbs_to_close"`
	VbiDetails map[uint16]*vbBlobInternal `json:"vb_blobs"`
}

type checkpointManager struct {
	sync.RWMutex

	prefix            string
	checkpointConfig  CheckpointConfig
	observer          notifier.Observer
	interruptCallback InterruptFunction
	broadcaster       common.Broadcaster

	collectionHandler atomic.Value
	keyspaceExists    atomic.Bool

	notifier *common.Signal

	// TODO: Use copy map such that we can avoid creating lot of copies
	vbBlobSlice map[uint16]*vbBlobInternal
	vbsToOwn    map[uint16]struct{}
	vbsToClose  map[uint16]*vbBlobInternal

	gocbMutateIn []gocb.MutateInSpec

	close func()
}

func NewCheckpointManager(cc CheckpointConfig, cluster *gocb.Cluster, clusterConfig *common.ClusterSettings,
	interruptCallback InterruptFunction, observer notifier.Observer, broadcaster common.Broadcaster) Checkpoint {

	bucket, _ := GetBucketObjectWithRetry(cluster, 5, observer, cc.Keyspace.BucketName)
	return NewCheckpointManagerForKeyspace(cc, interruptCallback, bucket, observer, broadcaster)
}

func NewCheckpointManagerForKeyspace(cc CheckpointConfig, interruptCallback InterruptFunction,
	bucket *gocb.Bucket, observer notifier.Observer, broadcaster common.Broadcaster) Checkpoint {

	return NewCheckpointManagerForKeyspaceWithContext(context.Background(), cc, interruptCallback, bucket, observer, broadcaster)
}

func NewCheckpointManagerForKeyspaceWithContext(ctx context.Context, cc CheckpointConfig, interruptCallback InterruptFunction,
	bucket *gocb.Bucket, observer notifier.Observer, broadcaster common.Broadcaster) Checkpoint {

	logPrefix := "checkpointManager::NewCheckpointManager"
	cm := &checkpointManager{
		checkpointConfig:  cc,
		prefix:            getCheckpointKeyTemplate(cc.AppID),
		interruptCallback: interruptCallback,
		observer:          observer,
		broadcaster:       broadcaster,

		vbsToOwn:    make(map[uint16]struct{}),
		vbsToClose:  make(map[uint16]*vbBlobInternal),
		vbBlobSlice: make(map[uint16]*vbBlobInternal),

		notifier: common.NewSignal(),

		gocbMutateIn: make([]gocb.MutateInSpec, 0, 6),
	}

	gocb.SetLogger(&GocbLogger{})
	// Store true and lazily detect if keyspace exists or not
	cm.keyspaceExists.Store(true)
	cm.collectionHandler.Store(GetCollectionHandle(bucket, cc.Keyspace))

	ctx, close := context.WithCancel(ctx)
	cm.close = close

	logging.Infof("%s Started new checkpoint manager with config: %s", logPrefix, cc)
	go cm.checkpointRoutine(ctx)

	return cm
}

func (cm *checkpointManager) OwnVbCheckpoint(vb uint16) {
	cm.Lock()
	// No need to delete from close vbs since close followed by own for same vb not possible
	cm.vbsToOwn[vb] = struct{}{}
	cm.Unlock()

	cm.notifier.Notify()
}

func (cm *checkpointManager) UpdateVal(vb uint16, field checkpointField, value interface{}) {
	// update the dirty value
	cm.RLock()
	blob, err := cm.getCheckpointBlobLocked(vb)
	cm.RUnlock()

	if err != nil {
		return
	}

	blob.updateCheckpointBlob(field, value)
}

func (cm *checkpointManager) StopCheckpoint(vb uint16) {
	cm.Lock()
	if _, ok := cm.vbsToOwn[vb]; ok {
		delete(cm.vbsToOwn, vb)
		cm.Unlock()
		vbBlob := getDummyVbBlob()
		cm.interruptCallback(OWNERSHIP_CLOSED, vb, vbBlob)
		return
	}

	blob, err := cm.getCheckpointBlobLocked(vb)
	if err != nil {
		cm.Unlock()
		vbBlob := getDummyVbBlob()
		cm.interruptCallback(OWNERSHIP_CLOSED, vb, vbBlob)
		return
	}

	cm.vbsToClose[vb] = blob
	cm.Unlock()
	cm.notifier.Notify()
}

func (cm *checkpointManager) TlsSettingChange(bucket *gocb.Bucket) {
	cm.keyspaceExists.Store(common.CheckKeyspaceExist(cm.observer, cm.checkpointConfig.Keyspace))
	cm.collectionHandler.Store(GetCollectionHandle(bucket, cm.checkpointConfig.Keyspace))
}

func (cm *checkpointManager) WaitTillAllGiveUp(numVbs uint16) {
	logPrefix := fmt.Sprintf("checkpointManager::WaitTillAllGiveUp[%s]", cm.checkpointConfig.AppLocation)

	// Check if all the nodes given up the ownership
	checkVbs := make(map[uint16]struct{})
	for vb := uint16(0); vb < numVbs; vb++ {
		checkVbs[vb] = struct{}{}
	}

	for vb := range checkVbs {
		owner, err := cm.vbCurrentOwner(vb)
		if err != nil || owner != "" {
			continue
		}
		delete(checkVbs, vb)
	}
	if len(checkVbs) == 0 {
		return
	}

	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()

	failoverCheckTicker := time.NewTicker(failoverCheck)
	defer failoverCheckTicker.Stop()

	for len(checkVbs) > 0 {
		select {
		case <-t.C:
			for vb := range checkVbs {
				owner, err := cm.vbCurrentOwner(vb)
				if err != nil || owner != "" {
					continue
				}
				delete(checkVbs, vb)
			}

		case <-failoverCheckTicker.C:
			nodesToVbs := make(map[string][]uint16)
			for vb := range checkVbs {
				owner, err := cm.vbCurrentOwner(vb)
				if err != nil {
					logging.Errorf("%s Error getting ownership info of vb: %d err: %v", logPrefix, vb, err)
					continue
				}

				if owner != "" {
					nodesToVbs[owner] = append(nodesToVbs[owner], vb)
					continue
				}
				delete(checkVbs, vb)
			}

			if len(nodesToVbs) == 0 {
				continue
			}

			logging.Infof("%s Ownership not given up for %s. Querying its owners", logPrefix, utils.CondenseMap(nodesToVbs))

			for uuid, vbs := range nodesToVbs {
				rebalanceProgress, err := cm.getOwnershipVbs(uuid)
				// This function is called when we are trying to undeploy function so that all the vbuckets are given up
				// and no more timer checkpoints are created. So no need to update the ownership in checkpoint.
				if errors.Is(err, common.ErrNodeNotAvailable) {
					logging.Errorf("%s Node %s not available so its vbs not gonna close. Continuing...", logPrefix, uuid)
					for _, vb := range vbs {
						delete(checkVbs, vb)
					}
					continue
				}

				if err != nil {
					logging.Errorf("%s Error getting progress details from uuid: %s err: %v", logPrefix, uuid, err)
					continue
				}

				logging.Infof("%s Node %s current owning vbs: %v", logPrefix, uuid, rebalanceProgress.OwnedVbs)
				for _, waitingVb := range vbs {
					found := false
					for _, ownedVb := range rebalanceProgress.OwnedVbs {
						if ownedVb == waitingVb {
							found = true
							break
						}
					}

					if !found {
						delete(checkVbs, waitingVb)
					}
				}
			}
		}
	}
}

func (cm *checkpointManager) GetKeyPrefix() string {
	return cm.prefix
}

func (cm *checkpointManager) CloseCheckpointManager() {
	cm.close()

	if !cm.keyspaceExists.Load() {
		return
	}

	cm.Lock()
	vbBlobSlice := cm.vbBlobSlice
	cm.vbBlobSlice = make(map[uint16]*vbBlobInternal)
	cm.Unlock()

	for vb, vbi := range vbBlobSlice {
		_, err := cm.closeOwnership(vb, vbi)
		if err != nil {
			logging.Errorf("checkpointManager::CloseCheckpointManager[%s] Error closing ownership for vb: %d err: %v", cm.checkpointConfig.AppLocation, vb, err)

			if errors.Is(err, errKeyspaceNotFound) {
				cm.keyspaceExists.Store(false)
				return
			}
		}
	}
}

func (cm *checkpointManager) DeleteCheckpointBlob(vb uint16) error {
	if !cm.keyspaceExists.Load() {
		return nil
	}

	key := cm.getKey(vb)
	err := remove(cm.getCollectionHandle(), cm.observer, cm.checkpointConfig.Keyspace, key)
	cm.keyspaceExists.Store(!errors.Is(err, errKeyspaceNotFound))
	return err
}

func (cm *checkpointManager) SyncUpsertCheckpoint(vb uint16, vbBlob *VbBlob) error {
	key := cm.getKey(vb)
	err := upsert(cm.getCollectionHandle(), cm.observer, cm.checkpointConfig.Keyspace, key, vbBlob)
	cm.keyspaceExists.Store(!errors.Is(err, errKeyspaceNotFound))
	return err
}

func (cm *checkpointManager) OwnershipSnapshot(snapshot *common.AppRebalanceProgress) {
	cm.RLock()
	defer cm.RUnlock()

	for vb := range cm.vbsToOwn {
		snapshot.ToOwn = append(snapshot.ToOwn, vb)
	}

	for vb := range cm.vbsToClose {
		snapshot.ToClose = append(snapshot.ToClose, vb)
	}

	for vb := range cm.vbBlobSlice {
		snapshot.OwnedVbs = append(snapshot.OwnedVbs, vb)
	}
}

func (cm *checkpointManager) GetAllCheckpoints(appId uint32) (*gocb.ScanResult, error) {
	startKey := fmt.Sprintf(startKeyCheckpointTemplate, appId)
	endKey := fmt.Sprintf(endKeyCheckpointTemplate, appId)
	return scan(cm.getCollectionHandle(), cm.observer, cm.checkpointConfig.Keyspace, startKey, endKey, true)
}

func (cm *checkpointManager) DeleteKeys(deleteKeys []string) {
	if !cm.keyspaceExists.Load() {
		return
	}

	if len(deleteKeys) == 0 {
		return
	}

	err := deleteBulk(cm.getCollectionHandle(), cm.observer, cm.checkpointConfig.Keyspace, deleteKeys)
	cm.keyspaceExists.Store(!errors.Is(err, errKeyspaceNotFound))
}

func (cm *checkpointManager) GetRuntimeStats() common.StatsInterface {
	s := &stats{}
	cm.RLock()
	defer cm.RUnlock()

	s.VbsToOwn = make([]uint16, 0, len(cm.vbsToOwn))
	for vb := range cm.vbsToOwn {
		s.VbsToOwn = append(s.VbsToOwn, vb)
	}

	s.VbsToClose = make([]uint16, 0, len(cm.vbsToClose))
	for vb := range cm.vbsToClose {
		s.VbsToClose = append(s.VbsToClose, vb)
	}

	s.VbiDetails = utils.CopyMap(cm.vbBlobSlice)

	return common.NewMarshalledData(s)
}

// TryToBeLeaderUrl try to be leader for this debugger session
// returns true if able to be leader false otherwise
func (cm *checkpointManager) TryTobeLeader(lType leaderType, seq uint32) (bool, error) {
	switch lType {
	case DebuggerLeader:
		result, checkpoint, err := getDebuggerCheckpoint(cm.getCollectionHandle(), cm.observer, cm.checkpointConfig.Keyspace, cm.checkpointConfig.AppID)
		// Already someone deleted the file. Maybe someone stopped the debugger
		if errors.Is(err, ErrDocumentNotFound) {
			return false, nil
		}

		if err != nil {
			return false, err
		}

		checkpoint.LeaderElected = true
		key := getDebuggerKey(cm.checkpointConfig.AppID)
		err = replace(cm.getCollectionHandle(), cm.observer, cm.checkpointConfig.Keyspace, key, checkpoint, result.Result.Cas())
		if errors.Is(err, gocb.ErrCasMismatch) || errors.Is(err, ErrDocumentNotFound) {
			return false, nil
		}

		if err != nil {
			return false, err
		}
		return true, nil

	case OnDeployLeader:
		doc := OnDeployCheckpoint{
			NodeUUID: cm.checkpointConfig.OwnerNodeUUID,
			Seq:      seq,
			Status:   PendingOnDeploy,
		}
		key := getOnDeployKey(cm.checkpointConfig.AppLocation)
		for {
			err := insert(cm.getCollectionHandle(), cm.observer, cm.checkpointConfig.Keyspace, key, doc)
			if err != nil {
				if errors.Is(err, gocb.ErrDocumentExists) {
					_, checkpoint := pollAndGetCheckpoint(cm.checkpointConfig.AppLocation, cm.getCollectionHandle(), cm.observer, cm.checkpointConfig.Keyspace)
					// This case can occur when OnDeploy node leader respawned
					// and is trying to become leader again
					if checkpoint.Status == PendingOnDeploy && checkpoint.NodeUUID == cm.checkpointConfig.OwnerNodeUUID {
						// Fail OnDeploy due to leader node failure
						cm.PublishOnDeployStatus(FailedStateOnDeploy)
					}
					return false, nil
				}

				if errors.Is(err, errKeyspaceNotFound) {
					cm.keyspaceExists.Store(false)
					return false, nil
				}
				// Let the caller routine handle this
				time.Sleep(100 * time.Millisecond)
				continue
			}
			break
		}
		return true, nil
	}
	return true, nil
}

// Caller should lock before calling this function
func (cm *checkpointManager) getCheckpointBlobLocked(vb uint16) (*vbBlobInternal, error) {
	blob, ok := cm.vbBlobSlice[vb]
	if !ok {
		return nil, fmt.Errorf("vb %d isn't owned by this node", vb)
	}

	return blob, nil
}

func (cm *checkpointManager) checkpointRoutine(ctx context.Context) {
	logPrefix := fmt.Sprintf("checkpointManager::checkpointRoutine[%s]", cm.checkpointConfig.AppLocation)

	ownershipRoutineTicker := time.NewTicker(ownershipCheckInterval)
	periodicUpdateTicker := time.NewTicker(updateInterval)
	failoverCheckTicker := time.NewTicker(failoverCheck)

	defer func() {
		periodicUpdateTicker.Stop()
		failoverCheckTicker.Stop()
	}()

	for {
		select {
		case <-periodicUpdateTicker.C:
			cm.updateCheckpoint()

		case <-cm.notifier.Wait():
			cm.notifier.Ready()
			cm.ownershipTakeover()

		case <-ownershipRoutineTicker.C:
			cm.ownershipTakeover()

		case <-failoverCheckTicker.C:
			nodesToVbs := cm.ownershipTakeover()
			if len(nodesToVbs) == 0 {
				continue
			}

			logging.Infof("%s Ownership not given up for %s. Querying its owners", logPrefix, utils.CondenseMap(nodesToVbs))
			var vbsStatus []struct {
				vb     uint16
				vbBlob *VbBlob
			}
			for uuid, vbs := range nodesToVbs {
				rebalanceProgress, err := cm.getOwnershipVbs(uuid)
				if errors.Is(err, common.ErrNodeNotAvailable) {
					logging.Infof("%s Node uuid %s not active. Forcefully taking over all vbs from this node", logPrefix, uuid)
					// check and forcefully takeover the vb

					for _, vb := range vbs {
						cm.RLock()
						if _, ok := cm.vbsToOwn[vb]; !ok {
							cm.RUnlock()
							continue
						}
						cm.RUnlock()

						vbBlob, _, err := cm.getOwnership(vb, true)
						if err == nil {
							vbsStatus = append(vbsStatus, struct {
								vb     uint16
								vbBlob *VbBlob
							}{
								vb:     vb,
								vbBlob: vbBlob,
							})

						}
					}
					continue
				}

				if err != nil {
					logging.Errorf("%s Error getting progress details from node uuid: %s err: %v", logPrefix, uuid, err)
					continue
				}

				for _, waitingVb := range vbs {
					found := false
					for _, ownedVb := range rebalanceProgress.OwnedVbs {
						if ownedVb == waitingVb {
							found = true
							break
						}
					}

					if !found {
						logging.Infof("%s Node uuid: %s already own given up vb: %d. Forcefully taking control now", logPrefix, uuid, waitingVb)
						cm.RLock()
						if _, ok := cm.vbsToOwn[waitingVb]; !ok {
							cm.RUnlock()
							continue
						}
						cm.RUnlock()

						vbBlob, _, err := cm.getOwnership(waitingVb, true)
						if err == nil {
							vbsStatus = append(vbsStatus, struct {
								vb     uint16
								vbBlob *VbBlob
							}{
								vb:     waitingVb,
								vbBlob: vbBlob,
							})
						}
					}
				}
			}

			for _, status := range vbsStatus {
				cm.interruptCallback(OWNERSHIP_OBTAINED, status.vb, status.vbBlob)
			}

			cm.Lock()
			for _, vb := range vbsStatus {
				delete(cm.vbsToClose, vb.vb)
			}
			cm.Unlock()

		case <-ctx.Done():
			return
		}
	}
}

func (cm *checkpointManager) getOwnershipVbs(uuid string) (*common.AppRebalanceProgress, error) {
	query := application.QueryMap(cm.checkpointConfig.AppLocation)
	req := &pc.Request{
		Query:   query,
		Timeout: common.HttpCallWaitTime,
	}

	responseBytes, _, err := cm.broadcaster.RequestFor(uuid, "/getOwnedVbsForApp", req)
	if errors.Is(err, common.ErrNodeNotAvailable) {
		return nil, err
	}

	if err != nil {
		return nil, fmt.Errorf("error for getOwnership call: %v", err)
	}

	if len(responseBytes) == 0 {
		// Check if node is healthy or not
		return nil, cm.healthCheck(uuid)
	}

	rebalanceProgress := &common.AppRebalanceProgress{}
	_ = json.Unmarshal(responseBytes[0], &rebalanceProgress)
	return rebalanceProgress, nil
}

func (cm *checkpointManager) healthCheck(uuid string) error {
	nodesInterface, err := cm.observer.GetCurrentState(notifier.InterestedEvent{
		Event: notifier.EventEventingTopologyChanges,
	})

	if err != nil {
		return err
	}

	nodes := nodesInterface.([]*notifier.Node)
	for _, node := range nodes {
		if node.NodeUUID == uuid && node.Status == notifier.NodeStatusHealthy {
			return nil
		}
	}

	return common.ErrNodeNotAvailable
}

func (cm *checkpointManager) updateCheckpoint() {
	logPrefix := fmt.Sprintf("checkpointManager::updateCheckpoint[%s]", cm.checkpointConfig.AppLocation)

	if !cm.keyspaceExists.Load() {
		return
	}

	cm.RLock()
	if len(cm.vbBlobSlice) == 0 {
		cm.RUnlock()
		return
	}
	vbBlobSlice := make(map[uint16]*vbBlobInternal)
	maps.Copy(vbBlobSlice, cm.vbBlobSlice)
	cm.RUnlock()

	for _, vbBlob := range vbBlobSlice {
		err := vbBlob.syncCheckpointBlob()
		if err != nil {
			if errors.Is(err, errKeyspaceNotFound) {
				cm.keyspaceExists.Store(false)
				return
			}
			if errors.Is(err, ErrDocumentNotFound) {
				err = vbBlob.createCheckpointBlob()
				logging.Errorf("%s Checkpoint blob deleted. Recreating checkpoint blob from inmemory blob for %s. error creating blob? %v", logPrefix, vbBlob.key, err)
			} else {
				logging.Errorf("%s Error while syncing vb blob: %s err: %v", logPrefix, vbBlob.key, err)
			}
		}
	}
}

func (cm *checkpointManager) vbCurrentOwner(vb uint16) (string, error) {
	if !cm.keyspaceExists.Load() {
		return "", nil
	}

	key := cm.getKey(vb)
	vbBlob := &VbBlob{}
	_, err := get(cm.getCollectionHandle(), cm.observer, cm.checkpointConfig.Keyspace, key, vbBlob)
	if err != nil {
		if errors.Is(err, ErrDocumentNotFound) {
			return "", nil
		}

		if errors.Is(err, errKeyspaceNotFound) {
			cm.keyspaceExists.Store(false)
			return "", nil
		}

		return "", err
	}

	return vbBlob.NodeUUID, nil
}

func (cm *checkpointManager) getCollectionHandle() *gocb.Collection {
	return cm.collectionHandler.Load().(*gocb.Collection)
}

func (cm *checkpointManager) GetCollectionHandle() *gocb.Collection {
	return cm.getCollectionHandle()
}

func (cm *checkpointManager) GetGocbMutateIn() []gocb.MutateInSpec {
	cm.gocbMutateIn = cm.gocbMutateIn[:0]
	return cm.gocbMutateIn
}

func (cm *checkpointManager) getOwnership(vb uint16, forced bool) (*VbBlob, string, error) {
	key := cm.getKey(vb)
	vbi, uuid, err := NewVbBlobInternal(key, cm.observer, &cm.checkpointConfig, cm, forced)
	if err != nil {
		return nil, uuid, err
	}

	cm.Lock()
	cm.vbBlobSlice[vb] = vbi
	cm.Unlock()
	copyVbBlob := vbi.getVbBlob()
	return &copyVbBlob, uuid, nil
}

func (cm *checkpointManager) closeOwnership(vb uint16, vbi *vbBlobInternal) (*VbBlob, error) {
	vbBlob, err := vbi.close()
	if err != nil {
		return nil, err
	}

	cm.Lock()
	delete(cm.vbBlobSlice, vb)
	cm.Unlock()
	return vbBlob, nil
}

func (cm *checkpointManager) getKey(vb uint16) string {
	return cm.prefix + fmt.Sprintf(":%d", vb)
}

func (cm *checkpointManager) ownershipTakeover() map[string][]uint16 {
	logPrefix := fmt.Sprintf("checkpointManager::ownershipTakeover[%s]", cm.checkpointConfig.AppLocation)

	cm.RLock()
	if len(cm.vbsToOwn) == 0 && len(cm.vbsToClose) == 0 {
		cm.RUnlock()
		return nil
	}

	nodesToVbs := make(map[string][]uint16)
	vbsStatus := make(map[OwnMsg][]struct {
		vb     uint16
		vbBlob *VbBlob
	})
	vbsToOwn := make(map[uint16]struct{})
	maps.Copy(vbsToOwn, cm.vbsToOwn)

	vbsToClose := make(map[uint16]*vbBlobInternal)
	maps.Copy(vbsToClose, cm.vbsToClose)
	cm.RUnlock()

	if !cm.keyspaceExists.Load() {
		for vb := range vbsToOwn {
			vbsStatus[OWNERSHIP_OBTAINED] = append(vbsStatus[OWNERSHIP_OBTAINED], struct {
				vb     uint16
				vbBlob *VbBlob
			}{
				vb:     vb,
				vbBlob: getDummyVbBlob(),
			})
		}

		for vb, vbi := range vbsToClose {
			vbBlob := vbi.unsetVbBlob()
			vbsStatus[OWNERSHIP_CLOSED] = append(vbsStatus[OWNERSHIP_CLOSED], struct {
				vb     uint16
				vbBlob *VbBlob
			}{
				vb:     vb,
				vbBlob: vbBlob,
			})
		}

	} else {
		for vb := range vbsToOwn {
			vbBlob, uuid, err := cm.getOwnership(vb, false)
			if errors.Is(err, errOwnerNotGivenUp) {
				nodesToVbs[uuid] = append(nodesToVbs[uuid], vb)
				continue
			}

			if err != nil {
				logging.Errorf("%s Error while taking over vbucket ownership: %v err: %v", logPrefix, vb, err)
				if !errors.Is(err, errKeyspaceNotFound) {
					// Ownership not closed continue
					continue
				}

				cm.keyspaceExists.Store(false)
				vbBlob = getDummyVbBlob()

				for vb := range vbsToOwn {
					vbsStatus[OWNERSHIP_OBTAINED] = append(vbsStatus[OWNERSHIP_OBTAINED], struct {
						vb     uint16
						vbBlob *VbBlob
					}{
						vb:     vb,
						vbBlob: vbBlob,
					})
				}
				break
			}

			vbsStatus[OWNERSHIP_OBTAINED] = append(vbsStatus[OWNERSHIP_OBTAINED], struct {
				vb     uint16
				vbBlob *VbBlob
			}{
				vb:     vb,
				vbBlob: vbBlob,
			})
		}

		for vb, vbBlobInternal := range vbsToClose {
			vbBlob, err := cm.closeOwnership(vb, vbBlobInternal)
			if err != nil {
				logging.Errorf("%s Error while closing ownership for: %d err: %v", logPrefix, vb, err)
				if !errors.Is(err, errKeyspaceNotFound) {
					// Ownership not closed continue
					continue
				}

				// keyspace is deleted
				cm.keyspaceExists.Store(false)
				for vb, vbi := range vbsToClose {
					vbBlob := vbi.unsetVbBlob()

					vbsStatus[OWNERSHIP_CLOSED] = append(vbsStatus[OWNERSHIP_CLOSED], struct {
						vb     uint16
						vbBlob *VbBlob
					}{
						vb:     vb,
						vbBlob: vbBlob,
					})
				}
				return nodesToVbs
			}

			vbsStatus[OWNERSHIP_CLOSED] = append(vbsStatus[OWNERSHIP_CLOSED], struct {
				vb     uint16
				vbBlob *VbBlob
			}{
				vb:     vb,
				vbBlob: vbBlob,
			})
		}
	}

	for _, vbs := range vbsStatus[OWNERSHIP_CLOSED] {
		cm.interruptCallback(OWNERSHIP_CLOSED, vbs.vb, vbs.vbBlob)
	}

	for _, vbs := range vbsStatus[OWNERSHIP_OBTAINED] {
		cm.interruptCallback(OWNERSHIP_OBTAINED, vbs.vb, vbs.vbBlob)
	}

	cm.Lock()
	for _, vbs := range vbsStatus[OWNERSHIP_CLOSED] {
		// closed so just delete from the vbsBlobSlice so that no more updates are done
		delete(cm.vbBlobSlice, vbs.vb)
		delete(cm.vbsToClose, vbs.vb)
	}

	for _, vbs := range vbsStatus[OWNERSHIP_OBTAINED] {
		delete(cm.vbsToOwn, vbs.vb)
	}
	cm.Unlock()

	return nodesToVbs
}

func (cm *checkpointManager) PollUntilOnDeployCompletes() *OnDeployCheckpoint {
	logPrefix := fmt.Sprintf("checkpointManager::PollUntilOnDeployCompletes[%s]", cm.checkpointConfig.AppLocation)

	failoverCheckTicker := time.NewTicker(failoverCheck)
	defer failoverCheckTicker.Stop()
	onDeployStatusCheckTicker := time.NewTicker(onDeployStatusCheck)
	defer onDeployStatusCheckTicker.Stop()

	ownerNode := ""
	for {
		select {
		case <-failoverCheckTicker.C:
			if ownerNode == "" {
				continue
			}

			if err := cm.healthCheck(ownerNode); err != nil {
				logging.Errorf("%s Failing OnDeploy since leader node %s is not alive", logPrefix, ownerNode)
				checkpoint := cm.PublishOnDeployStatus(FailedStateOnDeploy)
				return checkpoint
			}

		case <-onDeployStatusCheckTicker.C:
			_, checkpoint, err := readOnDeployCheckpoint(cm.checkpointConfig.AppLocation, cm.getCollectionHandle(), cm.observer, cm.checkpointConfig.Keyspace)
			if err != nil {
				if errors.Is(err, ErrDocumentNotFound) {
					checkpoint.Status = FailedStateOnDeploy
					return checkpoint
				}

				if errors.Is(err, errKeyspaceNotFound) {
					cm.keyspaceExists.Store(false)
					checkpoint.Status = FailedStateOnDeploy
					return checkpoint
				}
				continue
			}

			ownerNode = checkpoint.NodeUUID
			if checkpoint.Status != PendingOnDeploy {
				return checkpoint
			}
		}
	}
}

// PublishOnDeployStatus upserts the status field in the OnDeploy checkpoint
func (cm *checkpointManager) PublishOnDeployStatus(status OnDeployState) *OnDeployCheckpoint {

	gocbResult, checkpoint := pollAndGetCheckpoint(cm.checkpointConfig.AppLocation, cm.getCollectionHandle(), cm.observer, cm.checkpointConfig.Keyspace)
	// Some other node already written the status
	if checkpoint.Status != PendingOnDeploy {
		return checkpoint
	}

	cas := gocbResult.Cas()
	mutateIn := []gocb.MutateInSpec{gocb.UpsertSpec("on_deploy_status", status, upsertOptions)}
	key := getOnDeployKey(cm.checkpointConfig.AppLocation)

	for {
		err := subdocUpdateUsingCas(cm.getCollectionHandle(), cm.observer, cm.checkpointConfig.Keyspace, key, cas, mutateIn)
		if err != nil {
			if errors.Is(err, ErrDocumentNotFound) {
				checkpoint.Status = FailedStateOnDeploy
				return checkpoint
			}

			if errors.Is(err, errKeyspaceNotFound) {
				cm.keyspaceExists.Store(false)
				checkpoint.Status = FailedStateOnDeploy
				return checkpoint
			}

			if errors.Is(err, gocb.ErrCasMismatch) {
				_, checkpoint = pollAndGetCheckpoint(cm.checkpointConfig.AppLocation, cm.getCollectionHandle(), cm.observer, cm.checkpointConfig.Keyspace)
				return checkpoint
			}
			continue
		}
		break
	}
	// Able to write the status
	checkpoint.Status = status
	return checkpoint
}

func pollAndGetCheckpoint(applocation application.AppLocation, collectionHandler *gocb.Collection,
	observer notifier.Observer, keyspace application.Keyspace) (gocbResult *gocb.GetResult, checkpoint *OnDeployCheckpoint) {

	var err error
	for {
		gocbResult, checkpoint, err = readOnDeployCheckpoint(applocation, collectionHandler, observer, keyspace)
		if err != nil {
			if errors.Is(err, ErrDocumentNotFound) {
				return nil, &OnDeployCheckpoint{
					Status: FailedStateOnDeploy,
				}
			}

			if errors.Is(err, errKeyspaceNotFound) {
				return nil, &OnDeployCheckpoint{
					Status: FailedStateOnDeploy,
				}
			}
			time.Sleep(onDeployStatusCheck)
			continue
		}
		return
	}
}
