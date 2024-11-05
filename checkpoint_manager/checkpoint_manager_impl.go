package checkpointManager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	failoverCheck  = 1 * time.Minute
	updateInterval = 10 * time.Second
)

const (
	checkpointBlobTemplate          = "eventing::%d:"
	timerKeyCheckpointTemplate      = "eventing::%d:"
	timerStartKeyCheckpointTemplate = timerKeyCheckpointTemplate + "a"
	timerEndKeyCheckpointTemplate   = timerKeyCheckpointTemplate + "z"
)

type GocbLogger struct{}

func (r *GocbLogger) Log(level gocb.LogLevel, offset int, format string, v ...interface{}) error {
	// log.Printf(format, v...)
	return nil
}

var (
	errKeyspaceNotFound = errors.New("keyspace not found")
	errOwnerNotGivenUp  = errors.New("some other node owns this")
)

type collectionHandle interface {
	GetCollectionHandle() *gocb.Collection
}

// This will persist the checkpoint blob
// and keep it sync with the metadata holder
type vbBlobInternal struct {
	sync.RWMutex

	key    string
	vbBlob *VbBlob

	dirty      bool
	dirtyField []interface{}

	collectionHandler collectionHandle
	checkpointConfig  *CheckpointConfig
}

// Get the ownership of the blob
func NewVbBlobInternal(key string, checkpointConfig *CheckpointConfig, collectionHandler collectionHandle, forced bool) (*vbBlobInternal, string, error) {
	vbblob := &vbBlobInternal{
		key:               key,
		checkpointConfig:  checkpointConfig,
		vbBlob:            nil,
		collectionHandler: collectionHandler,
	}

	nodeUUID, err := vbblob.syncFromServerAndOwnTheKeyLocked(forced)
	if err == errOwnerNotGivenUp {
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
	vbi.Lock()
	defer vbi.Unlock()

	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}
	mutateIn := make([]gocb.MutateInSpec, 0, 6)
	mutateIn = append(mutateIn, gocb.UpsertSpec("node_uuid", "", upsertOptions))
	mutateIn, _ = vbi.getDirtyUpdateLocked(mutateIn)

	err := vbi.subdocUpdateLocked(mutateIn)
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return vbi.unsetVbBlobLocked(), nil
	}

	if err != nil {
		return nil, err
	}

	return vbi.unsetVbBlobLocked(), nil
}

func (vbi *vbBlobInternal) unsetVbBlobLocked() *VbBlob {
	vbi.dirty = false
	vbBlob := vbi.vbBlob
	vbi.vbBlob = nil
	vbi.dirtyField = make([]interface{}, 4)
	return vbBlob
}

func (vbi *vbBlobInternal) syncCheckpointBlob() error {
	vbi.Lock()
	defer vbi.Unlock()

	dirty := false
	var mutateIn []gocb.MutateInSpec
	mutateIn, dirty = vbi.getDirtyUpdateLocked(mutateIn)
	if !dirty {
		return nil
	}

	err := vbi.subdocUpdateLocked(mutateIn)
	if err != nil {
		return err
	}

	for index, _ := range vbi.dirtyField {
		vbi.dirtyField[index] = nil
	}

	vbi.dirty = false
	return nil
}

func (vbi *vbBlobInternal) getDirtyUpdateLocked(mutateIn []gocb.MutateInSpec) ([]gocb.MutateInSpec, bool) {
	if !vbi.dirty {
		return mutateIn, false
	}

	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}
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

type checkpointManager struct {
	sync.RWMutex

	prefix            string
	checkpointConfig  CheckpointConfig
	collectionHandler atomic.Value

	vbBlobSlice map[uint16]*vbBlobInternal

	vbsToOwn   map[uint16]struct{}
	vbsToClose map[uint16]*vbBlobInternal

	observer          notifier.Observer
	interruptCallback InterruptFunction
	keyspaceExists    atomic.Bool

	broadcaster common.Broadcaster
	notifier    *common.Signal
	close       func()
}

func NewCheckpointManager(cc CheckpointConfig, cluster *gocb.Cluster, clusterConfig *common.ClusterSettings, interruptCallback InterruptFunction, observer notifier.Observer, broadcaster common.Broadcaster) Checkpoint {
	bucket, err := GetBucketObjectWithRetry(cluster, 5, observer, cc.Keyspace.BucketName)
	keyspaceExist := ((err == nil) || (err != errKeyspaceNotFound))
	return NewCheckpointManagerForKeyspace(cc, interruptCallback, bucket, observer, broadcaster, keyspaceExist)
}

func NewCheckpointManagerForKeyspace(cc CheckpointConfig, interruptCallback InterruptFunction, bucket *gocb.Bucket, observer notifier.Observer, broadcaster common.Broadcaster, keyspaceExist bool) Checkpoint {
	return NewCheckpointManagerForKeyspaceWithContext(context.Background(), cc, interruptCallback, bucket, observer, broadcaster, keyspaceExist)
}

func NewCheckpointManagerForKeyspaceWithContext(ctx context.Context, cc CheckpointConfig, interruptCallback InterruptFunction, bucket *gocb.Bucket, observer notifier.Observer, broadcaster common.Broadcaster, keyspaceExist bool) Checkpoint {
	logPrefix := "checkpointManager::NewCheckpointManager"
	cm := &checkpointManager{
		checkpointConfig:  cc,
		prefix:            getCheckpointKeyTemplate(cc.AppID),
		vbsToOwn:          make(map[uint16]struct{}),
		vbsToClose:        make(map[uint16]*vbBlobInternal),
		vbBlobSlice:       make(map[uint16]*vbBlobInternal),
		interruptCallback: interruptCallback,
		observer:          observer,
		broadcaster:       broadcaster,

		notifier: common.NewSignal(),
	}

	gocb.SetLogger(&GocbLogger{})
	cm.keyspaceExists.Store(true)
	cm.collectionHandler.Store(GetCollectionHandle(bucket, cc.Keyspace))

	ctx, close := context.WithCancel(ctx)
	cm.close = close

	logging.Infof("%s Started new checkpoint manager with config: %s", logPrefix, cc)
	go cm.periodicCheckpoint(ctx)
	go cm.runOwnershipRoutine(ctx)

	return cm
}

func (cm *checkpointManager) OwnVbCheckpoint(vb uint16) {
	// Get the field from checkpoint doc
	// If not owned yet wait for some time and again check for ownership

	cm.Lock()
	defer cm.Unlock()

	// Delete from vbs to close since again we need to
	// own this vb
	delete(cm.vbsToClose, vb)

	cm.vbsToOwn[vb] = struct{}{}
	cm.notifier.Notify()
}

func (cm *checkpointManager) GetCheckpoint(vb uint16) (VbBlob, error) {
	cm.RLock()
	blob, err := cm.getCheckpointBlob(vb)
	cm.RUnlock()

	if err != nil {
		return VbBlob{}, err
	}

	return blob.getVbBlob(), nil
}

func (cm *checkpointManager) UpdateVal(vb uint16, field checkpointField, value interface{}) {
	// update the dirty value
	cm.RLock()
	blob, err := cm.getCheckpointBlob(vb)
	cm.RUnlock()

	if err != nil {
		return
	}

	blob.updateCheckpointBlob(field, value)
}

func (cm *checkpointManager) StopCheckpoint(vb uint16) {
	cm.Lock()
	defer cm.Unlock()
	delete(cm.vbsToOwn, vb)

	blob, err := cm.getCheckpointBlob(vb)
	if err != nil {
		vbBlob := getDummyVbBlob()
		cm.interruptCallback(OWNERSHIP_CLOSED, vb, vbBlob)
		return
	}

	cm.vbsToClose[vb] = blob
	cm.notifier.Notify()
}

func (cm *checkpointManager) TlsSettingChange(bucket *gocb.Bucket) {
	cm.keyspaceExists.Store(common.CheckKeyspaceExist(cm.observer, cm.checkpointConfig.Keyspace))
	cm.collectionHandler.Store(GetCollectionHandle(bucket, cm.checkpointConfig.Keyspace))
}

func (cm *checkpointManager) WaitTillAllGiveUp(numVbs uint16) {
	logPrefix := fmt.Sprintf("checkpointManager::WaitTillAllGiveUp[%s]", cm.checkpointConfig.Applocation)

	// Check if all the nodes given up the ownership
	checkVbs := make(map[uint16]struct{})
	for vb := uint16(0); vb < numVbs; vb++ {
		checkVbs[vb] = struct{}{}
	}

	for vb, _ := range checkVbs {
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
			for vb, _ := range checkVbs {
				owner, err := cm.vbCurrentOwner(vb)
				if err != nil || owner != "" {
					continue
				}
				delete(checkVbs, vb)
			}

		case <-failoverCheckTicker.C:
			nodesToVbs := make(map[string][]uint16)
			for vb, _ := range checkVbs {
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
			query := application.QueryMap(cm.checkpointConfig.Applocation)
			req := &pc.Request{
				Query:   query,
				Timeout: common.HttpCallWaitTime,
			}

			for uuid, vbs := range nodesToVbs {
				rebalanceProgress, err := cm.getOwnershipVbs(uuid, req)
				// TODO: Possibilty that due to this few vbs won't be closed
				if err == common.ErrNodeNotAvailable {
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
}

func (cm *checkpointManager) DeleteCheckpointBlob(vb uint16) error {
	logPrefix := fmt.Sprintf("checkpointManager::DeleteCheckpointBlob[%s]", cm.checkpointConfig.Applocation)
	if !cm.keyspaceExists.Load() {
		return nil
	}

	key := cm.getKey(vb)
	err := cm.remove(key)
	if err != nil {
		if !errors.Is(err, gocb.ErrDocumentNotFound) {
			logging.Errorf("%s Error while deleting ownership checkpoint for vb %d err: %v", logPrefix, vb, err)
		}
		// TODO: Try different mechanism to check scope or collection is deleted or not
		if common.CheckKeyspaceExist(cm.observer, cm.checkpointConfig.Keyspace) {
			// Maybe retry couple of time and then giveup
			return err
		}
		cm.keyspaceExists.Store(false)
	}
	return nil
}

func (cm *checkpointManager) SyncUpsertCheckpoint(vb uint16, vbBlob *VbBlob) error {
	key := cm.getKey(vb)
	err := cm.upsert(key, vbBlob)
	return err
}

func (cm *checkpointManager) OwnershipSnapshot(snapshot *common.AppRebalanceProgress) {
	cm.RLock()
	defer cm.RUnlock()

	for vb, _ := range cm.vbsToOwn {
		snapshot.ToOwn = append(snapshot.ToOwn, vb)
	}

	for vb, _ := range cm.vbsToClose {
		snapshot.ToClose = append(snapshot.ToClose, vb)
	}

	for vb, _ := range cm.vbBlobSlice {
		snapshot.OwnedVbs = append(snapshot.OwnedVbs, vb)
	}
}

func (cm *checkpointManager) GetTimerCheckpoints(appId uint32) (*gocb.ScanResult, error) {
	startTimerKey := fmt.Sprintf(timerStartKeyCheckpointTemplate, appId)
	endTimerKey := fmt.Sprintf(timerEndKeyCheckpointTemplate, appId)
	return cm.scan(startTimerKey, endTimerKey, true)
}

func (cm *checkpointManager) DeleteKeys(deleteKeys []string) {
	logPrefix := fmt.Sprintf("checkpointManager::DeleteKeys[%s]", cm.checkpointConfig.Applocation)
	if !cm.keyspaceExists.Load() {
		return
	}

	if len(deleteKeys) == 0 {
		return
	}

	err := cm.deleteBulk(deleteKeys)
	if err != nil {
		logging.Errorf("%s Error deleting bulk ownership checkpoint err: %v", logPrefix, err)
		// TODO: Try different mechanism to check scope or collection is deleted or not
		if common.CheckKeyspaceExist(cm.observer, cm.checkpointConfig.Keyspace) {
			// Maybe retry couple of time and then giveup
			return
		}
		cm.keyspaceExists.Store(false)
	}
}

// TryToBeLeaderUrl try to be leader for this debugger session
// returns true if able to be leader false otherwise
func (cm *checkpointManager) TryTobeLeader(lType leaderType) (bool, error) {
	switch lType {
	case DebuggerLeader:
		key := cm.prefix + ":debugger"
		result, err := getCheckpointWithPrefix(cm.getCollectionHandle(), key)
		// Already someone deleted the file. Maybe someone stopped the debugger
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return false, nil
		}

		if err != nil {
			return false, err
		}

		var checkpoint debuggerCheckpoint
		err = result.Content(&checkpoint)
		if err != nil {
			return false, err
		}
		checkpoint.LeaderElected = true
		replaceOptions := &gocb.ReplaceOptions{Cas: result.Result.Cas(),
			Expiry: 0}
		_, err = cm.getCollectionHandle().Replace(key, checkpoint, replaceOptions)
		if errors.Is(err, gocb.ErrCasMismatch) || errors.Is(err, gocb.ErrDocumentNotFound) {
			return false, nil
		}

		if err != nil {
			return false, err
		}
		return true, nil
	}
	return true, nil
}

// Caller should lock before calling this function
func (cm *checkpointManager) getCheckpointBlob(vb uint16) (*vbBlobInternal, error) {
	blob, ok := cm.vbBlobSlice[vb]
	if !ok {
		return nil, fmt.Errorf("vb %d isn't owned by this node", vb)
	}

	return blob, nil
}

func (cm *checkpointManager) periodicCheckpoint(ctx context.Context) {
	logPrefix := fmt.Sprintf("checkpointManager::periodicCheckpoint[%s]", cm.checkpointConfig.Applocation)

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

		case <-failoverCheckTicker.C:
			nodesToVbs := make(map[string][]uint16)
			cm.Lock()
			if len(cm.vbsToOwn) != 0 {
				for vb, _ := range cm.vbsToOwn {
					vbBlob, uuid, err := cm.getOwnership(vb, false)
					if err == errOwnerNotGivenUp {
						nodesToVbs[uuid] = append(nodesToVbs[uuid], vb)
						continue
					}

					if err == nil {
						delete(cm.vbsToOwn, vb)
						cm.interruptCallback(OWNERSHIP_OBTAINED, vb, vbBlob)
					}
				}
			}
			cm.Unlock()
			if len(nodesToVbs) == 0 {
				continue
			}

			logging.Infof("%s Ownership not given up for %s. Querying its owners", logPrefix, utils.CondenseMap(nodesToVbs))
			query := application.QueryMap(cm.checkpointConfig.Applocation)
			req := &pc.Request{
				Query:   query,
				Timeout: common.HttpCallWaitTime,
			}

			for uuid, vbs := range nodesToVbs {
				rebalanceProgress, err := cm.getOwnershipVbs(uuid, req)
				if err == common.ErrNodeNotAvailable {
					logging.Infof("%s Node uuid %s not active. Forcefully taking over all vbs from this node", logPrefix, uuid)
					// check and forcefully takeover the vb
					cm.Lock()
					for _, vb := range vbs {
						if _, ok := cm.vbsToOwn[vb]; !ok {
							continue
						}

						vbBlob, _, err := cm.getOwnership(vb, true)
						if err == nil {
							delete(cm.vbsToOwn, vb)
							cm.interruptCallback(OWNERSHIP_OBTAINED, vb, vbBlob)
						}
					}
					cm.Unlock()
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
						cm.Lock()
						if _, ok := cm.vbsToOwn[waitingVb]; !ok {
							continue
						}

						vbBlob, _, err := cm.getOwnership(waitingVb, true)
						if err == nil {
							delete(cm.vbsToOwn, waitingVb)
							cm.interruptCallback(OWNERSHIP_OBTAINED, waitingVb, vbBlob)
						}
						cm.Unlock()
					}
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

func (cm *checkpointManager) getOwnershipVbs(uuid string, req *pc.Request) (*common.AppRebalanceProgress, error) {
	responseBytes, _, err := cm.broadcaster.RequestFor(uuid, "/getOwnedVbsForApp", req)
	if err == common.ErrNodeNotAvailable {
		return nil, err
	}

	if err != nil {
		return nil, fmt.Errorf("error for getOwnership call: %v", err)
	}

	if len(responseBytes) == 0 {
		return nil, fmt.Errorf("got empty response")
	}

	rebalanceProgress := &common.AppRebalanceProgress{}
	json.Unmarshal(responseBytes[0], &rebalanceProgress)
	return rebalanceProgress, nil
}

func (cm *checkpointManager) runOwnershipRoutine(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)
	defer func() {
		t.Stop()
	}()

	for {
		select {
		case <-t.C:
			cm.ownershipTakeover()

		case <-cm.notifier.Wait():
			cm.ownershipTakeover()
			cm.notifier.Ready()

		case <-ctx.Done():
			return
		}
	}
}

func (cm *checkpointManager) updateCheckpoint() {
	logPrefix := fmt.Sprintf("checkpointManager::updateCheckpoint[%s]", cm.checkpointConfig.Applocation)

	cm.RLock()
	defer cm.RUnlock()

	if !cm.keyspaceExists.Load() {
		return
	}

	for _, vbBlob := range cm.vbBlobSlice {
		err := vbBlob.syncCheckpointBlob()
		if err != nil {
			if !common.CheckKeyspaceExist(cm.observer, cm.checkpointConfig.Keyspace) {
				cm.keyspaceExists.Store(false)
				return
			}
			if errors.Is(err, gocb.ErrDocumentNotFound) {
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
	result, err := getCheckpointWithPrefix(cm.getCollectionHandle(), key)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return "", nil
		}
		if !common.CheckKeyspaceExist(cm.observer, cm.checkpointConfig.Keyspace) {
			cm.keyspaceExists.Store(false)
			return "", nil
		}
		return "", err
	}

	vbBlob := &VbBlob{}
	err = result.Content(vbBlob)
	if err != nil {
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

func (cm *checkpointManager) getOwnership(vb uint16, forced bool) (*VbBlob, string, error) {
	key := cm.getKey(vb)
	vbi, uuid, err := NewVbBlobInternal(key, &cm.checkpointConfig, cm, forced)
	if err != nil {
		return nil, uuid, err
	}

	cm.vbBlobSlice[vb] = vbi
	copyVbBlob := vbi.getVbBlob()
	return &copyVbBlob, uuid, nil
}

func (cm *checkpointManager) closeOwnership(vb uint16, vbi *vbBlobInternal) (*VbBlob, error) {
	vbBlob, err := vbi.close()
	if err != nil {
		return nil, err
	}

	delete(cm.vbBlobSlice, vb)
	return vbBlob, nil
}

func (cm *checkpointManager) getKey(vb uint16) string {
	return cm.prefix + fmt.Sprintf(":%d", vb)
}

func (cm *checkpointManager) ownershipTakeover() {
	logPrefix := fmt.Sprintf("checkpointManager::ownershipTakeover[%s]", cm.checkpointConfig.Applocation)
	cm.Lock()
	defer cm.Unlock()

	if len(cm.vbsToOwn) != 0 {
		if !cm.keyspaceExists.Load() {
			for vb, _ := range cm.vbsToOwn {
				vbBlob := getDummyVbBlob()
				delete(cm.vbsToOwn, vb)
				cm.interruptCallback(OWNERSHIP_CLOSED, vb, vbBlob)
			}
			return
		}

		for vb, _ := range cm.vbsToOwn {
			vbBlob, _, err := cm.getOwnership(vb, false)
			if err == errOwnerNotGivenUp {
				continue
			}

			if err != nil {
				logging.Errorf("%s Error while taking over vbucket ownership: %v err: %v", logPrefix, vb, err)
				// TODO: Try different mechanism to check scope or collection is deleted or not
				if common.CheckKeyspaceExist(cm.observer, cm.checkpointConfig.Keyspace) {
					// Ownership not closed continue
					continue
				}

				cm.keyspaceExists.Store(false)
				vbBlob = getDummyVbBlob()
			}

			delete(cm.vbsToOwn, vb)
			cm.interruptCallback(OWNERSHIP_OBTAINED, vb, vbBlob)
		}
	}

	if len(cm.vbsToClose) != 0 {
		for vb, vbBlobInternal := range cm.vbsToClose {
			if !cm.keyspaceExists.Load() {
				for vb, vbi := range cm.vbsToClose {
					vbBlob := vbi.unsetVbBlob()
					delete(cm.vbsToClose, vb)
					cm.interruptCallback(OWNERSHIP_CLOSED, vb, vbBlob)
				}
				return
			}

			vbBlob, err := cm.closeOwnership(vb, vbBlobInternal)
			if err != nil {
				logging.Errorf("%s Error while closing ownership for: %d", logPrefix, vb)
				// TODO: Try different mechanism to check scope or collection is deleted or not
				if common.CheckKeyspaceExist(cm.observer, cm.checkpointConfig.Keyspace) {
					// Ownership not closed continue
					continue
				}
				// keyspace is deleted
				vbBlob = vbBlobInternal.unsetVbBlob()
				cm.keyspaceExists.Store(false)
			}

			delete(cm.vbsToClose, vb)
			cm.interruptCallback(OWNERSHIP_CLOSED, vb, vbBlob)
		}
	}
}

// ReadOnDeployCheckpoint returns contents of the OnDeploy checkpoint from the Metadata collection of the app
func (cm *checkpointManager) ReadOnDeployCheckpoint() (string, uint32, string) {
	logPrefix := fmt.Sprintf("checkpointManager::ReadOnDeployCheckpoint[%s]", cm.checkpointConfig.Applocation)

	nodeLeader, seq, status, err := readOnDeployCheckpoint(cm.checkpointConfig.Applocation, cm.getCollectionHandle())
	if err != nil {
		logging.Errorf("%s Error while reading OnDeploy checkpoint: %v", logPrefix, err)
		if !common.CheckKeyspaceExist(cm.observer, cm.checkpointConfig.Keyspace) {
			cm.keyspaceExists.Store(false)
			status = common.FAILED.String()
		}
	}
	return nodeLeader, seq, status
}

// WriteOnDeployCheckpoint inserts OnDeploy checkpoint in metadata collection,
// retries until OnDeploy checkpoint is successfully inserted and returns if current node is leader
func (cm *checkpointManager) WriteOnDeployCheckpoint(nodeUUID string, seq uint32, appLocation application.AppLocation) bool {
	doc := onDeployCheckpoint{
		NodeUUID:       nodeUUID,
		Seq:            seq,
		OnDeployStatus: common.PENDING.String(),
	}
	key := fmt.Sprintf(onDeployLeaderKeyTemplate, appLocation)
	for {
		_, err := cm.getCollectionHandle().Insert(key, doc, &gocb.InsertOptions{Timeout: opsTimeout})
		if err != nil {
			if errors.Is(err, gocb.ErrDocumentExists) {
				return false
			}
			if !common.CheckKeyspaceExist(cm.observer, cm.checkpointConfig.Keyspace) {
				cm.keyspaceExists.Store(false)
				// Let the caller routine handle this
				return false
			}
			continue
		}
		break
	}
	return true
}

func (cm *checkpointManager) PollUntilOnDeployCompletes() {
	_, _, onDeployStatus := cm.ReadOnDeployCheckpoint()
	for onDeployStatus == common.PENDING.String() {
		time.Sleep(1 * time.Second)
		_, _, onDeployStatus = cm.ReadOnDeployCheckpoint()
	}

	// TODO: Add for select, periodic timer with waitforevent, similar to broadcast observer, for Node failure
}

// PublishOnDeployStatus upserts the status field in the OnDeploy checkpoint
func (cm *checkpointManager) PublishOnDeployStatus(status string) error {
	upsertOptions := &gocb.UpsertSpecOptions{CreatePath: true}
	mutateIn := []gocb.MutateInSpec{gocb.UpsertSpec("on_deploy_status", status, upsertOptions)}
	key := fmt.Sprintf(onDeployLeaderKeyTemplate, cm.checkpointConfig.Applocation)
	_, err := cm.getCollectionHandle().MutateIn(key, mutateIn, &gocb.MutateInOptions{PreserveExpiry: true})
	if err != nil {
		if !common.CheckKeyspaceExist(cm.observer, cm.checkpointConfig.Keyspace) {
			cm.keyspaceExists.Store(false)
			return nil
		}
	}
	return err
}
