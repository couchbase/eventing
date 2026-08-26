package eventPool

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/couchbase/eventing/application"
	checkpointManager "github.com/couchbase/eventing/checkpoint_manager"
	"github.com/couchbase/eventing/common"
	dcpConn "github.com/couchbase/eventing/dcp_connection"
	dcpManager "github.com/couchbase/eventing/dcp_manager"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
	"github.com/couchbase/gocb/v2"
)

const (
	commonIdentifier    = "common_%s_%d"
	seqNumberIdentifier = "seqNumber_"
)

type dcpManagerKey struct {
	bucketName string
	configSig  string
}

func (dk dcpManagerKey) String() string {
	return fmt.Sprintf("{ bucket: %s, config: %s }", dk.bucketName, dk.configSig)
}

func newDcpManagerKey(bucketName string, config map[dcpConn.ConfigKey]interface{}) dcpManagerKey {
	managerKey := dcpManagerKey{
		bucketName: bucketName,
		configSig:  dcpConn.DcpConfigSignature(config),
	}

	return managerKey
}

type stats struct {
	CommSeqConn map[string]uint64 `json:"common_seq_conn"`
	CommDcpConn map[string]uint64 `json:"common_dcp_conn"`
}

func newStats() *stats {
	return &stats{
		CommDcpConn: make(map[string]uint64),
		CommSeqConn: make(map[string]uint64),
	}
}

func (s *stats) Copy() *stats {
	copyStats := newStats()
	maps.Copy(copyStats.CommDcpConn, s.CommDcpConn)
	maps.Copy(copyStats.CommSeqConn, s.CommSeqConn)
	return copyStats
}

type statsAlias stats

func (s *stats) MarshalJSON() ([]byte, error) {
	return json.Marshal((*statsAlias)(s))
}

func (s *stats) removeDcpConnection(bucketName string) {
	count, ok := s.CommDcpConn[bucketName]
	if !ok || count <= 1 {
		delete(s.CommDcpConn, bucketName)
		return
	}
	s.CommDcpConn[bucketName] = count - 1
}

func (s *stats) removeBucket(bucketName string) {
	delete(s.CommDcpConn, bucketName)
	delete(s.CommSeqConn, bucketName)
}

type managerPool struct {
	sync.RWMutex

	poolID          string
	seqNumID        uint16
	managerID       uint16
	notif           notifier.Observer
	broadcaster     common.Broadcaster
	clusterSettings *common.ClusterSettings
	gocbCluster     *common.AtomicTypes[*gocb.Cluster]

	// Keyed on bucket + dcp config so that callers wanting different
	// connection settings never end up sharing one connection.
	dcpManagers         map[dcpManagerKey]dcpManager.DcpManager
	dcpConnID           uint64
	dcpSeqNumberManager map[string]dcpManager.DcpManager
	checkpointManagers  map[string]checkpointManager.BucketCheckpoint
	stats               *stats

	close func()
}

func noop() {}

func NewManagerPool(ctx context.Context, poolID string, clusterSettings *common.ClusterSettings, notif notifier.Observer, gocbCluster *gocb.Cluster, broadcaster common.Broadcaster) ManagerPool {
	pool := &managerPool{
		poolID:          poolID,
		notif:           notif,
		managerID:       uint16(1),
		seqNumID:        uint16(1),
		clusterSettings: clusterSettings,
		close:           noop,
		broadcaster:     broadcaster,
		gocbCluster:     common.NewAtomicTypes(gocbCluster),

		dcpManagers:         make(map[dcpManagerKey]dcpManager.DcpManager),
		dcpSeqNumberManager: make(map[string]dcpManager.DcpManager),
		checkpointManagers:  make(map[string]checkpointManager.BucketCheckpoint),
		stats:               newStats(),
	}

	cancelCtx, close := context.WithCancel(ctx)
	pool.close = close
	go pool.observe(cancelCtx)
	return pool
}

func (pool *managerPool) observe(ctx context.Context) {
	logPrefix := fmt.Sprintf("eventPool::observe[%s]", pool.poolID)
	sub := pool.notif.GetSubscriberObject()

	bucketListChanges := notifier.InterestedEvent{
		Event: notifier.EventBucketListChanges,
	}

	defer func() {
		pool.notif.DeregisterEvent(sub, bucketListChanges)

		select {
		case <-ctx.Done():
			return
		default:
		}

		time.Sleep(time.Second)
		go pool.observe(ctx)
	}()

	bucketListInterface, err := pool.notif.RegisterForEvents(sub, bucketListChanges)
	if err != nil {
		logging.Errorf("%s Error fetching bucket list: %v", logPrefix, err)
		return
	}

	bucketList, ok := bucketListInterface.(map[string]string)
	if !ok {
		logging.Errorf("%s unexpected bucket list type: %T", logPrefix, bucketListInterface)
		return
	}

	deletedBuckets := make(map[string]struct{})
	pool.RLock()
	for key := range pool.dcpManagers {
		if _, ok := bucketList[key.bucketName]; !ok {
			deletedBuckets[key.bucketName] = struct{}{}
		}
	}

	for bucketName := range pool.dcpSeqNumberManager {
		if _, ok := bucketList[bucketName]; !ok {
			deletedBuckets[bucketName] = struct{}{}
		}
	}

	for bucketName := range pool.checkpointManagers {
		if _, ok := bucketList[bucketName]; !ok {
			deletedBuckets[bucketName] = struct{}{}
		}
	}
	pool.RUnlock()

	for bucketName := range deletedBuckets {
		pool.closeManagerForBucket(bucketName)
	}

	for {
		select {
		case trans := <-sub.WaitForEvent():
			if trans == nil {
				logging.Errorf("%s observer event got closed. Restarting...", logPrefix)
				return
			}

			// Keep observing: a bucket deletion is a normal event, not a reason
			// to tear down the subscription and re-register a second later.
			if trans.Deleted {
				pool.closeManagerForBucket(trans.Event.Filter)
			}

		case <-ctx.Done():
			return
		}
	}
}

func (pool *managerPool) GetRuntimeStats() common.StatsInterface {
	pool.RLock()
	defer pool.RUnlock()

	return pool.stats.Copy()
}

func (pool *managerPool) TlsSettingsChanged(gocbCluster *gocb.Cluster) {
	pool.gocbCluster.Store(gocbCluster)

	pool.RLock()
	checkpointManagers := make([]checkpointManager.BucketCheckpoint, 0, len(pool.checkpointManagers))
	for _, manager := range pool.checkpointManagers {
		checkpointManagers = append(checkpointManagers, manager)
	}
	pool.RUnlock()

	for _, bucketManager := range checkpointManagers {
		bucketManager.TlsSettingChange(pool.gocbCluster.Load())
	}
}

func (pool *managerPool) GetSeqManager(bucketName string) SeqNumerInterface {
	pool.Lock()
	defer pool.Unlock()

	manager, ok := pool.dcpSeqNumberManager[bucketName]
	if !ok {
		managerConfig := dcpManager.ManagerType{
			Mode:        dcpConn.InfoMode,
			SeqInterval: 0,
		}
		manager = dcpManager.NewDcpManager(managerConfig, pool.poolID, bucketName, pool.notif, nil)
		pool.dcpSeqNumberManager[bucketName] = manager
		pool.stats.CommSeqConn[bucketName]++
	}
	seqNum := pool.seqNumID
	pool.seqNumID++

	m := dcpManager.NewDcpManagerWrapper(manager)
	m.RegisterID(seqNum, nil)

	return m
}

func (pool *managerPool) GetDcpManagerPool(dcpManagerType DcpManagerType, identifier string, bucketName string, sendChannel chan<- *dcpConn.DcpEvent, dcpConnConfig map[dcpConn.ConfigKey]interface{}) dcpManager.DcpManager {
	if dcpManagerType == DedicatedConn {
		managerConfig := dcpManager.ManagerType{
			Mode:        dcpConn.StreamRequestMode,
			SeqInterval: 0,
		}

		manager := dcpManager.NewDcpManager(managerConfig, identifier, bucketName, pool.notif, dcpConnConfig)
		m := dcpManager.NewDcpManagerWrapper(manager)
		m.RegisterID(uint16(1), sendChannel)
		return m
	}

	key := newDcpManagerKey(bucketName, dcpConnConfig)
	pool.Lock()
	defer pool.Unlock()

	manager, ok := pool.dcpManagers[key]
	if !ok {
		managerConfig := dcpManager.ManagerType{
			Mode:        dcpConn.StreamRequestMode,
			SeqInterval: 0,
		}

		pool.dcpConnID++
		identifier := fmt.Sprintf(commonIdentifier, pool.poolID, pool.dcpConnID)

		manager = dcpManager.NewDcpManager(managerConfig, identifier, bucketName, pool.notif, dcpConnConfig)
		pool.dcpManagers[key] = manager
		pool.stats.CommDcpConn[bucketName]++

		logging.Infof("eventPool::GetDcpManagerPool[%s] new shared dcp connection %s for bucket: %s config: %s",
			pool.poolID, identifier, bucketName, key.configSig)
	}

	managerId := pool.managerID
	pool.managerID++

	m := dcpManager.NewDcpManagerWrapper(manager)
	m.RegisterID(managerId, sendChannel)
	return m
}

func (pool *managerPool) GetCheckpointManager(appId uint32, appInstanceID string, interruptCallback checkpointManager.InterruptFunction, appLocation application.AppLocation, keyspace application.Keyspace) checkpointManager.Checkpoint {
	pool.Lock()
	manager, ok := pool.checkpointManagers[keyspace.BucketName]
	if !ok {
		manager = checkpointManager.NewBucketCheckpointManager(pool.clusterSettings, keyspace.BucketName, pool.gocbCluster.Load(), pool.notif, pool.broadcaster)
		pool.checkpointManagers[keyspace.BucketName] = manager
	}
	pool.Unlock()

	manager.TlsSettingChange(pool.gocbCluster.Load())
	return manager.GetCheckpointManager(appId, appInstanceID, interruptCallback, appLocation, keyspace)
}

func (pool *managerPool) CloseConditional() {
	pool.Lock()
	seqMgr := make([]dcpManager.DcpManager, 0, len(pool.dcpSeqNumberManager))
	for bucketName, manager := range pool.dcpSeqNumberManager {
		deleteManager := manager.ClosePossible()
		if deleteManager {
			seqMgr = append(seqMgr, manager)
			delete(pool.dcpSeqNumberManager, bucketName)
			delete(pool.stats.CommSeqConn, bucketName)
		}
	}

	dcpMgr := make([]dcpManager.DcpManager, 0, len(pool.dcpManagers))
	for key, manager := range pool.dcpManagers {
		deleteManager := manager.ClosePossible()
		if deleteManager {
			dcpMgr = append(dcpMgr, manager)
			delete(pool.dcpManagers, key)
			pool.stats.removeDcpConnection(key.bucketName)
		}
	}
	pool.Unlock()

	for _, manager := range seqMgr {
		manager.CloseManager()
	}

	for _, manager := range dcpMgr {
		manager.CloseManager()
	}
}

// Clear any connection related to this bucket
func (pool *managerPool) closeManagerForBucket(bucketname string) {
	logging.Infof("managerPool::closeManagerForBucket[%s] closing any cached connections for bucket: %s", pool.poolID, bucketname)

	pool.Lock()
	seqMgr := pool.dcpSeqNumberManager[bucketname]
	delete(pool.dcpSeqNumberManager, bucketname)

	// A bucket can hold more than one shared connection, one per dcp config.
	dcpMgrs := make([]dcpManager.DcpManager, 0, len(pool.dcpManagers))
	for key, manager := range pool.dcpManagers {
		if key.bucketName != bucketname {
			continue
		}
		dcpMgrs = append(dcpMgrs, manager)
		delete(pool.dcpManagers, key)
	}

	checkpointMgr := pool.checkpointManagers[bucketname]
	delete(pool.checkpointManagers, bucketname)

	pool.stats.removeBucket(bucketname)
	pool.Unlock()

	if seqMgr != nil {
		seqMgr.CloseManager()
	}

	for _, dcpMgr := range dcpMgrs {
		dcpMgr.CloseManager()
	}

	if checkpointMgr != nil {
		checkpointMgr.CloseBucketManager()
	}
}

func (pool *managerPool) ClosePool() {
	pool.close()

	pool.Lock()
	checkpointManagers := pool.checkpointManagers
	pool.checkpointManagers = make(map[string]checkpointManager.BucketCheckpoint)

	dcpManagers := pool.dcpManagers
	pool.dcpManagers = make(map[dcpManagerKey]dcpManager.DcpManager)

	dcpSeqNumberManager := pool.dcpSeqNumberManager
	pool.dcpSeqNumberManager = make(map[string]dcpManager.DcpManager)

	pool.stats = newStats()
	pool.Unlock()

	for _, manager := range checkpointManagers {
		manager.CloseBucketManager()
	}

	for _, manager := range dcpManagers {
		manager.CloseManager()
	}

	for _, manager := range dcpSeqNumberManager {
		manager.CloseManager()
	}
}
