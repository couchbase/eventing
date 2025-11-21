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
	commonIdentifier    = "common_%s"
	seqNumberIdentifier = "seqNumber_"
)

type stats struct {
	CommDcpConn map[string]uint64 `json:"common_dcp_conn"`
	CommSeqConn map[string]uint64 `json:"common_seq_conn"`
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

func (s *stats) MarshalJSON() ([]byte, error) {
	return json.Marshal(s)
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

	dcpManagers         map[string]dcpManager.DcpManager
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

		dcpManagers:         make(map[string]dcpManager.DcpManager),
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

	bucketList := bucketListInterface.(map[string]string)
	deletedBucekts := make(map[string]struct{})
	pool.RLock()
	for bucketName := range pool.dcpManagers {
		if _, ok := bucketList[bucketName]; !ok {
			deletedBucekts[bucketName] = struct{}{}
		}
	}

	for bucketName := range pool.dcpSeqNumberManager {
		if _, ok := bucketList[bucketName]; !ok {
			deletedBucekts[bucketName] = struct{}{}
		}
	}
	pool.RUnlock()

	for bucketName := range deletedBucekts {
		pool.closeManagerForBucket(bucketName)
	}

	for {
		select {
		case trans := <-sub.WaitForEvent():
			if trans == nil {
				logging.Errorf("%s observer event got closed. Restarting...", logPrefix)
				return
			}

			if trans.Deleted {
				pool.closeManagerForBucket(trans.Event.Filter)
				return
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
		manager = dcpManager.NewDcpManager(dcpConn.InfoMode, pool.poolID, bucketName, pool.notif, nil)
		pool.dcpSeqNumberManager[bucketName] = manager
	}
	seqNum := pool.seqNumID
	pool.seqNumID++

	m := dcpManager.NewDcpManagerWrapper(manager)
	m.RegisterID(seqNum, nil)
	pool.stats.CommSeqConn[bucketName]++

	return m
}

func (pool *managerPool) GetDcpManagerPool(dcpManagerType DcpManagerType, identifier string, bucketName string, sendChannel chan<- *dcpConn.DcpEvent) dcpManager.DcpManager {
	var manager dcpManager.DcpManager
	managerId := uint16(1)

	switch dcpManagerType {
	case DedicatedConn:
		manager = dcpManager.NewDcpManager(dcpConn.StreamRequestMode, identifier, bucketName, pool.notif, nil)

	case CommonConn:
		var ok bool

		pool.Lock()
		defer pool.Unlock()
		manager, ok = pool.dcpManagers[bucketName]
		if !ok {
			manager = dcpManager.NewDcpManager(dcpConn.StreamRequestMode, fmt.Sprintf(commonIdentifier, pool.poolID), bucketName, pool.notif, nil)
			pool.dcpManagers[bucketName] = manager
		}
		managerId = pool.managerID
		pool.managerID++
		pool.stats.CommDcpConn[bucketName]++
	}

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
	for bucketName, manager := range pool.dcpManagers {
		deleteManager := manager.ClosePossible()
		if deleteManager {
			dcpMgr = append(dcpMgr, manager)
			delete(pool.dcpManagers, bucketName)
			delete(pool.stats.CommDcpConn, bucketName)
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
	seqMgr, ok := pool.dcpSeqNumberManager[bucketname]
	if ok {
		delete(pool.dcpSeqNumberManager, bucketname)
		delete(pool.stats.CommSeqConn, bucketname)
	}

	dcpMgr, ok := pool.dcpManagers[bucketname]
	if ok {
		delete(pool.dcpManagers, bucketname)
		delete(pool.stats.CommDcpConn, bucketname)
	}
	pool.Unlock()

	if seqMgr != nil {
		seqMgr.CloseManager()
	}

	if dcpMgr != nil {
		dcpMgr.CloseManager()
	}
}

// ClosePool closes all the managers in the pool
// After this call any function won't be called any function on the pool
func (pool *managerPool) ClosePool() {
	pool.close()

	pool.Lock()
	checkpointManagers := pool.checkpointManagers
	pool.checkpointManagers = make(map[string]checkpointManager.BucketCheckpoint)

	dcpManagers := pool.dcpManagers
	pool.dcpManagers = make(map[string]dcpManager.DcpManager)

	dcpSeqNumberManager := pool.dcpSeqNumberManager
	pool.dcpSeqNumberManager = make(map[string]dcpManager.DcpManager)
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
