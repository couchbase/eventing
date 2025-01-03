package eventPool

import (
	"context"
	"fmt"
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

type managerPool struct {
	sync.RWMutex

	poolID              string
	seqNumID            uint16
	managerID           uint16
	notif               notifier.Observer
	dcpManagers         map[string]dcpManager.DcpManager
	dcpSeqNumberManager map[string]dcpManager.DcpManager

	checkpointManagers map[string]checkpointManager.BucketCheckpoint
	clusterSettings    *common.ClusterSettings
	gocbCluster        *gocb.Cluster

	broadcaster common.Broadcaster
	stats       *stats
	close       func()
}

func noop() {}

func NewManagerPool(ctx context.Context, poolID string, clusterSettings *common.ClusterSettings, notif notifier.Observer, gocbCluster *gocb.Cluster, broadcaster common.Broadcaster) ManagerPool {
	pool := &managerPool{
		poolID:              poolID,
		notif:               notif,
		dcpManagers:         make(map[string]dcpManager.DcpManager),
		checkpointManagers:  make(map[string]checkpointManager.BucketCheckpoint),
		managerID:           uint16(1),
		seqNumID:            uint16(1),
		clusterSettings:     clusterSettings,
		close:               noop,
		broadcaster:         broadcaster,
		dcpSeqNumberManager: make(map[string]dcpManager.DcpManager),
		gocbCluster:         gocbCluster,
		stats:               &stats{},
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

	_, err := pool.notif.RegisterForEvents(sub, bucketListChanges)
	if err != nil {
		logging.Errorf("%s Error fetching bucket list: %v", logPrefix, err)
		return
	}

	// TODO: delete the buckets which are not in the list
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

type stats struct {
	numDedicatedConn uint64
}

func (s *stats) Copy() *stats {
	return &stats{
		numDedicatedConn: s.numDedicatedConn,
	}
}

func (s *stats) String() string {
	return fmt.Sprintf("{ \"num_dedicated_conn\": %d }", s.numDedicatedConn)
}

func (s *stats) MarshalJSON() ([]byte, error) {
	return []byte(s.String()), nil
}

func (pool *managerPool) GetRuntimeStats() common.StatsInterface {
	return pool.stats.Copy()
}

func (pool *managerPool) TlsSettingsChanged(gocbCluster *gocb.Cluster) {
	pool.Lock()
	defer pool.Unlock()

	pool.gocbCluster = gocbCluster
	for _, bucketManager := range pool.checkpointManagers {
		bucketManager.TlsSettingChange(pool.gocbCluster)
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

	m := dcpManager.NewDcpManagerWrapper(manager)
	m.RegisterID(pool.seqNumID, nil)
	pool.seqNumID++

	return m
}

func (pool *managerPool) GetDcpManagerPool(dcpManagerType DcpManagerType, identifier string, bucketName string, sendChannel chan<- *dcpConn.DcpEvent) dcpManager.DcpManager {
	pool.Lock()
	defer pool.Unlock()

	var manager dcpManager.DcpManager
	managerId := uint16(1)

	switch dcpManagerType {
	case DedicatedConn:
		manager = dcpManager.NewDcpManager(dcpConn.StreamRequestMode, identifier, bucketName, pool.notif, nil)

	case CommonConn:
		var ok bool
		manager, ok = pool.dcpManagers[bucketName]
		if !ok {
			manager = dcpManager.NewDcpManager(dcpConn.StreamRequestMode, fmt.Sprintf(commonIdentifier, pool.poolID), bucketName, pool.notif, nil)
			pool.dcpManagers[bucketName] = manager
		}
		managerId = pool.managerID
		pool.managerID++
	}

	m := dcpManager.NewDcpManagerWrapper(manager)
	m.RegisterID(managerId, sendChannel)
	return m
}

func (pool *managerPool) GetCheckpointManager(appId uint32, interruptCallback checkpointManager.InterruptFunction, appLocation application.AppLocation, keyspace application.Keyspace) checkpointManager.Checkpoint {
	pool.Lock()
	defer pool.Unlock()

	manager, ok := pool.checkpointManagers[keyspace.BucketName]
	if !ok {
		manager = checkpointManager.NewBucketCheckpointManager(pool.clusterSettings, keyspace.BucketName, pool.gocbCluster, pool.notif, pool.broadcaster)
		pool.checkpointManagers[keyspace.BucketName] = manager
	}

	return manager.GetCheckpointManager(appId, interruptCallback, appLocation, keyspace)
}

func (pool *managerPool) CloseConditional() {
	pool.Lock()
	defer pool.Unlock()

	for bucketName, manager := range pool.dcpSeqNumberManager {
		deleteManager := manager.CloseConditional()
		if deleteManager {
			delete(pool.dcpSeqNumberManager, bucketName)
		}
	}

	for bucketName, manager := range pool.dcpManagers {
		deleteManager := manager.CloseConditional()
		if deleteManager {
			delete(pool.dcpManagers, bucketName)
		}
	}
}

func (pool *managerPool) closeManagerForBucket(bucketname string) {
	logPrefix := fmt.Sprintf("managerPool::closeManagerForBucket[%s]", pool.poolID)
	// Clear any connection related to this bucket
	pool.Lock()
	defer pool.Unlock()

	logging.Infof("%s closing any cached connections for bucket: %s", logPrefix, bucketname)
	if pool.dcpSeqNumberManager != nil {
		manager, ok := pool.dcpSeqNumberManager[bucketname]
		if ok {
			delete(pool.dcpSeqNumberManager, bucketname)
			manager.CloseManager()
		}
	}

	if pool.dcpManagers != nil {
		manager, ok := pool.dcpManagers[bucketname]
		if ok {
			delete(pool.dcpManagers, bucketname)
			manager.CloseManager()
		}
	}
}

func (pool *managerPool) ClosePool() {
	pool.Lock()
	defer pool.Unlock()

	pool.close()
	pool.closeCheckpointManagerLocked()
	pool.closeDcpSeqManagerPoolLocked()
	pool.closeDcpManagerPoolLocked()
}

func (pool *managerPool) closeCheckpointManagerLocked() {
	pool.checkpointManagers = nil
	pool.gocbCluster = nil
}

func (pool *managerPool) closeDcpSeqManagerPoolLocked() {
	for _, manager := range pool.dcpSeqNumberManager {
		manager.CloseManager()
	}
	pool.dcpSeqNumberManager = nil
}

func (pool *managerPool) closeDcpManagerPoolLocked() {
	for _, manager := range pool.dcpManagers {
		manager.CloseManager()
	}
	pool.dcpManagers = nil
}
