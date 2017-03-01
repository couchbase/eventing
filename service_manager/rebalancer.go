package servicemanager

import (
	"encoding/binary"
	"fmt"
	"net/http"
	_ "net/http/pprof" // For debugging
	"sync"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

//NewServiceMgr creates handle for ServiceMgr, which implements cbuth service.Manager
func NewServiceMgr(config util.Config, rebalanceRunning bool, superSup common.EventingSuperSup) *ServiceMgr {

	logging.Infof("ServiceMgr::newServiceMgr config: %#v rebalanceRunning: %v", config, rebalanceRunning)

	mu := &sync.RWMutex{}

	mgr := &ServiceMgr{
		mu: mu,
		state: state{
			rebalanceID:   "",
			rebalanceTask: nil,
			rev:           0,
			servers:       make([]service.NodeID, 0),
		},
		servers:  make([]service.NodeID, 0),
		superSup: superSup,
	}

	mgr.config.Store(config)
	mgr.nodeInfo = &service.NodeInfo{
		NodeID: service.NodeID(config["uuid"].(string)),
	}

	mgr.rebalanceRunning = rebalanceRunning
	mgr.servers = append(mgr.servers, mgr.nodeInfo.NodeID)
	mgr.waiters = make(waiters)

	go mgr.initService()
	return mgr
}

func (m *ServiceMgr) initService() {
	cfg := m.config.Load()
	eventingAdminPort := cfg["eventing_admin_port"].(string)
	logging.Infof("ServiceMgr::initService eventingAdminPort: %s", eventingAdminPort)

	go m.registerWithServer()

	http.HandleFunc("/getApplication/", m.fetchAppSetup)
	http.HandleFunc("/setApplication/", m.storeAppSetup)
	http.HandleFunc("/getRebalanceProgress", m.getRebalanceProgress)

	logging.Fatalf("%v", http.ListenAndServe(":"+eventingAdminPort, nil))
}

func (m *ServiceMgr) registerWithServer() {
	cfg := m.config.Load()
	logging.Infof("Registering against cbauth_service, uuid: %v", cfg["uuid"].(string))

	err := service.RegisterManager(m, nil)
	if err != nil {
		logging.Errorf("Failed to register against cbauth_service, err: %v", err)
		return
	}
}

func (m *ServiceMgr) prepareRebalance(change service.TopologyChange) error {

	if isSingleNodeRebal(change) {
		if change.KeepNodes[0].NodeInfo.NodeID == m.nodeInfo.NodeID {
			logging.Infof("ServiceMgr::prepareRebalance - only node in the cluster")
		} else {
			return fmt.Errorf("node receiving prepare request isn't part of the cluster")
		}
	}

	return nil
}

func (m *ServiceMgr) startRebalance(change service.TopologyChange) error {

	if isSingleNodeRebal(change) {
		if change.KeepNodes[0].NodeInfo.NodeID == m.nodeInfo.NodeID {
			logging.Infof("ServiceMgr::startRebalance - only node in the cluster")
		} else {
			return fmt.Errorf("node receiving start request isn't part of the cluster")
		}
	}

	return nil
}

// Aggregates rebalance progress from all apps running across all eventing nodes
func (m *ServiceMgr) rebalanceProgress() float64 {
	clusterURL := fmt.Sprintf("127.0.0.1:%s", m.superSup.RestPort())
	u, p, err := cbauth.GetHTTPServiceAuth(clusterURL)
	if err != nil {
		logging.Errorf("Failed to get cluster auth creds, err: %v", err)
		return 0
	}

	auth := fmt.Sprintf("%s:%s", u, p)
	logging.Infof("Cluster auth: %s", auth)

	eventingNodeAddrs, err := util.EventingNodesAddresses(auth, clusterURL)
	if err != nil {
		logging.Errorf("Failed to get eventingNodeAddrs, err: %v", err)
		return 0
	}

	aggProgress := util.GetProgress("/getRebalanceProgress", eventingNodeAddrs)

	return aggProgress
}

func (m *ServiceMgr) updateRebalanceProgressLocked() {
	changeID := m.rebalanceCtx.change.ID
	progress := m.rebalanceProgress()
	rev := m.rebalanceCtx.incRev()
	task := &service.Task{
		Rev:          encodeRev(rev),
		ID:           fmt.Sprintf("rebalance/%s", changeID),
		Type:         service.TaskTypeRebalance,
		Status:       service.TaskStatusRunning,
		IsCancelable: true,
		Progress:     progress,

		Extra: map[string]interface{}{
			"rebalanceID": changeID,
		},
	}

	m.updateStateLocked(func(s *state) {
		s.rebalanceTask = task
	})
}

func (ctx *rebalanceContext) incRev() uint64 {
	curr := ctx.rev
	ctx.rev++

	return curr
}

func (m *ServiceMgr) wait(rev service.Revision, cancel service.Cancel) (state, error) {
	m.mu.Lock()
	unlock := newCleanup(func() { m.mu.Unlock() })
	defer unlock.run()

	currState := m.copyStateLocked()

	if rev == nil {
		return currState, nil
	}

	haveRev := decodeRev(rev)
	if haveRev != m.rev {
		return currState, nil
	}

	ch := m.addWaiterLocked()
	unlock.run()

	select {
	case <-cancel:
		return state{}, service.ErrCanceled
	case newState := <-ch:
		return newState, nil
	}
}

func stateToTaskList(s state) *service.TaskList {
	tasks := &service.TaskList{}

	tasks.Rev = encodeRev(s.rev)
	tasks.Tasks = make([]service.Task, 0)

	if s.rebalanceID != "" {
		id := s.rebalanceID

		task := service.Task{
			Rev:          encodeRev(0),
			ID:           fmt.Sprintf("prepare/%s", id),
			Type:         service.TaskTypePrepared,
			Status:       service.TaskStatusRunning,
			IsCancelable: true,

			Extra: map[string]interface{}{
				"rebalanceID": id,
			},
		}

		tasks.Tasks = append(tasks.Tasks, task)
	}

	if s.rebalanceTask != nil {
		tasks.Tasks = append(tasks.Tasks, *s.rebalanceTask)
	}

	return tasks
}

func (m *ServiceMgr) stateToTopology(s state) *service.Topology {
	topology := &service.Topology{}

	topology.Rev = encodeRev(s.rev)
	topology.Nodes = append([]service.NodeID(nil), m.servers...)
	topology.IsBalanced = true
	topology.Messages = nil

	return topology
}

func (m *ServiceMgr) addWaiterLocked() waiter {
	ch := make(waiter, 1)
	m.waiters[ch] = struct{}{}

	return ch
}

func (m *ServiceMgr) removeWaiter(w waiter) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.waiters, w)
}

func (m *ServiceMgr) copyStateLocked() state {
	return m.state
}

func isSingleNodeRebal(change service.TopologyChange) bool {
	if len(change.KeepNodes) == 1 && len(change.EjectNodes) == 0 {
		return true
	}
	return false
}

func (m *ServiceMgr) updateStateLocked(body func(state *state)) {
	body(&m.state)
	m.state.rev++

	m.notifyWaitersLocked()
}

func (m *ServiceMgr) notifyWaitersLocked() {
	s := m.copyStateLocked()
	for ch := range m.waiters {
		if ch != nil {
			ch <- s
		}
	}

	m.waiters = make(waiters)
}

func decodeRev(b service.Revision) uint64 {
	return binary.BigEndian.Uint64(b)
}

func encodeRev(rev uint64) service.Revision {
	ext := make(service.Revision, 8)
	binary.BigEndian.PutUint64(ext, rev)

	return ext
}

func newCleanup(f func()) *cleanup {
	return &cleanup{
		canceled: false,
		f:        f,
	}
}

func (c *cleanup) run() {
	if !c.canceled {
		c.f()
		c.cancel()
	}
}

func (c *cleanup) cancel() {
	c.canceled = true
}
