package servicemanager

import (
	"fmt"
	"net/http"
	_ "net/http/pprof" // For debugging
	"sync"
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

//NewServiceMgr creates handle for ServiceMgr, which implements cbauth service.Manager
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
		servers:      make([]service.NodeID, 0),
		superSup:     superSup,
		stopTracerCh: make(chan struct{}, 1),
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
	m.eventingAdminPort = cfg["eventing_admin_port"].(string)
	m.restPort = cfg["rest_port"].(string)
	m.uuid = cfg["uuid"].(string)
	m.initErrCodes()

	logging.Infof("ServiceMgr::initService eventingAdminPort: %s", m.eventingAdminPort)

	util.Retry(util.NewFixedBackoff(time.Second), getHTTPServiceAuth, m)

	go func(m *ServiceMgr) {
		for {
			err := m.registerWithServer()
			if err != nil {
				logging.Infof("Retrying to register against cbauth_service")
				time.Sleep(2 * time.Second)
			} else {
				break
			}
		}
	}(m)

	// TODO: Rest endpoints are growing big, need to document in source code purpose of each
	// Eventually it would work as documentation.

	http.HandleFunc("/clearEventStats", m.clearEventStats)
	http.HandleFunc("/getErrorCodes", m.getErrCodes)
	http.HandleFunc("/deleteApplication/", m.deleteApplication)
	http.HandleFunc("/deleteAppTempStore/", m.deleteAppTempStore)
	http.HandleFunc("/getAggRebalanceProgress", m.getAggRebalanceProgress)
	http.HandleFunc("/getApplication/", m.fetchAppSetup)
	http.HandleFunc("/getAppTempStore/", m.fetchAppTempStore)
	http.HandleFunc("/getAggEventsPSec", m.getAggEventsProcessedPSec)
	http.HandleFunc("/getEventProcessingStats", m.getEventProcessingStats)
	http.HandleFunc("/getEventsPSec", m.getEventsProcessedPSec)
	http.HandleFunc("/getDeployedApps", m.getDeployedApps)
	http.HandleFunc("/getRebalanceProgress", m.getRebalanceProgress)
	http.HandleFunc("/getSeqsProcessed", m.getSeqsProcessed)
	http.HandleFunc("/saveAppTempStore/", m.saveAppSetup)
	http.HandleFunc("/setApplication/", m.storeAppSetup)
	http.HandleFunc("/setSettings/", m.storeAppSettings)
	http.HandleFunc("/debugUrl/", m.getLocalDebuggerURL)
	http.HandleFunc("/getDebuggerUrl/", m.getDebuggerURL)
	http.HandleFunc("/debugging/", m.debugging)
	http.HandleFunc("/startDebugger/", m.startDebugger)
	http.HandleFunc("/stopDebugger/", m.stopDebugger)
	http.HandleFunc("/startTracing", m.startTracer)
	http.HandleFunc("/stopTracing", m.stopTracer)
	http.HandleFunc("/getTimerHostPortAddrs", m.getTimerHostPortAddrs)
	http.HandleFunc("/getAggTimerHostPortAddrs", m.getAggTimerHostPortAddrs)
	http.HandleFunc("/uuid", m.getNodeUUID)

	logging.Fatalf("%v", http.ListenAndServe(":"+m.eventingAdminPort, nil))
}

func (m *ServiceMgr) registerWithServer() error {
	cfg := m.config.Load()
	logging.Infof("Registering against cbauth_service, uuid: %v", cfg["uuid"].(string))

	err := service.RegisterManager(m, nil)
	if err != nil {
		logging.Errorf("Failed to register against cbauth_service, err: %v", err)
		return err
	}

	return nil
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

	if isSingleNodeRebal(change) && !m.failoverNotif {
		if change.KeepNodes[0].NodeInfo.NodeID == m.nodeInfo.NodeID {
			logging.Infof("ServiceMgr::startRebalance - only node in the cluster")
			m.updateRebalanceProgressLocked(1.0)
		} else {
			return fmt.Errorf("node receiving start request isn't part of the cluster")
		}
		return nil
	}

	// Reset the failoverNotif flag, which got set to signify failover action on the cluster
	if m.failoverNotif {
		m.failoverNotif = false
	}

	m.rebalanceCtx = &rebalanceContext{
		change: change,
		rev:    0,
	}

	// Garbage collect old Rebalance Tokens
	err := util.RecursiveDelete(metakvRebalanceTokenPath)
	if err != nil {
		logging.Errorf("SMRB ServiceMgr::StartTopologyChange Failed to garbage collect old rebalance token(s) from metakv, err: %v", err)
		return err
	}

	path := metakvRebalanceTokenPath + change.ID
	err = util.MetakvSet(path, []byte(change.ID), nil)
	if err != nil {
		logging.Errorf("SMRB ServiceMgr::StartTopologyChange Failed to store rebalance token in metakv, err: %v", err)
		return err
	}

	m.updateRebalanceProgressLocked(0.0)

	return nil
}

func (m *ServiceMgr) updateRebalanceProgressLocked(progress float64) {
	changeID := m.rebalanceCtx.change.ID
	rev := m.rebalanceCtx.incRev()

	task := &service.Task{
		Rev:          encodeRev(rev),
		ID:           fmt.Sprintf("%s", changeID),
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
	unlock := newCleanup(func() {
		m.mu.Unlock()
	})
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

func (m *ServiceMgr) cancelActualTaskLocked(task *service.Task) error {
	switch task.Type {
	case service.TaskTypeRebalance:
		return m.cancelRebalanceTaskLocked(task)
	default:
		panic("can't happen")
	}
}

func (m *ServiceMgr) cancelRebalanceTaskLocked(task *service.Task) error {
	switch task.Status {
	case service.TaskStatusRunning:
		return m.cancelRunningRebalanceTaskLocked(task)
	case service.TaskStatusFailed:
		return m.cancelFailedRebalanceTaskLocked()
	default:
		panic("can't happen")
	}
}

func (m *ServiceMgr) cancelRunningRebalanceTaskLocked(task *service.Task) error {
	m.rebalancer.cancel()
	m.onRebalanceDoneLocked(nil)

	path := metakvRebalanceTokenPath + task.ID
	err := util.MetakvSet(path, []byte(stopRebalance), nil)
	if err != nil {
		logging.Errorf("SMRB Failed to update rebalance token: %v in metakv as part of stop running rebalance, err: %v",
			task.ID, err)
		return err
	}

	return nil
}

func (m *ServiceMgr) cancelFailedRebalanceTaskLocked() error {
	m.updateStateLocked(func(s *state) {
		s.rebalanceTask = nil
	})

	return nil
}

func isSingleNodeRebal(change service.TopologyChange) bool {
	if len(change.KeepNodes) == 1 && len(change.EjectNodes) == 0 {
		return true
	}
	return false
}

func (m *ServiceMgr) updateState(body func(state *state)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.updateStateLocked(body)
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

func (m *ServiceMgr) runRebalanceCallback(cancel <-chan struct{}, body func()) {

	done := make(chan struct{})

	go func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		select {
		case <-cancel:
			break
		default:
			body()
		}

		close(done)
	}()

	select {
	case <-done:
	case <-cancel:
	}
}

func (m *ServiceMgr) rebalanceProgressCallback(progress float64, cancel <-chan struct{}) {
	m.runRebalanceCallback(cancel, func() {
		m.updateRebalanceProgressLocked(progress)
	})
}

func (m *ServiceMgr) rebalanceDoneCallback(err error, cancel <-chan struct{}) {
	m.runRebalanceCallback(cancel, func() {
		m.onRebalanceDoneLocked(err)
	})
}

func (m *ServiceMgr) onRebalanceDoneLocked(err error) {
	newTask := (*service.Task)(nil)
	if err != nil {
		ctx := m.rebalanceCtx
		rev := ctx.incRev()

		newTask = &service.Task{
			Rev:          encodeRev(rev),
			ID:           fmt.Sprintf("rebalance/%s", ctx.change.ID),
			Type:         service.TaskTypeRebalance,
			Status:       service.TaskStatusFailed,
			IsCancelable: true,

			ErrorMessage: err.Error(),

			Extra: map[string]interface{}{
				"rebalanceId": ctx.change.ID,
			},
		}
	}

	m.rebalancer = nil
	m.rebalanceCtx = nil

	m.updateStateLocked(func(s *state) {
		s.rebalanceTask = newTask
		s.rebalanceID = ""
	})
}
