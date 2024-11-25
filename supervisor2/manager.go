package supervisor2

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/supervisor2/distributor"
)

type Supervisor interface {
	GetGlobalRebalanceProgress(version string) (float64, error)
	SyncPhaseDone() bool
}

const (
	OldEventingVersionPriority = service.Priority(0)
	ReserverdPriority          = service.Priority(-1)
	LeaderPriority             = service.Priority(-2)
	NewEventingVersionPriority = service.Priority(-3)
	NewlyAddedPriority         = service.Priority(-4)
)

type Service interface {
	InitServiceManagerWithSup(sup Supervisor)

	GetNodeInfo() (*service.NodeInfo, error)
	Shutdown() error

	GetTaskList(rev service.Revision, cancel service.Cancel) (*service.TaskList, error)
	CancelTask(id string, rev service.Revision) error

	GetCurrentTopology(rev service.Revision, cancel service.Cancel) (*service.Topology, error)

	PrepareTopologyChange(change service.TopologyChange) error
	StartTopologyChange(change service.TopologyChange) error
}

type rebalanceContext struct {
	change service.TopologyChange
	rev    uint64
}

func (ctx *rebalanceContext) incRev() uint64 {
	curr := ctx.rev
	ctx.rev++

	return curr
}

type rebalanceService struct {
	rebalanceID   string
	rebalanceTask *service.Task

	rebalancer   *rebalancer
	rebalanceCtx *rebalanceContext
}

func (rs *rebalanceService) incRev() uint64 {
	return rs.rebalanceCtx.incRev()
}

func (rs *rebalanceService) getTask(taskList []service.Task) []service.Task {
	if rs.rebalanceID != "" {
		id := rs.rebalanceID

		task := service.Task{
			Rev:          encodeRev(0),
			ID:           fmt.Sprintf("prepare/%s", id),
			Type:         service.TaskTypePrepared,
			Status:       service.TaskStatusRunning,
			IsCancelable: true,

			Extra: map[string]interface{}{
				"rebalanceId": id,
			},
		}
		taskList = append(taskList, task)
	}

	if rs.rebalanceTask != nil {
		taskList = append(taskList, *rs.rebalanceTask)
	}

	return taskList
}

type stateDetails struct {
	taskList *service.TaskList
	topology *service.Topology
}

type detailsType uint8

const (
	topologyDetails detailsType = iota
	taskListDetails
	allDetails
)

type state struct {
	rev uint64

	isBalanced       bool
	nodeID           service.NodeID
	rebalanceService *rebalanceService
	servers          []service.NodeID
}

func newState(uuid string) state {
	node := service.NodeID(uuid)
	return state{
		rev:              uint64(0),
		isBalanced:       true,
		nodeID:           node,
		servers:          []service.NodeID{node},
		rebalanceService: &rebalanceService{},
	}
}

func (s state) getStateDetailsLocked(details detailsType) stateDetails {
	sd := stateDetails{}
	switch details {
	case topologyDetails:
		sd.topology = stateToTopology(s)

	case taskListDetails:
		sd.taskList = stateToTaskList(s)

	case allDetails:
		sd.taskList = stateToTaskList(s)
		sd.topology = stateToTopology(s)
	}

	return sd
}

func stateToTopology(s state) *service.Topology {
	topology := &service.Topology{}
	topology.Rev = encodeRev(s.rev)

	if len(s.servers) != 0 {
		topology.Nodes = append([]service.NodeID(nil), s.servers...)
	} else {
		topology.Nodes = append([]service.NodeID(nil), s.nodeID)
	}
	topology.IsBalanced = s.isBalanced
	topology.Messages = nil
	return topology
}

func stateToTaskList(s state) *service.TaskList {
	tasks := &service.TaskList{}
	tasks.Rev = encodeRev(s.rev)
	tasks.Tasks = make([]service.Task, 0)

	tasks.Tasks = s.rebalanceService.getTask(tasks.Tasks)
	return tasks
}

type waiter chan stateDetails
type waiters map[waiter]struct{}

type serviceManager struct {
	sync.RWMutex
	waiters waiters

	nodeInfo    *atomic.Value
	sup         Supervisor
	distributor distributor.Distributor

	state
}

func NewServiceManager(distributor distributor.Distributor, nodeUUID string) Service {
	sm := &serviceManager{
		waiters: make(waiters),

		nodeInfo:    &atomic.Value{},
		distributor: distributor,

		state: newState(nodeUUID),
	}

	nodeInfo := &service.NodeInfo{
		NodeID:   service.NodeID(nodeUUID),
		Priority: NewlyAddedPriority,
	}
	sm.nodeInfo.Store(nodeInfo)
	return sm
}

// setStateIsBalanced sets the m.state.isBalanced flag to the value passed in.
func (sm *serviceManager) setStateIsBalanced(isBal bool) {
	sm.Lock()
	sm.isBalanced = isBal
	sm.Unlock()
}

func (sm *serviceManager) InitServiceManagerWithSup(sup Supervisor) {
	logPrefix := "supervisor::InitServiceManagerWithSup"

	sm.sup = sup
	go func() {
		for {
			err := service.RegisterManager(sm, nil)
			if err != nil {
				logging.Errorf("%s Error registering with cbauth_service: %v. Retrying...: %v", logPrefix, err)
				time.Sleep(time.Second)
				continue
			}
			logging.Infof("%s Successfully registerd with cbauth_service", logPrefix)
			return
		}
	}()

	return
}

func (sm *serviceManager) copyStateLocked() stateDetails {
	return sm.getStateDetailsLocked(allDetails)
}

func (sm *serviceManager) GetNodeInfo() (*service.NodeInfo, error) {
	return sm.nodeInfo.Load().(*service.NodeInfo), nil
}

func (sm *serviceManager) Shutdown() error {
	os.Exit(0)
	return nil
}

func (sm *serviceManager) GetTaskList(rev service.Revision,
	cancel service.Cancel) (*service.TaskList, error) {

	state, err := sm.wait(rev, cancel)
	if err != nil {
		return nil, err
	}

	return state.taskList, nil
}

func (sm *serviceManager) CancelTask(id string, rev service.Revision) error {
	sm.Lock()
	defer sm.Unlock()

	details := sm.state.getStateDetailsLocked(taskListDetails)
	task := (*service.Task)(nil)

	for i, _ := range details.taskList.Tasks {
		t := &details.taskList.Tasks[i]

		if t.ID == id {
			task = t
			break
		}
	}

	if task == nil {
		return service.ErrNotFound
	}

	if !task.IsCancelable {
		return service.ErrNotSupported
	}

	if rev != nil && !bytes.Equal(rev, task.Rev) {
		return service.ErrConflict
	}

	return sm.cancelActualTaskLocked(task)
}

func (sm *serviceManager) GetCurrentTopology(rev service.Revision,
	cancel service.Cancel) (*service.Topology, error) {

	state, err := sm.wait(rev, cancel)
	if err != nil {
		return nil, err
	}

	return state.topology, nil
}

func (sm *serviceManager) PrepareTopologyChange(change service.TopologyChange) error {
	logPrefix := "supervisor::PrepareTopologyChange"

	topologyJson, _ := json.Marshal(change)
	logging.Infof("%s Called with change %s nodeUUID: %s", logPrefix, topologyJson, sm.nodeID)

	sm.RLock()
	rebalanceID := sm.rebalanceService.rebalanceID
	sm.RUnlock()

	if rebalanceID != "" {
		logging.Errorf("%s conflict detected: %s", logPrefix, rebalanceID)
		if change.Type == service.TopologyChangeTypeRebalance {
			sm.setStateIsBalanced(false)
		}
		return service.ErrConflict
	}

	switch change.Type {
	case service.TopologyChangeTypeFailover, service.TopologyChangeTypeRebalance:
	default:
		return service.ErrNotSupported
	}

	nodeList := make([]service.NodeID, 0)
	for _, node := range change.EjectNodes {
		nodeList = append(nodeList, node.NodeID)
	}

	for _, node := range change.KeepNodes {
		nodeList = append(nodeList, node.NodeInfo.NodeID)
	}

	sm.updateState(func(s *state) {
		s.rebalanceService.rebalanceID = change.ID
		s.servers = nodeList
	})

	return nil
}

func (sm *serviceManager) StartTopologyChange(change service.TopologyChange) error {
	logPrefix := "supervisor:StartTopologyChange"
	sm.RLock()

	topologyJson, _ := json.Marshal(change)
	logging.Infof("%s Called with change %s nodeUUID: %s", logPrefix, topologyJson, sm.nodeID)

	if sm.rebalanceService.rebalanceID != change.ID || sm.rebalanceService.rebalancer != nil {
		sm.RUnlock()
		logging.Errorf("%s conflict detected: %s", logPrefix)
		return service.ErrConflict
	}

	if change.CurrentTopologyRev != nil {
		haveRev := decodeRev(change.CurrentTopologyRev)
		if haveRev != sm.rev {
			sm.RUnlock()
			logging.Errorf("%s conflict detected: %s haveRev : %v", logPrefix, haveRev)
			return service.ErrConflict
		}
	}
	sm.RUnlock()

	keepNodes := make([]string, 0, len(change.KeepNodes))
	for _, node := range change.KeepNodes {
		keepNodes = append(keepNodes, string(node.NodeInfo.NodeID))
	}

	err := sm.distributor.ReDistribute(change.ID, keepNodes)
	if err != nil {
		sm.setStateIsBalanced(false)
		return err
	}

	ctx := &rebalanceContext{
		rev:    0,
		change: change,
	}

	nodeInfo := sm.nodeInfo.Load().(*service.NodeInfo)
	newNodeInfo := &service.NodeInfo{
		NodeID:   nodeInfo.NodeID,
		Priority: LeaderPriority,
	}
	sm.nodeInfo.Store(newNodeInfo)

	rebalancer := newRebalancer(sm.sup, change.ID,
		sm.rebalanceProgressCallback, sm.rebalanceDoneCallback)

	sm.Lock()
	sm.rebalanceService.rebalanceCtx = ctx
	sm.updateRebalanceProgressLocked(0)
	sm.rebalanceService.rebalancer = rebalancer
	sm.Unlock()

	logging.Infof("%s Successfully plan distributed. Starting rebalancer..", logPrefix)
	return nil
}

func (sm *serviceManager) runRebalanceCallback(cancel <-chan struct{}, body func()) {
	done := make(chan struct{})

	go func() {
		sm.Lock()
		defer sm.Unlock()

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

func (sm *serviceManager) rebalanceProgressCallback(progress float64, cancel <-chan struct{}) {
	sm.runRebalanceCallback(cancel, func() {
		sm.updateRebalanceProgressLocked(progress)
	})
}

func (sm *serviceManager) updateRebalanceProgressLocked(progress float64) {
	rev := sm.rebalanceService.rebalanceCtx.incRev()
	changeID := sm.rebalanceService.rebalanceCtx.change.ID
	task := &service.Task{
		Rev:          encodeRev(rev),
		ID:           fmt.Sprintf("rebalance/%s", changeID),
		Type:         service.TaskTypeRebalance,
		Status:       service.TaskStatusRunning,
		IsCancelable: true,
		Progress:     progress,

		Extra: map[string]interface{}{
			"rebalanceId": changeID,
		},
	}

	sm.updateStateLocked(func(s *state) {
		s.rebalanceService.rebalanceTask = task
	})
}

func (sm *serviceManager) rebalanceDoneCallback(err error, cancel <-chan struct{}) {
	sm.runRebalanceCallback(cancel, func() {
		sm.onRebalanceDoneLocked(err, false)
	})
}

func (sm *serviceManager) onRebalanceDoneLocked(err error, rebalanceCancelled bool) {
	logPrefix := "supervisor::RebalanceDoneCallback"

	newTask := (*service.Task)(nil)
	isBalanced := !rebalanceCancelled
	if err != nil {
		ctx := sm.rebalanceService.rebalanceCtx
		rev := ctx.incRev()
		isBalanced = false

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

	logging.Infof("%s called: %v. Rebalance cancelled? %v", logPrefix, err, rebalanceCancelled)
	sm.updateStateLocked(func(s *state) {
		s.isBalanced = isBalanced
		s.rebalanceService.rebalancer = nil
		s.rebalanceService.rebalanceCtx = nil
		s.rebalanceService.rebalanceTask = newTask
		s.rebalanceService.rebalanceID = ""
	})
}

func (sm *serviceManager) notifyWaitersLocked() {
	s := sm.copyStateLocked()
	for ch := range sm.waiters {
		if ch != nil {
			ch <- s
		}
	}

	sm.waiters = make(waiters)
}

func (sm *serviceManager) addWaiterLocked() waiter {
	ch := make(waiter, 1)
	sm.waiters[ch] = struct{}{}

	return ch
}

func (sm *serviceManager) removeWaiter(w waiter) {
	sm.Lock()
	defer sm.Unlock()

	delete(sm.waiters, w)
}

func (sm *serviceManager) updateState(body func(state *state)) {
	sm.Lock()
	defer sm.Unlock()

	sm.updateStateLocked(body)
}

func (sm *serviceManager) updateStateLocked(body func(state *state)) {
	body(&sm.state)
	sm.state.rev++

	sm.notifyWaitersLocked()
}

func (sm *serviceManager) wait(rev service.Revision,
	cancel service.Cancel) (stateDetails, error) {

	sm.Lock()
	currState := sm.getStateDetailsLocked(allDetails)
	if rev == nil {
		sm.Unlock()
		return currState, nil
	}

	haveRev := decodeRev(rev)
	if haveRev != sm.rev {
		sm.Unlock()
		return currState, nil
	}

	ch := sm.addWaiterLocked()
	sm.Unlock()

	select {
	case <-cancel:
		sm.removeWaiter(ch)
		return stateDetails{}, service.ErrCanceled
	case newState := <-ch:
		return newState, nil
	}
}

func (sm *serviceManager) cancelActualTaskLocked(task *service.Task) error {
	switch task.Type {
	case service.TaskTypePrepared:
		return sm.cancelPrepareTaskLocked()
	case service.TaskTypeRebalance:
		return sm.cancelRebalanceTaskLocked(task)
	default:
		panic("can't happen")
	}
}

func (sm *serviceManager) cancelPrepareTaskLocked() error {
	if sm.rebalanceService.rebalancer != nil {
		return service.ErrConflict
	}

	nodeInfo := sm.nodeInfo.Load().(*service.NodeInfo)
	newNodeInfo := &service.NodeInfo{
		NodeID:   nodeInfo.NodeID,
		Priority: NewEventingVersionPriority,
	}
	sm.nodeInfo.Store(newNodeInfo)

	sm.updateStateLocked(func(s *state) {
		s.rebalanceService.rebalanceID = ""
	})

	return nil
}

func (sm *serviceManager) cancelRebalanceTaskLocked(task *service.Task) error {
	switch task.Status {
	case service.TaskStatusRunning:
		return sm.cancelRunningRebalanceTaskLocked()
	case service.TaskStatusFailed:
		return sm.cancelFailedRebalanceTaskLocked()
	default:
		panic("can't happen")
	}
}

func (sm *serviceManager) cancelRunningRebalanceTaskLocked() error {
	sm.rebalanceService.rebalancer.Cancel()
	sm.onRebalanceDoneLocked(nil, true)

	return nil
}

func (sm *serviceManager) cancelFailedRebalanceTaskLocked() error {
	sm.updateStateLocked(func(s *state) {
		s.rebalanceService.rebalanceTask = nil
	})

	return nil
}

func encodeRev(rev uint64) service.Revision {
	ext := make(service.Revision, 8)
	binary.BigEndian.PutUint64(ext, rev)

	return ext
}

func decodeRev(ext service.Revision) uint64 {
	return binary.BigEndian.Uint64(ext)
}
