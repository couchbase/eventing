package servicemanager

import (
	"os"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/logging"
)

// GetNodeInfo callback for cbauth service.Manager
func (m *ServiceMgr) GetNodeInfo() (*service.NodeInfo, error) {
	logging.Infof("SMRB ServiceMgr::GetNodeInfo s.nodeInfo: %#v", m.nodeInfo)
	return m.nodeInfo, nil
}

// Shutdown callback for cbauth service.Manager
func (m *ServiceMgr) Shutdown() error {
	logging.Infof("SMRB ServiceMgr::Shutdown")

	os.Exit(0)

	return nil
}

// GetTaskList callback for cbauth service.Manager
func (m *ServiceMgr) GetTaskList(rev service.Revision, cancel service.Cancel) (*service.TaskList, error) {
	logging.Infof("SMRB ServiceMgr::GetTaskList rev: %#v", rev)

	state, err := m.wait(rev, cancel)
	if err != nil {
		return nil, err
	}

	taskList := stateToTaskList(state)
	logging.Infof("SMRB ServiceMgr::GetTaskList tasklist: %#v", taskList)

	return taskList, nil
}

// CancelTask callback for cbauth service.Manager
func (m *ServiceMgr) CancelTask(id string, rev service.Revision) error {
	logging.Infof("SMRB ServiceMgr::CancelTask id: %#v rev: %#v", id, rev)

	return nil
}

// GetCurrentTopology callback for cbauth service.Manager
func (m *ServiceMgr) GetCurrentTopology(rev service.Revision, cancel service.Cancel) (*service.Topology, error) {
	logging.Infof("SMRB ServiceMgr::GetCurrentTopology rev: %#v", rev)

	state, err := m.wait(rev, cancel)
	if err != nil {
		return nil, err
	}

	topology := m.stateToTopology(state)
	logging.Infof("ServiceMgr::GetCurrentTopology topology: %#v", topology)

	return topology, nil

}

// PrepareTopologyChange callback for cbauth service.Manager
func (m *ServiceMgr) PrepareTopologyChange(change service.TopologyChange) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logging.Infof("SMRB ServiceMgr::PrepareTopologyChange change: %#v", change)

	var keepNodeUUIDs []string

	for _, node := range change.KeepNodes {
		keepNodeUUIDs = append(keepNodeUUIDs, string(node.NodeInfo.NodeID))
	}

	logging.Infof("SMRB ServiceMgr::PrepareTopologyChange keepNodeUUIDs: %v", keepNodeUUIDs)

	m.updateStateLocked(func(s *state) {
		m.rebalanceID = change.ID
	})

	m.superSup.NotifyPrepareTopologyChange(keepNodeUUIDs)

	return nil
}

// StartTopologyChange callback for cbauth service.Manager
func (m *ServiceMgr) StartTopologyChange(change service.TopologyChange) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logging.Infof("SMRB ServiceMgr::StartTopologyChange change: %#v", change)

	if m.state.rebalanceID != change.ID || m.rebalancer != nil {
		return service.ErrConflict
	}

	if change.CurrentTopologyRev != nil {
		haveRev := decodeRev(change.CurrentTopologyRev)
		if haveRev != m.state.rev {
			return service.ErrConflict
		}
	}

	ctx := &rebalanceContext{
		rev:    0,
		change: change,
	}

	m.rebalanceCtx = ctx

	switch change.Type {
	case service.TopologyChangeTypeFailover:
		m.failoverNotif = true
	case service.TopologyChangeTypeRebalance:
		m.startRebalance(change)

		rebalancer := newRebalancer(m.eventingAdminPort, change, m.rebalanceDoneCallback, m.rebalanceProgressCallback)
		m.rebalancer = rebalancer

	default:
		return service.ErrNotSupported
	}

	return nil
}
