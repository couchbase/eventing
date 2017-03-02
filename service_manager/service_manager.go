package servicemanager

import (
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

func (m *ServiceMgr) GetNodeInfo() (*service.NodeInfo, error) {
	logging.Infof("SSRB ServiceMgr::GetNodeInfo s.nodeInfo: %#v", m.nodeInfo)
	return m.nodeInfo, nil
}

func (m *ServiceMgr) Shutdown() error {
	logging.Infof("SSRB ServiceMgr::Shutdown")
	return nil
}

func (m *ServiceMgr) GetTaskList(rev service.Revision, cancel service.Cancel) (*service.TaskList, error) {
	logging.Infof("SSRB ServiceMgr::GetTaskList rev: %#v", rev)

	state, err := m.wait(rev, cancel)
	if err != nil {
		return nil, err
	}

	taskList := stateToTaskList(state)
	logging.Infof("SSRB ServiceMgr::GetTaskList tasklist: %#v", taskList)

	return taskList, nil
}

func (m *ServiceMgr) CancelTask(id string, rev service.Revision) error {
	logging.Infof("SSRB ServiceMgr::CancelTask id: %#v rev: %#v", id, rev)

	return nil
}

func (m *ServiceMgr) GetCurrentTopology(rev service.Revision, cancel service.Cancel) (*service.Topology, error) {
	logging.Infof("SSRB ServiceMgr::GetCurrentTopology rev: %#v", rev)

	state, err := m.wait(rev, cancel)
	if err != nil {
		return nil, err
	}

	topology := m.stateToTopology(state)
	logging.Infof("ServiceMgr::GetCurrentTopology topology: %#v", topology)

	return topology, nil

}

func (m *ServiceMgr) PrepareTopologyChange(change service.TopologyChange) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logging.Infof("SSRB ServiceMgr::PrepareTopologyChange change: %#v", change)

	var keepNodeUUIDs []string

	for _, node := range change.KeepNodes {
		keepNodeUUIDs = append(keepNodeUUIDs, string(node.NodeInfo.NodeID))
	}

	logging.Infof("SSRB ServiceMgr::PrepareTopologyChange keepNodeUUIDs: %v", keepNodeUUIDs)

	m.superSup.NotifyPrepareTopologyChange(keepNodeUUIDs)

	return nil
}

func (m *ServiceMgr) StartTopologyChange(change service.TopologyChange) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logging.Infof("SSRB ServiceMgr::StartTopologyChange change: %#v", change)

	switch change.Type {
	case service.TopologyChangeTypeFailover:
		return nil
	case service.TopologyChangeTypeRebalance:
		break
	default:
		return service.ErrNotSupported
	}

	// Garbage collect old Rebalance Tokens
	err := util.RecursiveDelete(MetakvRebalanceTokenPath)
	if err != nil {
		logging.Errorf("SSRB ServiceMgr::StartTopologyChange Failed to garbage collect old rebalance token(s) from metakv, err: %v", err)
		return err
	}

	path := MetakvRebalanceTokenPath + change.ID
	err = util.MetakvSet(path, []byte(change.ID), nil)
	if err != nil {
		logging.Errorf("SSRB ServiceMgr::StartTopologyChange Failed to store rebalance token in metakv, err: %v", err)
		return err
	}

	return nil
}
