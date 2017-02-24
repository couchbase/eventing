package supervisor

import (
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/logging"
)

func (s *SuperSupervisor) GetNodeInfo() (*service.NodeInfo, error) {
	logging.Infof("SSRB[%d] ServiceMgr::GetNodeInfo s.nodeInfo: %v", len(s.runningProducers), s.nodeInfo)
	return s.nodeInfo, nil
}

func (s *SuperSupervisor) Shutdown() error {
	logging.Infof("SSRB[%d] ServiceMgr::Shutdown", len(s.runningProducers))
	return nil
}

func (s *SuperSupervisor) GetTaskList(rev service.Revision, cancel service.Cancel) (*service.TaskList, error) {
	logging.Infof("SSRB[%d] ServiceMgr::GetTaskList rev: %v", len(s.runningProducers), rev)

	taskList := new(service.TaskList)

	return taskList, nil
}

func (s *SuperSupervisor) CancelTask(id string, rev service.Revision) error {
	logging.Infof("SSRB[%d] ServiceMgr::CancelTask id: %v rev: %v", len(s.runningProducers), id, rev)

	return nil
}

func (s *SuperSupervisor) GetCurrentTopology(rev service.Revision, cancel service.Cancel) (*service.Topology, error) {
	logging.Infof("SSRB[%d] ServiceMgr::GetCurrentTopology rev: %v", len(s.runningProducers), rev)

	topology := new(service.Topology)

	return topology, nil

}

func (s *SuperSupervisor) PrepareTopologyChange(change service.TopologyChange) error {
	logging.Infof("SSRB[%d] ServiceMgr::PrepareTopologyChange change: %v", len(s.runningProducers), change)

	return nil
}

func (s *SuperSupervisor) StartTopologyChange(change service.TopologyChange) error {
	logging.Infof("SSRB[%d] ServiceMgr::StartTopologyChange change: %v", len(s.runningProducers), change)
	return nil
}
