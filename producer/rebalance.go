package producer

import (
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/logging"
)

func (p *Producer) GetNodeInfo() (*service.NodeInfo, error) {
	logging.Infof("PRRB:[%s:%d] ServiceMgr::GetNodeInfo p.nodeInfo: %v", p.AppName, p.LenRunningConsumers(), p.nodeInfo)
	return p.nodeInfo, nil
}

func (p *Producer) Shutdown() error {
	logging.Infof("PRRB:[%s:%d] ServiceMgr::Shutdown", p.AppName, p.LenRunningConsumers())
	return nil
}

func (p *Producer) GetTaskList(rev service.Revision, cancel service.Cancel) (*service.TaskList, error) {
	logging.Infof("PRRB:[%s:%d] ServiceMgr::GetTaskList rev: %v", p.AppName, p.LenRunningConsumers(), rev)

	taskList := new(service.TaskList)

	return taskList, nil
}

func (p *Producer) CancelTask(id string, rev service.Revision) error {
	logging.Infof("PRRB:[%s:%d] ServiceMgr::CancelTask id: %v rev: %v", p.AppName, p.LenRunningConsumers(), id, rev)

	return nil
}

func (p *Producer) GetCurrentTopology(rev service.Revision, cancel service.Cancel) (*service.Topology, error) {
	logging.Infof("PRRB:[%s:%d] ServiceMgr::GetCurrentTopology rev: %v", p.AppName, p.LenRunningConsumers(), rev)

	topology := new(service.Topology)

	return topology, nil

}

func (p *Producer) PrepareTopologyChange(change service.TopologyChange) error {
	logging.Infof("PRRB:[%s:%d] ServiceMgr::PrepareTopologyChange change: %v", p.AppName, p.LenRunningConsumers(), change)

	return nil
}

func (p *Producer) StartTopologyChange(change service.TopologyChange) error {
	logging.Infof("PRRB:[%s:%d] ServiceMgr::StartTopologyChange change: %v", p.AppName, p.LenRunningConsumers(), change)

	return nil
}
