package dcpManager

import (
	dcpConn "github.com/couchbase/eventing/dcp_connection"
)

type dcpManager struct {
	manager DcpManager
	id      uint16
}

func NewDcpManagerWrapper(manager DcpManager) DcpManager {
	if _, ok := manager.(*dcpManager); ok {
		return manager
	}

	return &dcpManager{
		manager: manager,
	}
}

func (m *dcpManager) DoneDcpEvent(event *dcpConn.DcpEvent) {
	m.manager.DoneDcpEvent(event)
}

func (m *dcpManager) StartStreamReq(sr *dcpConn.StreamReq) error {
	sr.ID = m.id
	return m.manager.StartStreamReq(sr)
}

func (m *dcpManager) PauseStreamReq(sr *dcpConn.StreamReq) {
	sr.ID = m.id
	m.manager.PauseStreamReq(sr)
}

func (m *dcpManager) CloseRequest(sr *dcpConn.StreamReq) (*dcpConn.StreamReq, error) {
	sr.ID = m.id
	return m.manager.CloseRequest(sr)
}

func (m *dcpManager) GetFailoverLog(vbs []uint16) (map[uint16]dcpConn.FailoverLog, error) {
	return m.manager.GetFailoverLog(vbs)
}

func (m *dcpManager) GetSeqNumber(vbs []uint16, collectionID string) (map[uint16]uint64, error) {
	return m.manager.GetSeqNumber(vbs, collectionID)
}

func (m *dcpManager) RegisterID(id uint16, sendChannel chan<- *dcpConn.DcpEvent) {
	m.id = id
	m.manager.RegisterID(id, sendChannel)
}

func (m *dcpManager) DeregisterID(id uint16) {
	// noop
}

func (m *dcpManager) CloseManager() {
	// Just remove the id from the manager
	m.manager.DeregisterID(m.id)
}

func (m *dcpManager) CloseConditional() bool {
	return m.manager.CloseConditional()
}

type dummy struct{}

func NewDummyManager() dummy {
	return dummy{}
}

func (_ dummy) DoneDcpEvent(event *dcpConn.DcpEvent) {
}

func (_ dummy) StartStreamReq(sr *dcpConn.StreamReq) error {
	panic("dummy manager is used to make stream request")
}

func (_ dummy) PauseStreamReq(sr *dcpConn.StreamReq) {
	panic("dummy dcp manager is used to pause stream request")
}

func (_ dummy) CloseRequest(sr *dcpConn.StreamReq) (*dcpConn.StreamReq, error) {
	panic("dummy dcp manager is used to close stream request")
}

func (_ dummy) GetFailoverLog(vbs []uint16) (map[uint16]dcpConn.FailoverLog, error) {
	panic("dummy dcp manager is used to request failover logs")
}

func (_ dummy) GetSeqNumber(vbs []uint16, collectionID string) (map[uint16]uint64, error) {
	panic("dummy dcp manager is used to request high seq number")
}

func (_ dummy) RegisterID(id uint16, sendChannel chan<- *dcpConn.DcpEvent) {
	panic("trying to register with dummy manager")
}

func (_ dummy) DeregisterID(id uint16) {
	panic("trying to deregister with dummy manager")
}

func (_ dummy) CloseManager() {
}

func (_ dummy) CloseConditional() bool {
	return true
}
