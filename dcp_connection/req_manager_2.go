package dcpConn

import (
	"sync"
	"time"
)

type dcpCommand int8

const (
	stream_request dcpCommand = iota
	stream_close
	mixed_request
)

type command struct {
	command dcpCommand
	id      uint16
	vbno    uint16
	opaque  uint32
}

type reqManagerStats struct {
	RunningMap map[uint32]*StreamReq `json:"running_map"`
	ReadyMap   map[uint32]*StreamReq `json:"ready_map"`
	ReqMap     map[uint32]*StreamReq `json:"req_map"`
}

// Lock hierarcy reqLock, readyLock, runningLock
type reqManager struct {
	acceptRequest bool

	reqLock *sync.RWMutex
	reqMap  map[uint32]*StreamReq

	readyLock *sync.RWMutex
	readyMap  map[uint32]*StreamReq

	runningLock *sync.RWMutex
	runningMap  map[uint32]*StreamReq

	requestChannel chan<- command
}

func newRequestManager(requestChannel chan<- command) *reqManager {
	manager := &reqManager{
		acceptRequest: true,
		reqLock:       &sync.RWMutex{},
		reqMap:        make(map[uint32]*StreamReq),

		readyLock: &sync.RWMutex{},
		readyMap:  make(map[uint32]*StreamReq),

		runningLock:    &sync.RWMutex{},
		runningMap:     make(map[uint32]*StreamReq),
		requestChannel: requestChannel,
	}

	return manager
}

func copyReqMap(reqMap map[uint32]*StreamReq) map[uint32]*StreamReq {
	copyMap := make(map[uint32]*StreamReq, len(reqMap))
	for opaque, req := range reqMap {
		copyMap[opaque] = req.Copy()
	}
	return copyMap
}

func (manager *reqManager) GetRuntimeStats() *reqManagerStats {
	reqStats := &reqManagerStats{}
	manager.reqLock.RLock()
	reqStats.ReqMap = copyReqMap(manager.reqMap)
	manager.reqLock.RUnlock()

	manager.readyLock.RLock()
	reqStats.ReadyMap = copyReqMap(manager.readyMap)
	manager.readyLock.RUnlock()

	manager.runningLock.RLock()
	reqStats.RunningMap = copyReqMap(manager.runningMap)
	manager.runningLock.RUnlock()

	return reqStats
}

func (manager *reqManager) initRequest(req *StreamReq) bool {
	manager.reqLock.Lock()
	if !manager.acceptRequest {
		manager.reqLock.Unlock()
		return false
	}
	req.running = true
	manager.reqMap[req.opaque] = req
	manager.reqLock.Unlock()

	manager.requestChannel <- command{
		command: stream_request,
		opaque:  req.opaque,
		vbno:    req.Vbno,
	}

	return true
}

func (manager *reqManager) readyRequest(opaque uint32) *StreamReq {
	manager.reqLock.Lock()
	defer manager.reqLock.Unlock()
	manager.readyLock.Lock()
	defer manager.readyLock.Unlock()

	req, ok := manager.reqMap[opaque]
	if !ok {
		return nil
	}
	delete(manager.reqMap, opaque)
	manager.readyMap[opaque] = req
	req.LastStreamRequestedTime = time.Now()
	return req.Copy()
}

func (manager *reqManager) rollbackReqNote(dcpMsg *DcpEvent) *StreamReq {
	manager.readyLock.Lock()
	req, ok := manager.readyMap[dcpMsg.opaque]
	if !ok {
		manager.readyLock.Unlock()
		return nil
	}
	delete(manager.readyMap, dcpMsg.opaque)
	manager.readyLock.Unlock()

	resetOso(req)
	failoverLog, vbuuid, seqNo := req.FailoverLog.Pop(req.StartSeq)
	req.Vbuuid = vbuuid
	req.StartSeq = seqNo
	req.FailoverLog = failoverLog

	return req
}

func (manager *reqManager) runningReq(dcpMsg *DcpEvent) bool {
	manager.readyLock.Lock()
	manager.runningLock.Lock()
	defer manager.runningLock.Unlock()

	req, ok := manager.readyMap[dcpMsg.opaque]
	if !ok {
		manager.readyLock.Unlock()
		return false
	}
	manager.runningMap[dcpMsg.opaque] = req
	delete(manager.readyMap, dcpMsg.opaque)
	manager.readyLock.Unlock()

	dcpMsg.Version = req.Version
	resetOso(req)
	req.FailoverLog = dcpMsg.FailoverLog
	req.Vbuuid, req.failoverLogIndex = GetVbUUID(req.StartSeq, dcpMsg.FailoverLog)
	req.LastStreamSuccessTime = time.Now()
	return true
}

func adjustVbuuid(req *StreamReq) {
	if req.failoverLogIndex > 0 {
		failoverLogEntry := req.FailoverLog[req.failoverLogIndex-1]
		seq := failoverLogEntry[1]
		if seq >= req.StartSeq {
			req.failoverLogIndex--
			req.Vbuuid = failoverLogEntry[0]
		}
	}
}

// Caller should be holding the lock which guards req.
func resetOso(req *StreamReq) {
	if req.osoActive {
		req.StartSeq = req.osoResume
	}
	req.osoActive = false
	req.osoResume = 0
	req.osoMaxSeq = 0
}

func (manager *reqManager) mutationNote(event *DcpEvent) bool {
	manager.runningLock.Lock()
	defer manager.runningLock.Unlock()

	req, ok := manager.runningMap[event.opaque]
	if !ok || !req.running {
		return false
	}

	event.Version = req.Version
	event.FailoverLog = req.FailoverLog

	if req.osoActive {
		if event.Seqno > req.osoMaxSeq {
			req.osoMaxSeq = event.Seqno
		}
		event.OsoSnapshot = true
		event.Vbuuid = req.Vbuuid
		return true
	}

	req.StartSeq = event.Seqno
	adjustVbuuid(req)
	event.Vbuuid = req.Vbuuid
	return true
}

func (manager *reqManager) osoNote(event *DcpEvent) bool {
	manager.runningLock.Lock()
	defer manager.runningLock.Unlock()

	req, ok := manager.runningMap[event.opaque]
	if !ok || !req.running {
		return false
	}

	switch event.EventType {
	case OSO_SNAPSHOT_START:
		req.osoActive = true
		req.osoResume = req.StartSeq
		req.osoMaxSeq = req.StartSeq

	case OSO_SNAPSHOT_END:
		if req.osoActive {
			req.osoActive = false
			if req.osoMaxSeq > req.StartSeq {
				req.StartSeq = req.osoMaxSeq
				adjustVbuuid(req)
			}
			req.osoResume, req.osoMaxSeq = 0, 0
		}
	}

	event.Version = req.Version
	event.Seqno = req.StartSeq
	event.Vbuuid = req.Vbuuid
	event.FailoverLog = req.FailoverLog
	event.OsoSnapshot = true
	return true
}

func (manager *reqManager) doneRequest(event *DcpEvent) (*StreamReq, bool) {
	manager.readyLock.Lock()
	manager.runningLock.Lock()
	req, ok := manager.runningMap[event.opaque]
	if !ok {
		req, ok = manager.readyMap[event.opaque]
		if !ok {
			manager.runningLock.Unlock()
			manager.readyLock.Unlock()
			return nil, false
		}
		delete(manager.readyMap, event.opaque)
	} else {
		delete(manager.runningMap, event.opaque)
	}
	manager.runningLock.Unlock()
	manager.readyLock.Unlock()

	resetOso(req)

	if event.Opcode == DCP_STREAM_END {
		switch event.Status {
		case SUCCESS:
			// Whatever requested is done
			if req.Flags != TillLatest && req.StartSeq < req.EndSeq {
				req.StartSeq = req.EndSeq
			}
		default:
		}
	}

	event.Seqno = req.StartSeq
	event.FailoverLog = req.FailoverLog
	event.Vbuuid = req.Vbuuid

	event.Version = req.Version
	event.SrRequest = req
	return req, true
}

// if delete is true then its a pause request
func (manager *reqManager) closeRequest(opaque uint32, closeReq bool) {
	manager.runningLock.RLock()
	_, ok := manager.runningMap[opaque]
	manager.runningLock.RUnlock()
	if ok {
		// No need to check again since it can't go back to any other level
		manager.runningLock.Lock()
		req, ok := manager.runningMap[opaque]
		if !ok {
			manager.runningLock.Unlock()
			return
		}
		if closeReq {
			req.running = false
		}
		manager.runningLock.Unlock()

		manager.requestChannel <- command{
			command: stream_close,
			vbno:    req.Vbno,
			opaque:  opaque,
			id:      req.ID,
		}
	}

	manager.readyLock.RLock()
	_, ok = manager.readyMap[opaque]
	manager.readyLock.RUnlock()
	if ok {
		manager.readyLock.Lock()
		req, ok := manager.readyMap[opaque]
		if ok {
			if closeReq {
				req.running = false
			}
			manager.readyLock.Unlock()
			manager.requestChannel <- command{
				command: stream_close,
				vbno:    req.Vbno,
				opaque:  opaque,
				id:      req.ID,
			}

			return
		}

		manager.runningLock.Lock()
		req, ok = manager.runningMap[opaque]
		if ok && closeReq {
			req.running = false
		}
		manager.runningLock.Unlock()
		manager.readyLock.Unlock()
		if ok {
			manager.requestChannel <- command{
				command: stream_close,
				vbno:    req.Vbno,
				opaque:  opaque,
				id:      req.ID,
			}
		}
		return
	}

	manager.reqLock.RLock()
	_, ok = manager.reqMap[opaque]
	manager.reqLock.RUnlock()

	if ok {
		manager.reqLock.Lock()
		req, ok := manager.reqMap[opaque]
		if ok {
			if closeReq {
				req.running = false
			}
			manager.reqLock.Unlock()
			return
		}

		manager.readyLock.Lock()
		req, ok = manager.readyMap[opaque]
		if ok {
			if closeReq {
				req.running = false
			}
			manager.readyLock.Unlock()
			manager.reqLock.Unlock()
			manager.requestChannel <- command{
				command: stream_close,
				vbno:    req.Vbno,
				opaque:  opaque,
				id:      req.ID,
			}
			return
		}

		manager.runningLock.Lock()
		req, ok = manager.runningMap[opaque]
		if ok && closeReq {
			req.running = false
		}
		manager.runningLock.Unlock()
		manager.readyLock.Unlock()
		manager.reqLock.Unlock()
		if ok {
			manager.requestChannel <- command{
				command: stream_close,
				vbno:    req.Vbno,
				opaque:  opaque,
				id:      req.ID,
			}
		}
		return
	}
	return
}

func (manager *reqManager) closeAllRequest(stopAcceptingRequest bool) []*StreamReq {
	manager.reqLock.Lock()
	manager.readyLock.Lock()
	manager.runningLock.Lock()

	if stopAcceptingRequest {
		manager.acceptRequest = false
	}
	reqList := make([]*StreamReq, 0, len(manager.reqMap)+len(manager.readyMap)+len(manager.runningMap))
	for _, req := range manager.runningMap {
		resetOso(req)
		reqList = append(reqList, req)
	}
	manager.runningMap = make(map[uint32]*StreamReq)
	manager.runningLock.Unlock()

	for _, req := range manager.readyMap {
		reqList = append(reqList, req)
	}
	manager.readyMap = make(map[uint32]*StreamReq)
	manager.readyLock.Unlock()

	for _, req := range manager.reqMap {
		reqList = append(reqList, req)
	}
	manager.reqMap = make(map[uint32]*StreamReq)
	manager.reqLock.Unlock()
	return reqList
}
