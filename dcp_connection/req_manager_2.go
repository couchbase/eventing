package dcpConn

import (
	"sync"
)

type reqState uint8

const (
	// caller made the request. Not yet made with the server
	req_init reqState = iota // send the request

	// start stream message is sent waiting for reply
	requesting

	// request is successfully accepted by kv node
	requested

	// paused allow all the request in the socket
	// and close the request
	paused

	// request is closed due to successly completing or some error
	closed
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
	return req
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
	req.FailoverLog = dcpMsg.FailoverLog
	req.Vbuuid, req.failoverLogIndex = GetVbUUID(req.StartSeq, dcpMsg.FailoverLog)
	return true
}

func (manager *reqManager) mutationNote(event *DcpEvent) bool {
	manager.runningLock.RLock()
	defer manager.runningLock.RUnlock()

	req, ok := manager.runningMap[event.opaque]
	if !ok || !req.running {
		return false
	}

	event.Version = req.Version
	req.StartSeq = event.Seqno
	event.FailoverLog = req.FailoverLog

	if req.failoverLogIndex > 0 {
		failoverLogEntry := req.FailoverLog[req.failoverLogIndex-1]
		seq := failoverLogEntry[1]
		if seq >= event.Seqno {
			req.failoverLogIndex--
			req.Vbuuid = failoverLogEntry[0]
		}
	}
	event.Vbuuid = req.Vbuuid
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
