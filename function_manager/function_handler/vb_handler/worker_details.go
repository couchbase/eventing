package vbhandler

import (
	"sync"
	"sync/atomic"
	"time"

	dcpMessage "github.com/couchbase/eventing/dcp_connection"
)

type status uint8

const (
	// We don't know whats the start seq number
	initStatus status = iota

	// Waiting for request
	waiting

	// Made the request waiting for success message from server
	ready

	// KV accepted the request. Mutations started
	running

	// Pause the request
	paused

	// forced closed
	forcedClosed

	// Asked to close the vb
	closed
)

func (s status) isRunning() bool {
	return (s == running) || (s == paused) || (s == ready)
}

func (s status) isRequested() bool {
	return (s == running) || (s == ready)
}

func (s status) isOwned() bool {
	return (s != initStatus)
}

func (vs status) String() string {
	switch vs {
	case initStatus:
		return "initStatus"

	case waiting:
		return "waiting"

	case ready:
		return "ready"

	case running:
		return "running"

	case paused:
		return "paused"

	case forcedClosed:
		return "forcedClosed"

	case closed:
		return "closed"
	}

	panic("unrecognised status")
}

type vbStatus struct {
	vbno        uint16
	version     uint32
	streamReq   *dcpMessage.StreamReq
	status      status
	lastSentSeq uint64
	isStreaming bool

	lastDoneRequest time.Time
}

func (vs vbStatus) isRunning() bool {
	return vs.status.isRunning()
}

func (vs vbStatus) isRequested() (bool, bool) {
	return vs.status.isRequested(), vs.isStreaming
}

func (vs vbStatus) isOwned() bool {
	return vs.status.isOwned()
}

type workerDetails struct {
	unackedDetails *bytesStats
	runningCount   *atomic.Int32

	sync.RWMutex
	version uint32
	// Map contains requests of ready/running/paused/forcedClosed
	runningMap map[uint16]*vbStatus
	allVbList  []*vbStatus
	index      int
}

// Concurency should be controlled by caller
func InitWorkerDetails() *workerDetails {
	return &workerDetails{
		unackedDetails: NewBytesStats(),
		runningCount:   &atomic.Int32{},

		runningMap: make(map[uint16]*vbStatus),
		allVbList:  make([]*vbStatus, 0),
		index:      0,
	}
}

func (wd *workerDetails) InitVb(vb uint16) {
	status := &vbStatus{
		vbno:            vb,
		status:          initStatus,
		version:         wd.version,
		lastDoneRequest: time.Now(),
		isStreaming:     true,
	}

	wd.version++
	wd.allVbList = append(wd.allVbList, status)
}

func (wd *workerDetails) AddVb(vb uint16, sr *dcpMessage.StreamReq, isStreaminMode bool) (int, bool) {
	sr.Version = wd.version
	sendStatus := true
	for index := 0; index < len(wd.allVbList); index++ {
		vbStatus := wd.allVbList[index]
		if vbStatus.vbno == vb {
			if vbStatus.status != initStatus {
				sendStatus = false
				break
			}

			sr.Version = vbStatus.version
			vbStatus.lastDoneRequest = time.Now()
			vbStatus.status = waiting
			vbStatus.lastSentSeq = sr.StartSeq
			vbStatus.streamReq = sr
			vbStatus.isStreaming = isStreaminMode
			break
		}
	}
	return wd.getVbsWithStatus(initStatus), sendStatus
}

// returns what all vbs remained in other mode
func (wd *workerDetails) updateModeTo(vbno uint16, isStreamingMode bool) int {
	count := 0
	for _, status := range wd.allVbList {
		if status.vbno == vbno {
			status.isStreaming = isStreamingMode
		}
		if status.isStreaming != isStreamingMode {
			count++
		}
	}
	return count
}

// This is called when vbs are shuffled
func (wd *workerDetails) CloseVb(vb uint16) (*vbStatus, bool) {
	// delete from waiting list
	_, ok := wd.runningMap[vb]
	if ok {
		wd.runningCount.Add(-1)
		delete(wd.runningMap, vb)
	}

	index := 0
	for ; index < len(wd.allVbList); index++ {
		vbDetails := wd.allVbList[index]
		if vbDetails.vbno == vb {
			wd.allVbList[index] = wd.allVbList[len(wd.allVbList)-1]
			wd.allVbList = wd.allVbList[:len(wd.allVbList)-1]
			if wd.index >= len(wd.allVbList) {
				wd.index = 0
			}
			return vbDetails, true
		}
	}

	return nil, false
}

func (wd *workerDetails) isRunningVb(vb uint16) (*vbStatus, bool) {
	vbStatus, ok := wd.runningMap[vb]
	if !ok {
		return nil, false
	}
	return vbStatus, true
}

func (wd *workerDetails) StillClaimedVbs(toOwn []uint16) []uint16 {
	for _, vbDetails := range wd.allVbList {
		if vbDetails.status == initStatus {
			toOwn = append(toOwn, vbDetails.vbno)
		}
	}

	return toOwn
}

func (wd *workerDetails) GetVbOwned(vbMap map[uint16]struct{}) {
	for _, vbDetails := range wd.allVbList {
		vbMap[vbDetails.vbno] = struct{}{}
	}
	return
}

func (wd *workerDetails) getVbsWithStatus(vbState status) (count int) {
	switch vbState {
	case ready, running, paused, forcedClosed:
		for _, vbDetails := range wd.runningMap {
			if vbDetails.status == vbState {
				count++
			}
		}

	default:
		for _, vbDetails := range wd.allVbList {
			if vbDetails.status == vbState {
				count++
			}
		}
	}
	return
}
