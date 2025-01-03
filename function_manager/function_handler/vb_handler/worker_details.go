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
	Vbno            uint16                `json:"vb"`
	Version         uint32                `json:"version"`
	StreamReq       *dcpMessage.StreamReq `json:"stream_req,omitempty"`
	Status          status                `json:"status"`
	LastSentSeq     uint64                `json:"last_sent_seq"`
	IsStreaming     bool                  `json:"is_streaming"`
	LastDoneRequest time.Time             `json:"last_done_request"`
}

func (vs vbStatus) Copy() vbStatus {
	copyVb := vs
	if vs.StreamReq != nil {
		copyVb.StreamReq = vs.StreamReq.Copy()
	}
	return copyVb
}

func (vs vbStatus) isRunning() bool {
	return vs.Status.isRunning()
}

func (vs vbStatus) isRequested() (bool, bool) {
	return vs.Status.isRequested(), vs.IsStreaming
}

func (vs vbStatus) isOwned() bool {
	return vs.Status.isOwned()
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

func (w *workerDetails) GetRuntimeStats() map[string]interface{} {
	stats := make(map[string]interface{})
	stats["running_count"] = w.runningCount.Load()
	count, size := w.unackedDetails.UnackedMessageCount()
	stats["unacked_count"] = count
	stats["unacked_size"] = size
	vbList := make([]vbStatus, 0, len(w.allVbList))
	for _, vb := range w.allVbList {
		vbList = append(vbList, vb.Copy())
	}
	stats["vb_list"] = vbList
	return stats
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
		Vbno:            vb,
		Status:          initStatus,
		Version:         wd.version,
		LastDoneRequest: time.Now(),
		IsStreaming:     true,
	}

	wd.version++
	wd.allVbList = append(wd.allVbList, status)
}

func (wd *workerDetails) AddVb(vb uint16, sr *dcpMessage.StreamReq, isStreaminMode bool) (int, bool) {
	sr.Version = wd.version
	sendStatus := true
	for index := 0; index < len(wd.allVbList); index++ {
		vbStatus := wd.allVbList[index]
		if vbStatus.Vbno == vb {
			if vbStatus.Status != initStatus {
				sendStatus = false
				break
			}

			sr.Version = vbStatus.Version
			vbStatus.LastDoneRequest = time.Now()
			vbStatus.Status = waiting
			vbStatus.LastSentSeq = sr.StartSeq
			vbStatus.StreamReq = sr
			vbStatus.IsStreaming = isStreaminMode
			break
		}
	}
	return wd.getVbsWithStatus(initStatus), sendStatus
}

// returns what all vbs remained in other mode
func (wd *workerDetails) updateModeTo(vbno uint16, isStreamingMode bool) int {
	count := 0
	for _, status := range wd.allVbList {
		if status.Vbno == vbno {
			status.IsStreaming = isStreamingMode
		}
		if status.IsStreaming != isStreamingMode {
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
		if vbDetails.Vbno == vb {
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
		if vbDetails.Status == initStatus {
			toOwn = append(toOwn, vbDetails.Vbno)
		}
	}

	return toOwn
}

func (wd *workerDetails) GetVbOwned(vbMap map[uint16]struct{}) {
	for _, vbDetails := range wd.allVbList {
		vbMap[vbDetails.Vbno] = struct{}{}
	}
	return
}

func (wd *workerDetails) getVbsWithStatus(vbState status) (count int) {
	switch vbState {
	case ready, running, paused, forcedClosed:
		for _, vbDetails := range wd.runningMap {
			if vbDetails.Status == vbState {
				count++
			}
		}

	default:
		for _, vbDetails := range wd.allVbList {
			if vbDetails.Status == vbState {
				count++
			}
		}
	}
	return
}
