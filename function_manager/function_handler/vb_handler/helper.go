package vbhandler

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"sync"
	"sync/atomic"

	checkpointManager "github.com/couchbase/eventing/checkpoint_manager"
	dcpMessage "github.com/couchbase/eventing/dcp_connection"
	processManager "github.com/couchbase/eventing/process_manager"
)

func planToBytes(vbsLength int, distributedVbs [][]uint16) ([]byte, map[string][]uint16) {
	distribution := make(map[string][]uint16)
	distributedVbsBytes := make([]byte, vbsLength*2+len(distributedVbs)*2)
	index := 0
	for workerId, vbs := range distributedVbs {
		workerVbs := make([]uint16, 0, len(vbs))
		binary.BigEndian.PutUint16(distributedVbsBytes[index:], uint16(len(vbs)))
		index += 2
		for _, vb := range vbs {
			workerVbs = append(workerVbs, vb)
			binary.BigEndian.PutUint16(distributedVbsBytes[index:], vb)
			index += 2
		}
		distribution[strconv.Itoa(workerId)] = workerVbs
	}
	return distributedVbsBytes, distribution
}

func (handler *vbHandler) initAndSendTrappedEvent(runtimeSystem RuntimeSystem, messageType uint8, msg *dcpMessage.DcpEvent, parsedDetails *checkpointManager.ParsedInternalDetails) {
	version := runtimeSystem.GetProcessVersion()
	msgBuffer := NewMsgBuffer(version, handler.config.InstanceID, runtimeSystem)
	dummyPlan := [][]uint16{
		{msg.Vbno},
	}
	dummyPlanBytes, _ := planToBytes(1, dummyPlan)
	runtimeSystem.VbSettings(version, processManager.VbMap, handler.config.InstanceID, nil, dummyPlanBytes)
	runtimeSystem.VbSettings(version, processManager.VbAddChanges, handler.config.InstanceID, msg.Vbno, []uint64{msg.Seqno, msg.Vbuuid})
	msgBuffer.Write(0, messageType, msg, parsedDetails)
	msgBuffer.Send()
}

type bytesStats struct {
	unackedBytes        *atomic.Int32
	unackedCount        *atomic.Int32
	dcpMutationExecuted *atomic.Uint64
}

func NewBytesStats() *bytesStats {
	return &bytesStats{
		unackedBytes:        &atomic.Int32{},
		unackedCount:        &atomic.Int32{},
		dcpMutationExecuted: &atomic.Uint64{},
	}
}

func (bs bytesStats) NoteUnackedMessage(count, size int32) (float64, float64) {
	unackedCount := bs.unackedCount.Add(count)
	unackedBytes := bs.unackedBytes.Add(size)
	return float64(unackedCount), float64(unackedBytes)
}

func (bs bytesStats) AckMessage(count, size int32, dcpMutationExecuted uint64) (float64, float64) {
	unackedCount := bs.unackedCount.Add(-1 * count)
	unackedBytes := bs.unackedBytes.Add(-1 * size)
	bs.dcpMutationExecuted.Add(dcpMutationExecuted)
	return float64(unackedCount), float64(unackedBytes)
}

func (bs bytesStats) UnackedMessageCount() (float64, float64) {
	count := bs.unackedCount.Load()
	size := bs.unackedBytes.Load()
	return float64(count), float64(size)
}

// MsgBuffer
type msgBuffer struct {
	sync.Mutex
	version    uint32
	instanceID []byte

	runtimeSystem RuntimeSystem
	msgBuffer     *bytes.Buffer

	bufferedCount int32
}

func NewMsgBuffer(version uint32, instanceID []byte, runtimeSystem RuntimeSystem) *msgBuffer {
	return &msgBuffer{
		runtimeSystem: runtimeSystem,
		version:       version,
		instanceID:    instanceID,
		msgBuffer:     &bytes.Buffer{},
		bufferedCount: int32(0),
	}
}

func (mBuffer *msgBuffer) Write(opcode uint8, workerID uint8, msg *dcpMessage.DcpEvent, internalInfo *checkpointManager.ParsedInternalDetails) (int32, int32) {
	mBuffer.Lock()
	defer mBuffer.Unlock()

	mBuffer.bufferedCount++
	msgSize := mBuffer.runtimeSystem.WriteDcpMessage(mBuffer.version, mBuffer.msgBuffer, opcode, workerID, mBuffer.instanceID, msg, internalInfo)
	return mBuffer.bufferedCount, msgSize
}

func (mBuffer *msgBuffer) Send() {
	mBuffer.Lock()
	defer mBuffer.Unlock()

	if mBuffer.bufferedCount == 0 {
		return
	}

	mBuffer.runtimeSystem.FlushMessage(mBuffer.version, mBuffer.msgBuffer)
	mBuffer.resetLocked()
}

func (mBuffer *msgBuffer) Clear() {
	mBuffer.Lock()
	defer mBuffer.Unlock()

	mBuffer.resetLocked()
}

func (mBuffer *msgBuffer) Close() {
	mBuffer.Lock()
	defer mBuffer.Unlock()

	mBuffer.resetLocked()
}

func (mBuffer *msgBuffer) resetLocked() {
	mBuffer.msgBuffer.Reset()
	mBuffer.bufferedCount = 0
}
