package vbhandler

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/couchbase/eventing/application"
	checkpointManager "github.com/couchbase/eventing/checkpoint_manager"
	"github.com/couchbase/eventing/common"
	dcpMessage "github.com/couchbase/eventing/dcp_connection"
	dcpManager "github.com/couchbase/eventing/dcp_manager"
	eventPool "github.com/couchbase/eventing/event_pool"
	"github.com/couchbase/eventing/logging"
	processManager "github.com/couchbase/eventing/process_manager"
	serverConfig "github.com/couchbase/eventing/server_config"
)

const (
	eventChannelSize = 100
	flushBufferCount = 100
)

type vbHandler struct {
	logPrefix    string
	config       *Config
	eventChannel chan *dcpMessage.DcpEvent

	commonDcpManager   dcpManager.DcpManager
	isolatedDcpManager dcpManager.DcpManager
	flushNotifier      *common.Signal
	allocator          *allocator
	workers            []*workerDetails

	msgBuffer *msgBuffer

	close func()
}

func NewVbHandler(ctx context.Context, logPrefix string, keyspace application.Keyspace, config *Config) VbHandler {
	handler := &vbHandler{
		logPrefix:    logPrefix,
		config:       config,
		eventChannel: make(chan *dcpMessage.DcpEvent, eventChannelSize),

		flushNotifier: common.NewSignal(),
	}

	switch config.DcpType {
	case serverConfig.FunctionGroup:
		handler.isolatedDcpManager = dcpManager.NewDummyManager()
		handler.commonDcpManager = config.Pool.GetDcpManagerPool(eventPool.CommonConn, "", keyspace.BucketName, handler.eventChannel)
	case serverConfig.IsolateFunction:
		handler.commonDcpManager = dcpManager.NewDummyManager()
		handler.isolatedDcpManager = config.Pool.GetDcpManagerPool(eventPool.DedicatedConn, config.AppLocation.String(), keyspace.BucketName, handler.eventChannel)
	case serverConfig.HybridMode:
		handler.commonDcpManager = config.Pool.GetDcpManagerPool(eventPool.CommonConn, "", keyspace.BucketName, handler.eventChannel)
		handler.isolatedDcpManager = config.Pool.GetDcpManagerPool(eventPool.DedicatedConn, config.AppLocation.String(), keyspace.BucketName, handler.eventChannel)
	}
	handler.msgBuffer = NewMsgBuffer(config.Version, config.InstanceID, config.RuntimeSystem)
	requester := requester{
		mode:               config.DcpType,
		commonDcpManager:   handler.commonDcpManager,
		isolatedDcpManager: handler.isolatedDcpManager,
	}

	handler.workers = make([]*workerDetails, int32(handler.config.HandlerSettings.CppWorkerThread))
	for index := int32(0); index < int32(handler.config.HandlerSettings.CppWorkerThread); index++ {
		handler.workers[index] = InitWorkerDetails()
	}
	handler.allocator = NewAllocatorWithContext(ctx, logPrefix, keyspace, handler.workers, requester, config)

	ctx2, closeContext := context.WithCancel(ctx)
	handler.close = closeContext

	go handler.eventReceiver(ctx2)
	go handler.flusher(ctx2)

	return handler
}

func (handler *vbHandler) eventReceiver(ctx context.Context) {
	logPrefix := fmt.Sprintf("vbHandler::eventReceiver[%s]", handler.logPrefix)
	for {
		select {
		case msg := <-handler.eventChannel:
			parsedDetails, workerID, filterMsg, wait := handler.allocator.FilterEvent(msg)
			for wait {
				parsedDetails, workerID, filterMsg, wait = handler.allocator.FilterEvent(msg)
				if filterMsg || !wait {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}

			if filterMsg {
				switch msg.Opcode {
				case dcpMessage.DCP_MUTATION:
					handler.config.StatsHandler.IncrementProcessingStats("dcp_mutation_suppressed_counter")
				case dcpMessage.DCP_DELETION:
					handler.config.StatsHandler.IncrementProcessingStats("dcp_deletion_suppressed_counter")
				case dcpMessage.DCP_EXPIRATION:
					handler.config.StatsHandler.IncrementProcessingStats("dcp_expiration_suppressed_counter")
				}
				handler.commonDcpManager.DoneDcpEvent(msg)
				continue
			}

			switch msg.Opcode {
			case dcpMessage.DCP_MUTATION:
				if runtimeInfo, ok := handler.config.Filter.IsTrapEvent(); ok {
					handler.initAndSendTrappedEvent(runtimeInfo, processManager.DcpMutation, msg, parsedDetails)
					continue
				}

				handler.config.StatsHandler.IncrementProcessingStats("dcp_mutation_sent_to_worker")
				handler.writeMessage(workerID, processManager.DcpMutation, msg, parsedDetails)

			case dcpMessage.DCP_DELETION:
				if runtimeInfo, ok := handler.config.Filter.IsTrapEvent(); ok {
					handler.initAndSendTrappedEvent(runtimeInfo, processManager.DcpDeletion, msg, parsedDetails)
					continue
				}

				handler.config.StatsHandler.IncrementProcessingStats("dcp_deletion_sent_to_worker")
				handler.writeMessage(workerID, processManager.DcpDeletion, msg, parsedDetails)

			case dcpMessage.DCP_EXPIRATION:
				if runtimeInfo, ok := handler.config.Filter.IsTrapEvent(); ok {
					handler.initAndSendTrappedEvent(runtimeInfo, processManager.DcpDeletion, msg, parsedDetails)
					continue
				}

				handler.config.StatsHandler.IncrementProcessingStats("dcp_expiration_sent_to_worker")
				handler.writeMessage(workerID, processManager.DcpDeletion, msg, parsedDetails)

			case dcpMessage.DCP_STREAMREQ:
				update := false
				switch msg.Status {
				case dcpMessage.SUCCESS:
					handler.config.StatsHandler.IncrementProcessingStats("dcp_streamreq")
					update = handler.allocator.vbReadyState(msg)

				default:
					streamendMsg := fmt.Sprintf("dcp_streamend-%d", msg.Status)
					handler.config.StatsHandler.IncrementProcessingStats(streamendMsg)
					handler.allocator.DoneVb(msg.SrRequest)
				}
				if update {
					handler.config.CheckpointManager.UpdateVal(msg.Vbno, checkpointManager.Checkpoint_FailoverLog, msg.FailoverLog)
				}

			case dcpMessage.DCP_SYSTEM_EVENT:
				handler.config.StatsHandler.IncrementProcessingStats("dcp_system_event")
				if msg.EventType == dcpMessage.COLLECTION_DROP || msg.EventType == dcpMessage.COLLECTION_FLUSH {
					handler.writeMessage(workerID, processManager.DcpCollectionDelete, msg, parsedDetails)
					handler.flushNotifier.Notify()
				}

			case dcpMessage.DCP_ADV_SEQNUM:
				handler.config.StatsHandler.IncrementProcessingStats("dcp_adv_seqno")
				handler.writeMessage(workerID, processManager.DcpNoOp, msg, parsedDetails)

			case dcpMessage.DCP_STREAM_END:
				if msg.Status == dcpMessage.ROLLBACK {
					handler.config.StatsHandler.IncrementProcessingStats("rollback")
					logging.Warnf("%s Got rollback message for vb: %d", logPrefix, msg.Vbno)
				}
				handler.config.StatsHandler.IncrementProcessingStats("dcp_streamend")
				sendNoOp := handler.allocator.DoneVb(msg.SrRequest)
				if sendNoOp {
					workerID = handler.allocator.GetWorkerId(msg)
					handler.writeMessage(workerID, processManager.DcpNoOp, msg, parsedDetails)
				} else {
					handler.config.StatsHandler.IncrementProcessingStats("forced_closed")
				}
			}
			handler.commonDcpManager.DoneDcpEvent(msg)

		case <-ctx.Done():
			return
		}
	}
}

func (handler *vbHandler) flusher(ctx context.Context) {
	logPrefix := fmt.Sprintf("vbHandler::flusher[%s]", handler.logPrefix)

	t := time.NewTicker(time.Duration(handler.config.HandlerSettings.FlushTimer) * time.Millisecond)

	defer func() {
		t.Stop()
	}()

	for {
		select {
		case <-t.C:
			handler.flushMessage()

		case <-handler.flushNotifier.Wait():
			handler.flushMessage()
			handler.flushNotifier.Ready()

		case <-handler.flushNotifier.PauseWait():
			logging.Infof("%s blocking flusher", logPrefix)
			handler.flushNotifier.WaitResume()
			logging.Infof("%s unblocking flusher", logPrefix)
			handler.allocator.notify()

		case <-ctx.Done():
			return
		}
	}
}

func (handler *vbHandler) GetHighSeqNum() map[uint16]uint64 {
	return handler.allocator.GetHighSeqNum()
}

// Notify that vb map is changed. Returns new vb map, added vbs and closed vbs
func (handler *vbHandler) NotifyOwnershipChange() (string, []uint16, []uint16, []uint16, error) {
	distributedVbsBytes, vbMapVersion, toOwn, toClose, notFullyOwned, err := handler.allocator.VbDistribution()
	if err != nil {
		return vbMapVersion, toOwn, toClose, notFullyOwned, err
	}
	if len(toClose) != 0 {
		// Don't send any message to c++. Wait till c++ get sync with the golang
		handler.flushNotifier.Pause()
	}

	for _, vb := range toClose {
		handler.config.RuntimeSystem.VbSettings(handler.config.Version, processManager.FilterVb, handler.config.InstanceID, vb, nil)
	}

	handler.config.RuntimeSystem.VbSettings(handler.config.Version, processManager.VbMap, handler.config.InstanceID, nil, distributedVbsBytes)
	if len(toClose) != 0 {
		handler.flushNotifier.Resume()
	}

	return vbMapVersion, toOwn, toClose, notFullyOwned, nil
}

// Returns the list of vbs that it needs to be claimed
func (handler *vbHandler) VbHandlerSnapshot(appProgress *common.AppRebalanceProgress) {
	handler.allocator.VbHandlerSnapshot(appProgress)
}

func (handler *vbHandler) AddVb(vb uint16, vbBlob *checkpointManager.VbBlob) int {
	numCurrent, _ := handler.allocator.AddVb(vb, vbBlob)
	return numCurrent
}

func (handler *vbHandler) RefreshSystemResourceLimits() {
	handler.allocator.refreshMemory()
}

func (handler *vbHandler) CloseVb(vb uint16) int {
	return handler.allocator.CloseVb(vb)
}

func (handler *vbHandler) AckMessages(value []byte) (int, int) {
	totalUnAckedCount, totalUnackedSize := 0, 0
	for index := int32(0); index < int32(handler.config.HandlerSettings.CppWorkerThread); index++ {
		ackedBytes := binary.BigEndian.Uint32(value)
		ackedCount := binary.BigEndian.Uint32(value[4:])
		dcpEventsExecutedCount := binary.BigEndian.Uint64(value[8:])
		value = value[16:]
		unAckedCount, unAckedBytes := handler.workers[index].unackedDetails.AckMessage(int32(ackedCount), int32(ackedBytes), uint64(dcpEventsExecutedCount))
		totalUnAckedCount += int(unAckedCount)
		totalUnackedSize += int(unAckedBytes)

	}
	return totalUnAckedCount, totalUnackedSize
}

// Close all the vbs
func (handler *vbHandler) Close() []uint16 {
	logPrefix := fmt.Sprintf("vbHandler::Close[%s]", handler.logPrefix)
	// Don't change the sequence of close
	// Close the requester before closing the consumer
	ownershipVbSlice := handler.allocator.Close()
	handler.msgBuffer.Close()
	handler.flushNotifier.Close()
	handler.commonDcpManager.CloseManager()
	handler.isolatedDcpManager.CloseManager()
	handler.isolatedDcpManager.CloseConditional()
	handler.close()
	logging.Infof("%s Done closing vbHandler", logPrefix)
	return ownershipVbSlice
}

func (handler *vbHandler) writeMessage(workerID int, opcode uint8, msg *dcpMessage.DcpEvent, internalInfo *checkpointManager.ParsedInternalDetails) {
	bufferCount, bufferSize := handler.msgBuffer.Write(opcode, uint8(workerID), msg, internalInfo)
	handler.workers[workerID].unackedDetails.NoteUnackedMessage(int32(1), bufferSize)
	if bufferCount > flushBufferCount {
		handler.flushNotifier.Notify()
	}
}

func (handler *vbHandler) flushMessage() {
	handler.msgBuffer.Send()
}
