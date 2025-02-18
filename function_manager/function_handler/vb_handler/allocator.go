package vbhandler

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/application"
	checkpointManager "github.com/couchbase/eventing/checkpoint_manager"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/utils"
	dcpMessage "github.com/couchbase/eventing/dcp_connection"
	dcpManager "github.com/couchbase/eventing/dcp_manager"
	eventPool "github.com/couchbase/eventing/event_pool"
	"github.com/couchbase/eventing/logging"
	processManager "github.com/couchbase/eventing/process_manager"
	serverConfig "github.com/couchbase/eventing/server_config"
)

const (
	minTimeWait     = 5 * time.Minute
	maxUnackedBytes = uint64(61 * common.MiB)

	noOpThreshold = 50
	lowerMark     = 0.5
	pauseMark     = 0.85
	upperMark     = 1.5
)

type requester struct {
	mode               serverConfig.DeploymentMode
	commonDcpManager   dcpManager.DcpManager
	isolatedDcpManager dcpManager.DcpManager
}

type allocator struct {
	version      uint32
	logPrefix    string
	config       *Config
	requestType  dcpMessage.RequestType
	isStreamMode *atomic.Bool

	maxUnackedBytes atomic.Uint64
	maxUnackedCount float64
	dcpManager      requester
	seqManager      eventPool.SeqNumerInterface

	vbToWorker            atomic.Value
	workerParallelRequest int32
	closedVbsLock         *sync.RWMutex
	closedVbs             map[uint16]struct{}
	workers               []*workerDetails

	ownedVbSlice   atomic.Value
	lastSeqFetched time.Time
	highSeqNum     atomic.Value

	observerNotifier  *common.Signal
	streamRequestList []struct {
		srRequest *dcpMessage.StreamReq
		lastDone  time.Time
	}

	close func()
}

func NewAllocatorWithContext(ctx context.Context, logPrefix string, keyspace application.Keyspace,
	workers []*workerDetails, dcpManager requester,
	config *Config) *allocator {

	al := &allocator{
		logPrefix:        logPrefix,
		version:          uint32(0),
		config:           config,
		dcpManager:       dcpManager,
		observerNotifier: common.NewSignal(),
		workers:          workers,
		closedVbsLock:    &sync.RWMutex{},
		closedVbs:        make(map[uint16]struct{}),
		isStreamMode:     &atomic.Bool{},
		maxUnackedBytes:  atomic.Uint64{},
		streamRequestList: make([]struct {
			srRequest *dcpMessage.StreamReq
			lastDone  time.Time
		}, 0),
	}

	if dcpManager.mode != serverConfig.FunctionGroup {
		al.isStreamMode.Store(true)
	}
	al.vbToWorker.Store(make(map[uint16]int))
	al.ownedVbSlice.Store(make([]uint16, 0))
	al.highSeqNum.Store(make(map[uint16]uint64))
	al.seqManager = config.Pool.GetSeqManager(keyspace.BucketName)

	totalParallelRequest := config.HandlerSettings.MaxParallelVb
	if config.ConfiguredVbs < totalParallelRequest {
		totalParallelRequest = config.ConfiguredVbs
	}

	// If totalParallelRequest is less than workerCount then we make atleast one vb request for each worker
	// uint32 cause uint16 atomic operations are not possible and need locks for concurrency control
	al.workerParallelRequest = int32((totalParallelRequest / uint16(al.config.HandlerSettings.CppWorkerThread)) + 1)

	al.refreshMemory()
	al.maxUnackedCount = math.Ceil(config.HandlerSettings.MaxUnackedCount / float64(al.config.HandlerSettings.CppWorkerThread))

	al.requestType = dcpMessage.Request_Collections
	switch config.MetaInfo.RequestType {
	case application.RequestBucket:
		al.requestType = dcpMessage.Request_Bucket

	case application.RequestScope:
		al.requestType = dcpMessage.Request_Scope

	case application.RequestCollection:
		al.requestType = dcpMessage.Request_Collections
	}
	ctx2, closeContext := context.WithCancel(ctx)
	al.close = closeContext

	go al.spawnObserver(ctx2)

	return al
}

func (al *allocator) GetRuntimeStats() *common.MarshalledData[map[string]interface{}] {
	stats := make(map[string]interface{})
	for index, worker := range al.workers {
		worker.RLock()
		stats[fmt.Sprintf("worker_%s_%d", al.logPrefix, index)] = worker.GetRuntimeStats()
		worker.RUnlock()
	}

	stats["dcp_manager_stats"] = al.dcpManager.isolatedDcpManager.GetRuntimeStats()
	s := common.NewMarshalledData(stats)
	return s
}

// FilterEvent check whether eventing requested this document or not
func (al *allocator) FilterEvent(msg *dcpMessage.DcpEvent) (*checkpointManager.ParsedInternalDetails, int, bool, bool) {
	switch msg.Opcode {
	case dcpMessage.DCP_MUTATION, dcpMessage.DCP_DELETION, dcpMessage.DCP_EXPIRATION, dcpMessage.DCP_ADV_SEQNUM, dcpMessage.DCP_SYSTEM_EVENT:
		parsedDetails, filter := al.config.Filter.CheckAndGetEventsInternalDetails(msg)
		if filter {
			return parsedDetails, 0, true, false
		}

		worker, workerID := al.getWorkerDetail(msg.Vbno)
		if workerID == -1 {
			return parsedDetails, 0, true, false
		}

		yieldGoRoutine := false
		defer func() {
			if yieldGoRoutine {
				runtime.Gosched()
			}
		}()

		vb := msg.Vbno

		worker.Lock()
		defer worker.Unlock()
		status, ok := worker.runningMap[vb]
		if !ok {
			return parsedDetails, workerID, true, false
		}

		if !status.isRunning() {
			return parsedDetails, workerID, true, false
		}

		totalMsg, totalBytes := worker.unackedDetails.UnackedMessageCount()
		if totalBytes > upperMark*float64(al.maxUnackedBytes.Load()) ||
			totalMsg > upperMark*al.maxUnackedCount {

			yieldGoRoutine = true
			if status.IsStreaming {
				// Streaming mode can't just leave
				return parsedDetails, workerID, false, true
			}

			al.dcpManager.commonDcpManager.CloseRequest(0, vb)
			al.config.StatsHandler.IncrementCountProcessingStats("already_sent_streamend", 1)
			status.Status = forcedClosed
			return parsedDetails, workerID, true, false
		}

		if !al.isStreamMode.Load() && totalBytes > pauseMark*float64(al.maxUnackedBytes.Load()) ||
			totalMsg > pauseMark*al.maxUnackedCount {
			if status.Status != paused {
				al.config.StatsHandler.IncrementCountProcessingStats("pause_request", 1)
				al.dcpManager.commonDcpManager.PauseStreamReq(0, vb)
				status.Status = paused
			}
		}
		status.LastSentSeq = msg.Seqno
		return parsedDetails, workerID, false, false

	default:
		// Other messages just allow
	}

	return nil, 0, false, false
}

func (al *allocator) GetHighSeqNum() map[uint16]uint64 {
	return al.getHighSeqNum()
}

// Don't change the order
func (al *allocator) VbDistribution() (distributedVbsBytes []byte, vbMapVersion string, toOwn, toClose, notFullyOwned []uint16, err error) {
	logPrefix := fmt.Sprintf("allocator::VbDistribution[%s]", al.logPrefix)

	vbMapVersion, vbs, err := al.getVbOwnershipMap()
	if err != nil {
		return nil, vbMapVersion, nil, nil, nil, fmt.Errorf("error getting vbownership map: %v", err)
	}
	vbtoWorker := make(map[uint16]int)

	perWorkerVbs := (int32(len(vbs)) / int32(al.config.HandlerSettings.CppWorkerThread)) + 1
	distributedVbs := make([][]uint16, al.config.HandlerSettings.CppWorkerThread)
	for index, _ := range distributedVbs {
		distributedVbs[index] = make([]uint16, 0, perWorkerVbs)
	}

	timer, nonTimer := GetTimerPartitionsInVbs(vbs, al.config.ConfiguredVbs, al.config.HandlerSettings.NumTimerPartition)
	oldVbToWorkerMap := al.getVbToWorkerMap()
	index := int32(0)

	for _, vb := range timer {
		if workerID, ok := oldVbToWorkerMap[vb]; ok {
			vbtoWorker[vb] = int(workerID)
			distributedVbs[workerID] = append(distributedVbs[workerID], vb)
			continue
		}
		vbtoWorker[vb] = int(index)
		distributedVbs[index] = append(distributedVbs[index], vb)
		index = (index + 1) % int32(al.config.HandlerSettings.CppWorkerThread)
	}

	index = 0
	for _, vb := range nonTimer {
		if workerID, ok := oldVbToWorkerMap[vb]; ok {
			vbtoWorker[vb] = int(workerID)
			distributedVbs[workerID] = append(distributedVbs[workerID], vb)
			continue
		}
		vbtoWorker[vb] = int(index)
		distributedVbs[index] = append(distributedVbs[index], vb)
		index = (index + 1) % int32(al.config.HandlerSettings.CppWorkerThread)
	}

	distributedVbsBytes, plan := planToBytes(len(vbs), distributedVbs)
	al.ownedVbSlice.Store(vbs)
	toOwn, toClose, notFullyOwned = al.updateNewOwnership(vbtoWorker)
	logging.Infof("%s vbs to be owned: %s allocate vbs plan perworker: %s", logPrefix, utils.Condense(vbs), utils.CondenseMap(plan))
	return
}

// Already init the vb
func (al *allocator) AddVb(vb uint16, vbBlob *checkpointManager.VbBlob) (int, bool) {
	sr := &dcpMessage.StreamReq{
		FailoverLog:   vbBlob.FailoverLog,
		RequestType:   al.requestType,
		ScopeID:       al.config.MetaInfo.SourceID.ScopeID,
		ManifestUID:   al.config.MetaInfo.SourceID.UID,
		CollectionIDs: []string{al.config.MetaInfo.SourceID.CollectionID},
		Vbno:          vb,
		StartSeq:      vbBlob.ProcessedSeqNum,
		Vbuuid:        vbBlob.Vbuuid,
	}

	worker, workerID := al.getWorkerDetail(vb)
	if workerID == -1 {
		// Atleast 1 vbs are not owned yet. Notify owner will be issued again and vbs will be acquired again
		return 1, false
	}

	worker.Lock()
	count, send := worker.AddVb(vb, sr, al.isStreamMode.Load())
	worker.Unlock()

	if send {
		al.config.RuntimeSystem.VbSettings(al.config.Version, processManager.VbAddChanges, al.config.InstanceID, vb, []uint64{vbBlob.ProcessedSeqNum, vbBlob.Vbuuid})
		al.config.StatsHandler.IncrementCountProcessingStats("agg_messages_sent_to_worker", 1)
	}
	return count, send
}

// This is called when ownership is given up
func (al *allocator) CloseVb(vb uint16) int {
	al.closedVbsLock.Lock()
	defer al.closedVbsLock.Unlock()

	delete(al.closedVbs, vb)
	return len(al.closedVbs)
}

func (al *allocator) GetWorkerId(msg *dcpMessage.DcpEvent) int {
	_, workerId := al.getWorkerDetail(msg.Vbno)
	return workerId
}

// DoneVb means the asked request done
func (al *allocator) DoneVb(streamReq *dcpMessage.StreamReq) (sendNoop bool) {
	vb := streamReq.Vbno
	worker, workerID := al.getWorkerDetail(vb)
	if workerID == -1 {
		return
	}

	worker.Lock()
	status, ok := worker.runningMap[vb]
	if !ok {
		worker.Unlock()
		return
	}

	if status.Version != streamReq.Version {
		worker.Unlock()
		return
	}

	sendNoop = true
	if status.Status == forcedClosed {
		sendNoop = false
		streamReq.StartSeq = status.LastSentSeq
	}
	delete(worker.runningMap, vb)

	status.Status = waiting
	status.LastDoneRequest = time.Now()
	status.StreamReq = streamReq
	worker.runningCount.Add(-1)
	worker.Unlock()

	totalCount, totalBytes := worker.unackedDetails.UnackedMessageCount()
	if al.isStreamMode.Load() || (totalBytes < lowerMark*float64(al.maxUnackedBytes.Load()) ||
		totalCount < lowerMark*al.maxUnackedCount) {
		al.notify()
	}
	return
}

func (al *allocator) VbHandlerSnapshot(appProgress *common.AppRebalanceProgress) {
	for _, worker := range al.workers {
		appProgress.ToOwn = worker.StillClaimedVbs(appProgress.ToOwn)
	}

	al.closedVbsLock.RLock()
	defer al.closedVbsLock.RUnlock()

	for vb, _ := range al.closedVbs {
		appProgress.ToClose = append(appProgress.ToClose, vb)
	}
}

func (al *allocator) vbReadyState(msg *dcpMessage.DcpEvent) bool {
	vb := msg.Vbno
	worker, workerID := al.getWorkerDetail(vb)
	if workerID == -1 {
		return false
	}

	worker.Lock()
	defer worker.Unlock()

	status, ok := worker.runningMap[vb]
	if !ok {
		return false
	}

	if status.Version != msg.Version || status.Status != ready {
		return false
	}
	status.Status = running
	return true
}

// Check if we need to request more or not
// TODO: Need to improve this algorithm to start request based on speed of the function
func (al *allocator) checkAndMakeRequest() {
	if len(al.ownedVbSlice.Load().([]uint16)) == 0 {
		return
	}

	isStreaming := al.isStreamMode.Load()
	dcpManager := al.dcpManager.isolatedDcpManager
	if !isStreaming {
		dcpManager = al.dcpManager.commonDcpManager
	}

	vbToSeq := al.getHighSeqNum()
	for _, worker := range al.workers {
		if !isStreaming {
			if worker.runningCount.Load() >= al.workerParallelRequest {
				continue
			}

			unackedMsg, unackedBytes := worker.unackedDetails.UnackedMessageCount()
			if unackedMsg > lowerMark*al.maxUnackedCount &&
				unackedBytes > lowerMark*float64(al.maxUnackedBytes.Load()) {
				continue
			}

			unackedMsg, unackedBytes = worker.unackedDetails.UnackedMessageCount()
			if unackedMsg > lowerMark*al.maxUnackedCount &&
				unackedBytes > lowerMark*float64(al.maxUnackedBytes.Load()) {
				continue
			}
		}
		worker.Lock()
		// Use one complete circle
		vbListLength := len(worker.allVbList)
		for count := 0; count < vbListLength; count++ {
			status := worker.allVbList[worker.index]
			worker.index = (worker.index + 1) % vbListLength
			if status.Status != waiting {
				continue
			}

			al.streamRequestList = append(al.streamRequestList, struct {
				srRequest *dcpMessage.StreamReq
				lastDone  time.Time
			}{
				srRequest: status.StreamReq,
				lastDone:  status.LastDoneRequest,
			})
			status.StreamReq = nil
			status.Status = ready
			worker.runningMap[status.Vbno] = status
			totalParallelRequest := worker.runningCount.Add(1)
			if !isStreaming && al.workerParallelRequest <= totalParallelRequest {
				break
			}
		}
		worker.Unlock()

		for _, srStruct := range al.streamRequestList {
			sr := srStruct.srRequest
			vb := sr.Vbno
			endSeqNum := uint64(math.MaxUint64)
			if !isStreaming {
				var ok bool
				endSeqNum, ok = vbToSeq[vb]
				if !ok {
					continue
				}
			}

			worker.updateModeTo(vb, isStreaming)
			if endSeqNum <= sr.StartSeq {
				// Maybe bucket flushed and high seq number is always less than executed seq number
				// check for when we fetched the last seq number and current seq number
				if al.lastSeqFetched.Sub(srStruct.lastDone) < minTimeWait {
					continue
				}

				// Check for rollback of the request
				endSeqNum = sr.StartSeq
			}
			sr.EndSeq = endSeqNum
			err := dcpManager.StartStreamReq(sr)
			if err != nil {
				continue
			}

			al.config.StatsHandler.IncrementCountProcessingStats("dcp_stream_req_counter", 1)
		}

		al.streamRequestList = al.streamRequestList[:0]
	}
}

func (al *allocator) notify() {
	al.observerNotifier.Notify()
}

// Once closed further requests are not gonna make
// Vbstatus will be held by this and will wait till
// All the CloseVb requests are not processed
func (al *allocator) Close() []uint16 {
	al.close()
	al.seqManager.CloseManager()

	possibleOwnedVbs := al.ownedVbSlice.Swap(make([]uint16, 0)).([]uint16)
	ownedVbs := make([]uint16, 0, len(possibleOwnedVbs))
	vbStatuses := make([]*vbStatus, 0, 128)

	for _, worker := range al.workers {
		worker.Lock()
		for _, status := range worker.allVbList {
			vbStatus, ok := worker.CloseVb(status.Vbno)
			if !ok {
				continue
			}
			vbStatuses = append(vbStatuses, vbStatus)
		}
		worker.Unlock()

		al.closedVbsLock.Lock()
		for _, vbStatus := range vbStatuses {
			if vbStatus.Status == initStatus {
				continue
			}
			vb := vbStatus.Vbno
			ownedVbs = append(ownedVbs, vb)
			al.closedVbs[vb] = struct{}{}
		}
		al.closedVbsLock.Unlock()
	}

	for _, vbStatus := range vbStatuses {
		if requested, streaming := vbStatus.isRequested(); requested {
			if !streaming {
				al.dcpManager.commonDcpManager.CloseRequest(0, vbStatus.Vbno)
			} else {
				al.dcpManager.isolatedDcpManager.CloseRequest(0, vbStatus.Vbno)
			}

		}
	}

	al.highSeqNum.Store(make(map[uint16]uint64))
	return ownedVbs
}

// Internal Functions
func (al *allocator) getVbOwnershipMap() (string, []uint16, error) {
	vbMapVersion, vbSlice, err := al.config.OwnershipRoutine.GetVbMap(&al.config.MetaInfo.FunctionScopeID, al.config.FuncID, al.config.ConfiguredVbs, al.config.HandlerSettings.NumTimerPartition, al.config.AppLocation)
	return vbMapVersion, vbSlice, err
}

func (al *allocator) updateNewOwnership(vbToWorker map[uint16]int) (toOwn, toClose, notFullyOwned []uint16) {
	toClose, toOwn, notFullyOwned = make([]uint16, 0, len(vbToWorker)), make([]uint16, 0, len(vbToWorker)), make([]uint16, 0, len(vbToWorker))
	oldVbToWorkerMap := al.getVbToWorkerMap()

	for vb, workerID := range oldVbToWorkerMap {
		if _, ok := vbToWorker[vb]; !ok {
			toClose = append(toClose, vb)
			al.workers[workerID].Lock()
			vbStatus, ok := al.workers[workerID].CloseVb(vb)
			if !ok {
				continue
			}

			if isRequestesd, streamMode := vbStatus.isRequested(); isRequestesd {
				if streamMode {
					al.dcpManager.isolatedDcpManager.CloseRequest(0, vb)
				} else {
					al.dcpManager.commonDcpManager.CloseRequest(0, vb)
				}
			}

			if !vbStatus.isOwned() {
				notFullyOwned = append(notFullyOwned, vb)
			}

			al.workers[workerID].Unlock()

			al.closedVbsLock.Lock()
			al.closedVbs[vb] = struct{}{}
			al.closedVbsLock.Unlock()
		}
	}

	for vb, workerID := range vbToWorker {
		_, ok := oldVbToWorkerMap[vb]
		if !ok {
			al.workers[workerID].Lock()
			al.workers[workerID].InitVb(vb)
			al.workers[workerID].Unlock()
			toOwn = append(toOwn, vb)
		}
	}

	al.vbToWorker.Store(vbToWorker)
	return
}

func (al *allocator) GetSeqNumber() {
	vbToSeq, err := al.seqManager.GetSeqNumber(al.ownedVbSlice.Load().([]uint16), "")
	if err != nil {
		return
	}
	al.lastSeqFetched = time.Now()
	al.highSeqNum.Store(vbToSeq)
}

func (al *allocator) spawnObserver(ctx context.Context) {
	logPrefix := fmt.Sprintf("allocator::AllocatorDetails[%s]", al.logPrefix)
	seqChecker := time.NewTicker(time.Duration(al.config.HandlerSettings.CheckInterval) * time.Millisecond)
	printLog := time.NewTicker(30 * time.Second)

	defer func() {
		seqChecker.Stop()
	}()

	for {
		select {
		case <-seqChecker.C:
			al.GetSeqNumber()
			al.checkAndMakeRequest()

		case <-al.observerNotifier.Wait():
			al.observerNotifier.Ready()
			al.checkAndMakeRequest()

		case <-printLog.C:
			for index, worker := range al.workers {
				parallelCount := worker.runningCount.Load()
				unackedMsg, unackedBytes := worker.unackedDetails.UnackedMessageCount()
				logging.Infof("%s->%d parallelRequest: %d unackedMsg: %v unackedBytes: %v", logPrefix, index, parallelCount, unackedMsg, unackedBytes)
			}

		case <-ctx.Done():
			return
		}
	}
}

func (al *allocator) refreshMemory() {
	if al.config.HandlerSettings.MaxUnackedBytes == 0 {
		// Get it from system resource limit of max 61MB or given memory
		memRequired := al.config.SystemResourceDetails.MemRequiredPerThread(al.config.MetaInfo.FunctionScopeID)
		al.maxUnackedBytes.Store(max(maxUnackedBytes, uint64(memRequired)))
	} else {
		al.maxUnackedBytes.Store(uint64(math.Ceil(al.config.HandlerSettings.MaxUnackedBytes / float64(al.config.HandlerSettings.CppWorkerThread))))
	}
}

func (al *allocator) getVbToWorkerMap() map[uint16]int {
	return al.vbToWorker.Load().(map[uint16]int)
}

func (al *allocator) getHighSeqNum() map[uint16]uint64 {
	return al.highSeqNum.Load().(map[uint16]uint64)
}

func (al *allocator) getWorkerDetail(vb uint16) (*workerDetails, int) {
	vbToWorker := al.getVbToWorkerMap()
	workerID, ok := vbToWorker[vb]
	if !ok {
		return nil, -1
	}
	return al.workers[workerID], workerID
}
