package functionHandler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/eventing/application"
	checkpointManager "github.com/couchbase/eventing/checkpoint_manager"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/utils"
	dcpMessage "github.com/couchbase/eventing/dcp_connection"
	eventPool "github.com/couchbase/eventing/event_pool"
	vbhandler "github.com/couchbase/eventing/function_manager/function_handler/vb_handler"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
	pc "github.com/couchbase/eventing/point_connection"
	processManager "github.com/couchbase/eventing/process_manager"
)

const (
	deleteKeyspaceCheck = 1 * time.Minute
)

const (
	eventingXattr    = "_eventing"
	syncgatewayXattr = "_sync"
	mouXattr         = "_mou"
	checkpointXattr  = "_checkpoints"
)

// Sequential event processing
func (fHandler *funcHandler) deleteAllCheckpoint() {
	logPrefix := fmt.Sprintf("funcHandler::deleteAllCheckpoint[%s]", fHandler.logPrefix)
	_, vbSlice, err := fHandler.ownershipRoutine.GetVbMap("", &fHandler.fd.MetaInfo.FunctionScopeID, fHandler.id, fHandler.fd.MetaInfo.MetaID.NumVbuckets, fHandler.fd.Settings.NumTimerPartition, fHandler.fd.AppLocation)
	for err != nil {
		_, vbSlice, err = fHandler.ownershipRoutine.GetVbMap("", &fHandler.fd.MetaInfo.FunctionScopeID, fHandler.id, fHandler.fd.MetaInfo.MetaID.NumVbuckets, fHandler.fd.Settings.NumTimerPartition, fHandler.fd.AppLocation)
		time.Sleep(1 * time.Second)
	}

	logging.Infof("%s getting vbs to be deleted by this node: %s", logPrefix, utils.Condense(vbSlice))
	vbsToDelete := make(map[uint16]struct{})
	for _, vb := range vbSlice {
		vbsToDelete[vb] = struct{}{}
	}

	if !fHandler.fd.MetaInfo.IsUsingTimer {
		for vb := range vbsToDelete {
			fHandler.checkpointManager.Load().DeleteCheckpointBlob(vb)
		}
		logging.Infof("%s deleted all checkpoints. Not using timer.. Exiting", logPrefix)
		return
	}

	logging.Infof("%s Using timers. Deleting all the checkpoint...", logPrefix)
	// Possible that some timer function still running so wait till all nodes gave up the vbucket
	fHandler.checkpointManager.Load().WaitTillAllGiveUp(fHandler.fd.MetaInfo.SourceID.NumVbuckets)
	parallelism := runtime.NumCPU()

	logging.Infof("%s all nodes given up owned vbs. Continue deleting remaining checkpoints documents with parallelism: %d", logPrefix, parallelism)
	function, err := initialiseScanner(fHandler.fd.AppID, fHandler.checkpointManager.Load())
	if err != nil {
		logging.Infof("%s range scan deletion not possible error: %v. Proceeding with dcp stream deletion...", logPrefix, err)
		common.DistributeAndWaitWork[uint16](parallelism, len(vbsToDelete), initDcpDeletion(vbsToDelete), fHandler.deleteCheckpointsUsingDcp)
	} else {
		// Delete metadata checkpoints using range scan
		common.DistributeAndWaitWork[string](parallelism, parallelism*batchDelete, function, fHandler.deleteCheckpointsUsingRangeScan)
	}
	logging.Infof("%s successfully deleted all checkpoint blobs", logPrefix)
}

const batchDelete = 50

func initialiseScanner(appId uint32, checkpointManager checkpointManager.Checkpoint) (func(chan<- string) error, error) {
	// TODO: Find effective way to distribute the load to all nodes
	// Currently all nodes will start range scan for all keys
	// Need to distribute range to all the nodes
	scanner, err := checkpointManager.GetAllCheckpoints(appId)
	if err != nil {
		return nil, err
	}

	return func(workerChan chan<- string) error {
		for {
			item := scanner.Next()
			if item == nil {
				return nil
			}
			workerChan <- item.ID()
		}

	}, nil
}

func (fHandler *funcHandler) deleteCheckpointsUsingRangeScan(waitGroup *sync.WaitGroup, workerChan <-chan string) {
	logPrefix := fmt.Sprintf("funcHandler::deleteCheckpointsUsingRangeScan[%s]", fHandler.logPrefix)

	deleteKeys := make([]string, 0, batchDelete)
	defer func() {
		deleteKeys = fHandler.tryDeleteKeys(true, deleteKeys)
		if len(deleteKeys) != 0 {
			logging.Warnf("%s some keys are not cleared: %d", logPrefix, len(deleteKeys))
		}
		defer waitGroup.Done()
	}()

	for key, ok := <-workerChan; ok; key, ok = <-workerChan {
		deleteKeys = append(deleteKeys, key)
		deleteKeys = fHandler.tryDeleteKeys(false, deleteKeys)
	}
}

func initDcpDeletion(vbsToDelete map[uint16]struct{}) func(chan<- uint16) error {
	return func(workerChan chan<- uint16) error {
		for vb := range vbsToDelete {
			workerChan <- vb
		}
		return nil
	}
}

func (fHandler *funcHandler) deleteCheckpointsUsingDcp(waitGroup *sync.WaitGroup, workerChan <-chan uint16) {
	logPrefix := fmt.Sprintf("funcHandler::deleteCheckpointsUsingDcp[%s]", fHandler.logPrefix)

	keyPrefix := checkpointManager.GetCheckpointKeyTemplate(fHandler.fd.AppID)
	eventChannel := make(chan *dcpMessage.DcpEvent, eventChannelSize)
	manager := fHandler.pool.GetDcpManagerPool(eventPool.CommonConn, "", fHandler.fd.DeploymentConfig.MetaKeyspace.BucketName, eventChannel)
	defer manager.CloseManager()

	deleteKeyspaceCheck := time.NewTicker(deleteKeyspaceCheck)
	defer deleteKeyspaceCheck.Stop()

	deleteKeys := make([]string, 0, batchDelete)

	defer func() {
		fHandler.tryDeleteKeys(true, deleteKeys)
		waitGroup.Done()
	}()

	for vb, ok := <-workerChan; ok; vb, ok = <-workerChan {
		sr := &dcpMessage.StreamReq{
			ID:            uint16(1),
			EndSeq:        uint64(0),
			Flags:         dcpMessage.TillLatest,
			RequestType:   dcpMessage.Request_Collections,
			CollectionIDs: []string{fHandler.fd.MetaInfo.MetaID.CollectionID},
			Vbno:          vb,
			StartSeq:      uint64(0),
		}

		retries := 5
		for ; retries > 0; retries-- {
			err := manager.StartStreamReq(sr)
			if err == nil {
				break
			}
			logging.Errorf("%s error making stream request for vb: %d err: %v, numRetries remaining: %d", logPrefix, vb, err, retries)
		}

		if retries == 0 {
			continue
		}

		processingVb := true
		for processingVb {
			select {
			case msg := <-eventChannel:
				switch msg.Opcode {
				case dcpMessage.DCP_MUTATION:
					docID := string(msg.Key)
					if strings.HasPrefix(docID, keyPrefix) {
						deleteKeys = append(deleteKeys, docID)
					}
					deleteKeys = fHandler.tryDeleteKeys(false, deleteKeys)

				case dcpMessage.DCP_STREAM_END, dcpMessage.DCP_STREAMREQ:
					if msg.Opcode == dcpMessage.DCP_STREAM_END && msg.Status == dcpMessage.SUCCESS {
						processingVb = false
					}

					if msg.Status != dcpMessage.SUCCESS {
						succeeded := false
						for ; retries > 0; retries-- {
							err := manager.StartStreamReq(sr)
							if err != nil {
								logging.Errorf("%s error making stream request for vb: %d err: %v, numRetries remaining: %d", logPrefix, vb, err, retries)
								continue
							}
							succeeded = true
							logging.Infof("%s successfully started streamrequest for %d after receiving error code: %d remaining: %d", logPrefix, vb, msg.Status, retries)
							break
						}
						if retries == 0 && !succeeded {
							processingVb = false
						}
					}

				default:
				}

			case <-deleteKeyspaceCheck.C:
				if !common.CheckKeyspaceExist(fHandler.observer, fHandler.fd.DeploymentConfig.MetaKeyspace) {
					logging.Errorf("%s keyspace is deleted. Exiting routine", logPrefix)
					return
				}
			}
		}
	}
}

func (fHandler *funcHandler) tryDeleteKeys(forceDelete bool, deleteKeys []string) []string {
	if forceDelete || len(deleteKeys) == batchDelete {
		fHandler.checkpointManager.Load().DeleteKeys(deleteKeys)
		return deleteKeys[:0]
	}
	return deleteKeys
}

type xattrMetadata struct {
	FunctionInstanceID string `json:"fiid"`
	SeqNo              string `json:"seqno"`
	ValueCRC           string `json:"crc"`
	CAS                string `json:"cas"`
}

func isRecursiveMutation(msg *dcpMessage.DcpEvent, instanceID string, rootCas uint64) bool {
	if msg.Opcode != dcpMessage.DCP_MUTATION && msg.Opcode != dcpMessage.DCP_DELETION {
		return false
	}

	if (msg.Datatype & dcpMessage.XATTR) != dcpMessage.XATTR {
		return false
	}

	eventingXattr, ok := msg.SystemXattr[eventingXattr]
	if !ok {
		return false
	}

	var xattr xattrMetadata
	err := json.Unmarshal(eventingXattr.Bytes(), &xattr)
	if err != nil {
		return false
	}

	if xattr.FunctionInstanceID != instanceID {
		return false
	}

	if xattr.CAS != "" {
		xCas := common.HexLittleEndianToUint64([]byte(xattr.CAS))
		return xCas == rootCas
	}

	// Old style recursive mutation detection
	seqNum, err := strconv.ParseUint(xattr.SeqNo, 0, 64)
	if err != nil {
		return false
	}

	return seqNum == msg.Seqno
}

type xattrMou struct {
	ImportCAS string `json:"cas"`
	PCAS      string `json:"pCas"`
}

type xattrChkpt struct {
	CAS  string `json:"cas"`
	PCAS string `json:"pcas"`
}

// Can potentially reach here because mutation was not an SBM.
// However, this might be a false negative as this mutation might be one
// of the checkpoint updates on top of an SBM.
// Hence, attempt to parse _checkpoints regardless of whether this handler
// is cursor aware or not
func processCheckpointMutation(parsedDetails *checkpointManager.ParsedInternalDetails, msg *dcpMessage.DcpEvent, instanceID string) bool {
	xattrMouBody, xattrMouFound := msg.SystemXattr[mouXattr]
	xattrChkptBody, xattrChkptFound := msg.SystemXattr[checkpointXattr]
	// Note: Assuming this app's cursor doesn't exist, hence thisAppRootCas == 0 by default
	// Assuming this mutation itself is the root mutation, hence thisMutationRootCas == cas by default
	var thisAppRootCas uint64
	thisMutationRootCas := msg.Cas
	if xattrMouFound {
		var xMouRaw xattrMou
		json.Unmarshal(xattrMouBody.Bytes(), &xMouRaw)
		xImportCAS := common.HexLittleEndianToUint64([]byte(xMouRaw.ImportCAS))
		if msg.Cas == xImportCAS {
			xPCAS := common.HexLittleEndianToUint64([]byte(xMouRaw.PCAS))
			thisMutationRootCas = xPCAS
		}
	}

	if xattrChkptFound {
		var xChkptsRaw map[string]xattrChkpt
		json.Unmarshal(xattrChkptBody.Bytes(), &xChkptsRaw)
		var appRootCasFound, mutationRootCasFound bool
		parsedDetails.AllCursorIds = make([]string, 0, len(xChkptsRaw))
		for cursorId, _ := range xChkptsRaw {
			parsedDetails.AllCursorIds = append(parsedDetails.AllCursorIds, cursorId)
		}

		for cursorId, value := range xChkptsRaw {
			xCAS := common.HexLittleEndianToUint64([]byte(value.CAS))
			xPCAS := common.HexLittleEndianToUint64([]byte(value.PCAS))
			if !appRootCasFound {
				if cursorId == instanceID {
					appRootCasFound = true
					thisAppRootCas = xPCAS
				}
			}

			if !mutationRootCasFound {
				if xCAS == thisMutationRootCas {
					mutationRootCasFound = true
					thisMutationRootCas = xPCAS
				}
			}

			if appRootCasFound && mutationRootCasFound {
				break
			}
		}
	}
	parsedDetails.RootCas = thisMutationRootCas
	return thisMutationRootCas == thisAppRootCas
}

func (fHandler *funcHandler) CheckAndGetEventsInternalDetails(msg *dcpMessage.DcpEvent) (*checkpointManager.ParsedInternalDetails, bool) {

	switch msg.Opcode {
	case dcpMessage.DCP_MUTATION:
		// if binary documents not allowed then supress them
		if !fHandler.fd.Settings.BinDocAllowed && ((msg.Datatype & dcpMessage.JSON) != dcpMessage.JSON) {
			return nil, true
		}

		// Check for transaction documents
		if !fHandler.fd.Settings.AllowTransactionDocument &&
			msg.Datatype == dcpMessage.XATTR && bytes.HasPrefix(msg.Key, common.TransactionMutationPrefix) {
			return nil, true
		}

		fallthrough

	case dcpMessage.DCP_DELETION, dcpMessage.DCP_EXPIRATION:
		if msg.Keyspace.GetOriginalKeyspace().ScopeName == dcpMessage.SystemScope {
			return nil, true
		}

		if !fHandler.fd.Settings.AllowSyncDocuments &&
			(bytes.HasPrefix(msg.Key, common.SyncGatewayMutationPrefix) && !bytes.HasPrefix(msg.Key, common.SyncGatewayAttachmentPrefix)) {
			return nil, true
		}

	default:
		return nil, false
	}

	if !fHandler.isSrcMutationPossible {
		return nil, false
	}

	parsedDetails := &checkpointManager.ParsedInternalDetails{}
	suppress := processCheckpointMutation(parsedDetails, msg, fHandler.fd.AppInstanceID)
	if suppress {
		return nil, true
	}

	if isRecursiveMutation(msg, fHandler.fd.AppInstanceID, parsedDetails.RootCas) {
		return nil, true
	}

	if !fHandler.fd.Settings.CursorAware || msg.Opcode != dcpMessage.DCP_MUTATION {
		return nil, false
	}

	keyspace := msg.Keyspace.GetOriginalKeyspace()
	activeCursors, found := fHandler.cursorCheckpointHandler.GetCursors(keyspace)
	if !found {
		parsedDetails.StaleCursorIds = parsedDetails.AllCursorIds
	}

	parsedDetails.StaleCursorIds = make([]string, 0, len(parsedDetails.AllCursorIds))
	for _, cursor := range parsedDetails.AllCursorIds {
		if _, found := activeCursors[cursor]; !found {
			parsedDetails.StaleCursorIds = append(parsedDetails.StaleCursorIds, cursor)
		}
	}
	// populate the stale cursors
	parsedDetails.Scope = keyspace.ScopeName
	parsedDetails.Collection = keyspace.CollectionName
	return parsedDetails, false
}

type spawnType int8

const (
	forDebugger spawnType = iota
	forOnDeploy
)

type debuggerMessageReceiver struct {
	appLocation application.AppLocation
	broadcaster common.Broadcaster
}

func (debuggerMessageReceiver) ApplicationLog(msg string) {
}

func (d debuggerMessageReceiver) ReceiveMessage(msg *processManager.ResponseMessage) {
	const logPrefix = "debuggerMessageReceiver::ReceiveMessage"

	if msg == nil {
		logging.Errorf("%s Unexpected message received", logPrefix)
		req := &pc.Request{
			Query:  application.QueryMap(d.appLocation),
			Method: pc.POST,
		}

		d.broadcaster.Request(true, false, "/stopDebugger", req)
		return
	}

	switch msg.Event {
	case processManager.DcpEvent:
		req := &pc.Request{
			Query:  application.QueryMap(d.appLocation),
			Method: pc.POST,
		}
		d.broadcaster.Request(true, false, "/stopDebugger", req)
	}
}

type onDeployMessageReceiver struct {
	appLocation application.AppLocation
	fHandler    *funcHandler
}

func (o onDeployMessageReceiver) ApplicationLog(msg string) {
	o.fHandler.ApplicationLog(msg)
}

func (o onDeployMessageReceiver) ReceiveMessage(msg *processManager.ResponseMessage) {
	const logPrefix string = "onDeployMessageReceiver::ReceiveMessage"

	if msg == nil {
		logging.Errorf("%s Unexpected message received", logPrefix)
		o.fHandler.checkpointManager.Load().PublishOnDeployStatus(checkpointManager.FailedStateOnDeploy)
		return
	}

	switch msg.Event {
	case processManager.InitEvent:
		switch msg.Opcode {
		case processManager.OnDeployHandler:
			var ack processManager.OnDeployAckMsg
			if err := json.Unmarshal(msg.Value, &ack); err != nil {
				logging.Errorf("%s Failed to unmarshal OnDeploy ack err: %v", logPrefix, err)
				o.fHandler.checkpointManager.Load().PublishOnDeployStatus(checkpointManager.FailedStateOnDeploy)
				return
			}

			if ack.Status != checkpointManager.PendingOnDeploy {
				logging.Infof("%s Received OnDeploy ack for %s with status %s", logPrefix, o.appLocation, ack.Status)
				o.fHandler.checkpointManager.Load().PublishOnDeployStatus(ack.Status)
				return
			}
		}
	}
}

func (fHandler *funcHandler) createRuntimeSystem(event spawnType, funcDetails *application.FunctionDetails, args *runtimeArgs) vbhandler.RuntimeSystem {
	var handler processManager.MessageDeliver

	switch event {
	case forDebugger:
		handler = debuggerMessageReceiver{
			appLocation: funcDetails.AppLocation,
			broadcaster: fHandler.broadcaster,
		}
	case forOnDeploy:
		handler = onDeployMessageReceiver{
			appLocation: funcDetails.AppLocation,
			fHandler:    fHandler,
		}
		funcDetails.Settings.ExecutionTimeout = funcDetails.Settings.OnDeployTimeout
	}

	runtimeEnvironment := fHandler.utilityWorker.CreateUtilityWorker(string(fHandler.instanceID), handler)
	funcDetails.ModifyAppCode(true)
	funcDetails.Settings.CppWorkerThread = 1
	processDetails := runtimeEnvironment.GetProcessDetails()
	runtimeEnvironment.InitEvent(processDetails.Version, processManager.InitHandler, fHandler.instanceID, funcDetails)
	_, config := fHandler.serverConfig.GetServerConfig(funcDetails.MetaInfo.FunctionScopeID)
	featureMatrix := config.FeatureList.GetFeatureMatrix()
	runtimeEnvironment.SendControlMessage(
		processDetails.Version,
		processManager.GlobalConfigChange,
		processManager.FeatureMatrix,
		fHandler.instanceID,
		nil,
		featureMatrix,
	)

	switch event {
	case forDebugger:
		nodes, _ := fHandler.observer.GetCurrentState(notifier.InterestedEvent{Event: notifier.EventEventingTopologyChanges})
		externalHostName := fHandler.clusterSettings.LocalAddress
		for _, node := range nodes.([]*notifier.Node) {
			if node.ThisNode {
				externalHostName = node.ExternalHostName
				break
			}
		}
		dirName, _ := application.GetLogDirectoryAndFileName(false, fHandler.fd, fHandler.clusterSettings.EventingDir)
		value := map[string]interface{}{
			"isIpv4":    (fHandler.clusterSettings.IpMode == common.Ipv4),
			"host_addr": externalHostName,
			"dir":       dirName,
		}

		runtimeEnvironment.InitEvent(processDetails.Version, processManager.DebugHandlerStart, fHandler.instanceID, value)
	case forOnDeploy:
		param := fHandler.getOnDeployActionObject(args.currState)
		runtimeEnvironment.InitEvent(processDetails.Version, processManager.OnDeployHandler, fHandler.instanceID, param)
	}
	return runtimeEnvironment
}

func (fHandler *funcHandler) validateStateForOnDeploy(nextState funcHandlerState) bool {
	currState := fHandler.currState.getCurrState()
	// OnDeploy should run only for "Deploy/Resume" operations
	return (currState == Undeployed || currState == Paused) && nextState == Deployed
}

func (fHandler *funcHandler) getOnDeployActionObject(prevState funcHandlerState) *processManager.OnDeployActionObject {
	var onDeployReason string
	var onDeployDelay int64

	switch prevState {
	case Paused:
		onDeployReason = "Resume"
		lastPausedTimestamp := fHandler.fd.MetaInfo.LastPaused
		currentTimestamp := time.Now()
		onDeployDelay = currentTimestamp.Sub(lastPausedTimestamp).Milliseconds()
	case Undeployed:
		onDeployReason = "Deploy"
		onDeployDelay = 0
	}

	return &processManager.OnDeployActionObject{
		Reason: onDeployReason,
		Delay:  onDeployDelay,
	}
}

func (fHandler *funcHandler) chooseNodeLeaderOnDeploy(seq uint32) (bool, error) {
	return fHandler.checkpointManager.Load().TryTobeLeader(checkpointManager.OnDeployLeader, seq)
}
