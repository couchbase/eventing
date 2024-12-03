package functionHandler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/parser"

	"github.com/couchbase/eventing/application"
	checkpointManager "github.com/couchbase/eventing/checkpoint_manager"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/utils"
	eventPool "github.com/couchbase/eventing/event_pool"
	vbhandler "github.com/couchbase/eventing/function_manager/function_handler/vb_handler"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
	processManager "github.com/couchbase/eventing/process_manager"
	serverConfig "github.com/couchbase/eventing/server_config"
)

const (
	NoStateChange   = uint8(0)
	Started         = uint8(1)
	CheckpointDone  = uint8(2)
	StateChangeDone = uint8(Started | CheckpointDone)
)

const (
	eventChannelSize = 100
	noEvent          = int32(0)
)

const (
	startTrapEvent int32 = 1 << iota
	leaderOfTrapEvent
)

func dummyClose() {}

const (
	lifeCycle uint8 = iota
	vbChangeNotifier
)

type command struct {
	command   uint8
	nextState funcHandlerState
	re        RuntimeEnvironment
	seq       uint32
}

type onDeployInfo struct {
	seq    uint32
	status string
}

type funcHandler struct {
	logPrefix         string
	id                uint16
	funcHandlerConfig Config

	clusterSettings         *common.ClusterSettings
	interruptHandler        InterruptHandler
	observer                notifier.Observer
	ownershipRoutine        vbhandler.Ownership
	pool                    eventPool.ManagerPool
	serverConfig            serverConfig.ServerConfig
	cursorCheckpointHandler CursorCheckpointHandler

	re      RuntimeEnvironment
	version uint32

	vbMapVersion          *atomic.Value
	statsHandler          *statsHandler
	checkpointManager     checkpointManager.Checkpoint
	appLogWriter          *appLogCloser
	isSrcMutationPossible bool
	utilityWorker         UtilityWorker
	broadcaster           common.Broadcaster

	currState *state

	config     *serverConfig.Config
	fd         *application.FunctionDetails
	instanceID []byte

	vbHandler *vbHandlerWrapper

	commandChan chan command
	close       func()

	// 0th bit will be trap event
	// 1st bit will be that this is the leader for the trap event
	trapEvent *atomic.Int32

	// Stores last OnDeploy status with seq number of the state
	onDeployInfo *onDeployInfo
}

func NewFunctionHandler(id uint16, config Config, location application.AppLocation,
	clusterSettings *common.ClusterSettings, interruptHandler InterruptHandler,
	observer notifier.Observer, ownershipRoutine vbhandler.Ownership,
	pool eventPool.ManagerPool, serverConfig serverConfig.ServerConfig, cursorCheckpointHandler CursorCheckpointHandler, utilityWorker UtilityWorker, broadcaster common.Broadcaster) FunctionHandler {

	fHandler := &funcHandler{
		logPrefix:               fmt.Sprintf("%s:%d", location, id),
		id:                      id,
		funcHandlerConfig:       config,
		observer:                observer,
		clusterSettings:         clusterSettings,
		interruptHandler:        interruptHandler,
		pool:                    pool,
		ownershipRoutine:        ownershipRoutine,
		serverConfig:            serverConfig,
		cursorCheckpointHandler: cursorCheckpointHandler,
		utilityWorker:           utilityWorker,
		broadcaster:             broadcaster,

		vbHandler:    NewVbHandlerWrapper(),
		vbMapVersion: &atomic.Value{},
		close:        dummyClose,

		currState:   newState(),
		commandChan: make(chan command, 3),

		trapEvent:    &atomic.Int32{},
		onDeployInfo: &onDeployInfo{},
	}

	fHandler.statsHandler = newStatsHandler(fHandler.logPrefix, location)
	fHandler.vbMapVersion.Store("")
	fHandler.trapEvent.Store(noEvent)

	go fHandler.commandReceiver()
	return fHandler
}

func (fHandler *funcHandler) commandReceiver() {
	logPrefix := fmt.Sprintf("funcHandler::commandReceiver[%s]", fHandler.logPrefix)

	for {
		select {
		case msg, ok := <-fHandler.commandChan:
			if !ok {
				return
			}

			switch msg.command {
			case lifeCycle:
				logging.Infof("%s Lifecycle msg command received: Seq: %d, NextState: %s", logPrefix, msg.seq, msg.nextState)
				ok := fHandler.handleOnDeploy(msg.seq, msg.nextState)
				if !ok {
					logging.Warnf("%s Skipping state change from %s -> %s due to OnDeploy failure", logPrefix, fHandler.currState.getCurrState(), msg.nextState)
					break
				}
				fHandler.changeState(msg.seq, msg.re, msg.nextState)

			case vbChangeNotifier:
				fHandler.notifyOwnershipChange()
			}
		}
	}
}

var valid_logline = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}`)

func (fHandler *funcHandler) GetApplicationLog(size int64) ([]string, error) {
	if fHandler.appLogWriter == nil {
		return nil, nil
	}
	logPrefix := fmt.Sprintf("functionHandler::GetApplicationLog[%s]", fHandler.logPrefix)

	buf, err := fHandler.appLogWriter.Tail(size)
	if err != nil {
		logging.Errorf("%s error fetching applicationLog %v", logPrefix, err)
		return nil, err
	}

	var lines []string
	scanner := bufio.NewScanner(bytes.NewReader(buf))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if len(lines) > 1 && !valid_logline.MatchString(lines[0]) {
		// drop first line as it appears truncated
		lines = lines[1:]
	}

	return lines, nil
}

func (fHandler *funcHandler) Stats() *common.Stats {
	return fHandler.statsHandler.getStats()
}

func (fHandler *funcHandler) ResetStats() {
	fHandler.statsHandler.resetStats()
}

func (fHandler *funcHandler) GetInsight() *common.Insight {
	insight := fHandler.statsHandler.getInsight()
	insight.Script = fHandler.fd.AppCode
	return insight
}

// Lifecycle related operations
// AddFunctionDetails and ChangeState will be called sequentially guranteed that old state is changed
func (fHandler *funcHandler) AddFunctionDetails(funcDetails *application.FunctionDetails) {
	logPrefix := fmt.Sprintf("funcHandler::AddFunctionDetails[%s]", fHandler.logPrefix)

	currState := fHandler.currState.getCurrState()
	switch currState {
	case Paused:
		fHandler.fd = funcDetails.Clone(false)

	case Undeployed:
		fHandler.fd = funcDetails.Clone(false)

	case TempPause:
		fHandler.fd = funcDetails.Clone(false)

	case Deployed:
		// Just apply dynamically changing settings like LogLevel or timer context size
		// Check for changes
		if fHandler.fd.Settings.LogLevel != funcDetails.Settings.LogLevel {
			fHandler.re.SendControlMessage(fHandler.version, processManager.HandlerDynamicSettings, processManager.LogLevelChange, fHandler.instanceID, nil, funcDetails.Settings.LogLevel)
			// take action here also
		}

		if fHandler.fd.Settings.TimerContextSize != funcDetails.Settings.TimerContextSize {
			fHandler.re.SendControlMessage(fHandler.version, processManager.HandlerDynamicSettings, processManager.TimeContextSize, fHandler.instanceID, nil, funcDetails.Settings.TimerContextSize)
		}

		fHandler.fd = funcDetails.Clone(false)

	default:
		// TODO: possible that we are in a transition state due to crash loop of c++ process and user clicked some lifecycle operation
		logging.Errorf("%s function is in internal transition state: %v", logPrefix, currState)
	}
}

func (fHandler *funcHandler) ChangeState(re RuntimeEnvironment, nextState funcHandlerState) *application.FunctionDetails {
	fHandler.commandChan <- command{
		command:   lifeCycle,
		re:        re,
		nextState: nextState,
		seq:       fHandler.fd.MetaInfo.Seq,
	}
	return fHandler.fd
}

func (fHandler *funcHandler) changeState(seq uint32, re RuntimeEnvironment, nextState funcHandlerState) {
	logPrefix := fmt.Sprintf("funcHandler::changeState[%s]", fHandler.logPrefix)

	currState := fHandler.currState.getCurrState()
	reVersion := -1
	if re != nil {
		reVersion = int(re.GetProcessVersion())
	}
	logging.Infof("%s change of state requested: %s -> %s with runtimeEnvironment version: %d", logPrefix, currState, nextState, reVersion)

	switch currState {
	case Deployed:
		fHandler.handleRunningState(seq, nextState)

	case Paused:
		switch nextState {
		case Undeployed:
			// Delete all the checkpoints along with normal checkpoints
			// Delete everything no need to wait for c++ response
			fHandler.currState.changeStateTo(seq, undeploying)
			fHandler.notifyInterrupt()

		case Paused:
			fHandler.notifyInterrupt()

		case Deployed:
			fHandler.currState.changeStateTo(seq, running)
			fHandler.spawnFunction(re)

		case TempPause:
			// State should be same as pause state

		default:
		}

	case pausing:
		switch nextState {
		case TempPause:
			// RuntimeEnvironment crashed so no function will be running
			// Close all the functions and checkpoints
			// State is same as pausing
			vbHandler := fHandler.vbHandler.Load()
			vbs := vbHandler.Close()
			fHandler.close()
			if len(vbs) == 0 {
				fHandler.notifyInterrupt()
				return
			}

			for _, vb := range vbs {
				fHandler.checkpointManager.StopCheckpoint(vb)
			}

		default:
			// Other settings can't possible
		}

	case TempPause:
		switch nextState {
		case Deployed:
			fHandler.currState.changeStateTo(seq, running)
			fHandler.spawnFunction(re)

		default:
			// Other settings not possible
		}

	case undeploying:
		switch nextState {
		case TempPause:
			// State will be same as undeploying
			fHandler.notifyInterrupt()

		default:
			// Other settings not possible
		}

	case Undeployed:
		// All the routines are closed by now so no need to close it again
		switch nextState {
		case Paused, Undeployed:
			fHandler.currState.changeStateTo(seq, nextState)
			fHandler.notifyInterrupt()

		case Deployed:
			fHandler.currState.changeStateTo(seq, running)
			fHandler.spawnFunction(re)

		case TempPause:
			// No need to change the state
		}

	case running:
		switch nextState {
		case TempPause:
			vbHandler := fHandler.vbHandler.Swap(vbhandler.DummyVbHandler)
			vbHandler.Close()
			fHandler.close()
			fHandler.currState.changeStateTo(seq, TempPause)

		default:
			// TODO: Need cleaner way to handle this
			// Continuously kill c++ process and and click undeploy button. State will move to undeploying
			// This can happen if c++ process crashed and during that period user clicked undeploy or other
			// lifecycle operation. Due to this signal will be lost and state will be moved to pausing/undeploying
		}
	}
}

func (fHandler *funcHandler) handleRunningState(seq uint32, newState funcHandlerState) {
	switch newState {
	case Paused:
		vbHandler := fHandler.vbHandler.Load()
		ownedVbs := vbHandler.Close()
		fHandler.close()
		fHandler.currState.changeStateTo(seq, pausing)

		for _, vb := range ownedVbs {
			fHandler.re.VbSettings(fHandler.version, processManager.FilterVb, fHandler.instanceID, vb, uint64(0))
		}
		fHandler.re.LifeCycleOp(fHandler.version, processManager.Stop, fHandler.instanceID)

		if len(ownedVbs) == 0 {
			fHandler.notifyInterrupt()
		}

	case TempPause:
		vbHandler := fHandler.vbHandler.Swap(vbhandler.DummyVbHandler)
		vbHandler.Close()
		fHandler.close()
		fHandler.currState.changeStateTo(seq, TempPause)

	case Undeployed:
		vbHandler := fHandler.vbHandler.Load()
		ownedVbs := vbHandler.Close()
		fHandler.close()
		fHandler.currState.changeStateTo(seq, undeploying)

		for _, vb := range ownedVbs {
			fHandler.re.VbSettings(fHandler.version, processManager.FilterVb, fHandler.instanceID, vb, uint64(0))
		}
		fHandler.re.LifeCycleOp(fHandler.version, processManager.Stop, fHandler.instanceID)
		if len(ownedVbs) == 0 {
			fHandler.notifyInterrupt()
		}
	default:
		return
	}
}

// handleOnDeploy checks if OnDeploy needs to run for this state change, and returns if we should proceed to fhandler change State
func (fHandler *funcHandler) handleOnDeploy(seq uint32, nextState funcHandlerState) bool {
	logPrefix := fmt.Sprintf("funcHandler::handleOnDeploy[%s]", fHandler.logPrefix)

	// Implies OnDeploy is done for this state change
	if fHandler.onDeployInfo.seq == seq {
		// OnDeploy finished for this state change, proceed to other state changes normally for the func handler
		if fHandler.onDeployInfo.status == common.FINISHED.String() {
			return true
		}
		// OnDeploy failed for this state change, so ignore rest of state changes from async funcSet spawnApp
		if nextState == TempPause || nextState == Deployed {
			return false
		}
		return true
	}

	// Check if operation is Deploy/Resume
	if !fHandler.validateStateForOnDeploy(nextState) {
		return true
	}

	if parser.IsCodeUsingOnDeploy(fHandler.fd.AppCode) {
		fHandler.initOnDeploy()
		fHandler.runOnDeploy(seq)

		status := fHandler.getOnDeployStatus()
		switch status {
		case common.FAILED.String():
			if fHandler.funcHandlerConfig.OnDeployLeader {
				logging.Errorf("%s OnDeploy failed for function: %s", logPrefix, fHandler.fd.AppLocation)
			}
			// Give interrupt for failing state change to upper layers
			fHandler.interruptHandler(fHandler.id, seq, fHandler.fd.AppLocation, common.ErrOnDeployFail)
			return false
		case common.FINISHED.String():
			// OnDeploy completed successfully, proceed to deployment
			return true
		}
	}

	return true
}

func (fHandler *funcHandler) initOnDeploy() {
	logPrefix := fmt.Sprintf("funcHandler::initOnDeploy[%s]", fHandler.logPrefix)

	fHandler.instanceID = []byte(fHandler.fd.AppInstanceID)

	var err error
	if fHandler.funcHandlerConfig.SpawnLogWriter && fHandler.appLogWriter == nil {
		_, logFileLocation := application.GetLogDirectoryAndFileName(fHandler.fd, fHandler.clusterSettings.EventingDir)
		fHandler.appLogWriter, err = openAppLog(logFileLocation, 0640, int64(fHandler.fd.Settings.AppLogMaxSize), int64(fHandler.fd.Settings.AppLogMaxFiles))
		if err != nil {
			logging.Errorf("%s Error in opening file: %s, err: %v", logPrefix, err)
		}
	}

	if fHandler.checkpointManager == nil {
		fHandler.checkpointManager = fHandler.pool.GetCheckpointManager(fHandler.fd.AppID, fHandler.interrupt, fHandler.fd.AppLocation, fHandler.fd.DeploymentConfig.MetaKeyspace)
	}

	fHandler.isSrcMutationPossible = fHandler.fd.IsSourceMutationPossible()
}

// runOnDeploy executes OnDeploy in a blocking way till OnDeploy is completed/failed
func (fHandler *funcHandler) runOnDeploy(seq uint32) {
	logPrefix := fmt.Sprintf("functionHandler::runOnDeploy[%s:%d]", fHandler.fd.AppLocation, fHandler.id)

	currState := fHandler.currState.getCurrState()

	isNodeLeader := false
	// There is one function handler OnDeploy leader per node
	if fHandler.funcHandlerConfig.OnDeployLeader {
		// Now choose the node which will do OnDeploy
		isNodeLeader = fHandler.chooseNodeLeaderOnDeploy(seq)

		if isNodeLeader {
			// Only the leader function handler on the chosen node starts OnDeploy process
			logging.Infof("%s Starting OnDeploy process", logPrefix)
			args := &runtimeArgs{
				currState: currState,
			}
			fHandler.createRuntimeSystem(forOnDeploy, fHandler.fd.Clone(false), args)
		}
	}

	// Function handlers on all nodes for the app keep polling till OnDeploy status gets updated by ReceiveMessage
	fHandler.checkpointManager.PollUntilOnDeployCompletes()

	fHandler.onDeployInfo = &onDeployInfo{
		seq:    seq,
		status: fHandler.getOnDeployStatus(),
	}

	if isNodeLeader && fHandler.funcHandlerConfig.OnDeployLeader {
		fHandler.utilityWorker.DoneUtilityWorker(string(fHandler.instanceID))
	}
}

// TODO: This function should always succeed
// Start the function checkpoint handler and dcp stream for ownership vbs
func (fHandler *funcHandler) spawnFunction(re RuntimeEnvironment) {
	logPrefix := fmt.Sprintf("funcHandler::spawnFunction[%s]", fHandler.logPrefix)

	fHandler.re = re
	fHandler.version = re.GetProcessVersion()
	ctx, close := context.WithCancel(context.Background())
	fHandler.close = close

	fHandler.instanceID = []byte(fHandler.fd.AppInstanceID)
	fHandler.fd.ModifyAppCode(true)
	re.InitEvent(fHandler.version, processManager.InitHandler, fHandler.instanceID, fHandler.fd)
	fHandler.config = nil
	fHandler.notifyGlobalConfigChange()

	var err error
	if fHandler.checkpointManager == nil {
		fHandler.checkpointManager = fHandler.pool.GetCheckpointManager(fHandler.fd.AppID, fHandler.interrupt, fHandler.fd.AppLocation, fHandler.fd.DeploymentConfig.MetaKeyspace)
	}

	if fHandler.funcHandlerConfig.SpawnLogWriter && fHandler.appLogWriter == nil {
		_, logFileLocation := application.GetLogDirectoryAndFileName(fHandler.fd, fHandler.clusterSettings.EventingDir)
		fHandler.appLogWriter, err = openAppLog(logFileLocation, 0640, int64(fHandler.fd.Settings.AppLogMaxSize), int64(fHandler.fd.Settings.AppLogMaxFiles))
		if err != nil {
			logging.Errorf("%s Error in opening file: %s, err: %v", logPrefix, err)
			// return nil
		}
	}
	fHandler.isSrcMutationPossible = fHandler.fd.IsSourceMutationPossible()
	_, serverConfig := fHandler.serverConfig.GetServerConfig(fHandler.fd.MetaInfo.FunctionScopeID)

	config := &vbhandler.Config{
		Version:           fHandler.version,
		FuncID:            fHandler.id,
		TenantID:          fHandler.fd.MetaInfo.FunctionScopeID.BucketID,
		AppLocation:       fHandler.fd.AppLocation,
		ConfiguredVbs:     fHandler.fd.MetaInfo.SourceID.NumVbuckets,
		InstanceID:        fHandler.instanceID,
		DcpType:           serverConfig.DeploymentMode,
		HandlerSettings:   fHandler.fd.Settings,
		MetaInfo:          fHandler.fd.MetaInfo,
		RuntimeSystem:     re,
		OwnershipRoutine:  fHandler.ownershipRoutine,
		Pool:              fHandler.pool,
		StatsHandler:      fHandler.statsHandler,
		CheckpointManager: fHandler.checkpointManager,
		Filter:            fHandler,
	}
	vbHandler := vbhandler.NewVbHandler(ctx, fHandler.logPrefix, fHandler.fd.DeploymentConfig.SourceKeyspace, config)
	fHandler.vbHandler.Store(vbHandler)
	go fHandler.statsHandler.start(ctx, fHandler.version, fHandler.instanceID, re, vbHandler, time.Duration(fHandler.fd.Settings.StatsDuration))

	fHandler.notifyOwnershipChange()
	logging.Infof("%s successfully spawned function..", fHandler.logPrefix)
}

func (fHandler *funcHandler) NotifyOwnershipChange() {
	fHandler.commandChan <- command{
		command: vbChangeNotifier,
	}
}

func (fHandler *funcHandler) TrapEvent(trapEvent TrapEventOp, value interface{}) error {
	switch trapEvent {
	case StartTrapEvent:
		if !fHandler.trapEvent.CompareAndSwap(noEvent, startTrapEvent) {
			return fmt.Errorf("debugger already started")
		}

	case StopTrapEvent:
		// This is the leader so close the debugger
		for {
			loadValue := fHandler.trapEvent.Load()
			if loadValue == leaderOfTrapEvent {
				if !fHandler.trapEvent.CompareAndSwap(loadValue, noEvent) {
					continue
				}
				re := fHandler.utilityWorker.GetUtilityWorker()
				if re != nil {
					re.InitEvent(re.GetProcessVersion(), processManager.DebugHandlerStop, fHandler.instanceID, nil)
					fHandler.utilityWorker.DoneUtilityWorker(string(fHandler.instanceID))
				}

			} else if fHandler.trapEvent.CompareAndSwap(loadValue, noEvent) {
				break
			}
		}
	}

	return nil
}

func (fHandler *funcHandler) IsTrapEvent() (vbhandler.RuntimeSystem, bool) {
	logPrefix := fmt.Sprintf("functionHandler::IsTrapEvent[%s]", fHandler.logPrefix)
	if !fHandler.trapEvent.CompareAndSwap(startTrapEvent, leaderOfTrapEvent) {
		return nil, false
	}

	// Try to get the leadership
	leader, err := fHandler.checkpointManager.TryTobeLeader(checkpointManager.DebuggerLeader)
	if err != nil {
		logging.Errorf("%s unable to acquire debugger token: %v", logPrefix, err)
		return nil, false
	}
	if !leader {
		fHandler.trapEvent.Store(noEvent)
		return nil, false
	}

	return fHandler.createRuntimeSystem(forDebugger, fHandler.fd.Clone(false), nil), true
}

func (fHandler *funcHandler) notifyOwnershipChange() {
	logPrefix := fmt.Sprintf("functionHandler::notifyOwnershipChange[%s]", fHandler.logPrefix)
	if !fHandler.currState.isRunning() {
		logging.Infof("%s function not in running state. state: %v", logPrefix, fHandler.currState.isRunning())
		return
	}

	vbHandler := fHandler.vbHandler.Load()
	vbMapVersion, toOwn, toClose, notFullyOwned, err := vbHandler.NotifyOwnershipChange()
	if err != nil {
		// Possible that ownership info not yet receieved. Wait for second notifyownership change
		logging.Errorf("%s error allocating ownership %v", logPrefix, err)
		return
	}

	logging.Infof("%s vb distribution for version: %s notFully owned: %s vbs to own: %s vbs to giveup: %s",
		logPrefix, vbMapVersion, utils.Condense(notFullyOwned), utils.Condense(toOwn), utils.Condense(toClose))

	for _, vb := range notFullyOwned {
		fHandler.checkpointManager.StopCheckpoint(vb)
	}

	fHandler.vbMapVersion.Store(vbMapVersion)
	if len(toOwn) == 0 && len(toClose) == 0 {
		fHandler.notifyInterrupt()
		return
	}

	for _, vb := range toOwn {
		fHandler.checkpointManager.OwnVbCheckpoint(vb)
	}
}

func (fHandler *funcHandler) NotifyGlobalConfigChange() {
	fHandler.notifyGlobalConfigChange()
}

func (fHandler *funcHandler) notifyGlobalConfigChange() {
	if !fHandler.currState.isRunning() {
		return
	}

	_, config := fHandler.serverConfig.GetServerConfig(fHandler.fd.MetaInfo.FunctionScopeID)
	defer func() {
		fHandler.config = config
	}()

	newFeatureMatrix := config.FeatureList.GetFeatureMatrix()
	if fHandler.config == nil {
		fHandler.re.SendControlMessage(fHandler.version, processManager.GlobalConfigChange, processManager.FeatureMatrix, fHandler.instanceID, nil, newFeatureMatrix)
		return
	}

	oldFeatureMatrix := fHandler.config.GetFeatureMatrix()
	if newFeatureMatrix != oldFeatureMatrix {
		fHandler.re.SendControlMessage(fHandler.version, processManager.GlobalConfigChange, processManager.FeatureMatrix, fHandler.instanceID, nil, newFeatureMatrix)
	}
}

func (fHandler *funcHandler) ReceiveMessage(msg *processManager.ResponseMessage) {
	switch msg.Event {
	case processManager.VbSettings:
		vb := binary.BigEndian.Uint16(msg.Extras)
		seq := binary.BigEndian.Uint64(msg.Extras[2:])
		vbuuid := binary.BigEndian.Uint64(msg.Extras[10:])
		fHandler.checkpointManager.UpdateVal(vb, checkpointManager.Checkpoint_SeqNum, []uint64{seq, vbuuid})
		fHandler.checkpointManager.StopCheckpoint(vb)

	case processManager.StatsEvent:
		switch msg.Opcode {
		case processManager.ProcessedEvents:
			processedSeq := fHandler.statsHandler.processedSeqEvents(msg)
			for vb, vbinfo := range processedSeq {
				fHandler.checkpointManager.UpdateVal(vb, checkpointManager.Checkpoint_SeqNum, vbinfo)
			}

		case processManager.StatsAckBytes:
			vbHandler := fHandler.vbHandler.Load()
			unackedCount, unackedBytes := vbHandler.AckMessages(msg.Value)
			fHandler.statsHandler.AddExecutionStats("agg_queue_memory", unackedBytes)
			fHandler.statsHandler.AddExecutionStats("agg_queue_size", unackedCount)

		default:
			fHandler.statsHandler.handleStats(msg)
		}

	case processManager.InternalComm:
		vbHandler := fHandler.vbHandler.Load()
		vbHandler.AckMessages(msg.Value)

	default:
	}
}

func (fHandler *funcHandler) ApplicationLog(log string) {
	ts := time.Now().Format("2006-01-02T15:04:05.000-07:00")
	fmt.Fprintf(fHandler.appLogWriter, "%s [INFO] %s\n", ts, log)
}

// Return if rebalance in progress for given version.
func (fHandler *funcHandler) GetRebalanceProgress(version string, appProgress *common.AppRebalanceProgress) bool {
	// First check whether app is in deployed state or not
	currState := fHandler.currState.getCurrState()
	// If its pausing or undeploying wait till the state transfer happens and Paused and Undeployed returns false
	if currState == TempPause {
		return true
	}

	// Already did all the vb ownership giveup
	if currState == Paused || currState == Undeployed {
		return false
	}

	if version != "" && fHandler.vbMapVersion.Load().(string) != version {
		return true
	}

	fHandler.checkpointManager.OwnershipSnapshot(appProgress)
	vbHandler := fHandler.vbHandler.Load()
	vbHandler.VbHandlerSnapshot(appProgress)

	return false
}

func (fHandler *funcHandler) interrupt(msg checkpointManager.OwnMsg, vb uint16, vbBlob *checkpointManager.VbBlob) {
	remaining := 0
	vbHandler := fHandler.vbHandler.Load()
	switch msg {
	case checkpointManager.OWNERSHIP_OBTAINED:
		remaining = vbHandler.AddVb(vb, vbBlob)

	case checkpointManager.OWNERSHIP_CLOSED:
		remaining = vbHandler.CloseVb(vb)
	}

	if remaining == 0 {
		fHandler.notifyInterrupt()
	}
}

func (fHandler *funcHandler) notifyInterrupt() {
	logPrefix := fmt.Sprintf("funcHandler::notifyInterrupt[%s]", fHandler.logPrefix)
	seq, stateChanged := fHandler.currState.doneState()

	switch stateChanged {
	case running, Deployed:
		// No need to handle this just give the interrupt

	case pausing, Paused:
		// Don't delete any checkpoint
		if fHandler.appLogWriter != nil {
			fHandler.appLogWriter.Close()
			fHandler.appLogWriter = nil
		}

		vbHandler := fHandler.vbHandler.Swap(vbhandler.DummyVbHandler)
		vbHandler.Close()

	case undeploying, Undeployed:
		fHandler.deleteAllCheckpoint()
		if fHandler.appLogWriter != nil {
			fHandler.appLogWriter.Close()
			fHandler.appLogWriter = nil
		}

		if fHandler.checkpointManager != nil {
			fHandler.checkpointManager.CloseCheckpointManager()
			fHandler.checkpointManager = nil
		}

		vbHandler := fHandler.vbHandler.Swap(vbhandler.DummyVbHandler)
		vbHandler.Close()

	default:
		return
	}

	logging.Infof("%s state changed interrupt called: %s seq: %v", logPrefix, stateChanged, seq)
	fHandler.interruptHandler(fHandler.id, seq, fHandler.fd.AppLocation, nil)
}

func (fHandler *funcHandler) CloseFunctionHandler() {
	logPrefix := fmt.Sprintf("funcHandler::closeFunctionHandler[%s]", fHandler.logPrefix)
	vbHandler := fHandler.vbHandler.Swap(vbhandler.DummyVbHandler)
	vbHandler.Close()

	if fHandler.checkpointManager != nil {
		fHandler.checkpointManager.CloseCheckpointManager()
		fHandler.checkpointManager = nil
	}

	fHandler.close()
	close(fHandler.commandChan)
	logging.Infof("%s Function handler closed successfully", logPrefix)
}
