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

	"github.com/couchbase/eventing/application"
	checkpointManager "github.com/couchbase/eventing/checkpoint_manager"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/utils"
	eventPool "github.com/couchbase/eventing/event_pool"
	vbhandler "github.com/couchbase/eventing/function_manager/function_handler/vb_handler"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
	"github.com/couchbase/eventing/parser"
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
	version   string
}

type runtimeContext struct {
	seq            uint32
	onDeployStatus checkpointManager.OnDeployState
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
	isSrcMutationPossible bool
	utilityWorker         UtilityWorker
	broadcaster           common.Broadcaster
	systemResourceDetails vbhandler.SystemResourceDetails

	currState *state

	config       *serverConfig.Config
	configSignal *common.Signal
	fd           *application.FunctionDetails
	instanceID   []byte

	appLogWriter      *common.AtomicTypes[LogWriter]
	checkpointManager *common.AtomicTypes[checkpointManager.Checkpoint]
	vbHandler         *common.AtomicTypes[vbhandler.VbHandler]

	commandChan chan command
	close       func()

	// 0th bit will be trap event
	// 1st bit will be that this is the leader for the trap event
	trapEvent *atomic.Int32

	// Stores last OnDeploy status with seq number of the state
	runtimeContext *runtimeContext
}

func NewFunctionHandler(id uint16, fd *application.FunctionDetails, re RuntimeEnvironment, config Config, location application.AppLocation,
	clusterSettings *common.ClusterSettings, interruptHandler InterruptHandler,
	systemResourceDetails vbhandler.SystemResourceDetails,
	observer notifier.Observer, ownershipRoutine vbhandler.Ownership,
	pool eventPool.ManagerPool, serverConfig serverConfig.ServerConfig, cursorCheckpointHandler CursorCheckpointHandler,
	utilityWorker UtilityWorker, broadcaster common.Broadcaster) FunctionHandler {

	fHandler := &funcHandler{
		logPrefix:               fmt.Sprintf("%s:%d", location, id),
		fd:                      fd,
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
		systemResourceDetails:   systemResourceDetails,

		vbHandler:         common.NewAtomicTypes(vbhandler.NewDummyVbHandler()),
		checkpointManager: common.NewAtomicTypes(checkpointManager.NewDummyCheckpointManager()),
		appLogWriter:      common.NewAtomicTypes(NewDummyLogWriter()),

		configSignal: common.NewSignal(),
		vbMapVersion: &atomic.Value{},
		close:        dummyClose,

		currState: newState(),

		// In any situation we will not have more than 3 commands in the channel
		// TempPause/other lifecycle operations/ rebalance. So 3 is good enough
		commandChan: make(chan command, 3),

		re: re,

		trapEvent: &atomic.Int32{},
		runtimeContext: &runtimeContext{
			seq:            application.OldAppSeq,
			onDeployStatus: checkpointManager.FinishedOnDeploy,
		},
	}

	fHandler.statsHandler = newStatsHandler(fHandler.logPrefix, location)
	fHandler.vbMapVersion.Store("")
	fHandler.trapEvent.Store(noEvent)

	go fHandler.commandReceiver()
	return fHandler
}

var valid_logline = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}`)

func (fHandler *funcHandler) GetApplicationLog(size int64) ([]string, error) {
	logPrefix := fmt.Sprintf("functionHandler::GetApplicationLog[%s]", fHandler.logPrefix)

	buf, err := fHandler.appLogWriter.Load().Tail(size)
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

func (fHandler *funcHandler) Stats(statType common.StatsType) *common.Stats {
	stat := fHandler.statsHandler.getStats(statType)
	switch statType {
	case common.FullDebugStats:
		stat.CheckPointStats[fHandler.logPrefix] = fHandler.checkpointManager.Load().GetRuntimeStats()
		stat.ProcessStats[fHandler.logPrefix] = fHandler.re.GetRuntimeStats()
		stat.VbStats[fHandler.logPrefix] = fHandler.vbHandler.Load().GetRuntimeStats()
		fallthrough

	case common.FullStats:
		fallthrough

	case common.PartialStats:
		appProgress := &common.AppRebalanceProgress{}
		fHandler.checkpointManager.Load().OwnershipSnapshot(appProgress)
		stat.InternalVbDistributionStats[fHandler.logPrefix] = utils.Condense(appProgress.OwnedVbs)
		stat.WorkerPids[fHandler.logPrefix] = fHandler.re.GetProcessDetails().PID
	}
	return stat
}

func (fHandler *funcHandler) ResetStats() {
	fHandler.statsHandler.resetStats()
}

func (fHandler *funcHandler) GetInsight() *common.Insight {
	insight := fHandler.statsHandler.getInsight()
	if insight == nil {
		return nil
	}

	insight.Script = fHandler.fd.AppCode
	return insight
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
					re.InitEvent(re.GetProcessDetails().Version, processManager.DebugHandlerStop, fHandler.instanceID, nil)
					fHandler.utilityWorker.DoneUtilityWorker(string(fHandler.instanceID))
				}

			} else if fHandler.trapEvent.CompareAndSwap(loadValue, noEvent) {
				break
			}
		}
	}

	return nil
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
	if !fHandler.currState.isRunning() {
		return false
	}

	if version != "" && fHandler.vbMapVersion.Load().(string) != version {
		return true
	}

	fHandler.checkpointManager.Load().OwnershipSnapshot(appProgress)
	fHandler.vbHandler.Load().VbHandlerSnapshot(appProgress)

	return false
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
		// If fd is not nil that means we already have the latest fd
		if fHandler.fd != nil {
			if fHandler.fd.Settings.LogLevel != funcDetails.Settings.LogLevel {
				fHandler.re.SendControlMessage(fHandler.version, processManager.HandlerDynamicSettings, processManager.LogLevelChange, fHandler.instanceID, nil, funcDetails.Settings.LogLevel)
				// take action here also
				fHandler.statsHandler.incrementCountProcessingStatsLocked("agg_messages_sent_to_worker", 1)
			}

			if fHandler.fd.Settings.TimerContextSize != funcDetails.Settings.TimerContextSize {
				fHandler.re.SendControlMessage(fHandler.version, processManager.HandlerDynamicSettings, processManager.TimeContextSize, fHandler.instanceID, nil, funcDetails.Settings.TimerContextSize)
				fHandler.statsHandler.incrementCountProcessingStatsLocked("agg_messages_sent_to_worker", 1)
			}
		}

		fHandler.fd = funcDetails.Clone(false)

	default:
		fHandler.fd = funcDetails.Clone(false)
		logging.Errorf("%s function is in internal transition state: %v", logPrefix, currState)
	}
}

// ChangeState is called after AddFunctionDetails is called in a sequential manner
func (fHandler *funcHandler) ChangeState(re RuntimeEnvironment, nextState funcHandlerState) {
	fHandler.commandChan <- command{
		command:   lifeCycle,
		re:        re,
		nextState: nextState,
		seq:       fHandler.fd.MetaInfo.Seq,
	}
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
				// Already executed state change for the given seq number
				if fHandler.runtimeContext.seq > msg.seq {
					break
				}

				ok := fHandler.handleOnDeploy(msg.seq, msg.nextState)
				if !ok {
					logging.Warnf("%s Skipping state change from %s -> %s due to OnDeploy failure", logPrefix, fHandler.currState.getCurrState(), msg.nextState)
					fHandler.runtimeContext.seq = msg.seq
					break
				}
				fHandler.changeState(msg.seq, msg.re, msg.nextState)
				fHandler.runtimeContext.seq = msg.seq

			case vbChangeNotifier:
				fHandler.notifyOwnershipChange(msg.version)
			}

		case <-fHandler.configSignal.Wait():
			fHandler.configSignal.Ready()
			fHandler.notifyGlobalConfigChange()

		}
	}
}

// Functions to run OnDeploy function

// handleOnDeploy checks if OnDeploy needs to run for this state change, and returns if we should proceed to fhandler change State
func (fHandler *funcHandler) handleOnDeploy(seq uint32, nextState funcHandlerState) bool {
	logPrefix := fmt.Sprintf("funcHandler::handleOnDeploy[%s]", fHandler.logPrefix)

	// Implies OnDeploy is done for this state change
	if fHandler.runtimeContext.seq == seq {
		// OnDeploy finished for this state change, proceed to other state changes normally for the func handler
		return nextState != Deployed || fHandler.runtimeContext.onDeployStatus == checkpointManager.FinishedOnDeploy
	}

	// Check if operation is Deploy/Resume and code contains OnDeploy function
	if !(fHandler.validateStateForOnDeploy(nextState) && parser.IsCodeUsingOnDeploy(fHandler.fd.AppCode)) {
		return true
	}

	fHandler.runtimeContext.onDeployStatus = fHandler.runOnDeploy(seq)
	switch fHandler.runtimeContext.onDeployStatus {
	case checkpointManager.FailedStateOnDeploy:
		logging.Errorf("%s OnDeploy failed for function: %s", logPrefix, fHandler.fd.AppLocation)
		// Give interrupt for failing state change to upper layers
		fHandler.interruptHandler(fHandler.id, seq, fHandler.fd.AppLocation, common.ErrOnDeployFail)
		return false

	case checkpointManager.FinishedOnDeploy:
		// OnDeploy completed successfully, proceed to deployment
		return true
	}

	return true
}

// runOnDeploy executes OnDeploy in a blocking way till OnDeploy is completed/failed
func (fHandler *funcHandler) runOnDeploy(seq uint32) checkpointManager.OnDeployState {
	logPrefix := fmt.Sprintf("functionHandler::runOnDeploy[%s:%d]", fHandler.fd.AppLocation, fHandler.id)

	currState := fHandler.currState.getCurrState()
	isNodeLeader := false

	// Init everything which is required to run and poll on deploy function
	fHandler.initOnDeploy()
	defer fHandler.doneOnDeploy()
	// There is one function handler OnDeploy leader per node
	if fHandler.funcHandlerConfig.LeaderHandler {
		isNodeLeader, _ = fHandler.chooseNodeLeaderOnDeploy(seq)
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
	onDeployCheckpoint := fHandler.checkpointManager.Load().PollUntilOnDeployCompletes()
	if isNodeLeader && fHandler.funcHandlerConfig.LeaderHandler {
		fHandler.utilityWorker.DoneUtilityWorker(string(fHandler.instanceID))
	}
	return onDeployCheckpoint.Status
}

func (fHandler *funcHandler) initOnDeploy() {
	logPrefix := fmt.Sprintf("funcHandler::initOnDeploy[%s]", fHandler.logPrefix)

	if fHandler.funcHandlerConfig.LeaderHandler {
		_, logFileLocation := application.GetLogDirectoryAndFileName(false, fHandler.fd, fHandler.clusterSettings.EventingDir)
		appLogWriter, err := openAppLog(logFileLocation, 0640, int64(fHandler.fd.Settings.AppLogMaxSize), int64(fHandler.fd.Settings.AppLogMaxFiles))
		if err != nil {
			logging.Errorf("%s Error in opening file: %s, err: %v", logPrefix, err)
		} else {
			fHandler.appLogWriter.Store(appLogWriter)
		}
	}

	fHandler.instanceID = []byte(fHandler.fd.AppInstanceID)
	fHandler.checkpointManager.Store(fHandler.pool.GetCheckpointManager(fHandler.fd.AppID, fHandler.interrupt, fHandler.fd.AppLocation, fHandler.fd.DeploymentConfig.MetaKeyspace))
	fHandler.isSrcMutationPossible = fHandler.fd.IsSourceMutationPossible()
}

func (fHandler *funcHandler) doneOnDeploy() {
	appHandler := fHandler.appLogWriter.Swap(NewDummyLogWriter())
	appHandler.Close()
}

// Function to change state of a function handler

func (fHandler *funcHandler) changeState(seq uint32, re RuntimeEnvironment, nextState funcHandlerState) {
	logPrefix := fmt.Sprintf("funcHandler::changeState[%s]", fHandler.logPrefix)

	currState := fHandler.currState.getCurrState()
	logging.Infof("%s change of state requested: %s -> %s with runtimeEnvironment version: %d", logPrefix, currState, nextState, re.GetProcessDetails().Version)
	if nextState == TempPause {
		fHandler.statsHandler.IncrementCountProcessingStats("v8_init", 1)
		fHandler.statsHandler.IncrementCountProcessingStats("worker_spawn_counter", 1)
	}

	switch currState {
	case Deployed:
		fHandler.handleRunningState(seq, nextState)

	case Paused:
		switch nextState {
		case Undeployed:
			// Delete all the checkpoints along with normal checkpoints
			// Delete everything no need to wait for c++ response
			// Initilise checkpoint mgr
			newCheckpointMgr := fHandler.pool.GetCheckpointManager(fHandler.fd.AppID, fHandler.interrupt, fHandler.fd.AppLocation, fHandler.fd.DeploymentConfig.MetaKeyspace)
			oldCheckpointMgr := fHandler.checkpointManager.Swap(newCheckpointMgr)
			fHandler.currState.changeStateTo(seq, undeploying)
			oldCheckpointMgr.CloseCheckpointManager()
			fHandler.notifyInterrupt()

		case Paused:
			fHandler.currState.changeStateTo(seq, pausing)
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

			// TODO: Need to give up ownership of the vbs from checkpoint mgr

			fHandler.notifyInterrupt()

		default:
			// Other settings can't possible
		}

	case TempPause:
		switch nextState {
		case Deployed:
			fHandler.currState.changeStateTo(seq, running)
			fHandler.spawnFunction(re)

		// Very rare case where function in Undeployed/Paused and process crashed.
		// Might happen in GroupOfProcess mode only
		case Paused, Undeployed:
			fHandler.currState.changeStateTo(seq, nextState)
			fHandler.notifyInterrupt()

		}

	case undeploying:
		switch nextState {
		case TempPause:
			// State will be same as undeploying
			// notify interrupt will delete all the checkpoints
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
			// This closes all the stats handler and vb handler unimportant go routines which flushes messages to c++ side
			fHandler.close()

			vbHandler := fHandler.vbHandler.Swap(vbhandler.NewDummyVbHandler())
			vbHandler.Close()
			fHandler.currState.changeStateTo(seq, TempPause)

		case Deployed:
			// Already running so no need to do anything

		case Undeployed, Paused:
			// process crashed deplyed called internally. By this time user called undeploy/pause.
			if nextState == Undeployed {
				fHandler.currState.changeStateTo(seq, undeploying)
			} else {
				fHandler.currState.changeStateTo(seq, pausing)
			}
			fHandler.currState.changeStateTo(seq, pausing)
			fHandler.close()

			vbhandler := fHandler.vbHandler.Load()
			ownedVbs := vbhandler.Close()

			for _, vb := range ownedVbs {
				fHandler.re.VbSettings(fHandler.version, processManager.FilterVb, fHandler.instanceID, vb, uint64(0))
			}

			fHandler.re.LifeCycleOp(fHandler.version, processManager.Stop, fHandler.instanceID)
			fHandler.statsHandler.incrementCountProcessingStatsLocked("agg_messages_sent_to_worker", uint64(len(ownedVbs)+1))

			if len(ownedVbs) == 0 {
				fHandler.notifyInterrupt()
			}

		default:
		}
	}
}

func (fHandler *funcHandler) handleRunningState(seq uint32, newState funcHandlerState) {
	switch newState {
	case Paused, Undeployed:
		if newState == Undeployed {
			fHandler.currState.changeStateTo(seq, undeploying)
		} else {
			fHandler.currState.changeStateTo(seq, pausing)
		}

		fHandler.close()

		// Don't swap the vb handler as it contains vbs that needs to be closed
		vbHandler := fHandler.vbHandler.Load()
		ownedVbs := vbHandler.Close()

		for _, vb := range ownedVbs {
			fHandler.re.VbSettings(fHandler.version, processManager.FilterVb, fHandler.instanceID, vb, uint64(0))
		}
		fHandler.statsHandler.IncrementCountProcessingStats("agg_messages_sent_to_worker", 4)
		fHandler.re.LifeCycleOp(fHandler.version, processManager.Stop, fHandler.instanceID)
		fHandler.statsHandler.incrementCountProcessingStatsLocked("agg_messages_sent_to_worker", uint64(len(ownedVbs)+1))

		// Nothing to be owned. Possible in scenario where function is paused and rebalanced or process crashed and respawned
		if len(ownedVbs) == 0 {
			fHandler.notifyInterrupt()
		}

	case TempPause:
		fHandler.close()

		vbHandler := fHandler.vbHandler.Swap(vbhandler.NewDummyVbHandler())
		vbHandler.Close()
		fHandler.currState.changeStateTo(seq, TempPause)

	default:
		return
	}
}

// TODO: This function should always succeed
// Start the function checkpoint handler and dcp stream for ownership vbs
func (fHandler *funcHandler) spawnFunction(re RuntimeEnvironment) {
	logPrefix := fmt.Sprintf("funcHandler::spawnFunction[%s]", fHandler.logPrefix)

	fHandler.re = re
	fHandler.version = re.GetProcessDetails().Version
	ctx, close := context.WithCancel(context.Background())
	fHandler.close = close

	fHandler.instanceID = []byte(fHandler.fd.AppInstanceID)
	fHandler.fd.ModifyAppCode(true)

	re.InitEvent(fHandler.version, processManager.InitHandler, fHandler.instanceID, fHandler.fd)
	fHandler.statsHandler.incrementCountProcessingStatsLocked("agg_messages_sent_to_worker", 1)

	// This will make sure we get recent config
	fHandler.config = nil
	fHandler.notifyGlobalConfigChange()
	checkpointMgr := fHandler.checkpointManager.Swap(fHandler.pool.GetCheckpointManager(fHandler.fd.AppID, fHandler.interrupt, fHandler.fd.AppLocation, fHandler.fd.DeploymentConfig.MetaKeyspace))
	checkpointMgr.CloseCheckpointManager()

	if fHandler.funcHandlerConfig.LeaderHandler {
		_, logFileLocation := application.GetLogDirectoryAndFileName(false, fHandler.fd, fHandler.clusterSettings.EventingDir)
		var err error
		appLogWriter, err := openAppLog(logFileLocation, 0640, int64(fHandler.fd.Settings.AppLogMaxSize), int64(fHandler.fd.Settings.AppLogMaxFiles))
		if err != nil {
			logging.Errorf("%s Error in opening file: %s, err: %v", logPrefix, err)
			// return nil
		} else {
			appLogWriter := fHandler.appLogWriter.Swap(appLogWriter)
			appLogWriter.Close()
		}
	}
	fHandler.isSrcMutationPossible = fHandler.fd.IsSourceMutationPossible()

	config := &vbhandler.Config{
		Version:               fHandler.version,
		FuncID:                fHandler.id,
		TenantID:              fHandler.fd.MetaInfo.FunctionScopeID.BucketID,
		AppLocation:           fHandler.fd.AppLocation,
		ConfiguredVbs:         fHandler.fd.MetaInfo.SourceID.NumVbuckets,
		InstanceID:            fHandler.instanceID,
		DcpType:               fHandler.config.DeploymentMode,
		HandlerSettings:       fHandler.fd.Settings,
		MetaInfo:              fHandler.fd.MetaInfo,
		RuntimeSystem:         re,
		OwnershipRoutine:      fHandler.ownershipRoutine,
		Pool:                  fHandler.pool,
		StatsHandler:          fHandler.statsHandler,
		CheckpointManager:     fHandler.checkpointManager.Load(),
		SystemResourceDetails: fHandler.systemResourceDetails,
		Filter:                fHandler,
	}
	vbHandler := vbhandler.NewVbHandler(ctx, fHandler.logPrefix, fHandler.fd.DeploymentConfig.SourceKeyspace, config)
	oldVbHandler := fHandler.vbHandler.Swap(vbHandler)
	oldVbHandler.Close()
	go fHandler.statsHandler.start(ctx, fHandler.version, fHandler.instanceID, re, vbHandler, time.Duration(fHandler.fd.Settings.StatsDuration))

	fHandler.notifyOwnershipChange("")
	logging.Infof("%s successfully spawned function..", fHandler.logPrefix)
}

func (fHandler *funcHandler) NotifyOwnershipChange(version string) {
	fHandler.commandChan <- command{
		command: vbChangeNotifier,
		version: version,
	}
}

func (fHandler *funcHandler) notifyOwnershipChange(version string) {
	logPrefix := fmt.Sprintf("functionHandler::notifyOwnershipChange[%s]", fHandler.logPrefix)
	if !fHandler.currState.isRunning() {
		logging.Infof("%s function not in running state: %v. Skipping...", logPrefix, fHandler.currState.isRunning())
		return
	}

	vbMapVersion, toOwn, toClose, notFullyOwned, err := fHandler.vbHandler.Load().NotifyOwnershipChange(version)
	if err != nil {
		// Possible that ownership info not yet receieved. Wait for second notifyownership change
		logging.Errorf("%s error allocating ownership %v", logPrefix, err)
		return
	}

	logging.Infof("%s vb distribution for version: %s notFully owned: %s vbs to own: %s vbs to giveup: %s",
		logPrefix, vbMapVersion, utils.Condense(notFullyOwned), utils.Condense(toOwn), utils.Condense(toClose))

	// notFullyOwned is basically this node needs to own this vb but not yet owned it.
	// c++ side doesn't know about this vb so we can simply close this vb
	for _, vb := range notFullyOwned {
		fHandler.checkpointManager.Load().StopCheckpoint(vb)
	}

	fHandler.vbMapVersion.Store(vbMapVersion)
	// everything is working fine. Possible that other services are getting rebalanced
	if len(toOwn) == 0 && len(toClose) == 0 {
		fHandler.notifyInterrupt()
		return
	}

	// Own this vbs
	for _, vb := range toOwn {
		fHandler.checkpointManager.Load().OwnVbCheckpoint(vb)
	}
}

func (fHandler *funcHandler) NotifyGlobalConfigChange() {
	fHandler.configSignal.Notify()
}

func (fHandler *funcHandler) notifyGlobalConfigChange() {
	if !fHandler.currState.isRunning() {
		return
	}

	fHandler.vbHandler.Load().RefreshSystemResourceLimits()
	_, config := fHandler.serverConfig.GetServerConfig(fHandler.fd.MetaInfo.FunctionScopeID)
	defer func() {
		fHandler.config = config
	}()

	newFeatureMatrix := config.FeatureList.GetFeatureMatrix()
	if fHandler.config == nil {
		fHandler.re.SendControlMessage(fHandler.version, processManager.GlobalConfigChange, processManager.FeatureMatrix, fHandler.instanceID, nil, newFeatureMatrix)
		fHandler.statsHandler.incrementCountProcessingStatsLocked("agg_messages_sent_to_worker", 1)
		return
	}

	oldFeatureMatrix := fHandler.config.GetFeatureMatrix()
	if newFeatureMatrix != oldFeatureMatrix {
		fHandler.re.SendControlMessage(fHandler.version, processManager.GlobalConfigChange, processManager.FeatureMatrix, fHandler.instanceID, nil, newFeatureMatrix)
		fHandler.statsHandler.incrementCountProcessingStatsLocked("agg_messages_sent_to_worker", 1)
	}
}

// Callbacks
func (fHandler *funcHandler) IsTrapEvent() (vbhandler.RuntimeSystem, bool) {
	if !fHandler.trapEvent.CompareAndSwap(startTrapEvent, leaderOfTrapEvent) {
		return nil, false
	}

	logPrefix := fmt.Sprintf("functionHandler::IsTrapEvent[%s]", fHandler.logPrefix)
	// Try to get the leadership
	leader, err := fHandler.checkpointManager.Load().TryTobeLeader(checkpointManager.DebuggerLeader, fHandler.fd.MetaInfo.Seq)
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

func (fHandler *funcHandler) ReceiveMessage(msg *processManager.ResponseMessage) {
	switch msg.Event {
	case processManager.VbSettings:
		vb := binary.BigEndian.Uint16(msg.Extras)
		seq := binary.BigEndian.Uint64(msg.Extras[2:])
		vbuuid := binary.BigEndian.Uint64(msg.Extras[10:])
		fHandler.checkpointManager.Load().UpdateVal(vb, checkpointManager.Checkpoint_SeqNum, []uint64{seq, vbuuid})
		fHandler.checkpointManager.Load().StopCheckpoint(vb)

	case processManager.StatsEvent:
		switch msg.Opcode {
		case processManager.ProcessedEvents:
			processedSeq := fHandler.statsHandler.processedSeqEvents(msg)
			for vb, vbinfo := range processedSeq {
				fHandler.checkpointManager.Load().UpdateVal(vb, checkpointManager.Checkpoint_SeqNum, vbinfo)
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
	fmt.Fprintf(fHandler.appLogWriter.Load(), "%s [INFO] %s\n", ts, log)
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
	seq, currState := fHandler.currState.doneState()

	switch currState {
	case running, Deployed:
		// No need to handle this just give the interrupt

	case pausing, Paused, undeploying, Undeployed:
		// Get it into basic state
		fHandler.close()
		appLogWriter := fHandler.appLogWriter.Swap(NewDummyLogWriter())
		appLogWriter.Close()
		vbHandler := fHandler.vbHandler.Swap(vbhandler.NewDummyVbHandler())
		vbHandler.Close()

		if currState == undeploying {
			fHandler.deleteAllCheckpoint()
		}

		checkpointManager := fHandler.checkpointManager.Swap(checkpointManager.NewDummyCheckpointManager())
		checkpointManager.CloseCheckpointManager()
		fHandler.vbMapVersion.Store("")
		fHandler.trapEvent.Store(noEvent)

	default:
		return
	}

	logging.Infof("%s state changed interrupt called: %s seq: %v", logPrefix, currState, seq)
	fHandler.interruptHandler(fHandler.id, seq, fHandler.fd.AppLocation, nil)
}

func (fHandler *funcHandler) CloseFunctionHandler() {
	fHandler.close()

	logPrefix := fmt.Sprintf("funcHandler::closeFunctionHandler[%s]", fHandler.logPrefix)
	vbHandler := fHandler.vbHandler.Swap(vbhandler.NewDummyVbHandler())
	vbHandler.Close()

	checkpointMgr := fHandler.checkpointManager.Swap(checkpointManager.NewDummyCheckpointManager())
	checkpointMgr.CloseCheckpointManager()

	close(fHandler.commandChan)
	logging.Infof("%s Function handler closed successfully", logPrefix)
}
