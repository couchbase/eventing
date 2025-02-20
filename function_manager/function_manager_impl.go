package functionManager

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	processManager "github.com/couchbase/eventing/process_manager"

	eventPool "github.com/couchbase/eventing/event_pool"
	functionHandler "github.com/couchbase/eventing/function_manager/function_handler"
	vbDistribution "github.com/couchbase/eventing/function_manager/function_handler/vb_handler"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
	serverConfig "github.com/couchbase/eventing/server_config"
	"github.com/couchbase/gocb/v2"
)

const (
	serverlessProcessID = uint32(0)
)

type funcRuntimeDetails struct {
	sync.RWMutex

	state             application.LifeCycleOp
	funcHandlerIDList []uint16
	vbsAllocated      map[uint16]uint16
	vbsVersion        string
	statusChanged     map[uint16]bool
	funcSetList       []functionSet
	seq               uint32
}

func NewFuncRuntimeDetails(id uint16, funcSet functionSet) *funcRuntimeDetails {
	fdetails := &funcRuntimeDetails{
		funcHandlerIDList: make([]uint16, 0, 1),
		statusChanged:     make(map[uint16]bool),
		funcSetList:       make([]functionSet, 0, 1),
		vbsAllocated:      make(map[uint16]uint16),
		seq:               0,
	}
	fdetails.state = application.Undeploy
	fdetails.addFuncDetails(id, funcSet)
	return fdetails
}

func (fdetails *funcRuntimeDetails) initFuncDetails(seq uint32) {
	fdetails.Lock()
	defer fdetails.Unlock()

	for id := range fdetails.statusChanged {
		fdetails.statusChanged[id] = false
	}
	fdetails.seq = seq
}

// remove everything and returns first id
func (fdetails *funcRuntimeDetails) reset() uint16 {
	fdetails.Lock()
	defer fdetails.Unlock()

	fdetails.funcSetList = fdetails.funcSetList[:0]
	handlerID := fdetails.funcHandlerIDList[0]
	fdetails.funcHandlerIDList = fdetails.funcHandlerIDList[:0]
	fdetails.vbsAllocated = make(map[uint16]uint16)
	delete(fdetails.statusChanged, handlerID)
	return handlerID
}

func (fdetails *funcRuntimeDetails) addFuncDetails(id uint16, functionSet functionSet) {
	fdetails.Lock()
	defer fdetails.Unlock()
	fdetails.addFuncDetailsLocked(id, functionSet)
}

func (fdetails *funcRuntimeDetails) addFuncDetailsLocked(id uint16, functionSet functionSet) {
	fdetails.statusChanged[id] = false
	fdetails.funcSetList = append(fdetails.funcSetList, functionSet)
	fdetails.funcHandlerIDList = append(fdetails.funcHandlerIDList, id)
}

func (fdetails *funcRuntimeDetails) resetTo(id uint16, functionSet functionSet) {
	fdetails.Lock()
	defer fdetails.Unlock()

	fdetails.funcSetList = fdetails.funcSetList[:0]
	for _, funcID := range fdetails.funcHandlerIDList {
		delete(fdetails.statusChanged, funcID)
	}
	fdetails.funcHandlerIDList = fdetails.funcHandlerIDList[:0]
	fdetails.vbsAllocated = make(map[uint16]uint16)
	fdetails.addFuncDetailsLocked(id, functionSet)
}

func (fdetails *funcRuntimeDetails) notifyStateChange(id uint16, seq uint32) bool {
	fdetails.Lock()
	defer fdetails.Unlock()

	if fdetails.seq != seq {
		return false
	}

	notified, ok := fdetails.statusChanged[id]
	if !ok {
		return false
	}

	fdetails.statusChanged[id] = true
	for _, done := range fdetails.statusChanged {
		if !done {
			return false
		}
	}
	return !notified
}

func (fdetails *funcRuntimeDetails) getFunctionHandlerList() []functionSet {
	fdetails.RLock()
	defer fdetails.RUnlock()

	funcSetList := fdetails.funcSetList
	funcHandlerList := make([]functionSet, len(funcSetList))
	copy(funcHandlerList, funcSetList)
	return funcHandlerList
}

func (fdetails *funcRuntimeDetails) getLeaderFunctionHandler() (functionSet, bool) {
	fdetails.RLock()
	defer fdetails.RUnlock()

	if len(fdetails.funcSetList) == 0 {
		return nil, false
	}

	return fdetails.funcSetList[0], true
}

func (fdetails *funcRuntimeDetails) getFunctionSet() []functionSet {
	fdetails.RLock()
	defer fdetails.RUnlock()

	fSet := make([]functionSet, len(fdetails.funcSetList))
	if len(fdetails.funcSetList) == 0 {
		return fSet
	}

	copy(fSet, fdetails.funcSetList)
	return fSet
}

type functionManager struct {
	id                      string
	clusterSettings         *common.ClusterSettings
	observer                notifier.Observer
	interrupt               InterruptHandler
	serverConfig            serverConfig.ServerConfig
	systemConfig            serverConfig.SystemConfig
	ownershipRoutine        common.OwnershipRoutine
	cursorCheckpointHandler functionHandler.CursorCheckpointHandler
	utilityWorker           functionHandler.UtilityWorker
	broadcaster             common.Broadcaster
	systemResourceDetails   SystemResourceDetails

	pool eventPool.ManagerPool

	funcCache             funcCache
	serverlessFunctionSet functionSet

	// Accessed by sequential process so no need to protect these
	incrementalFuncSetID        uint32
	incrementalFuncHandlerCount uint16

	close func()
}

func NewFunctionManager(id string, cluster *gocb.Cluster, clusterSettings *common.ClusterSettings,
	observer notifier.Observer, interrupt InterruptHandler,
	systemResourceDetails SystemResourceDetails,
	ownershipRoutine common.OwnershipRoutine,
	serverConfig serverConfig.ServerConfig,
	systemConfig serverConfig.SystemConfig,
	cursorCheckpointHandler functionHandler.CursorCheckpointHandler,
	broadcaster common.Broadcaster) *functionManager {

	fm := &functionManager{
		id:                          id,
		interrupt:                   interrupt,
		clusterSettings:             clusterSettings,
		observer:                    observer,
		ownershipRoutine:            ownershipRoutine,
		serverConfig:                serverConfig,
		systemConfig:                systemConfig,
		cursorCheckpointHandler:     cursorCheckpointHandler,
		broadcaster:                 broadcaster,
		systemResourceDetails:       systemResourceDetails,
		incrementalFuncSetID:        1,
		incrementalFuncHandlerCount: 0,
	}

	ctx, close := context.WithCancel(context.Background())
	fm.close = close

	// Initialise the common pool for checkpoint and dcp requests
	poolString := fmt.Sprintf("%s%s", clusterSettings.UUID, id)
	fm.pool = eventPool.NewManagerPool(ctx, poolString, clusterSettings, observer, cluster, broadcaster)

	// Initialise the function cache for name conversion and observation
	fm.funcCache = NewFunctionNameCache(ctx, id, observer, interrupt)

	// Initialise the init funcSet whose id will be 0. This is used for serverless features and all the
	// serverless functions are spawned in this function set. Also it will hold functions which are not deployed
	initFuncSetID := fmt.Sprintf("%s_%d", id, serverlessProcessID)
	fm.serverlessFunctionSet = NewFunctionSet("", GroupOfFunctions, initFuncSetID, config{},
		clusterSettings, fm.appCallback, systemConfig)

	fm.utilityWorker = processManager.NewUtilityWorker(fm.clusterSettings, fm.id, fm.systemConfig)

	return fm
}

func (fm *functionManager) DeployFunction(fd *application.FunctionDetails) {
	fm.lifeCycleOp(fd, application.Deploy)
}

func (fm *functionManager) StopFunction(fd *application.FunctionDetails) {
	fm.lifeCycleOp(fd, application.Undeploy)
}

func (fm *functionManager) PauseFunction(fd *application.FunctionDetails) {
	fm.lifeCycleOp(fd, application.Pause)
}

// Notify all the ownership changes to all the function set
func (fm *functionManager) NotifyOwnershipChange() {
	fSetList := fm.getAllFunctionSet()
	for _, funcSet := range fSetList {
		funcSet.NotifyOwnershipChange()
	}
}

func (fm *functionManager) NotifyGlobalConfigChange() {
	fSetList := fm.getAllFunctionSet()
	for _, funcSet := range fSetList {
		funcSet.NotifyGlobalConfigChange()
	}
}

func (fm *functionManager) NotifyTlsChanges(cluster *gocb.Cluster) {
	fm.pool.TlsSettingsChanged(cluster)
}

func (fm *functionManager) RebalanceProgress(version string, appLocation application.AppLocation, rebalanceProgress *common.AppRebalanceProgress) {
	instanceID, fhList := fm.getFunctionHandlerListFromLocation(appLocation)
	for _, fh := range fhList {
		// GetRebalanceProgress should be first so that we get the correct progress from all workers
		rebalanceProgress.RebalanceInProgress = (fh.GetRebalanceProgress(instanceID, version, rebalanceProgress) || rebalanceProgress.RebalanceInProgress)
	}

	if len(rebalanceProgress.ToOwn) != 0 || len(rebalanceProgress.ToClose) != 0 {
		rebalanceProgress.RebalanceInProgress = true
	}
}

func (fm *functionManager) GetStats(appLocation application.AppLocation, statType common.StatsType) *common.Stats {
	instanceID, fhList := fm.getFunctionHandlerListFromLocation(appLocation)
	stats := common.NewStats(true, appLocation.Namespace, appLocation.Appname, statType)
	for _, fh := range fhList {
		functionStats := fh.Stats(instanceID, statType)
		stats.Add(functionStats, statType)
	}

	return stats
}

func (fm *functionManager) ResetStats(appLocation application.AppLocation) {
	instanceID, fhList := fm.getFunctionHandlerListFromLocation(appLocation)
	for _, fh := range fhList {
		fh.ResetStats(instanceID)
	}
}

func (fm *functionManager) GetInsight(appLocation application.AppLocation) *common.Insight {
	instanceID, fhList := fm.getFunctionHandlerListFromLocation(appLocation)
	insight := common.NewInsight()
	for _, fh := range fhList {
		nInsight := fh.GetInsight(instanceID)
		if nInsight == nil {
			continue
		}

		insight.Accumulate(nInsight)
	}
	return insight
}

// All app log is gone into first function handler so read it from it
func (fm *functionManager) GetApplicationLog(appLocation application.AppLocation, size int64) ([]string, error) {
	instanceID, fh, ok := fm.getAggFunctionHandlerFromLocation(appLocation)
	if !ok {
		return nil, nil
	}

	return fh.GetApplicationLog(instanceID, size)
}

func (fm *functionManager) RemoveFunction(funcDetails *application.FunctionDetails) int {
	instanceID, count := fm.funcCache.DeleteFromFuncCache(funcDetails)
	fHandler, _ := fm.serverlessFunctionSet.DeleteFunctionHandler(instanceID)
	fHandler.CloseFunctionHandler()

	return count
}

func (fm *functionManager) TrapEventOp(trapEvent functionHandler.TrapEventOp, appLocation application.AppLocation, value interface{}) error {
	instanceID, fh, ok := fm.getAggFunctionHandlerFromLocation(appLocation)
	if !ok {
		return nil
	}
	return fh.TrapEvent(instanceID, trapEvent, value)
}

// Make sure all the functions are deleted before calling this
// else there will be go routine leak
func (fm *functionManager) CloseFunctionManager() {
	logPrefix := fmt.Sprintf("functionManager::CloseFunctionManager[%s]", fm.id)
	fm.pool.ClosePool()
	fm.close()
	fm.serverlessFunctionSet.DeleteFunctionSet()
	fm.funcCache.CloseFuncCache()
	logging.Infof("%s closed function manager", logPrefix)
}

func (fm *functionManager) lifeCycleOp(fd *application.FunctionDetails, nextState application.LifeCycleOp) {
	// Create the function first if not present it will be associated with function set 0
	instanceID, fRuntimeDetails, ok := fm.funcCache.GetFunctionRuntimeDetails(fd.AppLocation)
	if !ok {
		fm.incrementalFuncHandlerCount++
		config := functionHandler.Config{
			LeaderHandler: true,
		}
		fHandler := functionHandler.NewFunctionHandler(fm.incrementalFuncHandlerCount, fd, config, fd.AppLocation, fm.clusterSettings,
			fm.interruptForStateChange, fm.systemResourceDetails, fm.observer, fm, fm.pool, fm.serverConfig, fm.cursorCheckpointHandler, fm.utilityWorker, fm.broadcaster)

		fRuntimeDetails = NewFuncRuntimeDetails(fm.incrementalFuncHandlerCount, fm.serverlessFunctionSet)
		fm.serverlessFunctionSet.AddFunctionHandler(fd.AppInstanceID, fHandler)
		instanceID = fd.AppInstanceID
	}
	fm.funcCache.AddToFuncCache(fd, fRuntimeDetails, nextState)
	fRuntimeDetails.initFuncDetails(fd.MetaInfo.Seq)

	switch fRuntimeDetails.state {
	case application.Undeploy, application.Pause:
		switch nextState {
		case application.Deploy:
			fm.deployFunctionLocked(fd, instanceID, fRuntimeDetails)
		}

	case application.Deploy:
		// Already in deployed state. Change the state of the function
	}

	fRuntimeDetails.state = nextState
	for _, funcSet := range fRuntimeDetails.funcSetList {
		funcSet.ChangeState(instanceID, fd, nextState)
	}
}

func (fm *functionManager) deployFunctionLocked(fd *application.FunctionDetails, instanceID string, fdetails *funcRuntimeDetails) {
	logPrefix := fmt.Sprintf("functionManager::deployFunctionLocked[%s]", fm.id)
	_, namespaceConfig := fm.serverConfig.GetServerConfig(fd.MetaInfo.FunctionScopeID)

	logging.Infof("%s deploying function %s in %s mode function: %s", logPrefix, fd.AppLocation, namespaceConfig.DeploymentMode, logging.TagUD(fd.String()))
	switch namespaceConfig.DeploymentMode {
	case serverConfig.FunctionGroup:
		// function is in correct functionSet so no need to do anything
		fd.Settings.CppWorkerThread = fd.Settings.CppWorkerThread * fd.Settings.WorkerCount
		return

	case serverConfig.IsolateFunction, serverConfig.HybridMode:
		// Get the already created fHandler
		fHandler, _ := fm.serverlessFunctionSet.DeleteFunctionHandler(instanceID)
		funcID := fdetails.reset()

		// Create worker count number of function set
		workerCount := fd.Settings.WorkerCount
		for i := uint32(0); i < workerCount; i++ {
			// Already we have 1 function handler
			if i != uint32(0) {
				fm.incrementalFuncHandlerCount++
				config := functionHandler.Config{
					LeaderHandler: false,
				}
				fHandler = functionHandler.NewFunctionHandler(fm.incrementalFuncHandlerCount, fd, config, fd.AppLocation, fm.clusterSettings,
					fm.interruptForStateChange, fm.systemResourceDetails, fm.observer, fm, fm.pool, fm.serverConfig, fm.cursorCheckpointHandler, fm.utilityWorker, fm.broadcaster)
				funcID = fm.incrementalFuncHandlerCount
			}

			initFuncSetID := fmt.Sprintf("%s_%d", fm.id, fm.incrementalFuncSetID)
			funcSet := NewFunctionSet(instanceID, SingleFunction, initFuncSetID, config{spawnImmediately: true},
				fm.clusterSettings, fm.appCallback, fm.systemConfig)
			// Add it with old instance id since function is in undeploy state and ChangeState will change the function to new instanceID
			funcSet.AddFunctionHandler(instanceID, fHandler)
			fdetails.addFuncDetails(funcID, funcSet)
			fm.incrementalFuncSetID++
		}
	}
}

// Helper functions to get the info
func (fm *functionManager) getAllFunctionSet() []functionSet {
	runtimeDetails := fm.funcCache.GetAllFunctionRuntimeDetails()
	fSetList := make([]functionSet, 0, len(runtimeDetails))
	for _, runtimeDetail := range runtimeDetails {
		fSetList = append(fSetList, runtimeDetail.getFunctionSet()...)
	}

	return fSetList
}

func (fm *functionManager) getFunctionHandlerListFromLocation(appLocation application.AppLocation) (string, []functionSet) {
	instanceID, funcDetails, ok := fm.funcCache.GetFunctionRuntimeDetails(appLocation)
	if !ok {
		return instanceID, nil
	}

	return instanceID, funcDetails.getFunctionHandlerList()
}

func (fm *functionManager) getAggFunctionHandlerFromLocation(appLocation application.AppLocation) (string, functionSet, bool) {
	instanceID, funcRuntimeDetails, ok := fm.funcCache.GetFunctionRuntimeDetails(appLocation)
	if !ok {
		return instanceID, nil, false
	}

	funcSet, ok := funcRuntimeDetails.getLeaderFunctionHandler()
	return instanceID, funcSet, ok
}

func (fm *functionManager) getAggFunctionHandlerFromInstanceID(instanceID string) (functionSet, bool) {
	runtimeDetails, ok := fm.funcCache.GetFunctionRuntimeDetailsFromInstanceID(instanceID)
	if !ok {
		return nil, false
	}

	return runtimeDetails.getLeaderFunctionHandler()
}

// Callbacks
func (fm *functionManager) appCallback(handlerID string, msg string) {
	fh, ok := fm.getAggFunctionHandlerFromInstanceID(handlerID)
	if ok {
		fh.ApplicationLog(handlerID, msg)
	}
}

func (fm *functionManager) interruptForStateChange(id uint16, seq uint32, appLocation application.AppLocation, err error) {
	logPrefix := fmt.Sprintf("functionManager::interruptForStateChange[%s]", fm.id)

	instanceID, runtimeDetails, ok := fm.funcCache.GetFunctionRuntimeDetails(appLocation)
	if !ok {
		return
	}
	done := runtimeDetails.notifyStateChange(id, seq)
	if !done {
		return
	}

	lifecycleChange := runtimeDetails.state
	// TODO: Change this to appropriate previous state, although should not affect functionality
	if errors.Is(err, common.ErrOnDeployFail) {
		lifecycleChange = application.Undeploy
	}

	// If lifecycle is undeploy or pause then move it to functionSet serverlessProcessID
	switch lifecycleChange {
	case application.Undeploy, application.Pause:
		fm.pool.CloseConditional()

		funcHandlerList := runtimeDetails.funcHandlerIDList
		funcSet := runtimeDetails.funcSetList[0]

		if funcSet.GetID() != GroupOfFunctions {

			// Get the first func set and put it into serverless mode

			funcSet := runtimeDetails.funcSetList[0]
			funcSet.DeleteFunctionSet()
			fHandler, _ := funcSet.DeleteFunctionHandler(instanceID)
			fHandlerID := funcHandlerList[0]
			fm.serverlessFunctionSet.AddFunctionHandler(instanceID, fHandler)

			for index := 1; index < len(runtimeDetails.funcSetList); index++ {
				funcSet = runtimeDetails.funcSetList[index]
				fHandler, _ = funcSet.DeleteFunctionHandler(instanceID)
				fHandler.CloseFunctionHandler()
				funcSet.DeleteFunctionSet()
			}
			runtimeDetails.resetTo(fHandlerID, fm.serverlessFunctionSet)
		}

	case application.Deploy:
		// No need to take any action
	}

	if err != nil {
		logging.Infof("%s state change failing for %s seq: %d", logPrefix, appLocation, seq)
		msg := common.LifecycleMsg{
			InstanceID:  instanceID,
			Applocation: appLocation,
			Revert:      true,
			Description: err.Error(),
		}
		// Calls supervisor's StopCalledInterupt to restore previous state in the app state machine
		fm.interrupt.StopCalledInterupt(seq, msg)
		return
	}

	logging.Infof("%s state change done for %s seq: %d. state: %s", logPrefix, appLocation, seq, runtimeDetails.state)
	fm.interrupt.StateChangeInterupt(seq, appLocation)
}

func (fm *functionManager) GetVbMap(keyspaceInfo *application.KeyspaceInfo, id uint16, numVb, timerPartitions uint16, appLocation application.AppLocation) (string, []uint16, error) {
	// This is called sequentially so no need to store latest vbMapVersion for a single funcHandler.
	// Guranteed to have latest one once notifyOwnership is called
	vbMapVersion, vbs, err := fm.ownershipRoutine.GetVbMap(keyspaceInfo, numVb)
	if err != nil {
		return "", nil, err
	}

	_, runtimeDetails, ok := fm.funcCache.GetFunctionRuntimeDetails(appLocation)
	if !ok {
		return vbMapVersion, vbs, nil
	}

	runtimeDetails.Lock()
	defer runtimeDetails.Unlock()

	funHandlerIDs := runtimeDetails.funcHandlerIDList
	handlerPosition := 0
	for index, hId := range runtimeDetails.funcHandlerIDList {
		if hId == id {
			handlerPosition = index
			break
		}
	}
	if runtimeDetails.vbsVersion != vbMapVersion {
		for vb, _ := range runtimeDetails.vbsAllocated {
			found := false
			for _, newVb := range vbs {
				if newVb == vb {
					found = true
					break
				}
			}
			if !found {
				delete(runtimeDetails.vbsAllocated, vb)
			}
		}
		runtimeDetails.vbsVersion = vbMapVersion
	}

	numVBsToOwn := len(vbs) / len(funHandlerIDs)
	remainingVbs := len(vbs) % len(funHandlerIDs)
	if handlerPosition < remainingVbs {
		numVBsToOwn++
	}

	ownedVbs := make([]uint16, 0, numVBsToOwn)
	for _, vb := range vbs {
		if runtimeDetails.vbsAllocated[vb] == id {
			numVBsToOwn--
			ownedVbs = append(ownedVbs, vb)
		}
	}

	timerVbs, nonTimerVbs := vbDistribution.GetTimerPartitionsInVbs(vbs, numVb, timerPartitions)
	perHandlerTimerVbs := len(timerVbs) / len(funHandlerIDs)
	if handlerPosition < len(timerVbs)%len(funHandlerIDs) {
		perHandlerTimerVbs++
	}

	for index := handlerPosition; index < len(timerVbs) && perHandlerTimerVbs != 0; index = index + len(funHandlerIDs) {
		vb := timerVbs[index]
		if owner, ok := runtimeDetails.vbsAllocated[vb]; !ok || owner == id {
			perHandlerTimerVbs--
			numVBsToOwn--
			if !ok {
				runtimeDetails.vbsAllocated[vb] = id
				ownedVbs = append(ownedVbs, vb)
			}
		}
	}

	for index := 0; index < len(timerVbs) && perHandlerTimerVbs != 0; index++ {
		vb := timerVbs[index]
		if owner, ok := runtimeDetails.vbsAllocated[vb]; !ok || owner == id {
			perHandlerTimerVbs--
			numVBsToOwn--
			if !ok {
				runtimeDetails.vbsAllocated[vb] = id
				ownedVbs = append(ownedVbs, vb)
			}
		}
	}

	// all the old remainging vbs are allocated
	for index := handlerPosition; index < len(nonTimerVbs) && numVBsToOwn != 0; index = index + len(funHandlerIDs) {
		vb := nonTimerVbs[index]
		if _, ok := runtimeDetails.vbsAllocated[vb]; !ok {
			runtimeDetails.vbsAllocated[vb] = id
			numVBsToOwn--
			ownedVbs = append(ownedVbs, vb)
		}

	}

	for index := 0; index < len(nonTimerVbs) && numVBsToOwn != 0; index++ {
		vb := nonTimerVbs[index]
		if _, ok := runtimeDetails.vbsAllocated[vb]; !ok {
			runtimeDetails.vbsAllocated[vb] = id
			numVBsToOwn--
			ownedVbs = append(ownedVbs, vb)
		}
	}

	return vbMapVersion, ownedVbs, nil
}
