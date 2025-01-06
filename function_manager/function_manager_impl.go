package functionManager

import (
	"context"
	"fmt"
	"sync"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	processManager "github.com/couchbase/eventing/process_manager"

	eventPool "github.com/couchbase/eventing/event_pool"
	functionHandler "github.com/couchbase/eventing/function_manager/function_handler"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
	serverConfig "github.com/couchbase/eventing/server_config"
	"github.com/couchbase/gocb/v2"
)

const (
	serverlessProcessID = uint32(0)
)

type funcDetails struct {
	state             application.LifeCycleOp
	funcHandlerIDList []uint16
	vbsAllocated      map[uint16]uint16
	vbsVersion        string
	statusChanged     map[uint16]bool
	funcSetList       []uint32
	seq               uint32
}

func NewFuncDetails(id uint16) *funcDetails {
	fdetails := &funcDetails{
		funcHandlerIDList: make([]uint16, 0, 1),
		statusChanged:     make(map[uint16]bool),
		funcSetList:       make([]uint32, 0, 1),
		vbsAllocated:      make(map[uint16]uint16),
		seq:               0,
	}
	fdetails.state = application.Undeploy
	fdetails.addFuncDetails(id, serverlessProcessID)
	return fdetails
}

func (fdetails *funcDetails) initFuncDetails(seq uint32) {
	for id, _ := range fdetails.statusChanged {
		fdetails.statusChanged[id] = false
	}
	fdetails.seq = seq
}

// remove everything and returns first id
func (fdetails *funcDetails) reset() uint16 {
	fdetails.funcSetList = fdetails.funcSetList[:0]
	handlerID := fdetails.funcHandlerIDList[0]
	fdetails.funcHandlerIDList = fdetails.funcHandlerIDList[:0]
	fdetails.vbsAllocated = make(map[uint16]uint16)
	delete(fdetails.statusChanged, handlerID)
	return handlerID
}

func (fdetails *funcDetails) addFuncDetails(id uint16, setId uint32) {
	fdetails.statusChanged[id] = false
	fdetails.funcSetList = append(fdetails.funcSetList, setId)
	fdetails.funcHandlerIDList = append(fdetails.funcHandlerIDList, id)
}

func (fdetails *funcDetails) resetTo(id uint16, setID uint32) {
	fdetails.funcSetList = fdetails.funcSetList[:0]
	for _, funcID := range fdetails.funcHandlerIDList {
		delete(fdetails.statusChanged, funcID)
	}
	fdetails.funcHandlerIDList = fdetails.funcHandlerIDList[:0]
	fdetails.vbsAllocated = make(map[uint16]uint16)
	fdetails.addFuncDetails(id, setID)
}

func (fdetails *funcDetails) notifyStateChange(id uint16, seq uint32) bool {
	if fdetails.seq != seq {
		return false
	}

	fdetails.statusChanged[id] = true
	for _, done := range fdetails.statusChanged {
		if !done {
			return false
		}
	}
	return true
}

type functionManager struct {
	sync.RWMutex

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

	funcCache *funcCache

	incrementalFuncSetID uint32
	// function_set id -> function_set
	funcSet map[uint32]functionSet

	incrementalFuncHandlerCount uint16
	// instanceID -> functionDetails
	functionMapper map[string]*funcDetails

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
		id:                      id,
		interrupt:               interrupt,
		clusterSettings:         clusterSettings,
		observer:                observer,
		ownershipRoutine:        ownershipRoutine,
		serverConfig:            serverConfig,
		systemConfig:            systemConfig,
		cursorCheckpointHandler: cursorCheckpointHandler,
		broadcaster:             broadcaster,
		systemResourceDetails:   systemResourceDetails,
	}

	ctx, close := context.WithCancel(context.Background())
	fm.close = close

	// Initialise the common pool for checkpoint and dcp requests
	poolString := fmt.Sprintf("%s%s", clusterSettings.UUID, id)
	fm.pool = eventPool.NewManagerPool(ctx, poolString, clusterSettings, observer, cluster, broadcaster)

	// Initialise the function cache for name conversion and observation
	fm.funcCache = NewFunctionNameCache(ctx, id, observer, interrupt)

	// Initialise the init funcSet whose id will be 0. This is used for serverless features and all the
	// serverless functions are spawned in this function set
	fm.funcSet = make(map[uint32]functionSet)
	initFuncSetID := fmt.Sprintf("%s_%d", id, serverlessProcessID)
	fm.funcSet[serverlessProcessID] = NewFunctionSet(initFuncSetID, config{},
		clusterSettings, fm.appCallback, systemConfig)
	fm.incrementalFuncSetID = uint32(1)

	fm.functionMapper = make(map[string]*funcDetails)
	fm.incrementalFuncHandlerCount = uint16(0)
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
	fhList := fm.getFunctionHandlerListFromLocation(appLocation)
	for _, fh := range fhList {
		// GetRebalanceProgress should be first
		rebalanceProgress.RebalanceInProgress = (fh.GetRebalanceProgress(version, rebalanceProgress) || rebalanceProgress.RebalanceInProgress)
	}

	if len(rebalanceProgress.ToOwn) != 0 || len(rebalanceProgress.ToClose) != 0 {
		rebalanceProgress.RebalanceInProgress = true
	}
}

func (fm *functionManager) GetStats(appLocation application.AppLocation, statType common.StatsType) *common.Stats {
	fhList := fm.getFunctionHandlerListFromLocation(appLocation)
	stats := common.NewStats(true, appLocation.Namespace, appLocation.Appname, statType)
	for _, fh := range fhList {
		functionStats := fh.Stats(statType)
		stats.Add(functionStats, statType)
	}

	return stats
}

func (fm *functionManager) ResetStats(appLocation application.AppLocation) {
	fhList := fm.getFunctionHandlerListFromLocation(appLocation)
	for _, fh := range fhList {
		fh.ResetStats()
	}
}

func (fm *functionManager) GetInsight(appLocation application.AppLocation) *common.Insight {
	fhList := fm.getFunctionHandlerListFromLocation(appLocation)
	insight := common.NewInsight()
	for _, fh := range fhList {
		nInsight := fh.GetInsight()
		if nInsight == nil {
			continue
		}

		insight.Accumulate(nInsight)
	}
	return insight
}

// All app log is gone into first function handler so read it from it
func (fm *functionManager) GetApplicationLog(appLocation application.AppLocation, size int64) ([]string, error) {
	fh, ok := fm.getAggFunctionHandlerFromLocation(appLocation)
	if !ok {
		return nil, nil
	}

	return fh.GetApplicationLog(size)
}

func (fm *functionManager) RemoveFunction(funcDetails *application.FunctionDetails) int {
	fm.Lock()
	funcSet := fm.funcSet[serverlessProcessID]
	instanceID, count := fm.funcCache.DeleteFromFuncCache(funcDetails)
	fHandler, _ := funcSet.DeleteFunctionHandler(instanceID)
	fm.Unlock()
	fHandler.CloseFunctionHandler()

	return count
}

func (fm *functionManager) TrapEventOp(trapEvent functionHandler.TrapEventOp, appLocation application.AppLocation, value interface{}) error {
	fh, ok := fm.getAggFunctionHandlerFromLocation(appLocation)
	if !ok {
		return nil
	}
	return fh.TrapEvent(trapEvent, value)
}

// Make sure all the functions are deleted before calling this
// else there will be go routine leak
func (fm *functionManager) CloseFunctionManager() {
	logPrefix := fmt.Sprintf("functionManager::CloseFunctionManager[%s]", fm.id)
	fm.pool.ClosePool()

	fm.Lock()
	funcSet := fm.funcSet[serverlessProcessID]
	delete(fm.funcSet, serverlessProcessID)
	fm.close()
	fm.Unlock()

	funcSet.DeleteFunctionSet()
	logging.Infof("%s closed function manager", logPrefix)
}

func (fm *functionManager) lifeCycleOp(fd *application.FunctionDetails, nextState application.LifeCycleOp) {
	logPrefix := fmt.Sprintf("functionManager::lifeCycleOp[%s]", fd.AppLocation)
	fm.Lock()
	defer fm.Unlock()

	// Create the function first if not present it will be associated with function set 0
	oldInstanceID, instanceID, ok := fm.funcCache.AddToFuncCache(fd, nextState)
	logging.Infof("%s lifecycle change called -> %s, OldInstanceID: %s NewInstanceID: %s. Is function present? %v", logPrefix, nextState, oldInstanceID, instanceID, ok)
	if !ok {
		fm.incrementalFuncHandlerCount++
		config := functionHandler.Config{
			SpawnLogWriter: true,
			OnDeployLeader: true,
		}
		fHandler := functionHandler.NewFunctionHandler(fm.incrementalFuncHandlerCount, config, fd.AppLocation, fm.clusterSettings,
			fm.interruptForStateChange, fm.systemResourceDetails, fm.observer, fm, fm.pool, fm.serverConfig, fm.cursorCheckpointHandler, fm.utilityWorker, fm.broadcaster)
		fHandler.AddFunctionDetails(fd)

		fdetails := NewFuncDetails(fm.incrementalFuncHandlerCount)
		fm.funcSet[serverlessProcessID].AddFunctionHandler(instanceID, fHandler)
		fm.functionMapper[instanceID] = fdetails

	} else {
		if oldInstanceID != instanceID {
			// Move the functionDetails to new instanceID
			fm.functionMapper[instanceID] = fm.functionMapper[oldInstanceID]
			delete(fm.functionMapper, oldInstanceID)
		}
	}

	fdetails := fm.functionMapper[instanceID]
	fdetails.initFuncDetails(fd.MetaInfo.Seq)

	switch fdetails.state {
	case application.Undeploy, application.Pause:
		switch nextState {
		case application.Deploy:
			fm.deployFunctionLocked(fd, oldInstanceID, fdetails)
		}

	case application.Deploy:
		// Already in deployed state. Change the state of the function
	}

	fdetails.state = nextState
	for _, funcSetID := range fdetails.funcSetList {
		funcSet := fm.funcSet[funcSetID]
		funcSet.ChangeState(oldInstanceID, fd, nextState)
	}
}

func (fm *functionManager) deployFunctionLocked(fd *application.FunctionDetails, instanceID string, fdetails *funcDetails) {
	logPrefix := fmt.Sprintf("functionManager::deployFunctionLocked[%s]", fm.id)
	_, namespaceConfig := fm.serverConfig.GetServerConfig(fd.MetaInfo.FunctionScopeID)

	logging.Infof("%s deploying function %s in %s mode", logPrefix, fd.AppLocation, namespaceConfig.DeploymentMode)
	switch namespaceConfig.DeploymentMode {
	case serverConfig.FunctionGroup:
		// function is in correct functionSet so no need to do anything
		fd.Settings.CppWorkerThread = fd.Settings.CppWorkerThread * fd.Settings.WorkerCount
		return

	case serverConfig.IsolateFunction, serverConfig.HybridMode:
		// Get the already created fHandler
		fHandler, _ := fm.funcSet[serverlessProcessID].DeleteFunctionHandler(instanceID)
		funcID := fdetails.reset()

		// Create worker count number of function set
		workerCount := fd.Settings.WorkerCount
		for i := uint32(0); i < workerCount; i++ {
			// Already we have 1 function handler
			if i != uint32(0) {
				fm.incrementalFuncHandlerCount++
				config := functionHandler.Config{
					SpawnLogWriter: false,
					OnDeployLeader: false,
				}
				fHandler = functionHandler.NewFunctionHandler(fm.incrementalFuncHandlerCount, config, fd.AppLocation, fm.clusterSettings,
					fm.interruptForStateChange, fm.systemResourceDetails, fm.observer, fm, fm.pool, fm.serverConfig, fm.cursorCheckpointHandler, fm.utilityWorker, fm.broadcaster)
				funcID = fm.incrementalFuncHandlerCount
			}

			fHandler.AddFunctionDetails(fd)
			initFuncSetID := fmt.Sprintf("%s_%d", fm.id, fm.incrementalFuncSetID)
			funcSet := NewFunctionSet(initFuncSetID, config{spawnImmediately: true},
				fm.clusterSettings, fm.appCallback, fm.systemConfig)
			// Add it with old instance id since function is in undeploy state and ChangeState will change the function to new instanceID
			funcSet.AddFunctionHandler(instanceID, fHandler)
			fm.funcSet[fm.incrementalFuncSetID] = funcSet
			fdetails.addFuncDetails(funcID, fm.incrementalFuncSetID)
			fm.incrementalFuncSetID++
		}
	}
}

// Helper functions to get the info
func (fm *functionManager) getAllFunctionSet() []functionSet {
	fm.RLock()
	defer fm.RUnlock()

	fSetList := make([]functionSet, 0, len(fm.funcSet))
	for _, funcSet := range fm.funcSet {
		fSetList = append(fSetList, funcSet)
	}

	return fSetList
}

func (fm *functionManager) getFunctionHandlerListFromLocation(appLocation application.AppLocation) []functionHandler.FunctionHandler {
	fm.RLock()
	defer fm.RUnlock()

	instanceID, ok := fm.funcCache.GetInstanceID(appLocation)
	if !ok {
		return nil
	}

	return fm.getFunctionHandlerListFromInstanceIDLocked(instanceID)
}

func (fm *functionManager) getFunctionHandlerListFromInstanceID(instanceID string) []functionHandler.FunctionHandler {
	fm.RLock()
	defer fm.RUnlock()

	return fm.getFunctionHandlerListFromInstanceIDLocked(instanceID)
}

func (fm *functionManager) getFunctionHandlerListFromInstanceIDLocked(instanceID string) []functionHandler.FunctionHandler {
	funcDetails := fm.functionMapper[instanceID]
	funcSetList := funcDetails.funcSetList

	funcHandlerList := make([]functionHandler.FunctionHandler, 0, len(funcSetList))
	for _, fIndex := range funcSetList {
		fSet := fm.funcSet[fIndex]
		funcHandler, ok := fSet.GetFunctionHandler(instanceID)
		if !ok {
			continue
		}
		funcHandlerList = append(funcHandlerList, funcHandler)
	}
	return funcHandlerList
}

func (fm *functionManager) getAggFunctionHandlerFromLocation(appLocation application.AppLocation) (functionHandler.FunctionHandler, bool) {
	fm.RLock()
	defer fm.RUnlock()

	instanceID, ok := fm.funcCache.GetInstanceID(appLocation)
	if !ok {
		return nil, false
	}

	return fm.getFunctionHandlerInstanceIDLocked(instanceID)
}

func (fm *functionManager) getAggFunctionHandlerFromInstanceID(instanceID string) (functionHandler.FunctionHandler, bool) {
	fm.RLock()
	defer fm.RUnlock()

	return fm.getFunctionHandlerInstanceIDLocked(instanceID)
}

func (fm *functionManager) getFunctionHandlerInstanceIDLocked(instanceID string) (functionHandler.FunctionHandler, bool) {
	funcDetails := fm.functionMapper[instanceID]
	if len(funcDetails.funcSetList) == 0 {
		return nil, false
	}

	funcSetID := funcDetails.funcSetList[0]
	funcSet := fm.funcSet[funcSetID]
	return funcSet.GetFunctionHandler(instanceID)
}

// Callbacks
func (fm *functionManager) appCallback(handlerID string, msg string) {
	fh, ok := fm.getAggFunctionHandlerFromInstanceID(handlerID)
	if ok {
		fh.ApplicationLog(msg)
	}
}

func (fm *functionManager) interruptForStateChange(id uint16, seq uint32, appLocation application.AppLocation, err error) {
	logPrefix := fmt.Sprintf("functionManager::interruptForStateChange[%s]", fm.id)

	fm.Lock()
	defer fm.Unlock()

	instanceID, _ := fm.funcCache.GetInstanceID(appLocation)
	funcDetails := fm.functionMapper[instanceID]
	done := funcDetails.notifyStateChange(id, seq)
	if !done {
		return
	}

	lifecycleChange := funcDetails.state
	// TODO: Change this to appropriate previous state, although should not affect functionality
	if err == common.ErrOnDeployFail {
		lifecycleChange = application.Undeploy
	}

	// If lifecycle is undeploy or pause then move it to functionSet serverlessProcessID
	switch lifecycleChange {
	case application.Undeploy, application.Pause:
		fm.pool.CloseConditional()

		funcHandlerList := funcDetails.funcHandlerIDList
		funcSetList := funcDetails.funcSetList

		if funcSetList[0] != serverlessProcessID {
			var fHandler functionHandler.FunctionHandler
			var fHandlerID uint16
			// This is the onprem mode. Close all the functionSet and move the first one to serverlessProcessID
			for index, funcSetIndex := range funcSetList {
				funcSet := fm.funcSet[funcSetIndex]
				fHandlerTemp, _ := funcSet.DeleteFunctionHandler(instanceID)
				fHandlerIDTemp := funcHandlerList[index]
				if index == 0 {
					// This handler will have essential config
					// Maybe we need to pass the config during ChangeState
					fHandler = fHandlerTemp
					fHandlerID = fHandlerIDTemp
				} else {
					fHandlerTemp.CloseFunctionHandler()
				}
				funcSet.DeleteFunctionSet()
				delete(fm.funcSet, funcSetIndex)
			}
			fm.funcSet[serverlessProcessID].AddFunctionHandler(instanceID, fHandler)
			funcDetails.resetTo(fHandlerID, serverlessProcessID)
		} else {
			// Serverless mode already in correct state
		}

	case application.Deploy:
		// No need to take any action
	}

	if err != nil {
		logging.Infof("%s state change failing for %s seq: %d", logPrefix, appLocation, seq)
		msg := common.LifecycleMsg{
			InstanceID:  instanceID,
			Applocation: appLocation,
		}
		// Calls supervisor's FailStateInterrupt to restore previous state in the app state machine
		fm.interrupt.FailStateInterrupt(seq, appLocation, msg)
		return
	}

	logging.Infof("%s state change done for %s seq: %d. state: %s", logPrefix, appLocation, seq, funcDetails.state)
	fm.interrupt.StateChangeInterupt(seq, appLocation)
}

func (fm *functionManager) GetVbMap(keyspaceInfo *application.KeyspaceInfo, id uint16, numVb uint16, appLocation application.AppLocation) (string, []uint16, error) {
	vbMapVersion, vbs, err := fm.ownershipRoutine.GetVbMap(keyspaceInfo, numVb)
	if err != nil {
		return "", nil, err
	}

	fm.Lock()
	defer fm.Unlock()
	instanceID, ok := fm.funcCache.GetInstanceID(appLocation)
	if !ok {
		return vbMapVersion, vbs, nil
	}

	funcDetails := fm.functionMapper[instanceID]
	funHandlerIDs := funcDetails.funcHandlerIDList
	handlerPosition := 0
	for index, hId := range funcDetails.funcHandlerIDList {
		if hId == id {
			handlerPosition = index
			break
		}
	}
	if funcDetails.vbsVersion != vbMapVersion {
		for vb, _ := range funcDetails.vbsAllocated {
			found := false
			for _, newVb := range vbs {
				if newVb == vb {
					found = true
					break
				}
			}
			if !found {
				delete(funcDetails.vbsAllocated, vb)
			}
		}
		funcDetails.vbsVersion = vbMapVersion
	}

	numOwningvbs := len(vbs) / len(funHandlerIDs)
	remainingVbs := len(vbs) % len(funHandlerIDs)
	if handlerPosition < remainingVbs {
		numOwningvbs++
	}
	ownedVbs := make([]uint16, 0, numOwningvbs)
	for _, vb := range vbs {
		if funcDetails.vbsAllocated[vb] == id {
			numOwningvbs--
			ownedVbs = append(ownedVbs, vb)
		}
	}
	// all the old remainging vbs are allocated
	if numOwningvbs != 0 {
		for index := handlerPosition; index < len(vbs); index = index + len(funHandlerIDs) {
			vb := vbs[index]
			if _, ok := funcDetails.vbsAllocated[vb]; !ok {
				funcDetails.vbsAllocated[vb] = id
				numOwningvbs--
				ownedVbs = append(ownedVbs, vb)
			}
			if numOwningvbs == 0 {
				break
			}
		}
	}

	if numOwningvbs != 0 {
		for _, vb := range vbs {
			if _, ok := funcDetails.vbsAllocated[vb]; !ok {
				funcDetails.vbsAllocated[vb] = id
				numOwningvbs--
				ownedVbs = append(ownedVbs, vb)
			}
			if numOwningvbs == 0 {
				break
			}
		}
	}

	return vbMapVersion, ownedVbs, nil
}
