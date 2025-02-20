package functionManager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	functionHandler "github.com/couchbase/eventing/function_manager/function_handler"
	"github.com/couchbase/eventing/logging"
	processManager "github.com/couchbase/eventing/process_manager"
	serverConfig "github.com/couchbase/eventing/server_config"
)

type funcSetType uint8

const (
	// FunctionSetType is the type of function set
	GroupOfFunctions funcSetType = iota
	SingleFunction
)

// This manages multiple fuction in one single executor
type functionSet interface {
	GetID() funcSetType

	AddFunctionHandler(instanceID string, fhHandler functionHandler.FunctionHandler)
	DeleteFunctionHandler(instanceID string) (functionHandler.FunctionHandler, int)

	ChangeState(instanceID string, funcDetails *application.FunctionDetails, nextState application.LifeCycleOp) (application.LifeCycleOp, bool)

	Stats(instanceID string, statsType common.StatsType) *common.Stats
	ApplicationLog(instanceID, msg string)
	GetInsight(instanceID string) *common.Insight
	GetApplicationLog(instanceID string, size int64) ([]string, error)
	// ResetStats will reset all the stats
	ResetStats(instanceID string)
	TrapEvent(instanceID string, trapEvent functionHandler.TrapEventOp, value interface{}) error

	GetRebalanceProgress(instanceID string, version string, appProgress *common.AppRebalanceProgress) bool

	NotifyOwnershipChange()
	NotifyGlobalConfigChange()

	DeleteFunctionSet()
}

type funcHandlerDetails struct {
	funcHandler  functionHandler.FunctionHandler
	currentState application.LifeCycleOp
}

type funcSet struct {
	sync.RWMutex

	fType   funcSetType
	id      string
	process processManager.ProcessManager

	// spawned is used in sequential manner so no need to protect it
	spawned bool
	config  config

	functionHandlerMap map[string]*funcHandlerDetails

	singleFunctionHandler *funcHandlerDetails
	close                 func()
}

type config struct {
	spawnImmediately bool
}

func dummyClose() {}

func NewFunctionSet(instanceID string, fType funcSetType, id string, funcSetConfig config, clusterSettings *common.ClusterSettings, appCallback processManager.AppLogFunction, systemConfig serverConfig.SystemConfig) functionSet {
	processConfig := processManager.ProcessConfig{
		Username:        clusterSettings.LocalUsername,
		Password:        clusterSettings.LocalPassword,
		Address:         clusterSettings.LocalAddress,
		IPMode:          clusterSettings.IpMode,
		BreakpadOn:      true,
		DebuggerPort:    clusterSettings.DebugPort,
		DiagDir:         clusterSettings.DiagDir,
		EventingDir:     clusterSettings.EventingDir,
		EventingPort:    clusterSettings.AdminHTTPPort,
		EventingSSLPort: clusterSettings.AdminSSLPort,
		ExecPath:        clusterSettings.ExecutablePath,
		CertPath:        clusterSettings.SslCAFile,
		ClientCertPath:  clusterSettings.ClientCertFile,
		ClientKeyPath:   clusterSettings.ClientKeyFile,
		ID:              id,
		AppLogCallback:  appCallback,
		NsServerPort:    clusterSettings.RestPort,
		InstanceID:      id,
	}

	if fType == SingleFunction {
		processConfig.SingleFunctionMode = true
	}

	fset := &funcSet{
		fType:   fType,
		id:      id,
		config:  funcSetConfig,
		spawned: false,
		process: processManager.NewProcessManager(processConfig, systemConfig),
		close:   dummyClose,
	}

	switch fType {
	case SingleFunction:
		fset.singleFunctionHandler = &funcHandlerDetails{
			funcHandler:  functionHandler.NewDummyFunctionHandler(),
			currentState: application.Undeploy,
		}

	case GroupOfFunctions:
		fset.functionHandlerMap = make(map[string]*funcHandlerDetails)
	}

	if funcSetConfig.spawnImmediately {
		ctx, close := context.WithCancel(context.Background())
		fset.close = close

		fset.startProcessWithContext(ctx)
		fset.spawned = true
	}
	return fset
}

func (fs *funcSet) GetID() funcSetType {
	return fs.fType
}

func (fs *funcSet) AddFunctionHandler(instanceID string, fHandler functionHandler.FunctionHandler) {
	fDetails := &funcHandlerDetails{
		currentState: application.Undeploy,
		funcHandler:  fHandler,
	}

	if fs.fType == SingleFunction {
		fs.singleFunctionHandler = fDetails
		return
	}

	fs.Lock()
	fs.functionHandlerMap[instanceID] = fDetails
	fs.Unlock()
}

func (fs *funcSet) ChangeState(oldInstanceID string, funcDetails *application.FunctionDetails, nextState application.LifeCycleOp) (application.LifeCycleOp, bool) {
	instanceID := funcDetails.AppInstanceID

	fDetails := fs.getFunctionHandler(oldInstanceID)
	if fs.fType == GroupOfFunctions {
		if oldInstanceID != instanceID {
			fs.Lock()
			delete(fs.functionHandlerMap, oldInstanceID)
			fs.functionHandlerMap[instanceID] = fDetails
			fs.Unlock()
		}
	}

	currState := fDetails.currentState
	fDetails.currentState = nextState
	fHandler := fDetails.funcHandler

	fHandler.AddFunctionDetails(funcDetails)
	switch nextState {
	case application.Undeploy:
		fHandler.ChangeState(fs.process, functionHandler.Undeployed)

	case application.Pause:
		fHandler.ChangeState(fs.process, functionHandler.Paused)

	case application.Deploy:
		if !fs.spawned {
			ctx, close := context.WithCancel(context.Background())
			fs.close = close

			fs.startProcessWithContext(ctx)
			fs.spawned = true
		}
		fHandler.ChangeState(fs.process, functionHandler.Deployed)
		return currState, true
	}

	return currState, true
}

func (fs *funcSet) Stats(instanceID string, statsType common.StatsType) *common.Stats {
	fHandler := fs.getFunctionHandler(instanceID)
	return fHandler.funcHandler.Stats(statsType)
}

func (fs *funcSet) ApplicationLog(instanceID, msg string) {
	fHandler := fs.getFunctionHandler(instanceID)
	fHandler.funcHandler.ApplicationLog(msg)
}

func (fs *funcSet) GetInsight(instanceID string) *common.Insight {
	fHandler := fs.getFunctionHandler(instanceID)
	return fHandler.funcHandler.GetInsight()
}

func (fs *funcSet) GetApplicationLog(instanceID string, size int64) ([]string, error) {
	fHandler := fs.getFunctionHandler(instanceID)
	return fHandler.funcHandler.GetApplicationLog(size)
}

func (fs *funcSet) ResetStats(instanceID string) {
	fHandler := fs.getFunctionHandler(instanceID)
	fHandler.funcHandler.ResetStats()
}

func (fs *funcSet) TrapEvent(instanceID string, trapEvent functionHandler.TrapEventOp, value interface{}) error {
	fHandler := fs.getFunctionHandler(instanceID)
	return fHandler.funcHandler.TrapEvent(trapEvent, value)
}

func (fs *funcSet) GetRebalanceProgress(instanceID string, version string, appProgress *common.AppRebalanceProgress) bool {
	fHandler := fs.getFunctionHandler(instanceID)
	return fHandler.funcHandler.GetRebalanceProgress(version, appProgress)
}

func (fs *funcSet) NotifyOwnershipChange() {
	funcHandlerList := fs.getFunctionHandlerList()

	for _, fHandler := range funcHandlerList {
		fHandler.funcHandler.NotifyOwnershipChange()
	}
}

func (fs *funcSet) NotifyGlobalConfigChange() {
	funcHandlerList := fs.getFunctionHandlerList()

	for _, fHandler := range funcHandlerList {
		fHandler.funcHandler.NotifyGlobalConfigChange()
	}
}

// Remove it from the list of processes and send how many process remained
func (fs *funcSet) DeleteFunctionHandler(instanceID string) (functionHandler.FunctionHandler, int) {
	if fs.fType == SingleFunction {
		fHandler := fs.singleFunctionHandler.funcHandler
		fs.singleFunctionHandler = &funcHandlerDetails{
			funcHandler:  functionHandler.NewDummyFunctionHandler(),
			currentState: application.Undeploy,
		}
		return fHandler, 0
	}

	fs.Lock()
	defer fs.Unlock()

	fh := fs.functionHandlerMap[instanceID]
	delete(fs.functionHandlerMap, instanceID)
	return fh.funcHandler, len(fs.functionHandlerMap)
}

// It will be called when all the functions are undeployed successfully
func (fs *funcSet) DeleteFunctionSet() {
	fs.close()
	fs.process.StopProcess()
}

// Start the process
func (fs *funcSet) startProcessWithContext(ctx context.Context) {
	logPrefix := fmt.Sprintf("funcSet::spawnApp[%s]", fs.id)
	receive, err := fs.spawnProcessLocked(ctx)
	if err != nil {
		logging.Errorf("%s error spawning process err: %v. Retrying...", logPrefix, err)
		receive = nil
	}
	go fs.spawnApp(ctx, receive)
}

func (fs *funcSet) spawnApp(ctx context.Context, receive <-chan *processManager.ResponseMessage) {
	logPrefix := fmt.Sprintf("funcSet::spawnApp[%s]", fs.id)

	defer func() {
		select {
		case <-ctx.Done():
			logging.Infof("%s done function set routine", logPrefix)
			return
		default:
		}

		fs.process.StopProcess()
		time.Sleep(10 * time.Millisecond)
		go fs.spawnApp(ctx, nil)
	}()

	var err error
	if receive == nil {
		receive, err = fs.spawnProcess(ctx)
		if err != nil {
			logging.Errorf("%s error spawning process err: %v. Retrying...", logPrefix, err)
			return
		}
	}

	for {
		select {
		case msg, ok := <-receive:
			if !ok {
				fs.process.StopProcess()
				logging.Infof("%s process stopped. Restarting...", logPrefix)
				return
			}

			funcHandler := fs.getFunctionHandler(msg.HandlerID)
			funcHandler.funcHandler.ReceiveMessage(msg)

		case <-ctx.Done():
			return
		}
	}
}

func (fs *funcSet) spawnProcess(ctx context.Context) (<-chan *processManager.ResponseMessage, error) {
	return fs.spawnProcessLocked(ctx)
}

func (fs *funcSet) spawnProcessLocked(ctx context.Context) (<-chan *processManager.ResponseMessage, error) {
	funcHandlerList := fs.getFunctionHandlerList()
	for _, fh := range funcHandlerList {
		fh.funcHandler.ChangeState(processManager.NewDummyProcessManager(), functionHandler.TempPause)
	}

	receive, err := fs.process.StartWithContext(ctx)
	if err != nil {
		return nil, err
	}

	funcHandlerList = fs.getFunctionHandlerList()
	for _, fh := range funcHandlerList {
		switch fh.currentState {
		case application.Deploy:
			fh.funcHandler.ChangeState(fs.process, functionHandler.Deployed)
		case application.Pause:
			fh.funcHandler.ChangeState(fs.process, functionHandler.Paused)
		case application.Undeploy:
			fh.funcHandler.ChangeState(fs.process, functionHandler.Undeployed)
		}
	}

	return receive, nil
}

func (fs *funcSet) getFunctionHandlerList() []*funcHandlerDetails {
	if fs.fType == SingleFunction {
		return []*funcHandlerDetails{fs.singleFunctionHandler}
	}

	fs.RLock()
	defer fs.RUnlock()

	fhs := make([]*funcHandlerDetails, 0, len(fs.functionHandlerMap))
	for _, fDetails := range fs.functionHandlerMap {
		fhs = append(fhs, fDetails)
	}
	return fhs
}

func (fs *funcSet) getFunctionHandler(instanceID string) *funcHandlerDetails {
	if fs.fType == SingleFunction {
		return fs.singleFunctionHandler
	}

	fs.RLock()
	defer fs.RUnlock()

	fh, ok := fs.functionHandlerMap[instanceID]
	if !ok {
		return &funcHandlerDetails{
			funcHandler:  functionHandler.NewDummyFunctionHandler(),
			currentState: application.Undeploy,
		}
	}
	return fh
}
