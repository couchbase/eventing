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

// This manages multiple fuction in one single executor
type functionSet interface {
	GetFunctionHandler(instanceID string) (functionHandler.FunctionHandler, bool)
	AddFunctionHandler(instanceID string, fhHandler functionHandler.FunctionHandler)
	DeleteFunctionHandler(instanceID string) (functionHandler.FunctionHandler, int)

	ChangeState(instanceID string, funcDetails *application.FunctionDetails, nextState application.LifeCycleOp) (application.LifeCycleOp, bool)

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

	id      string
	process processManager.ProcessManager
	spawned bool
	config  config

	functionHandlerMap map[string]*funcHandlerDetails
	close              func()
}

type config struct {
	spawnImmediately bool
}

func dummyClose() {}

func NewFunctionSet(id string, funcSetConfig config, clusterSettings *common.ClusterSettings, appCallback processManager.AppLogFunction, systemConfig serverConfig.SystemConfig) functionSet {
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
		ID:              id,
		AppLogCallback:  appCallback,
		NsServerPort:    clusterSettings.RestPort,
	}

	fset := &funcSet{
		id:                 id,
		config:             funcSetConfig,
		spawned:            false,
		functionHandlerMap: make(map[string]*funcHandlerDetails),
		process:            processManager.NewProcessManager(processConfig, systemConfig),
		close:              dummyClose,
	}

	if funcSetConfig.spawnImmediately {
		ctx, close := context.WithCancel(context.Background())
		fset.close = close

		fset.startProcessWithContext(ctx)
		fset.spawned = true
	}
	return fset
}

func (fs *funcSet) GetFunctionHandler(instanceID string) (functionHandler.FunctionHandler, bool) {
	fs.RLock()
	defer fs.RUnlock()

	fh, ok := fs.functionHandlerMap[instanceID]
	if !ok {
		return nil, false
	}

	return fh.funcHandler, true
}

func (fs *funcSet) AddFunctionHandler(instanceID string, fHandler functionHandler.FunctionHandler) {
	fDetails := &funcHandlerDetails{
		currentState: application.Undeploy,
		funcHandler:  fHandler,
	}

	fs.Lock()
	defer fs.Unlock()

	fs.functionHandlerMap[instanceID] = fDetails
}

func (fs *funcSet) ChangeState(oldInstanceID string, funcDetails *application.FunctionDetails, nextState application.LifeCycleOp) (application.LifeCycleOp, bool) {
	fs.Lock()
	defer fs.Unlock()

	instanceID := funcDetails.AppInstanceID
	fDetails := fs.functionHandlerMap[oldInstanceID]
	if oldInstanceID != instanceID {
		delete(fs.functionHandlerMap, oldInstanceID)
		fs.functionHandlerMap[instanceID] = fDetails
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

func (fs *funcSet) NotifyOwnershipChange() {
	fs.RLock()
	defer fs.RUnlock()

	for _, fDetails := range fs.functionHandlerMap {
		if fDetails.currentState == application.Deploy {
			fDetails.funcHandler.NotifyOwnershipChange()
		}
	}
}

func (fs *funcSet) NotifyGlobalConfigChange() {
	fs.RLock()
	defer fs.RUnlock()

	for _, fDetails := range fs.functionHandlerMap {
		if fDetails.currentState == application.Deploy {
			fDetails.funcHandler.NotifyGlobalConfigChange()
		}
	}
}

// Remove it from the list of processes and send how many process remained
func (fs *funcSet) DeleteFunctionHandler(instanceID string) (functionHandler.FunctionHandler, int) {
	fs.Lock()
	defer fs.Unlock()

	fh, ok := fs.functionHandlerMap[instanceID]
	if !ok {
		return nil, len(fs.functionHandlerMap)
	}

	delete(fs.functionHandlerMap, instanceID)
	return fh.funcHandler, len(fs.functionHandlerMap)
}

// It will be called when all the functions are undeployed successfully
func (fs *funcSet) DeleteFunctionSet() {
	fs.Lock()
	if !fs.spawned {
		fs.Unlock()
		return
	}
	fs.spawned = false
	fs.Unlock()

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

			fs.RLock()
			fh, ok := fs.functionHandlerMap[msg.HandlerID]
			fs.RUnlock()

			if ok {
				fh.funcHandler.ReceiveMessage(msg)
			}

		case <-ctx.Done():
			return
		}
	}
}

func (fs *funcSet) spawnProcess(ctx context.Context) (<-chan *processManager.ResponseMessage, error) {
	fs.Lock()
	defer fs.Unlock()

	return fs.spawnProcessLocked(ctx)
}

func (fs *funcSet) spawnProcessLocked(ctx context.Context) (<-chan *processManager.ResponseMessage, error) {
	fs.Lock()
	defer fs.Unlock()

	deployedList := make([]functionHandler.FunctionHandler, 0, len(fs.functionHandlerMap))
	for _, fh := range fs.functionHandlerMap {
		if fh.currentState == application.Deploy {
			deployedList = append(deployedList, fh.funcHandler)
		}
		fh.funcHandler.ChangeState(nil, functionHandler.TempPause)
	}

	receive, err := fs.process.StartWithContext(ctx)
	if err != nil {
		return nil, err
	}

	// No need to notify pause or undeploy list since function handler will take care of it
	for _, fh := range deployedList {
		fh.ChangeState(fs.process, functionHandler.Deployed)
	}

	return receive, nil
}
