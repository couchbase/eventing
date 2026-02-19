package servicemanager2

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/parser"
	pc "github.com/couchbase/eventing/point_connection"
	serverConfig "github.com/couchbase/eventing/server_config"
	"github.com/couchbase/eventing/service_manager2/response"
)

// Config related functions
func (m *serviceMgr) checkAndSaveConfig(cred cbauth.Creds, runtimeInfo *response.RuntimeInfo, namespace application.Namespace, data []byte) {
	leaderNode := m.superSup.GetLeaderNode()
	if leaderNode != m.config.UUID {

		req := &pc.Request{
			Body:   data,
			Method: pc.POST,
			Query:  application.QueryMapNamesapce(namespace),
		}

		redirectRequestToLeader(cred, m.broadcaster, leaderNode, runtimeInfo, "/api/v1/config", req, runtimeInfo)
		return
	}

	keyspaceInfo, err := m.namespaceToKeyspaceInfo(namespace)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%s", err)
		return
	}

	m.lifeCycleOpSeq.Lock()
	defer m.lifeCycleOpSeq.Unlock()

	changedFields, configBytes, err := m.serverConfig.UpsertServerConfig(serverConfig.RestApi, keyspaceInfo, data)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%s", err)
		return
	}

	// Nothing changed no need to save the config
	if len(configBytes) == 0 {
		return
	}

	// Check whether we can store the config or not
	m.checkConfigStorageCondition(runtimeInfo, changedFields, keyspaceInfo)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	WriteConfig(runtimeInfo, keyspaceInfo, configBytes)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}
	m.serverConfig.UpsertServerConfig(serverConfig.MetaKvStore, keyspaceInfo, configBytes)
	return
}

func (m *serviceMgr) deleteConfig(cred cbauth.Creds, runtimeInfo *response.RuntimeInfo, namespace application.Namespace) {
	if namespace.String() == application.GlobalValue {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = "Deleting of global config not allowed"
		return
	}

	leaderNode := m.superSup.GetLeaderNode()
	if leaderNode != m.config.UUID {
		req := &pc.Request{
			Method: pc.DELETE,
			Query:  application.QueryMapNamesapce(namespace),
		}

		redirectRequestToLeader(cred, m.broadcaster, leaderNode, runtimeInfo, "/api/v1/config", req, runtimeInfo)
		return
	}

	keyspaceInfo, err := m.namespaceToKeyspaceInfo(namespace)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%s", err)
		return
	}
	m.lifeCycleOpSeq.Lock()
	defer m.lifeCycleOpSeq.Unlock()

	m.checkFunctionUsingConfig(runtimeInfo, keyspaceInfo)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}
	DeleteConfig(runtimeInfo, keyspaceInfo)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	m.serverConfig.DeleteSettings(keyspaceInfo)
	return
}

// eventing function related ops
func (m *serviceMgr) storeFunctionList(cred cbauth.Creds, runtimeInfo *response.RuntimeInfo, extractedList []*application.ExtractedFunction) []string {
	importedFuncs := make([]string, 0, len(extractedList))
	responseFuncs := make([]*response.RuntimeInfo, 0, len(extractedList))

	leaderNode := m.superSup.GetLeaderNode()
	if leaderNode != m.config.UUID {
		apps := make([]*application.FunctionDetails, 0, len(extractedList))
		for _, extractedFunc := range extractedList {
			if extractedFunc.Err != nil {
				continue
			}
			apps = append(apps, extractedFunc.FunctionDetails)
		}
		body, _ := json.Marshal(apps)
		req := &pc.Request{
			Body:   body,
			Method: pc.POST,
		}
		createdFuncs := make([]*response.RuntimeInfo, 0, len(apps))
		createdFuncs = redirectRequestToLeader(cred, m.broadcaster, leaderNode, runtimeInfo, "/api/v1/import", req, createdFuncs)
		if runtimeInfo.ErrCode == response.Ok {
			createdFuncIndex := 0
			for _, extractedDetails := range extractedList {
				if extractedDetails.Err != nil {
					responseFuncs = append(responseFuncs, &response.RuntimeInfo{
						ErrCode:     response.ErrInvalidRequest,
						Description: fmt.Sprintf("%v", extractedDetails.Err),
					})
					continue
				}

				if createdFuncIndex < len(createdFuncs) {
					res := createdFuncs[createdFuncIndex]
					if res.ErrCode == response.Ok {
						funcName := extractedDetails.FunctionDetails.AppLocation.String()
						importedFuncs = append(importedFuncs, funcName)
					}
					responseFuncs = append(responseFuncs, createdFuncs[createdFuncIndex])
					createdFuncIndex++
				} else {
					responseFuncs = append(responseFuncs, &response.RuntimeInfo{
						ErrCode: response.ErrInternalServer,
					})
				}
			}
			runtimeInfo.Description = responseFuncs
			runtimeInfo.OnlyDescription = true
		}
		return importedFuncs
	}

	for _, extractedDetails := range extractedList {
		if extractedDetails.Err != nil {
			responseFuncs = append(responseFuncs, &response.RuntimeInfo{
				ErrCode:     response.ErrInvalidRequest,
				Description: fmt.Sprintf("%v", extractedDetails.Err),
			})
			continue
		}
		funcDetails := extractedDetails.FunctionDetails
		rInfo, _ := m.storeFunction(cred, funcDetails)
		if rInfo.ErrCode == response.Ok {
			funcName := funcDetails.AppLocation.String()
			importedFuncs = append(importedFuncs, funcName)
		}
		responseFuncs = append(responseFuncs, rInfo)
	}

	runtimeInfo.Description = responseFuncs
	runtimeInfo.OnlyDescription = true
	return importedFuncs
}

// Store entire function
// Only metadata remain constant if function is upsert
// If its a insert then create functions
func (m *serviceMgr) storeFunction(cred cbauth.Creds, funcDetails *application.FunctionDetails) (*response.RuntimeInfo, bool) {
	runtimeInfo := &response.RuntimeInfo{}

	created := false
	leaderNode := m.superSup.GetLeaderNode()
	if leaderNode != m.config.UUID {
		body, _ := json.Marshal(funcDetails)
		req := &pc.Request{
			Body:   body,
			Method: pc.POST,
		}
		path := fmt.Sprintf("/api/v1/functions/%s", funcDetails.AppLocation.Appname)
		redirectRequestToLeader(cred, m.broadcaster, leaderNode, runtimeInfo, path, req, runtimeInfo)
		return runtimeInfo, created
	}

	m.lifeCycleOpSeq.Lock()
	defer m.lifeCycleOpSeq.Unlock()
	// Check for status on all the eventing nodes
	runtimeInfo, changed, created := m.verifyAndCreateFunction(cred, funcDetails)
	if runtimeInfo.ErrCode != response.Ok || !changed {
		return runtimeInfo, created
	}

	_, config := m.serverConfig.GetServerConfig(funcDetails.MetaInfo.FunctionScopeID)
	WriteToMetakv(runtimeInfo, config, funcDetails)
	if runtimeInfo.ErrCode != response.Ok {
		return runtimeInfo, created
	}
	runtimeInfo.WarningInfo = determineWarnings(funcDetails)

	// Update the local state with this function details
	m.updateLocalState(funcDetails)
	return runtimeInfo, created
}

func (m *serviceMgr) storeFunctionCode(cred cbauth.Creds, appLocation application.AppLocation, appCode string) (runtimeInfo *response.RuntimeInfo) {
	runtimeInfo = &response.RuntimeInfo{}

	leaderNode := m.superSup.GetLeaderNode()
	if leaderNode != m.config.UUID {
		req := &pc.Request{
			Body:   []byte(appCode),
			Method: pc.POST,
			Query:  application.QueryMap(appLocation),
		}

		path := fmt.Sprintf("/api/v1/functions/%s/appcode", appLocation.Appname)
		redirectRequestToLeader(cred, m.broadcaster, leaderNode, runtimeInfo, path, req, runtimeInfo)
		return runtimeInfo
	}

	m.lifeCycleOpSeq.Lock()
	defer m.lifeCycleOpSeq.Unlock()

	funcDetails, ok := m.appManager.GetApplication(appLocation, false)
	if !ok {
		runtimeInfo.ErrCode = response.ErrAppNotFoundTs
		runtimeInfo.Description = fmt.Sprintf("Function: %s not found", appLocation)
		return
	}

	// If function appcode is same as saved code then just return
	if funcDetails.AppCode == appCode {
		return
	}

	currAppState, _ := m.appState.GetAppState(appLocation)
	if currAppState.State == application.Deployed || currAppState.State == application.Deploying {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("Function %s is already deployed", appLocation)
		return
	}

	// Check for status on all the eventing nodes
	aggStatus, _ := m.getAggAppsStatus(runtimeInfo, []application.AppLocation{appLocation})
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	appStatus, ok := aggStatus[appLocation.ToLocationString()]
	if !ok {
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}
	possibleChanges, err := application.PossibleStateChange(appStatus.CompositeStatus, funcDetails.AppState)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%v", err)
		return
	}

	changed, err := funcDetails.VerifyAppCode(possibleChanges, appCode)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%v", err)
		return
	}

	if !changed {
		return
	}

	funcDetails.AppCode = appCode
	m.verifyFunctionAppCode(runtimeInfo, funcDetails)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	_, config := m.serverConfig.GetServerConfig(funcDetails.MetaInfo.FunctionScopeID)
	WriteToMetakv(runtimeInfo, config, funcDetails)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	// Update the local state with this function details
	m.updateLocalState(funcDetails)
	return
}

func (m *serviceMgr) storeFunctionBinding(cred cbauth.Creds, appLocation application.AppLocation, data []byte) (runtimeInfo *response.RuntimeInfo) {
	runtimeInfo = &response.RuntimeInfo{}
	leaderNode := m.superSup.GetLeaderNode()
	if leaderNode != m.config.UUID {
		req := &pc.Request{
			Body:   data,
			Method: pc.POST,
			Query:  application.QueryMap(appLocation),
		}

		path := fmt.Sprintf("/api/v1/functions/%s/config", appLocation.Appname)
		redirectRequestToLeader(cred, m.broadcaster, leaderNode, runtimeInfo, path, req, runtimeInfo)
		return runtimeInfo
	}

	depcfg, bindings, err := application.GetDeploymentConfig(data)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%v", err)
		return
	}

	m.lifeCycleOpSeq.Lock()
	defer m.lifeCycleOpSeq.Unlock()

	funcDetails, ok := m.appManager.GetApplication(appLocation, false)
	if !ok {
		runtimeInfo.ErrCode = response.ErrAppNotFoundTs
		runtimeInfo.Description = fmt.Sprintf("Function: %s not found", appLocation)
		return
	}

	// Check for status on all the eventing nodes
	aggStatus, _ := m.getAggAppsStatus(runtimeInfo, []application.AppLocation{appLocation})
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	appStatus, ok := aggStatus[appLocation.ToLocationString()]
	if !ok {
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}

	possibleChanges, err := application.PossibleStateChange(appStatus.CompositeStatus, funcDetails.AppState)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%v", err)
		return
	}

	changed, err := funcDetails.VerifyAndMergeDepCfg(possibleChanges, depcfg, bindings)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%v", err)
		return
	}

	if !changed {
		return
	}

	_, config := m.serverConfig.GetServerConfig(funcDetails.MetaInfo.FunctionScopeID)
	WriteToMetakv(runtimeInfo, config, funcDetails)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	// Update the local state with this function details
	m.updateLocalState(funcDetails)
	return
}

func (m *serviceMgr) storeFunctionSettings(cred cbauth.Creds, appLocation application.AppLocation, settings map[string]interface{}) (runtimeInfo *response.RuntimeInfo) {
	runtimeInfo = &response.RuntimeInfo{}

	leaderNode := m.superSup.GetLeaderNode()
	if leaderNode != m.config.UUID {
		settingsBytes, _ := json.Marshal(settings)
		req := &pc.Request{
			Body:   settingsBytes,
			Method: pc.POST,
			Query:  application.QueryMap(appLocation),
		}

		path := fmt.Sprintf("/api/v1/functions/%s/settings", appLocation.Appname)
		redirectRequestToLeader(cred, m.broadcaster, leaderNode, runtimeInfo, path, req, runtimeInfo)
		return runtimeInfo
	}

	m.lifeCycleOpSeq.Lock()
	defer m.lifeCycleOpSeq.Unlock()

	funcDetails, ok := m.appManager.GetApplication(appLocation, false)
	if !ok {
		runtimeInfo.ErrCode = response.ErrAppNotFoundTs
		runtimeInfo.Description = fmt.Sprintf("Function: %s not found", appLocation)
		return
	}

	// Get the nextState from the settings
	appStateChanged, err := funcDetails.AppState.VerifyAndMergeAppState(settings)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%v", err)
		return
	}

	// Check for status on all the eventing nodes
	aggStatus, _ := m.getAggAppsStatus(runtimeInfo, []application.AppLocation{appLocation})
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	currAppStatus, ok := aggStatus[appLocation.ToLocationString()]
	if !ok {
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}

	possibleChanges, err := application.PossibleStateChange(currAppStatus.CompositeStatus, funcDetails.AppState)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%v", err)
		return
	}

	changed, err := funcDetails.VerifyAndMergeSettings(possibleChanges, settings)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%v", err)
		return
	}

	if !changed && !appStateChanged {
		return
	}

	m.verifyAndAddMetaDetailsFunction(runtimeInfo, application.StringToAppState(currAppStatus.CompositeStatus), funcDetails)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	_, config := m.serverConfig.GetServerConfig(funcDetails.MetaInfo.FunctionScopeID)
	WriteToMetakv(runtimeInfo, config, funcDetails)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	// Update the local state with this function details
	m.updateLocalState(funcDetails)
	return
}

// Helper function to verify and update function fields
func (m *serviceMgr) verifyFunctionAppCode(runtimeInfo *response.RuntimeInfo, funcDetails *application.FunctionDetails) {
	compilationInfo, err := m.superSup.CompileHandler(funcDetails)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("Internal server error while compiling handler: err: %v", err)
		return
	}

	if !compilationInfo.CompileSuccess {
		runtimeInfo.ErrCode = response.ErrHandlerCompile
		runtimeInfo.Description = compilationInfo
		return
	}
}

func (m *serviceMgr) verifyAndCreateFunction(cred cbauth.Creds, fDetails *application.FunctionDetails) (runtimeInfo *response.RuntimeInfo, changed bool, created bool) {
	logPrefix := "serviceMgr::verifyAndCreateFunction"

	runtimeInfo = &response.RuntimeInfo{}
	changed = true
	created = false

	currState := application.Undeployed
	// If function doesn't exist then its created for the first time
	oldApp, ok := m.appManager.GetApplication(fDetails.AppLocation, false)
	if !ok {
		_, err := application.PossibleStateChange("", fDetails.AppState)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInvalidRequest
			runtimeInfo.Description = fmt.Sprintf("%v", err)
			return
		}

		created = true
		instanceID, err := getFunctionInstanceID()
		if err != nil {
			logging.Errorf("%s Unable to create instance id for: %s. err: %v", logPrefix, fDetails.AppLocation, err)
			runtimeInfo.ErrCode = response.ErrInternalServer
			return
		}

		fDetails.AppID, err = getFunctionID()
		if err != nil {
			logging.Errorf("%s Unable to create function id for: %s. err: %v", logPrefix, fDetails.AppLocation, err)
			runtimeInfo.ErrCode = response.ErrInternalServer
			return
		}
		fDetails.AppInstanceID = strconv.Itoa(int(fDetails.AppID)) + "-" + instanceID

		name, domain := cred.User()
		uuid := ""
		// GetUserUuid returns error when domain is not local
		// This will ensure that eventing won't throw error when domain is not local
		if domain == "local" {
			var err error
			uuid, err = cbauth.GetUserUuid(name, domain)
			if err != nil {
				logging.Errorf("%s Unable to get user uuid for %s err: %v", logPrefix, fDetails.AppLocation, err)
				runtimeInfo.ErrCode = response.ErrInternalServer
				return
			}
		}

		fDetails.Owner = application.Owner{
			User:   name,
			Domain: domain,
			UUID:   uuid,
		}

		funcScope := application.Keyspace{
			Namespace:      fDetails.AppLocation.Namespace,
			CollectionName: "*",
		}

		fDetails.MetaInfo.FunctionScopeID = m.superSup.PopulateID(runtimeInfo, funcScope)
		if runtimeInfo.ErrCode != response.Ok {
			return
		}
		fDetails.MetaInfo.LogFileName = generateRandomNameSuffix()
	} else {
		// Check for status on all the eventing nodes
		aggStatus, _ := m.getAggAppsStatus(runtimeInfo, []application.AppLocation{fDetails.AppLocation})
		if runtimeInfo.ErrCode != response.Ok {
			return
		}

		currAppStatus, ok := aggStatus[fDetails.AppLocation.ToLocationString()]
		if !ok {
			runtimeInfo.ErrCode = response.ErrInternalServer
			return
		}

		currState = application.StringToAppState(currAppStatus.CompositeStatus)
		switch currState {
		case application.Paused, application.Undeployed, application.Deployed:
			fDetails.AppID = oldApp.AppID
			fDetails.AppInstanceID = oldApp.AppInstanceID
			fDetails.MetaInfo = oldApp.MetaInfo.Clone()
			fDetails.Owner = oldApp.Owner.Clone()

		default:
			runtimeInfo.ErrCode = response.ErrInvalidRequest
			logging.Errorf("%s Function %s is in invalid state %s", logPrefix, fDetails.AppLocation, currState)
			runtimeInfo.Description = fmt.Sprintf("Function %s is in %s state. Try again later.", fDetails.AppLocation, currState)
			return
		}

		err := oldApp.IsPossibleToMerge(currAppStatus.CompositeStatus, fDetails)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInvalidRequest
			runtimeInfo.Description = fmt.Sprintf("%v", err)
			return
		}

	}

	m.verifyFunctionAppCode(runtimeInfo, fDetails)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	m.verifyAndAddMetaDetailsFunction(runtimeInfo, currState, fDetails)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	return
}

func (m *serviceMgr) verifyAndAddMetaDetailsFunction(runtimeInfo *response.RuntimeInfo, currStatus application.State, funcDetails *application.FunctionDetails) {
	logPrefix := "serviceMgr::verifyAndAddMetaDetailsFunction"

	nextState := m.verifyAndGetNextState(runtimeInfo, currStatus, funcDetails)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	// Caller will verify all the fields
	currAppState, _ := m.appState.GetAppState(funcDetails.AppLocation)
	state := currAppState.State
	m.updateMetaSeqAndPrevState(funcDetails, nextState, state)

	if nextState == application.Pause && state != application.Paused {
		funcDetails.MetaInfo.LastPaused = time.Now()
	}

	if nextState == application.Deploy {
		err := m.superSup.AssignOwnership(funcDetails)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInvalidRequest
			runtimeInfo.Description = fmt.Sprintf("%v", err)
			return
		}
	}

	if nextState == application.Deploy && state != application.Deployed {
		if parser.IsCodeUsingOnDeploy(funcDetails.AppCode) {
			// Delete checkpoint only for previous OnDeploy runs to avoid race condition
			err := m.superSup.DeleteOnDeployCheckpoint(funcDetails, false)
			if err != nil {
				logging.Errorf("%s Error in deleting OnDeploy checkpoint: %v", logPrefix, err)
				runtimeInfo.ErrCode = response.ErrInternalServer
				return
			}
		}
	}

	// Fresh deployment. Persist important fields for deployment
	if nextState == application.Deploy && (state == application.Undeployed || state == application.NoState) {
		instanceID, err := getFunctionInstanceID()
		if err != nil {
			logging.Errorf("%s Unable to create instance id for: %s. err: %v", logPrefix, funcDetails.AppLocation, err)
			runtimeInfo.ErrCode = response.ErrInternalServer
			return
		}

		funcDetails.AppID, err = getFunctionID()
		if err != nil {
			logging.Errorf("%s Unable to create function id for: %s. err: %v", logPrefix, funcDetails.AppLocation, err)
			runtimeInfo.ErrCode = response.ErrInternalServer
			return
		}
		funcDetails.AppInstanceID = strconv.Itoa(int(funcDetails.AppID)) + "-" + instanceID

		funcDetails.MetaInfo.IsUsingTimer = parser.UsingTimer(funcDetails.AppCode)

		m.superSup.CreateInitCheckpoint(runtimeInfo, funcDetails)
		if runtimeInfo.ErrCode != response.Ok {
			return
		}
	}
}

// delete the function from metakv
func (m *serviceMgr) deleteFunction(appLocation application.AppLocation, handlerID uint32) (runtimeInfo *response.RuntimeInfo) {
	const logPrefix string = "serviceMgr::deleteFunction"

	runtimeInfo = &response.RuntimeInfo{}

	// TODO: Redirect this request to leader node

	m.lifeCycleOpSeq.Lock()
	defer m.lifeCycleOpSeq.Unlock()

	funcDetails, ok := m.appManager.GetApplication(appLocation, true)
	if !ok {
		runtimeInfo.ErrCode = response.ErrAppNotFoundTs
		runtimeInfo.Description = fmt.Sprintf("Function: %s not found", appLocation)
		return
	}

	if handlerID != uint32(0) && funcDetails.AppID != handlerID {
		runtimeInfo.ErrCode = response.ErrAppNotFoundTs
		runtimeInfo.Description = fmt.Sprintf("Function: %s with handlerID %v not found", appLocation, handlerID)
		return
	}

	// Check for status on all the eventing nodes
	aggStatus, _ := m.getAggAppsStatus(runtimeInfo, []application.AppLocation{appLocation})
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	appStatus, ok := aggStatus[appLocation.ToLocationString()]
	if !ok {
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}

	currState := application.StringToAppState(appStatus.CompositeStatus)
	if currState != application.Undeployed {
		runtimeInfo.ErrCode = response.ErrAppNotUndeployed
		runtimeInfo.Description = fmt.Sprintf("Function: %s skipping delete request as it hasn't been undeployed", appLocation)
		return
	}

	if err := m.superSup.DeleteOnDeployCheckpoint(funcDetails, true); err != nil {
		logging.Errorf("%s Error in deleting OnDeploy checkpoint for app: %s, err: %v", logPrefix, appLocation, err)
	}

	DeleteFromMetakv(runtimeInfo, appLocation)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	return
}

// Helper function
func (m *serviceMgr) updateMetaSeqAndPrevState(funcDetails *application.FunctionDetails, op application.LifeCycleOp, currState application.State) {
	nextState := application.GetStateFromLifeCycle(op)

	// Life cycle operation is performed
	if (nextState != currState) || (funcDetails.MetaInfo.Seq == 0) {
		funcDetails.MetaInfo.Seq++

		if currState == application.NoState {
			funcDetails.MetaInfo.PrevState = application.Undeployed
		} else {
			funcDetails.MetaInfo.PrevState = currState
		}
	}
}
