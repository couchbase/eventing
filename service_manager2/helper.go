package servicemanager2

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/couchbase/cbauth"
	stateMachine "github.com/couchbase/eventing/app_manager/app_state_machine"
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/authenticator/rbac"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
	pc "github.com/couchbase/eventing/point_connection"
	serverConfig "github.com/couchbase/eventing/server_config"
	"github.com/couchbase/eventing/service_manager2/response"
	"github.com/couchbase/eventing/service_manager2/syncgateway"
)

// redirectRequestToLeader redirects request to leader node. Returns redirected to false if this is the leader node
func redirectRequestToLeader[T any](cred cbauth.Creds, broadcaster common.Broadcaster, leaderNode string, runtimeInfo *response.RuntimeInfo, path string, req *pc.Request, successResponse T) T {
	logPrefix := "serviceMgr::redirectRequestToLeader"

	req.Query = rbac.PutOnBehalfOf(cred, req.Query)
	responseBytes, res, err := broadcaster.RequestFor(leaderNode, path, req)
	if err != nil {
		if res == nil {
			logging.Errorf("%s Error sending request to leader: %s path: %s err: %v", logPrefix, leaderNode, path, err)
			runtimeInfo.ErrCode = response.ErrInternalServer
			return successResponse
		}

		if len(res.Err.Error()) == 0 {
			logging.Errorf("%s Unexpected response from leader: %s path: %s", logPrefix, leaderNode, path)
			runtimeInfo.ErrCode = response.ErrInternalServer
			return successResponse
		}

		errInfo := &response.ErrorInfo{}
		err := json.Unmarshal([]byte(res.Err.Error()), errInfo)
		if err != nil {
			logging.Errorf("%s Unable to unmarshal body into runtime info: %s path: %s err: %v responseErr: %v", logPrefix, leaderNode, path, err, res.Err)
			runtimeInfo.ErrCode = response.ErrInternalServer
			return successResponse
		}

		runtimeInfo.Convert(errInfo)
		return successResponse
	}

	if len(responseBytes) == 0 {
		logging.Errorf("%s Unexpected response from leader: %s path: %s", logPrefix, leaderNode, path)
		runtimeInfo.ErrCode = response.ErrInternalServer
		return successResponse
	}

	// Check if runtime info returned is correct or not
	err = json.Unmarshal(responseBytes[0], &successResponse)
	if err != nil {
		logging.Errorf("%s Unable to unmarshal body for leader: %s path: %s err: %v responseErr: %v", logPrefix, leaderNode, path, err)
		runtimeInfo.ErrCode = response.ErrInternalServer
		return successResponse
	}
	return successResponse
}

func (m *serviceMgr) verifyAndGetNextState(runtimeInfo *response.RuntimeInfo, funcDetails *application.FunctionDetails) (nextState application.LifeCycleOp) {
	nextState = funcDetails.AppState.GetLifeCycle()
	if checkKeyspacePermissions(runtimeInfo, nextState, funcDetails); runtimeInfo.ErrCode != response.Ok {
		return
	}

	if m.isInterFunctionRecursion(runtimeInfo, nextState, funcDetails); runtimeInfo.ErrCode != response.Ok {
		return
	}

	if m.checkSyncgatewayCoexistence(runtimeInfo, nextState, funcDetails); runtimeInfo.ErrCode != response.Ok {
		return
	}

	if m.checkCursorLimit(runtimeInfo, nextState, funcDetails); runtimeInfo.ErrCode != response.Ok {
		return
	}
	return
}

func (m *serviceMgr) updateLocalState(funcDetails *application.FunctionDetails) {
	m.appManager.StoreApplication(funcDetails)

	if funcDetails.AppState.GetLifeCycle() == application.Deploy {
		m.cursorRegistry.Register(funcDetails.DeploymentConfig.SourceKeyspace.Clone(), funcDetails.AppInstanceID)
		src, dst, _ := funcDetails.GetSourceAndDestinations(true)
		m.bucketGraph.InsertEdges(funcDetails.AppLocation.ToLocationString(), src, dst)
	}

	m.appState.StartStateChange(funcDetails.MetaInfo.Seq, funcDetails.AppLocation, funcDetails.AppState)
}

func checkKeyspacePermissions(runtimeInfo *response.RuntimeInfo, nextState application.LifeCycleOp, funcDetails *application.FunctionDetails) {
	if nextState != application.Deploy {
		return
	}
	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: funcDetails.AppLocation.Namespace})
	bucketPerms := rbac.HandlerBucketPermissions(funcDetails.DeploymentConfig.SourceKeyspace, funcDetails.DeploymentConfig.MetaKeyspace)

	reqFuncPerms := append(bucketPerms, perms...)
	if notAllowed, err := rbac.HasPermissions(&funcDetails.Owner, reqFuncPerms, true); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, true, err)
	}
}

func (m *serviceMgr) isInterFunctionRecursion(runtimeInfo *response.RuntimeInfo, nextState application.LifeCycleOp, funcDetails *application.FunctionDetails) {
	if nextState != application.Deploy {
		return
	}
	_, config := m.serverConfig.GetServerConfig(funcDetails.MetaInfo.FunctionScopeID)
	if config.AllowInterbucketRecursion {
		return
	}

	src, dst, err := funcDetails.GetSourceAndDestinations(true)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInterBucketRecursion
		runtimeInfo.Description = fmt.Sprintf("Function: %s %v", funcDetails.AppLocation, err)
		return
	}

	if possible, path := m.bucketGraph.IsAcyclicInsertPossible(funcDetails.AppLocation.ToLocationString(), src, dst); !possible {
		runtimeInfo.ErrCode = response.ErrInterBucketRecursion
		runtimeInfo.Description = fmt.Sprintf("Inter bucket recursion error; function: %s causes a cycle "+
			"involving functions: %v, hence deployment is disallowed", funcDetails.AppLocation, path)
		return
	}
	m.checkInterFunctionRecursion(runtimeInfo, funcDetails)
}

func (m *serviceMgr) namespaceToKeyspaceInfo(namespace application.Namespace) (application.KeyspaceInfo, error) {
	keyspaceInfo := application.NewKeyspaceInfo("", "", "", "", 0)
	if namespace.String() != application.GlobalValue {
		bucketDetails, err := m.observer.GetCurrentState(notifier.InterestedEvent{
			Event:  notifier.EventBucketChanges,
			Filter: namespace.BucketName,
		})
		if err != nil {
			return keyspaceInfo, fmt.Errorf("bucket %s doesn't exist", namespace.BucketName)
		}
		details := bucketDetails.(*notifier.Bucket)
		keyspaceInfo.BucketID = details.UUID

		if namespace.ScopeName != application.GlobalValue {
			bucketManifest, err := m.observer.GetCurrentState(notifier.InterestedEvent{
				Event:  notifier.EventScopeOrCollectionChanges,
				Filter: namespace.BucketName,
			})
			if err != nil {
				return keyspaceInfo, fmt.Errorf("bucket %s doesn't exist", namespace.BucketName)
			}

			manifest := bucketManifest.(*notifier.CollectionManifest)
			scopeID, ok := manifest.Scopes[namespace.ScopeName]
			if !ok {
				return keyspaceInfo, fmt.Errorf("scope %s doesn't exist", namespace.ScopeName)
			}
			keyspaceInfo.ScopeID = scopeID.SID
		}
	}
	return keyspaceInfo, nil
}

// Just checks the local node. Maybe need to extend for remote nodes as well
func (m *serviceMgr) checkInterFunctionRecursion(runtimeInfo *response.RuntimeInfo, funcDetails *application.FunctionDetails) {
	if !funcDetails.IsSourceMutationPossible() {
		return
	}

	src := funcDetails.DeploymentConfig.SourceKeyspace
	appLocationList := m.appManager.ListApplication()
	for _, appLocation := range appLocationList {
		state, err := m.appState.GetAppState(appLocation)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInternalServer
			runtimeInfo.Description = "sync issue. Retry later"
			return
		}

		if !state.IsDeployed() {
			continue
		}

		fDetails, ok := m.appManager.GetApplication(appLocation, true)
		if !ok {
			runtimeInfo.ErrCode = response.ErrInternalServer
			runtimeInfo.Description = "sync issue. Retry later"
			return
		}
		if !fDetails.IsSourceMutationPossible() {
			continue
		}

		if fDetails.DeploymentConfig.SourceKeyspace.Match(src) {
			runtimeInfo.ErrCode = response.ErrInterFunctionRecursion
			runtimeInfo.Description = fmt.Sprintf("Inter handler recursion error with app: %s", appLocation)
			return
		}
	}
}

func (m *serviceMgr) checkSyncgatewayCoexistence(runtimeInfo *response.RuntimeInfo, nextState application.LifeCycleOp, funcDetails *application.FunctionDetails) {
	logPrefix := "serviceMgr::checkSyncgatewayCoexistence"
	if nextState != application.Deploy {
		return
	}

	if !funcDetails.IsSourceMutationPossible() {
		return
	}

	keyspace, _ := application.NewKeyspace(
		funcDetails.DeploymentConfig.SourceKeyspace.BucketName,
		application.DefaultScopeOrCollection,
		application.DefaultScopeOrCollection,
		false,
	)
	if !common.CheckKeyspaceExist(m.observer, keyspace) {
		return
	}
	collection, err := m.superSup.GetCollectionObject(keyspace)
	if err != nil {
		logging.Errorf("%s unable to create collection object: %v", logPrefix, err)
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}

	syncgateway.CheckSyncgatewayDeployment(runtimeInfo, collection, funcDetails.DeploymentConfig.SourceKeyspace)
}

func (m *serviceMgr) checkCursorLimit(runtimeInfo *response.RuntimeInfo, nextState application.LifeCycleOp, funcDetails *application.FunctionDetails) {
	if nextState != application.Deploy || !funcDetails.Settings.CursorAware {
		return
	}

	if ok := m.cursorRegistry.IsRegisterPossible(funcDetails.DeploymentConfig.SourceKeyspace, ""); !ok {
		runtimeInfo.ErrCode = response.ErrCursorLimitReached
		runtimeInfo.Description = fmt.Sprintf("Reached cursor aware functions limit on this keyspace. Deployment not allowed for function %s", funcDetails.AppLocation)
		return
	}
}

func (m *serviceMgr) checkAndChangeName(app *application.FunctionDetails) {
	appLocation := app.AppLocation
	for {
		state, err := m.appState.GetAppState(app.AppLocation)
		if err != nil {
			break
		}

		if !state.IsDeployed() {
			break
		}

		appLocation.Appname = fmt.Sprintf("%s_%s", appLocation.Appname, generateRandomNameSuffix())
	}
	app.AppLocation = appLocation
}

func (m *serviceMgr) getApplication(cred cbauth.Creds, filterMap map[string]bool, filterType string) []json.Marshaler {
	appLocations := m.appManager.ListApplication()
	apps := make([]json.Marshaler, 0, len(appLocations))

	for _, location := range appLocations {
		perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: location.Namespace})
		if _, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
			continue
		}

		if filterType != "" {
			app, ok := m.appManager.GetApplication(location, true)
			if !ok {
				continue
			}

			if !applyFilter(app, filterMap, filterType) {
				continue
			}
		}

		funcDetails, err := m.appManager.GetAppMarshaler(location, application.Version1)
		if err != nil {
			continue
		}
		apps = append(apps, funcDetails)
	}
	return apps
}

func (m *serviceMgr) getLocalBootstrappingApps() map[string]string {
	appLocationList := m.appManager.ListApplication()
	permBootstrapping := make(map[string]string)
	for _, appLocation := range appLocationList {
		state, err := m.appState.GetAppState(appLocation)
		if err != nil {
			continue
		}

		if state.IsBootstrapping() {
			permBootstrapping[appLocation.ToLocationString()] = state.String()
		}
	}

	return permBootstrapping
}

func (m *serviceMgr) getLocalAppsWithStatus(reqState application.State) map[string]string {
	appLocationList := m.appManager.ListApplication()
	permReqState := make(map[string]string)
	for _, appLocation := range appLocationList {
		state, err := m.appState.GetAppState(appLocation)
		if err != nil {
			continue
		}

		if state.State == reqState {
			permReqState[appLocation.ToLocationString()] = state.String()
		}
	}

	return permReqState
}

func (m *serviceMgr) getAggAppsStatus(runtimeInfo *response.RuntimeInfo,
	appLocation []application.AppLocation) (aggStatus map[string]*common.AppStatus, numEventingNode int) {
	logPrefix := "serviceMgr::getAggAppsStatus"

	req := &pc.Request{
		Method: pc.GET,
	}

	aggStatus = make(map[string]*common.AppStatus)
	location := make([]string, 0, len(appLocation))
	for _, appL := range appLocation {
		funcDetails, ok := m.appManager.GetApplication(appL, false)
		if !ok {
			continue
		}

		stringLocation := appL.ToLocationString()
		aggStatus[stringLocation] = &common.AppStatus{
			FunctionScope:    appL.Namespace,
			Name:             appL.Appname,
			DeploymentStatus: funcDetails.AppState.DeploymentState,
			ProcessingStatus: funcDetails.AppState.ProcessingState,
		}
		location = append(location, stringLocation)
	}

	req.Query = map[string][]string{"appNames": location}
	resBytes, _, err := m.broadcastRequestGlobal("/getStatus", req)
	if err != nil {
		logging.Errorf("%s unable to get status from all active eventing nodes: %v", logPrefix, err)
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}
	numEventingNode = len(resBytes)

	for _, statusBytes := range resBytes {
		statusMap := make(map[string]stateMachine.AppState)
		err := json.Unmarshal(statusBytes, &statusMap)
		if err != nil {
			logging.Errorf("%s unable to marshal status bytes from all active eventing nodes: %v", logPrefix, err)
			runtimeInfo.ErrCode = response.ErrInternalServer
			return
		}

		for stringLocation, status := range statusMap {
			state, ok := aggStatus[stringLocation]
			if !ok {
				// App not present on the remote node
				// status should be some bootstrapping
				continue
			}
			state.AppState = stateMachine.DetermineStatus(state.AppState, status)
			state.NumDeployedNodes++
			aggStatus[stringLocation] = state
		}
	}

	for _, appStatus := range aggStatus {
		if appStatus.NumDeployedNodes == numEventingNode {
			appStatus.CompositeStatus = appStatus.AppState.String()
		} else {
			appStatus.CompositeStatus = appStatus.AppState.BootstrapingString()
		}
	}

	return
}

func (m *serviceMgr) clearStats(params map[string][]string) error {
	logPrefix := "serviceMgr::getAggAppsStatus"

	req := &pc.Request{
		Method: pc.GET,
		Query:  params,
	}

	_, _, err := m.broadcastRequestGlobal("/resetNodeStatsCounters", req)
	if err != nil {
		logging.Errorf("%s unable to clear stats from all active eventing nodes: %v", logPrefix, err)
		return err
	}
	return nil
}

const (
	allFunc uint8 = iota
	sbm
	notsbm
)

type filterFunction struct {
	sourceKeyspace *application.Keyspace
	funcType       uint8
	deployedFunc   bool
}

func getFilterFunc(query url.Values) (fFunction filterFunction, err error) {
	buckets, ok := query["source_bucket"]
	if ok && len(buckets) > 1 {
		return fFunction, fmt.Errorf("more than one bucket name present in function list query")
	}
	scopes, ok := query["source_scope"]
	if ok && len(scopes) > 1 {
		return fFunction, fmt.Errorf("more than one scope name present in function list query")
	}

	collections, ok := query["source_collection"]
	if ok && len(collections) > 1 {
		return fFunction, fmt.Errorf("more than one collection name present in function list query")
	}

	if buckets[0] != "" {
		sourceKeyspace, err := application.NewKeyspace(buckets[0], scopes[0], collections[0], true)
		if err != nil {
			return fFunction, err
		}

		fFunction.sourceKeyspace = &sourceKeyspace
	}

	functionTypes, ok := query["function_type"]
	if ok {
		if len(functionTypes) > 1 {
			return fFunction, fmt.Errorf("more than one function type present in function list query")
		}
		switch functionTypes[0] {
		case "sbm":
			fFunction.funcType = sbm
		case "notsbm":
			fFunction.funcType = notsbm
		default:
			return fFunction, fmt.Errorf("invalid function type, supported function types are: sbm, notsbm")
		}
	}

	deploymentStatus, ok := query["deployed"]
	if ok {
		if len(deploymentStatus) > 1 {
			return fFunction, fmt.Errorf("more than one deployment status present in function list query")
		}

		switch deploymentStatus[0] {
		case "true":
			fFunction.deployedFunc = true
		case "false":
			fFunction.deployedFunc = false
		default:
			return fFunction, fmt.Errorf("invalid deployment status, supported deployment status are: true, false")
		}
	}
	return
}

func (m *serviceMgr) functionTypeMatch(fFunction filterFunction, aggStatus map[string]*common.AppStatus, funcDetails *application.FunctionDetails) bool {
	if fFunction.sourceKeyspace != nil && !fFunction.sourceKeyspace.Match(funcDetails.DeploymentConfig.SourceKeyspace) {
		return false
	}

	funcType := notsbm
	if funcDetails.IsSourceMutationPossible() {
		funcType = sbm
	}

	if fFunction.funcType != allFunc && funcType != fFunction.funcType {
		return false
	}

	state, ok := aggStatus[funcDetails.AppLocation.ToLocationString()]
	if !ok {
		return false
	}
	if application.StringToAppState(state.CompositeStatus).IsDeployed() != fFunction.deployedFunc {
		return false
	}
	return true
}

func (m *serviceMgr) getFunctionList(cred cbauth.Creds, runtimeInfo *response.RuntimeInfo, params url.Values) []string {
	fFunction, err := getFilterFunc(params)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = err
		return nil
	}

	applications := m.appManager.ListApplication()
	aggStatus, _ := m.getAggAppsStatus(runtimeInfo, applications)
	if runtimeInfo.ErrCode != response.Ok {
		return nil
	}

	funcList := make([]string, 0, len(applications))
	for _, appLocation := range applications {
		perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
		if _, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
			continue
		}
		funcDetails, ok := m.appManager.GetApplication(appLocation, false)
		if !ok {
			continue
		}

		if m.functionTypeMatch(fFunction, aggStatus, funcDetails) {
			funcList = append(funcList, appLocation.String())
		}
	}
	return funcList
}

func (m *serviceMgr) checkConfigStorageCondition(runtimeInfo *response.RuntimeInfo, changedFields []string, keyspaceInfo application.KeyspaceInfo) {
	checkNamespaceConfigBeingUsed := false
	for _, field := range changedFields {
		switch field {
		case serverConfig.NumNodesRunningJSON, serverConfig.CursorLimitJSON, serverConfig.DeploymentModeJSON:
			if checkNamespaceConfigBeingUsed {
				continue
			}

			checkNamespaceConfigBeingUsed = true
			m.checkFunctionUsingConfig(runtimeInfo, keyspaceInfo)
			if runtimeInfo.ErrCode != response.Ok {
				return
			}
		}
	}
}

func (m *serviceMgr) checkFunctionUsingConfig(runtimeInfo *response.RuntimeInfo, changingKeyspaceInfo application.KeyspaceInfo) {
	// This is called on the master node so apps latest status or change of status will be there on this node
	// So no need to do agg status check

	// check if any function which gonna use the current change is deployed or not. If deployed then return error
	appLocations := m.appManager.ListApplication()
	for _, appLocation := range appLocations {
		state, err := m.appState.GetAppState(appLocation)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInternalServer
			runtimeInfo.Description = "sync issue. Retry later"
			return
		}

		if !state.IsDeployed() {
			continue
		}

		appKeyspaceInfo, err := m.namespaceToKeyspaceInfo(appLocation.Namespace)
		if err != nil {
			continue
		}
		if m.serverConfig.WillItChange(changingKeyspaceInfo, appKeyspaceInfo) {
			runtimeInfo.ErrCode = response.ErrInvalidRequest
			runtimeInfo.Description = "Some application already using the old config. Undeploy/pause these functions to save config"
			return
		}
	}
}
