package servicemanager2

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/authenticator/rbac"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/parser"
	"github.com/couchbase/eventing/service_manager2/response"
)

const (
	APPROX_METRIC_COUNT  = 27
	APPROX_METRIC_SIZE   = 50
	METRICS_PREFIX       = "eventing_"
	prometheus_stats_str = "%v%v{functionName=\"%s\"} %v\n"
)

var (
	functions             = regexp.MustCompile("^/api/v1/functions/?$")
	functionsName         = regexp.MustCompile("^/api/v1/functions/(.*[^/])/?$") // Match is agnostic of trailing '/'
	functionsNameSettings = regexp.MustCompile("^/api/v1/functions/(.*[^/])/settings/?$")
	functionsDeploy       = regexp.MustCompile("^/api/v1/functions/(.*[^/])/deploy/?$")
	functionsUndeploy     = regexp.MustCompile("^/api/v1/functions/(.*[^/])/undeploy/?$")
	functionsPause        = regexp.MustCompile("^/api/v1/functions/(.*[^/])/pause/?$")
	functionsResume       = regexp.MustCompile("^/api/v1/functions/(.*[^/])/resume/?$")
	functionsAppcode      = regexp.MustCompile("^/api/v1/functions/(.*[^/])/appcode(/checksum)?/?$")
	functionsConfig       = regexp.MustCompile("^/api/v1/functions/(.*[^/])/config/?$")

	singleFuncStatusPattern = regexp.MustCompile("^/api/v1/status/(.*[^/])/?$") // Match is agnostic of trailing '/'
	statsFuncRegex          = regexp.MustCompile("^/api/v1/stats/(.*[^/])/?$")
)

// Clears up all Eventing related artifacts from metakv, typically will be
// used for rebalance tests. Only admin can do this
func (m *serviceMgr) cleanupEventing(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventCleanupEventing)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	cleanupEventingMetaKvPath()
	runtimeInfo.Description = "Successfully cleaned up eventing"
}

func (m *serviceMgr) clearEventStats(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventClearStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	appLocation := application.GetApplocation(params)
	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowed(r, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	err := m.clearStats(params)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}
	runtimeInfo.ExtraAttributes = map[string]interface{}{common.AppLocationsTag: []string{appLocation.String()}}
	runtimeInfo.Description = "Done resetting counters"
}

func (m *serviceMgr) clearNodeEventStats(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventClearStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	appLocation := application.GetApplocation(params)
	res.AddRequestData(common.AppLocationTag, appLocation.String())
	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowed(r, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	// check if app exist or not
	if err := m.superSup.ClearStats(appLocation); err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.SendRawDescription = true
		runtimeInfo.Description = fmt.Sprintf(`{"error":"Failed to reset counters error: %v"}`, err)
		return
	}

	runtimeInfo.ExtraAttributes = map[string]interface{}{common.AppLocationsTag: []string{appLocation.String()}}
	runtimeInfo.Description = "Done resetting counters"
}

// GA functions
func (m *serviceMgr) statusHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetFunctionStatus)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET is allowed"
		return
	}

	var tempAppList []application.AppLocation
	isSingleAppStatus := false

	if match := singleFuncStatusPattern.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		// api/v1/status/<function_name> type URL for checking status of a single function
		appLocation := application.GetApplocation(r.URL.Query())
		res.AddRequestData(common.AppLocationTag, appLocation.String())
		appLocation.Appname = match[1]
		tempAppList = append(tempAppList, appLocation)
		isSingleAppStatus = true
	} else {
		params := r.URL.Query()
		if appNames, ok := params["appNames"]; ok {
			tempAppList = make([]application.AppLocation, 0, len(appNames))
			for _, appName := range appNames {
				tempAppList = append(tempAppList, application.StringToAppLocation(appName))
			}
		} else {
			tempAppList = m.appManager.ListApplication()
		}
	}

	appLocationList := make([]application.AppLocation, 0, len(tempAppList))
	for _, appLocation := range tempAppList {
		perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
		if _, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
			continue
		}
		appLocationList = append(appLocationList, appLocation)
	}

	status := m.statusImpl(runtimeInfo, appLocationList, isSingleAppStatus)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	runtimeInfo.Description = status
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) statusImpl(runtimeInfo *response.RuntimeInfo,
	appLocation []application.AppLocation, isSingleAppStatus bool) (resp interface{}) {
	aggStatus, numEventingNode := m.getAggAppsStatus(runtimeInfo, appLocation)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	statusList := make([]*common.AppStatus, 0, len(appLocation))
	for _, appStatus := range aggStatus {
		statusList = append(statusList, appStatus)
	}

	if isSingleAppStatus && len(statusList) == 1 {
		resp = common.SingleAppStatusResponse{
			App:              statusList[0],
			NumEventingNodes: numEventingNode,
		}
		return
	}

	resp = common.AppStatusResponse{
		Apps:             statusList,
		NumEventingNodes: numEventingNode,
	}

	return
}

func (m *serviceMgr) statsHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventFetchStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {

		if r := recover(); r != nil {
			logging.Infof("Recovered in statsHandler, reason: %v stack: %v", r, string(debug.Stack()))
			return
		}
		res.LogAndSend(runtimeInfo)
	}()

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET is allowed"
		return
	}

	query := r.URL.Query()
	res.AddRequestData("query", query)
	statType := common.PartialStats
	if val, ok := query["type"]; ok {
		if len(val) > 0 {
			switch val[0] {
			case "full":
				statType = common.FullStats
				// For backward compatibility use type=full&debug=true
				// This will make sure that older version will get full stats and new version will get full debug stats in cbcollect
				if val, ok := query["debug"]; ok {
					if len(val) > 0 && val[0] == "true" {
						statType = common.FullDebugStats
					}
				}

			case "debug":
				statType = common.FullDebugStats
			}
		}
	}

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	runtimeInfo.OnlyDescription = true
	singleStat := false
	appLocationList := make([]application.AppLocation, 0, 1)
	if match := statsFuncRegex.FindStringSubmatch(r.URL.Path); len(match) > 0 {
		appLocation := application.GetApplocation(query)
		appLocation.Appname = match[1]
		perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
		if _, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
			runtimeInfo.Description = &common.Stats{}
			return
		}
		appLocationList = append(appLocationList, appLocation)
		res.AddRequestData(common.AppLocationTag, appLocation.String())
		singleStat = true
	} else {
		tempAppList := m.appManager.ListApplication()
		for _, appLocation := range tempAppList {
			perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
			if _, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
				continue
			}
			appLocationList = append(appLocationList, appLocation)
		}
	}

	statsList := m.populateStats(appLocationList, statType)
	if singleStat && len(statsList) > 0 {
		runtimeInfo.Description = statsList[0]
	} else {
		runtimeInfo.Description = statsList
	}
}

func (m *serviceMgr) populateStats(appLocations []application.AppLocation, statsType common.StatsType) []*common.Stats {
	statsList := make([]*common.Stats, 0, len(appLocations))

	for _, location := range appLocations {
		stat, err := m.superSup.GetStats(location, statsType)
		if stat == nil || err != nil {
			continue
		}
		stat.LcbCredsRequestCounter = m.globalStatsCounter.LcbCredsStats.Load()
		stat.GoCbCredsRequestCounter = m.globalStatsCounter.GocbCredsStats.Load()

		statsList = append(statsList, stat)
	}
	return statsList
}

func (m *serviceMgr) configHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventSaveConfig)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if r.Method != http.MethodGet {
		allowed := m.superSup.LifeCycleOperationAllowed()
		if !allowed {
			runtimeInfo.ErrCode = response.ErrMixedMode
			runtimeInfo.Description = "config cannot be changed in mixed mode"
			return
		}
	}

	params := r.URL.Query()
	namespace, _ := application.GetNamespace(params, true)
	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: namespace})
	if notAllowed, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	switch r.Method {
	case http.MethodGet:
		res.SetRequestEvent(response.EventGetConfig)
		keyspaceInfo, err := m.namespaceToKeyspaceInfo(namespace)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInvalidRequest
			runtimeInfo.Description = fmt.Sprintf("%s", err)
			return
		}
		_, c := m.serverConfig.GetServerConfig(keyspaceInfo)
		runtimeInfo.Description = c
		runtimeInfo.OnlyDescription = true

	case http.MethodPost:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrReadReq
			runtimeInfo.Description = fmt.Sprintf("failed to read request body, err: %v", err)
			return
		}
		res.AddRequestData("body", string(data))

		m.checkAndSaveConfig(cred, runtimeInfo, namespace, data)
		if runtimeInfo.ErrCode != response.Ok {
			return
		}

		runtimeInfo.Description = fmt.Sprintf("Config saved for %s", namespace)

	case http.MethodDelete:
		m.deleteConfig(cred, runtimeInfo, namespace)
		if runtimeInfo.ErrCode != response.Ok {
			return
		}

		runtimeInfo.Description = fmt.Sprintf("Config deleted for %s", namespace)

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET, POST and DELETE are allowed"
		return
	}
}

func (m *serviceMgr) functionsHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "serviceMgr::functionsHandler"

	params := r.URL.Query()
	appLocation := application.GetApplocation(params)

	if r.Method != http.MethodGet {
		logging.Infof("%s Request: %v query: %v method: %v", logPrefix, r.URL.Path, params, r.Method)
	}

	if match := functionsNameSettings.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		appLocation.Appname = match[1]
		m.functionNameSettings(w, r, appLocation)

	} else if match := functionsAppcode.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		appLocation.Appname = match[1]
		m.functionAppcode(w, r, appLocation, match[2])

	} else if match := functionsConfig.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		appLocation.Appname = match[1]
		m.functionConfig(w, r, appLocation)

	} else if match := functionsPause.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		appLocation.Appname = match[1]
		m.functionPause(w, r, appLocation)

	} else if match := functionsResume.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		appLocation.Appname = match[1]
		m.functionResume(w, r, appLocation)

	} else if match := functionsDeploy.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		appLocation.Appname = match[1]
		m.functionDeploy(w, r, appLocation)

	} else if match := functionsUndeploy.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		appLocation.Appname = match[1]
		m.functionUndeploy(w, r, appLocation)

	} else if match := functionsName.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		appLocation.Appname = match[1]
		m.functionName(w, r, appLocation)

	} else if match := functions.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		m.functions(w, r)
	}
}

func (m *serviceMgr) functionNameSettings(w http.ResponseWriter, r *http.Request,
	appLocation application.AppLocation) {
	res := response.NewResponseWriter(w, r, response.EventUpdateFunction)
	res.AddRequestData(common.AppLocationTag, appLocation.String())
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	if r.Method != http.MethodGet {
		allowed := m.superSup.LifeCycleOperationAllowed()
		if !allowed {
			runtimeInfo.ErrCode = response.ErrMixedMode
			runtimeInfo.Description = "lifecycle operation not possible in mixed mode"
			return
		}
	}

	switch r.Method {
	case http.MethodGet:
		res.SetRequestEvent(response.EventGetFunctionDraft)
		funcDetails, ok := m.appManager.GetApplication(appLocation, false)
		if !ok {
			runtimeInfo.ErrCode = response.ErrAppNotFoundTs
			runtimeInfo.Description = fmt.Sprintf("Function: %s not found", appLocation)
			return
		}

		runtimeInfo.Description = funcDetails.Settings
		runtimeInfo.OnlyDescription = true

	case http.MethodPost:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrReadReq
			runtimeInfo.Description = fmt.Sprintf("failed to read request body, err : %v", err)
			return
		}

		settings := make(map[string]interface{})
		err = json.Unmarshal(data, &settings)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrUnmarshalPld
			runtimeInfo.Description = fmt.Sprintf("failed to unmarshal retry, err: %v", err)
			return
		}

		res.AddRequestData("body", settings)
		runtimeInfo = m.storeFunctionSettings(cred, appLocation, settings)
		if runtimeInfo.ErrCode != response.Ok {
			return
		}

		runtimeInfo.Description = fmt.Sprintf("Function: %s stored settings", appLocation)

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET and POST are allowed"
		return
	}
}

func (m *serviceMgr) functionAppcode(w http.ResponseWriter, r *http.Request,
	appLocation application.AppLocation, checksum string) {

	res := response.NewResponseWriter(w, r, response.EventGetFunctionDraft)
	res.AddRequestData(common.AppLocationTag, appLocation.String())
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	if r.Method != http.MethodGet {
		allowed := m.superSup.LifeCycleOperationAllowed()
		if !allowed {
			runtimeInfo.ErrCode = response.ErrMixedMode
			runtimeInfo.Description = "lifecycle operation not possible in mixed mode"
			return
		}
	}

	switch r.Method {
	case http.MethodGet:
		funcDetails, ok := m.appManager.GetApplication(appLocation, false)
		if !ok {
			runtimeInfo.ErrCode = response.ErrAppNotFoundTs
			runtimeInfo.Description = fmt.Sprintf("Function: %s not found", appLocation)
			return
		}

		response := []byte(funcDetails.AppCode)
		if checksum == "/checksum" {
			response = []byte(fmt.Sprintf("%x", sha256.Sum256(response)))
		}

		runtimeInfo.SendRawDescription = true
		runtimeInfo.Description = string(response)
		runtimeInfo.OnlyDescription = true

	case http.MethodPost:
		res.SetRequestEvent(response.EventUpdateFunction)
		data, err := io.ReadAll(r.Body)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrReadReq
			runtimeInfo.Description = fmt.Sprintf("Failed to read request body, err: %v", err)
			return
		}

		appCode := string(data)
		if checksum == "/checksum" {
			runtimeInfo.ErrCode = response.ErrMethodNotAllowed
			runtimeInfo.Description = "'/checksum' option is not allowed on a POST request"
			return
		}

		res.AddRequestData("body", appCode)
		runtimeInfo = m.storeFunctionCode(cred, appLocation, appCode)
		if runtimeInfo.ErrCode != response.Ok {
			return
		}

		deprecatedFnsList := parser.ListDeprecatedFunctions(appCode)
		overloadedFnsList := parser.ListOverloadedFunctions(appCode)
		warningList := make([]string, 0, 2)
		if len(deprecatedFnsList) > 0 {
			jsonList, _ := json.Marshal(deprecatedFnsList)
			warningList = append(warningList, fmt.Sprintf("It uses the following APIs that are Deprecated: %s", jsonList))
		}
		if len(overloadedFnsList) > 0 {
			jsonList, _ := json.Marshal(overloadedFnsList)
			warningList = append(warningList, fmt.Sprintf("The following built-in APIs are Overloaded: %s", jsonList))
		}
		if len(warningList) > 0 {
			runtimeInfo.WarningInfo = warningList
		}
		runtimeInfo.Description = fmt.Sprintf("Function: %s appcode stored in the metakv", appLocation)

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET and POST are allowed"
		return
	}
}

func (m *serviceMgr) functionConfig(w http.ResponseWriter, r *http.Request,
	appLocation application.AppLocation) {

	res := response.NewResponseWriter(w, r, response.EventGetFunctionDraft)
	res.AddRequestData(common.AppLocationTag, appLocation.String())
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	if r.Method != http.MethodGet {
		allowed := m.superSup.LifeCycleOperationAllowed()
		if !allowed {
			runtimeInfo.ErrCode = response.ErrMixedMode
			runtimeInfo.Description = "lifecycle operation not possible in mixed mode"
			return
		}
	}

	switch r.Method {
	case http.MethodGet:
		funcDetails, ok := m.appManager.GetApplication(appLocation, true)
		if !ok {
			runtimeInfo.ErrCode = response.ErrAppNotFoundTs
			runtimeInfo.Description = fmt.Sprintf("Function: %s not found", appLocation)
			return
		}

		// TODO: Maybe need old version of it
		runtimeInfo.Description = funcDetails.DeploymentConfig
		runtimeInfo.OnlyDescription = true

	case http.MethodPost:
		res.SetRequestEvent(response.EventUpdateFunction)
		data, err := io.ReadAll(r.Body)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrReadReq
			runtimeInfo.Description = fmt.Sprintf("Failed to read request body, err: %v", err)
			return
		}

		res.AddRequestData("body", string(data))
		depcfg, bindings, err := application.GetDeploymentConfig(data)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInvalidRequest
			runtimeInfo.Description = fmt.Sprintf("%v", err)
			return
		}

		runtimeInfo = m.storeFunctionBinding(cred, appLocation, depcfg, bindings)
		if runtimeInfo.ErrCode != response.Ok {
			return
		}

		runtimeInfo.Description = fmt.Sprintf("Function: %s stored deployment config", appLocation)

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET and POST are allowed"
		return
	}
}

func (m *serviceMgr) functionPause(w http.ResponseWriter, r *http.Request,
	appLocation application.AppLocation) {

	res := response.NewResponseWriter(w, r, response.EventPauseFunction)
	res.AddRequestData(common.AppLocationTag, appLocation.String())
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	allowed := m.superSup.LifeCycleOperationAllowed()
	if !allowed {
		runtimeInfo.ErrCode = response.ErrMixedMode
		runtimeInfo.Description = "lifecycle operation not possible in mixed mode"
		return
	}

	switch r.Method {
	case http.MethodPost:
		settings := map[string]interface{}{
			"deployment_status": true,
			"processing_status": false,
		}

		runtimeInfo = m.storeFunctionSettings(cred, appLocation, settings)
		if runtimeInfo.ErrCode != response.Ok {
			return
		}

		runtimeInfo.Description = fmt.Sprintf("Function: %s is Pausing", appLocation)

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET, POST and DELETE are allowed"
	}
}

func (m *serviceMgr) functionResume(w http.ResponseWriter, r *http.Request,
	appLocation application.AppLocation) {

	res := response.NewResponseWriter(w, r, response.EventResumeFunction)
	res.AddRequestData(common.AppLocationTag, appLocation.String())
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	allowed := m.superSup.LifeCycleOperationAllowed()
	if !allowed {
		runtimeInfo.ErrCode = response.ErrMixedMode
		runtimeInfo.Description = "lifecycle operation not possible in mixed mode"
		return
	}

	switch r.Method {
	case http.MethodPost:
		settings := map[string]interface{}{
			"deployment_status": true,
			"processing_status": true,
		}

		runtimeInfo = m.storeFunctionSettings(cred, appLocation, settings)
		if runtimeInfo.ErrCode != response.Ok {
			return
		}

		runtimeInfo.Description = fmt.Sprintf("Function: %s is Resuming", appLocation)

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET, POST and DELETE are allowed"
	}
}

func (m *serviceMgr) functionDeploy(w http.ResponseWriter, r *http.Request,
	appLocation application.AppLocation) {

	res := response.NewResponseWriter(w, r, response.EventDeployFunction)
	res.AddRequestData(common.AppLocationTag, appLocation.String())
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	allowed := m.superSup.LifeCycleOperationAllowed()
	if !allowed {
		runtimeInfo.ErrCode = response.ErrMixedMode
		runtimeInfo.Description = "lifecycle operation not possible in mixed mode"
		return
	}

	switch r.Method {
	case http.MethodPost:
		settings := map[string]interface{}{
			"deployment_status": true,
			"processing_status": true,
		}

		runtimeInfo = m.storeFunctionSettings(cred, appLocation, settings)
		if runtimeInfo.ErrCode != response.Ok {
			return
		}

		runtimeInfo.Description = fmt.Sprintf("Function: %s is deploying", appLocation)

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET, POST and DELETE are allowed"
	}
}

func (m *serviceMgr) functionUndeploy(w http.ResponseWriter, r *http.Request,
	appLocation application.AppLocation) {
	res := response.NewResponseWriter(w, r, response.EventUndeployFunction)
	res.AddRequestData(common.AppLocationTag, appLocation.String())
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	allowed := m.superSup.LifeCycleOperationAllowed()
	if !allowed {
		runtimeInfo.ErrCode = response.ErrMixedMode
		runtimeInfo.Description = "lifecycle operation not possible in mixed mode"
		return
	}

	switch r.Method {
	case http.MethodPost:
		settings := map[string]interface{}{
			"deployment_status": false,
			"processing_status": false,
		}

		runtimeInfo = m.storeFunctionSettings(cred, appLocation, settings)
		if runtimeInfo.ErrCode != response.Ok {
			return
		}

		runtimeInfo.Description = fmt.Sprintf("Function: %s is undeploying", appLocation)

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET, POST and DELETE are allowed"
	}
}

func (m *serviceMgr) functionName(w http.ResponseWriter, r *http.Request,
	appLocation application.AppLocation) {
	res := response.NewResponseWriter(w, r, response.EventUpdateFunction)
	res.AddRequestData(common.AppLocationTag, appLocation.String())
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	if r.Method != http.MethodGet {
		allowed := m.superSup.LifeCycleOperationAllowed()
		if !allowed {
			runtimeInfo.ErrCode = response.ErrMixedMode
			runtimeInfo.Description = "lifecycle operation not possible in mixed mode"
			return
		}
	}

	switch r.Method {
	case http.MethodGet:
		res.SetRequestEvent(response.EventGetFunctionDraft)
		application, err := m.appManager.GetAppMarshaler(appLocation, application.Version1)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrAppNotFoundTs
			runtimeInfo.Description = fmt.Sprintf("Function: %s not found", appLocation)
			return
		}

		runtimeInfo.Description = application
		runtimeInfo.OnlyDescription = true

	case http.MethodPost:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrReadReq
			runtimeInfo.Description = fmt.Sprintf("Failed to read request body, err: %v", err)
			return
		}
		data = bytes.Trim(data, "[]\n \t\r\f\v")

		res.AddRequestData("body", string(data))
		sb := application.StorageBytes{
			Body: data,
		}
		funcDetails, err := application.NewApplication(sb, application.RestApi)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInvalidRequest
			runtimeInfo.Description = fmt.Sprintf("%v", err)
			return
		}

		if application.AppLocationInQuery(r.URL.Query()) {
			funcDetails.AppLocation = appLocation
		}

		rInfo := m.storeFunction(cred, funcDetails)
		if rInfo.ErrCode != response.Ok {
			runtimeInfo = rInfo
			return
		}

		runtimeInfo.Description = "Successfully stored"

	case http.MethodDelete:
		params := r.URL.Query()

		handlerID := uint32(0)
		requestId, ok := params["handleruuid"]
		if ok {
			handlerIDUint64, err := strconv.ParseUint(requestId[0], 10, 32)
			if err != nil {
				runtimeInfo.ErrCode = response.ErrInvalidRequest
				return
			}
			handlerID = uint32(handlerIDUint64)
		}

		rInfo := m.deleteFunction(appLocation, handlerID)
		if rInfo.ErrCode != response.Ok {
			runtimeInfo = rInfo
			return
		}
		runtimeInfo.Description = "Succesfully deleted"

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET, POST and DELETE are allowed"
	}

}

func (m *serviceMgr) extractFunctionList(runtimeInfo *response.RuntimeInfo, cred cbauth.Creds, r *http.Request,
	filterMap map[string]bool, remap map[string]application.Keyspace,
	filterType string) (funcList []*application.FunctionDetails) {

	data, err := io.ReadAll(r.Body)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrReadReq
		runtimeInfo.Description = fmt.Sprintf("Failed to read request body, err: %v", err)
		return
	}

	sb := application.StorageBytes{
		Body: data,
	}

	tempFuncList, err := application.NewApplicationList(sb, application.RestApi)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%v", err)
		return
	}

	funcList = make([]*application.FunctionDetails, 0, len(tempFuncList))
	for _, app := range tempFuncList {
		perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: app.AppLocation.Namespace})
		if _, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
			continue
		}

		if filterType == "" {
			funcList = append(funcList, app)
		} else {
			if applyFilter(app, filterMap, filterType) {
				val, length, ok := remapContains(remap, app.AppLocation.Namespace.BucketName, app.AppLocation.Namespace.ScopeName, "")
				if ok {
					app.AppLocation.Namespace.BucketName = val.BucketName
					if length == 2 {
						app.AppLocation.Namespace.ScopeName = val.ScopeName
					}
				}

				deploymentConfig := app.DeploymentConfig
				val, length, ok = remapContains(remap, deploymentConfig.SourceKeyspace.BucketName, deploymentConfig.SourceKeyspace.ScopeName, deploymentConfig.SourceKeyspace.CollectionName)
				if ok {
					app.DeploymentConfig.SourceKeyspace.BucketName = val.BucketName
					if length == 2 {
						app.DeploymentConfig.SourceKeyspace.ScopeName = val.ScopeName
					}
					if length == 3 {
						app.DeploymentConfig.SourceKeyspace.ScopeName = val.ScopeName
						app.DeploymentConfig.SourceKeyspace.CollectionName = val.CollectionName
					}
				}

				val, length, ok = remapContains(remap, deploymentConfig.MetaKeyspace.BucketName, deploymentConfig.MetaKeyspace.ScopeName, deploymentConfig.MetaKeyspace.CollectionName)
				if ok {
					app.DeploymentConfig.MetaKeyspace.BucketName = val.BucketName
					if length == 2 {
						app.DeploymentConfig.MetaKeyspace.ScopeName = val.ScopeName
					}
					if length == 3 {
						app.DeploymentConfig.MetaKeyspace.ScopeName = val.ScopeName
						app.DeploymentConfig.MetaKeyspace.CollectionName = val.CollectionName
					}
				}

				for _, binding := range app.Bindings {
					if binding.BindingType == application.Bucket {
						val, length, ok = remapContains(remap, binding.BucketBinding.Keyspace.BucketName, binding.BucketBinding.Keyspace.ScopeName, binding.BucketBinding.Keyspace.CollectionName)
						if ok {
							binding.BucketBinding.Keyspace.BucketName = val.BucketName
							if length == 2 {
								binding.BucketBinding.Keyspace.ScopeName = val.ScopeName
							}
							if length == 3 {
								binding.BucketBinding.Keyspace.ScopeName = val.ScopeName
								binding.BucketBinding.Keyspace.CollectionName = val.CollectionName
							}
						}
					}
				}
				m.checkAndChangeName(app)
				funcList = append(funcList, app)
			}
		}
	}

	return
}

func (m *serviceMgr) functions(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventListAllfunction)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	if r.Method != http.MethodGet {
		allowed := m.superSup.LifeCycleOperationAllowed()
		if !allowed {
			runtimeInfo.ErrCode = response.ErrMixedMode
			runtimeInfo.Description = "lifecycle operation not possible in mixed mode"
			return
		}
	}

	switch r.Method {
	case http.MethodGet:
		appList := m.getApplication(cred, nil, "")

		runtimeInfo.Description = appList
		runtimeInfo.OnlyDescription = true

	case http.MethodPost:
		res.SetRequestEvent(response.EventImportFunctions)
		funcList := m.extractFunctionList(runtimeInfo, cred, r, nil, nil, "")
		if runtimeInfo.ErrCode != response.Ok {
			return
		}

		importedFuncs := m.storeFunctionList(cred, runtimeInfo, funcList)
		res.AddRequestData(common.AppLocationsTag, importedFuncs)
		runtimeInfo.ExtraAttributes = map[string]interface{}{common.AppLocationsTag: importedFuncs}

	case http.MethodDelete:
		res.SetRequestEvent(response.EventDeleteFunction)
		listApps := m.appManager.ListApplication()
		for _, appLocation := range listApps {
			perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
			if _, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
				continue
			}
			m.deleteFunction(appLocation, uint32(0))
		}

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET, POST and DELETE are allowed"
	}
}

func (m *serviceMgr) exportHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventExportFunctions)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET is allowed"
		return
	}

	// TODO: Maybe we can have per function export instead of all export
	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	runtimeInfo.Description = m.getApplication(cred, nil, "")
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) importHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventImportFunctions)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	if r.Method != "POST" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only POST is allowed"
		return
	}

	allowed := m.superSup.LifeCycleOperationAllowed()
	if !allowed {
		runtimeInfo.ErrCode = response.ErrMixedMode
		runtimeInfo.Description = "Import not allowed in mixed mode"
		return
	}

	funcList := m.extractFunctionList(runtimeInfo, cred, r, nil, nil, "")
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	importedFuncs := m.storeFunctionList(cred, runtimeInfo, funcList)
	res.AddRequestData(common.AppLocationsTag, importedFuncs)
	runtimeInfo.ExtraAttributes = map[string]interface{}{common.AppLocationsTag: importedFuncs}
}

func (m *serviceMgr) backupHandler(w http.ResponseWriter, r *http.Request) {
	url := filepath.Clean(r.URL.Path)

	res := response.NewResponseWriter(w, r, response.EventBackupFunctions)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	if r.Method != http.MethodGet {
		allowed := m.superSup.LifeCycleOperationAllowed()
		if !allowed {
			runtimeInfo.ErrCode = response.ErrMixedMode
			runtimeInfo.Description = "Restore not allowed in mixed mode"
			return
		}
	}

	req := strings.Split(url, "/")
	// eventing only allows cluster level backup/restore
	if len(req) > 4 {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("Only cluster level backup is allowed: request Url %s", url)
		return
	}

	params := r.URL.Query()
	res.AddRequestData("query", params)

	include := r.FormValue("include")
	exclude := r.FormValue("exclude")

	if len(include) != 0 && len(exclude) != 0 {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = "Only one include or exclude filter should be present"
		return
	}

	var filterMap map[string]bool
	var filterType string
	if len(include) != 0 {
		filterType = "include"
		filterMap, err = filterQueryMap(include, true)
	}

	if len(exclude) != 0 {
		filterType = "exclude"
		filterMap, err = filterQueryMap(exclude, false)
	}

	switch r.Method {
	case http.MethodGet:
		exportedFun := m.getApplication(cred, filterMap, filterType)
		runtimeInfo.ExtraAttributes = map[string]interface{}{common.AppLocationsTag: exportedFun}
		runtimeInfo.Description = exportedFun
		runtimeInfo.OnlyDescription = true

	case http.MethodPost:
		// call restore handler
		res.SetRequestEvent(response.EventRestoreFunctions)

		remap, err := getRestoreMap(r)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInvalidRequest
			runtimeInfo.Description = fmt.Sprintf("%s", err)
			return
		}

		funcList := m.extractFunctionList(runtimeInfo, cred, r, filterMap, remap, filterType)
		if runtimeInfo.ErrCode != response.Ok {
			return
		}
		res.AddRequestData(common.AppLocationsTag, funcList)
		importedFuncs := m.storeFunctionList(cred, runtimeInfo, funcList)
		runtimeInfo.ExtraAttributes = map[string]interface{}{common.AppLocationsTag: importedFuncs}

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET and POST are allowed"
		return
	}
}

func (m *serviceMgr) listFunctions(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventListAllfunction)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET is allowed"
		return
	}

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	params := r.URL.Query()
	res.AddRequestData("query", params)

	fnList := m.getFunctionList(cred, runtimeInfo, params)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	runtimeInfo.Description = struct {
		Functions []string `json:"functions"`
	}{
		Functions: fnList,
	}
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) prometheusLow(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventFetchStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingStatsPermission, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	// TODO: avg script execution time, avg timer scan time
	out := make([]byte, 0)
	out = append(out, []byte(fmt.Sprintf("eventing_worker_restart_count %v\n", 0))...)

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = string(out)
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) prometheusHigh(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventFetchStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingStatsPermission, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	appLocations := m.appManager.ListApplication()
	stats := make([]byte, 0, APPROX_METRIC_COUNT*APPROX_METRIC_SIZE*len(appLocations))
	for _, location := range appLocations {
		stat, err := m.superSup.GetStats(location, common.PrometheusStats)
		if stat == nil || err != nil {
			continue
		}

		stats = populate[uint64](location, "dcp_backlog", stats, stat.EventRemaining)

		stats = populate[interface{}](location, "agg_queue_memory", stats, stat.ExecutionStats)
		stats = populate[interface{}](location, "agg_queue_size", stats, stat.ExecutionStats)
		stats = populate[interface{}](location, "on_update_success", stats, stat.ExecutionStats)
		stats = populate[interface{}](location, "on_update_failure", stats, stat.ExecutionStats)
		stats = populate[interface{}](location, "dcp_delete_msg_counter", stats, stat.ExecutionStats)
		stats = populate[interface{}](location, "dcp_mutations_msg_counter", stats, stat.ExecutionStats)
		stats = populate[interface{}](location, "on_delete_success", stats, stat.ExecutionStats)
		stats = populate[interface{}](location, "on_delete_failure", stats, stat.ExecutionStats)
		stats = populate[interface{}](location, "timer_cancel_counter", stats, stat.ExecutionStats)
		stats = populate[interface{}](location, "timer_create_counter", stats, stat.ExecutionStats)
		stats = populate[interface{}](location, "timer_create_failure", stats, stat.ExecutionStats)
		stats = populate[interface{}](location, "timer_callback_success", stats, stat.ExecutionStats)
		stats = populate[interface{}](location, "timer_callback_failure", stats, stat.ExecutionStats)
		// The following metric tracks the total number of times a Timer callback is invoked.
		// => timer_msg_counter = timer_callback_missing_counter + timer_callback_success + timer_callback_failure.

		stats = populate[uint64](location, "dcp_mutation_sent_to_worker", stats, stat.EventProcessingStats)
		stats = populate[uint64](location, "dcp_mutation_suppressed_counter", stats, stat.EventProcessingStats)
		stats = populate[uint64](location, "dcp_deletion_sent_to_worker", stats, stat.EventProcessingStats)
		stats = populate[uint64](location, "dcp_expiry_sent_to_worker", stats, stat.EventProcessingStats)
		stats = populate[uint64](location, "dcp_deletion_suppressed_counter", stats, stat.EventProcessingStats)
		stats = populate[uint64](location, "worker_spawn_counter", stats, stat.EventProcessingStats)

		stats = populate[interface{}](location, "bucket_op_exception_count", stats, stat.FailureStats)
		stats = populate[interface{}](location, "timeout_count", stats, stat.FailureStats)
		stats = populate[interface{}](location, "n1ql_op_exception_count", stats, stat.FailureStats)
		stats = populate[interface{}](location, "timer_context_size_exception_counter", stats, stat.FailureStats)
		stats = populate[interface{}](location, "timer_callback_missing_counter", stats, stat.FailureStats)
		stats = populate[interface{}](location, "bkt_ops_cas_mismatch_count", stats, stat.FailureStats)
		stats = populate[interface{}](location, "checkpoint_failure_count", stats, stat.FailureStats)
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = string(stats)
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) startDebugger(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventStartDebugger)
	runtimeInfo := &response.RuntimeInfo{}
	defer res.LogAndSend(runtimeInfo)

	appLocation, _ := m.debuggerOp(runtimeInfo, r, common.StartDebuggerOp)
	res.AddRequestData(common.AppLocationTag, appLocation)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	runtimeInfo.ExtraAttributes = r.URL.Query()
	runtimeInfo.Description = fmt.Sprintf("Function: %s Started Debugger", appLocation)
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getDebuugerUrl(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetDebuggerUrl)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	appLocation, debugURL := m.debuggerOp(runtimeInfo, r, common.GetDebuggerURl)
	res.AddRequestData(common.AppLocationTag, appLocation)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = strings.Replace(debugURL, "[::1]", "127.0.0.1", -1)
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) stopDebugger(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventStopDebugger)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	appLocation, _ := m.debuggerOp(runtimeInfo, r, common.StopDebuggerOp)
	res.AddRequestData(common.AppLocationTag, appLocation)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	runtimeInfo.ExtraAttributes = r.URL.Query()
	runtimeInfo.Description = fmt.Sprintf("Function: %s stopped Debugger", appLocation)
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) debuggerOp(runtimeInfo *response.RuntimeInfo, r *http.Request, debuggerOp common.DebuggerOp) (appLocation application.AppLocation, token string) {
	params := r.URL.Query()
	if !application.AppLocationInQuery(params) {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = "applocation should be provided"
		return
	}

	appLocation = application.GetApplocation(params)

	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowed(r, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	if !m.superSup.LifeCycleOperationAllowed() {
		runtimeInfo.ErrCode = response.ErrMixedMode
		runtimeInfo.Description = "Not able to start debugger"
		return
	}

	keyspaceInfo, err := m.namespaceToKeyspaceInfo(appLocation.Namespace)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%s", err)
		return
	}
	_, config := m.serverConfig.GetServerConfig(keyspaceInfo)
	if !config.EnableDebugger {
		runtimeInfo.ErrCode = response.ErrDebuggerDisabled
		runtimeInfo.Description = "Debugger is not enabled"
		return
	}

	aggStatus, _ := m.getAggAppsStatus(runtimeInfo, []application.AppLocation{appLocation})
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	if status, ok := aggStatus[appLocation.String()]; !ok {
		runtimeInfo.ErrCode = response.ErrAppNotFound
		runtimeInfo.Description = fmt.Sprintf("%s not found", appLocation)
		return
	} else if application.StringToAppState(status.CompositeStatus) != application.Deployed {
		runtimeInfo.ErrCode = response.ErrAppNotDeployed
		runtimeInfo.Description = fmt.Sprintf("Function: %s is not in deployed state, debugger cannot start", appLocation)
		return
	}

	funcDetails, ok := m.appManager.GetApplication(appLocation, false)
	if !ok {
		runtimeInfo.ErrCode = response.ErrAppNotFound
		runtimeInfo.Description = fmt.Sprintf("%s not found", appLocation)
		return
	}
	token, err = m.superSup.DebuggerOp(debuggerOp, funcDetails, nil)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}

	return appLocation, token
}

func (m *serviceMgr) logFileLocation(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetLogFileLocation)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed, Only Get request is allowed"
		return
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = fmt.Sprintf(`{"log_dir":"%v"}`, m.config.EventingDir)
	runtimeInfo.OnlyDescription = true
}
