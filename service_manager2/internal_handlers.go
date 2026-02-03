package servicemanager2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"

	stateMachine "github.com/couchbase/eventing/app_manager/app_state_machine"
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/authenticator/rbac"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/notifier"
	"github.com/couchbase/eventing/parser"
	pc "github.com/couchbase/eventing/point_connection"
	"github.com/couchbase/eventing/service_manager2/response"
)

type annotation struct {
	Name            string                `json:"name"`
	FunctionScope   application.Namespace `json:"function_scope"`
	DeprecatedNames []string              `json:"deprecatedNames,omitempty"`
	OverloadedNames []string              `json:"overloadedNames,omitempty"`
}

func (m *serviceMgr) getAggBootstrapStatusHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetBootstrapStatus)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	req := &pc.Request{
		Method: pc.GET,
	}

	resBytes, _, err := m.broadcastRequestGlobal("/getBootstrapStatus", req)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}

	status := false
	for _, buf := range resBytes {
		status, err = strconv.ParseBool(string(buf))
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInternalServer
			return
		}

		if status {
			break
		}
	}

	runtimeInfo.Description = status
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getRunningApps(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRunningApps)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	applications := m.appManager.ListApplication()
	aggStatus, _ := m.getAggAppsStatus(runtimeInfo, applications)
	if runtimeInfo.ErrCode != response.Ok {
		return
	}

	runningApps := make(map[string]string)
	for _, appLocation := range applications {
		perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
		if _, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
			continue
		}
		status, ok := aggStatus[appLocation.ToLocationString()]
		if !ok {
			continue
		}

		state := application.StringToAppState(status.CompositeStatus)
		if state.IsBootstrapping() || state.IsDeployed() {
			runningApps[appLocation.ToLocationString()] = ""
		}
	}

	runtimeInfo.Description = runningApps
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getAggBootstrapAppStatusHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetBootstrapStatus)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	req := &pc.Request{
		Method: pc.GET,
	}

	req.Query = r.URL.Query()
	resBytes, _, err := m.broadcastRequestGlobal("/getBootstrapAppStatus", req)

	status := false
	for _, buf := range resBytes {
		// TODO: possible that app doesn't exis in that case return appropriate error
		status, err = strconv.ParseBool(string(buf))
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInternalServer
			return
		}

		if status {
			break
		}
	}
	runtimeInfo.Description = status
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getAggDeployedAppsHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetDeployedApps)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	req := &pc.Request{
		Method: pc.GET,
	}

	resBytes, _, err := m.broadcastRequestGlobal("/getLocallyDeployedApps", req)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}

	reqCount := len(resBytes)
	countApps := make(map[string]int)

	for _, resp := range resBytes {
		resMap := make(map[string]string)
		err := json.Unmarshal(resp, &resMap)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInternalServer
			return
		}

		for appLocation, _ := range resMap {
			countApps[appLocation]++
		}
	}

	deployedApps := make(map[string]string)
	for appLocation, count := range countApps {
		if count == reqCount {
			deployedApps[appLocation] = ""
		}
	}

	runtimeInfo.Description = deployedApps
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getAggRebalanceProgress(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRebalanceProgress)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	req := &pc.Request{
		Method: pc.GET,
		Query:  r.URL.Query(),
	}

	resBytes, _, err := m.broadcastRequestGlobal("/getRebalanceProgress", req)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}

	progressMap := make(map[string]float64)
	for _, resp := range resBytes {
		resMap := make(map[string]float64)
		json.Unmarshal(resp, &resMap)
		for appLocation := range resMap {
			// Maybe add progress instead of just count
			progressMap[appLocation]++
		}
	}

	runtimeInfo.Description = progressMap
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getAggRebalanceStatus(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRebalanceStatus)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	progress, err := m.superSup.GetGlobalRebalanceProgress("")
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = strconv.FormatBool(progress == float64(0))
	runtimeInfo.OnlyDescription = true
}

type rebalanceProgress struct {
	CloseStreamVbsLen     int
	StreamReqVbsLen       int
	VbsRemainingToShuffle int
	VbsOwnedPerPlan       int
	NodeLevelStats        interface{}
}

// Local functions
func (m *serviceMgr) getRebalanceProgress(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRebalanceProgress)
	runtimeInfo := &response.RuntimeInfo{}
	params := r.URL.Query()

	changeID := ""
	changeIDList := params[common.QueryVbMapVersion]
	if len(changeIDList) != 0 {
		changeID = changeIDList[0]
	}

	var err error
	newVersion := false
	newVersionList := params[common.NewResponse]
	if len(newVersionList) != 0 {
		newVersion, err = strconv.ParseBool(newVersionList[0])
		if err != nil {
			newVersion = false
		}
	}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	appLocationList := m.appManager.ListApplication()
	progressMap := make(map[string]int)
	for _, appLocation := range appLocationList {
		perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
		if _, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
			continue
		}

		progress := m.superSup.RebalanceProgress(changeID, appLocation)
		if progress.RebalanceInProgress {
			progressMap[appLocation.String()] = 0
		}
	}

	runtimeInfo.Description = progressMap
	if !newVersion {
		vbsToShuffle := 0
		if len(progressMap) != 0 {
			vbsToShuffle = len(progressMap)
		}
		rProgress := rebalanceProgress{
			CloseStreamVbsLen:     0,
			StreamReqVbsLen:       0,
			VbsRemainingToShuffle: vbsToShuffle,
			VbsOwnedPerPlan:       vbsToShuffle,
		}
		runtimeInfo.Description = rProgress
	}

	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getOwnedVbsForApp(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRebalanceProgress)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	params := r.URL.Query()
	appLocation := application.GetApplocation(params)

	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowed(r, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	progress := m.superSup.RebalanceProgress("", appLocation)
	runtimeInfo.Description = progress
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getOwnershipMap(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRebalanceProgress)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	runtimeInfo.Description = m.superSup.GetOwnershipDetails()
	runtimeInfo.OnlyDescription = true
	runtimeInfo.SendRawDescription = true
}

func (m *serviceMgr) getAnnotations(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetAnnotations)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	appLocationList := m.appManager.ListApplication()
	// TODO: Optimise this
	respData := make([]annotation, 0, len(appLocationList))
	for _, appLocation := range appLocationList {
		app, ok := m.appManager.GetApplication(appLocation, false)
		if !ok {
			continue
		}

		perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
		if _, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
			continue
		}

		respObj := annotation{
			Name:            appLocation.Appname,
			FunctionScope:   appLocation.Namespace,
			DeprecatedNames: parser.ListDeprecatedFunctions(app.AppCode),
			OverloadedNames: parser.ListOverloadedFunctions(app.AppCode),
		}
		respData = append(respData, respObj)
	}

	runtimeInfo.OnlyDescription = true
	runtimeInfo.Description = respData
}

const (
	defaultLogSize = int64(40960)
)

func (m *serviceMgr) getAppLog(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetAppLog)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed, Only Get request is allowed"
		return
	}

	params := r.URL.Query()
	res.AddRequestData("query", params)
	appLocation := application.GetApplocation(params)
	res.AddRequestData(common.AppLocationTag, appLocation.String())

	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowed(r, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	if !m.appManager.IsAppPresent(appLocation) {
		runtimeInfo.ErrCode = response.ErrAppNotFoundTs
		runtimeInfo.Description = fmt.Sprintf("Function: %s not found", appLocation)
		return
	}
	sz := defaultLogSize

	sv := params["size"]
	if len(sv) == 1 {
		psz, err := strconv.Atoi(sv[0])
		if err == nil {
			sz = int64(psz)
		}
	}

	var lines []string
	var err error
	if rv := params["aggregate"]; len(rv) > 0 && rv[0] == "true" {
		lines, err = m.getGlobalAppLog(appLocation, sz)
	} else {
		lines, err = m.getLocalAppLog(appLocation, sz)
	}
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = "Unable to get app logs"
		return
	}

	sort.Sort(sort.Reverse(sort.StringSlice(lines)))

	runtimeInfo.ContentType = "text/plain"
	runtimeInfo.SendRawDescription = true
	runtimeInfo.OnlyDescription = true
	runtimeInfo.Description = strings.Join(lines, "\n")
}

func (m *serviceMgr) getLocalAppLog(appLocation application.AppLocation, size int64) ([]string, error) {
	return m.superSup.GetApplicationLog(appLocation, size)
}

func (m *serviceMgr) getGlobalAppLog(appLocation application.AppLocation, size int64) ([]string, error) {
	query := application.QueryMap(appLocation)
	query["size"] = []string{fmt.Sprintf("%d", size)}

	req := &pc.Request{
		Method: pc.GET,
		Query:  query,
	}

	namespace := appLocation.Namespace
	res, _, err := m.broadcastRequest(namespace, "/getAppLog", req)
	if err != nil {
		return nil, err
	}

	lines := make([]string, 0, size)
	for _, msgBytes := range res {
		msgs := bytes.Split(msgBytes, []byte("\n"))
		for _, msg := range msgs {
			msg = bytes.Trim(msg, "\r")
			if len(msg) > 0 {
				lines = append(lines, string(msg))
			}
		}
	}

	return lines, nil
}

// TODO: Modify this to get the whole system insight based on user permission
func (m *serviceMgr) getInsight(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetInsight)
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
		runtimeInfo.Description = "Method not allowed, Only Get request is allowed"
		return
	}

	params := r.URL.Query()
	var possibleApplocations []application.AppLocation
	if application.AppLocationInQuery(params) {
		possibleApplocations = append(possibleApplocations, application.GetApplocation(params))
	} else {
		// Need applocation for all the system wide function
		possibleApplocations = m.appManager.ListApplication()
	}

	appLocationList := make([]application.AppLocation, 0, len(possibleApplocations))
	for _, appLocation := range possibleApplocations {
		perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
		if _, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
			continue
		}
		appLocationList = append(appLocationList, appLocation)
	}

	var insight map[string]*common.Insight
	if len(appLocationList) != 0 {
		if rv := params["aggregate"]; len(rv) > 0 && rv[0] == "true" {
			insight = m.getGlobalInsights(appLocationList)
		} else {
			insight = m.getLocalInsights(appLocationList)
		}
	}

	runtimeInfo.Description = insight
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getLocalInsights(appLocations []application.AppLocation) map[string]*common.Insight {
	insightsMaps := make(map[string]*common.Insight)
	for _, appLocation := range appLocations {
		insightsMaps[appLocation.ToLocationString()] = m.superSup.GetInsights(appLocation)
	}
	return insightsMaps
}

func (m *serviceMgr) getGlobalInsights(appLocations []application.AppLocation) map[string]*common.Insight {
	query := make(map[string][]string)
	if len(appLocations) == 1 {
		query = application.QueryMap(appLocations[0])
	}

	req := &pc.Request{
		Method: pc.GET,
		Query:  query,
	}

	res, _, err := m.broadcastRequestGlobal("/getInsight", req)
	if err != nil {
		return nil
	}

	insightsMaps := make(map[string]*common.Insight)
	for _, msgBytes := range res {
		nInsightMap := make(map[string]*common.Insight)
		err := json.Unmarshal(msgBytes, &nInsightMap)
		if err != nil {
			continue
		}
		for funcString, nInsight := range nInsightMap {
			insight, ok := insightsMaps[funcString]
			if !ok {
				insight = common.NewInsight()
				insightsMaps[funcString] = insight
			}
			insight.Accumulate(nInsight)
		}
	}

	return insightsMaps
}

func (m *serviceMgr) getApplicationHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetFunctionDraft)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	runtimeInfo.Description = m.getApplication(cred, nil, "")
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getBootstrappingAppsHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetBootstrappingApps)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	runtimeInfo.Description = m.getLocalBootstrappingApps()
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getBootstrapStatusHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetBootstrappingApps)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	bootstrappingList := m.getLocalBootstrappingApps()
	bootstrappingStatus := (len(bootstrappingList) > 0)

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = strconv.FormatBool(bootstrappingStatus)
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getBootstrapAppStatusHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetBootstrapStatus)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	appLocation := application.GetApplocation(r.URL.Query())
	res.AddRequestData(common.AppLocationTag, appLocation)
	perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
	if notAllowed, err := rbac.IsAllowed(r, perms, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	state, err := m.appState.GetAppState(appLocation)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%s", err)
		return
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = strconv.FormatBool(state.IsBootstrapping())
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getLocallyDeployedAppsHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetDeployedApps)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	runtimeInfo.Description = m.getLocalAppsWithStatus(application.Deployed)
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getPausingAppsHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetPausingApps)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	runtimeInfo.Description = m.getLocalAppsWithStatus(application.Pausing)
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getCPUCount(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetCpuCount)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingAnyManageReadPermissions, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	runtimeInfo.Description = runtime.GOMAXPROCS(-1)
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getErrCodes(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetErrCodes)
	runtimeInfo := &response.RuntimeInfo{}
	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if _, err := rbac.AuthWebCreds(r); err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	runtimeInfo.Description = response.GetErrCodes()
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getStatus(w http.ResponseWriter, r *http.Request) {
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

	result := make(map[string]stateMachine.AppState)

	var appLocations []application.AppLocation
	appLocationString, ok := r.URL.Query()["appNames"]
	if !ok {
		appLocations = m.appManager.ListApplication()
	} else {
		appLocations = make([]application.AppLocation, 0, len(appLocationString))
		for _, locationString := range appLocationString {
			location := application.StringToAppLocation(locationString)
			appLocations = append(appLocations, location)
		}
	}

	for _, appLocation := range appLocations {
		perms := rbac.HandlerManagePermissions(application.Keyspace{Namespace: appLocation.Namespace})
		if _, err := rbac.IsAllowedCreds(cred, perms, false); err != nil {
			continue
		}

		state, err := m.appState.GetAppState(appLocation)
		if err != nil {
			continue
		}
		result[appLocation.String()] = state
	}

	runtimeInfo.Description = result
	runtimeInfo.OnlyDescription = true
}

func (m *serviceMgr) getWorkerCount(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetWorkerCount)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingAnyManageReadPermissions, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	// TODO: Add alternate way to detect any sizing issue and worker counts
	runtimeInfo.Description = "0\n"
	runtimeInfo.OnlyDescription = true
}

/*
TODO: complete this to redistribute the vbs for function
func (m *serviceMgr) distributeTenantOwnership(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventResdistributeWorkload)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	if r.Method != http.MethodPost {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("Only POST request is allowed")
		return
	}

	params := r.URL.Query()
	query := params[common.TenantID]
	if len(query) == 0 {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("'%s' is not provided", common.TenantID)
		return
	}

	tenantID := query[0]
	err := m.superSup.AssignOwnership(tenantID)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("%s", err)
		return
	}
	runtimeInfo.Description = fmt.Sprintf("Successfully allocated")
	runtimeInfo.OnlyDescription = true
}
*/

type userPermissions struct {
	FuncScope     []application.Keyspace `json:"func_scope"`
	ReadPerm      []application.Keyspace `json:"read_permission"`
	WritePerm     []application.Keyspace `json:"write_permission"`
	ReadWritePerm []application.Keyspace `json:"read_write_permission"`
	DcpStreamPerm []application.Keyspace `json:"dcp_stream_permission"`
}

func (m *serviceMgr) getUserInfoHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetUserInfo)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		getAuthErrorInfo(runtimeInfo, nil, false, err)
		return
	}

	u := &userPermissions{
		FuncScope:     make([]application.Keyspace, 0),
		ReadPerm:      make([]application.Keyspace, 0),
		WritePerm:     make([]application.Keyspace, 0),
		ReadWritePerm: make([]application.Keyspace, 0),
		DcpStreamPerm: make([]application.Keyspace, 0),
	}

	ie := notifier.InterestedEvent{
		Event: notifier.EventBucketListChanges,
	}
	bucketlistInterface, err := m.observer.GetCurrentState(ie)
	if err != nil {
		return
	}
	bucketList := bucketlistInterface.(map[string]string)

	ie.Event = notifier.EventScopeOrCollectionChanges
	k, _ := application.NewKeyspace("*", "*", "*", true)

	manage := rbac.GetPermissions(k, rbac.EventingManage)
	if _, err := rbac.IsAllowedCreds(cred, manage, false); err == nil {
		u.FuncScope = append(u.FuncScope, k)
	}

	// bucket.*.*
	for bucketName, _ := range bucketList {
		// checkAndAppendPermissions is a helper to check and add permissions for a given keyspace.
		checkAndAppendPermissions := func(k application.Keyspace, checkManage bool) {
			if checkManage {
				manage = rbac.GetPermissions(k, rbac.EventingManage)
				if _, err := rbac.IsAllowedCreds(cred, manage, false); err == nil {
					u.FuncScope = append(u.FuncScope, k)
				}
			}

			manage = rbac.GetPermissions(k, rbac.BucketDcp)
			if _, err := rbac.IsAllowedCreds(cred, manage, true); err == nil {
				u.DcpStreamPerm = append(u.DcpStreamPerm, k)
			}

			manage = rbac.GetPermissions(k, rbac.BucketRead)
			read := false
			if _, err := rbac.IsAllowedCreds(cred, manage, true); err == nil {
				u.ReadPerm = append(u.ReadPerm, k)
				read = true
			}

			manage = rbac.GetPermissions(k, rbac.BucketWrite)
			if _, err := rbac.IsAllowedCreds(cred, manage, true); err == nil && read {
				u.WritePerm = append(u.WritePerm, k)
				u.ReadWritePerm = append(u.ReadWritePerm, k)
			}
		}

		k, _ = application.NewKeyspace("*", "*", "*", true)
		k.BucketName = bucketName
		checkAndAppendPermissions(k, true)

		ie.Filter = bucketName
		manifestInterface, err := m.observer.GetCurrentState(ie)
		if err != nil {
			continue
		}
		manifests := manifestInterface.(*notifier.CollectionManifest)

		// bucket.scope.*
		for scopeName, scopes := range manifests.Scopes {
			k.ScopeName = scopeName
			k.CollectionName = application.GlobalValue
			checkAndAppendPermissions(k, true)

			// bucket.scope.collection
			for collName, _ := range scopes.Collections {
				k.CollectionName = collName
				checkAndAppendPermissions(k, false)
			}
		}
	}

	application.SortKeyspaces(u.FuncScope)
	application.SortKeyspaces(u.ReadPerm)
	application.SortKeyspaces(u.WritePerm)
	application.SortKeyspaces(u.ReadWritePerm)
	application.SortKeyspaces(u.DcpStreamPerm)

	runtimeInfo.Description = u
	runtimeInfo.OnlyDescription = true
}
