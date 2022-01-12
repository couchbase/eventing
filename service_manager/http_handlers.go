package servicemanager

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"expvar"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/audit"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/consumer"
	"github.com/couchbase/eventing/gen/auditevent"
	"github.com/couchbase/eventing/gen/flatbuf/cfg"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/parser"
	"github.com/couchbase/eventing/rbac"
	"github.com/couchbase/eventing/service_manager/response"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/goutils/systemeventlog"
)

func (m *ServiceMgr) startTracing(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::startTracing"

	res := response.NewResponseWriter(w, r, response.EventStartTracing)
	runtimeInfo := &response.RuntimeInfo{}
	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	audit.Log(auditevent.StartTracing, r, "Start tracing", nil, nil)
	logging.Infof("%s REST Call: %v %v", logPrefix, r.URL.Path, r.Method)

	os.Remove(m.uuid + "_trace.out")

	f, err := os.Create(m.uuid + "_trace.out")
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("Failed to open file to write trace output, err: %v", err)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}
	defer f.Close()

	m.logSystemEvent(util.EVENTID_START_TRACING, systemeventlog.SEInfo, nil)
	startTime := time.Now()

	err = trace.Start(f)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("Failed to start runtime.Trace, err: %v", err)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	<-m.stopTracerCh
	trace.Stop()

	stopTime := time.Now()

	totalTime := stopTime.Sub(startTime).Seconds()
	m.logSystemEvent(util.EVENTID_STOP_TRACING, systemeventlog.SEInfo,
		map[string]interface{}{"tracingExecutionTimeSecs": totalTime})

	runtimeInfo.Description = fmt.Sprintf("tracing execution time secs : %v", totalTime)
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) stopTracing(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::stopTracing"

	res := response.NewResponseWriter(w, r, response.EventStopTracing)
	runtimeInfo := &response.RuntimeInfo{}
	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	audit.Log(auditevent.StopTracing, r, "Tracing stopped", nil, nil)
	logging.Infof("%s Got request to stop tracing", logPrefix)
	m.stopTracerCh <- struct{}{}

	runtimeInfo.Description = "Successfully stopped tracing"
}

func (m *ServiceMgr) getNodeUUID(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetUUID)
	runtimeInfo := &response.RuntimeInfo{}
	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	logging.Debugf("Received request from host %s to fetch UUID", r.Host)

	runtimeInfo.SendRawDescription = true
	runtimeInfo.OnlyDescription = true
	runtimeInfo.Description = m.uuid
}

func (m *ServiceMgr) getNodeVersion(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetVersion)
	runtimeInfo := &response.RuntimeInfo{}
	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	logging.Debugf("Received request from host %s to fetch version", r.Host)

	runtimeInfo.SendRawDescription = true
	runtimeInfo.OnlyDescription = true
	runtimeInfo.Description = util.EventingVer()
}

func (m *ServiceMgr) deletePrimaryStoreHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventDeleteFunction)
	runtimeInfo := &response.RuntimeInfo{}
	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	params := r.URL.Query()
	res.AddRequestData("query", params)
	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info = m.deletePrimaryStore(cred, appName); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	runtimeInfo.Description = fmt.Sprintf("Function: %s deleting in the background", appName)
	runtimeInfo.ExtraAttributes = map[string]interface{}{"appName": appName}
}

// Deletes application from primary store and returns the appropriate success/error code
func (m *ServiceMgr) deletePrimaryStore(cred cbauth.Creds, appName string) *response.RuntimeInfo {
	logPrefix := "ServiceMgr::deletePrimaryStore"

	info := &response.RuntimeInfo{}
	if info = m.checkPermissionFromCred(cred, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
		return info
	}

	logging.Infof("%s Function: %s deleting from primary store", logPrefix, appName)

	appStateCached, info := m.fetchAppCompositeState(appName)
	if info.ErrCode != response.Ok {
		return info
	}

	if appStateCached != common.AppStateUndeployed {
		info.ErrCode = response.ErrAppNotUndeployed
		info.Description = fmt.Sprintf("Function: %s skipping delete request as it hasn't been undeployed", appName)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return info
	}

	settingPath := metakvAppSettingsPath + appName
	err := util.MetaKvDelete(settingPath, nil)
	if err != nil {
		info.ErrCode = response.ErrDelAppSettingsPs
		info.Description = fmt.Sprintf("Function: %s failed to delete settings, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return info
	}

	err = util.DeleteAppContent(metakvAppsPath, metakvChecksumPath, appName)
	if err != nil {
		info.ErrCode = response.ErrDelAppPs
		info.Description = fmt.Sprintf("Function: %s failed to delete, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return info
	}

	logging.Infof("%s %s", logPrefix, info.Description)

	return info
}

func (m *ServiceMgr) deleteTempStoreHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::deleteTempStoreHandler"

	res := response.NewResponseWriter(w, r, response.EventDeleteFunction)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	params := r.URL.Query()
	res.AddRequestData("query", params)
	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	logging.Infof("%s REST Call: %v %v", logPrefix, r.URL.Path, r.Method)
	if info = m.deleteTempStore(cred, appName); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}
	runtimeInfo.Description = fmt.Sprintf("Function: %s deleting in the background", appName)
	runtimeInfo.ExtraAttributes = map[string]interface{}{"appName": appName}
}

// Deletes application from temporary store and returns the appropriate success/error code
func (m *ServiceMgr) deleteTempStore(cred cbauth.Creds, appName string) (info *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::deleteTempStore"

	info = &response.RuntimeInfo{}
	logging.Infof("%s Function: %s deleting drafts from temporary store", logPrefix, appName)

	info = m.checkPermissionFromCred(cred, appName, rbac.HandlerManagePermissions, false)
	if info.ErrCode != response.Ok {
		return
	}

	appStateCached, info := m.fetchAppCompositeState(appName)
	if info.ErrCode != response.Ok {
		return
	}

	if appStateCached != common.AppStateUndeployed {
		info.ErrCode = response.ErrAppNotUndeployed
		info.Description = fmt.Sprintf("Function: %s skipping delete request from temp store, as it hasn't been undeployed", appName)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	if err := util.DeleteAppContent(metakvTempAppsPath, metakvTempChecksumPath, appName); err != nil {
		info.ErrCode = response.ErrDelAppTs
		info.Description = fmt.Sprintf("Function: %s failed to delete, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	logging.Infof("%s %s", logPrefix, info.Description)
	return
}

func (m *ServiceMgr) die(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventDie)
	runtimeInfo := &response.RuntimeInfo{}

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	logging.Errorf("Got request to die, killing all consumers")
	m.superSup.KillAllConsumers()
	logging.Errorf("Got request to die, killing producer")
	runtimeInfo.Description = "Killing all eventing consumers and the eventing producer"
	res.LogAndSend(runtimeInfo)
	time.Sleep(5 * time.Second)

	os.Exit(-1)
}

func (m *ServiceMgr) getAppLog(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetAppLog)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed, Only Get request is allowed"
		return
	}

	params := r.URL.Query()
	res.AddRequestData("query", params)
	nv := params["name"]
	if len(nv) != 1 {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = "Parameter 'name' must appear exactly once"
		return
	}
	appName := nv[0]

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerGetPermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	sz := int64(40960)

	sv := params["size"]
	if len(sv) == 1 {
		psz, err := strconv.Atoi(sv[0])
		if err == nil {
			sz = int64(psz)
		}
	}

	var lines []string
	if rv := params["aggregate"]; len(rv) > 0 && rv[0] == "true" {
		creds := r.Header
		lines = getGlobalAppLog(m, appName, sz, creds)
	} else {
		lines = getLocalAppLog(m, appName, sz)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(lines)))

	runtimeInfo.ContentType = "text/plain"
	runtimeInfo.SendRawDescription = true
	runtimeInfo.OnlyDescription = true
	runtimeInfo.Description = strings.Join(lines, "\n")
}

func (m *ServiceMgr) getInsight(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetInsight)
	runtimeInfo := &response.RuntimeInfo{}

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed, Only Get request is allowed"
		res.LogAndSend(runtimeInfo)
		return
	}

	params := r.URL.Query()
	res.AddRequestData("query", params)
	apps := make([]string, 0)
	apps = append(apps, params["name"]...)
	if len(apps) < 1 {
		for app, _ := range m.superSup.GetDeployedApps() {
			apps = append(apps, app)
		}
	}

	permApps := make([]string, 0, len(apps))
	for _, appName := range apps {
		info := m.checkPermissionFromCred(cred, appName, rbac.HandlerGetPermissions, false)
		if info.ErrCode == response.Ok {
			permApps = append(permApps, appName)
		}
	}

	var insights *common.Insights
	if rv := params["aggregate"]; len(rv) > 0 && rv[0] == "true" {
		creds := r.Header
		insights = getGlobalInsights(m, permApps, creds)
	} else {
		insights = getLocalInsights(m, permApps)
	}

	if rv := params["udmark"]; len(rv) > 0 && rv[0] == "true" {
		pspec := logging.RedactFormat("%ru")
		for name, insight := range *insights {
			insight.Script = fmt.Sprintf(pspec, insight.Script)
			for num, line := range insight.Lines {
				line.LastLog = fmt.Sprintf(pspec, line.LastLog)
				insight.Lines[num] = line
			}
			(*insights)[name] = insight
		}
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false) // otherwise <ud> gets mangled
	enc.SetIndent("", " ")
	enc.Encode(insights)
}

func getLocalAppLog(m *ServiceMgr, appName string, sz int64) []string {
	return m.superSup.GetAppLog(appName, sz)
}

func getGlobalAppLog(m *ServiceMgr, appName string, sz int64, creds http.Header) []string {
	nodes, err := m.getActiveNodeAddrs()
	if err != nil {
		// TODO: send internal server error
		logging.Errorf("Got failure getting nodes", err)
		return nil
	}

	oboAuthHeader := ""
	oboAuthInfo := util.ComposeOBOAuthInfo(creds.Get("Menelaus-Auth-User"), creds.Get("Menelaus-Auth-Domain"))
	if oboAuthInfo != "" {
		oboAuthHeader = "Basic " + oboAuthInfo
	}

	psz := sz
	if len(nodes) > 1 {
		psz = sz / int64(len(nodes))
	}

	var lines []string
	for _, node := range nodes {

		var url string
		var client *util.Client

		m.configMutex.RLock()
		check := m.clusterEncryptionConfig != nil && m.clusterEncryptionConfig.EncryptData
		m.configMutex.RUnlock()

		if check {
			url = "https://" + node + "/getAppLog?name=" + appName + "&aggregate=false" + "&size=" + strconv.Itoa(int(psz))
			client = util.NewTLSClient(time.Second*15, m.superSup.GetSecuritySetting())
		} else {
			url = "http://" + node + "/getAppLog?name=" + appName + "&aggregate=false" + "&size=" + strconv.Itoa(int(psz))
			client = util.NewClient(time.Second * 15)
		}

		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			logging.Errorf("Got failure creating http request to %v: %v", node, err)
			continue
		}
		if oboAuthHeader != "" {
			req.Header.Add(util.OBOAuthHeader, oboAuthInfo)
		}
		resp, err := client.Do(req)
		if err != nil {
			logging.Errorf("Got failure doing http request to %v: %v", node, err)
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logging.Errorf("Got failure reading http request to %v: %v", node, err)
			continue
		}

		msgs := strings.Split(string(body), "\n")
		for _, msg := range msgs {
			msg = strings.Trim(msg, "\r")
			if len(msg) > 0 {
				lines = append(lines, msg)
			}
		}
	}
	return lines
}

func getLocalInsights(m *ServiceMgr, apps []string) *common.Insights {
	insights := common.NewInsights()
	for _, app := range apps {
		(*insights)[app] = m.superSup.GetInsight(app)
	}
	return insights
}

func getGlobalInsights(m *ServiceMgr, apps []string, creds http.Header) *common.Insights {
	insights := common.NewInsights()
	nodes, err := m.getActiveNodeAddrs()
	if err != nil {
		// TODO: send internal server error
		logging.Errorf("Got failure getting nodes", err)
		return insights
	}
	oboAuthHeader := ""
	oboAuthInfo := util.ComposeOBOAuthInfo(creds.Get("Menelaus-Auth-User"), creds.Get("Menelaus-Auth-Domain"))
	if oboAuthInfo != "" {
		oboAuthHeader = "Basic " + oboAuthInfo
	}
	for _, node := range nodes {
		var url string
		var client *util.Client

		m.configMutex.RLock()
		check := m.clusterEncryptionConfig != nil && m.clusterEncryptionConfig.EncryptData
		m.configMutex.RUnlock()

		if check {
			url = "https://" + node + "/getInsight?aggregate=false"
			client = util.NewTLSClient(time.Second*15, m.superSup.GetSecuritySetting())
		} else {
			url = "http://" + node + "/getInsight?aggregate=false"
			client = util.NewClient(time.Second * 15)
		}

		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			logging.Errorf("Got failure creating http request to %v: %v", node, err)
			continue
		}
		if oboAuthHeader != "" {
			req.Header.Add(util.OBOAuthHeader, oboAuthInfo)
		}
		resp, err := client.Do(req)
		if err != nil {
			logging.Errorf("Got failure doing http request to %v: %v", node, err)
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logging.Errorf("Got failure reading http request to %v: %v", node, err)
			continue
		}
		ans := common.NewInsights()
		err = json.Unmarshal(body, &ans)
		if err != nil {
			logging.Errorf("Got failure unmarshaling http request to %v: %v body: %v", node, err, body)
			continue
		}
		insights.Accumulate(ans)
	}
	return insights
}

func (m *ServiceMgr) getDebuggerURL(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getDebuggerURL"
	res := response.NewResponseWriter(w, r, response.EventGetDebuggerUrl)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)
	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerGetPermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	logging.Debugf("%s Function: %s got request to get V8 debugger url", logPrefix, appName)

	if m.checkIfDeployed(appName) {
		debugURL, err := m.superSup.GetDebuggerURL(appName)
		if err != nil {
			logging.Errorf("Error in getting debugger url for %s err: %v", appName, err)
			runtimeInfo.ErrCode = response.ErrInternalServer
			return
		}
		debugURL = strings.Replace(debugURL, "[::1]", "127.0.0.1", -1)

		runtimeInfo.SendRawDescription = true
		runtimeInfo.Description = debugURL
		runtimeInfo.OnlyDescription = true
		return
	}

	runtimeInfo.ErrCode = response.ErrAppNotDeployed
	runtimeInfo.Description = fmt.Sprintf("Function: %s not deployed", appName)
}

func (m *ServiceMgr) getLocalDebugURL(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getLocalDebugURL"
	res := response.NewResponseWriter(w, r, response.EventGetDebuggerUrl)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)
	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerGetPermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	logging.Debugf("%s Function: %s got request to get local V8 debugger url", logPrefix, appName)

	config := m.config.Load()
	dir := config["eventing_dir"].(string)

	filePath := fmt.Sprintf("%s/%s_frontend.url", dir, appName)
	u, err := ioutil.ReadFile(filePath)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		logging.Errorf("%s Function: %s failed to read contents from debugger frontend url file, err: %v",
			logPrefix, appName, err)
		return
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = string(u)
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) logFileLocation(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetLogFileLocation)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed, Only Get request is allowed"
		return
	}

	c := m.config.Load()

	runtimeInfo.Description = fmt.Sprintf(`{"log_dir":"%v"}`, c["eventing_dir"])
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) notifyDebuggerStart(appName string, hostnames []string) (info *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::notifyDebuggerStart"
	info = &response.RuntimeInfo{}

	uuidGen, err := util.NewUUID()
	if err != nil {
		info.ErrCode = response.ErrInternalServer
		info.Description = fmt.Sprintf("Unable to initialize UUID generator, err: %v", err)
		return
	}

	token := uuidGen.Str()
	m.superSup.WriteDebuggerToken(appName, token, hostnames)
	logging.Infof("%s Function: %s notifying on debugger path %s",
		logPrefix, appName, common.MetakvDebuggerPath+appName)

	err = util.MetakvSet(common.MetakvDebuggerPath+appName, []byte(token), nil)

	if err != nil {
		logging.Errorf("%s Function: %s Failed to write to metakv err: %v", logPrefix, appName, err)
		info.ErrCode = response.ErrMetakvWriteFailed
		info.Description = fmt.Sprintf("Failed to write to metakv debugger path for Function: %s, err: %v", appName, err)
	}

	return
}

func (m *ServiceMgr) startDebugger(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::startDebugger"

	res := response.NewResponseWriter(w, r, response.EventStartDebugger)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)
	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	logging.Infof("%s REST Call: %v %v", logPrefix, r.URL.Path, r.Method)

	config, info := m.getConfig()
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	enabled, exists := config["enable_debugger"]
	if !exists || !enabled.(bool) {
		runtimeInfo.ErrCode = response.ErrDebuggerDisabled
		runtimeInfo.Description = "Debugger is not enabled"
		return
	}

	if !m.checkIfDeployedAndRunning(appName) {
		runtimeInfo.ErrCode = response.ErrAppNotDeployed
		runtimeInfo.Description = fmt.Sprintf("Function: %s is not in deployed state, debugger cannot start", appName)
		return
	}

	isMixedMode, err := m.isMixedModeCluster()
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	if isMixedMode {
		runtimeInfo.ErrCode = response.ErrMixedMode
		runtimeInfo.Description = "Debugger can not be spawned in a mixed mode cluster"
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrReadReq
		runtimeInfo.Description = fmt.Sprintf("Failed to read request, err : %v", err)
		return
	}

	var data map[string]interface{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrUnmarshalPld
		runtimeInfo.Description = fmt.Sprintf("Failed to unmarshal request, err : %v", err)
		return
	}

	if info = m.notifyDebuggerStart(appName, GetNodesHostname(data)); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	runtimeInfo.ExtraAttributes = params
	runtimeInfo.Description = fmt.Sprintf("Function: %s Started Debugger", appName)
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) stopDebugger(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::stopDebugger"

	res := response.NewResponseWriter(w, r, response.EventStopDebugger)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)
	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	logging.Infof("%s REST Call: %v %v", logPrefix, r.URL.Path, r.Method)

	if m.checkIfDeployed(appName) {
		err := m.superSup.SignalStopDebugger(appName)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInternalServer
			runtimeInfo.Description = fmt.Sprintf("%v", err)
			logging.Errorf("Error in stopping debugger for %s err: %v", appName, err)
			return
		}

		runtimeInfo.ExtraAttributes = params
		runtimeInfo.Description = fmt.Sprintf("Function: %s stopped Debugger", appName)
		runtimeInfo.OnlyDescription = true
		return
	}

	runtimeInfo.Description = fmt.Sprintf("Function: %s not deployed", appName)
	runtimeInfo.ErrCode = response.ErrAppNotDeployed
	logging.Debugf("%s %s", logPrefix, runtimeInfo.Description)
}

func (m *ServiceMgr) writeDebuggerURLHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventWriteDebuggerUrl)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if !m.validateLocalAuth(w, r) {
		return
	}

	appName := path.Base(r.URL.Path)
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrReadReq
		runtimeInfo.Description = fmt.Sprintf("%v", err)
		return
	}

	logging.Infof("Received Debugger URL: %s for Function: %s", string(data), appName)
	m.superSup.WriteDebuggerURL(appName, string(data))
	runtimeInfo.Description = "Successfully written debugger url"
}

func (m *ServiceMgr) getEventProcessingStats(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getEventProcessingStats"

	res := response.NewResponseWriter(w, r, response.EventFetchProcessingStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)

	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerGetPermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if m.checkIfDeployed(appName) {
		stats := m.superSup.GetEventProcessingStats(appName)
		runtimeInfo.Description = stats
		runtimeInfo.OnlyDescription = true
		return
	}

	runtimeInfo.Description = fmt.Sprintf("Function: %s not deployed", appName)
	runtimeInfo.ErrCode = response.ErrAppNotDeployed
	logging.Debugf("%s %s", logPrefix, runtimeInfo.Description)
}

func (m *ServiceMgr) getAppList() (map[string]int, map[string]int, map[string]int, int, bool, *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::getAppList"
	info := &response.RuntimeInfo{}

	nodeAddrs, err := m.getActiveNodeAddrs()
	if err != nil {
		logging.Warnf("%s failed to fetch active Eventing nodes, err: %v", logPrefix, err)
		info.ErrCode = response.ErrInternalServer
		info.Description = fmt.Sprintf("Unable to fetch active Eventing nodes, err: %v", err)
		return nil, nil, nil, 0, false, info
	}

	numEventingNodes := len(nodeAddrs)
	if numEventingNodes == 0 {
		info.ErrCode = response.ErrInternalServer
		return nil, nil, nil, 0, false, info
	}

	aggDeployedApps := make(map[string]map[string]string)
	util.Retry(util.NewFixedBackoff(time.Second), nil, getDeployedAppsCallback, &aggDeployedApps, nodeAddrs)

	appDeployedNodesCounter := make(map[string]int)

	for _, apps := range aggDeployedApps {
		for app := range apps {
			if _, ok := appDeployedNodesCounter[app]; !ok {
				appDeployedNodesCounter[app] = 0
			}
			appDeployedNodesCounter[app]++
		}
	}

	aggBootstrappingApps := make(map[string]map[string]string)
	util.Retry(util.NewFixedBackoff(time.Second), nil, getBootstrappingAppsCallback, &aggBootstrappingApps, nodeAddrs)

	appBootstrappingNodesCounter := make(map[string]int)
	for _, apps := range aggBootstrappingApps {
		for app := range apps {
			if _, ok := appBootstrappingNodesCounter[app]; !ok {
				appBootstrappingNodesCounter[app] = 0
			}
			appBootstrappingNodesCounter[app]++
		}
	}

	mhVersion := common.CouchbaseVerMap["mad-hatter"]
	if m.compareEventingVersionOnNodes(mhVersion, nodeAddrs) {
		aggPausingApps := make(map[string]map[string]string)
		util.Retry(util.NewFixedBackoff(time.Second), nil, getPausingAppsCallback, &aggPausingApps, nodeAddrs)

		appPausingNodesCounter := make(map[string]int)
		for _, apps := range aggPausingApps {
			for app := range apps {
				if _, ok := appPausingNodesCounter[app]; !ok {
					appPausingNodesCounter[app] = 0
				}
				appPausingNodesCounter[app]++
			}
		}

		return appDeployedNodesCounter, appBootstrappingNodesCounter, appPausingNodesCounter, numEventingNodes, true, info
	}
	return appDeployedNodesCounter, appBootstrappingNodesCounter, nil, numEventingNodes, false, info
}

// Returns list of apps that are deployed i.e. finished dcp/timer/debugger related bootstrap
func (m *ServiceMgr) getDeployedApps(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetDeployedApps)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	appDeployedNodesCounter, _, appPausingNodesCounter, numEventingNodes, _, info := m.getAppList()
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	deployedApps := make(map[string]string)
	for appName, numNodesDeployed := range appDeployedNodesCounter {
		info := m.checkPermissionFromCred(cred, appName, rbac.HandlerGetPermissions, false)
		if info.ErrCode != response.Ok {
			continue
		}
		if appPausingNodesCounter != nil {
			_, ok := appPausingNodesCounter[appName]
			if numNodesDeployed == numEventingNodes && !ok {
				deployedApps[appName] = ""
			}
		} else {
			if numNodesDeployed == numEventingNodes {
				deployedApps[appName] = ""
			}
		}
	}

	runtimeInfo.Description = deployedApps
	runtimeInfo.OnlyDescription = true
}

// Returns list of apps that:
// * may be undergoing undeploy on one ore more nodes,
// * maybe undergoing bootstrap on one or more nodes or
// * are already deployed on all nodes
func (m *ServiceMgr) getRunningApps(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRunningApps)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	appDeployedNodesCounter, _, _, _, _, info := m.getAppList()
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	runningApps := make(map[string]string)
	for appName, _ := range appDeployedNodesCounter {
		info := m.checkPermissionFromCred(cred, appName, rbac.HandlerGetPermissions, false)
		if info.ErrCode != response.Ok {
			continue
		}
		runningApps[appName] = ""
	}
	runtimeInfo.Description = runningApps
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) getLocallyDeployedApps(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetDeployedApps)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	deployedApps := m.superSup.GetDeployedApps()

	permDeployedApps := make(map[string]string)
	for appName, val := range deployedApps {
		info := m.checkPermissionFromCred(cred, appName, rbac.HandlerGetPermissions, false)
		if info.ErrCode != response.Ok {
			continue
		}

		permDeployedApps[appName] = val
	}
	runtimeInfo.Description = deployedApps
	runtimeInfo.OnlyDescription = true
}

// Reports progress across all producers on current node
// TODO: Should only for allowed apps
func (m *ServiceMgr) getRebalanceProgress(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getRebalanceProgress"

	res := response.NewResponseWriter(w, r, response.EventGetRebalanceProgress)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	progress := &common.RebalanceProgress{}

	m.fnMu.RLock()
	appNames := make([]string, 0, len(m.fnsInPrimaryStore))
	for appName := range m.fnsInPrimaryStore {
		appNames = append(appNames, appName)
	}
	m.fnMu.RUnlock()
	for _, appName := range appNames {
		// TODO: Leverage error returned from rebalance task progress and fail the rebalance
		// if it occurs
		info := m.checkPermissionFromCred(cred, appName, rbac.HandlerGetPermissions, false)
		if info.ErrCode != response.Ok {
			continue
		}

		appProgress, err := m.superSup.RebalanceTaskProgress(appName)
		logging.Infof("%s Function: %s rebalance progress from node with rest port: %rs progress: %v err: %v",
			logPrefix, appName, m.restPort, appProgress, err)
		if err == nil {
			progress.CloseStreamVbsLen += appProgress.CloseStreamVbsLen
			progress.StreamReqVbsLen += appProgress.StreamReqVbsLen

			progress.VbsOwnedPerPlan += appProgress.VbsOwnedPerPlan
			progress.VbsRemainingToShuffle += appProgress.VbsRemainingToShuffle
		}
	}

	runtimeInfo.Description = progress
	runtimeInfo.OnlyDescription = true
}

// Report back state of rebalance on current node
func (m *ServiceMgr) getRebalanceStatus(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRebalanceStatus)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingReadPermissions, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = strconv.FormatBool(m.superSup.RebalanceStatus())
	runtimeInfo.OnlyDescription = true
}

// Report back state of bootstrap on current node
func (m *ServiceMgr) getBootstrapStatus(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetBootstrapStatus)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingReadPermissions, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	bootstrapAppList := m.superSup.BootstrapAppList()
	bootstrapping := false
	if len(bootstrapAppList) > 0 {
		bootstrapping = true
	} else {
		bootstrapping = m.superSup.BootstrapStatus()
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = strconv.FormatBool(bootstrapping)
	runtimeInfo.OnlyDescription = true
}

// Report back state of an app bootstrap on current node
func (m *ServiceMgr) getBootstrapAppStatus(w http.ResponseWriter, r *http.Request) {

	res := response.NewResponseWriter(w, r, response.EventGetBootstrapStatus)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)
	appName, info := CheckAndGetQueryParam(params, "appName")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	info = m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerGetPermissions, false)
	if info.ErrCode == response.ErrAppNotFound {
		runtimeInfo.Description = false
		runtimeInfo.OnlyDescription = true
		return
	}

	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	bootstrapAppList := m.superSup.BootstrapAppList()
	_, isBootstrapping := bootstrapAppList[appName]
	bootstrapping := false
	if isBootstrapping {
		bootstrapping = true
	} else {
		bootstrapping = m.superSup.BootstrapAppStatus(appName)
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = strconv.FormatBool(bootstrapping)
	runtimeInfo.OnlyDescription = true
}

// Reports aggregated event processing stats from all producers
func (m *ServiceMgr) getAggEventProcessingStats(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventFetchProcessingStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingReadPermissions, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	params := r.URL.Query()
	res.AddRequestData("query", params)
	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}
	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	pStats, err := util.GetEventProcessingStats("/getEventProcessingStats?name="+appName, m.eventingNodeAddrs)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("Failed to get event processing stats, err: %v", err)
		return
	}

	runtimeInfo.Description = pStats
	runtimeInfo.OnlyDescription = true
}

// Reports aggregated rebalance progress from all Eventing nodes in the cluster
func (m *ServiceMgr) getAggRebalanceProgress(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getAggRebalanceProgress"

	res := response.NewResponseWriter(w, r, response.EventGetRebalanceProgress)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingReadPermissions, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	logging.Infof("%s going to query eventing nodes: %rs for rebalance progress",
		logPrefix, m.eventingNodeAddrs)

	aggProgress, progressMap, errMap := util.GetProgress("/getRebalanceProgress", m.eventingNodeAddrs)
	if len(errMap) > 0 {
		logging.Warnf("%s failed to get progress from some/all eventing nodes: %rs err: %rs",
			logPrefix, m.eventingNodeAddrs, errMap)
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("Failed to get progress")
		return
	}

	aggProgress.NodeLevelStats = progressMap

	runtimeInfo.Description = aggProgress
	runtimeInfo.OnlyDescription = true
}

// Report aggregated rebalance status from all Eventing nodes in the cluster
func (m *ServiceMgr) getAggRebalanceStatus(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getAggRebalanceStatus"

	res := response.NewResponseWriter(w, r, response.EventGetRebalanceStatus)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingReadPermissions, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	status, err := util.CheckIfRebalanceOngoing("/getRebalanceStatus", m.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("%s failed to grab correct rebalance status from some/all nodes, err: %v", logPrefix, err)
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("Failed to get progress err: %v", err)
		return
	}

	runtimeInfo.Description = status
	runtimeInfo.OnlyDescription = true
}

// Report aggregated bootstrap status from all Eventing nodes in the cluster
func (m *ServiceMgr) getAggBootstrapStatus(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getAggBootstrapStatus"
	res := response.NewResponseWriter(w, r, response.EventGetBootstrapStatus)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingReadPermissions, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	status, err := util.CheckIfBootstrapOngoing("/getBootstrapStatus", m.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("%s failed to grab correct bootstrap status from some/all nodes, err: %v", logPrefix, err)
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("Failed to get progress err: %v", err)
		return
	}

	runtimeInfo.Description = status
	runtimeInfo.OnlyDescription = true
}

// Report aggregated bootstrap status of an app from all Eventing nodes in the cluster
func (m *ServiceMgr) getAggBootstrapAppStatus(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getAggBootstrapAppStatus"
	res := response.NewResponseWriter(w, r, response.EventGetBootstrapStatus)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)

	appName, info := CheckAndGetQueryParam(params, "appName")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerGetPermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	status, err := util.CheckIfAppBootstrapOngoing("/getBootstrapAppStatus", m.eventingNodeAddrs, appName)
	if err != nil {
		logging.Errorf("%s failed to grab correct bootstrap status of app from some/all nodes, err: %v", logPrefix, err)
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("Failed to get progress err: %v", err)
		return
	}

	runtimeInfo.Description = status
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) getLatencyStats(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventFetchLatencyStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)

	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerGetPermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if m.checkIfDeployed(appName) {
		lStats := m.superSup.GetLatencyStats(appName)
		runtimeInfo.Description = lStats
		runtimeInfo.OnlyDescription = true
		return
	}

	runtimeInfo.ErrCode = response.ErrAppNotDeployed
	runtimeInfo.Description = fmt.Sprintf("Function: %s not deployed", appName)
}

func (m *ServiceMgr) getExecutionStats(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventFetchExecutionStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)

	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerGetPermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if m.checkIfDeployed(appName) {
		eStats := m.superSup.GetExecutionStats(appName)
		runtimeInfo.Description = eStats
		runtimeInfo.OnlyDescription = true
		return
	}

	runtimeInfo.ErrCode = response.ErrAppNotDeployed
	runtimeInfo.Description = fmt.Sprintf("Function: %s not deployed", appName)
}

func (m *ServiceMgr) getFailureStats(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventFetchFailureStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)

	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerGetPermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if m.checkIfDeployed(appName) {
		fStats := m.superSup.GetFailureStats(appName)
		runtimeInfo.Description = fStats
		runtimeInfo.OnlyDescription = true
		return
	}

	runtimeInfo.ErrCode = response.ErrAppNotDeployed
	runtimeInfo.Description = fmt.Sprintf("Function: %s not deployed", appName)
}

func (m *ServiceMgr) getSeqsProcessed(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetSeqProcessed)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)

	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerGetPermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if m.checkIfDeployed(appName) {
		seqNoProcessed := m.superSup.GetSeqsProcessed(appName)
		runtimeInfo.Description = seqNoProcessed
		runtimeInfo.OnlyDescription = true
		return
	}
	runtimeInfo.ErrCode = response.ErrAppNotDeployed
	runtimeInfo.Description = fmt.Sprintf("Function: %s not deployed", appName)
}

func (m *ServiceMgr) setSettingsHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::setSettingsHandler"

	res := response.NewResponseWriter(w, r, response.EventUpdateFunction)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)

	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	force := false
	forceVal, tmpInfo := CheckAndGetQueryParam(params, "force")
	if tmpInfo.ErrCode == response.Ok && forceVal == "true" {
		force = true
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	logging.Infof("%s REST Call: %v %v", logPrefix, r.URL.Path, r.Method)
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logging.Errorf("%s Function: %s failed to read request body, err: %v", logPrefix, appName, err)
		runtimeInfo.ErrCode = response.ErrReadReq
		runtimeInfo.Description = fmt.Sprintf("Failed to read request body, err: %v", err)
		return
	}

	var settings map[string]interface{}
	err = json.Unmarshal(data, &settings)
	if err != nil {
		logging.Errorf("%s Function: %s failed to unmarshal setting supplied, err: %v", logPrefix, appName, err)
		runtimeInfo.ErrCode = response.ErrUnmarshalPld
		runtimeInfo.Description = fmt.Sprintf("Failed to unmarshal setting supplied, err: %v", err)
		return
	}

	if info := m.setSettings(appName, data, force); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	runtimeInfo.Description = "Settings saved"
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) getSettings(appName string) (*map[string]interface{}, *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::getSettings"

	logging.Infof("%s Function: %s fetching settings", logPrefix, appName)
	app, status := m.getTempStore(appName)
	if status.ErrCode != response.Ok {
		return nil, status
	}

	info := &response.RuntimeInfo{}

	info.Description = fmt.Sprintf("Function: %s fetched settings", appName)
	logging.Infof("%s %s", logPrefix, info.Description)
	return &app.Settings, info
}

func (m *ServiceMgr) setSettings(appName string, data []byte, force bool) (info *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::setSettings"

	info = &response.RuntimeInfo{}
	logging.Infof("%s Function: %s save settings", logPrefix, appName)

	var settings map[string]interface{}
	err := json.Unmarshal(data, &settings)
	if err != nil {
		info.ErrCode = response.ErrUnmarshalPld
		info.Description = fmt.Sprintf("Function: %s failed to unmarshal setting supplied, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	if value, ok := settings["num_timer_partitions"]; ok {
		switch value.(type) {
		case string:
			settings["num_timer_partitions"], err = strconv.ParseFloat(value.(string), 64)
			if err != nil {
				logging.Errorf("%s Function: num_timer_partitions is in invalid format.", logPrefix)
				info.ErrCode = response.ErrInvalidConfig
				info.Description = "num_timer_partitions format is invalid"
				return
			}
		case int:
			settings["num_timer_partitions"] = float64(value.(int))
		}
	}

	// This block is helpful in mixed mode && upgraded cluster, since we are getting rid of 'from_prior' in 6.6.2
	// In a cluster upgradation when the functions are in the paused state, the incoming requests
	// to resume will replace the dcp_stream_boundary to default value. Since in resume processing
	// we are ignoring the dcp_stream_boundary, replacing the value should not be a problem
	if value, ok := settings["dcp_stream_boundary"]; ok && value == "from_prior" {
		settings["dcp_stream_boundary"] = "everything"
	}

	if info = m.validateSettings(appName, util.DeepCopy(settings)); info.ErrCode != response.Ok {
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	logging.Infof("%s Function: %s settings params: %+v", logPrefix, appName, settings)

	_, procStatExists := settings["processing_status"]
	_, depStatExists := settings["deployment_status"]

	mhVersion := common.CouchbaseVerMap["mad-hatter"]

	if procStatExists || depStatExists {
		if m.compareEventingVersion(mhVersion) {
			if settings["deployment_status"].(bool) {
				status, err := util.GetAggBootstrapAppStatus(net.JoinHostPort(util.Localhost(), m.adminHTTPPort), appName, true)
				if err != nil {
					logging.Errorf("%s %s", logPrefix, err)
					info.ErrCode = response.ErrStatusesNotFound
					info.Description = "Failed to find app status"
					return
				}

				if status {
					info.ErrCode = response.ErrAppNotInit
					info.Description = "Function is undergoing bootstrap"
					return
				}
			}
		}

		if !force {
			if info = m.checkLifeCycleOpsDuringRebalance(); info.ErrCode != response.Ok {
				return
			}
		}
	}

	// Get the app from temp store and update its settings with those provided
	var app application
	app, info = m.getTempStore(appName)
	if info.ErrCode != response.Ok {
		return
	}
	m.addDefaultDeploymentConfig(&app)

	newTPValue, timerPartitionsPresent := settings["num_timer_partitions"]
	oldTPValue, oldTimerPartitionsPresent := app.Settings["num_timer_partitions"]

	deployed := (m.superSup.GetAppCompositeState(appName) == common.AppStateEnabled)

	for setting := range settings {
		if deployed && !isDynamicSetting(setting) && app.Settings[setting] != settings[setting] {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("Function: %s setting: %s can only be altered when the function is paused or undeployed.", appName, setting)
			logging.Errorf("%s %s", logPrefix, info.Description)
			return
		}
		app.Settings[setting] = settings[setting]
	}

	processingStatus, pOk := app.Settings["processing_status"].(bool)
	deploymentStatus, dOk := app.Settings["deployment_status"].(bool)

	logging.Infof("%s Function: %s deployment status: %t processing status: %t",
		logPrefix, appName, deploymentStatus, processingStatus)

	deployedApps := m.superSup.GetDeployedApps()
	if pOk && dOk {
		isMixedMode, err := m.isMixedModeCluster()
		if err != nil {
			info.ErrCode = response.ErrInternalServer
			info.Description = fmt.Sprintf("err: %v", err)
			logging.Errorf("%s %s", logPrefix, info.Description)
			return
		}

		if isMixedMode && !m.isUndeployOperation(app.Settings) {
			info.ErrCode = response.ErrMixedMode
			info.Description = "Life-cycle operations except delete and undeploy are not allowed in a mixed mode cluster"
			logging.Errorf("%s %s", logPrefix, info.Description)
			return
		}

		// Add the cycle meta setting based on the current app state
		m.addLifeCycleStateByFunctionState(&app)

		// Check for pause processing
		if deploymentStatus && !processingStatus {
			if !m.compareEventingVersion(mhVersion) {
				info.ErrCode = response.ErrClusterVersion
				info.Description = fmt.Sprintf("All eventing nodes in the cluster must be on version %s or higher for pausing function execution",
					mhVersion)
				logging.Warnf("%s Version compat check failed: %s", logPrefix, info.Description)
				return
			}

			if _, ok := deployedApps[appName]; !ok {
				info.ErrCode = response.ErrAppNotInit
				info.Description = fmt.Sprintf("Function: %s not processing mutations. Operation is not permitted. Edit function instead", appName)
				logging.Errorf("%s %s", logPrefix, info.Description)
				return
			}

			if oldTimerPartitionsPresent {
				if timerPartitionsPresent && oldTPValue != newTPValue {
					info.ErrCode = response.ErrInvalidConfig
					info.Description = fmt.Sprintf("Function: %s num_timer_partitions cannot be altered when trying to pause the function.", appName)
					logging.Errorf("%s %s", logPrefix, info.Description)
					return
				}
			} else {
				if timerPartitionsPresent {
					info.ErrCode = response.ErrInvalidConfig
					info.Description = fmt.Sprintf("Function: %s num_timer_partitions cannot be set when trying to pause the function.", appName)
					logging.Errorf("%s %s", logPrefix, info.Description)
					return
				}
			}
		}

		if deploymentStatus && processingStatus && m.superSup.GetAppCompositeState(appName) == common.AppStatePaused && !m.compareEventingVersion(mhVersion) {
			info.ErrCode = response.ErrClusterVersion
			info.Description = fmt.Sprintf("All eventing nodes in cluster must be on version %s or higher for resuming function execution",
				mhVersion)
			logging.Warnf("%s Version compat check failed: %s", logPrefix, info.Description)
			return
		}

		if deploymentStatus && processingStatus {

			if m.superSup.GetAppCompositeState(appName) == common.AppStatePaused {
				if oldTimerPartitionsPresent {
					if timerPartitionsPresent && oldTPValue != newTPValue {
						info.ErrCode = response.ErrInvalidConfig
						info.Description = fmt.Sprintf("Function: %s num_timer_partitions cannot be changed when trying to resume the function.", appName)
						logging.Errorf("%s %s", logPrefix, info.Description)
						return
					}
				} else {
					if timerPartitionsPresent {
						info.ErrCode = response.ErrInvalidConfig
						info.Description = fmt.Sprintf("Function: %s num_timer_partitions cannot be set when trying to resume the function.", appName)
						logging.Errorf("%s %s", logPrefix, info.Description)
						return
					}
				}
			}

			if oldTimerPartitionsPresent {
				if timerPartitionsPresent && m.checkIfDeployed(appName) && oldTPValue != newTPValue {
					info.ErrCode = response.ErrInvalidConfig
					info.Description = fmt.Sprintf("Function: %s num_timer_partitions cannot be changed when the function is in deployed state.", appName)
					logging.Errorf("%s %s", logPrefix, info.Description)
					return
				}
			} else {
				if timerPartitionsPresent && m.checkIfDeployed(appName) {
					info.ErrCode = response.ErrInvalidConfig
					info.Description = fmt.Sprintf("Function: %s num_timer_partitions cannot be changed when the function is in deployed state.", appName)
					logging.Errorf("%s %s", logPrefix, info.Description)
					return
				}
				if !m.checkIfDeployed(appName) {
					m.addDefaultTimerPartitionsIfMissing(&app)
				}
			}

			if info = m.validateApplication(&app); info.ErrCode != response.Ok {
				logging.Errorf("%s Function: %s recursion error %d: %s", logPrefix, app.Name, info.ErrCode, info.Description)
				return
			}

			// Write to primary store in case of deployment
			if !m.checkIfDeployedAndRunning(appName) {
				info = m.savePrimaryStore(&app)
				if info.ErrCode != response.Ok {
					logging.Errorf("%s %s", logPrefix, info.Description)
					return
				}
			}
		}
	} else {
		info.ErrCode = response.ErrStatusesNotFound
		info.Description = fmt.Sprintf("Function: %s missing processing or deployment statuses or both", appName)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	data, err = json.MarshalIndent(app.Settings, "", " ")
	if err != nil {
		info.ErrCode = response.ErrMarshalResp
		info.Description = fmt.Sprintf("Function: %s failed to marshal settings, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	metakvPath := metakvAppSettingsPath + appName
	err = util.MetakvSet(metakvPath, data, nil)
	if err != nil {
		info.ErrCode = response.ErrSetSettingsPs
		info.Description = fmt.Sprintf("Function: %s failed to store setting, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	// Here function scope and owner won't be changed so no need to check for permissions
	// Write the updated app along with its settings back to temp store
	if info = m.saveTempStore(app); info.ErrCode != response.Ok {
		return
	}

	info.ErrCode = response.Ok
	info.Description = fmt.Sprintf("Function: %s stored settings", appName)
	logging.Infof("%s %s", logPrefix, info.Description)
	return
}

func (m *ServiceMgr) parseFunctionPayload(data []byte, fnName string) application {
	logPrefix := "ServiceMgr::parseFunctionPayload"

	config := cfg.GetRootAsConfig(data, 0)

	var app application
	app.AppHandlers = string(config.AppCode())
	app.Name = string(config.AppName())
	app.FunctionID = uint32(config.HandlerUUID())
	app.FunctionInstanceID = string(config.FunctionInstanceID())
	if config.EnforceSchema() == byte(0x1) {
		app.EnforceSchema = true
	} else {
		app.EnforceSchema = false
	}

	d := new(cfg.DepCfg)
	depcfg := new(depCfg)
	dcfg := config.DepCfg(d)

	depcfg.MetadataBucket = string(dcfg.MetadataBucket())
	depcfg.SourceBucket = string(dcfg.SourceBucket())
	depcfg.SourceScope = common.CheckAndReturnDefaultForScopeOrCollection(string(dcfg.SourceScope()))
	depcfg.SourceCollection = common.CheckAndReturnDefaultForScopeOrCollection(string(dcfg.SourceCollection()))
	depcfg.MetadataCollection = common.CheckAndReturnDefaultForScopeOrCollection(string(dcfg.MetadataCollection()))
	depcfg.MetadataScope = common.CheckAndReturnDefaultForScopeOrCollection(string(dcfg.MetadataScope()))

	var buckets []bucket
	b := new(cfg.Bucket)
	for i := 0; i < dcfg.BucketsLength(); i++ {

		if dcfg.Buckets(b, i) {
			newBucket := bucket{
				Alias:          string(b.Alias()),
				BucketName:     string(b.BucketName()),
				Access:         string(config.Access(i)),
				ScopeName:      common.CheckAndReturnDefaultForScopeOrCollection(string(b.ScopeName())),
				CollectionName: common.CheckAndReturnDefaultForScopeOrCollection(string(b.CollectionName())),
			}
			buckets = append(buckets, newBucket)
		}
	}

	settingsPath := metakvAppSettingsPath + fnName
	sData, sErr := util.MetakvGet(settingsPath)
	if sErr == nil {
		settings := make(map[string]interface{})
		uErr := json.Unmarshal(sData, &settings)
		if uErr != nil {
			logging.Errorf("%s failed to unmarshal settings data from metakv, err: %v", logPrefix, uErr)
		} else {
			app.Settings = settings
			if _, ok := app.Settings["language_compatibility"]; !ok {
				app.Settings["language_compatibility"] = common.LanguageCompatibility[0]
			}
		}
	} else {
		logging.Errorf("%s failed to fetch settings data from metakv, err: %v", logPrefix, sErr)
	}

	depcfg.Buckets = buckets
	app.DeploymentConfig = *depcfg

	f := new(cfg.FunctionScope)
	fS := config.FunctionScope(f)
	funcScope := common.FunctionScope{}

	if fS != nil {
		funcScope.BucketName = string(fS.BucketName())
		funcScope.ScopeName = string(fS.ScopeName())
	}

	o := new(cfg.Owner)
	ownerEncrypted := config.Owner(o)
	owner := &common.Owner{}

	if ownerEncrypted != nil {
		owner.User = string(ownerEncrypted.User())
		owner.Domain = string(ownerEncrypted.Domain())
		owner.UUID = string(ownerEncrypted.Uuid())
	}

	app.FunctionScope = funcScope
	app.Owner = owner
	return app
}

func (m *ServiceMgr) getPrimaryStoreHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getPrimaryStoreHandler"

	res := response.NewResponseWriter(w, r, response.EventGetFunctionDraft)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	logging.Infof("%s getting all functions from primary store", logPrefix)

	appList, err := util.ListChildren(metakvAppsPath)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("Error in reading app path: err", err)
		return
	}
	respData := make([]application, 0, len(appList))

	for _, fnName := range appList {
		info := m.checkPermissionFromCred(cred, fnName, rbac.HandlerGetPermissions, false)
		if info.ErrCode != response.Ok {
			continue
		}

		data, err := util.ReadAppContent(metakvAppsPath, metakvChecksumPath, fnName)
		if err == nil && data != nil {
			respData = append(respData, m.parseFunctionPayload(data, fnName))
		}
	}

	runtimeInfo.OnlyDescription = true
	runtimeInfo.Description = respData
}

func (m *ServiceMgr) getAnnotations(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetAnnotations)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	applications := m.getTempStoreAll()
	respData := make([]annotation, 0, len(applications))
	for _, app := range applications {

		info := m.checkPermissionFromCred(cred, app.Name, rbac.HandlerGetPermissions, false)
		if info.ErrCode != response.Ok {
			continue
		}
		respObj := annotation{}
		respObj.Name = app.Name
		respObj.DeprecatedNames = parser.ListDeprecatedFunctions(app.AppHandlers)
		respObj.OverloadedNames = parser.ListOverloadedFunctions(app.AppHandlers)
		respData = append(respData, respObj)
	}

	runtimeInfo.OnlyDescription = true
	runtimeInfo.Description = respData
}

func (m *ServiceMgr) getTempStoreHandlerHelper(cred cbauth.Creds) []application {
	applications := m.getTempStoreAll()
	tempApps := make([]application, 0, len(applications))

	// Remove curl creds and "num_timer_partitions" before sending it to the UI
	for _, app := range applications {
		info := m.checkPermissionFromCred(cred, app.Name, rbac.HandlerGetPermissions, false)
		if info.ErrCode != response.Ok {
			continue
		}
		if _, ok := app.Settings["num_timer_partitions"]; ok {
			delete(app.Settings, "num_timer_partitions")
		}
		redactSesitiveData(&app)
		tempApps = append(tempApps, app)
	}
	return tempApps
}

func (m *ServiceMgr) getTempStoreHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetFunctionDraft)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	tempApps := m.getTempStoreHandlerHelper(cred)

	runtimeInfo.Description = tempApps
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) getTempStore(appName string) (application, *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::getTempStore"
	logging.Infof("%s Function: %s fetching function draft definitions", logPrefix, appName)

	app, info := m.getAppFromTempStore(appName)
	if info.ErrCode != response.Ok {
		logging.Errorf("%s %s", logPrefix, info.Description)
		return app, info
	}

	m.addDefaultDeploymentConfig(&app)
	m.maybeReplaceFromPrior(&app)
	delete(app.Settings, "handler_uuid")
	return app, info
}

func (m *ServiceMgr) getTempStoreAll() []application {
	m.fnMu.RLock()
	defer m.fnMu.RUnlock()

	applications := []application{}

	for _, tempApp := range m.fnsInTempStore {
		app := tempApp.copy()
		app.Owner = nil
		m.maybeDeleteLifeCycleState(&app)
		m.addDefaultDeploymentConfig(&app)
		m.maybeReplaceFromPrior(&app)
		applications = append(applications, app)
	}

	return applications
}

func (m *ServiceMgr) getAppFromTempStore(appName string) (application, *response.RuntimeInfo) {
	info := &response.RuntimeInfo{}

	m.fnMu.Lock()
	defer m.fnMu.Unlock()

	app, ok := m.fnsInTempStore[appName]
	if ok {
		return app.copy(), info
	}

	// Possible due to metakv callback delay
	// Check if app exists in metakv store
	return m.getAppFromMetakvTempStore(appName)
}

func (m *ServiceMgr) getAppFromMetakvTempStore(appName string) (application, *response.RuntimeInfo) {
	info := &response.RuntimeInfo{}

	app := application{}

	data, err := util.ReadAppContent(metakvTempAppsPath, metakvTempChecksumPath, appName)
	if err == util.AppNotExist {
		info.ErrCode = response.ErrAppNotFoundTs
		info.Description = fmt.Sprintf("Function: %s not found", appName)
		return app, info
	}

	if err != nil || data == nil {
		info.ErrCode = response.ErrInternalServer
		return app, info
	}

	uErr := json.Unmarshal(data, &app)
	if uErr != nil {
		info.ErrCode = response.ErrReadReq
		info.Description = fmt.Sprintf("Unmarshalling from metakv failed for Function: %s", appName)
		return application{}, info
	}

	return app, info
}

func (m *ServiceMgr) saveTempStoreHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::saveTempStoreHandler"
	res := response.NewResponseWriter(w, r, response.EventUpdateFunction)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)

	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	logging.Infof("%s REST Call: %v %v", logPrefix, r.URL.Path, r.Method)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logging.Errorf("%s Function: %s failed to read request body, err: %v", logPrefix, appName, err)
		runtimeInfo.ErrCode = response.ErrReadReq
		runtimeInfo.Description = fmt.Sprintf("Failed to read request body, err: %v", err)
		return
	}

	var app application
	err = json.Unmarshal(data, &app)
	if err != nil {
		errString := fmt.Sprintf("Function: %s failed to unmarshal payload err: %v", appName, err)
		logging.Errorf("%s %s, err: %v", logPrefix, errString)
		runtimeInfo.ErrCode = response.ErrUnmarshalPld
		runtimeInfo.Description = errString
		return
	}
	res.AddRequestData("body", app)

	m.addDefaultTimerPartitionsIfMissing(&app)

	m.addDefaultDeploymentConfig(&app)
	if info := m.validateApplication(&app); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	oldApp, oldInfo := m.getTempStore(appName)
	if oldInfo.ErrCode == response.Ok {
		copyPasswords(&app, &oldApp)
	} else if oldInfo.ErrCode != response.ErrAppNotFoundTs {
		*runtimeInfo = *oldInfo
		return
	}

	info = m.saveTempStore(app)
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	deprecatedFnsList := parser.ListDeprecatedFunctions(app.AppHandlers)
	overloadedFnsList := parser.ListOverloadedFunctions(app.AppHandlers)
	warningList := make([]string, 0, 2)
	if len(deprecatedFnsList) > 0 {
		jsonList, _ := json.Marshal(deprecatedFnsList)
		warningList = append(warningList, fmt.Sprintf("%s; Deprecated: %s", info.Description, jsonList))
	}
	if len(overloadedFnsList) > 0 {
		jsonList, _ := json.Marshal(overloadedFnsList)
		warningList = append(warningList, fmt.Sprintf("%s; Overloaded: %s", info.Description, jsonList))
	}

	if len(warningList) > 0 {
		info.WarningInfo = warningList
	}
	*runtimeInfo = *info
}

// Saves application to temp store
func (m *ServiceMgr) saveTempStore(app application) (info *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::saveTempStore"
	appName := app.Name

	currApp, info := m.checkAppExists(app.Name)
	if info.ErrCode == response.Ok {
		ok := currApp.functionScopeEquals(app)
		if !ok {
			info.ErrCode = response.ErrInvalidRequest
			info.Description = fmt.Sprintf("Function scope cannot be changed")
			return
		}

		copyPasswords(&app, currApp)
		app.Owner = currApp.Owner
	} else if info.ErrCode == response.ErrAppNotFoundTs {
		// Its a new app so its not defined yet
		info.ErrCode = response.Ok
		info.Description = ""
	} else {
		return
	}

	depConfig, dOk := app.Settings["deployment_status"].(bool)
	processConfig, pOk := app.Settings["deployment_status"].(bool)
	if dOk && depConfig && pOk && processConfig {
		info = m.checkPermissionWithOwner(app)
		if info.ErrCode != response.Ok {
			return
		}
	}

	data, err := json.MarshalIndent(app, "", " ")
	if err != nil {
		info.ErrCode = response.ErrMarshalResp
		info.Description = fmt.Sprintf("Function: %s failed to marshal data, err : %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	//Delete stale entry
	err = util.DeleteStaleAppContent(metakvTempAppsPath, appName)
	if err != nil {
		info.ErrCode = response.ErrSaveAppTs
		info.Description = fmt.Sprintf("Function: %s failed to clean up stale entry from temp store, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	compressPayload := m.checkCompressHandler()
	err = util.WriteAppContent(metakvTempAppsPath, metakvTempChecksumPath, appName, data, compressPayload)
	if err != nil {
		info.ErrCode = response.ErrSaveAppTs
		info.Description = fmt.Sprintf("Function: %s failed to store in temp store, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	m.fnMu.Lock()
	defer m.fnMu.Unlock()
	// Cache the app in temp store for faster access on this node
	m.fnsInTempStore[app.Name] = &app

	info.ErrCode = response.Ok
	info.Description = fmt.Sprintf("Function: %s stored in temp store", appName)
	logging.Infof("%s %s", logPrefix, info.Description)
	return
}

func (m *ServiceMgr) savePrimaryStoreHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::savePrimaryStoreHandler"

	res := response.NewResponseWriter(w, r, response.EventUpdateFunction)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)

	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	logging.Infof("%s REST Call: %v %v", logPrefix, r.URL.Path, r.Method)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errString := fmt.Sprintf("Function: %s failed to read content from http request body, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, errString)
		info.ErrCode = response.ErrReadReq
		info.Description = errString
		return
	}

	var app application
	err = json.Unmarshal(data, &app)
	if err != nil {
		errString := fmt.Sprintf("Function: %s failed to unmarshal payload err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, errString)
		info.ErrCode = response.ErrReadReq
		info.Description = errString
		return
	}
	res.AddRequestData("body", app)

	m.addDefaultDeploymentConfig(&app)
	m.addDefaultTimerPartitionsIfMissing(&app)

	if info := m.validateApplication(&app); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	info = m.savePrimaryStore(&app)
	*runtimeInfo = *info
}

func (m *ServiceMgr) checkRebalanceStatus() (info *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::checkRebalanceStatus"
	info = &response.RuntimeInfo{}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	rebStatus, err := util.CheckIfRebalanceOngoing("/getRebalanceStatus", m.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("%s Failed to grab correct rebalance or failover status from some/all Eventing nodes, err: %v", logPrefix, err)
		info.ErrCode = response.ErrGetRebStatus
		info.Description = "Failed to get rebalance or failover status from eventing nodes"
		return
	}

	logging.Infof("%s Rebalance or Failover ongoing across some/all Eventing nodes: %v", logPrefix, rebStatus)

	if rebStatus {
		logging.Warnf("%s Rebalance or Failover ongoing on some/all Eventing nodes", logPrefix)
		info.ErrCode = response.ErrRebOrFailoverOngoing
		info.Description = "Rebalance or Failover processing ongoing on some/all Eventing nodes, creating new functions, deployment or undeployment of existing functions is not allowed"
		return
	}

	return
}

func filterFeedBoundary(settings map[string]interface{}) common.DcpStreamBoundary {
	if val, ok := settings["dcp_stream_boundary"]; ok {
		if boundary, bOk := val.(string); bOk {
			return common.StreamBoundary(boundary)
		}
	}

	return common.StreamBoundary("")
}

// Saves application to metakv and returns appropriate success/error code
func (m *ServiceMgr) savePrimaryStore(app *application) (info *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::savePrimaryStore"

	logging.Infof("%s Function: %s saving to primary store", logPrefix, app.Name)

	currApp, info := m.checkAppExists(app.Name)
	if info.ErrCode == response.Ok {
		ok := currApp.functionScopeEquals(*app)
		if !ok {
			info.ErrCode = response.ErrInvalidRequest
			info.Description = fmt.Sprintf("Function scope cannot be changed")
			return
		}

		app.Owner = currApp.Owner
	} else if info.ErrCode == response.ErrAppNotFoundTs {
		// Its a new app so its not defined yet
		info.ErrCode = response.Ok
		info.Description = ""
	} else {
		return
	}

	depConfig, dOk := app.Settings["deployment_status"].(bool)
	processConfig, pOk := app.Settings["deployment_status"].(bool)
	if dOk && depConfig && pOk && processConfig {
		info = m.checkPermissionWithOwner(*app)
		if info.ErrCode != response.Ok {
			return
		}
	}

	if info = m.checkLifeCycleOpsDuringRebalance(); info.ErrCode != response.Ok {
		return
	}

	if m.checkIfDeployed(app.Name) && m.superSup.GetAppCompositeState(app.Name) != common.AppStatePaused {
		info.ErrCode = response.ErrAppDeployed
		info.Description = fmt.Sprintf("Function: %s another function with same name is already deployed, skipping save request", app.Name)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	sourceKeyspace := common.Keyspace{BucketName: app.DeploymentConfig.SourceBucket,
		ScopeName:      app.DeploymentConfig.SourceScope,
		CollectionName: app.DeploymentConfig.SourceCollection}

	metadataKeyspace := common.Keyspace{BucketName: app.DeploymentConfig.MetadataBucket,
		ScopeName:      app.DeploymentConfig.MetadataScope,
		CollectionName: app.DeploymentConfig.MetadataCollection}

	if sourceKeyspace == metadataKeyspace {
		info.ErrCode = response.ErrSrcMbSame
		info.Description = fmt.Sprintf("Function: %s source keyspace same as metadata keyspace. source : %s metadata : %s",
			app.Name, sourceKeyspace, metadataKeyspace)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	mhVersion := common.CouchbaseVerMap["mad-hatter"]
	if dOk && depConfig && pOk && processConfig && m.superSup.GetAppCompositeState(app.Name) == common.AppStatePaused && !m.compareEventingVersion(mhVersion) {
		info.ErrCode = response.ErrClusterVersion
		info.Description = fmt.Sprintf("All eventing nodes in the cluster must be on version %s or higher for using the pause functionality",
			mhVersion)
		logging.Warnf("%s Version compat check failed: %s", logPrefix, info.Description)
		return
	}

	srcMutationEnabled := m.isSrcMutationEnabled(&app.DeploymentConfig)
	if srcMutationEnabled && !m.compareEventingVersion(mhVersion) {
		info.ErrCode = response.ErrClusterVersion
		info.Description = fmt.Sprintf("All eventing nodes in the cluster must be on version %s or higher for allowing mutations against source bucket",
			mhVersion)
		logging.Warnf("%s Version compat check failed: %s", logPrefix, info.Description)
		return
	}

	if srcMutationEnabled {
		keySpace := &common.Keyspace{BucketName: app.DeploymentConfig.SourceBucket,
			ScopeName:      app.DeploymentConfig.SourceScope,
			CollectionName: app.DeploymentConfig.SourceCollection,
		}
		if enabled, err := util.IsSyncGatewayEnabled(logPrefix, keySpace, m.restPort, m.superSup); err == nil && enabled {
			info.ErrCode = response.ErrSyncGatewayEnabled
			info.Description = fmt.Sprintf("SyncGateway is enabled on: %s, deployement of source bucket mutating handler will cause Intra Bucket Recursion", app.DeploymentConfig.SourceBucket)
			return
		}
	}

	logging.Infof("%v Function UUID: %v for function name: %v stored in primary store", logPrefix, app.FunctionID, app.Name)

	preparedApplication, _ := applicationAdapter(app)
	appContent := util.EncodeAppPayload(&preparedApplication)

	compressPayload := m.checkCompressHandler()
	payload, err := util.MaybeCompress(appContent, compressPayload)
	if err != nil {
		info.ErrCode = response.ErrSaveAppPs
		info.Description = fmt.Sprintf("Function: %s Error in compressing: %v", app.Name, err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}
	if len(payload) > util.MaxFunctionSize() {
		info.ErrCode = response.ErrAppCodeSize
		info.Description = fmt.Sprintf("Function: %s handler Code size is more than %d. Code Size: %d", app.Name, util.MaxFunctionSize(), len(payload))
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	c := &consumer.Consumer{}
	var handlerHeaders []string
	if headers, exists := app.Settings["handler_headers"]; exists {
		handlerHeaders = util.ToStringArray(headers)
	} else {
		handlerHeaders = common.GetDefaultHandlerHeaders()
	}

	var n1qlParams string
	if consistency, exists := app.Settings["n1ql_consistency"]; exists {
		n1qlParams = "{ 'consistency': '" + consistency.(string) + "' }"
	}
	parsedCode, _ := parser.TranspileQueries(app.AppHandlers, n1qlParams)

	handlerFooters := util.ToStringArray(app.Settings["handler_footers"])
	compilationInfo, err := c.SpawnCompilationWorker(parsedCode, string(appContent), app.Name, m.adminHTTPPort,
		handlerHeaders, handlerFooters)
	if err != nil || !compilationInfo.CompileSuccess {
		info.ErrCode = response.ErrHandlerCompile
		info.Description = compilationInfo
		return
	}

	preparedApp, _ := applicationAdapter(app)
	appContent = util.EncodeAppPayload(&preparedApp)
	settingsPath := metakvAppSettingsPath + app.Name
	settings := app.Settings

	mData, mErr := json.MarshalIndent(&settings, "", " ")
	if mErr != nil {
		info.ErrCode = response.ErrMarshalResp
		info.Description = fmt.Sprintf("Function: %s failed to marshal settings, err: %v", app.Name, mErr)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	//Delete stale entry
	err = util.DeleteStaleAppContent(metakvAppsPath, app.Name)
	if err != nil {
		info.ErrCode = response.ErrSaveAppPs
		info.Description = fmt.Sprintf("Function: %s failed to clean up stale entry, err: %v", app.Name, err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	err = util.WriteAppContent(metakvAppsPath, metakvChecksumPath, app.Name, appContent, compressPayload)
	if err != nil {
		info.ErrCode = response.ErrSaveAppPs
		info.Description = fmt.Sprintf("Function: %s unable to save to primary store, err: %v", app.Name, err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	mkvErr := util.MetakvSet(settingsPath, mData, nil)
	if mkvErr != nil {
		info.ErrCode = response.ErrSetSettingsPs
		info.Description = fmt.Sprintf("Function: %s failed to store updated settings in metakv, err: %v", app.Name, mkvErr)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	wInfo := m.determineWarnings(app, compilationInfo)
	info.WarningInfo = *wInfo
	return
}

func (m *ServiceMgr) determineWarnings(app *application, compilationInfo *common.CompileStatus) *response.WarningsInfo {
	wInfo := &response.WarningsInfo{}
	wInfo.Status = fmt.Sprintf("Stored function: '%s' in metakv", app.Name)

	curlWarning, err := m.determineCurlWarning(app)
	if err != nil {
		logging.Errorf("Function: %s unable to determine curl warnings, err : %v", app.Name, err)
	} else if curlWarning != "" {
		wInfo.Warnings = append(wInfo.Warnings, curlWarning)
	}

	numWarnings := len(wInfo.Warnings)
	if numWarnings > 0 {
		wInfo.Warnings[numWarnings-1] += " Do not use in production environments"
	}
	return wInfo
}

func (m *ServiceMgr) determineCurlWarning(app *application) (string, error) {
	nsServerEndpoint := net.JoinHostPort(util.Localhost(), m.restPort)
	cic, err := util.FetchClusterInfoClient(nsServerEndpoint)
	if err != nil {
		return "", err
	}
	clusterInfo := cic.GetClusterInfoCache()
	clusterInfo.RLock()
	defer clusterInfo.RUnlock()

	allNodes := clusterInfo.GetAllNodes()
	for _, curl := range app.DeploymentConfig.Curl {
		parsedUrl, err := url.Parse(curl.Hostname)
		if err != nil {
			return fmt.Sprintf("Unable to parse URL of cURL binding alias %s, err : %v", curl.Value, err), nil
		}

		node, err := util.NewNodeWithScheme(parsedUrl.Host, parsedUrl.Scheme)
		if err != nil {
			return fmt.Sprintf("Unable to resolve hostname for cURL binding alias %s, err : %v", curl.Value, err), nil
		}
		if node.HasLoopbackAddress() {
			msg := fmt.Sprintf(" Function '%s' has a curl binding to a Couchbase node in the same cluster.", app.Name)
			return msg, nil
		}

		for _, nodeInCluster := range allNodes {
			if node.IsEqual(nodeInCluster) {
				msg := fmt.Sprintf(" Function '%s' has a curl binding to a Couchbase node in the same cluster.", app.Name)
				return msg, nil
			}
		}
	}
	return "", nil
}

func (m *ServiceMgr) getErrCodes(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetErrCodes)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	_, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = string(m.statusPayload)
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) getDcpEventsRemaining(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetDcpEventsRemaining)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)

	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerGetPermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if m.checkIfDeployed(appName) {
		eventsRemaining := m.superSup.GetDcpEventsRemainingToProcess(appName)
		resp := backlogStat{DcpBacklog: eventsRemaining}
		runtimeInfo.Description = resp
		runtimeInfo.OnlyDescription = true
		return
	}

	runtimeInfo.ErrCode = response.ErrAppNotDeployed
	runtimeInfo.Description = fmt.Sprintf("Function: %s not deployed", appName)
}

func (m *ServiceMgr) getAggPausingApps(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getAggPausingApps"

	res := response.NewResponseWriter(w, r, response.EventGetPausingApps)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingReadPermissions, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	appsPausing, err := util.GetAggPausingApps("/getPausingApps", m.eventingNodeAddrs)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("Failed to grab pausing function list err: %v", err)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	runtimeInfo.Description = strconv.FormatBool(appsPausing)
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) getAggBootstrappingApps(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getAggBootstrappingApps"

	res := response.NewResponseWriter(w, r, response.EventGetBootstrappingApps)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingReadPermissions, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	appsBootstrapping, err := util.GetAggBootstrappingApps("/getBootstrappingApps", m.eventingNodeAddrs)
	if appsBootstrapping {
		runtimeInfo.SendRawDescription = true
		runtimeInfo.Description = strconv.FormatBool(appsBootstrapping)
		runtimeInfo.OnlyDescription = true
		return
	}

	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("Failed to grab bootstrapping function list err: %v", err)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = strconv.FormatBool(appsBootstrapping)
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) getPausingApps(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetPausingApps)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	pausingApps := m.superSup.PausingAppList()
	permPausing := make(map[string]string)
	for appName, val := range pausingApps {
		info := m.checkPermissionFromCred(cred, appName, rbac.HandlerGetPermissions, false)
		if info.ErrCode != response.Ok {
			continue
		}
		permPausing[appName] = val
	}

	runtimeInfo.Description = permPausing
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) getBootstrappingApps(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetBootstrappingApps)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	bootstrappingApps := m.superSup.BootstrapAppList()
	permBootstrapping := make(map[string]string)
	for appName, val := range bootstrappingApps {
		info := m.checkPermissionFromCred(cred, appName, rbac.HandlerGetPermissions, false)
		if info.ErrCode != response.Ok {
			continue
		}
		permBootstrapping[appName] = val
	}

	runtimeInfo.Description = permBootstrapping
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) getEventingConsumerPids(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetConsumerPids)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)

	appName, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerGetPermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if m.checkIfDeployed(appName) {
		workerPidMapping := m.superSup.GetEventingConsumerPids(appName)
		runtimeInfo.Description = workerPidMapping
		runtimeInfo.OnlyDescription = true
		return
	}

	runtimeInfo.ErrCode = response.ErrAppNotDeployed
	runtimeInfo.Description = fmt.Sprintf("Function: %s not deployed", appName)
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) getCreds(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getCreds"

	if !m.validateLocalAuth(w, r) {
		return
	}

	m.lcbCredsCounter++

	w.Header().Set("Content-Type", "application/x-www-form-urlencoded")

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(http.StatusBadRequest))
		fmt.Fprintf(w, "Failed to read request body, err: %v", err)
		return
	}

	strippedEndpoint := util.StripScheme(string(data))
	username, password, err := cbauth.GetMemcachedServiceAuth(strippedEndpoint)
	if err != nil {
		logging.Errorf("%s Failed to get credentials for endpoint: %rs, err: %v", logPrefix, strippedEndpoint, err)
		w.Header().Add(headerKey, strconv.Itoa(http.StatusInternalServerError))
		fmt.Fprintf(w, "Failed to get credentials for endpoint: %rs, err: %v", strippedEndpoint, err)
		return
	}
	response := url.Values{}
	response.Add("username", username)
	response.Add("password", password)

	w.Header().Add(headerKey, strconv.Itoa(http.StatusOK))
	fmt.Fprintf(w, "%s", response.Encode())
}

func (m *ServiceMgr) getKVNodesAddresses(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getKVNodesAddresses"
	if !m.validateLocalAuth(w, r) {
		return
	}

	w.Header().Set("Content-Type", "application/json")

	nsServer := net.JoinHostPort(util.Localhost(), m.restPort)
	cic, err := util.FetchClusterInfoClient(nsServer)
	if err != nil {
		logging.Errorf("%s Failed to get cluster info cache, err : %v", logPrefix, err)
		return
	}

	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	kvNodes, err := cinfo.GetAddressOfActiveKVNodes()
	cinfo.RUnlock()
	if err != nil {
		logging.Errorf("%s Failed to get KV nodes addresses, err : %v", logPrefix, err)
		return
	}

	response := make(map[string]interface{})
	response["is_error"] = false
	response["kv_nodes"] = kvNodes
	data, err := json.Marshal(response)
	if err != nil {
		fmt.Fprintf(w, `{"is_error" : true, "error" : %s}`, strconv.Quote(err.Error()))
		return
	}
	w.Write(data)
}

// This clears all the stats so only admin can do it
func (m *ServiceMgr) clearEventStats(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::clearEventStats"
	res := response.NewResponseWriter(w, r, response.EventClearStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	logging.Infof("%s Got request to clear event stats from host: %rs", logPrefix, r.Host)
	appNames := m.superSup.ClearEventStats()

	runtimeInfo.ExtraAttributes = map[string]interface{}{"appNames": appNames}
	runtimeInfo.Description = "Stats cleared"
}

func (m *ServiceMgr) getConfig() (common.Config, *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::getConfig"

	var c common.Config
	info := &response.RuntimeInfo{}
	data, err := util.MetakvGet(metakvConfigPath)
	if err != nil {
		info.ErrCode = response.ErrGetConfig
		info.Description = fmt.Sprintf("failed to get config, err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return c, info
	}

	if !bytes.Equal(data, nil) {
		err = json.Unmarshal(data, &c)
		if err != nil {
			info.ErrCode = response.ErrUnmarshalPld
			info.Description = fmt.Sprintf("failed to unmarshal payload from metakv, err: %v", err)
			logging.Errorf("%s %s", logPrefix, info.Description)
			return c, info
		}
	}

	logging.Infof("%s Retrieving config from metakv: %+v", logPrefix, c)
	return c, info
}

func (m *ServiceMgr) saveConfig(c common.Config) *response.RuntimeInfo {
	logPrefix := "ServiceMgr::saveConfig"

	storedConfig, info := m.getConfig()
	if info.ErrCode != response.Ok {
		return info
	}

	data, err := json.MarshalIndent(util.SuperImpose(c, storedConfig), "", " ")
	if err != nil {
		info.ErrCode = response.ErrMarshalResp
		info.Description = fmt.Sprintf("failed to marshal config, err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return info
	}

	logging.Infof("%s Saving config into metakv: %v", logPrefix, c)

	err = util.MetakvSet(metakvConfigPath, data, nil)
	if err != nil {
		logging.Errorf("%s Failed to write to metakv err: %v", logPrefix, err)
		info.ErrCode = response.ErrMetakvWriteFailed
		info.Description = fmt.Sprintf("Failed to write to metakv, err: %v", err)
		return info
	}

	return info
}

func (m *ServiceMgr) configHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::configHandler"
	res := response.NewResponseWriter(w, r, response.EventGetConfig)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.ValidateAuthForOp(r, rbac.EventingAnyManageReadPermissions, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	logging.Infof("%s REST Call: %v %v", logPrefix, r.URL.Path, r.Method)

	switch r.Method {
	case "GET":
		c, info := m.getConfig()
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		runtimeInfo.Description = c
		runtimeInfo.OnlyDescription = true

	case "POST":
		res.SetRequestEvent(response.EventSaveConfig)

		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrReadReq
			runtimeInfo.Description = fmt.Sprintf("failed to read request body, err: %v", err)
			logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
			return
		}

		var c common.Config
		err = json.Unmarshal(data, &c)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrUnmarshalPld
			runtimeInfo.Description = fmt.Sprintf("failed to unmarshal config from metakv, err: %v", err)
			logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
			return
		}
		res.AddRequestData("body", c)

		if info := m.validateConfig(c); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		if value, exists := c["enable_debugger"]; exists {
			securitySetting := m.superSup.GetSecuritySetting()
			DisableNonSSLPorts := false
			if securitySetting != nil {
				DisableNonSSLPorts = securitySetting.DisableNonSSLPorts
			}
			if value == true && DisableNonSSLPorts == true {
				runtimeInfo.ErrCode = response.ErrDebuggerDisabled
				runtimeInfo.Description = "Debugger cannot be enabled as encryption level is strict"
				logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
				return
			}
		}

		if info := m.saveConfig(c); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		response := configResponse{false}
		runtimeInfo.Description = response
		runtimeInfo.OnlyDescription = true
		runtimeInfo.ExtraAttributes = map[string]interface{}{"body": c}

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET and POST are allowed"
		return
	}
}

func (m *ServiceMgr) assignFunctionID(fnName string, app *application) error {
	logPrefix := "ServiceMgr::assignFunctionID"

	data, err := util.ReadAppContent(metakvAppsPath, metakvChecksumPath, fnName)
	if err == util.AppNotExist {
		var uErr error
		app.FunctionID, uErr = util.GenerateFunctionID()
		if uErr != nil {
			return fmt.Errorf("Function: %s FunctionID generation failed err: %v", fnName, uErr)
		}
		logging.Infof("%s Function: %s FunctionID: %d generated", logPrefix, app.Name, app.FunctionID)
		return nil
	}

	if err != nil {
		logging.Errorf("%s err: %v", logPrefix, err)
		return fmt.Errorf("Function: %s failed to read definitions from metakv", fnName)
	}

	tApp := m.parseFunctionPayload(data, fnName)
	app.FunctionID = tApp.FunctionID
	logging.Infof("%s Function: %s assigned previous function ID: %d", logPrefix, app.Name, app.FunctionID)
	return nil
}

func (m *ServiceMgr) assignFunctionInstanceID(functionName string, app *application) error {
	logPrefix := "ServiceMgr:assignFunctionInstanceID"

	if m.superSup.GetAppCompositeState(functionName) != common.AppStatePaused {
		fiid, err := util.GenerateFunctionInstanceID()
		if err != nil {
			logging.Errorf("%s err: %v", logPrefix, err)
			return fmt.Errorf("FunctionInstanceID generation failed. err: %v", err)
		}
		app.FunctionInstanceID = fiid
		logging.Infof("%s Function: %s FunctionInstanceID: %s generated", logPrefix, app.Name, app.FunctionInstanceID)
	} else {
		data, err := util.ReadAppContent(metakvAppsPath, metakvChecksumPath, functionName)
		if err != nil || data == nil {
			return fmt.Errorf("Function: %s failed to read definitions from metakv. err: %v", functionName, err)
		}
		prevApp := m.parseFunctionPayload(data, functionName)
		app.FunctionInstanceID = prevApp.FunctionInstanceID
		logging.Infof("%s Function: %s assigned previous FunctionInstanceID: %s", logPrefix, app.Name, app.FunctionInstanceID)
	}
	return nil
}

func (m *ServiceMgr) isUndeployOperation(settings map[string]interface{}) bool {
	if len(settings) < 2 {
		return false
	}

	pstatus := true
	if val, ok := settings["processing_status"]; ok {
		if pstatus, ok = val.(bool); !ok {
			return false
		}
	} else {
		return false
	}

	dstatus := true
	if val, ok := settings["deployment_status"]; ok {
		if dstatus, ok = val.(bool); !ok {
			return false
		}
	} else {
		return false
	}
	return (pstatus == false) && (dstatus == false)
}

func (m *ServiceMgr) functionsHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::functionsHandler"

	if r.Method != "GET" { // We do not want to flood logs with GET calls
		logging.Infof("%s REST Call: %v %v", logPrefix, r.URL.Path, r.Method)
	}

	functions := regexp.MustCompile("^/api/v1/functions/?$")
	functionsName := regexp.MustCompile("^/api/v1/functions/(.*[^/])/?$") // Match is agnostic of trailing '/'
	functionsNameSettings := regexp.MustCompile("^/api/v1/functions/(.*[^/])/settings/?$")
	functionsNameRetry := regexp.MustCompile("^/api/v1/functions/(.*[^/])/retry/?$")
	functionsDeploy := regexp.MustCompile("^/api/v1/functions/(.*[^/])/deploy/?$")
	functionsUndeploy := regexp.MustCompile("^/api/v1/functions/(.*[^/])/undeploy/?$")
	functionsPause := regexp.MustCompile("^/api/v1/functions/(.*[^/])/pause/?$")
	functionsResume := regexp.MustCompile("^/api/v1/functions/(.*[^/])/resume/?$")
	functionsAppcode := regexp.MustCompile("^/api/v1/functions/(.*[^/])/appcode(/checksum)?/?$")
	functionsConfig := regexp.MustCompile("^/api/v1/functions/(.*[^/])/config/?$")

	if match := functionsNameRetry.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		m.functionNameRetry(w, r, match[1])

	} else if match := functionsNameSettings.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		m.functionNameSettings(w, r, match[1])

	} else if match := functionsAppcode.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		m.functionAppcode(w, r, match[1], match[2])

	} else if match := functionsConfig.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		m.functionConfig(w, r, match[1])

	} else if match := functionsPause.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		m.functionPause(w, r, match[1])

	} else if match := functionsResume.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		m.functionResume(w, r, match[1])

	} else if match := functionsDeploy.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		m.functionDeploy(w, r, match[1])

	} else if match := functionsUndeploy.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		m.functionUndeploy(w, r, match[1])

	} else if match := functionsName.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		m.functionName(w, r, match[1])

	} else if match := functions.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		m.functions(w, r)
	}
}

// Helper functions for lifecycle operations
// It will authenticate and sends back the result to http request
func (m *ServiceMgr) functionNameRetry(w http.ResponseWriter, r *http.Request, appName string) {
	logPrefix := "service::functionNameRetry"
	res := response.NewResponseWriter(w, r, response.EventUpdateFunction)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if r.Method != "POST" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only POST is allowed"
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrReadReq
		runtimeInfo.Description = fmt.Sprintf("failed to read request body, err : %v", err)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	var retryBody retry
	err = json.Unmarshal(data, &retryBody)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrMarshalResp
		runtimeInfo.Description = fmt.Sprintf("failed to unmarshal retry, err: %v", err)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}
	res.AddRequestData("body", retryBody)

	if info := m.notifyRetryToAllProducers(appName, &retryBody); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}
}

func (m *ServiceMgr) functionNameSettings(w http.ResponseWriter, r *http.Request, appName string) {
	logPrefix := "serviceMgr::functionNameSettings"
	res := response.NewResponseWriter(w, r, response.EventUpdateFunction)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	rPerm, wPerm, info := m.getReadAndWritePermission(appName)
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if notAllowed, err := rbac.ValidateAuthForOp(r, rPerm, wPerm, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	switch r.Method {
	case "GET":
		res.SetRequestEvent(response.EventGetFunctionDraft)
		settings, info := m.getSettings(appName)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		runtimeInfo.Description = settings
		runtimeInfo.OnlyDescription = true

	case "POST":
		res.SetRequestEvent(response.EventUpdateFunction)

		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrReadReq
			runtimeInfo.Description = fmt.Sprintf("failed to read request body, err : %v", err)
			logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
			return
		}

		app, info := m.getTempStore(appName)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		if app.EnforceSchema == true {
			info = m.MaybeEnforceSettingsSchema(data)
			if info.ErrCode != response.Ok {
				*runtimeInfo = *info
				return
			}
		}

		var settings map[string]interface{}
		err = json.Unmarshal(data, &settings)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrUnmarshalPld
			runtimeInfo.Description = fmt.Sprintf("failed to unmarshal retry, err: %v", err)
			logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
			return
		}
		res.AddRequestData("body", settings)

		if info = m.setSettings(appName, data, false); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET and POST are allowed"
		return
	}
}

func (m *ServiceMgr) functionName(w http.ResponseWriter, r *http.Request, appName string) {
	logPrefix := "serviceMgr::functionName"

	res := response.NewResponseWriter(w, r, response.EventGetFunctionDraft)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	switch r.Method {
	case "GET":
		if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerGetPermissions, false); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		app, info := m.getTempStore(appName)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		m.maybeDeleteLifeCycleState(&app)
		redactSesitiveData(&app)

		runtimeInfo.Description = app
		runtimeInfo.OnlyDescription = true

	case "POST":
		res.SetRequestEvent(response.EventUpdateFunction)
		createFunction := false

		appInStore, info := m.checkAppExists(appName)
		if info.ErrCode == response.ErrAppNotFoundTs {
			info.ErrCode = response.Ok
			info.Description = ""
			res.SetRequestEvent(response.EventCreateFunction)
			createFunction = true
		} else if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		app, info := m.unmarshalApp(r)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
		res.AddRequestData("body", app)

		m.addDefaultVersionIfMissing(&app)
		m.addDefaultDeploymentConfig(&app)
		m.addDefaultTimerPartitionsIfMissing(&app)
		m.addLifeCycleStateByFunctionState(&app)

		isMixedMode, err := m.isMixedModeCluster()
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInternalServer
			runtimeInfo.Description = fmt.Sprintf("err: %v", err)
			logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
			return
		}

		if isMixedMode {
			runtimeInfo.ErrCode = response.ErrMixedMode
			runtimeInfo.Description = "Life-cycle operations except delete and undeploy are not allowed in a mixed mode cluster"
			return
		}

		info = m.MaybeEnforceFunctionSchema(app)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		if createFunction {
			info = m.verifyAndCreateApp(cred, &app)
			if info.ErrCode != response.Ok {
				*runtimeInfo = *info
				return
			}
		} else {
			if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
				*runtimeInfo = *info
				return
			}
		}

		if info = m.validateApplication(&app); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		if m.superSup.GetAppCompositeState(appName) != common.AppStateUndeployed {
			if !CheckIfAppKeyspacesAreSame(*appInStore, app) {
				runtimeInfo.ErrCode = response.ErrInvalidConfig
				runtimeInfo.Description = "Source and Meta Keyspaces can only be changed when the function is in undeployed state."
				return
			}
		}

		// Reject the request if there is a mismatch of app name in URL and body
		if app.Name != appName {
			runtimeInfo.ErrCode = response.ErrAppNameMismatch
			runtimeInfo.Description = fmt.Sprintf("function name in the URL (%s) and body (%s) must be same", appName, app.Name)
			logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
			return
		}

		err = m.assignFunctionID(appName, &app)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInternalServer
			runtimeInfo.Description = err
			return
		}

		err = m.assignFunctionInstanceID(appName, &app)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInternalServer
			runtimeInfo.Description = err
			return
		}

		if _, ok := app.Settings["language_compatibility"]; !ok {
			app.Settings["language_compatibility"] = common.LanguageCompatibility[0]
		}

		info = m.savePrimaryStore(&app)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
		// Save to temp store only if saving to primary store succeeds
		if info = m.saveTempStore(app); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
		*runtimeInfo = *info
		runtimeInfo.ExtraAttributes = map[string]interface{}{"appName": appName}

	case "DELETE":
		res.SetRequestEvent(response.EventDeleteFunction)
		if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		info := m.deletePrimaryStore(cred, appName)
		// Delete the application from temp store only if app does not exist in primary store
		// or if the deletion succeeds on primary store
		if info.ErrCode == response.ErrAppNotFoundTs || info.ErrCode == response.Ok {
			info = m.deleteTempStore(cred, appName)
		}

		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		runtimeInfo.Description = fmt.Sprintf("Function: %s deleting in background", appName)
		runtimeInfo.ExtraAttributes = map[string]interface{}{"appName": appName}

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET, POST and DELETE are allowed"
	}
}

// Checks for permission and add the intial fields
func (m *ServiceMgr) verifyAndCreateApp(cred cbauth.Creds, app *application) *response.RuntimeInfo {
	info := &response.RuntimeInfo{}

	rbacSupport := m.rbacSupport()
	if rbacSupport {
		fS := app.FunctionScope
		_, _, info = m.getBSId(&fS)
		if info.ErrCode != response.Ok {
			return info
		}
	} else {
		app.FunctionScope = common.FunctionScope{}
	}

	notAllowed, err := checkPermissions(app, cred)
	if err != nil {
		*info = getAuthErrorInfo(notAllowed, false, err)
		return info
	}

	if rbacSupport {
		name, domain := cred.User()
		uuid := ""
		// GetUserUuid returns error when domain is not local
		// This will ensure that eventing won't throw error when domain is local
		if domain == "local" {
			uuid, err = cbauth.GetUserUuid(name, domain)
			if err != nil {
				info.ErrCode = response.ErrInternalServer
				return info
			}
		}
		app.Owner = &common.Owner{
			UUID:   uuid,
			User:   name,
			Domain: domain,
		}
	} else {
		app.Owner = &common.Owner{}
	}

	if app.Settings["deployment_status"] != app.Settings["processing_status"] {
		app.Settings["deployment_status"] = false
		app.Settings["processing_status"] = false
	}

	// If the app doesn't exist or has 'from_prior', set the stream boundary to everything
	if val, ok := app.Settings["dcp_stream_boundary"]; !ok || val == "from_prior" {
		app.Settings["dcp_stream_boundary"] = "everything"
	}

	return info
}

func checkPermissions(app *application, creds cbauth.Creds) ([]string, error) {
	fg := app.FunctionScope
	ks := fg.ToKeyspace()

	mPermission := rbac.HandlerManagePermissions(ks)
	src := &common.Keyspace{
		BucketName:     app.DeploymentConfig.SourceBucket,
		ScopeName:      app.DeploymentConfig.SourceScope,
		CollectionName: app.DeploymentConfig.SourceCollection,
	}
	meta := &common.Keyspace{
		BucketName:     app.DeploymentConfig.MetadataBucket,
		ScopeName:      app.DeploymentConfig.MetadataScope,
		CollectionName: app.DeploymentConfig.MetadataCollection,
	}
	permission := append(mPermission, rbac.HandlerBucketPermissions(src, meta)...)
	notAllowed, err := rbac.IsAllowedCreds(creds, permission, true)
	return notAllowed, err
}

func (m *ServiceMgr) functions(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventListAllfunction)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	switch r.Method {
	case "GET":
		tempApps := m.getTempStoreHandlerHelper(cred)
		runtimeInfo.Description = tempApps
		runtimeInfo.OnlyDescription = true

	case "POST":
		res.SetRequestEvent(response.EventImportFunctions)
		appList, info := m.unmarshalAppList(r)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
		res.AddRequestData("body", appList)

		isMixedMode, err := m.isMixedModeCluster()
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInternalServer
			runtimeInfo.Description = fmt.Sprintf("err: %v", err)
			return
		}

		if isMixedMode {
			runtimeInfo.ErrCode = response.ErrMixedMode
			runtimeInfo.Description = "Life-cycle operations except delete and undeploy are not allowed in a mixed mode cluster"
			return
		}

		for _, app := range *appList {
			info = m.MaybeEnforceFunctionSchema(app)
			if info.ErrCode != response.Ok {
				*runtimeInfo = *info
				return
			}
		}

		infoList, importedFns := m.createApplications(cred, appList, false)
		runtimeInfo.ExtraAttributes = map[string]interface{}{"appNames": importedFns}
		runtimeInfo.Description = infoList
		runtimeInfo.OnlyDescription = true

	case "DELETE":
		res.SetRequestEvent(response.EventDeleteFunction)
		appsNames := m.getTempStoreAppNames()
		infoList := make([]*response.RuntimeInfo, 0, len(appsNames))

		for _, app := range appsNames {

			info := m.deletePrimaryStore(cred, app)
			// Delete the application from temp store only if app does not exist in primary store
			// or if the deletion succeeds on primary store
			if info.ErrCode != response.Ok && info.ErrCode != response.ErrAppNotFoundTs {
				infoList = append(infoList, info)
				continue
			}

			info = m.deleteTempStore(cred, app)
			if info.ErrCode == response.Ok {
				info.Description = fmt.Sprintf("Function: %s deleting in background", app)
			}
			infoList = append(infoList, info)
		}
		runtimeInfo.Description = infoList
		runtimeInfo.OnlyDescription = true

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET, POST and DELETE are allowed"
	}
}

func (m *ServiceMgr) functionUndeploy(w http.ResponseWriter, r *http.Request, appName string) {
	logPrefix := "serviceMgr::functionUndeploy"
	res := response.NewResponseWriter(w, r, response.EventUndeployFunction)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if r.Method != "POST" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only POST is allowed"
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	appState := m.superSup.GetAppCompositeState(appName)

	if appState == common.AppStateUndeployed {
		runtimeInfo.ErrCode = response.ErrAppNotDeployed
		runtimeInfo.Description = fmt.Sprintf("Invalid operation. Function: %v already in undeployed state.", appName)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	var settings = make(map[string]interface{})
	settings["deployment_status"] = false
	settings["processing_status"] = false

	data, err := json.MarshalIndent(settings, "", " ")
	if err != nil {
		runtimeInfo.ErrCode = response.ErrMarshalResp
		runtimeInfo.Description = fmt.Sprintf("failed to marshal function settings, err : %v", err)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	if info := m.setSettings(appName, data, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}
	runtimeInfo.Description = fmt.Sprintf("Function: %s is undeploying", appName)

}

func (m *ServiceMgr) functionDeploy(w http.ResponseWriter, r *http.Request, appName string) {
	logPrefix := "ServiceMgr::functionDeploy"
	res := response.NewResponseWriter(w, r, response.EventDeployFunction)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if r.Method != "POST" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only POST is allowed"
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	appState := m.superSup.GetAppCompositeState(appName)

	if appState == common.AppStateEnabled {
		runtimeInfo.ErrCode = response.ErrAppDeployed
		runtimeInfo.Description = fmt.Sprintf("Invalid operation. Function: %v already in deployed state.", appName)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	if appState == common.AppStatePaused {
		runtimeInfo.ErrCode = response.ErrAppPaused
		runtimeInfo.Description = fmt.Sprintf("Invalid operation. Function: %v already in paused state.", appName)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrReadReq
		runtimeInfo.Description = fmt.Sprintf("failed to read request body, err: %v", err)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	settings := make(map[string]interface{})
	if len(data) != 0 {
		err = json.Unmarshal(data, &settings)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrMarshalResp
			runtimeInfo.Description = fmt.Sprintf("%v failed to unmarshal setting supplied, err: %v", len(data), err)
			logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
			return
		}
		if settings == nil {
			runtimeInfo.ErrCode = response.ErrMarshalResp
			runtimeInfo.Description = fmt.Sprintf("%v failed to unmarshal setting supplied, data sent in the request body is invalid.", len(data))
			logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
			return
		}
	}

	app, info := m.getTempStore(appName)
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	if app.EnforceSchema == true {
		info = m.MaybeEnforceSettingsSchema(data)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
	}

	settings["deployment_status"] = true
	settings["processing_status"] = true

	data, err = json.MarshalIndent(settings, "", " ")
	if err != nil {
		runtimeInfo.ErrCode = response.ErrMarshalResp
		runtimeInfo.Description = fmt.Sprintf("failed to marshal function settings, err : %v", err)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	if info = m.setSettings(appName, data, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}
	runtimeInfo.Description = fmt.Sprintf("Function: %s is deploying", appName)
}

func (m *ServiceMgr) functionPause(w http.ResponseWriter, r *http.Request, appName string) {
	logPrefix := "serviceMgr::functionPause"
	res := response.NewResponseWriter(w, r, response.EventPauseFunction)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if r.Method != "POST" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only POST is allowed"
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	appState := m.superSup.GetAppCompositeState(appName)

	if appState == common.AppStatePaused {
		runtimeInfo.ErrCode = response.ErrAppPaused
		runtimeInfo.Description = fmt.Sprintf("Invalid operation. Function: %v already in paused state.", appName)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	var settings = make(map[string]interface{})
	settings["deployment_status"] = true
	settings["processing_status"] = false

	data, err := json.MarshalIndent(settings, "", " ")
	if err != nil {
		runtimeInfo.ErrCode = response.ErrMarshalResp
		runtimeInfo.Description = fmt.Sprintf("failed to marshal function settings, err : %v", err)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	if info := m.setSettings(appName, data, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}
	runtimeInfo.Description = fmt.Sprintf("Function: %s is Pausing", appName)
}

func (m *ServiceMgr) functionResume(w http.ResponseWriter, r *http.Request, appName string) {
	logPrefix := "serviceMgr::functionResume"
	res := response.NewResponseWriter(w, r, response.EventResumeFunction)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if r.Method != "POST" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only POST is allowed"
		return
	}

	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	appState := m.superSup.GetAppCompositeState(appName)

	if appState == common.AppStateEnabled {
		runtimeInfo.ErrCode = response.ErrAppDeployed
		runtimeInfo.Description = fmt.Sprintf("Invalid operation. Function: %v already in Deployed state.", appName)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	if appState == common.AppStateUndeployed {
		runtimeInfo.ErrCode = response.ErrAppNotDeployed
		runtimeInfo.Description = fmt.Sprintf("Invalid operation. Function: %v already in undeployed state.", appName)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	var settings = make(map[string]interface{})
	settings["deployment_status"] = true
	settings["processing_status"] = true

	data, err := json.MarshalIndent(settings, "", " ")
	if err != nil {
		runtimeInfo.ErrCode = response.ErrMarshalResp
		runtimeInfo.Description = fmt.Sprintf("failed to marshal function settings, err : %v", err)
		logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
		return
	}

	if info := m.setSettings(appName, data, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}
	runtimeInfo.Description = fmt.Sprintf("Function: %s is Resuming", appName)
}

func (m *ServiceMgr) functionAppcode(w http.ResponseWriter, r *http.Request, appName string, wantChecksum string) {
	logPrefix := "serviceMgr::functionAppcode"
	res := response.NewResponseWriter(w, r, response.EventGetFunctionDraft)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	switch r.Method {
	case "GET":
		if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		app, info := m.getTempStore(appName)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
		response := []byte(app.AppHandlers)
		if wantChecksum == "/checksum" {
			response = []byte(fmt.Sprintf("%x", sha256.Sum256(response)))
		}

		runtimeInfo.SendRawDescription = true
		runtimeInfo.Description = response
		runtimeInfo.OnlyDescription = true

	case "POST":
		res.SetRequestEvent(response.EventUpdateFunction)
		if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		appState := m.superSup.GetAppCompositeState(appName)
		if appState == common.AppStateEnabled {
			runtimeInfo.ErrCode = response.ErrAppDeployed
			runtimeInfo.Description = fmt.Sprintf("Function: %s is in deployed state, appcode can only be updated when a function is either undeployed or paused", appName)
			logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
			return
		}

		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrReadReq
			runtimeInfo.Description = fmt.Sprintf("Failed to read request body, err: %v", err)
			logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
			return
		}
		handler := string(data)
		res.AddRequestData("body", handler)

		if wantChecksum == "/checksum" {
			runtimeInfo.ErrCode = response.ErrMethodNotAllowed
			runtimeInfo.Description = "Method not allowed"
			return
		}

		app, info := m.getTempStore(appName)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
		app.AppHandlers = handler

		info = m.savePrimaryStore(&app)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		if info := m.saveTempStore(app); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		deprecatedFnsList := parser.ListDeprecatedFunctions(app.AppHandlers)
		overloadedFnsList := parser.ListOverloadedFunctions(app.AppHandlers)
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
		runtimeInfo.Description = fmt.Sprintf("Function: %s appcode stored in the metakv.", appName)

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET and POST are allowed"
		return
	}
}

func (m *ServiceMgr) functionConfig(w http.ResponseWriter, r *http.Request, appName string) {
	logPrefix := "serviceMgr::functionConfig"
	res := response.NewResponseWriter(w, r, response.EventGetFunctionDraft)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	switch r.Method {
	case "GET":
		if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		app, info := m.getTempStore(appName)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
		redactSesitiveData(&app)

		runtimeInfo.Description = app.DeploymentConfig
		runtimeInfo.OnlyDescription = true

	case "POST":
		res.SetRequestEvent(response.EventUpdateFunction)
		if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
		appState := m.superSup.GetAppCompositeState(appName)
		if appState == common.AppStateEnabled {
			runtimeInfo.ErrCode = response.ErrAppDeployed
			runtimeInfo.Description = fmt.Sprintf("Function: %s is in deployed state, config can only be updated when a function is either undeployed or paused", appName)
			logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
			return
		}

		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrReadReq
			runtimeInfo.Description = fmt.Sprintf("Failed to read request body, err: %v", err)
			logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
			return
		}

		app, info := m.getTempStore(appName)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
		appCopy := app

		config := depCfg{}
		unmarshalErr := json.Unmarshal(data, &config)
		if unmarshalErr != nil {
			runtimeInfo.ErrCode = response.ErrUnmarshalPld
			runtimeInfo.Description = fmt.Sprintf("Failed to Unmarshal request body, err: %v", err)
			logging.Errorf("%s %s", logPrefix, runtimeInfo.Description)
			return
		}
		res.AddRequestData("body", config)

		app.DeploymentConfig = config
		copyPasswords(&app, &appCopy)

		// Validate Recursion Checks and deployment configurations
		if info = m.validateApplication(&app); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
		// Don't allow the user to change the meta and source keyspaces
		if appState == common.AppStatePaused {
			if !CheckIfAppKeyspacesAreSame(appCopy, app) {
				runtimeInfo.ErrCode = response.ErrInvalidConfig
				runtimeInfo.Description = "Source and Meta Keyspaces can only be changed when the function is in undeployed state."
				return
			}
		}

		info = m.savePrimaryStore(&app)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		if info := m.saveTempStore(app); info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}

		runtimeInfo.Description = fmt.Sprintf("Function: %s config stored in the metakv.", appName)

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET and POST are allowed"
		return
	}
}

func (m *ServiceMgr) addDefaultVersionIfMissing(app *application) {
	if app.EventingVersion == "" {
		app.EventingVersion = util.EventingVer()
	}
}

func (m *ServiceMgr) addDefaultDeploymentConfig(app *application) {
	app.DeploymentConfig.SourceScope = common.CheckAndReturnDefaultForScopeOrCollection(app.DeploymentConfig.SourceScope)
	app.DeploymentConfig.SourceCollection = common.CheckAndReturnDefaultForScopeOrCollection(app.DeploymentConfig.SourceCollection)
	app.DeploymentConfig.MetadataScope = common.CheckAndReturnDefaultForScopeOrCollection(app.DeploymentConfig.MetadataScope)
	app.DeploymentConfig.MetadataCollection = common.CheckAndReturnDefaultForScopeOrCollection(app.DeploymentConfig.MetadataCollection)

	for i := range app.DeploymentConfig.Buckets {
		app.DeploymentConfig.Buckets[i].ScopeName = common.CheckAndReturnDefaultForScopeOrCollection(app.DeploymentConfig.Buckets[i].ScopeName)
		app.DeploymentConfig.Buckets[i].CollectionName = common.CheckAndReturnDefaultForScopeOrCollection(app.DeploymentConfig.Buckets[i].CollectionName)
	}
}

func (m *ServiceMgr) addDefaultTimerPartitionsIfMissing(app *application) {
	if _, ok := app.Settings["num_timer_partitions"]; !ok {
		app.Settings["num_timer_partitions"] = float64(defaultNumTimerPartitions)
	}
}

func (m *ServiceMgr) addLifeCycleStateByFunctionState(app *application) {
	deploymentStatus, _ := app.Settings["deployment_status"]
	processingStatus, _ := app.Settings["processing_status"]
	state := m.superSup.GetAppCompositeState(app.Name)
	if app.Metainfo == nil {
		app.Metainfo = make(map[string]interface{})
	}
	if deploymentStatus == true && processingStatus == false {
		app.Metainfo["lifecycle_state"] = "pause"
	} else if deploymentStatus == false && processingStatus == false {
		app.Metainfo["lifecycle_state"] = "undeploy"
	} else {
		if state == common.AppStatePaused {
			app.Metainfo["lifecycle_state"] = "pause"
		} else {
			app.Metainfo["lifecycle_state"] = "undeploy"
		}
	}
}

func (m *ServiceMgr) maybeDeleteLifeCycleState(app *application) {
	// Resetting it to nil won't make it available during export since 'omitempty' is used in the definition
	app.Metainfo = nil
}

func (m *ServiceMgr) maybeReplaceFromPrior(app *application) {
	if value, ok := app.Settings["dcp_stream_boundary"]; ok && value == "from_prior" {
		app.Settings["dcp_stream_boundary"] = "everything"
	}
}

func (m *ServiceMgr) notifyRetryToAllProducers(appName string, r *retry) (info *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::notifyRetryToAllProducers"

	info = &response.RuntimeInfo{}

	retryPath := metakvAppsRetryPath + appName
	retryCount := []byte(strconv.Itoa(int(r.Count)))

	err := util.MetakvSet(retryPath, retryCount, nil)
	if err != nil {
		info.ErrCode = response.ErrInternalServer
		info.Description = fmt.Sprintf("unable to set metakv path for retry, err : %v", err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	return
}

var singleFuncStatusPattern = regexp.MustCompile("^/api/v1/status/(.*[^/])/?$") // Match is agnostic of trailing '/'

func (m *ServiceMgr) statusHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetFunctionStatus)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET is allowed"
		return
	}

	var appNameFromURI string

	if match := singleFuncStatusPattern.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		appNameFromURI = match[1]
	}

	status, info := m.statusHandlerImpl(cred, appNameFromURI)

	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	runtimeInfo.Description = status
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) statusHandlerImpl(cred cbauth.Creds, appName string) (funcStatus interface{}, info *response.RuntimeInfo) {

	appDeployedNodesCounter, appBootstrappingNodesCounter, appPausingNodesCounter, numEventingNodes, mhCompat, info := m.getAppList()
	if info.ErrCode != response.Ok {
		return
	}

	eventingNodeAddrs := m.eventingNodeAddrs
	appsNames := m.getTempStoreAppNames()
	var statusHandlerResponse appStatusResponse
	statusHandlerResponse.NumEventingNodes = numEventingNodes

	// Get a list of apps that need to be redeployed on encryption level change
	encryptionEnabled := false
	var alreadyRedeployedApps map[string]struct{}
	if securitySetting := m.superSup.GetSecuritySetting(); securitySetting != nil {
		encryptionEnabled = securitySetting.EncryptData
	}
	alreadyRedeployedApps = m.superSup.GetGocbSubscribedApps(encryptionEnabled)

	for _, fnName := range appsNames {

		// Is the status of this app needed?
		if appName != "" && appName != fnName {
			continue
		}

		if info := m.checkPermissionFromCred(cred, fnName, rbac.HandlerGetPermissions, false); info.ErrCode != response.Ok {
			continue
		}

		deploymentStatus, processingStatus, err := m.getStatuses(fnName)
		if err != nil {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = err.Error()
			return
		}

		status := appStatus{
			Name:             fnName,
			DeploymentStatus: deploymentStatus,
			ProcessingStatus: processingStatus,
			RedeployRequired: false,
		}
		if num, exists := appDeployedNodesCounter[fnName]; exists {
			status.NumDeployedNodes = num
		}
		if num, exists := appBootstrappingNodesCounter[fnName]; exists {
			status.NumBootstrappingNodes = num
		}

		if mhCompat {
			bootstrapStatus := true
			// Possible that consumer process might be restarting on some node which will update the status but not bootstrapping list
			// synchronise with other node only if app is not in bootstrapping list
			if status.NumBootstrappingNodes == 0 {
				// By the time this code path hits, eventing Nodes are updated in eventingNodeAddrs variable
				bootstrapStatus, err = util.CheckIfAppBootstrapOngoing("/getBootstrapAppStatus", eventingNodeAddrs, fnName)
				if err != nil {
					info.ErrCode = response.ErrInternalServer
					return
				}
			}
			status.CompositeStatus = m.determineStatus(status, appPausingNodesCounter, numEventingNodes, bootstrapStatus)
		} else {
			status.CompositeStatus = m.determineStatus(status, appPausingNodesCounter, numEventingNodes, false)
		}

		if _, found := alreadyRedeployedApps[fnName]; !found && status.CompositeStatus == "deployed" {
			status.RedeployRequired = true
		}

		statusHandlerResponse.Apps = append(statusHandlerResponse.Apps, status)

		// Do we already have the status of the app we care about?
		if appName != "" && appName == fnName {
			break
		}
	}

	if appName != "" {
		if len(statusHandlerResponse.Apps) == 0 {
			info.ErrCode = response.ErrAppNotFoundTs
			info.Description = fmt.Sprintf("Function: %s not found", appName)
		} else {
			funcStatus = &singleAppStatusResponse{statusHandlerResponse.Apps[0], statusHandlerResponse.NumEventingNodes}

			return
		}
	}

	funcStatus = statusHandlerResponse

	return
}

func (m *ServiceMgr) determineStatus(status appStatus, pausingAppsList map[string]int, numEventingNodes int, bootstrapStatus bool) string {
	logPrefix := "ServiceMgr::determineStatus"

	if status.DeploymentStatus && status.ProcessingStatus {
		if !bootstrapStatus && status.NumBootstrappingNodes == 0 && status.NumDeployedNodes == numEventingNodes {
			return "deployed"
		}
		return "deploying"
	}

	// For case:
	// T1 - bootstrap was requested
	// T2 - undeploy was requested
	// T3 - bootstrap finished
	// During the period T2 - T3, Eventing is spending cycles to bring up
	// the function is ready state i.e. state should be "deploying". Reporting
	// undeployed by looking up in temp store would be unreasonable
	if status.NumBootstrappingNodes > 0 {
		return "deploying"
	}

	if !status.DeploymentStatus && !status.ProcessingStatus {
		if status.NumDeployedNodes == 0 {
			return "undeployed"
		}
		return "undeploying"
	}

	if status.DeploymentStatus && !status.ProcessingStatus {
		if pausingAppsList != nil {
			_, ok := pausingAppsList[status.Name]
			if status.NumDeployedNodes == numEventingNodes && !ok {
				return "paused"
			}
			return "pausing"
		}
		if status.NumDeployedNodes == numEventingNodes {
			return "paused"
		}
		return "pausing"
	}

	logging.Errorf("%s Function: %s inconsistent deployment state %v", logPrefix, status.Name, status)
	return "invalid"
}

func (m *ServiceMgr) statsHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventFetchStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET is allowed"
		return
	}

	params := r.URL.Query()
	res.AddRequestData("query", params)

	// Check whether type=full is present in query
	fullStats := false
	if typeParam, info := CheckAndGetQueryParam(params, "type"); info.ErrCode == response.Ok && typeParam == "full" {
		fullStats = true
	}

	// populate stats will validate the permissions
	statsList := m.populateStats(cred, fullStats)
	runtimeInfo.Description = statsList
	runtimeInfo.OnlyDescription = true
	return
}

func percentileN(latencyStats map[string]uint64, p int) int {
	var samples sort.IntSlice
	var numSamples uint64
	for bin, binCount := range latencyStats {
		sample, err := strconv.Atoi(bin)
		if err == nil {
			samples = append(samples, sample)
			numSamples += binCount
		}
	}
	sort.Sort(samples)
	i := numSamples*uint64(p)/100 - 1

	var counter uint64
	var prevSample int
	for _, sample := range samples {
		if counter > i {
			return prevSample
		}
		counter += latencyStats[strconv.Itoa(sample)]
		prevSample = sample
	}

	if len(samples) > 0 {
		return samples[len(samples)-1]
	}
	return 0
}

func (m *ServiceMgr) populateStats(cred cbauth.Creds, fullStats bool) []stats {
	statsList := make([]stats, 0)

	for _, app := range m.getTempStoreAll() {
		if m.checkIfDeployed(app.Name) {
			info := m.checkPermissionFromCred(cred, app.Name, rbac.HandlerGetPermissions, false)
			if info.ErrCode != response.Ok {
				continue
			}

			stats := stats{}
			feedBoundary, err := m.superSup.DcpFeedBoundary(app.Name)
			if err == nil {
				stats.DCPFeedBoundary = feedBoundary
			}
			stats.EventProcessingStats = m.superSup.GetEventProcessingStats(app.Name)
			stats.EventsRemaining = backlogStat{DcpBacklog: m.superSup.GetDcpEventsRemainingToProcess(app.Name)}
			stats.ExecutionStats = m.superSup.GetExecutionStats(app.Name)
			stats.FailureStats = m.superSup.GetFailureStats(app.Name)
			stats.FunctionName = app.Name
			stats.GocbCredsRequestCounter = util.GocbCredsRequestCounter
			stats.FunctionID = app.FunctionID
			stats.InternalVbDistributionStats = m.superSup.InternalVbDistributionStats(app.Name)
			stats.LcbCredsRequestCounter = m.lcbCredsCounter
			stats.LcbExceptionStats = m.superSup.GetLcbExceptionsStats(app.Name)
			stats.MetastoreStats = m.superSup.GetMetaStoreStats(app.Name)
			stats.WorkerPids = m.superSup.GetEventingConsumerPids(app.Name)
			stats.PlannerStats = m.superSup.PlannerStats(app.Name)
			stats.VbDistributionStatsFromMetadata = m.superSup.VbDistributionStatsFromMetadata(app.Name)

			latencyStats := m.superSup.GetLatencyStats(app.Name)
			ls := make(map[string]int)
			ls["50"] = percentileN(latencyStats, 50)
			ls["80"] = percentileN(latencyStats, 80)
			ls["90"] = percentileN(latencyStats, 90)
			ls["95"] = percentileN(latencyStats, 95)
			ls["99"] = percentileN(latencyStats, 99)
			ls["100"] = percentileN(latencyStats, 100)
			stats.LatencyPercentileStats = ls

			m.rebalancerMutex.RLock()
			if m.rebalancer != nil {
				rebalanceStats := make(map[string]interface{})
				rebalanceStats["is_leader"] = true
				rebalanceStats["node_level_stats"] = m.rebalancer.NodeLevelStats
				rebalanceStats["rebalance_progress"] = m.rebalancer.RebalanceProgress
				rebalanceStats["rebalance_progress_counter"] = m.rebalancer.RebProgressCounter
				rebalanceStats["rebalance_start_ts"] = m.rebalancer.RebalanceStartTs
				rebalanceStats["total_vbs_to_shuffle"] = m.rebalancer.TotalVbsToShuffle
				rebalanceStats["vbs_remaining_to_shuffle"] = m.rebalancer.VbsRemainingToShuffle

				stats.RebalanceStats = rebalanceStats
			}
			m.rebalancerMutex.RUnlock()

			if fullStats {
				checkpointBlobDump, err := m.superSup.CheckpointBlobDump(app.Name)
				if err == nil {
					stats.CheckpointBlobDump = checkpointBlobDump
				}

				stats.LatencyStats = m.superSup.GetLatencyStats(app.Name)
				stats.CurlLatencyStats = m.superSup.GetCurlLatencyStats(app.Name)
				stats.SeqsProcessed = m.superSup.GetSeqsProcessed(app.Name)

				spanBlobDump, err := m.superSup.SpanBlobDump(app.Name)
				if err == nil {
					stats.SpanBlobDump = spanBlobDump
				}

				stats.VbDcpEventsRemaining = m.superSup.VbDcpEventsRemainingToProcess(app.Name)
				debugStats, err := m.superSup.TimerDebugStats(app.Name)
				if err == nil {
					stats.DocTimerDebugStats = debugStats
				}
				vbSeqnoStats, err := m.superSup.VbSeqnoStats(app.Name)
				if err == nil {
					stats.VbSeqnoStats = vbSeqnoStats
				}
			}

			statsList = append(statsList, stats)
		}
	}

	return statsList
}

// Clears up all Eventing related artifacts from metakv, typically will be used for rebalance tests
// Only admin can do this
func (m *ServiceMgr) cleanupEventing(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::cleanupEventing"

	res := response.NewResponseWriter(w, r, response.EventCleanupEventing)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	logging.Infof("%s REST Call: %v %v", logPrefix, r.URL.Path, r.Method)

	util.Retry(util.NewFixedBackoff(time.Second), nil, cleanupEventingMetaKvPath, metakvChecksumPath)
	util.Retry(util.NewFixedBackoff(time.Second), nil, cleanupEventingMetaKvPath, metakvTempChecksumPath)
	util.Retry(util.NewFixedBackoff(time.Second), nil, cleanupEventingMetaKvPath, metakvAppsPath)
	util.Retry(util.NewFixedBackoff(time.Second), nil, cleanupEventingMetaKvPath, metakvTempAppsPath)
	util.Retry(util.NewFixedBackoff(time.Second), nil, cleanupEventingMetaKvPath, metakvAppSettingsPath)

	runtimeInfo.Description = "Successfully cleaned up eventing"
}

func (m *ServiceMgr) exportHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::exportHandler"

	res := response.NewResponseWriter(w, r, response.EventExportFunctions)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET is allowed"
		return
	}

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	logging.Infof("%s REST Call: %v %v", logPrefix, r.URL.Path, r.Method)

	apps := m.getTempStoreAll()
	exportedFns := make([]string, 0, len(apps))
	exportedFuncs := make([]application, 0, len(apps))

	for _, app := range apps {
		info := m.checkPermissionFromCred(cred, app.Name, rbac.HandlerGetPermissions, false)
		if info.ErrCode != response.Ok {
			continue
		}

		for i := range app.DeploymentConfig.Curl {
			app.DeploymentConfig.Curl[i].Username = ""
			app.DeploymentConfig.Curl[i].Password = ""
			app.DeploymentConfig.Curl[i].BearerKey = ""
		}

		app.Settings["deployment_status"] = false
		app.Settings["processing_status"] = false
		exportedFns = append(exportedFns, app.Name)
		exportedFuncs = append(exportedFuncs, app)
	}

	logging.Infof("%s Exported function list: %+v", logPrefix, exportedFns)

	runtimeInfo.ExtraAttributes = map[string]interface{}{"appNames": exportedFns}
	runtimeInfo.Description = exportedFuncs
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) importHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::importHandler"

	res := response.NewResponseWriter(w, r, response.EventImportFunctions)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if r.Method != "POST" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only POST is allowed"
		return
	}

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	logging.Infof("%s REST Call: %v %v", logPrefix, r.URL.Path, r.Method)

	appList, info := m.unmarshalAppList(r)
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}
	res.AddRequestData("body", appList)

	for _, app := range *appList {
		info = m.MaybeEnforceFunctionSchema(app)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
	}
	isMixedMode, err := m.isMixedModeCluster()
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	if isMixedMode {
		runtimeInfo.ErrCode = response.ErrMixedMode
		runtimeInfo.Description = "Life-cycle operations except delete and undeploy are not allowed in a mixed mode cluster"
		return
	}

	infoList, importedFns := m.createApplications(cred, appList, true)

	runtimeInfo.ExtraAttributes = map[string]interface{}{"appNames": importedFns}
	runtimeInfo.Description = infoList
	runtimeInfo.OnlyDescription = true
	logging.Infof("%s Imported functions: %+v", logPrefix, importedFns)
}

func (m *ServiceMgr) backupHandler(w http.ResponseWriter, r *http.Request) {
	url := filepath.Clean(r.URL.Path)

	res := response.NewResponseWriter(w, r, response.EventBackupFunctions)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
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
	case "GET":
		// call for backup
		exportedFun := m.backupApps(cred, filterMap, filterType)
		appNames := make([]string, 0, len(exportedFun))
		for _, app := range exportedFun {
			appNames = append(appNames, app.Name)
		}
		runtimeInfo.ExtraAttributes = map[string]interface{}{"appNames": appNames}
		runtimeInfo.Description = exportedFun
		runtimeInfo.OnlyDescription = true

	case "POST":
		// call restore handler
		res.SetRequestEvent(response.EventRestoreFunctions)
		isMixedMode, err := m.isMixedModeCluster()
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInternalServer
			runtimeInfo.Description = fmt.Sprintf("err: %v", err)
			return
		}

		if isMixedMode {
			runtimeInfo.ErrCode = response.ErrMixedMode
			runtimeInfo.Description = "Life-cycle operations except delete and undeploy are not allowed in a mixed mode cluster"
			return
		}

		appList, info := m.unmarshalAppList(r)
		if info.ErrCode != response.Ok {
			*runtimeInfo = *info
			return
		}
		res.AddRequestData("body", appList)

		for _, app := range *appList {
			info = m.MaybeEnforceFunctionSchema(app)
			if info.ErrCode != response.Ok {
				*runtimeInfo = *info
				return
			}
		}

		remap, err := getRestoreMap(r)
		if err != nil {
			runtimeInfo.ErrCode = response.ErrInvalidRequest
			runtimeInfo.Description = fmt.Sprintf("%s", err)
			return
		}

		apps := m.restoreAppList(appList, filterMap, remap, filterType)
		infoList, importedList := m.createApplications(cred, apps, true)

		runtimeInfo.ExtraAttributes = map[string]interface{}{"appNames": importedList}
		runtimeInfo.Description = infoList
		runtimeInfo.OnlyDescription = true

	default:
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET and POST are allowed"
		return
	}
}

func (m *ServiceMgr) backupApps(cred cbauth.Creds, filterMap map[string]bool, filterType string) []application {
	apps := m.getTempStoreAll()
	appList := make([]application, 0, len(apps))
	for _, app := range apps {
		info := m.checkPermissionFromCred(cred, app.Name, rbac.HandlerManagePermissions, false)
		if info.ErrCode != response.Ok {
			continue
		}
		appList = append(appList, app)
	}

	return m.filterAppList(appList, filterMap, filterType, true)
}

func (m *ServiceMgr) restoreAppList(apps *[]application, filterMap map[string]bool, remap map[string]common.Keyspace, filterType string) *[]application {
	filteredApps := m.filterAppList(*apps, filterMap, filterType, false)
	appList := make([]application, 0, len(filteredApps))
	for _, app := range filteredApps {
		val, length, ok := remapContains(remap, app.FunctionScope.BucketName, app.FunctionScope.ScopeName, "")
		if ok {
			app.FunctionScope.BucketName = val.BucketName
			if length == 2 {
				app.FunctionScope.ScopeName = val.ScopeName
			}
		}

		deploymentConfig := app.DeploymentConfig
		val, length, ok = remapContains(remap, deploymentConfig.SourceBucket, deploymentConfig.SourceScope, deploymentConfig.SourceCollection)
		if ok {
			app.DeploymentConfig.SourceBucket = val.BucketName
			if length == 2 {
				app.DeploymentConfig.SourceScope = val.ScopeName
			}
			if length == 3 {
				app.DeploymentConfig.SourceScope = val.ScopeName
				app.DeploymentConfig.SourceCollection = val.CollectionName
			}
		}

		val, length, ok = remapContains(remap, deploymentConfig.MetadataBucket, deploymentConfig.MetadataScope, deploymentConfig.MetadataCollection)
		if ok {
			app.DeploymentConfig.MetadataBucket = val.BucketName
			if length == 2 {
				app.DeploymentConfig.MetadataScope = val.ScopeName
			}
			if length == 3 {
				app.DeploymentConfig.MetadataScope = val.ScopeName
				app.DeploymentConfig.MetadataCollection = val.CollectionName
			}
		}

		for i := range deploymentConfig.Buckets {
			val, length, ok = remapContains(remap, deploymentConfig.Buckets[i].BucketName, deploymentConfig.Buckets[i].ScopeName, deploymentConfig.Buckets[i].CollectionName)
			if ok {
				app.DeploymentConfig.Buckets[i].BucketName = val.BucketName
				if length == 2 {
					app.DeploymentConfig.Buckets[i].ScopeName = val.ScopeName
				}
				if length == 3 {
					app.DeploymentConfig.Buckets[i].ScopeName = val.ScopeName
					app.DeploymentConfig.Buckets[i].CollectionName = val.CollectionName
				}
			}
		}
		appList = append(appList, app)
	}
	return &appList
}

func (m *ServiceMgr) filterAppList(apps []application, filterMap map[string]bool, filterType string, backup bool) []application {
	filteredFns := make([]application, 0, len(apps))
	for _, app := range apps {
		m.addDefaultDeploymentConfig(&app)
		if applyFilter(app, filterMap, filterType) {
			if backup {
				for i := range app.DeploymentConfig.Curl {
					app.DeploymentConfig.Curl[i].Username = ""
					app.DeploymentConfig.Curl[i].Password = ""
					app.DeploymentConfig.Curl[i].BearerKey = ""
				}
				app.Settings["deployment_status"] = false
				app.Settings["processing_status"] = false
			}
			filteredFns = append(filteredFns, app)
		}
	}
	return filteredFns
}

func (m *ServiceMgr) createApplications(cred cbauth.Creds, appList *[]application, isImport bool) (infoList []*response.RuntimeInfo, importedFns []string) {
	logPrefix := "ServiceMgr::createApplications"

	infoList = []*response.RuntimeInfo{}
	importedFns = make([]string, 0, len(*appList))

	for _, app := range *appList {

		if isImport {
			app.Settings["deployment_status"] = false
			app.Settings["processing_status"] = false
		} else {
			m.addDefaultVersionIfMissing(&app)
		}
		m.addDefaultTimerPartitionsIfMissing(&app)
		m.addLifeCycleStateByFunctionState(&app)
		if val, ok := app.Settings["dcp_stream_boundary"]; ok && val == "from_prior" {
			app.Settings["dcp_stream_boundary"] = "everything"
		}

		m.addDefaultDeploymentConfig(&app)
		if infoVal := m.validateApplication(&app); infoVal.ErrCode != response.Ok {
			logging.Warnf("%s Validating %ru failed: %v", logPrefix, app, infoVal)
			infoList = append(infoList, infoVal)
			continue
		}

		info := &response.RuntimeInfo{}
		err := m.assignFunctionID(app.Name, &app)
		if err != nil {
			info.ErrCode = response.ErrInternalServer
			info.Description = "Unable to assign Function id"
			infoList = append(infoList, info)
			continue
		}

		err = m.assignFunctionInstanceID(app.Name, &app)
		if err != nil {
			info.ErrCode = response.ErrInternalServer
			info.Description = "Unable to assign Function instance id"
			infoList = append(infoList, info)
			continue
		}

		_, info = m.checkAppExists(app.Name)
		if info.ErrCode == response.ErrAppNotFoundTs {
			info = m.verifyAndCreateApp(cred, &app)
			if info.ErrCode != response.Ok {
				infoList = append(infoList, info)
				continue
			}
		} else if info.ErrCode == response.Ok {
			info = m.checkPermissionFromCred(cred, app.Name, rbac.HandlerManagePermissions, false)
			if info.ErrCode != response.Ok {
				infoList = append(infoList, info)
				continue
			}

			if m.checkIfDeployed(app.Name) && isImport {
				info.ErrCode = response.ErrAppDeployed
				info.Description = fmt.Sprintf("Function: %s another function with same name is already present, skipping import request", app.Name)
				logging.Errorf("%s %s", logPrefix, info.Description)
				infoList = append(infoList, info)
				continue
			}
		} else {
			infoList = append(infoList, info)
			continue
		}

		infoPri := m.savePrimaryStore(&app)
		if infoPri.ErrCode != response.Ok {
			logging.Errorf("%s Function: %s saving %ru to primary store failed: %v", logPrefix, app.Name, infoPri)
			infoList = append(infoList, infoPri)
			continue
		}

		// Save to temp store only if saving to primary store succeeds
		// audit.Log(auditevent.SaveDraft, r, app.Name)
		infoTmp := m.saveTempStore(app)
		if infoTmp.ErrCode != response.Ok {
			logging.Errorf("%s Function: %s saving to temporary store failed: %v", logPrefix, app.Name, infoTmp)
			infoList = append(infoList, infoTmp)
			continue
		}

		// If everything succeeded, use infoPri as that has warnings, if any
		infoList = append(infoList, infoPri)
		importedFns = append(importedFns, app.Name)
	}

	return
}

func (m *ServiceMgr) getCPUCount(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetCpuCount)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET is allowed"
		return
	}
	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingAnyManageReadPermissions, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	runtimeInfo.Description = util.CPUCount(false)
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) getWorkerCount(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetWorkerCount)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if r.Method != "GET" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only GET is allowed"
		return
	}

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	count := 0

	apps := m.getTempStoreAll()
	for _, app := range apps {
		info := m.checkPermissionFromCred(cred, app.Name, rbac.HandlerGetPermissions, false)
		if info.ErrCode != response.Ok {
			continue
		}

		deployed, ok := app.Settings["deployment_status"].(bool)
		if !ok || !deployed {
			continue
		}
		if val, ok := app.Settings["worker_count"].(float64); ok {
			count += int(val)
		} else {
			// Picking up default worker count
			count += 3
		}
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = fmt.Sprintf("%v\n", count)
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) isMixedModeCluster() (bool, error) {
	nsServerEndpoint := net.JoinHostPort(util.Localhost(), m.restPort)
	cic, err := util.FetchClusterInfoClient(nsServerEndpoint)
	if err != nil {
		return false, fmt.Errorf("Failed to get cluster info cache, err: %v", err)
	}
	clusterInfo := cic.GetClusterInfoCache()
	clusterInfo.RLock()
	defer clusterInfo.RUnlock()

	nodes := clusterInfo.GetActiveEventingNodes()
	if len(nodes) == 0 {
		return false, nil
	}

	ver, err := common.FrameCouchbaseVerFromNsServerStreamingRestApi(nodes[0].Version)
	if err != nil {
		return true, fmt.Errorf("Failed to frame couchbase version")
	}

	for _, node := range nodes {
		nVer, err := common.FrameCouchbaseVerFromNsServerStreamingRestApi(node.Version)
		if err != nil {
			return true, fmt.Errorf("Failed to frame couchbase version")
		}
		if !ver.Equals(nVer) {
			return true, nil
		}
	}

	return false, nil
}

type version struct {
	major int
	minor int
}

var verMap = map[string]version{
	"vulcan":       {5, 5},
	"alice":        {6, 0},
	"mad-hatter":   {6, 5},
	"cheshire-cat": {7, 0},
}

func (r version) satisfies(need version) bool {
	return r.major > need.major ||
		r.major == need.major && r.minor >= need.minor
}

func (r version) String() string {
	return fmt.Sprintf("%v.%v", r.major, r.minor)
}

func (m *ServiceMgr) checkVersionCompat(required string, info *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::checkVersionCompat"
	info = &response.RuntimeInfo{}

	nsServerEndpoint := net.JoinHostPort(util.Localhost(), m.restPort)
	cic, err := util.FetchClusterInfoClient(nsServerEndpoint)
	if err != nil {
		info.ErrCode = response.ErrInternalServer
		info.Description = fmt.Sprintf("Failed to get cluster info cache, err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}
	clusterInfo := cic.GetClusterInfoCache()
	clusterInfo.RLock()
	defer clusterInfo.RUnlock()

	var need, have version
	have.major, have.minor = clusterInfo.GetClusterVersion()
	need, ok := verMap[required]

	if !ok || !have.satisfies(need) {
		info.ErrCode = response.ErrClusterVersion
		info.Description = fmt.Sprintf("Function requires %v but cluster is at %v", need, have)
		logging.Warnf("%s Version compat check failed: %s", logPrefix, info.Description)
		return
	}

	logging.Debugf("%s Function need %v satisfied by cluster %v", logPrefix, need, have)
}

func (m *ServiceMgr) triggerGC(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::triggerGC"
	res := response.NewResponseWriter(w, r, response.EventTriggerGC)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}
	logging.Infof("%s Triggering GC", logPrefix)
	runtime.GC()
	logging.Infof("%s Finished GC run", logPrefix)
	runtimeInfo.Description = "Finished GC run"
}

func (m *ServiceMgr) freeOSMemory(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::freeOSMemory"

	res := response.NewResponseWriter(w, r, response.EventFreeOSMemory)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	logging.Infof("%s Freeing up memory to OS", logPrefix)
	debug.FreeOSMemory()
	logging.Infof("%s Freed up memory to OS", logPrefix)
	runtimeInfo.Description = "Freed up memory to OS"
}

//expvar handler
func (m *ServiceMgr) expvarHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRuntimeProfiling)
	runtimeInfo := &response.RuntimeInfo{}

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	res.Log(runtimeInfo)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

//pprof index handler
func (m *ServiceMgr) indexHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRuntimeProfiling)
	runtimeInfo := &response.RuntimeInfo{}
	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	res.Log(runtimeInfo)
	pprof.Index(w, r)
}

//pprof cmdline handler
func (m *ServiceMgr) cmdlineHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRuntimeProfiling)

	runtimeInfo := &response.RuntimeInfo{}
	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	res.Log(runtimeInfo)
	pprof.Cmdline(w, r)
}

//pprof profile handler
func (m *ServiceMgr) profileHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRuntimeProfiling)

	runtimeInfo := &response.RuntimeInfo{}
	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	res.Log(runtimeInfo)
	pprof.Profile(w, r)
}

//pprof symbol handler
func (m *ServiceMgr) symbolHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRuntimeProfiling)

	runtimeInfo := &response.RuntimeInfo{}
	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	res.Log(runtimeInfo)
	pprof.Symbol(w, r)
}

//pprof trace handler
func (m *ServiceMgr) traceHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRuntimeProfiling)
	runtimeInfo := &response.RuntimeInfo{}

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	res.Log(runtimeInfo)
	pprof.Trace(w, r)
}

func (m *ServiceMgr) listFunctions(w http.ResponseWriter, r *http.Request) {
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
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	params := r.URL.Query()
	res.AddRequestData("query", params)

	fnlist, info := m.getFunctionList(params)
	if info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}

	functions := make([]string, 0, len(fnlist.Functions))
	for _, appName := range fnlist.Functions {
		info := m.checkPermissionFromCred(cred, appName, rbac.HandlerGetPermissions, false)
		if info.ErrCode != response.Ok {
			continue
		}
		functions = append(functions, appName)
	}

	functionList := functionList{
		Functions: functions,
	}

	runtimeInfo.Description = functionList
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) triggerInternalRebalance(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::triggerInternalRebalance"

	res := response.NewResponseWriter(w, r, response.EventResdistributeWorkload)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if r.Method != "POST" {
		runtimeInfo.ErrCode = response.ErrMethodNotAllowed
		runtimeInfo.Description = "Method not allowed. Only POST is allowed"
		return
	}

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	err := m.checkTopologyChangeReadiness(service.TopologyChangeTypeRebalance)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = err
		return
	}
	path := "rebalance_request_from_rest"
	value := []byte(startRebalance)
	logging.Infof("%s triggering rebalance processing from rest path: %v, value:%v", logPrefix, path, value)
	m.superSup.TopologyChangeNotifCallback(metakv.KVEntry{Path: path, Value: value, Rev: m.state.rev})

	runtimeInfo.Description = "Successfully triggered internal rebalance"
}

func (m *ServiceMgr) prometheusLow(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventFetchStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingStatsPermission, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	//TODO: avg script execution time, avg timer scan time
	out := make([]byte, 0)
	out = append(out, []byte(fmt.Sprintf("%vworker_restart_count %v\n", METRICS_PREFIX, m.superSup.WorkerRespawnedCount()))...)

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = fmt.Sprintf("%s", out)
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) prometheusHigh(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventFetchStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingStatsPermission, false); err != nil {
		*runtimeInfo = getAuthErrorInfo(notAllowed, false, err)
		return
	}

	list := m.highCardStats()

	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = fmt.Sprintf("%s", list)
	runtimeInfo.OnlyDescription = true
}

func (m *ServiceMgr) highCardStats() []byte {
	// service_type{bucket, scope, collection, functionName} value
	fmtStr := "%v%v{functionName=\"%v\"} %v\n"

	deployedApps := m.superSup.GetDeployedApps()
	stats := make([]byte, 0, APPROX_METRIC_COUNT*APPROX_METRIC_SIZE*len(deployedApps))

	for appName, _ := range deployedApps {
		backlog := fmt.Sprintf(fmtStr, METRICS_PREFIX, "dcp_backlog", appName,
			m.superSup.GetDcpEventsRemainingToProcess(appName))
		stats = append(stats, []byte(backlog)...)

		processingStats := m.superSup.GetEventProcessingStats(appName)
		if processingStats != nil {
			stats = populateUint(fmtStr, appName, "dcp_mutation_sent_to_worker", stats, processingStats)
			stats = populateUint(fmtStr, appName, "dcp_mutation_suppressed_counter", stats, processingStats)
			stats = populateUint(fmtStr, appName, "dcp_deletion_sent_to_worker", stats, processingStats)
			stats = populateUint(fmtStr, appName, "dcp_expiry_sent_to_worker", stats, processingStats)
			stats = populateUint(fmtStr, appName, "dcp_deletion_suppressed_counter", stats, processingStats)
			stats = populateUint(fmtStr, appName, "worker_spawn_counter", stats, processingStats)
		}

		executionStats := m.superSup.GetExecutionStats(appName)
		if executionStats != nil {
			stats = populate(fmtStr, appName, "agg_queue_memory", stats, executionStats)
			stats = populate(fmtStr, appName, "agg_queue_size", stats, executionStats)
			stats = populate(fmtStr, appName, "on_update_success", stats, executionStats)
			stats = populate(fmtStr, appName, "on_update_failure", stats, executionStats)
			stats = populate(fmtStr, appName, "dcp_delete_msg_counter", stats, executionStats)
			stats = populate(fmtStr, appName, "dcp_mutations_msg_counter", stats, executionStats)
			stats = populate(fmtStr, appName, "on_delete_success", stats, executionStats)
			stats = populate(fmtStr, appName, "on_delete_failure", stats, executionStats)
			stats = populate(fmtStr, appName, "timer_cancel_counter", stats, executionStats)
			stats = populate(fmtStr, appName, "timer_create_counter", stats, executionStats)
			stats = populate(fmtStr, appName, "timer_create_failure", stats, executionStats)
			stats = populate(fmtStr, appName, "timer_callback_success", stats, executionStats)
			stats = populate(fmtStr, appName, "timer_callback_failure", stats, executionStats)
			// The following metric tracks the total number of times a Timer callback is invoked.
			// => timer_msg_counter = timer_callback_missing_counter + timer_callback_success + timer_callback_failure.
			stats = populate(fmtStr, appName, "timer_msg_counter", stats, executionStats)
		}

		failureStats := m.superSup.GetFailureStats(appName)
		if failureStats != nil {
			//TODO: Add num_curl_exceptions, num_curl_timeout
			stats = populate(fmtStr, appName, "bucket_op_exception_count", stats, failureStats)
			stats = populate(fmtStr, appName, "timeout_count", stats, failureStats)
			stats = populate(fmtStr, appName, "n1ql_op_exception_count", stats, failureStats)
			stats = populate(fmtStr, appName, "timer_context_size_exception_counter", stats, failureStats)
			stats = populate(fmtStr, appName, "timer_callback_missing_counter", stats, failureStats)
			stats = populate(fmtStr, appName, "bkt_ops_cas_mismatch_count", stats, failureStats)
			stats = populate(fmtStr, appName, "checkpoint_failure_count", stats, failureStats)
		}

	}
	return stats
}

func (m *ServiceMgr) resetStatsCounters(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventClearStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)

	appNames := params["appName"]
	if len(appNames) == 0 || len(appNames) > 1 {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("Either 0 or more than 1 appnames passed")
		return
	}

	appName := appNames[0]
	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}
	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)
	httpresp, err := util.ResetStatsCounters("/resetNodeStatsCounters?appName="+appName, m.eventingNodeAddrs)
	if err != nil && httpresp != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("%s", httpresp)
		return
	} else if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf(`{"error":"Failed to reset counters error: %v"}`, err)
		return
	}

	runtimeInfo.ExtraAttributes = map[string]interface{}{"appNames": []string{appName}}
	runtimeInfo.Description = "Done resetting counters"
}

func (m *ServiceMgr) resetNodeStatsCounters(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventClearStats)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	params := r.URL.Query()
	res.AddRequestData("query", params)

	appNames := params["appName"]
	if len(appNames) == 0 || len(appNames) > 1 {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("Either 0 or more than 1 appnames passed")
		return
	}

	appName := appNames[0]
	if info := m.checkAuthAndPermissionWithApp(w, r, appName, rbac.HandlerManagePermissions, false); info.ErrCode != response.Ok {
		*runtimeInfo = *info
		return
	}
	appState := m.superSup.GetAppCompositeState(appName)
	if appState != common.AppStateEnabled {
		runtimeInfo.ErrCode = response.ErrAppNotDeployed
		runtimeInfo.Description = fmt.Sprintf("Function: %v should be in deployed state", appName)
		return
	}

	if err := m.superSup.ResetCounters(appName); err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf(`{"error":"Failed to reset counters error: %v"}`, err)
		return
	}

	runtimeInfo.ExtraAttributes = map[string]interface{}{"appNames": []string{appName}}
	runtimeInfo.Description = "Done resetting counters"
}

func (m *ServiceMgr) checkAuthAndPermissionWithApp(w http.ResponseWriter, r *http.Request,
	appName string, permFunction func(*common.Keyspace) []string, all bool) *response.RuntimeInfo {
	info := &response.RuntimeInfo{}

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*info = getAuthErrorInfo(nil, false, err)
		return info
	}

	return m.checkPermissionFromCred(cred, appName, permFunction, all)
}

func (m *ServiceMgr) checkPermissionFromCred(cred cbauth.Creds, appName string,
	permFunction func(*common.Keyspace) []string, all bool) *response.RuntimeInfo {

	app, info := m.checkAppExists(appName)
	if info.ErrCode != response.Ok {
		return info
	}

	perm := permFunction(app.FunctionScope.ToKeyspace())
	if notAllowed, err := rbac.IsAllowedCreds(cred, perm, all); err != nil {
		*info = getAuthErrorInfo(notAllowed, all, err)
		return info
	}

	return info
}

func (m *ServiceMgr) getReadAndWritePermission(appName string) ([]string, []string, *response.RuntimeInfo) {
	app, info := m.checkAppExists(appName)
	if info.ErrCode != response.Ok {
		return nil, nil, info
	}

	keyspace := app.FunctionScope.ToKeyspace()
	rPriv := rbac.HandlerGetPermissions(keyspace)
	wPriv := rbac.HandlerManagePermissions(keyspace)
	return rPriv, wPriv, info
}

type UserPermissions struct {
	FuncScope     []common.Keyspace `json:"func_scope"`
	ReadPerm      []common.Keyspace `json:"read_permission"`
	WritePerm     []common.Keyspace `json:"write_permission"`
	ReadWritePerm []common.Keyspace `json:"read_write_permission"`
	DcpStreamPerm []common.Keyspace `json:"dcp_stream_permission"`
}

func (m *ServiceMgr) getUserInfo(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetUserInfo)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	cred, err := rbac.AuthWebCreds(r)
	if err != nil {
		*runtimeInfo = getAuthErrorInfo(nil, false, err)
		return
	}

	// map[bucketName]map[scopeName][]collection
	snapShot, err := m.superSup.GetBSCSnapshot()
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("Failed to get collection snapshot")
		return
	}
	u := &UserPermissions{
		FuncScope:     make([]common.Keyspace, 0),
		ReadPerm:      make([]common.Keyspace, 0),
		WritePerm:     make([]common.Keyspace, 0),
		ReadWritePerm: make([]common.Keyspace, 0),
		DcpStreamPerm: make([]common.Keyspace, 0),
	}

	k := &common.Keyspace{BucketName: "*", ScopeName: "*"}
	manage := rbac.GetPermissions(k, rbac.EventingManage)
	if _, err := rbac.IsAllowedCreds(cred, manage, true); err == nil {
		u.FuncScope = append(u.FuncScope, *k)
	}

	for bucketName, scopeMap := range snapShot {
		for scopeName, collectionList := range scopeMap {
			k := &common.Keyspace{BucketName: bucketName, ScopeName: scopeName}

			manage := rbac.GetPermissions(k, rbac.EventingManage)
			if _, err := rbac.IsAllowedCreds(cred, manage, true); err == nil {
				u.FuncScope = append(u.FuncScope, *k)
			}

			for _, collName := range collectionList {
				k.CollectionName = collName

				manage = rbac.GetPermissions(k, rbac.BucketDcp)
				if _, err := rbac.IsAllowedCreds(cred, manage, true); err == nil {
					u.DcpStreamPerm = append(u.DcpStreamPerm, *k)
				}

				manage = rbac.GetPermissions(k, rbac.BucketRead)
				read, write := false, false
				// checking read permissions
				if _, err := rbac.IsAllowedCreds(cred, manage, true); err == nil {
					u.ReadPerm = append(u.ReadPerm, *k)
					read = true
				}

				// checking for write permissions
				manage = rbac.GetPermissions(k, rbac.BucketWrite)
				if _, err := rbac.IsAllowedCreds(cred, manage, true); err == nil {
					u.WritePerm = append(u.WritePerm, *k)
					write = true
				}

				if read && write {
					u.ReadWritePerm = append(u.ReadWritePerm, *k)
				}
			}
		}
	}

	runtimeInfo.Description = u
	runtimeInfo.OnlyDescription = true
}
