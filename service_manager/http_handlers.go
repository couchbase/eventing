package servicemanager

import (
	"bytes"
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
	"regexp"
	"runtime"
	"runtime/debug"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/audit"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/consumer"
	"github.com/couchbase/eventing/gen/auditevent"
	"github.com/couchbase/eventing/gen/flatbuf/cfg"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/parser"
	"github.com/couchbase/eventing/util"
	"github.com/google/flatbuffers/go"
)

func (m *ServiceMgr) startTracing(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::startTracing"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	logging.Infof("%s Got request to start tracing", logPrefix)
	audit.Log(auditevent.StartTracing, r, nil)

	os.Remove(m.uuid + "_trace.out")

	f, err := os.Create(m.uuid + "_trace.out")
	if err != nil {
		logging.Infof("%s Failed to open file to write trace output, err: %v", logPrefix, err)
		return
	}
	defer f.Close()

	err = trace.Start(f)
	if err != nil {
		logging.Infof("%s Failed to start runtime.Trace, err: %v", logPrefix, err)
		return
	}

	<-m.stopTracerCh
	trace.Stop()
}

func (m *ServiceMgr) stopTracing(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::stopTracing"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	audit.Log(auditevent.StopTracing, r, nil)
	logging.Infof("%s Got request to stop tracing", logPrefix)
	m.stopTracerCh <- struct{}{}
}

func (m *ServiceMgr) getNodeUUID(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}
	logging.Debugf("Got request to fetch UUID from host %s", r.Host)
	fmt.Fprintf(w, "%v", m.uuid)
}

func (m *ServiceMgr) getNodeVersion(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}
	logging.Debugf("Got request to fetch version from host %s", r.Host)
	fmt.Fprintf(w, "%v", util.EventingVer())
}

func (m *ServiceMgr) deletePrimaryStoreHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::deletePrimaryStoreHandler"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("%s Function: %s deleting from primary store", logPrefix, appName)
	audit.Log(auditevent.DeleteFunction, r, appName)
	m.deletePrimaryStore(appName)
}

// Deletes application from primary store and returns the appropriate success/error code
func (m *ServiceMgr) deletePrimaryStore(appName string) (info *runtimeInfo) {
	logPrefix := "ServiceMgr::deletePrimaryStore"

	info = &runtimeInfo{}
	logging.Infof("%s Function: %s deleting from primary store", logPrefix, appName)

	checkIfDeployed := false
	for _, app := range util.ListChildren(metakvAppsPath) {
		if app == appName {
			checkIfDeployed = true
		}
	}

	if !checkIfDeployed {
		info.Code = m.statusCodes.errAppNotDeployed.Code
		info.Info = fmt.Sprintf("Function: %s not deployed", appName)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	appState := m.superSup.GetAppState(appName)
	if appState != common.AppStateUndeployed {
		info.Code = m.statusCodes.errAppNotUndeployed.Code
		info.Info = fmt.Sprintf("Function: %s skipping delete request from primary store, as it hasn't been undeployed", appName)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	settingPath := metakvAppSettingsPath + appName
	err := util.MetaKvDelete(settingPath, nil)
	if err != nil {
		info.Code = m.statusCodes.errDelAppSettingsPs.Code
		info.Info = fmt.Sprintf("Function: %s failed to delete settings, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	err = util.DeleteAppContent(metakvAppsPath, metakvChecksumPath, appName)
	if err != nil {
		info.Code = m.statusCodes.errDelAppPs.Code
		info.Info = fmt.Sprintf("Function: %s failed to delete, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	// TODO : This must be changed to app not deployed / found
	info.Code = m.statusCodes.ok.Code
	info.Info = fmt.Sprintf("Function: %s deleting in the background", appName)
	logging.Infof("%s %s", logPrefix, info.Info)
	return
}

func (m *ServiceMgr) deleteTempStoreHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	audit.Log(auditevent.DeleteDrafts, r, appName)

	m.deleteTempStore(appName)
}

// Deletes application from temporary store and returns the appropriate success/error code
func (m *ServiceMgr) deleteTempStore(appName string) (info *runtimeInfo) {
	logPrefix := "ServiceMgr::deleteTempStore"

	info = &runtimeInfo{}
	logging.Infof("%s Function: %s deleting drafts from temporary store", logPrefix, appName)

	checkIfDeployed := false
	for _, app := range util.ListChildren(metakvTempAppsPath) {
		if app == appName {
			checkIfDeployed = true
		}
	}

	if !checkIfDeployed {
		info.Code = m.statusCodes.errAppNotDeployed.Code
		info.Info = fmt.Sprintf("Function: %s not deployed", appName)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	appState := m.superSup.GetAppState(appName)
	if appState != common.AppStateUndeployed {
		info.Code = m.statusCodes.errAppNotUndeployed.Code
		info.Info = fmt.Sprintf("Function: %s skipping delete request from temp store, as it hasn't been undeployed", appName)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	if err := util.DeleteAppContent(metakvTempAppsPath, metakvTempChecksumPath, appName); err != nil {
		info.Code = m.statusCodes.errDelAppTs.Code
		info.Info = fmt.Sprintf("Function: %s failed to delete, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}
	info.Code = m.statusCodes.ok.Code
	info.Info = fmt.Sprintf("Function: %s deleting in the background", appName)
	logging.Infof("%s %s", logPrefix, info.Info)
	return
}

func (m *ServiceMgr) die(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}
	logging.Errorf("Got request to die, killing all consumers")
	m.superSup.KillAllConsumers()
	logging.Errorf("Got request to die, killing producer")
	os.Exit(-1)
}

func (m *ServiceMgr) getAppLog(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	nv := r.URL.Query()["name"]
	if len(nv) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, "Parameter 'name' must appear exactly once")
		return
	}

	appName := nv[0]
	sz := int64(40960)

	sv := r.URL.Query()["size"]
	if len(sv) == 1 {
		psz, err := strconv.Atoi(sv[0])
		if err == nil {
			sz = int64(psz)
		}
	}

	var lines []string
	if rv := r.URL.Query()["aggregate"]; len(rv) > 0 && rv[0] == "true" {
		creds := r.Header
		lines = getGlobalAppLog(m, appName, sz, creds)
	} else {
		lines = getLocalAppLog(m, appName, sz)
	}

	sort.Sort(sort.Reverse(sort.StringSlice(lines)))

	w.Header().Set("Content-Type", "text/plain")
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
}

func (m *ServiceMgr) getInsight(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	apps := make([]string, 0)
	apps = append(apps, r.URL.Query()["name"]...)
	if len(apps) < 1 {
		for app, _ := range m.superSup.GetDeployedApps() {
			apps = append(apps, app)
		}
	}

	var insights *common.Insights
	if rv := r.URL.Query()["aggregate"]; len(rv) > 0 && rv[0] == "true" {
		creds := r.Header
		insights = getGlobalInsights(m, apps, creds)
	} else {
		insights = getLocalInsights(m, apps)
	}

	if rv := r.URL.Query()["udmark"]; len(rv) > 0 && rv[0] == "true" {
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
		logging.Errorf("Got failure getting nodes", err)
		return nil
	}

	psz := sz
	if len(nodes) > 1 {
		psz = sz / int64(len(nodes))
	}

	var lines []string
	for _, node := range nodes {
		url := "http://" + node + "/getAppLog?name=" + appName + "&aggregate=false" + "&size=" + strconv.Itoa(int(psz))
		client := http.Client{Timeout: time.Second * 15}
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			logging.Errorf("Got failure creating http request to %v: %v", node, err)
			continue
		}
		for hk, hvs := range creds {
			for _, hv := range hvs {
				req.Header.Add(hk, hv)
			}
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
		logging.Errorf("Got failure getting nodes", err)
		return insights
	}
	for _, node := range nodes {
		url := "http://" + node + "/getInsight?aggregate=false"
		client := http.Client{Timeout: time.Second * 15}
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			logging.Errorf("Got failure creating http request to %v: %v", node, err)
			continue
		}
		for hk, hvs := range creds {
			for _, hv := range hvs {
				req.Header.Add(hk, hv)
			}
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

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	logging.Debugf("%s Function: %s got request to get V8 debugger url", logPrefix, appName)

	if m.checkIfDeployed(appName) {
		debugURL, _ := m.superSup.GetDebuggerURL(appName)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", debugURL)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "Function: %s not deployed", appName)

}

func (m *ServiceMgr) getLocalDebugURL(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getLocalDebugURL"
	values := r.URL.Query()
	appName := values["name"][0]

	logging.Debugf("%s Function: %s got request to get local V8 debugger url", logPrefix, appName)

	config := m.config.Load()
	dir := config["eventing_dir"].(string)

	filePath := fmt.Sprintf("%s/%s_frontend.url", dir, appName)
	u, err := ioutil.ReadFile(filePath)
	if err != nil {
		logging.Errorf("%s Function: %s failed to read contents from debugger frontend url file, err: %v",
			logPrefix, appName, err)
		fmt.Fprintf(w, "")
		return
	}

	fmt.Fprintf(w, "%v", string(u))
}

func (m *ServiceMgr) logFileLocation(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	c := m.config.Load()
	fmt.Fprintf(w, `{"log_dir":"%v"}`, c["eventing_dir"])
}

func (m *ServiceMgr) notifyDebuggerStart(appName string, hostnames []string) (info *runtimeInfo) {
	logPrefix := "ServiceMgr::notifyDebuggerStart"
	info = &runtimeInfo{}

	uuidGen, err := util.NewUUID()
	if err != nil {
		info.Code = m.statusCodes.errUUIDGen.Code
		info.Info = fmt.Sprintf("Unable to initialize UUID generator, err: %v", err)
		return
	}

	token := uuidGen.Str()
	m.superSup.WriteDebuggerToken(appName, token, hostnames)
	logging.Infof("%s Function: %s notifying on debugger path %s",
		logPrefix, appName, common.MetakvDebuggerPath+appName)

	err = util.MetakvSet(common.MetakvDebuggerPath+appName, []byte(token), nil)

	if err != nil {
		logging.Errorf("%s Function: %s Failed to write to metakv err: %v", logPrefix, appName, err)
		info.Code = m.statusCodes.errMetakvWriteFailed.Code
		info.Info = fmt.Sprintf("Failed to write to metakv debugger path for Function: %s, err: %v", appName, err)
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) startDebugger(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::startDebugger"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("%s Function: %s got request to start debugger", logPrefix, appName)
	audit.Log(auditevent.StartDebug, r, appName)

	config, info := m.getConfig()
	if info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}

	enabled, exists := config["enable_debugger"]
	if !exists || !enabled.(bool) {
		info.Code = m.statusCodes.errDebuggerDisabled.Code
		info.Info = "Debugger is not enabled"
		m.sendErrorInfo(w, info)
		return
	}

	if !m.checkAppExists(appName) {
		info.Code = m.statusCodes.errAppNotFound.Code
		info.Info = fmt.Sprintf("Function %s not found, debugger cannot start", appName)
		m.sendErrorInfo(w, info)
		return
	}

	if !m.checkIfDeployedAndRunning(appName) {
		info.Code = m.statusCodes.errAppNotDeployed.Code
		info.Info = fmt.Sprintf("Function: %s is not in deployed state, debugger cannot start", appName)
		m.sendErrorInfo(w, info)
		return
	}

	var isMixedMode bool
	if isMixedMode, info = m.isMixedModeCluster(); info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}

	if isMixedMode {
		info.Code = m.statusCodes.errMixedMode.Code
		info.Info = "Debugger can not be spawned in a mixed mode cluster"
		m.sendErrorInfo(w, info)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		info.Code = m.statusCodes.errReadReq.Code
		info.Info = fmt.Sprintf("Failed to read request, err : %v", err)
		m.sendErrorInfo(w, info)
		return
	}

	var data map[string]interface{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		info.Code = m.statusCodes.errUnmarshalPld.Code
		info.Info = fmt.Sprintf("Failed to unmarshal request, err : %v", err)
		m.sendErrorInfo(w, info)
		return
	}

	if info = m.notifyDebuggerStart(appName, GetNodesHostname(data)); info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}
	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "Function: %s Started Debugger", appName)
}

func (m *ServiceMgr) stopDebugger(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::stopDebugger"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("%s Function: %s got request to stop V8 debugger", logPrefix, appName)
	audit.Log(auditevent.StopDebug, r, appName)

	if m.checkIfDeployed(appName) {
		m.superSup.SignalStopDebugger(appName)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "Function: %s stopped Debugger", appName)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	respString := fmt.Sprintf("Function: %s not deployed", appName)
	fmt.Fprintf(w, respString)
	logging.Infof("%s %s", logPrefix, respString)
}

func (m *ServiceMgr) writeDebuggerURLHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateLocalAuth(w, r) {
		return
	}

	w.Header().Set("Content-Type", "application/x-www-form-urlencoded")

	appName := path.Base(r.URL.Path)
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		return
	}

	logging.Infof("Received Debugger URL: %s for Function: %s", string(data), appName)
	m.superSup.WriteDebuggerURL(appName, string(data))
	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
}

func (m *ServiceMgr) getEventProcessingStats(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getEventProcessingStats"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	if m.checkIfDeployed(appName) {
		stats := m.superSup.GetEventProcessingStats(appName)

		data, err := json.MarshalIndent(&stats, "", " ")
		if err != nil {
			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
			fmt.Fprintf(w, "Failed to marshal response event processing stats, err: %v", err)
			logging.Errorf("%s Failed to marshal response event processing stats, err: %v", logPrefix, err)
			return
		}

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", string(data))
	} else {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
		respString := fmt.Sprintf("Function: %s not deployed", appName)
		fmt.Fprintf(w, respString)
		logging.Infof("%s %s", logPrefix, respString)
	}
}

func (m *ServiceMgr) getAppList() (map[string]int, map[string]int, map[string]int, int, *runtimeInfo) {
	logPrefix := "ServiceMgr::getAppList"
	info := &runtimeInfo{}

	nodeAddrs, err := m.getActiveNodeAddrs()
	if err != nil {
		logging.Warnf("%s failed to fetch active Eventing nodes, err: %v", logPrefix, err)

		info.Code = m.statusCodes.errActiveEventingNodes.Code
		info.Info = fmt.Sprintf("Unable to fetch active Eventing nodes, err: %v", err)
		return nil, nil, nil, 0, info
	}

	numEventingNodes := len(nodeAddrs)
	if numEventingNodes == 0 {
		info.Code = m.statusCodes.errNoEventingNodes.Code
		return nil, nil, nil, 0, info
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

	mhVersion := eventingVerMap["mad-hatter"]
	if m.compareEventingVersion(mhVersion) {
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

		info.Code = m.statusCodes.ok.Code
		return appDeployedNodesCounter, appBootstrappingNodesCounter, appPausingNodesCounter, numEventingNodes, info
	}

	info.Code = m.statusCodes.ok.Code
	return appDeployedNodesCounter, appBootstrappingNodesCounter, nil, numEventingNodes, info
}

// Returns list of apps that are deployed i.e. finished dcp/timer/debugger related bootstrap
func (m *ServiceMgr) getDeployedApps(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getDeployedApps"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	audit.Log(auditevent.ListDeployed, r, nil)

	appDeployedNodesCounter, _, appPausingNodesCounter, numEventingNodes, info := m.getAppList()
	if info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}

	deployedApps := make(map[string]string)
	for app, numNodesDeployed := range appDeployedNodesCounter {
		if appPausingNodesCounter != nil {
			_, ok := appPausingNodesCounter[app]
			if numNodesDeployed == numEventingNodes && !ok {
				deployedApps[app] = ""
			}
		} else {
			if numNodesDeployed == numEventingNodes {
				deployedApps[app] = ""
			}
		}
	}

	data, err := json.MarshalIndent(deployedApps, "", " ")
	if err != nil {
		logging.Errorf("%s failed to marshal list of deployed apps, err: %v", logPrefix, err)

		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("Unable to marshall response, err: %v", err)
		m.sendErrorInfo(w, info)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s", string(data))
}

// Returns list of apps that are running i.e. they may be undergoing undeploy(one ore more nodes)
// or are deployed on all nodes
func (m *ServiceMgr) getRunningApps(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getRunningApps"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	audit.Log(auditevent.ListRunning, r, nil)

	appDeployedNodesCounter, _, _, numEventingNodes, info := m.getAppList()
	if info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}

	runningApps := make(map[string]string)
	for app, numNodesDeployed := range appDeployedNodesCounter {
		if numNodesDeployed <= numEventingNodes {
			runningApps[app] = ""
		}
	}

	data, err := json.MarshalIndent(runningApps, "", " ")
	if err != nil {
		logging.Errorf("%s failed to marshal list of running apps, err: %v", logPrefix, err)

		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("Unable to marshal response, err: %v", err)
		m.sendErrorInfo(w, info)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s", string(data))
}

func (m *ServiceMgr) getLocallyDeployedApps(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getLocallyDeployedApps"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	deployedApps := m.superSup.GetDeployedApps()

	buf, err := json.MarshalIndent(deployedApps, "", " ")
	if err != nil {
		logging.Errorf("%s failed to marshal list of deployed apps, err: %v", logPrefix, err)
		fmt.Fprintf(w, "")
		return
	}

	fmt.Fprintf(w, "%s", string(buf))
}

// Reports progress across all producers on current node
func (m *ServiceMgr) getRebalanceProgress(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getRebalanceProgress"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	progress := &common.RebalanceProgress{}

	m.fnMu.RLock()
	for appName := range m.fnsInPrimaryStore {
		// TODO: Leverage error returned from rebalance task progress and fail the rebalance
		// if it occurs
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
	m.fnMu.RUnlock()

	if progress.VbsRemainingToShuffle > 0 {
		m.statsWritten = false
	}

	if progress.VbsRemainingToShuffle == 0 && progress.VbsOwnedPerPlan == 0 && !m.statsWritten {
		// Picking up subset of the stats
		statsList := m.populateStats(false)
		data, err := json.MarshalIndent(statsList, "", " ")
		if err != nil {
			logging.Errorf("%s failed to unmarshal stats, err: %v", logPrefix, err)
		} else {
			logging.Tracef("%s no more vbucket remaining to shuffle. Stats dump: %v", logPrefix, string(data))
		}

		m.statsWritten = true
	}

	buf, err := json.MarshalIndent(progress, "", " ")
	if err != nil {
		logging.Errorf("%s failed to unmarshal rebalance progress across all producers on current node, err: %v", logPrefix, err)
		return
	}

	w.Write(buf)
}

// Report back state of rebalance on current node
func (m *ServiceMgr) getRebalanceStatus(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	w.Write([]byte(strconv.FormatBool(m.superSup.RebalanceStatus())))
}

// Report back state of bootstrap on current node
func (m *ServiceMgr) getBootstrapStatus(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	bootstrapAppList := m.superSup.BootstrapAppList()
	if len(bootstrapAppList) > 0 {
		w.Write([]byte(strconv.FormatBool(true)))
	} else {
		w.Write([]byte(strconv.FormatBool(m.superSup.BootstrapStatus())))
	}
}

// Report back state of an app bootstrap on current node
func (m *ServiceMgr) getBootstrapAppStatus(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	appName := r.URL.Query()["appName"]
	if len(appName) == 0 {
		return
	}

	bootstrapAppList := m.superSup.BootstrapAppList()
	_, isBootstrapping := bootstrapAppList[appName[0]]
	if isBootstrapping {
		w.Write([]byte(strconv.FormatBool(true)))
	} else {
		w.Write([]byte(strconv.FormatBool(m.superSup.BootstrapAppStatus(appName[0]))))
	}
}

// Reports aggregated event processing stats from all producers
func (m *ServiceMgr) getAggEventProcessingStats(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getAggEventProcessingStats"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	pStats, err := util.GetEventProcessingStats("/getEventProcessingStats?name="+appName, m.eventingNodeAddrs)
	if err != nil {
		fmt.Fprintf(w, "Failed to get event processing stats, err: %v", err)
		return
	}

	buf, err := json.MarshalIndent(pStats, "", " ")
	if err != nil {
		logging.Errorf("%s Failed to unmarshal event processing stats from all producers, err: %v", logPrefix, err)
		return
	}

	fmt.Fprintf(w, "%s", string(buf))
}

// Reports aggregated rebalance progress from all Eventing nodes in the cluster
func (m *ServiceMgr) getAggRebalanceProgress(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getAggRebalanceProgress"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	logging.Infof("%s going to query eventing nodes: %rs for rebalance progress",
		logPrefix, m.eventingNodeAddrs)

	aggProgress, progressMap, errMap := util.GetProgress("/getRebalanceProgress", m.eventingNodeAddrs)
	if len(errMap) > 0 {
		logging.Warnf("%s failed to get progress from some/all eventing nodes: %rs err: %rs",
			logPrefix, m.eventingNodeAddrs, errMap)
		return
	}

	aggProgress.NodeLevelStats = progressMap

	buf, err := json.MarshalIndent(aggProgress, "", " ")
	if err != nil {
		logging.Errorf("%s failed to unmarshal rebalance progress across all producers, err: %v", logPrefix, err)
		return
	}

	w.Write(buf)
}

// Report aggregated rebalance status from all Eventing nodes in the cluster
func (m *ServiceMgr) getAggRebalanceStatus(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getAggRebalanceStatus"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	status, err := util.CheckIfRebalanceOngoing("/getRebalanceStatus", m.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("%s failed to grab correct rebalance status from some/all nodes, err: %v", logPrefix, err)
		return
	}

	w.Write([]byte(strconv.FormatBool(status)))
}

// Report aggregated bootstrap status from all Eventing nodes in the cluster
func (m *ServiceMgr) getAggBootstrapStatus(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getAggBootstrapStatus"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	status, err := util.CheckIfBootstrapOngoing("/getBootstrapStatus", m.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("%s failed to grab correct bootstrap status from some/all nodes, err: %v", logPrefix, err)
		return
	}

	w.Write([]byte(strconv.FormatBool(status)))
}

// Report aggregated bootstrap status of an app from all Eventing nodes in the cluster
func (m *ServiceMgr) getAggBootstrapAppStatus(w http.ResponseWriter, r *http.Request) {
	logPrefix := "SeriveMgr::getAggBootstrapAppStatus"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	appName := r.URL.Query()["appName"]
	if len(appName) == 0 {
		return
	}

	status, err := util.CheckIfAppBootstrapOngoing("/getBootstrapAppStatus", m.eventingNodeAddrs, appName[0])
	if err != nil {
		logging.Errorf("%s failed to grab correct bootstrap status of app from some/all nodes, err: %v", logPrefix, err)
		return
	}

	w.Write([]byte(strconv.FormatBool(status)))
}

func (m *ServiceMgr) getLatencyStats(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getLatencyStats"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	if m.checkIfDeployed(appName) {
		lStats := m.superSup.GetLatencyStats(appName)

		data, err := json.MarshalIndent(lStats, "", " ")
		if err != nil {
			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
			fmt.Fprintf(w, "Failed to unmarshal latency stats, err: %v\n", err)
			logging.Errorf("%s Function: %s failed to unmarshal latency stats, err: %v", logPrefix, appName, err)
			return
		}

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", string(data))
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "Function: %s not deployed", appName)
}

func (m *ServiceMgr) getExecutionStats(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getExecutionStats"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	if m.checkIfDeployed(appName) {
		eStats := m.superSup.GetExecutionStats(appName)

		data, err := json.MarshalIndent(eStats, "", " ")
		if err != nil {
			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
			fmt.Fprintf(w, "Failed to unmarshal execution stats, err: %v\n", err)
			logging.Errorf("%s Function: %s failed to unmarshal execution stats, err: %v", logPrefix, appName, err)
			return
		}

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", string(data))
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "Function: %s not deployed", appName)
}

func (m *ServiceMgr) getFailureStats(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getFailureStats"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	if m.checkIfDeployed(appName) {
		fStats := m.superSup.GetFailureStats(appName)

		data, err := json.MarshalIndent(fStats, "", " ")
		if err != nil {
			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
			fmt.Fprintf(w, "Failed to unmarshal failure stats, err: %v\n", err)
			logging.Errorf("%s Function: %s failed to unmarshal failure stats, err: %v", logPrefix, appName, err)
			return
		}

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", string(data))
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "Function: %s not deployed", appName)
}

func (m *ServiceMgr) getSeqsProcessed(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getSeqsProcessed"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	if m.checkIfDeployed(appName) {
		seqNoProcessed := m.superSup.GetSeqsProcessed(appName)

		data, err := json.MarshalIndent(seqNoProcessed, "", " ")
		if err != nil {
			logging.Errorf("%s Function: %s failed to fetch vb sequences processed so far, err: %v", logPrefix, appName, err)
			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errGetVbSeqs.Code))
			fmt.Fprintf(w, "")
			return
		}

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", string(data))
	} else {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
		fmt.Fprintf(w, "Function: %s not deployed", appName)
	}

}

func (m *ServiceMgr) setSettingsHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::setSettingsHandler"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	audit.Log(auditevent.SetSettings, r, appName)
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logging.Errorf("%s Function: %s failed to read request body, err: %v", logPrefix, appName, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errReadReq.Code))
		fmt.Fprintf(w, "Failed to read request body, err: %v", err)
		return
	}

	var settings map[string]interface{}
	err = json.Unmarshal(data, &settings)
	if err != nil {
		logging.Errorf("%s Function: %s failed to unmarshal setting supplied, err: %v", logPrefix, appName, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errUnmarshalPld.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errUnmarshalPld.Code))
		fmt.Fprintf(w, "Failed to unmarshal setting supplied, err: %v", err)
		return
	}

	if info := m.setSettings(appName, data); info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
}

func (m *ServiceMgr) getSettings(appName string) (*map[string]interface{}, *runtimeInfo) {
	logPrefix := "ServiceMgr::getSettings"

	logging.Infof("%s Function: %s fetching settings", logPrefix, appName)
	app, status := m.getTempStore(appName)
	if status.Code != m.statusCodes.ok.Code {
		return nil, status
	}

	info := runtimeInfo{}

	info.Code = m.statusCodes.ok.Code
	info.Info = fmt.Sprintf("Function: %s fetched settings", appName)
	logging.Infof("%s %s", logPrefix, info.Info)
	return &app.Settings, &info
}

func (m *ServiceMgr) setSettings(appName string, data []byte) (info *runtimeInfo) {
	logPrefix := "ServiceMgr::setSettings"

	info = &runtimeInfo{}
	logging.Infof("%s Function: %s save settings", logPrefix, appName)

	var settings map[string]interface{}
	err := json.Unmarshal(data, &settings)
	if err != nil {
		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("Function: %s failed to unmarshal setting supplied, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	if value, ok := settings["num_timer_partitions"]; ok {
		switch value.(type) {
		case string:
			settings["num_timer_partitions"], err = strconv.ParseFloat(value.(string), 64)
			if err != nil {
				logging.Errorf("%s Function: num_timer_partitions is in invalid format.", logPrefix)
				info.Code = m.statusCodes.errInvalidConfig.Code
				info.Info = fmt.Sprintf("num_timer_partitions format is invalid.")
				return
			}
		case int:
			settings["num_timer_partitions"] = float64(value.(int))
		}
	}

	if info = m.validateSettings(appName, util.DeepCopy(settings)); info.Code != m.statusCodes.ok.Code {
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	logging.Infof("%s Function: %s settings params: %+v", logPrefix, appName, settings)

	_, procStatExists := settings["processing_status"]
	_, depStatExists := settings["deployment_status"]

	mhVersion := eventingVerMap["mad-hatter"]

	if procStatExists || depStatExists {
		if m.compareEventingVersion(mhVersion) {
			if settings["deployment_status"].(bool) {
				status, err := util.GetAggBootstrapAppStatus(net.JoinHostPort(util.Localhost(), m.adminHTTPPort), appName)
				if err != nil {
					logging.Errorf("%s %s", logPrefix, err)
					info.Code = m.statusCodes.errStatusesNotFound.Code
					info.Info = "Failed to find app status"
					return
				}

				if status {
					info.Code = m.statusCodes.errAppNotInit.Code
					info.Info = "Function is undergoing bootstrap"
					return
				}
			}
		}

		if lifeCycleOpsInfo := m.checkLifeCycleOpsDuringRebalance(); lifeCycleOpsInfo.Code != m.statusCodes.ok.Code {
			info.Code = lifeCycleOpsInfo.Code
			info.Info = lifeCycleOpsInfo.Info
			return
		}
	}

	// Get the app from temp store and update its settings with those provided
	app, info := m.getTempStore(appName)
	if info.Code != m.statusCodes.ok.Code {
		return
	}

	existingBoundary := app.Settings["dcp_stream_boundary"]
	newBoundary, dsbOk := settings["dcp_stream_boundary"]
	newTPValue, timerPartitionsPresent := settings["num_timer_partitions"]
	oldTPValue, oldTimerPartitionsPresent := app.Settings["num_timer_partitions"]

	for setting := range settings {
		app.Settings[setting] = settings[setting]
	}

	processingStatus, pOk := app.Settings["processing_status"].(bool)
	deploymentStatus, dOk := app.Settings["deployment_status"].(bool)

	logging.Infof("%s Function: %s deployment status: %t processing status: %t",
		logPrefix, appName, deploymentStatus, processingStatus)

	deployedApps := m.superSup.GetDeployedApps()
	if pOk && dOk {
		var isMixedMode bool
		if isMixedMode, info = m.isMixedModeCluster(); info.Code != m.statusCodes.ok.Code {
			logging.Errorf("%s %s", logPrefix, info.Info)
			return
		}

		if isMixedMode && !m.isUndeployOperation(app.Settings) {
			info.Code = m.statusCodes.errMixedMode.Code
			info.Info = "Life-cycle operations except delete and undeploy are not allowed in a mixed mode cluster"
			logging.Errorf("%s %s", logPrefix, info.Info)
			return
		}

		// Resetting dcp_stream_boundary to everything during undeployment
		if !processingStatus && !deploymentStatus {
			app.Settings["dcp_stream_boundary"] = common.DcpEverything
		}

		// Check for pause processing
		if deploymentStatus && !processingStatus {
			if !m.compareEventingVersion(mhVersion) {
				info.Code = m.statusCodes.errClusterVersion.Code
				info.Info = fmt.Sprintf("All eventing nodes in the cluster must be on version %d.%d or higher for pausing function execution",
					mhVersion.major, mhVersion.minor)
				logging.Warnf("%s Version compat check failed: %s", logPrefix, info.Info)
				return
			}

			if _, ok := deployedApps[appName]; !ok {
				info.Code = m.statusCodes.errAppNotInit.Code
				info.Info = fmt.Sprintf("Function: %s not processing mutations. Operation is not permitted. Edit function instead", appName)
				logging.Errorf("%s %s", logPrefix, info.Info)
				return
			}

			if oldTimerPartitionsPresent {
				if timerPartitionsPresent && oldTPValue != newTPValue {
					info.Code = m.statusCodes.errInvalidConfig.Code
					info.Info = fmt.Sprintf("Function: %s num_timer_partitions cannot be altered when trying to pause the function.", appName)
					logging.Errorf("%s %s", logPrefix, info.Info)
					return
				}
			} else {
				if timerPartitionsPresent {
					info.Code = m.statusCodes.errInvalidConfig.Code
					info.Info = fmt.Sprintf("Function: %s num_timer_partitions cannot be set when trying to pause the function.", appName)
					logging.Errorf("%s %s", logPrefix, info.Info)
					return
				}
			}
		}

		if filterFeedBoundary(settings) == common.DcpFromPrior && !m.compareEventingVersion(mhVersion) {
			info.Code = m.statusCodes.errClusterVersion.Code
			info.Info = fmt.Sprintf("All eventing nodes in cluster must be on version %d.%d or higher for resuming function execution",
				mhVersion.major, mhVersion.minor)
			logging.Warnf("%s Version compat check failed: %s", logPrefix, info.Info)
			return
		}

		if filterFeedBoundary(settings) == common.DcpFromPrior && m.superSup.GetAppState(appName) != common.AppStatePaused {
			info.Code = m.statusCodes.errInvalidConfig.Code
			info.Info = fmt.Sprintf("Function: %s feed boundary: from_prior is only allowed if function is in paused state, current state: %v",
				appName, m.superSup.GetAppState(appName))
			logging.Errorf("%s %s", logPrefix, info.Info)
			return
		}

		if deploymentStatus && processingStatus {
			if m.superSup.GetAppState(appName) == common.AppStatePaused {
				switch filterFeedBoundary(settings) {
				case common.DcpFromNow, common.DcpEverything:
					info.Code = m.statusCodes.errInvalidConfig.Code
					info.Info = fmt.Sprintf("Function: %s only from_prior feed boundary is allowed during resume", appName)
					logging.Errorf("%s %s", logPrefix, info.Info)
					return
				case common.DcpStreamBoundary(""):
					app.Settings["dcp_stream_boundary"] = "from_prior"
				default:
				}
				if oldTimerPartitionsPresent {
					if timerPartitionsPresent && oldTPValue != newTPValue {
						info.Code = m.statusCodes.errInvalidConfig.Code
						info.Info = fmt.Sprintf("Function: %s num_timer_partitions cannot be changed when trying to resume the function.", appName)
						logging.Errorf("%s %s", logPrefix, info.Info)
						return
					}
				} else {
					if timerPartitionsPresent {
						info.Code = m.statusCodes.errInvalidConfig.Code
						info.Info = fmt.Sprintf("Function: %s num_timer_partitions cannot be set when trying to resume the function.", appName)
						logging.Errorf("%s %s", logPrefix, info.Info)
						return
					}
				}
			}

			if oldTimerPartitionsPresent {
				if timerPartitionsPresent && m.checkIfDeployed(appName) && oldTPValue != newTPValue {
					info.Code = m.statusCodes.errInvalidConfig.Code
					info.Info = fmt.Sprintf("Function: %s num_timer_partitions cannot be changed when the function is in deployed state.", appName)
					logging.Errorf("%s %s", logPrefix, info.Info)
					return
				}
			} else {
				if timerPartitionsPresent && m.checkIfDeployed(appName) {
					info.Code = m.statusCodes.errInvalidConfig.Code
					info.Info = fmt.Sprintf("Function: %s num_timer_partitions cannot be changed when the function is in deployed state.", appName)
					logging.Errorf("%s %s", logPrefix, info.Info)
					return
				}
				if !m.checkIfDeployed(appName) {
					m.addDefaultTimerPartitionsIfMissing(&app)
				}
			}

			if dsbOk && m.superSup.GetAppState(appName) == common.AppStateEnabled && newBoundary != existingBoundary {
				info.Code = m.statusCodes.errAppDeployed.Code
				info.Info = "DCP stream boundary cannot be changed while the app is deployed"
				logging.Errorf("%s %s", logPrefix, info.Info)
				return
			}

			// Write to primary store in case of deployment
			if !m.checkIfDeployedAndRunning(appName) {
				info = m.savePrimaryStore(&app)
				if info.Code != m.statusCodes.ok.Code {
					logging.Errorf("%s %s", logPrefix, info.Info)
					return
				}
			}
		}
	} else {
		info.Code = m.statusCodes.errStatusesNotFound.Code
		info.Info = fmt.Sprintf("Function: %s missing processing or deployment statuses or both", appName)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	data, err = json.MarshalIndent(app.Settings, "", " ")
	if err != nil {
		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("Function: %s failed to marshal settings, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	metakvPath := metakvAppSettingsPath + appName
	err = util.MetakvSet(metakvPath, data, nil)
	if err != nil {
		info.Code = m.statusCodes.errSetSettingsPs.Code
		info.Info = fmt.Sprintf("Function: %s failed to store setting, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Info)

		return
	}

	// Write the updated app along with its settings back to temp store
	if info = m.saveTempStore(app); info.Code != m.statusCodes.ok.Code {
		return
	}

	info.Code = m.statusCodes.ok.Code
	info.Info = fmt.Sprintf("Function: %s stored settings", appName)
	logging.Infof("%s %s", logPrefix, info.Info)
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

	d := new(cfg.DepCfg)
	depcfg := new(depCfg)
	dcfg := config.DepCfg(d)

	depcfg.MetadataBucket = string(dcfg.MetadataBucket())
	depcfg.SourceBucket = string(dcfg.SourceBucket())
	depcfg.SourceScope = string(dcfg.SourceScope())
	depcfg.SourceCollection = string(dcfg.SourceCollection())
	depcfg.MetadataCollection = string(dcfg.MetadataCollection())
	depcfg.MetadataScope = string(dcfg.MetadataScope())

	var buckets []bucket
	b := new(cfg.Bucket)
	for i := 0; i < dcfg.BucketsLength(); i++ {

		if dcfg.Buckets(b, i) {
			newBucket := bucket{
				Alias:          string(b.Alias()),
				BucketName:     string(b.BucketName()),
				Access:         string(config.Access(i)),
				ScopeName:      string(b.ScopeName()),
				CollectionName: string(b.CollectionName()),
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

	return app
}

func (m *ServiceMgr) getPrimaryStoreHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getPrimaryStoreHandler"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	logging.Infof("%s getting all functions from primary store", logPrefix)
	audit.Log(auditevent.FetchFunctions, r, nil)

	appList := util.ListChildren(metakvAppsPath)
	respData := make([]application, len(appList))

	for index, fnName := range appList {
		data, err := util.ReadAppContent(metakvAppsPath, metakvChecksumPath, fnName)
		if err == nil && data != nil {
			respData[index] = m.parseFunctionPayload(data, fnName)
		}
	}

	data, err := json.MarshalIndent(respData, "", " ")
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, "Failed to marshal response for all functions, err: %v", err)
		logging.Errorf("%s failed to marshal response for all functions, err: %v", logPrefix, err)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s\n", data)
}

func (m *ServiceMgr) getTempStoreHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getTempStoreHandler"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	// Moving just this case to trace log level as ns_server keeps polling
	// eventing every 5s to see if new functions have been created. So on an idle
	// cluster it will log lot of this message.
	logging.Tracef("%s fetching function draft definitions", logPrefix)
	audit.Log(auditevent.FetchDrafts, r, nil)
	applications := m.getTempStoreAll()

	// Remove the "num_timer_partitions" and don't send it to the UI
	for _, app := range applications {
		if _, ok := app.Settings["num_timer_partitions"]; ok {
			delete(app.Settings, "num_timer_partitions")
		}
	}

	data, err := json.MarshalIndent(applications, "", " ")
	if err != nil {
		logging.Errorf("%s failed to marshal response, err: %v", logPrefix, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, `{"error":"Failed to marshal response, err: %v"}`, err)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s\n", data)
}

func (m *ServiceMgr) getTempStore(appName string) (application, *runtimeInfo) {
	logPrefix := "ServiceMgr::getTempStore"

	info := &runtimeInfo{}
	logging.Infof("%s Function: %s fetching function draft definitions", logPrefix, appName)

	for _, name := range util.ListChildren(metakvTempAppsPath) {
		data, err := util.ReadAppContent(metakvTempAppsPath, metakvTempChecksumPath, name)
		if err == nil && data != nil {
			var app application
			uErr := json.Unmarshal(data, &app)
			if uErr != nil {
				logging.Errorf("%s Function: %s failed to unmarshal data from metakv, err: %v", logPrefix, appName, uErr)
				continue
			}

			if app.Name == appName {
				info.Code = m.statusCodes.ok.Code
				// Hide some internal settings from being exported
				delete(app.Settings, "handler_uuid")
				return app, info
			}
		}
	}

	info.Code = m.statusCodes.errAppNotFoundTs.Code
	info.Info = fmt.Sprintf("Function: %s not found", appName)
	logging.Infof("%s %s", logPrefix, info.Info)
	return application{}, info
}

func (m *ServiceMgr) getTempStoreAll() []application {
	logPrefix := "ServiceMgr::getTempStoreAll"

	m.fnMu.RLock()
	defer m.fnMu.RUnlock()

	applications := []application{}

	for fnName := range m.fnsInTempStore {
		data, err := util.ReadAppContent(metakvTempAppsPath, metakvTempChecksumPath, fnName)
		if err == nil && data != nil {
			var app application
			uErr := json.Unmarshal(data, &app)
			if uErr != nil {
				logging.Errorf("%s Function: %s failed to unmarshal data from metakv, err: %v data: %v",
					logPrefix, fnName, uErr, string(data))
				continue
			}
			applications = append(applications, app)
		} else if err != nil {
			logging.Errorf("%s Function: %s failed to read data from metakv, err: %v", logPrefix, fnName, err)
		} else {
			logging.Errorf("%s Function: %s data read is nil", logPrefix, fnName)
		}
	}

	return applications
}

func (m *ServiceMgr) saveTempStoreHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::saveTempStoreHandler"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	audit.Log(auditevent.SaveDraft, r, appName)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logging.Errorf("%s Function: %s failed to read request body, err: %v", logPrefix, appName, err)

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errReadReq.Code))
		fmt.Fprintf(w, "Failed to read request body, err: %v", err)
		return
	}

	var app application
	err = json.Unmarshal(data, &app)
	if err != nil {
		errString := fmt.Sprintf("Function: %s failed to unmarshal payload", appName)
		logging.Errorf("%s %s, err: %v", logPrefix, errString, err)

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errUnmarshalPld.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errUnmarshalPld.Code))

		logging.Errorf("%s %s, err: %v", logPrefix, errString, err)
		fmt.Fprintf(w, "%s\n", errString)
		return
	}
	m.addDefaultTimerPartitionsIfMissing(&app)

	m.addDefaultDeploymentConfig(&app)
	if info := m.validateApplication(&app); info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}

	info := m.saveTempStore(app)
	m.sendRuntimeInfo(w, info)
}

// Saves application to temp store
func (m *ServiceMgr) saveTempStore(app application) (info *runtimeInfo) {
	logPrefix := "ServiceMgr::saveTempStore"
	info = &runtimeInfo{}
	appName := app.Name

	data, err := json.MarshalIndent(app, "", " ")
	if err != nil {
		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("Function: %s failed to marshal data, err : %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	//Delete stale entry
	err = util.DeleteStaleAppContent(metakvTempAppsPath, appName)
	if err != nil {
		info.Code = m.statusCodes.errSaveAppTs.Code
		info.Info = fmt.Sprintf("Function: %s failed to clean up stale entry from temp store, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	compressPayload := m.checkCompressHandler()
	err = util.WriteAppContent(metakvTempAppsPath, metakvTempChecksumPath, appName, data, compressPayload)
	if err != nil {
		info.Code = m.statusCodes.errSaveAppTs.Code
		info.Info = fmt.Sprintf("Function: %s failed to store in temp store, err: %v", appName, err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	info.Code = m.statusCodes.ok.Code
	info.Info = fmt.Sprintf("Function: %s stored in temp store", appName)
	logging.Infof("%s %s", logPrefix, info.Info)
	return
}

func (m *ServiceMgr) savePrimaryStoreHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::savePrimaryStoreHandler"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	audit.Log(auditevent.CreateFunction, r, appName)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errString := fmt.Sprintf("Function: %s failed to read content from http request body", appName)
		logging.Errorf("%s %s err: %v", logPrefix, errString, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		fmt.Fprintf(w, "%s\n", errString)
		return
	}

	var app application
	err = json.Unmarshal(data, &app)
	if err != nil {
		errString := fmt.Sprintf("Function: %s failed to unmarshal payload", appName)
		logging.Errorf("%s %s, err: %v", logPrefix, errString, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errUnmarshalPld.Code))
		fmt.Fprintf(w, "%s\n", errString)
		return
	}

	m.addDefaultDeploymentConfig(&app)
	m.addDefaultTimerPartitionsIfMissing(&app)

	if info := m.validateApplication(&app); info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}

	info := m.savePrimaryStore(&app)
	m.sendRuntimeInfo(w, info)
}

func (m *ServiceMgr) checkRebalanceStatus() (info *runtimeInfo) {
	logPrefix := "ServiceMgr::checkRebalanceStatus"
	info = &runtimeInfo{}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	rebStatus, err := util.CheckIfRebalanceOngoing("/getRebalanceStatus", m.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("%s Failed to grab correct rebalance or failover status from some/all Eventing nodes, err: %v", logPrefix, err)
		info.Code = m.statusCodes.errGetRebStatus.Code
		info.Info = "Failed to get rebalance or failover status from eventing nodes"
		return
	}

	logging.Infof("%s Rebalance or Failover ongoing across some/all Eventing nodes: %v", logPrefix, rebStatus)

	if rebStatus {
		logging.Warnf("%s Rebalance or Failover ongoing on some/all Eventing nodes", logPrefix)
		info.Code = m.statusCodes.errRebOrFailoverOngoing.Code
		info.Info = "Rebalance or Failover processing ongoing on some/all Eventing nodes, creating new functions, deployment or undeployment of existing functions is not allowed"
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) encodeAppPayload(app *application) []byte {
	builder := flatbuffers.NewBuilder(0)

	var curlBindings []flatbuffers.UOffsetT
	for i := 0; i < len(app.DeploymentConfig.Curl); i++ {
		authTypeEncoded := builder.CreateString(app.DeploymentConfig.Curl[i].AuthType)
		hostnameEncoded := builder.CreateString(app.DeploymentConfig.Curl[i].Hostname)
		valueEncoded := builder.CreateString(app.DeploymentConfig.Curl[i].Value)
		passwordEncoded := builder.CreateString(app.DeploymentConfig.Curl[i].Password)
		usernameEncoded := builder.CreateString(app.DeploymentConfig.Curl[i].Username)
		bearerKeyEncoded := builder.CreateString(app.DeploymentConfig.Curl[i].BearerKey)
		cookiesEncoded := byte(0x0)
		if app.DeploymentConfig.Curl[i].AllowCookies {
			cookiesEncoded = byte(0x1)
		}
		validateSSLCertificateEncoded := byte(0x0)
		if app.DeploymentConfig.Curl[i].ValidateSSLCertificate {
			validateSSLCertificateEncoded = byte(0x1)
		}

		cfg.CurlStart(builder)
		cfg.CurlAddAuthType(builder, authTypeEncoded)
		cfg.CurlAddHostname(builder, hostnameEncoded)
		cfg.CurlAddValue(builder, valueEncoded)
		cfg.CurlAddPassword(builder, passwordEncoded)
		cfg.CurlAddUsername(builder, usernameEncoded)
		cfg.CurlAddBearerKey(builder, bearerKeyEncoded)
		cfg.CurlAddAllowCookies(builder, cookiesEncoded)
		cfg.CurlAddValidateSSLCertificate(builder, validateSSLCertificateEncoded)
		curlBindingsEnd := cfg.CurlEnd(builder)

		curlBindings = append(curlBindings, curlBindingsEnd)
	}

	cfg.ConfigStartCurlVector(builder, len(curlBindings))
	for i := 0; i < len(curlBindings); i++ {
		builder.PrependUOffsetT(curlBindings[i])
	}
	curlBindingsVector := builder.EndVector(len(curlBindings))

	var bNames []flatbuffers.UOffsetT
	var bucketAccess []flatbuffers.UOffsetT
	for i := 0; i < len(app.DeploymentConfig.Buckets); i++ {
		alias := builder.CreateString(app.DeploymentConfig.Buckets[i].Alias)
		bName := builder.CreateString(app.DeploymentConfig.Buckets[i].BucketName)
		bAccess := builder.CreateString(app.DeploymentConfig.Buckets[i].Access)
		sName := builder.CreateString(app.DeploymentConfig.Buckets[i].ScopeName)
		cName := builder.CreateString(app.DeploymentConfig.Buckets[i].CollectionName)

		cfg.BucketStart(builder)
		cfg.BucketAddAlias(builder, alias)
		cfg.BucketAddBucketName(builder, bName)
		cfg.BucketAddScopeName(builder, sName)
		cfg.BucketAddCollectionName(builder, cName)
		csBucket := cfg.BucketEnd(builder)

		bNames = append(bNames, csBucket)
		bucketAccess = append(bucketAccess, bAccess)
	}

	cfg.ConfigStartAccessVector(builder, len(bucketAccess))
	for i := 0; i < len(bucketAccess); i++ {
		builder.PrependUOffsetT(bucketAccess[i])
	}
	access := builder.EndVector(len(bucketAccess))

	cfg.DepCfgStartBucketsVector(builder, len(bNames))
	for i := 0; i < len(bNames); i++ {
		builder.PrependUOffsetT(bNames[i])
	}
	buckets := builder.EndVector(len(bNames))

	metaBucket := builder.CreateString(app.DeploymentConfig.MetadataBucket)
	sourceBucket := builder.CreateString(app.DeploymentConfig.SourceBucket)
	metadataCollection := builder.CreateString(app.DeploymentConfig.MetadataCollection)
	metadataScope := builder.CreateString(app.DeploymentConfig.MetadataScope)
	sourceScope := builder.CreateString(app.DeploymentConfig.SourceScope)
	sourceCollection := builder.CreateString(app.DeploymentConfig.SourceCollection)

	cfg.DepCfgStart(builder)
	cfg.DepCfgAddBuckets(builder, buckets)

	cfg.DepCfgAddMetadataBucket(builder, metaBucket)
	cfg.DepCfgAddSourceBucket(builder, sourceBucket)
	cfg.DepCfgAddMetadataCollection(builder, metadataCollection)
	cfg.DepCfgAddSourceCollection(builder, sourceCollection)
	cfg.DepCfgAddSourceScope(builder, sourceScope)
	cfg.DepCfgAddMetadataScope(builder, metadataScope)

	depcfg := cfg.DepCfgEnd(builder)

	appCode := builder.CreateString(app.AppHandlers)
	aName := builder.CreateString(app.Name)
	fiid := builder.CreateString(app.FunctionInstanceID)

	cfg.ConfigStart(builder)
	cfg.ConfigAddAppCode(builder, appCode)
	cfg.ConfigAddAppName(builder, aName)
	cfg.ConfigAddDepCfg(builder, depcfg)
	cfg.ConfigAddHandlerUUID(builder, app.FunctionID)
	cfg.ConfigAddCurl(builder, curlBindingsVector)
	cfg.ConfigAddAccess(builder, access)
	cfg.ConfigAddFunctionInstanceID(builder, fiid)

	config := cfg.ConfigEnd(builder)

	builder.Finish(config)

	return builder.FinishedBytes()
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
func (m *ServiceMgr) savePrimaryStore(app *application) (info *runtimeInfo) {
	logPrefix := "ServiceMgr::savePrimaryStore"

	info = &runtimeInfo{}
	logging.Infof("%s Function: %s saving to primary store", logPrefix, app.Name)

	if lifeCycleOpsInfo := m.checkLifeCycleOpsDuringRebalance(); lifeCycleOpsInfo.Code != m.statusCodes.ok.Code {
		info.Code = lifeCycleOpsInfo.Code
		info.Info = lifeCycleOpsInfo.Info
		return
	}

	if m.checkIfDeployed(app.Name) && m.superSup.GetAppState(app.Name) != common.AppStatePaused {
		info.Code = m.statusCodes.errAppDeployed.Code
		info.Info = fmt.Sprintf("Function: %s another function with same name is already deployed, skipping save request", app.Name)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	if app.DeploymentConfig.SourceBucket == app.DeploymentConfig.MetadataBucket {
		info.Code = m.statusCodes.errSrcMbSame.Code
		info.Info = fmt.Sprintf("Function: %s source bucket same as metadata bucket. source_bucket : %s metadata_bucket : %s",
			app.Name, app.DeploymentConfig.SourceBucket, app.DeploymentConfig.MetadataBucket)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	mhVersion := eventingVerMap["mad-hatter"]
	if filterFeedBoundary(app.Settings) == common.DcpFromPrior && !m.compareEventingVersion(mhVersion) {
		info.Code = m.statusCodes.errClusterVersion.Code
		info.Info = fmt.Sprintf("All eventing nodes in the cluster must be on version %d.%d or higher for using 'from prior' deployment feed boundary",
			mhVersion.major, mhVersion.minor)
		logging.Warnf("%s Version compat check failed: %s", logPrefix, info.Info)
		return
	}

	if filterFeedBoundary(app.Settings) == common.DcpFromPrior && m.superSup.GetAppState(app.Name) != common.AppStatePaused {
		info.Code = m.statusCodes.errInvalidConfig.Code
		info.Info = fmt.Sprintf("Function: %s feed boundary: from_prior is only allowed if function is in paused state", app.Name)

		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	if m.superSup.GetAppState(app.Name) == common.AppStatePaused {
		switch filterFeedBoundary(app.Settings) {
		case common.DcpFromNow, common.DcpEverything:
			info.Code = m.statusCodes.errInvalidConfig.Code
			info.Info = fmt.Sprintf("Function: %s only from_prior feed boundary is allowed during resume", app.Name)
			logging.Errorf("%s %s", logPrefix, info.Info)
			return
		case common.DcpStreamBoundary(""):
			app.Settings["dcp_stream_boundary"] = "from_prior"
		default:
		}
	}

	srcMutationEnabled := m.isSrcMutationEnabled(&app.DeploymentConfig)
	if srcMutationEnabled && !m.compareEventingVersion(mhVersion) {
		info.Code = m.statusCodes.errClusterVersion.Code
		info.Info = fmt.Sprintf("All eventing nodes in the cluster must be on version %d.%d or higher for allowing mutations against source bucket",
			mhVersion.major, mhVersion.minor)
		logging.Warnf("%s Version compat check failed: %s", logPrefix, info.Info)
		return
	}

	if srcMutationEnabled {
		keySpace := &common.Keyspace{BucketName: app.DeploymentConfig.SourceBucket,
			ScopeName:      app.DeploymentConfig.SourceScope,
			CollectionName: app.DeploymentConfig.SourceCollection,
		}
		if enabled, err := util.IsSyncGatewayEnabled(logPrefix, keySpace, m.restPort); err == nil && enabled {
			info.Code = m.statusCodes.errSyncGatewayEnabled.Code
			info.Info = fmt.Sprintf("SyncGateway is enabled on: %s, deployement of source bucket mutating handler will cause Intra Bucket Recursion", app.DeploymentConfig.SourceBucket)
			return
		}
	}

	logging.Infof("%v Function UUID: %v for function name: %v stored in primary store", logPrefix, app.FunctionID, app.Name)

	appContent := m.encodeAppPayload(app)

	compressPayload := m.checkCompressHandler()
	payload, err := util.MaybeCompress(appContent, compressPayload)
	if err != nil {
		info.Code = m.statusCodes.errSaveAppPs.Code
		info.Info = fmt.Sprintf("Function: %s Error in compressing: %v", app.Name, err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}
	if len(payload) > util.MaxFunctionSize() {
		info.Code = m.statusCodes.errAppCodeSize.Code
		info.Info = fmt.Sprintf("Function: %s handler Code size is more than %d. Code Size: %d", app.Name, util.MaxFunctionSize(), len(payload))
		logging.Errorf("%s %s", logPrefix, info.Info)
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
		info.Code = m.statusCodes.errHandlerCompile.Code
		info.Info = compilationInfo
		return
	}

	appContent = m.encodeAppPayload(app)
	settingsPath := metakvAppSettingsPath + app.Name
	settings := app.Settings

	mData, mErr := json.MarshalIndent(&settings, "", " ")
	if mErr != nil {
		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("Function: %s failed to marshal settings, err: %v", app.Name, mErr)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	mkvErr := util.MetakvSet(settingsPath, mData, nil)
	if mkvErr != nil {
		info.Code = m.statusCodes.errSetSettingsPs.Code
		info.Info = fmt.Sprintf("Function: %s failed to store updated settings in metakv, err: %v", app.Name, mkvErr)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	//Delete stale entry
	err = util.DeleteStaleAppContent(metakvAppsPath, app.Name)
	if err != nil {
		info.Code = m.statusCodes.errSaveAppPs.Code
		info.Info = fmt.Sprintf("Function: %s failed to clean up stale entry, err: %v", app.Name, err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	err = util.WriteAppContent(metakvAppsPath, metakvChecksumPath, app.Name, appContent, compressPayload)
	if err != nil {
		info.Code = m.statusCodes.errSaveAppPs.Code
		logging.Errorf("%s Function: %s unable to save to primary store, err: %v", logPrefix, app.Name, err)
		return
	}

	wInfo, err := m.determineWarnings(app, compilationInfo)
	if err != nil {
		info.Code = m.statusCodes.errGetConfig.Code
		info.Info = fmt.Sprintf("Function: %s failed to determine warnings, err : %v", app.Name, err)
		return
	}

	info.Code = m.statusCodes.ok.Code
	info.Info = *wInfo
	return
}

func (m *ServiceMgr) determineWarnings(app *application, compilationInfo *common.CompileStatus) (*warningsInfo, error) {
	wInfo := &warningsInfo{}
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
	return wInfo, nil
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
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	w.Write(m.statusPayload)
}

func (m *ServiceMgr) getDcpEventsRemaining(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]
	if m.checkIfDeployed(appName) {
		eventsRemaining := m.superSup.GetDcpEventsRemainingToProcess(appName)
		resp := backlogStat{DcpBacklog: eventsRemaining}
		data, _ := json.MarshalIndent(&resp, "", " ")
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%v", string(data))
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "Function: %s not deployed", appName)
}

func (m *ServiceMgr) getAggPausingApps(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getAggPausingApps"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	appsPausing, err := util.GetAggPausingApps("/getPausingApps", m.eventingNodeAddrs)
	if appsPausing {
		w.Write([]byte(strconv.FormatBool(appsPausing)))
		return
	} else if !appsPausing && err != nil {
		logging.Errorf("%s Failed to grab pausing function list from all eventing nodes or some functions being paused."+
			"Node list: %v", logPrefix, m.eventingNodeAddrs)
		return
	}

	if err != nil {
		logging.Errorf("%s Failed to grab pausing function list from all eventing nodes."+
			"Node list: %v", logPrefix, m.eventingNodeAddrs)
		return
	}

	w.Write([]byte(strconv.FormatBool(appsPausing)))

}

func (m *ServiceMgr) getAggBootstrappingApps(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getAggBootstrappingApps"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	appsBootstrapping, err := util.GetAggBootstrappingApps("/getBootstrappingApps", m.eventingNodeAddrs)
	if appsBootstrapping {
		w.Write([]byte(strconv.FormatBool(appsBootstrapping)))
		return
	} else if !appsBootstrapping && err != nil {
		logging.Errorf("%s Failed to grab bootstrapping function list from all eventing nodes or some functions are undergoing bootstrap."+
			"Node list: %v", logPrefix, m.eventingNodeAddrs)
		return
	}

	if err != nil {
		logging.Errorf("%s Failed to grab bootstrapping function list from all eventing nodes or some functions."+
			"Node list: %v", logPrefix, m.eventingNodeAddrs)
		return
	}

	w.Write([]byte(strconv.FormatBool(appsBootstrapping)))
}

func (m *ServiceMgr) getPausingApps(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	pausingApps := m.superSup.PausingAppList()
	data, err := json.MarshalIndent(pausingApps, "", " ")
	if err != nil {
		fmt.Fprintf(w, "Failed to marshal function list which are being paused, err: %v", err)
		return
	}

	w.Write(data)
}

func (m *ServiceMgr) getBootstrappingApps(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	bootstrappingApps := m.superSup.BootstrapAppList()
	data, err := json.MarshalIndent(bootstrappingApps, "", " ")
	if err != nil {
		fmt.Fprintf(w, "Failed to marshal bootstrapping function list, err: %v", err)
		return
	}

	w.Write(data)
}

func (m *ServiceMgr) getEventingConsumerPids(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	if m.checkIfDeployed(appName) {
		workerPidMapping := m.superSup.GetEventingConsumerPids(appName)

		data, err := json.MarshalIndent(&workerPidMapping, "", " ")
		if err != nil {
			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
			fmt.Fprintf(w, "Failed to marshal consumer pids, err: %v", err)
			return
		}

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%v", string(data))
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "Function: %v not deployed", appName)
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
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		fmt.Fprintf(w, "Failed to read request body, err: %v", err)
		return
	}

	strippedEndpoint := util.StripScheme(string(data))
	username, password, err := cbauth.GetMemcachedServiceAuth(strippedEndpoint)
	if err != nil {
		logging.Errorf("%s Failed to get credentials for endpoint: %rs, err: %v", logPrefix, strippedEndpoint, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errRbacCreds.Code))
	} else {
		response := url.Values{}
		response.Add("username", username)
		response.Add("password", password)

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", response.Encode())
	}
}

func (m *ServiceMgr) getKVNodesAddresses(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getKVNodesAddresses"
	if !m.validateLocalAuth(w, r) {
		return
	}

	w.Header().Set("Content-Type", "application/json")

	nsServer := net.JoinHostPort(util.Localhost(), m.restPort)
	clusterInfo, err := util.FetchNewClusterInfoCache(nsServer)
	if err != nil {
		logging.Errorf("%s Failed to get cluster info cache, err : %v", logPrefix, err)
		return
	}

	kvNodes, err := clusterInfo.GetAddressOfActiveKVNodes()
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

func (m *ServiceMgr) clearEventStats(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::clearEventStats"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	logging.Infof("%s Got request to clear event stats from host: %rs", logPrefix, r.Host)
	m.superSup.ClearEventStats()
}

func (m *ServiceMgr) getConfig() (c common.Config, info *runtimeInfo) {
	logPrefix := "ServiceMgr::getConfig"

	info = &runtimeInfo{}
	data, err := util.MetakvGet(metakvConfigPath)
	if err != nil {
		info.Code = m.statusCodes.errGetConfig.Code
		info.Info = fmt.Sprintf("failed to get config, err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	if !bytes.Equal(data, nil) {
		err = json.Unmarshal(data, &c)
		if err != nil {
			info.Code = m.statusCodes.errUnmarshalPld.Code
			info.Info = fmt.Sprintf("failed to unmarshal payload from metakv, err: %v", err)
			logging.Errorf("%s %s", logPrefix, info.Info)
			return
		}
	}

	logging.Infof("%s Retrieving config from metakv: %+v", logPrefix, c)
	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) saveConfig(c common.Config) (info *runtimeInfo) {
	logPrefix := "ServiceMgr::saveConfig"

	info = &runtimeInfo{}
	storedConfig, info := m.getConfig()
	if info.Code != m.statusCodes.ok.Code {
		return
	}

	data, err := json.MarshalIndent(util.SuperImpose(c, storedConfig), "", " ")
	if err != nil {
		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("failed to marshal config, err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	logging.Infof("%s Saving config into metakv: %v", logPrefix, c)

	err = util.MetakvSet(metakvConfigPath, data, nil)
	if err != nil {
		logging.Errorf("%s Failed to write to metakv err: %v", logPrefix, err)
		info.Code = m.statusCodes.errMetakvWriteFailed.Code
		info.Info = fmt.Sprintf("Failed to write to metakv, err: %v", err)
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) configHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::configHandler"

	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	info := &runtimeInfo{}

	switch r.Method {
	case "GET":
		audit.Log(auditevent.FetchConfig, r, nil)

		c, info := m.getConfig()
		if info.Code != m.statusCodes.ok.Code {
			m.sendErrorInfo(w, info)
			return
		}

		response, err := json.MarshalIndent(c, "", " ")
		if err != nil {
			info.Code = m.statusCodes.errMarshalResp.Code
			info.Info = fmt.Sprintf("failed to marshal config, err : %v", err)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		fmt.Fprintf(w, "%s", string(response))

	case "POST":
		audit.Log(auditevent.SaveConfig, r, nil)

		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			info.Code = m.statusCodes.errReadReq.Code
			info.Info = fmt.Sprintf("failed to read request body, err: %v", err)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		var c common.Config
		err = json.Unmarshal(data, &c)
		if err != nil {
			info.Code = m.statusCodes.errUnmarshalPld.Code
			info.Info = fmt.Sprintf("failed to unmarshal config from metakv, err: %v", err)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		if info := m.validateConfig(c); info.Code != m.statusCodes.ok.Code {
			m.sendErrorInfo(w, info)
			return
		}

		if info = m.saveConfig(c); info.Code != m.statusCodes.ok.Code {
			m.sendErrorInfo(w, info)
			return
		}

		response := configResponse{false}
		data, err = json.MarshalIndent(response, "", " ")
		if err != nil {
			info.Code = m.statusCodes.errMarshalResp.Code
			info.Info = fmt.Sprintf("failed to marshal response, err: %v", err)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		fmt.Fprintf(w, "%s", string(data))

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
}

func (m *ServiceMgr) assignFunctionID(fnName string, app *application, info *runtimeInfo) error {
	logPrefix := "ServiceMgr::assignFunctionID"

	data, err := util.ReadAppContent(metakvAppsPath, metakvChecksumPath, fnName)
	if err != nil && data != nil {
		info.Code = m.statusCodes.errGetAppPs.Code
		info.Info = fmt.Sprintf("Function: %s failed to read definitions from metakv", fnName)

		logging.Errorf("%s %s, err: %v", logPrefix, info.Info, err)
		return fmt.Errorf("%s", info.Info)
	}

	if err == nil && data != nil {
		tApp := m.parseFunctionPayload(data, fnName)
		app.FunctionID = tApp.FunctionID
		logging.Infof("%s Function: %s assigned previous function ID: %d", logPrefix, app.Name, app.FunctionID)
	} else {
		var uErr error
		app.FunctionID, uErr = util.GenerateFunctionID()
		if uErr != nil {
			info.Code = m.statusCodes.errFunctionIDGen.Code
			info.Info = fmt.Sprintf("Function: %s FunctionID generation failed", fnName)

			logging.Errorf("%s %s", logPrefix, info.Info)
			return uErr
		}
		logging.Infof("%s Function: %s FunctionID: %d generated", logPrefix, app.Name, app.FunctionID)
	}

	return nil
}

func (m *ServiceMgr) assignFunctionInstanceID(functionName string, app *application, info *runtimeInfo) error {
	logPrefix := "ServiceMgr:assignFunctionInstanceID"

	if m.superSup.GetAppState(functionName) != common.AppStatePaused {
		fiid, err := util.GenerateFunctionInstanceID()
		if err != nil {
			info.Code = m.statusCodes.errFunctionInstanceIDGen.Code
			info.Info = fmt.Sprintf("FunctionInstanceID generation failed")

			logging.Errorf("%s %s", logPrefix, info.Info)
			return err
		}
		app.FunctionInstanceID = fiid
		logging.Infof("%s Function: %s FunctionInstanceID: %s generated", logPrefix, app.Name, app.FunctionInstanceID)
	} else {
		data, err := util.ReadAppContent(metakvAppsPath, metakvChecksumPath, functionName)
		if err != nil || data == nil {
			info.Code = m.statusCodes.errGetAppPs.Code
			info.Info = fmt.Sprintf("Function: %s failed to read definitions from metakv", functionName)

			logging.Errorf("%s %s, err: %v", logPrefix, info.Info, err)
			return fmt.Errorf("%s err: %v", info.Info, err)
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

	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	logging.Tracef("%s Function handler invoked.", logPrefix)
	functions := regexp.MustCompile("^/api/v1/functions/?$")
	functionsName := regexp.MustCompile("^/api/v1/functions/(.*[^/])/?$") // Match is agnostic of trailing '/'
	functionsNameSettings := regexp.MustCompile("^/api/v1/functions/(.*[^/])/settings/?$")
	functionsNameRetry := regexp.MustCompile("^/api/v1/functions/(.*[^/])/retry/?$")
	functionsDeploy := regexp.MustCompile("^/api/v1/functions/(.*[^/])/deploy/?$")
	functionsUndeploy := regexp.MustCompile("^/api/v1/functions/(.*[^/])/undeploy/?$")
	functionsPause := regexp.MustCompile("^/api/v1/functions/(.*[^/])/pause/?$")
	functionsResume := regexp.MustCompile("^/api/v1/functions/(.*[^/])/resume/?$")

	if match := functionsNameRetry.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		appName := match[1]
		info := &runtimeInfo{}

		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			info.Code = m.statusCodes.errReadReq.Code
			info.Info = fmt.Sprintf("failed to read request body, err : %v", err)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		var retryBody retry
		err = json.Unmarshal(data, &retryBody)
		if err != nil {
			info.Code = m.statusCodes.errMarshalResp.Code
			info.Info = fmt.Sprintf("failed to unmarshal retry, err: %v", err)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		if info = m.notifyRetryToAllProducers(appName, &retryBody); info.Code != m.statusCodes.ok.Code {
			m.sendErrorInfo(w, info)
			return
		}
	} else if match := functionsNameSettings.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		appName := match[1]
		info := &runtimeInfo{}

		switch r.Method {
		case "GET":
			audit.Log(auditevent.GetSettings, r, nil)
			settings, info := m.getSettings(appName)
			if info.Code != m.statusCodes.ok.Code {
				w.WriteHeader(http.StatusNotFound)
				m.sendErrorInfo(w, info)
				return
			}

			response, err := json.MarshalIndent(settings, "", " ")
			if err != nil {
				info.Code = m.statusCodes.errMarshalResp.Code
				info.Info = fmt.Sprintf("failed to marshal function, err : %v", err)
				logging.Errorf("%s %s", logPrefix, info.Info)
				m.sendErrorInfo(w, info)
				return
			}

			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
			fmt.Fprintf(w, "%s", string(response))

		case "POST":
			audit.Log(auditevent.SetSettings, r, appName)

			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				info.Code = m.statusCodes.errReadReq.Code
				info.Info = fmt.Sprintf("failed to read request body, err: %v", err)
				logging.Errorf("%s %s", logPrefix, info.Info)
				m.sendErrorInfo(w, info)
				return
			}

			var settings map[string]interface{}
			err = json.Unmarshal(data, &settings)
			if err != nil {
				info.Code = m.statusCodes.errMarshalResp.Code
				info.Info = fmt.Sprintf("failed to unmarshal setting supplied, err: %v", err)
				logging.Errorf("%s %s", logPrefix, info.Info)
				m.sendErrorInfo(w, info)
				return
			}

			if info = m.setSettings(appName, data); info.Code != m.statusCodes.ok.Code {
				m.sendErrorInfo(w, info)
				return
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	} else if match := functionsPause.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		info := &runtimeInfo{}
		if r.Method != "POST" {
			info.Code = m.statusCodes.errInvalidConfig.Code
			info.Info = fmt.Sprintf("Only POST call allowed to this endpoint")
			m.sendErrorInfo(w, info)
			return
		}
		appName := match[1]
		appState := m.superSup.GetAppState(appName)

		if appState == common.AppStatePaused {
			info.Code = m.statusCodes.errAppNotDeployed.Code
			info.Info = fmt.Sprintf("Invalid operation. Function: %v already in paused state.", appName)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		audit.Log(auditevent.SetSettings, r, appName)

		var settings = make(map[string]interface{})
		settings["deployment_status"] = true
		settings["processing_status"] = false
		settings["dcp_stream_boundary"] = "everything"

		data, err := json.MarshalIndent(settings, "", " ")
		if err != nil {
			info.Code = m.statusCodes.errMarshalResp.Code
			info.Info = fmt.Sprintf("failed to marshal function settings, err : %v", err)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		if info = m.setSettings(appName, data); info.Code != m.statusCodes.ok.Code {
			m.sendErrorInfo(w, info)
			return
		}

	} else if match := functionsResume.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		info := &runtimeInfo{}
		if r.Method != "POST" {
			info.Code = m.statusCodes.errInvalidConfig.Code
			info.Info = fmt.Sprintf("Only POST call allowed to this endpoint")
			m.sendErrorInfo(w, info)
			return
		}
		appName := match[1]
		appState := m.superSup.GetAppState(appName)

		if appState == common.AppStateEnabled {
			info.Code = m.statusCodes.errAppDeployed.Code
			info.Info = fmt.Sprintf("Invalid operation. Function: %v already in deployed state.", appName)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		if appState == common.AppStateUndeployed {
			info.Code = m.statusCodes.errAppNotDeployed.Code
			info.Info = fmt.Sprintf("Invalid operation. Function: %v already in undeployed state.", appName)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		audit.Log(auditevent.SetSettings, r, appName)

		var settings = make(map[string]interface{})
		settings["deployment_status"] = true
		settings["processing_status"] = true
		settings["dcp_stream_boundary"] = "from_prior"

		data, err := json.MarshalIndent(settings, "", " ")
		if err != nil {
			info.Code = m.statusCodes.errMarshalResp.Code
			info.Info = fmt.Sprintf("failed to marshal function settings, err : %v", err)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		if info = m.setSettings(appName, data); info.Code != m.statusCodes.ok.Code {
			m.sendErrorInfo(w, info)
			return
		}

	} else if match := functionsDeploy.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		info := &runtimeInfo{}
		if r.Method != "POST" {
			info.Code = m.statusCodes.errInvalidConfig.Code
			info.Info = fmt.Sprintf("Only POST call allowed to this endpoint")
			m.sendErrorInfo(w, info)
			return
		}
		appName := match[1]
		appState := m.superSup.GetAppState(appName)

		if appState == common.AppStateEnabled {
			info.Code = m.statusCodes.errAppDeployed.Code
			info.Info = fmt.Sprintf("Invalid operation. Function: %v already in deployed state.", appName)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		audit.Log(auditevent.SetSettings, r, appName)

		if appState == common.AppStatePaused {
			info.Code = m.statusCodes.errAppNotUndeployed.Code
			info.Info = fmt.Sprintf("Function: %v is in paused state, Please use /resume API to deploy the function.", appName)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			info.Code = m.statusCodes.errReadReq.Code
			info.Info = fmt.Sprintf("failed to read request body, err: %v", err)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		settings := make(map[string]interface{})
		if len(data) != 0 {
			err = json.Unmarshal(data, &settings)
			if err != nil {
				info.Code = m.statusCodes.errMarshalResp.Code
				info.Info = fmt.Sprintf("%v failed to unmarshal setting supplied, err: %v", len(data), err)
				logging.Errorf("%s %s", logPrefix, info.Info)
				m.sendErrorInfo(w, info)
				return
			}
			if settings == nil {
				info.Code = m.statusCodes.errMarshalResp.Code
				info.Info = fmt.Sprintf("%v failed to unmarshal setting supplied, data sent in the request body is invalid.", len(data))
				logging.Errorf("%s %s", logPrefix, info.Info)
				m.sendErrorInfo(w, info)
				return
			}
		}

		settings["deployment_status"] = true
		settings["processing_status"] = true

		data, err = json.MarshalIndent(settings, "", " ")
		if err != nil {
			info.Code = m.statusCodes.errMarshalResp.Code
			info.Info = fmt.Sprintf("failed to marshal function settings, err : %v", err)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		if info = m.setSettings(appName, data); info.Code != m.statusCodes.ok.Code {
			m.sendErrorInfo(w, info)
			return
		}

	} else if match := functionsUndeploy.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		info := &runtimeInfo{}
		if r.Method != "POST" {
			info.Code = m.statusCodes.errInvalidConfig.Code
			info.Info = fmt.Sprintf("Only POST call allowed to this endpoint")
			m.sendErrorInfo(w, info)
			return
		}
		appName := match[1]
		appState := m.superSup.GetAppState(appName)

		if appState == common.AppStateUndeployed {
			info.Code = m.statusCodes.errAppNotDeployed.Code
			info.Info = fmt.Sprintf("Invalid operation. Function: %v already in undeployed state.", appName)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		audit.Log(auditevent.SetSettings, r, appName)

		var settings = make(map[string]interface{})
		settings["deployment_status"] = false
		settings["processing_status"] = false

		data, err := json.MarshalIndent(settings, "", " ")
		if err != nil {
			info.Code = m.statusCodes.errMarshalResp.Code
			info.Info = fmt.Sprintf("failed to marshal function settings, err : %v", err)
			logging.Errorf("%s %s", logPrefix, info.Info)
			m.sendErrorInfo(w, info)
			return
		}

		if info = m.setSettings(appName, data); info.Code != m.statusCodes.ok.Code {
			m.sendErrorInfo(w, info)
			return
		}

	} else if match := functionsName.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		appName := match[1]
		switch r.Method {
		case "GET":
			audit.Log(auditevent.FetchDrafts, r, appName)

			app, info := m.getTempStore(appName)
			if info.Code != m.statusCodes.ok.Code {
				m.sendErrorInfo(w, info)
				return
			}

			response, err := json.MarshalIndent(app, "", " ")
			if err != nil {
				info.Code = m.statusCodes.errMarshalResp.Code
				info.Info = fmt.Sprintf("failed to marshal function, err : %v", err)
				logging.Errorf("%s %s", logPrefix, info.Info)
				m.sendErrorInfo(w, info)
				return
			}

			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
			fmt.Fprintf(w, "%s", string(response))

		case "POST":
			audit.Log(auditevent.CreateFunction, r, appName)

			app, info := m.unmarshalApp(r)
			if info.Code != m.statusCodes.ok.Code {
				m.sendErrorInfo(w, info)
				return
			}

			m.addDefaultVersionIfMissing(&app)
			m.addDefaultDeploymentConfig(&app)
			m.addDefaultTimerPartitionsIfMissing(&app)

			var isMixedMode bool
			if isMixedMode, info = m.isMixedModeCluster(); info.Code != m.statusCodes.ok.Code {
				m.sendErrorInfo(w, info)
				return
			}

			if isMixedMode {
				info.Code = m.statusCodes.errMixedMode.Code
				info.Info = "Life-cycle operations except delete and undeploy are not allowed in a mixed mode cluster"
				m.sendErrorInfo(w, info)
				return
			}

			if !m.checkAppExists(appName) {
				if app.Settings["deployment_status"] != app.Settings["processing_status"] {
					app.Settings["deployment_status"] = false
					app.Settings["processing_status"] = false
				}
			}

			if info = m.validateApplication(&app); info.Code != m.statusCodes.ok.Code {
				m.sendErrorInfo(w, info)
				return
			}

			// Reject the request if there is a mismatch of app name in URL and body
			if app.Name != appName {
				info.Code = m.statusCodes.errAppNameMismatch.Code
				info.Info = fmt.Sprintf("function name in the URL (%s) and body (%s) must be same", appName, app.Name)
				logging.Errorf("%s %s", logPrefix, info.Info)
				m.sendErrorInfo(w, info)
				return
			}

			err := m.assignFunctionID(appName, &app, info)
			if err != nil {
				m.sendErrorInfo(w, info)
				return
			}

			err = m.assignFunctionInstanceID(appName, &app, info)
			if err != nil {
				m.sendErrorInfo(w, info)
				return
			}

			if _, ok := app.Settings["language_compatibility"]; !ok {
				app.Settings["language_compatibility"] = common.LanguageCompatibility[len(common.LanguageCompatibility)-1]
			}

			runtimeInfo := m.savePrimaryStore(&app)
			if runtimeInfo.Code == m.statusCodes.ok.Code {
				audit.Log(auditevent.SaveDraft, r, appName)
				// Save to temp store only if saving to primary store succeeds
				if tempInfo := m.saveTempStore(app); tempInfo.Code != m.statusCodes.ok.Code {
					m.sendErrorInfo(w, tempInfo)
					return
				}
			}
			m.sendRuntimeInfo(w, runtimeInfo)

		case "DELETE":
			audit.Log(auditevent.DeleteFunction, r, appName)
			info := m.deletePrimaryStore(appName)
			// Delete the application from temp store only if app does not exist in primary store
			// or if the deletion succeeds on primary store
			if info.Code == m.statusCodes.errAppNotDeployed.Code || info.Code == m.statusCodes.ok.Code {
				audit.Log(auditevent.DeleteDrafts, r, appName)
				info = m.deleteTempStore(appName)
			}
			m.sendRuntimeInfo(w, info)

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

	} else if match := functions.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		switch r.Method {
		case "GET":
			m.getTempStoreHandler(w, r)

		case "POST":
			appList, info := m.unmarshalAppList(w, r)
			if info.Code != m.statusCodes.ok.Code {
				m.sendErrorInfo(w, info)
				return
			}
			var isMixedMode bool
			if isMixedMode, info = m.isMixedModeCluster(); info.Code != m.statusCodes.ok.Code {
				m.sendErrorInfo(w, info)
				return
			}

			if isMixedMode {
				info.Code = m.statusCodes.errMixedMode.Code
				info.Info = "Life-cycle operations except delete and undeploy are not allowed in a mixed mode cluster"
				m.sendErrorInfo(w, info)
				return
			}
			infoList := m.createApplications(r, appList, false)
			m.sendRuntimeInfoList(w, infoList)

		case "DELETE":
			infoList := []*runtimeInfo{}
			for _, app := range m.getTempStoreAll() {
				audit.Log(auditevent.DeleteFunction, r, app.Name)
				info := m.deletePrimaryStore(app.Name)
				// Delete the application from temp store only if app does not exist in primary store
				// or if the deletion succeeds on primary store
				if info.Code == m.statusCodes.errAppNotDeployed.Code || info.Code == m.statusCodes.ok.Code {
					audit.Log(auditevent.DeleteDrafts, r, app.Name)
					info = m.deleteTempStore(app.Name)
				}
				infoList = append(infoList, info)
			}
			m.sendRuntimeInfoList(w, infoList)

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	}
}

func (m *ServiceMgr) addDefaultVersionIfMissing(app *application) {
	if app.EventingVersion == "" {
		app.EventingVersion = util.EventingVer()
	}
}

func (m *ServiceMgr) addDefaultDeploymentConfig(app *application) {
	if app.DeploymentConfig.SourceScope == "" {
		app.DeploymentConfig.SourceScope = "_default"
	}
	if app.DeploymentConfig.SourceCollection == "" {
		app.DeploymentConfig.SourceCollection = "_default"
	}
	if app.DeploymentConfig.MetadataScope == "" {
		app.DeploymentConfig.MetadataScope = "_default"
	}
	if app.DeploymentConfig.MetadataCollection == "" {
		app.DeploymentConfig.MetadataCollection = "_default"
	}

	for i := range app.DeploymentConfig.Buckets {
		if app.DeploymentConfig.Buckets[i].ScopeName == "" {
			app.DeploymentConfig.Buckets[i].ScopeName = "_default"
		}

		if app.DeploymentConfig.Buckets[i].CollectionName == "" {
			app.DeploymentConfig.Buckets[i].CollectionName = "_default"
		}
	}
}

func (m *ServiceMgr) addDefaultTimerPartitionsIfMissing(app *application) {
	if _, ok := app.Settings["num_timer_partitions"]; !ok {
		app.Settings["num_timer_partitions"] = float64(defaultNumTimerPartitions)
	}
}

func (m *ServiceMgr) notifyRetryToAllProducers(appName string, r *retry) (info *runtimeInfo) {
	logPrefix := "ServiceMgr::notifyRetryToAllProducers"

	info = &runtimeInfo{}

	retryPath := metakvAppsRetryPath + appName
	retryCount := []byte(strconv.Itoa(int(r.Count)))

	err := util.MetakvSet(retryPath, retryCount, nil)
	if err != nil {
		info.Code = m.statusCodes.errAppRetry.Code
		info.Info = fmt.Sprintf("unable to set metakv path for retry, err : %v", err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	audit.Log(auditevent.ListDeployed, r, nil)
	response, info := m.statusHandlerImpl()
	if info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}

	data, err := json.MarshalIndent(response, "", " ")
	if err != nil {
		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("Unable to marshal response, err: %v", err)
		m.sendErrorInfo(w, info)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s", string(data))
}

func (m *ServiceMgr) statusHandlerImpl() (response appStatusResponse, info *runtimeInfo) {
	appDeployedNodesCounter, appBootstrappingNodesCounter, appPausingNodesCounter, numEventingNodes, info := m.getAppList()
	if info.Code != m.statusCodes.ok.Code {
		return
	}

	response.NumEventingNodes = numEventingNodes
	for _, app := range m.getTempStoreAll() {
		deploymentStatus, processingStatus, err := m.getStatuses(app.Name)
		if err != nil {
			info.Code = m.statusCodes.errInvalidConfig.Code
			return
		}

		status := appStatus{
			Name:             app.Name,
			DeploymentStatus: deploymentStatus,
			ProcessingStatus: processingStatus,
		}
		if num, exists := appDeployedNodesCounter[app.Name]; exists {
			status.NumDeployedNodes = num
		}
		if num, exists := appBootstrappingNodesCounter[app.Name]; exists {
			status.NumBootstrappingNodes = num
		}

		mhVersion := eventingVerMap["mad-hatter"]
		if m.compareEventingVersion(mhVersion) {
			bootstrapStatus, err := util.GetAggBootstrapAppStatus(net.JoinHostPort(util.Localhost(), m.adminHTTPPort), status.Name)
			if err != nil {
				info.Code = m.statusCodes.errInvalidConfig.Code
				return
			}
			status.CompositeStatus = m.determineStatus(status, appPausingNodesCounter, numEventingNodes, bootstrapStatus)
		} else {
			status.CompositeStatus = m.determineStatus(status, appPausingNodesCounter, numEventingNodes, false)
		}
		response.Apps = append(response.Apps, status)
	}
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
	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	if r.Method == "GET" {
		// Check whether type=full is present in query
		fullStats := false
		if typeParam := r.URL.Query().Get("type"); typeParam != "" {
			fullStats = typeParam == "full"
		}

		statsList := m.populateStats(fullStats)

		response, err := json.MarshalIndent(statsList, "", " ")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `{"error":"Failed to marshal response for stats, err: %v"}`, err)
			return
		}

		fmt.Fprintf(w, "%s", string(response))
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

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

func (m *ServiceMgr) populateStats(fullStats bool) []stats {
	statsList := make([]stats, 0)
	for _, app := range m.getTempStoreAll() {
		if m.checkIfDeployed(app.Name) {
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
func (m *ServiceMgr) cleanupEventing(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	audit.Log(auditevent.CleanupEventing, r, nil)

	util.Retry(util.NewFixedBackoff(time.Second), nil, cleanupEventingMetaKvPath, metakvChecksumPath)
	util.Retry(util.NewFixedBackoff(time.Second), nil, cleanupEventingMetaKvPath, metakvTempChecksumPath)
	util.Retry(util.NewFixedBackoff(time.Second), nil, cleanupEventingMetaKvPath, metakvAppsPath)
	util.Retry(util.NewFixedBackoff(time.Second), nil, cleanupEventingMetaKvPath, metakvTempAppsPath)
	util.Retry(util.NewFixedBackoff(time.Second), nil, cleanupEventingMetaKvPath, metakvAppSettingsPath)
}

func (m *ServiceMgr) exportHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::exportHandler"

	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	audit.Log(auditevent.ExportFunctions, r, nil)

	exportedFns := make([]string, 0)
	apps := m.getTempStoreAll()
	for _, app := range apps {
		for i := range app.DeploymentConfig.Curl {
			app.DeploymentConfig.Curl[i].Username = ""
			app.DeploymentConfig.Curl[i].Password = ""
			app.DeploymentConfig.Curl[i].BearerKey = ""
		}
		app.Settings["deployment_status"] = false
		app.Settings["processing_status"] = false
		exportedFns = append(exportedFns, app.Name)
	}

	logging.Infof("%s Exported function list: %+v", logPrefix, exportedFns)

	data, err := json.MarshalIndent(apps, "", " ")
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, `{"error":"Failed to marshal response, err: %v"}`, err)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s\n", data)
}

func (m *ServiceMgr) importHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::importHandler"

	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	audit.Log(auditevent.ImportFunctions, r, nil)

	appList, info := m.unmarshalAppList(w, r)
	if info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}
	var isMixedMode bool
	if isMixedMode, info = m.isMixedModeCluster(); info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}

	if isMixedMode {
		info.Code = m.statusCodes.errMixedMode.Code
		info.Info = "Life-cycle operations except delete and undeploy are not allowed in a mixed mode cluster"
		m.sendErrorInfo(w, info)
		return
	}

	infoList := m.createApplications(r, appList, true)

	importedFns := make([]string, 0)
	for _, app := range *appList {
		importedFns = append(importedFns, app.Name)
	}

	logging.Infof("%s Imported functions: %+v", logPrefix, importedFns)
	m.sendRuntimeInfoList(w, infoList)
}

func (m *ServiceMgr) createApplications(r *http.Request, appList *[]application, isImport bool) (infoList []*runtimeInfo) {
	logPrefix := "ServiceMgr::createApplications"

	infoList = []*runtimeInfo{}
	var err error
	for _, app := range *appList {
		audit.Log(auditevent.CreateFunction, r, app.Name)

		if isImport {
			app.Settings["deployment_status"] = false
			app.Settings["processing_status"] = false
		} else {
			m.addDefaultVersionIfMissing(&app)
		}
		m.addDefaultTimerPartitionsIfMissing(&app)

		m.addDefaultDeploymentConfig(&app)
		if infoVal := m.validateApplication(&app); infoVal.Code != m.statusCodes.ok.Code {
			logging.Warnf("%s Validating %ru failed: %v", logPrefix, app, infoVal)
			infoList = append(infoList, infoVal)
			continue
		}

		info := &runtimeInfo{}
		err = m.assignFunctionID(app.Name, &app, info)
		if err != nil {
			infoList = append(infoList, info)
			continue
		}

		info = &runtimeInfo{}
		err = m.assignFunctionInstanceID(app.Name, &app, info)
		if err != nil {
			infoList = append(infoList, info)
			continue
		}

		infoPri := m.savePrimaryStore(&app)
		if infoPri.Code != m.statusCodes.ok.Code {
			logging.Errorf("%s Function: %s saving %ru to primary store failed: %v", logPrefix, app.Name, infoPri)
			infoList = append(infoList, infoPri)
			continue
		}

		// Save to temp store only if saving to primary store succeeds
		audit.Log(auditevent.SaveDraft, r, app.Name)
		infoTmp := m.saveTempStore(app)
		if infoTmp.Code != m.statusCodes.ok.Code {
			logging.Errorf("%s Function: %s saving to temporary store failed: %v", logPrefix, app.Name, infoTmp)
			infoList = append(infoList, infoTmp)
			continue
		}

		// If everything succeeded, use infoPri as that has warnings, if any
		infoList = append(infoList, infoPri)
	}

	return
}

func (m *ServiceMgr) getCPUCount(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%v\n", util.CPUCount(false))
}

func (m *ServiceMgr) getWorkerCount(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		w.WriteHeader(http.StatusUnauthorized)
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	count := 0

	apps := m.getTempStoreAll()
	for _, app := range apps {
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

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%v\n", count)
}

func (m *ServiceMgr) isMixedModeCluster() (bool, *runtimeInfo) {
	info := &runtimeInfo{}

	nsServerEndpoint := net.JoinHostPort(util.Localhost(), m.restPort)
	cic, err := util.FetchClusterInfoClient(nsServerEndpoint)
	if err != nil {
		info.Code = m.statusCodes.errConnectNsServer.Code
		info.Info = fmt.Sprintf("Failed to get cluster info cache, err: %v", err)
		return false, info
	}
	clusterInfo := cic.GetClusterInfoCache()
	clusterInfo.RLock()
	defer clusterInfo.RUnlock()

	info.Code = m.statusCodes.ok.Code
	nodes := clusterInfo.GetActiveEventingNodes()
	if len(nodes) == 0 {
		return false, info
	}

	first := nodes[0]
	for _, node := range nodes {
		if first.Version != node.Version {
			return true, info
		}
	}

	return false, info
}

type version struct {
	major int
	minor int
}

var verMap = map[string]version{
	"vulcan":     {5, 5},
	"alice":      {6, 0},
	"mad-hatter": {6, 5},
}

func (r version) satisfies(need version) bool {
	return r.major > need.major ||
		r.major == need.major && r.minor >= need.minor
}

func (r version) String() string {
	return fmt.Sprintf("%v.%v", r.major, r.minor)
}

func (m *ServiceMgr) checkVersionCompat(required string, info *runtimeInfo) {
	logPrefix := "ServiceMgr::checkVersionCompat"

	nsServerEndpoint := net.JoinHostPort(util.Localhost(), m.restPort)
	cic, err := util.FetchClusterInfoClient(nsServerEndpoint)
	if err != nil {
		info.Code = m.statusCodes.errConnectNsServer.Code
		info.Info = fmt.Sprintf("Failed to get cluster info cache, err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}
	clusterInfo := cic.GetClusterInfoCache()
	clusterInfo.RLock()
	defer clusterInfo.RUnlock()

	var need, have version
	have.major, have.minor = clusterInfo.GetClusterVersion()
	need, ok := verMap[required]

	if !ok || !have.satisfies(need) {
		info.Code = m.statusCodes.errClusterVersion.Code
		info.Info = fmt.Sprintf("Function requires %v but cluster is at %v", need, have)
		logging.Warnf("%s Version compat check failed: %s", logPrefix, info.Info)
		return
	}

	logging.Infof("%s Function need %v satisfied by cluster %v", logPrefix, need, have)
	info.Code = m.statusCodes.ok.Code
}

func (m *ServiceMgr) triggerGC(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::triggerGC"

	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	logging.Infof("%s Triggering GC", logPrefix)
	runtime.GC()
	logging.Infof("%s Finished GC run", logPrefix)
}

func (m *ServiceMgr) freeOSMemory(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::freeOSMemory"

	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	logging.Infof("%s Freeing up memory to OS", logPrefix)
	debug.FreeOSMemory()
	logging.Infof("%s Freed up memory to OS", logPrefix)
}

//expvar handler
func (m *ServiceMgr) expvarHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

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
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}
	pprof.Index(w, r)
}

//pprof cmdline handler
func (m *ServiceMgr) cmdlineHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}
	pprof.Cmdline(w, r)
}

//pprof profile handler
func (m *ServiceMgr) profileHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}
	pprof.Profile(w, r)
}

//pprof symbol handler
func (m *ServiceMgr) symbolHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}
	pprof.Symbol(w, r)
}

//pprof trace handler
func (m *ServiceMgr) traceHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}
	pprof.Trace(w, r)
}

func (m *ServiceMgr) listFunctions(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::listFunctions"
	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	fnlist, info := m.getFunctionList(q)
	if info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}

	response, err := json.MarshalIndent(fnlist, "", " ")
	if err != nil {
		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("failed to marshal function list, err : %v", err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		m.sendErrorInfo(w, info)
		return
	}
	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s", string(response))
}

func (m *ServiceMgr) triggerInternalRebalance(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::triggerInternalRebalance"

	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	info := &runtimeInfo{}
	info.Code = m.statusCodes.ok.Code

	err := m.checkTopologyChangeReadiness(service.TopologyChangeTypeRebalance)
	if err == nil {
		path := "rebalance_request_from_rest"
		value := []byte(startRebalance)
		logging.Errorf("%s triggering rebalance processing from rest path: %v, value:%v", logPrefix, path, value)
		m.superSup.TopologyChangeNotifCallback(path, value, m.state.rev)
	} else {
		info.Code = m.statusCodes.errRequestedOpFailed.Code
		info.Info = fmt.Sprintf("%v", err)
		m.sendErrorInfo(w, info)
		return
	}
}

func (m *ServiceMgr) prometheusLow(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionStats) {
		cbauth.SendForbidden(w, EventingPermissionStats)
		return
	}
	//TODO: avg script execution time, avg timer scan time
	out := make([]byte, 0)
	out = append(out, []byte(fmt.Sprintf("%vworker_restart_count %v\n", METRICS_PREFIX, m.superSup.WorkerRespawnedCount()))...)

	w.WriteHeader(200)
	w.Write([]byte(out))
}

func (m *ServiceMgr) prometheusHigh(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionStats) {
		cbauth.SendForbidden(w, EventingPermissionStats)
		return
	}

	list := m.highCardStats()
	w.WriteHeader(200)
	w.Write(list)
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
			stats = populate(fmtStr, appName, "timer_callback_failure", stats, executionStats)
			// TODO: change it to timer_callback_success
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
