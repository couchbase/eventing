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
	"github.com/couchbase/eventing/audit"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/consumer"
	"github.com/couchbase/eventing/gen/auditevent"
	"github.com/couchbase/eventing/gen/flatbuf/cfg"
	"github.com/couchbase/eventing/logging"
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

	deployedApps := m.superSup.GetDeployedApps()
	if _, ok := deployedApps[appName]; ok {
		info.Code = m.statusCodes.errAppDelete.Code
		info.Info = fmt.Sprintf("Function: %s is currently undergoing undeploy. Only undeployed function can be deleted", appName)
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
	util.Retry(util.NewFixedBackoff(time.Second), nil,
		metakvSetCallback, common.MetakvDebuggerPath+appName, []byte(token))

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

	if !m.checkIfDeployed(appName) {
		info.Code = m.statusCodes.errAppNotDeployed.Code
		info.Info = fmt.Sprintf("Function: %s not deployed", appName)
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

		data, err := json.Marshal(&stats)
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

func (m *ServiceMgr) getAppList() (map[string]int, map[string]int, int, *runtimeInfo) {
	logPrefix := "ServiceMgr::getAppList"
	info := &runtimeInfo{}

	nodeAddrs, err := m.getActiveNodeAddrs()
	if err != nil {
		logging.Warnf("%s failed to fetch active Eventing nodes, err: %v", logPrefix, err)

		info.Code = m.statusCodes.errActiveEventingNodes.Code
		info.Info = fmt.Sprintf("Unable to fetch active Eventing nodes, err: %v", err)
		return nil, nil, 0, info
	}

	numEventingNodes := len(nodeAddrs)
	if numEventingNodes == 0 {
		info.Code = m.statusCodes.errNoEventingNodes.Code
		return nil, nil, 0, info
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

	info.Code = m.statusCodes.ok.Code
	return appDeployedNodesCounter, appBootstrappingNodesCounter, numEventingNodes, info
}

// Returns list of apps that are deployed i.e. finished dcp/timer/debugger related bootstrap
func (m *ServiceMgr) getDeployedApps(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getDeployedApps"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	audit.Log(auditevent.ListDeployed, r, nil)

	appDeployedNodesCounter, _, numEventingNodes, info := m.getAppList()
	if info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}

	deployedApps := make(map[string]string)
	for app, numNodesDeployed := range appDeployedNodesCounter {
		if numNodesDeployed == numEventingNodes {
			deployedApps[app] = ""
		}
	}

	data, err := json.Marshal(deployedApps)
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

	appDeployedNodesCounter, _, numEventingNodes, info := m.getAppList()
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

	data, err := json.Marshal(runningApps)
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

	buf, err := json.Marshal(deployedApps)
	if err != nil {
		logging.Errorf("%s failed to marshal list of deployed apps, err: %v", logPrefix, err)
		fmt.Fprintf(w, "")
		return
	}

	fmt.Fprintf(w, "%s", string(buf))
}

func (m *ServiceMgr) getNamedParamsHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateLocalAuth(w, r) {
		return
	}

	w.Header().Set("Content-Type", "application/x-www-form-urlencoded")

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		return
	}

	query := string(data)
	info := util.GetNamedParams(query)
	response := url.Values{}

	info.PInfo.FlattenParseInfo(&response)
	response.Add("named_params_size", strconv.Itoa(len(info.NamedParams)))

	for i, namedParam := range info.NamedParams {
		response.Add(strconv.Itoa(i), namedParam)
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s", response.Encode())
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
		data, err := json.Marshal(statsList)
		if err != nil {
			logging.Errorf("%s failed to unmarshal stats, err: %v", logPrefix, err)
		} else {
			logging.Tracef("%s no more vbucket remaining to shuffle. Stats dump: %v", logPrefix, string(data))
		}

		m.statsWritten = true
	}

	buf, err := json.Marshal(progress)
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

	buf, err := json.Marshal(pStats)
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
		logging.Errorf("%s failed to get progress from some/all eventing nodes: %rs err: %rs",
			logPrefix, m.eventingNodeAddrs, errMap)
		return
	}

	aggProgress.NodeLevelStats = progressMap

	buf, err := json.Marshal(aggProgress)
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

func (m *ServiceMgr) getLatencyStats(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getLatencyStats"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	if m.checkIfDeployed(appName) {
		lStats := m.superSup.GetLatencyStats(appName)

		data, err := json.Marshal(lStats)
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

		data, err := json.Marshal(eStats)
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

		data, err := json.Marshal(fStats)
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

		data, err := json.Marshal(seqNoProcessed)
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

	logging.Infof("%s Function: %s settings params: %+v", logPrefix, appName, settings)

	_, procStatExists := settings["processing_status"]
	_, depStatExists := settings["deployment_status"]

	if procStatExists || depStatExists {
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

	for setting := range settings {
		app.Settings[setting] = settings[setting]
	}

	processingStatus, pOk := app.Settings["processing_status"].(bool)
	deploymentStatus, dOk := app.Settings["deployment_status"].(bool)

	logging.Infof("%s Function: %s deployment status: %t processing status: %t",
		logPrefix, appName, deploymentStatus, processingStatus)

	deployedApps := m.superSup.GetDeployedApps()
	if pOk && dOk {
		mhVersion := eventingVerMap["mad-hatter"]

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

		if deploymentStatus && processingStatus && m.superSup.GetAppState(appName) == common.AppStatePaused {
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
		}
	} else {
		info.Code = m.statusCodes.errStatusesNotFound.Code
		info.Info = fmt.Sprintf("Function: %s missing processing or deployment statuses or both", appName)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	data, err = json.Marshal(app.Settings)
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
	app.ID = int(config.Id())
	app.FunctionID = uint32(config.FunctionID())
	app.FunctionInstanceID = string(config.FunctionInstanceID())

	d := new(cfg.DepCfg)
	depcfg := new(depCfg)
	dcfg := config.DepCfg(d)

	depcfg.MetadataBucket = string(dcfg.MetadataBucket())
	depcfg.SourceBucket = string(dcfg.SourceBucket())

	var buckets []bucket
	b := new(cfg.Bucket)
	for i := 0; i < dcfg.BucketsLength(); i++ {

		if dcfg.Buckets(b, i) {
			newBucket := bucket{
				Alias:      string(b.Alias()),
				BucketName: string(b.BucketName()),
				Access:     string(b.Access()),
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

	data, err := json.Marshal(respData)
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

	data, err := json.Marshal(applications)
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

func (m *ServiceMgr) getTempStore(appName string) (app application, info *runtimeInfo) {
	logPrefix := "ServiceMgr::getTempStore"

	info = &runtimeInfo{}
	logging.Infof("%s Function: %s fetching function draft definitions", logPrefix, appName)

	for _, name := range util.ListChildren(metakvTempAppsPath) {
		data, err := util.ReadAppContent(metakvTempAppsPath, metakvTempChecksumPath, name)
		if err == nil && data != nil {
			uErr := json.Unmarshal(data, &app)
			if uErr != nil {
				logging.Errorf("%s Function: %s failed to unmarshal data from metakv, err: %v", logPrefix, appName, uErr)
				continue
			}

			if app.Name == appName {
				info.Code = m.statusCodes.ok.Code

				// Hide some internal settings from being exported
				app.UsingTimer = false
				delete(app.Settings, "handler_uuid")
				delete(app.Settings, "using_timers")
				return
			}
		}
	}

	info.Code = m.statusCodes.errAppNotFoundTs.Code
	info.Info = fmt.Sprintf("Function: %s not found", appName)
	logging.Infof("%s %s", logPrefix, info.Info)
	return
}

func (m *ServiceMgr) getTempStoreAll() []application {
	logPrefix := "ServiceMgr::getTempStoreAll"

	m.fnMu.RLock()
	defer m.fnMu.RUnlock()

	applications := make([]application, len(m.fnsInTempStore))
	i := -1

	for fnName := range m.fnsInTempStore {
		data, err := util.ReadAppContent(metakvTempAppsPath, metakvTempChecksumPath, fnName)
		if err == nil && data != nil {
			var app application
			i++
			uErr := json.Unmarshal(data, &app)
			if uErr != nil {
				logging.Errorf("%s Function: %s failed to unmarshal data from metakv, err: %v data: %v",
					logPrefix, fnName, uErr, string(data))
				continue
			}

			applications[i] = app
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

	if info := m.validateApplication(&app); info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}

	info := m.saveTempStore(app)
	m.sendErrorInfo(w, info)
}

// Saves application to temp store
func (m *ServiceMgr) saveTempStore(app application) (info *runtimeInfo) {
	logPrefix := "ServiceMgr::saveTempStore"
	info = &runtimeInfo{}
	appName := app.Name

	data, err := json.Marshal(app)
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

	err = util.WriteAppContent(metakvTempAppsPath, metakvTempChecksumPath, appName, data)
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
		logging.Errorf("%s Failed to grab correct rebalance status from some/all Eventing nodes, err: %v", logPrefix, err)
		info.Code = m.statusCodes.errGetRebStatus.Code
		info.Info = "Failed to get rebalance status from eventing nodes"
		return
	}

	logging.Infof("%s Rebalance ongoing across some/all Eventing nodes: %v", logPrefix, rebStatus)

	if rebStatus {
		logging.Warnf("%s Rebalance ongoing on some/all Eventing nodes", logPrefix)
		info.Code = m.statusCodes.errRebOngoing.Code
		info.Info = "Rebalance ongoing on some/all Eventing nodes, creating new functions, deployment or undeployment of existing functions is not allowed"
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
		cookiesEncoded := builder.CreateString(app.DeploymentConfig.Curl[i].Cookies)

		cfg.CurlStart(builder)
		cfg.CurlAddAuthType(builder, authTypeEncoded)
		cfg.CurlAddHostname(builder, hostnameEncoded)
		cfg.CurlAddValue(builder, valueEncoded)
		cfg.CurlAddPassword(builder, passwordEncoded)
		cfg.CurlAddUsername(builder, usernameEncoded)
		cfg.CurlAddBearerKey(builder, bearerKeyEncoded)
		cfg.CurlAddCookies(builder, cookiesEncoded)
		curlBindingsEnd := cfg.CurlEnd(builder)

		curlBindings = append(curlBindings, curlBindingsEnd)
	}

	cfg.DepCfgStartCurlVector(builder, len(curlBindings))
	for i := 0; i < len(curlBindings); i++ {
		builder.PrependUOffsetT(curlBindings[i])
	}
	curlBindingsVector := builder.EndVector(len(curlBindings))

	var bNames []flatbuffers.UOffsetT
	for i := 0; i < len(app.DeploymentConfig.Buckets); i++ {
		alias := builder.CreateString(app.DeploymentConfig.Buckets[i].Alias)
		bName := builder.CreateString(app.DeploymentConfig.Buckets[i].BucketName)
		bAccess := builder.CreateString(app.DeploymentConfig.Buckets[i].Access)

		cfg.BucketStart(builder)
		cfg.BucketAddAlias(builder, alias)
		cfg.BucketAddBucketName(builder, bName)
		cfg.BucketAddAccess(builder, bAccess)
		csBucket := cfg.BucketEnd(builder)

		bNames = append(bNames, csBucket)
	}

	cfg.DepCfgStartBucketsVector(builder, len(bNames))
	for i := 0; i < len(bNames); i++ {
		builder.PrependUOffsetT(bNames[i])
	}
	buckets := builder.EndVector(len(bNames))

	metaBucket := builder.CreateString(app.DeploymentConfig.MetadataBucket)
	sourceBucket := builder.CreateString(app.DeploymentConfig.SourceBucket)

	cfg.DepCfgStart(builder)
	cfg.DepCfgAddBuckets(builder, buckets)
	cfg.DepCfgAddCurl(builder, curlBindingsVector)
	cfg.DepCfgAddMetadataBucket(builder, metaBucket)
	cfg.DepCfgAddSourceBucket(builder, sourceBucket)
	depcfg := cfg.DepCfgEnd(builder)

	appCode := builder.CreateString(app.AppHandlers)
	aName := builder.CreateString(app.Name)
	fiid := builder.CreateString(app.FunctionInstanceID)

	cfg.ConfigStart(builder)
	cfg.ConfigAddId(builder, uint32(app.ID))
	cfg.ConfigAddAppCode(builder, appCode)
	cfg.ConfigAddAppName(builder, aName)
	cfg.ConfigAddDepCfg(builder, depcfg)
	cfg.ConfigAddFunctionID(builder, app.FunctionID)
	cfg.ConfigAddFunctionInstanceID(builder, fiid)

	udtp := byte(0x0)
	if app.UsingTimer {
		udtp = byte(0x1)
	}
	cfg.ConfigAddUsingTimer(builder, udtp)

	srcMutation := byte(0x0)
	if app.SrcMutationEnabled {
		srcMutation = byte(0x1)
	}
	cfg.ConfigAddSrcMutationEnabled(builder, srcMutation)
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

	app.SrcMutationEnabled = m.isSrcMutationEnabled(&app.DeploymentConfig)
	if app.SrcMutationEnabled && !m.compareEventingVersion(mhVersion) {
		info.Code = m.statusCodes.errClusterVersion.Code
		info.Info = fmt.Sprintf("All eventing nodes in the cluster must be on version %d.%d or higher for allowing mutations against source bucket",
			mhVersion.major, mhVersion.minor)
		logging.Warnf("%s Version compat check failed: %s", logPrefix, info.Info)
		return
	}

	appContent := m.encodeAppPayload(app)

	if len(appContent) > util.MaxFunctionSize() {
		info.Code = m.statusCodes.errAppCodeSize.Code
		info.Info = fmt.Sprintf("Function: %s handler Code size is more than %d", app.Name, util.MaxFunctionSize())
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	c := &consumer.Consumer{}
	handlerHeaders := util.ToStringArray(app.Settings["handler_headers"])
	handlerFooters := util.ToStringArray(app.Settings["handler_footers"])
	compilationInfo, err := c.SpawnCompilationWorker(app.AppHandlers, string(appContent), app.Name, m.adminHTTPPort,
		handlerHeaders, handlerFooters)
	if err != nil || !compilationInfo.CompileSuccess {
		info.Code = m.statusCodes.errHandlerCompile.Code
		info.Info = compilationInfo
		return
	}

	logging.Infof("%s Function: %s using_timer: %s", logPrefix, app.Name, compilationInfo.UsingTimer)

	switch compilationInfo.UsingTimer {
	case "true":
		app.Settings["using_timer"] = true
		app.UsingTimer = true
	case "false":
		app.Settings["using_timer"] = false
		app.UsingTimer = false
	}
	appContent = m.encodeAppPayload(app)

	m.checkVersionCompat(compilationInfo.Version, info)
	if info.Code != m.statusCodes.ok.Code {
		return
	}

	settingsPath := metakvAppSettingsPath + app.Name
	settings := app.Settings

	mData, mErr := json.Marshal(&settings)
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

	err = util.WriteAppContent(metakvAppsPath, metakvChecksumPath, app.Name, appContent)
	if err != nil {
		info.Code = m.statusCodes.errSaveAppPs.Code
		logging.Errorf("%s Function: %s unable to save to primary store, err: %v", logPrefix, app.Name, err)
		return
	}

	var wInfo warningsInfo
	wInfo.Status = fmt.Sprintf("Stored function: '%s' in metakv", app.Name)

	switch strings.ToLower(compilationInfo.Level) {
	case "dp":
		msg := fmt.Sprintf("Function '%s' uses Developer Preview features. Do not use in production environments", app.Name)
		wInfo.Warnings = append(wInfo.Warnings, msg)

	case "beta":
		msg := fmt.Sprintf("Function '%s' uses Beta features. Do not use in production environments", app.Name)
		wInfo.Warnings = append(wInfo.Warnings, msg)
	}

	info.Code = m.statusCodes.ok.Code
	info.Info = wInfo
	return
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
		data, _ := json.Marshal(&resp)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%v", string(data))
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "Function: %s not deployed", appName)
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
}

func (m *ServiceMgr) getBootstrappingApps(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	bootstrappingApps := m.superSup.BootstrapAppList()
	data, err := json.Marshal(bootstrappingApps)
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

		data, err := json.Marshal(&workerPidMapping)
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

func (m *ServiceMgr) clearEventStats(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::clearEventStats"
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	logging.Infof("%s Got request to clear event stats from host: %rs", logPrefix, r.Host)
	m.superSup.ClearEventStats()
}

func (m *ServiceMgr) parseQueryHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateLocalAuth(w, r) {
		return
	}

	w.Header().Set("Content-Type", "application/x-www-form-urlencoded")

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		return
	}

	query := string(data)
	info, _ := util.Parse(query)
	response := url.Values{}
	info.FlattenParseInfo(&response)

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s", response.Encode())
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

	data, err := json.Marshal(util.SuperImpose(c, storedConfig))
	if err != nil {
		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("failed to marshal config, err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	logging.Infof("%s Saving config into metakv: %v", logPrefix, c)
	util.Retry(util.NewFixedBackoff(metakvOpRetryInterval),
		nil, metakvSetCallback, metakvConfigPath, data)
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

		response, err := json.Marshal(c)
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
		data, err = json.Marshal(response)
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

func (m *ServiceMgr) functionsHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::functionsHandler"

	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		cbauth.SendForbidden(w, EventingPermissionManage)
		return
	}

	functions := regexp.MustCompile("^/api/v1/functions/?$")
	functionsName := regexp.MustCompile("^/api/v1/functions/(.*[^/])/?$") // Match is agnostic of trailing '/'
	functionsNameSettings := regexp.MustCompile("^/api/v1/functions/(.*[^/])/settings/?$")
	functionsNameRetry := regexp.MustCompile("^/api/v1/functions/(.*[^/])/retry/?$")

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

			response, err := json.Marshal(settings)
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

			if info = m.validateSettings(settings); info.Code != m.statusCodes.ok.Code {
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

			response, err := json.Marshal(app)
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

			app.EventingVersion = util.EventingVer()

			runtimeInfo := m.savePrimaryStore(&app)
			if runtimeInfo.Code == m.statusCodes.ok.Code {
				audit.Log(auditevent.SaveDraft, r, appName)
				// Save to temp store only if saving to primary store succeeds
				if tempInfo := m.saveTempStore(app); tempInfo.Code != m.statusCodes.ok.Code {
					m.sendErrorInfo(w, tempInfo)
					return
				}
				m.sendRuntimeInfo(w, runtimeInfo)
			} else {
				m.sendErrorInfo(w, runtimeInfo)
			}

		case "DELETE":
			audit.Log(auditevent.DeleteFunction, r, appName)

			info := m.deletePrimaryStore(appName)
			// Delete the application from temp store only if app does not exist in primary store
			// or if the deletion succeeds on primary store
			if info.Code == m.statusCodes.errAppNotDeployed.Code || info.Code == m.statusCodes.ok.Code {
				audit.Log(auditevent.DeleteDrafts, r, appName)

				if runtimeInfo := m.deleteTempStore(appName); runtimeInfo.Code != m.statusCodes.ok.Code {
					m.sendErrorInfo(w, runtimeInfo)
					return
				}
			} else {
				m.sendErrorInfo(w, info)
			}

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

	data, err := json.Marshal(response)
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
	appDeployedNodesCounter, appBootstrappingNodesCounter, numEventingNodes, info := m.getAppList()
	if info.Code != m.statusCodes.ok.Code {
		return
	}

	response.NumEventingNodes = numEventingNodes
	for _, app := range m.getTempStoreAll() {
		deploymentStatus, dOk := app.Settings["deployment_status"].(bool)
		processingStatus, pOk := app.Settings["processing_status"].(bool)
		if !dOk || !pOk {
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
		status.CompositeStatus = determineStatus(status, numEventingNodes)
		response.Apps = append(response.Apps, status)
	}
	return
}

func determineStatus(status appStatus, numEventingNodes int) string {
	logPrefix := "ServiceMgr::determineStatus"

	if status.DeploymentStatus && status.ProcessingStatus {
		if status.NumBootstrappingNodes == 0 && status.NumDeployedNodes == numEventingNodes {
			return "deployed"
		}
		return "deploying"
	}

	// For case:
	// T1 - bootstrap was requested
	// T2 - undeploy was requested
	// T3 - boostrap finished
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

		response, err := json.Marshal(statsList)
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
			app.DeploymentConfig.Curl[i].Password = ""
		}
		app.Settings["deployment_status"] = false
		app.Settings["processing_status"] = false
		exportedFns = append(exportedFns, app.Name)
	}

	logging.Infof("%s Exported function list: %+v", logPrefix, exportedFns)

	data, err := json.Marshal(apps)
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

	infoList := m.createApplications(r, appList, true)

	importedFns := make([]string, 0)
	for _, app := range *appList {
		importedFns = append(importedFns, app.Name)
	}

	logging.Infof("%s Imported functions: %+v", logPrefix, importedFns)
	m.sendRuntimeInfoList(w, infoList)
}

func (m *ServiceMgr) createApplications(r *http.Request, appList *[]application, undeploy bool) (infoList []*runtimeInfo) {
	logPrefix := "ServiceMgr::createApplications"

	infoList = []*runtimeInfo{}
	var err error
	for _, app := range *appList {
		audit.Log(auditevent.CreateFunction, r, app.Name)

		if infoVal := m.validateApplication(&app); infoVal.Code != m.statusCodes.ok.Code {
			logging.Warnf("%s Validating %ru failed: %v", logPrefix, app, infoVal)
			infoList = append(infoList, infoVal)
			continue
		}

		if undeploy {
			app.Settings["deployment_status"] = false
			app.Settings["processing_status"] = false
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

		app.EventingVersion = util.EventingVer()

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
	clusterInfo, err := util.FetchNewClusterInfoCache(nsServerEndpoint)
	if err != nil {
		info.Code = m.statusCodes.errConnectNsServer.Code
		info.Info = fmt.Sprintf("Failed to get cluster info cache, err: %v", err)
		return false, info
	}

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
	"vulcan": {5, 5},
	"alice":  {6, 0},
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
	clusterInfo, err := util.FetchNewClusterInfoCache(nsServerEndpoint)
	if err != nil {
		info.Code = m.statusCodes.errConnectNsServer.Code
		info.Info = fmt.Sprintf("Failed to get cluster info cache, err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	var need, have version
	have.major, have.minor = clusterInfo.GetClusterVersion()
	need, ok := verMap[required]

	if !ok || !have.satisfies(need) {
		info.Code = m.statusCodes.errClusterVersion.Code
		info.Info = fmt.Sprintf("Function requires %v but cluster is at %v", need, have)
		logging.Warnf("%s Version compat check failed: %s", logPrefix, info.Info)
		return
	}

	logging.Infof("%s Function need %v satisfied by cluster %v", logPrefix, have, need)
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
