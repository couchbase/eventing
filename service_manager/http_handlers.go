package servicemanager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"runtime/trace"
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
	flatbuffers "github.com/google/flatbuffers/go"
)

func (m *ServiceMgr) startTracing(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	logging.Infof("Got request to start tracing")
	audit.Log(auditevent.StartTracing, r, nil)

	os.Remove(m.uuid + "_trace.out")

	f, err := os.Create(m.uuid + "_trace.out")
	if err != nil {
		logging.Infof("Failed to open file to write trace output, err: %v", err)
		return
	}
	defer f.Close()

	err = trace.Start(f)
	if err != nil {
		logging.Infof("Failed to start runtime.Trace, err: %v", err)
		return
	}

	<-m.stopTracerCh
	trace.Stop()
}

func (m *ServiceMgr) stopTracing(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	audit.Log(auditevent.StopTracing, r, nil)
	logging.Infof("Got request to stop tracing")
	m.stopTracerCh <- struct{}{}
}

func (m *ServiceMgr) getNodeUUID(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}
	logging.Debugf("Got request to fetch UUID from host %v", r.Host)
	fmt.Fprintf(w, "%v", m.uuid)
}

func (m *ServiceMgr) getNodeVersion(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}
	logging.Debugf("Got request to fetch version from host %v", r.Host)
	fmt.Fprintf(w, "%v", util.EventingVer())
}

func (m *ServiceMgr) debugging(w http.ResponseWriter, r *http.Request) {
	logging.Debugf("Got debugging fetch %v", r.URL)
	jsFile := path.Base(r.URL.Path)
	if strings.HasSuffix(jsFile, srcCodeExt) {
		appName := jsFile[:len(jsFile)-len(srcCodeExt)]
		handler := m.getHandler(appName)

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", handler)
		if handler == "" {
			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
			fmt.Fprintf(w, "App: %s not deployed", appName)
		}
	} else if strings.HasSuffix(jsFile, srcMapExt) {
		appName := jsFile[:len(jsFile)-len(srcMapExt)]
		sourceMap := m.getSourceMap(appName)

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", sourceMap)

		if sourceMap == "" {
			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
			fmt.Fprintf(w, "App: %s not deployed", appName)
		}
	} else {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errInvalidExt.Code))
		fmt.Fprintf(w, "Invalid extension for %s", jsFile)
	}
}

func (m *ServiceMgr) deletePrimaryStoreHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("Deleting application %v from primary store", appName)
	audit.Log(auditevent.DeleteFunction, r, appName)
	m.deletePrimaryStore(appName)
}

// Deletes application from primary store and returns the appropriate success/error code
func (m *ServiceMgr) deletePrimaryStore(appName string) (info *runtimeInfo) {
	info = &runtimeInfo{}
	logging.Infof("Deleting application %v from primary store", appName)

	checkIfDeployed := false
	for _, app := range util.ListChildren(metakvAppsPath) {
		if app == appName {
			checkIfDeployed = true
		}
	}

	if !checkIfDeployed {
		info.Code = m.statusCodes.errAppNotDeployed.Code
		info.Info = fmt.Sprintf("App: %v not deployed", appName)
		return
	}

	appState := m.superSup.GetAppState(appName)
	if appState != common.AppStateUndeployed {
		info.Code = m.statusCodes.errAppNotUndeployed.Code
		info.Info = fmt.Sprintf("Skipping delete request from primary store for app: %v as it hasn't been undeployed", appName)
		return
	}

	settingPath := metakvAppSettingsPath + appName
	err := util.MetaKvDelete(settingPath, nil)
	if err != nil {
		info.Code = m.statusCodes.errDelAppSettingsPs.Code
		info.Info = fmt.Sprintf("Failed to delete setting for app: %v, err: %v", appName, err)
		return
	}

	err = util.DeleteAppContent(metakvAppsPath, metakvChecksumPath, appName)
	if err != nil {
		info.Code = m.statusCodes.errDelAppPs.Code
		info.Info = fmt.Sprintf("Failed to delete app: %v, err: %v", appName, err)
		return
	}

	// TODO : This must be changed to app not deployed / found
	info.Code = m.statusCodes.ok.Code
	info.Info = fmt.Sprintf("Deleting app: %v in the background", appName)
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
	info = &runtimeInfo{}
	logging.Infof("Deleting drafts from temporary store: %v", appName)

	checkIfDeployed := false
	for _, app := range util.ListChildren(metakvTempAppsPath) {
		if app == appName {
			checkIfDeployed = true
		}
	}

	if !checkIfDeployed {
		info.Code = m.statusCodes.errAppNotDeployed.Code
		info.Info = fmt.Sprintf("App: %v not deployed", appName)
		return
	}

	appState := m.superSup.GetAppState(appName)
	if appState != common.AppStateUndeployed {
		info.Code = m.statusCodes.errAppNotUndeployed.Code
		info.Info = fmt.Sprintf("Skipping delete request from temp store for app: %v as it hasn't been undeployed", appName)
		return
	}

	if err := util.DeleteAppContent(metakvTempAppsPath, metakvTempChecksumPath, appName); err != nil {
		info.Code = m.statusCodes.errDelAppTs.Code
		info.Info = fmt.Sprintf("Failed to delete App definition : %v, err: %v", appName, err)
		return
	}
	info.Code = m.statusCodes.ok.Code
	info.Info = fmt.Sprintf("Deleting app: %v in the background", appName)
	return
}

func (m *ServiceMgr) getDebuggerURL(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("App: %v got request to get V8 debugger url", appName)

	if m.checkIfDeployed(appName) {
		debugURL, _ := m.superSup.GetDebuggerURL(appName)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", debugURL)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "App: %v not deployed", appName)
}

func (m *ServiceMgr) getLocalDebugURL(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("App: %v got request to get local V8 debugger url", appName)

	cfg := m.config.Load()
	dir := cfg["eventing_dir"].(string)

	filePath := fmt.Sprintf("%s/%s_frontend.url", dir, appName)
	u, err := ioutil.ReadFile(filePath)
	if err != nil {
		logging.Errorf("App: %v Failed to read contents from debugger frontend url file, err: %v", appName, err)
		fmt.Fprintf(w, "")
		return
	}

	fmt.Fprintf(w, "%v", string(u))
}

func (m *ServiceMgr) startDebugger(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("App: %v got request to start V8 debugger", appName)
	audit.Log(auditevent.StartDebug, r, appName)

	if m.checkIfDeployed(appName) {
		m.superSup.SignalStartDebugger(appName)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "App: %v Started Debugger", appName)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "App: %v not deployed", appName)
}

func (m *ServiceMgr) stopDebugger(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("App: %v got request to stop V8 debugger", appName)
	audit.Log(auditevent.StopDebug, r, appName)

	if m.checkIfDeployed(appName) {
		m.superSup.SignalStopDebugger(appName)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "App: %v Stopped Debugger", appName)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "App: %v not deployed", appName)
}

func (m *ServiceMgr) getEventProcessingStats(w http.ResponseWriter, r *http.Request) {
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
			return
		}

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", string(data))
	} else {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
		fmt.Fprintf(w, "App: %v not deployed", appName)
	}
}

var getDeployedAppsCallback = func(args ...interface{}) error {
	aggDeployedApps := args[0].(*map[string]map[string]string)
	nodeAddrs := args[1].([]string)

	var err error
	*aggDeployedApps, err = util.GetDeployedApps("/getLocallyDeployedApps", nodeAddrs)
	if err != nil {
		logging.Errorf("Failed to get deployed apps, err: %v", err)
		return err
	}

	logging.Tracef("Cluster wide deployed app status: %rm", *aggDeployedApps)

	return nil
}

// Returns list of apps that are deployed i.e. finished dcp/timer/debugger related bootstrap
func (m *ServiceMgr) getDeployedApps(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getDeployedApps"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	audit.Log(auditevent.ListDeployed, r, nil)

	nodeAddrs, err := m.getActiveNodeAddrs()
	if err != nil {
		logging.Warnf("%s Failed to fetch active Eventing nodes, err: %v", logPrefix, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errActiveEventingNodes.Code))
		fmt.Fprintf(w, "")
		return
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

	numEventingNodes := len(nodeAddrs)
	if numEventingNodes <= 0 {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errNoEventingNodes.Code))
		fmt.Fprintf(w, "")
		return
	}

	deployedApps := make(map[string]string)
	for app, numNodesDeployed := range appDeployedNodesCounter {
		if numNodesDeployed == numEventingNodes {
			deployedApps[app] = ""
		}
	}

	buf, err := json.Marshal(deployedApps)
	if err != nil {
		logging.Errorf("%s Failed to marshal list of deployed apps, err: %v", logPrefix, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, "")
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s", string(buf))
}

func (m *ServiceMgr) getLocallyDeployedApps(w http.ResponseWriter, r *http.Request) {
	logPrefix := "ServiceMgr::getLocallyDeployedApps"

	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	deployedApps := m.superSup.GetDeployedApps()

	buf, err := json.Marshal(deployedApps)
	if err != nil {
		logging.Errorf("%s Failed to marshal list of deployed apps, err: %v", logPrefix, err)
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

	appList := util.ListChildren(metakvAppsPath)

	for _, appName := range appList {
		// TODO: Leverage error returned from rebalance task progress and fail the rebalance
		// if it occurs
		appProgress, err := m.superSup.RebalanceTaskProgress(appName)
		logging.Infof("%s Rebalance progress from node with rest port: %rs progress: %v",
			logPrefix, m.restPort, appProgress)
		if err == nil {
			progress.VbsOwnedPerPlan += appProgress.VbsOwnedPerPlan
			progress.VbsRemainingToShuffle += appProgress.VbsRemainingToShuffle
		}
	}

	if progress.VbsRemainingToShuffle > 0 {
		m.statsWritten = false
	}

	if progress.VbsRemainingToShuffle == 0 && progress.VbsOwnedPerPlan == 0 && !m.statsWritten {
		statsList := m.populateStats(true)
		data, err := json.Marshal(statsList)
		if err != nil {
			logging.Errorf("%s Failed to unmarshal stats, err: %v", logPrefix, err)
		} else {
			logging.Infof("%s No more vbucket remaining to shuffle. Stats dump: %v", logPrefix, string(data))
		}

		m.statsWritten = true
	}

	buf, err := json.Marshal(progress)
	if err != nil {
		logging.Errorf("%s Failed to unmarshal rebalance progress across all producers on current node, err: %v", logPrefix, err)
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
		logging.Errorf("Failed to unmarshal event processing stats from all producers, err: %v", err)
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

	logging.Infof("%s Going to query eventing nodes: %rs for rebalance progress",
		logPrefix, m.eventingNodeAddrs)

	aggProgress, errMap := util.GetProgress("/getRebalanceProgress", m.eventingNodeAddrs)
	if len(errMap) > 0 {
		logging.Errorf("%s Failed to get progress from all eventing nodes: %rs err: %rs",
			logPrefix, m.eventingNodeAddrs, errMap)
		return
	}

	buf, err := json.Marshal(aggProgress)
	if err != nil {
		logging.Errorf("%s Failed to unmarshal rebalance progress across all producers, err: %v", logPrefix, err)
		return
	}

	w.Write(buf)
}

// Report aggregated rebalance status from all Eventing nodes in the cluster
func (m *ServiceMgr) getAggRebalanceStatus(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	status, err := util.CheckIfRebalanceOngoing("/getRebalanceStatus", m.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("Failed to grab correct rebalance status from some/all nodes, err: %v", err)
		return
	}

	w.Write([]byte(strconv.FormatBool(status)))
}

func (m *ServiceMgr) getLatencyStats(w http.ResponseWriter, r *http.Request) {
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
			return
		}

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", string(data))
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "App: %v not deployed", appName)
}

func (m *ServiceMgr) getExecutionStats(w http.ResponseWriter, r *http.Request) {
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
			return
		}

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", string(data))
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "App: %v not deployed", appName)
}

func (m *ServiceMgr) getFailureStats(w http.ResponseWriter, r *http.Request) {
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
			return
		}

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", string(data))
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "App: %v not deployed", appName)
}

func (m *ServiceMgr) getSeqsProcessed(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	if m.checkIfDeployed(appName) {
		seqNoProcessed := m.superSup.GetSeqsProcessed(appName)

		data, err := json.Marshal(seqNoProcessed)
		if err != nil {
			logging.Errorf("App: %v, failed to fetch vb sequences processed so far, err: %v", appName, err)

			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errGetVbSeqs.Code))
			fmt.Fprintf(w, "")
			return
		}

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", string(data))
	} else {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
		fmt.Fprintf(w, "App: %v not deployed", appName)
	}

}

func (m *ServiceMgr) setSettingsHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	audit.Log(auditevent.SetSettings, r, appName)
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errReadReq.Code))
		fmt.Fprintf(w, "Failed to read request body, err: %v", err)
		return
	}

	var settings map[string]interface{}
	err = json.Unmarshal(data, &settings)
	if err != nil {
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
	logging.Infof("Get settings for app %v", appName)

	app, status := m.getTempStore(appName)
	if status.Code != m.statusCodes.ok.Code {
		return nil, status
	}

	// State validation - app must be in deployed state
	deployedApps := m.superSup.GetDeployedApps()
	processingStatus, pOk := app.Settings["processing_status"].(bool)
	deploymentStatus, dOk := app.Settings["deployment_status"].(bool)
	info := runtimeInfo{}

	if pOk && dOk {
		// Check for disable processing
		if deploymentStatus == true && processingStatus == false {
			if _, ok := deployedApps[appName]; !ok {
				info.Code = m.statusCodes.errAppNotInit.Code
				info.Info = fmt.Sprintf("App: %v not processing mutations. Operation not permitted. Fetch function definition instead", appName)
				return nil, &info
			}
		}

		// Check for undeploy
		if deploymentStatus == false && processingStatus == false {
			if _, ok := deployedApps[appName]; !ok {
				info.Code = m.statusCodes.errAppNotInit.Code
				info.Info = fmt.Sprintf("App: %v not bootstrapped. Operation not permitted. Fetch function definition instead", appName)
				return nil, &info
			}
		}
	} else {
		info.Code = m.statusCodes.errStatusesNotFound.Code
		info.Info = fmt.Sprintf("App: %v Missing processing or deployment statuses or both", appName)
		return nil, &info
	}

	info.Code = m.statusCodes.ok.Code
	info.Info = fmt.Sprintf("Got settings for app: %v", appName)
	return &app.Settings, &info
}

func (m *ServiceMgr) setSettings(appName string, data []byte) (info *runtimeInfo) {
	info = &runtimeInfo{}
	logging.Infof("Set settings for app %v", appName)

	if rebStatus := m.checkRebalanceStatus(); rebStatus.Code != m.statusCodes.ok.Code {
		info.Code = rebStatus.Code
		info.Info = rebStatus.Info
		return
	}

	var settings map[string]interface{}
	err := json.Unmarshal(data, &settings)
	if err != nil {
		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("Failed to unmarshal setting supplied, err: %v", err)
		return
	}

	// Get the app from temp store and update its settings with those provided
	app, info := m.getTempStore(appName)
	if info.Code != m.statusCodes.ok.Code {
		return
	}

	for setting := range settings {
		app.Settings[setting] = settings[setting]
	}

	// State validation - app must be in deployed state
	processingStatus, pOk := app.Settings["processing_status"].(bool)
	deploymentStatus, dOk := app.Settings["deployment_status"].(bool)

	deployedApps := m.superSup.GetDeployedApps()
	if pOk && dOk {
		// Check for disable processing
		if deploymentStatus == true && processingStatus == false {
			if _, ok := deployedApps[appName]; !ok {
				info.Code = m.statusCodes.errAppNotInit.Code
				info.Info = fmt.Sprintf("App: %v not processing mutations. Operation is not permitted. Edit function instead", appName)
				return
			}
		}

		// Check for undeploy
		if deploymentStatus == false && processingStatus == false {
			if _, ok := deployedApps[appName]; !ok {
				info.Code = m.statusCodes.errAppNotInit.Code
				info.Info = fmt.Sprintf("App: %v not bootstrapped. Operation not permitted. Edit function instead", appName)
				return
			}
		}
	} else {
		info.Code = m.statusCodes.errStatusesNotFound.Code
		info.Info = fmt.Sprintf("App: %v Missing processing or deployment statuses or both", appName)
		return
	}

	data, err = json.Marshal(app.Settings)
	if err != nil {
		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("Failed to marshal settings as JSON, err : %v", err)
		return
	}

	metakvPath := metakvAppSettingsPath + appName
	err = util.MetakvSet(metakvPath, data, nil)
	if err != nil {
		info.Code = m.statusCodes.errSetSettingsPs.Code
		info.Info = fmt.Sprintf("Failed to store setting for app: %v, err: %v", appName, err)
		return
	}

	// Write the updated app along with its settings back to temp store
	if info = m.saveTempStore(app); info.Code != m.statusCodes.ok.Code {
		return
	}

	info.Code = m.statusCodes.ok.Code
	info.Info = fmt.Sprintf("stored settings for app: %v", appName)
	return
}

func (m *ServiceMgr) getPrimaryStoreHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	logging.Infof("Getting all applications in primary store")
	audit.Log(auditevent.FetchFunctions, r, nil)

	appList := util.ListChildren(metakvAppsPath)
	respData := make([]application, len(appList))

	for index, appName := range appList {
		data, err := util.ReadAppContent(metakvAppsPath, metakvChecksumPath, appName)
		if err == nil {

			config := cfg.GetRootAsConfig(data, 0)

			app := new(application)
			app.AppHandlers = string(config.AppCode())
			app.Name = string(config.AppName())
			app.ID = int(config.Id())

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
					}
					buckets = append(buckets, newBucket)
				}
			}

			settingsPath := metakvAppSettingsPath + appName
			sData, sErr := util.MetakvGet(settingsPath)
			if sErr == nil {
				settings := make(map[string]interface{})
				uErr := json.Unmarshal(sData, &settings)
				if uErr != nil {
					logging.Errorf("Failed to unmarshal settings data from metakv, err: %v", uErr)
				} else {
					app.Settings = settings
				}
			} else {
				logging.Errorf("Failed to fetch settings data from metakv, err: %v", sErr)
			}

			depcfg.Buckets = buckets
			app.DeploymentConfig = *depcfg

			respData[index] = *app
		}
	}

	data, err := json.Marshal(respData)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, "Failed to marshal response for get_application, err: %v", err)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s\n", data)
}

func (m *ServiceMgr) getTempStoreHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintln(w, `{"error":"Request not authorized"}`)
		return
	}

	// Moving just this case to trace log level as ns_server keeps polling
	// eventing every 5s to see if new functions have been created. So on an idle
	// cluster it will log lot of this message.
	logging.Tracef("Fetching function draft definitions")
	audit.Log(auditevent.FetchDrafts, r, nil)
	applications := m.getTempStoreAll()

	data, err := json.Marshal(applications)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, `{"error":"Failed to marshal response, err: %v"}`, err)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s\n", data)
}

func (m *ServiceMgr) getTempStore(appName string) (app application, info *runtimeInfo) {
	info = &runtimeInfo{}
	logging.Infof("Fetching function draft definitions for %s", appName)

	for _, name := range util.ListChildren(metakvTempAppsPath) {
		data, err := util.ReadAppContent(metakvTempAppsPath, metakvTempChecksumPath, name)
		if err == nil {
			uErr := json.Unmarshal(data, &app)
			if uErr != nil {
				logging.Errorf("Failed to unmarshal data from metakv, err: %v", uErr)
				continue
			}

			if app.Name == appName {
				info.Code = m.statusCodes.ok.Code
				return
			}
		}
	}

	info.Code = m.statusCodes.errAppNotFoundTs.Code
	info.Info = fmt.Sprintf("Function %s not found", appName)
	return
}

func (m *ServiceMgr) getTempStoreAll() []application {
	tempAppList := util.ListChildren(metakvTempAppsPath)
	applications := make([]application, len(tempAppList))

	for i, name := range tempAppList {
		data, err := util.ReadAppContent(metakvTempAppsPath, metakvTempChecksumPath, name)
		if err == nil {
			var app application
			uErr := json.Unmarshal(data, &app)
			if uErr != nil {
				logging.Errorf("Failed to unmarshal data from metakv, name:%v, err: %v data: %v", name, uErr, string(data))
				continue
			}

			applications[i] = app
		}
	}

	return applications
}

func (m *ServiceMgr) saveTempStoreHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	audit.Log(auditevent.SaveDraft, r, appName)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errReadReq.Code))
		fmt.Fprintf(w, "Failed to read request body, err: %v", err)
		return
	}

	var app application
	err = json.Unmarshal(data, &app)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errUnmarshalPld.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errUnmarshalPld.Code))
		errString := fmt.Sprintf("App: %s, Failed to unmarshal payload", appName)
		logging.Errorf("%s, err: %v", errString, err)
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
	info = &runtimeInfo{}
	appName := app.Name

	data, err := json.Marshal(app)
	if err != nil {
		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("Failed to marshal data as JSON to save in temp store, err : %v", err)
		return
	}

	//Delete stale entry
	err = util.DeleteStaleAppContent(metakvTempAppsPath, appName)
	if err != nil {
		info.Code = m.statusCodes.errSaveAppTs.Code
		info.Info = fmt.Sprintf("Failed to clean up stale entry from TempStore: %v err: %v", appName, err)
		return
	}

	err = util.WriteAppContent(metakvTempAppsPath, metakvTempChecksumPath, appName, data)
	if err != nil {
		info.Code = m.statusCodes.errSaveAppTs.Code
		info.Info = fmt.Sprintf("Failed to store handlers for app: %v err: %v", appName, err)
		return
	}

	info.Code = m.statusCodes.ok.Code
	info.Info = fmt.Sprintf("Stored handlers for app: %v", appName)
	return
}

func (m *ServiceMgr) savePrimaryStoreHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	audit.Log(auditevent.CreateFunction, r, appName)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errString := fmt.Sprintf("App: %s, failed to read content from http request body", appName)
		logging.Errorf("%s, err: %v", errString, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		fmt.Fprintf(w, "%s\n", errString)
		return
	}

	var app application
	err = json.Unmarshal(data, &app)
	if err != nil {
		errString := fmt.Sprintf("App: %s, Failed to unmarshal payload", appName)
		logging.Errorf("%s, err: %v", errString, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errUnmarshalPld.Code))
		fmt.Fprintf(w, "%s\n", errString)
		return
	}

	if info := m.validateApplication(&app); info.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, info)
		return
	}

	info := m.savePrimaryStore(app)
	m.sendRuntimeInfo(w, info)
}

func (m *ServiceMgr) checkRebalanceStatus() (info *runtimeInfo) {
	info = &runtimeInfo{}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	rebStatus, err := util.CheckIfRebalanceOngoing("/getRebalanceStatus", m.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("Failed to grab correct rebalance status from some/all Eventing nodes, err: %v", err)

		info.Code = m.statusCodes.errGetRebStatus.Code
		info.Info = "Failed to get rebalance status from eventing nodes"
		return
	}

	logging.Infof("Rebalance ongoing across some/all Eventing nodes: %v", rebStatus)

	if rebStatus {
		logging.Warnf("Rebalance ongoing on some/all Eventing nodes")

		info.Code = m.statusCodes.errRebOngoing.Code
		info.Info = "Rebalance ongoing on some/all Eventing nodes, creating new apps or changing settings for existing apps isn't allowed"
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

// Saves application to metakv and returns appropriate success/error code
func (m *ServiceMgr) savePrimaryStore(app application) (info *runtimeInfo) {
	info = &runtimeInfo{}
	logging.Infof("Saving application %s to primary store", app.Name)

	if rebStatus := m.checkRebalanceStatus(); rebStatus.Code != m.statusCodes.ok.Code {
		info.Code = rebStatus.Code
		info.Info = rebStatus.Info
		return
	}

	if m.checkIfDeployed(app.Name) {
		info.Code = m.statusCodes.errAppDeployed.Code
		info.Info = fmt.Sprintf("App with same name %s is already deployed, skipping save request", app.Name)
		return
	}

	if app.DeploymentConfig.SourceBucket == app.DeploymentConfig.MetadataBucket {
		info.Code = m.statusCodes.errSrcMbSame.Code
		info.Info = fmt.Sprintf("Source bucket same as metadata bucket. source_bucket : %s metadata_bucket : %s", app.DeploymentConfig.SourceBucket, app.DeploymentConfig.MetadataBucket)
		return
	}

	builder := flatbuffers.NewBuilder(0)
	var bNames []flatbuffers.UOffsetT

	for i := 0; i < len(app.DeploymentConfig.Buckets); i++ {
		alias := builder.CreateString(app.DeploymentConfig.Buckets[i].Alias)
		bName := builder.CreateString(app.DeploymentConfig.Buckets[i].BucketName)

		cfg.BucketStart(builder)
		cfg.BucketAddAlias(builder, alias)
		cfg.BucketAddBucketName(builder, bName)
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
	cfg.DepCfgAddMetadataBucket(builder, metaBucket)
	cfg.DepCfgAddSourceBucket(builder, sourceBucket)
	depcfg := cfg.DepCfgEnd(builder)

	appCode := builder.CreateString(app.AppHandlers)
	aName := builder.CreateString(app.Name)

	cfg.ConfigStart(builder)
	cfg.ConfigAddId(builder, uint32(app.ID))
	cfg.ConfigAddAppCode(builder, appCode)
	cfg.ConfigAddAppName(builder, aName)
	cfg.ConfigAddDepCfg(builder, depcfg)
	cfg.ConfigAddHandlerUUID(builder, app.HandlerUUID)
	config := cfg.ConfigEnd(builder)

	builder.Finish(config)

	appContent := builder.FinishedBytes()

	if len(appContent) > maxHandlerSize {
		info.Code = m.statusCodes.errAppCodeSize.Code
		info.Info = fmt.Sprintf("App: %s Handler Code size is more than 128K", app.Name)
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

	logging.Infof("App: %s using doc_timers: %s", app.Name, compilationInfo.UsingDocTimer)

	switch compilationInfo.UsingDocTimer {
	case "true":
		app.Settings["using_doc_timer"] = true
	case "false":
		app.Settings["using_doc_timer"] = false
	}

	m.checkVersionCompat(compilationInfo.Version, info)
	if info.Code != m.statusCodes.ok.Code {
		return
	}

	settingsPath := metakvAppSettingsPath + app.Name
	settings := app.Settings

	mData, mErr := json.Marshal(&settings)
	if mErr != nil {
		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("App: %s Failed to marshal settings, err: %v", app.Name, mErr)
		return
	}

	mkvErr := util.MetakvSet(settingsPath, mData, nil)
	if mkvErr != nil {
		info.Code = m.statusCodes.errSetSettingsPs.Code
		info.Info = fmt.Sprintf("App: %s Failed to store updated settings in metakv, err: %v", app.Name, mkvErr)
		return
	}

	//Delete stale entry
	err = util.DeleteStaleAppContent(metakvAppsPath, app.Name)
	if err != nil {
		info.Code = m.statusCodes.errSaveAppPs.Code
		info.Info = fmt.Sprintf("App: %s failed to clean up stale entry, err: %v", app.Name, err)
		return
	}

	err = util.WriteAppContent(metakvAppsPath, metakvChecksumPath, app.Name, appContent)
	if err != nil {
		info.Code = m.statusCodes.errSaveAppPs.Code
		info.Info = fmt.Sprintf("App: %s failed to write to metakv, err: %v", app.Name, err)
		return
	}

	var wInfo warningsInfo
	wInfo.Status = "Stored application config in metakv"

	switch strings.ToLower(compilationInfo.Level) {
	case "dp":
		msg := fmt.Sprintf("Handler '%v' uses Developer Preview features. Do not use in production environments", app.Name)
		wInfo.Warnings = append(wInfo.Warnings, msg)

	case "beta":
		msg := fmt.Sprintf("Handler '%v' uses Beta features. Do not use in production environments", app.Name)
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
	fmt.Fprintf(w, "App: %v not deployed", appName)
}

func (m *ServiceMgr) getAggBootstrappingApps(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m)

	appsBootstrapping, err := util.GetAggBootstrappingApps("/getBootstrappingApps", m.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("Failed to grab bootstrapping app list from all eventing nodes or some apps are undergoing bootstrap")
		return
	}

	w.Write([]byte(strconv.FormatBool(appsBootstrapping)))
}

func (m *ServiceMgr) getBootstrappingApps(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	bootstrappingApps := m.superSup.BootstrapAppList()
	data, err := json.Marshal(bootstrappingApps)
	if err != nil {
		fmt.Fprintf(w, "Failed to marshal bootstrapping app list, err: %v", err)
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
	fmt.Fprintf(w, "App: %v not deployed", appName)
}

func (m *ServiceMgr) getCreds(w http.ResponseWriter, r *http.Request) {
	if !m.validateLocalAuth(w, r) {
		return
	}

	m.credsCounter++

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
		logging.Errorf("Failed to get credentials for endpoint: %rs, err: %v", strippedEndpoint, err)
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
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	logging.Infof("Got request to clear event stats from host: %rs", r.Host)
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

func (m *ServiceMgr) getConfig() (c config, info *runtimeInfo) {
	info = &runtimeInfo{}
	data, err := util.MetakvGet(metakvConfigPath)
	if err != nil {
		info.Code = m.statusCodes.errGetConfig.Code
		info.Info = fmt.Sprintf("Failed to get config, err: %v", err)
		return
	}

	if !bytes.Equal(data, nil) {
		err = json.Unmarshal(data, &c)
		if err != nil {
			info.Code = m.statusCodes.errUnmarshalPld.Code
			info.Info = fmt.Sprintf("Failed to unmarshal payload from metakv, err: %v", err)
			return
		}
	}

	logging.Infof("Retrieving config from metakv: %ru", c)
	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) saveConfig(c config) (info *runtimeInfo) {
	info = &runtimeInfo{}
	data, err := json.Marshal(c)
	if err != nil {
		info.Code = m.statusCodes.errMarshalResp.Code
		info.Info = fmt.Sprintf("Failed to marshal config, err: %v", err)
		return
	}

	logging.Infof("Saving config into metakv: %v", c)
	err = util.MetakvSet(metakvConfigPath, data, nil)
	if err != nil {
		info.Code = m.statusCodes.errSaveConfig.Code
		info.Info = fmt.Sprintf("Failed to store config to meta kv, err: %v", err)
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) configHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		fmt.Fprintln(w, `{"error":"Request not authorized"}`)
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
			info.Info = fmt.Sprintf("Failed to marshal config, err : %v", err)
			m.sendErrorInfo(w, info)
			return
		}

		fmt.Fprintf(w, "%s", string(response))

	case "POST":
		audit.Log(auditevent.SaveConfig, r, nil)

		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			info.Code = m.statusCodes.errReadReq.Code
			info.Info = fmt.Sprintf("Failed to read request body, err: %v", err)
			m.sendErrorInfo(w, info)
			return
		}

		var c config
		err = json.Unmarshal(data, &c)
		if err != nil {
			info.Code = m.statusCodes.errUnmarshalPld.Code
			info.Info = fmt.Sprintf("Failed to unmarshal config from metakv, err: %v", err)
			m.sendErrorInfo(w, info)
			return
		}

		if info = m.saveConfig(c); info.Code != m.statusCodes.ok.Code {
			m.sendErrorInfo(w, info)
		}

		response := configResponse{false}
		data, err = json.Marshal(response)
		if err != nil {
			info.Code = m.statusCodes.errMarshalResp.Code
			info.Info = fmt.Sprintf("Failed to marshal response, err: %v", err)
			m.sendErrorInfo(w, info)
			return
		}

		fmt.Fprintf(w, "%s", string(data))

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
}

func (m *ServiceMgr) functionsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		fmt.Fprintln(w, `{"error":"Request not authorized"}`)
		return
	}

	functions := regexp.MustCompile("^/api/v1/functions/?$")
	functionsName := regexp.MustCompile("^/api/v1/functions/(.+[^/])/?$") // Match is agnostic of trailing '/'
	functionsNameSettings := regexp.MustCompile("^/api/v1/functions/(.+[^/])/settings/?$")
	functionsNameRetry := regexp.MustCompile("^/api/v1/functions/(.+[^/])/retry/?$")

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
			info.Info = fmt.Sprintf("Failed to read request body, err : %v", err)
			m.sendErrorInfo(w, info)
			return
		}

		var retryBody retry
		err = json.Unmarshal(data, &retryBody)
		if err != nil {
			info.Code = m.statusCodes.errMarshalResp.Code
			info.Info = fmt.Sprintf("Failed to unmarshal retry, err: %v", err)
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
				info.Info = fmt.Sprintf("Failed to marshal app, err : %v", err)
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
				info.Info = fmt.Sprintf("Failed to read request body, err: %v", err)
				m.sendErrorInfo(w, info)
				return
			}

			var settings map[string]interface{}
			err = json.Unmarshal(data, &settings)
			if err != nil {
				info.Code = m.statusCodes.errMarshalResp.Code
				info.Info = fmt.Sprintf("Failed to unmarshal setting supplied, err: %v", err)
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
				info.Info = fmt.Sprintf("Failed to marshal app, err : %v", err)
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
				info.Info = fmt.Sprintf("Function name in the URL (%s) and body (%s) must be same", appName, app.Name)
				m.sendErrorInfo(w, info)
				return
			}

			var err error
			app.EventingVersion = util.EventingVer()
			app.HandlerUUID, err = util.GenerateHandlerUUID()
			if err != nil {
				info.Code = m.statusCodes.errUUIDGen.Code
				info.Info = fmt.Sprintf("UUID generation failed for handler: %s", appName)
				m.sendErrorInfo(w, info)
				return
			}
			logging.Infof("HandlerUUID generated for handler: %s, UUID: %d", app.Name, app.HandlerUUID)
			runtimeInfo := m.savePrimaryStore(app)
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
	info = &runtimeInfo{}

	retryPath := metakvAppsRetryPath + appName
	retryCount := []byte(strconv.Itoa(int(r.Count)))

	err := util.MetakvSet(retryPath, retryCount, nil)
	if err != nil {
		info.Code = m.statusCodes.errAppRetry.Code
		info.Info = fmt.Sprintf("Unable to set metakv path for retry, err : %v", err)
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) statsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		fmt.Fprintln(w, `{"error":"Request not authorized"}`)
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

func (m *ServiceMgr) populateStats(fullStats bool) []stats {
	statsList := make([]stats, 0)
	for _, app := range m.getTempStoreAll() {
		if m.checkIfDeployed(app.Name) {
			stats := stats{}
			stats.CredsRequestCounter = m.credsCounter
			stats.EventProcessingStats = m.superSup.GetEventProcessingStats(app.Name)
			stats.EventsRemaining = backlogStat{DcpBacklog: m.superSup.GetDcpEventsRemainingToProcess(app.Name)}
			stats.ExecutionStats = m.superSup.GetExecutionStats(app.Name)
			stats.FailureStats = m.superSup.GetFailureStats(app.Name)
			stats.FunctionName = app.Name
			stats.InternalVbDistributionStats = m.superSup.InternalVbDistributionStats(app.Name)
			stats.LcbExceptionStats = m.superSup.GetLcbExceptionsStats(app.Name)
			stats.WorkerPids = m.superSup.GetEventingConsumerPids(app.Name)
			stats.PlannerStats = m.superSup.PlannerStats(app.Name)
			stats.VbDistributionStatsFromMetadata = m.superSup.VbDistributionStatsFromMetadata(app.Name)

			if fullStats {
				checkpointBlobDump, err := m.superSup.CheckpointBlobDump(app.Name)
				if err == nil {
					stats.CheckpointBlobDump = checkpointBlobDump
				}

				stats.LatencyStats = m.superSup.GetLatencyStats(app.Name)

				plasmaStats, err := m.superSup.GetPlasmaStats(app.Name)
				if err == nil {
					stats.PlasmaStats = plasmaStats
				}

				stats.SeqsProcessed = m.superSup.GetSeqsProcessed(app.Name)
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
		fmt.Fprintln(w, `{"error":"Request not authorized"}`)
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
	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintln(w, `{"error":"Request not authorized"}`)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	audit.Log(auditevent.ExportFunctions, r, nil)

	apps := m.getTempStoreAll()
	for _, app := range apps {
		app.Settings["deployment_status"] = false
		app.Settings["processing_status"] = false
	}

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
	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintln(w, `{"error":"Request not authorized"}`)
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
	m.sendRuntimeInfoList(w, infoList)
}

func (m *ServiceMgr) createApplications(r *http.Request, appList *[]application, undeploy bool) (infoList []*runtimeInfo) {
	infoList = []*runtimeInfo{}
	var err error
	for _, app := range *appList {
		audit.Log(auditevent.CreateFunction, r, app.Name)

		if infoVal := m.validateApplication(&app); infoVal.Code != m.statusCodes.ok.Code {
			logging.Warnf("Validating %ru failed: %v", app, infoVal)
			infoList = append(infoList, infoVal)
			continue
		}

		if undeploy {
			app.Settings["deployment_status"] = false
			app.Settings["processing_status"] = false
		}

		app.EventingVersion = util.EventingVer()
		app.HandlerUUID, err = util.GenerateHandlerUUID()
		if err != nil {
			info := &runtimeInfo{}
			info.Code = m.statusCodes.errUUIDGen.Code
			info.Info = fmt.Sprintf("UUID generation failed for handler: %s", app.Name)
			infoList = append(infoList, info)
			continue
		}
		logging.Infof("HandlerUUID generated for handler: %s, UUID: %d", app.Name, app.HandlerUUID)

		infoPri := m.savePrimaryStore(app)
		if infoPri.Code != m.statusCodes.ok.Code {
			logging.Warnf("Saving %ru to primary store failed: %v", app, infoPri)
			infoList = append(infoList, infoPri)
			continue
		}

		// Save to temp store only if saving to primary store succeeds
		audit.Log(auditevent.SaveDraft, r, app.Name)
		infoTmp := m.saveTempStore(app)
		if infoTmp.Code != m.statusCodes.ok.Code {
			logging.Warnf("Saving %ru to temporary store failed: %v", app, infoTmp)
			infoList = append(infoList, infoTmp)
			continue
		}

		// If everything succeeded, use infoPri as that has warnings, if any
		infoList = append(infoList, infoPri)
	}

	return
}

func (m *ServiceMgr) getWorkerCount(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintln(w, `{"error":"Request not authorized"}`)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	count := 0

	apps := m.getTempStoreAll()
	for _, app := range apps {
		if app.Settings["deployment_status"].(bool) != true {
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

func (m *ServiceMgr) checkVersionCompat(required string, info *runtimeInfo) {
	nsServerEndpoint := net.JoinHostPort(util.Localhost(), m.restPort)
	clusterInfo, err := util.FetchNewClusterInfoCache(nsServerEndpoint)
	if err != nil {
		info.Code = m.statusCodes.errConnectNsServer.Code
		info.Info = fmt.Sprintf("Failed to get cluster info cache, err: %v", err)
		return
	}

	ok := false
	major, minor := clusterInfo.GetClusterVersion()
	switch required {
	case "vulcan":
		ok = (major >= 5 && minor >= 5)
	}

	if !ok {
		info.Code = m.statusCodes.errClusterVersion.Code
		info.Info = fmt.Sprintf("Handler requires '%v', but cluster is at '%v.%v'",
			required, major, minor)
		logging.Warnf("Version compat check failed: %v", info.Info)
		return
	}

	logging.Infof("Handler need '%v' satisfied by cluster '%v.%v'", required, major, minor)
	info.Code = m.statusCodes.ok.Code
}
