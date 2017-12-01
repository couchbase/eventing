package servicemanager

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime/trace"
	"time"

	"path"
	"strconv"
	"strings"

	"regexp"

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
	m.deletePrimaryStore(w, appName)
}

// Deletes application from primary store and returns the appropriate success/error code
func (m *ServiceMgr) deletePrimaryStore(w http.ResponseWriter, appName string) int {
	checkIfDeployed := false
	for _, app := range util.ListChildren(metakvAppsPath) {
		if app == appName {
			checkIfDeployed = true
		}
	}

	if !checkIfDeployed {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
		fmt.Fprintf(w, "App: %v not deployed", appName)
		return m.statusCodes.errAppNotDeployed.Code
	}

	appState := m.superSup.GetAppState(appName)
	if appState != common.AppStateUndeployed {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotUndeployed.Code))
		fmt.Fprintf(w, "Skipping delete request from primary store for app: %v as it hasn't been undeployed", appName)
		return m.statusCodes.errAppNotUndeployed.Code
	}

	appList := util.ListChildren(metakvAppsPath)
	for _, app := range appList {
		if app == appName {
			settingsPath := metakvAppSettingsPath + appName
			err := util.MetaKvDelete(settingsPath, nil)
			if err != nil {
				w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errDelAppSettingsPs.Code))
				fmt.Fprintf(w, "Failed to delete setting for app: %v, err: %v", appName, err)
				return m.statusCodes.errDelAppSettingsPs.Code
			}

			appsPath := metakvAppsPath + appName
			err = util.MetaKvDelete(appsPath, nil)
			if err != nil {
				w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errDelAppPs.Code))
				fmt.Fprintf(w, "Failed to delete app definition for app: %v, err: %v", appName, err)
				return m.statusCodes.errDelAppPs.Code
			}

			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
			fmt.Fprintf(w, "Deleting app: %v in the background", appName)
			return m.statusCodes.ok.Code
		}
	}

	return m.statusCodes.ok.Code
}

func (m *ServiceMgr) deleteTempStoreHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	logging.Infof("Deleting drafts")
	audit.Log(auditevent.DeleteDrafts, r, nil)

	values := r.URL.Query()
	appName := values["name"][0]
	m.deleteTempStore(w, appName)
}

// Deletes application from temporary store and returns the appropriate success/error code
func (m *ServiceMgr) deleteTempStore(w http.ResponseWriter, appName string) int {
	checkIfDeployed := false
	for _, app := range util.ListChildren(metakvTempAppsPath) {
		if app == appName {
			checkIfDeployed = true
		}
	}

	if !checkIfDeployed {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
		fmt.Fprintf(w, "App: %v not deployed", appName)
		return m.statusCodes.errAppNotDeployed.Code
	}

	appState := m.superSup.GetAppState(appName)
	if appState != common.AppStateUndeployed {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotUndeployed.Code))
		fmt.Fprintf(w, "Skipping delete request from temp store for app: %v as it hasn't been undeployed", appName)
		return m.statusCodes.errAppNotUndeployed.Code
	}

	tempAppList := util.ListChildren(metakvTempAppsPath)

	for _, tempAppName := range tempAppList {
		if appName == tempAppName {
			path := metakvTempAppsPath + tempAppName
			err := util.MetaKvDelete(path, nil)
			if err != nil {
				w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errDelAppTs.Code))
				fmt.Fprintf(w, "Failed to delete from temp store for %v, err: %v", appName, err)
				return m.statusCodes.errDelAppTs.Code
			}

			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
			fmt.Fprintf(w, "Deleting app: %v in the background", appName)
			return m.statusCodes.ok.Code
		}
	}

	return m.statusCodes.ok.Code
}

func (m *ServiceMgr) getDebuggerURL(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("App: %v got request to get V8 debugger url", appName)

	if m.checkIfDeployed(appName) {
		debugURL := m.superSup.GetDebuggerURL(appName)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%s", debugURL)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
	fmt.Fprintf(w, "App: %v not deployed", appName)
}

func (m *ServiceMgr) getLocalDebugURL(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

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

func (m *ServiceMgr) getTimerHostPortAddrs(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]
	logging.Infof("App: %v got request for timer host port address", appName)

	data, err := m.superSup.AppTimerTransferHostPortAddrs(appName)
	if err == nil {
		buf, err := json.Marshal(data)
		if err != nil {
			fmt.Fprintf(w, "err: %v", err)
			return
		}
		fmt.Fprintf(w, "%v", string(buf))
	}

	fmt.Fprintf(w, "")
}

func (m *ServiceMgr) getAggEventsPSec(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	logging.Debugf("Reading aggregate events processed per second for %v", appName)

	util.Retry(util.NewFixedBackoff(time.Second), getEventingNodesAddressesOpCallback, m)

	pStats, err := util.GetAggProcessedPSec(fmt.Sprintf("/getEventsPSec?name=%s", appName), m.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("Failed to processing stats for app: %v, err: %v", appName, err)
		return
	}

	fmt.Fprintf(w, "%v", pStats)
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

func (m *ServiceMgr) getEventsProcessedPSec(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	if m.checkIfDeployed(appName) {
		producerHostPortAddr := m.superSup.AppProducerHostPortAddr(appName)

		pSec, err := util.GetProcessedPSec("/getEventsPSec", producerHostPortAddr)
		if err != nil {
			logging.Errorf("Failed to capture events processed/sec stat from producer for app: %v on current node, err: %v",
				appName, err)
			return
		}

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
		fmt.Fprintf(w, "%v", pSec)
	} else {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
		fmt.Fprintf(w, "App: %v not deployed", appName)
	}
}

func (m *ServiceMgr) getAggTimerHostPortAddrs(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	util.Retry(util.NewFixedBackoff(time.Second), getEventingNodesAddressesOpCallback, m)

	addrs, err := util.GetTimerHostPortAddrs(fmt.Sprintf("/getTimerHostPortAddrs?name=%s", appName), m.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("Failed to marshal timer hosts for app: %v, err: %v", appName, err)
		return
	}

	fmt.Fprintf(w, "%v", addrs)
}

// Returns list of apps that are deployed i.e. finished dcp/timer/debugger related bootstrap
func (m *ServiceMgr) getDeployedApps(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	logging.Infof("Listing deployed applications")
	audit.Log(auditevent.ListDeployed, r, nil)

	deployedApps := m.superSup.GetDeployedApps()

	buf, err := json.Marshal(deployedApps)
	if err != nil {
		logging.Errorf("Failed to marshal list of deployed apps, err: %v", err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, "")
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s", string(buf))
}

// Reports progress across all producers on current node
func (m *ServiceMgr) getRebalanceProgress(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	producerHostPortAddrs := m.superSup.ProducerHostPortAddrs()

	progress, _ := util.GetProgress("/getRebalanceStatus", producerHostPortAddrs)

	buf, err := json.Marshal(progress)
	if err != nil {
		logging.Errorf("Failed to unmarshal rebalance progress across all producers on current node, err: %v", err)
		return
	}

	w.Write(buf)
}

// Reports aggregated event processing stats from all producers
func (m *ServiceMgr) getAggEventProcessingStats(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	util.Retry(util.NewFixedBackoff(time.Second), getEventingNodesAddressesOpCallback, m)

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

// Reports aggregated rebalance progress from all producers
func (m *ServiceMgr) getAggRebalanceProgress(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	util.Retry(util.NewFixedBackoff(time.Second), getEventingNodesAddressesOpCallback, m)

	aggProgress, _ := util.GetProgress("/getRebalanceProgress", m.eventingNodeAddrs)

	buf, err := json.Marshal(aggProgress)
	if err != nil {
		logging.Errorf("Failed to unmarshal rebalance progress across all producers, err: %v", err)
		return
	}

	w.Write(buf)
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

func (m *ServiceMgr) setSettings(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	logging.Infof("Set settings for app %v", appName)
	audit.Log(auditevent.SetSettings, r, appName)

	path := metakvAppSettingsPath + appName
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		fmt.Fprintf(w, "Failed to read request body, err: %v", err)
		return
	}

	var settings map[string]interface{}
	err = json.Unmarshal(data, &settings)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, "Failed to unmarshal setting supplied, err: %v", err)
		return
	}

	deployedApps := m.superSup.GetDeployedApps()

	processingStatus, pOk := settings["processing_status"].(bool)
	deploymentStatus, dOk := settings["deployment_status"].(bool)

	if pOk && dOk {
		// Check for disable processing
		if deploymentStatus == true && processingStatus == false {
			if _, ok := deployedApps[appName]; !ok {
				w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotInit.Code))
				fmt.Fprintf(w, "App: %v not bootstrapped, discarding request to disable processing for it", appName)
				return
			}
		}

		// Check for undeploy
		if deploymentStatus == false && processingStatus == false {
			if _, ok := deployedApps[appName]; !ok {
				w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotInit.Code))
				fmt.Fprintf(w, "App: %v not bootstrapped, discarding request to undeploy it", appName)
				return
			}
		}
	} else {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errStatusesNotFound.Code))
		fmt.Fprintf(w, "App: %v Missing processing or deployment statuses or both in supplied settings", appName)
		return
	}

	err = util.MetakvSet(path, data, nil)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errSetSettingsPs.Code))
		fmt.Fprintf(w, "Failed to store setting for app: %v, err: %v", appName, err)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "stored settings for app: %v", appName)
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

		path := metakvAppsPath + appName
		data, err := util.MetakvGet(path)
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
		return
	}

	logging.Infof("Fetching function draft definitions")
	audit.Log(auditevent.FetchDrafts, r, nil)
	respData := m.getTempStore("")

	data, err := json.Marshal(respData)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, "Failed to marshal response for getAppTempStore, err: %v", err)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "%s\n", data)
}

// Gets the application from temp store
// If appName is empty, it returns all the applications
func (m *ServiceMgr) getTempStore(appName string) []application {
	tempAppList := util.ListChildren(metakvTempAppsPath)
	applications := make([]application, len(tempAppList))

	for i, name := range tempAppList {
		if appName == "" || appName == name {
			path := metakvTempAppsPath + name
			data, err := util.MetakvGet(path)
			if err == nil {
				var app application
				uErr := json.Unmarshal(data, &app)
				if uErr != nil {
					logging.Errorf("Failed to unmarshal data from metakv, err: %v", uErr)
					continue
				}

				applications[i] = app
			}

			if appName != "" {
				break
			}
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

	logging.Infof("Got request to save handler into temporary store: %v", appName)
	audit.Log(auditevent.SaveDraft, r, appName)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		fmt.Fprintf(w, "Failed to read request body, err: %v", err)
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

	m.saveTempStore(w, app, data)
}

// Saves application to temp store
func (m *ServiceMgr) saveTempStore(w http.ResponseWriter, app application, data []byte) int {
	appName := app.Name
	path := metakvTempAppsPath + appName
	nsServerEndpoint := fmt.Sprintf("127.0.0.1:%s", m.restPort)
	cinfo, err := util.ClusterInfoCache(m.auth, nsServerEndpoint)
	if err != nil {
		logging.Errorf("Failed to initialise cluster info cache, err: %v", err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errConnectNsServer.Code))
		fmt.Fprintf(w, "Failed to connect to cluster manager")
		return m.statusCodes.errConnectNsServer.Code
	}

	isMemcached, err := cinfo.IsMemcached(app.DeploymentConfig.SourceBucket)
	if err != nil {
		logging.Errorf("Failed to check bucket type using cluster info cache, err: %v", err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errBucketTypeCheck.Code))
		fmt.Fprintf(w, "Failed to check if source bucket is memcached")
		return m.statusCodes.errBucketTypeCheck.Code
	}

	if isMemcached {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMemcachedBucket.Code))
		fmt.Fprintf(w, "Source bucket is memcached, should be either couchbase or ephemeral")
		return m.statusCodes.errMemcachedBucket.Code
	}

	err = util.MetakvSet(path, data, nil)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errSaveAppTs.Code))
		fmt.Fprintf(w, "Failed to store handlers for app: %v err: %v", appName, err)
		return m.statusCodes.errSaveAppTs.Code
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "Stored handlers for app: %v", appName)
	return m.statusCodes.ok.Code
}

func (m *ServiceMgr) savePrimaryStoreHandler(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("Saving application %v to primary store", appName)
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

	m.savePrimaryStore(w, app)
}

// Saves application to metakv and returns appropriate success/error code
func (m *ServiceMgr) savePrimaryStore(w http.ResponseWriter, app application) int {
	appName := app.Name
	if m.checkIfDeployed(appName) {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppDeployed.Code))
		fmt.Fprintf(w, "App with same name is already deployed, skipping save request")
		return m.statusCodes.errAppDeployed.Code
	}

	if app.DeploymentConfig.SourceBucket == app.DeploymentConfig.MetadataBucket {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errSrcMbSame.Code))
		fmt.Fprintf(w, "Source bucket same as metadata bucket")
		return m.statusCodes.errSrcMbSame.Code
	}

	nsServerEndpoint := fmt.Sprintf("127.0.0.1:%s", m.restPort)
	cinfo, err := util.ClusterInfoCache(m.auth, nsServerEndpoint)
	if err != nil {
		logging.Errorf("Failed to initialise cluster info cache, err: %v", err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errConnectNsServer.Code))
		fmt.Fprintf(w, "Failed to connect to cluster manager")
		return m.statusCodes.errConnectNsServer.Code
	}

	isMemcached, err := cinfo.IsMemcached(app.DeploymentConfig.SourceBucket)
	if err != nil {
		logging.Errorf("Failed to check bucket type using cluster info cache, err: %v", err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errBucketTypeCheck.Code))
		fmt.Fprintf(w, "Failed to check if source bucket is memcached")
		return m.statusCodes.errBucketTypeCheck.Code
	}

	if isMemcached {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMemcachedBucket.Code))
		fmt.Fprintf(w, "Source bucket is memcached, should be either couchbase or ephemeral")
		return m.statusCodes.errMemcachedBucket.Code
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
	config := cfg.ConfigEnd(builder)

	builder.Finish(config)

	appContent := builder.FinishedBytes()

	c := &consumer.Consumer{}
	info, err := c.SpawnCompilationWorker(app.AppHandlers, string(appContent), appName)
	if err != nil || !info.CompileSuccess {
		res, mErr := json.Marshal(&info)
		if mErr != nil {
			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
			fmt.Fprintf(w, "App: %s Failed to marshal compilation status, err: %v", appName, mErr)
			return m.statusCodes.errMarshalResp.Code
		}

		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errHandlerCompile.Code))
		fmt.Fprintf(w, "%v\n", string(res))
		return m.statusCodes.errHandlerCompile.Code
	}

	settingsPath := metakvAppSettingsPath + appName
	settings := app.Settings

	mData, mErr := json.Marshal(&settings)
	if mErr != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, "App: %s Failed to marshal settings, err: %v", appName, mErr)
		return m.statusCodes.errMarshalResp.Code
	}

	mkvErr := util.MetakvSet(settingsPath, mData, nil)
	if mkvErr != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errSetSettingsPs.Code))
		fmt.Fprintf(w, "App: %s Failed to store updated settings in metakv, err: %v", appName, mkvErr)
		return m.statusCodes.errSetSettingsPs.Code
	}

	path := metakvAppsPath + appName
	err = util.MetakvSet(path, appContent, nil)
	if err != nil {
		logging.Errorf("App: %v failed to write to metakv, err: %v", appName, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errSaveAppPs.Code))
		fmt.Fprintf(w, "Failed to write app config to metakv, err: %v", err)
		return m.statusCodes.errSaveAppPs.Code
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "Stored application config in metakv")
	return m.statusCodes.ok.Code
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

func (m *ServiceMgr) validateAuth(w http.ResponseWriter, r *http.Request, perm string) bool {
	creds, err := cbauth.AuthWebCreds(r)
	if err != nil || creds == nil {
		logging.Warnf("Cannot authenticate request to %v", r.URL)
		w.WriteHeader(401)
		return false
	}
	allowed, err := creds.IsAllowed(perm)
	if err != nil || !allowed {
		logging.Warnf("Cannot authorize request to %v", r.URL)
		w.WriteHeader(403)
		return false
	}
	logging.Debugf("Allowing access to %v", r.URL)
	return true
}

func (m *ServiceMgr) clearEventStats(w http.ResponseWriter, r *http.Request) {
	if !m.validateAuth(w, r, EventingPermissionManage) {
		return
	}

	logging.Infof("Got request to clear event stats from host: %v", r.Host)
	m.superSup.ClearEventStats()
}

func (m *ServiceMgr) getHandler(appName string) string {
	if m.checkIfDeployed(appName) {
		return m.superSup.GetHandlerCode(appName)
	}

	return ""
}

func (m *ServiceMgr) getSourceMap(appName string) string {
	if m.checkIfDeployed(appName) {
		return m.superSup.GetSourceMap(appName)
	}

	return ""
}

func (m *ServiceMgr) checkIfDeployed(appName string) bool {
	deployedApps := m.superSup.DeployedAppList()
	for _, app := range deployedApps {
		if app == appName {
			return true
		}
	}
	return false
}

// Unmarshals list of application and returns application objects
func (m *ServiceMgr) unmarshalAppList(w http.ResponseWriter, r *http.Request) []application {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		fmt.Fprintf(w, "Failed to read request body, err: %v", err)
		return nil
	}

	var appList []application
	err = json.Unmarshal(data, &appList)
	if err != nil {
		errString := fmt.Sprintf("Failed to unmarshal payload err: %v", err)
		logging.Errorf("%s", errString)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errUnmarshalPld.Code))
		fmt.Fprintf(w, "%s\n", errString)
		return nil
	}

	return appList
}

func (m *ServiceMgr) functionsHandler(w http.ResponseWriter, r *http.Request) {
	functions := regexp.MustCompile("^/api/v1/functions/?$")
	functionsSettings := regexp.MustCompile("^/api/v1/functions/settings/?$")
	functionsName := regexp.MustCompile("^/api/v1/functions/(.+)/?$")
	functionsNameSettings := regexp.MustCompile("^/api/v1/functions/(.+)/settings/?$")

	if match := functionsSettings.FindStringSubmatch(r.URL.Path); len(match) != 0 {
	} else if match := functionsNameSettings.FindStringSubmatch(r.URL.Path); len(match) != 0 {
	} else if match := functionsName.FindStringSubmatch(r.URL.Path); len(match) != 0 {
	} else if match := functions.FindStringSubmatch(r.URL.Path); len(match) != 0 {
		switch r.Method {
		case "GET":
			m.getTempStoreHandler(w, r)
			break

		case "POST":
			appList := m.unmarshalAppList(w, r)
			for _, app := range appList {
				data, err := json.Marshal(app)
				if err != nil {
					w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
					fmt.Fprintf(w, "Failed to marshal response, err: %v", err)
					return
				}

				// Save to temp store only if saving to primary store succeeds
				if m.savePrimaryStore(w, app) == m.statusCodes.ok.Code {
					m.saveTempStore(w, app, data)
				}
			}
			break

		case "DELETE":
			for _, app := range m.getTempStore("") {
				code := m.deletePrimaryStore(w, app.Name)
				// Delete the application from temp store only if app does not exist in primary store
				// or if the deletion succeeds on primary store
				if code == m.statusCodes.errAppNotDeployed.Code || code == m.statusCodes.ok.Code {
					m.deleteTempStore(w, app.Name)
				}
			}
			break

		}
	}
}

func (m *ServiceMgr) statsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !m.validateAuth(w, r, EventingPermissionManage) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintln(w, "{\"error\":\"Request not authorized\"}")
		return
	}

	// Check whether type=full is present in query
	fullStats := false
	if typeParam := r.URL.Query().Get("type"); typeParam != "" {
		fullStats = typeParam == "full"
	}

	statsList := make([]stats, 0)
	for _, app := range m.getTempStore("") {
		if m.checkIfDeployed(app.Name) {
			stats := stats{}
			stats.FunctionName = app.Name
			stats.LatencyStats = m.superSup.GetLatencyStats(app.Name)
			stats.ExecutionStats = m.superSup.GetExecutionStats(app.Name)
			stats.EventProcessingStats = m.superSup.GetEventProcessingStats(app.Name)
			stats.FailureStats = m.superSup.GetFailureStats(app.Name)
			stats.WorkerPids = m.superSup.GetEventingConsumerPids(app.Name)
			stats.EventsRemaining = backlogStat{DcpBacklog: m.superSup.GetDcpEventsRemainingToProcess(app.Name)}
			if fullStats {
				stats.SeqsProcessed = m.superSup.GetSeqsProcessed(app.Name)
			}

			statsList = append(statsList, stats)
		}
	}

	response, err := json.Marshal(statsList)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{\"error\":\"Failed to marshal response for stats, err: %v\"}", err)
		return
	}

	fmt.Fprintf(w, "%s", string(response))
	return
}
