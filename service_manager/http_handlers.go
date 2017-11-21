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

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/gen/flatbuf/cfg"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	flatbuffers "github.com/google/flatbuffers/go"
)

func (m *ServiceMgr) validateAuth(w http.ResponseWriter, r *http.Request, perm string) bool {
	creds, err := cbauth.AuthWebCreds(r)
	if err != nil || creds == nil {
		logging.Warnf("Cannot authenticate request, rejecting")
		w.WriteHeader(401)
		return false
	}
	allowed, err := creds.IsAllowed(perm)
	if err != nil || !allowed {
		logging.Warnf("Cannot authorize request, rejecting")
		w.WriteHeader(403)
		return false
	}
	return true
}

func (m *ServiceMgr) clearEventStats(w http.ResponseWriter, r *http.Request) {
	logging.Infof("Got request to clear event stats from host: %v", r.Host)
	valid := m.validateAuth(w, r, EventingPermissionWrite)
	if !valid {
		return
	}

	m.superSup.ClearEventStats()
}

func (m *ServiceMgr) startTracer(w http.ResponseWriter, r *http.Request) {
	logging.Infof("Got request to start tracing")
	valid := m.validateAuth(w, r, EventingPermissionWrite)
	if !valid {
		return
	}

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

func (m *ServiceMgr) stopTracer(w http.ResponseWriter, r *http.Request) {
	valid := m.validateAuth(w, r, EventingPermissionWrite)
	if !valid {
		return
	}

	m.stopTracerCh <- struct{}{}
}

func (m *ServiceMgr) getNodeUUID(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "%v", m.uuid)
}

func (m *ServiceMgr) debugging(w http.ResponseWriter, r *http.Request) {
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

func (m *ServiceMgr) deleteApplication(w http.ResponseWriter, r *http.Request) {
	valid := m.validateAuth(w, r, EventingPermissionWrite)
	if !valid {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	checkIfDeployed := false
	for _, app := range util.ListChildren(metakvAppsPath) {
		if app == appName {
			checkIfDeployed = true
		}
	}

	if !checkIfDeployed {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
		fmt.Fprintf(w, "App: %v not deployed", appName)
		return
	}

	appState := m.superSup.GetAppState(appName)
	if appState != common.AppStateUndeployed {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotUndeployed.Code))
		fmt.Fprintf(w, "Skipping delete request from primary store for app: %v as it hasn't been undeployed", appName)
		return
	}

	appList := util.ListChildren(metakvAppsPath)
	for _, app := range appList {
		if app == appName {
			settingsPath := metakvAppSettingsPath + appName
			err := util.MetaKvDelete(settingsPath, nil)
			if err != nil {
				w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errDelAppSettingsPs.Code))
				fmt.Fprintf(w, "Failed to delete setting for app: %v, err: %v", appName, err)
				return
			}

			appsPath := metakvAppsPath + appName
			err = util.MetaKvDelete(appsPath, nil)
			if err != nil {
				w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errDelAppPs.Code))
				fmt.Fprintf(w, "Failed to delete app definition for app: %v, err: %v", appName, err)
				return
			}

			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
			fmt.Fprintf(w, "Deleting app: %v in the background", appName)
			return
		}
	}
}

func (m *ServiceMgr) deleteAppTempStore(w http.ResponseWriter, r *http.Request) {
	valid := m.validateAuth(w, r, EventingPermissionWrite)
	if !valid {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	checkIfDeployed := false
	for _, app := range util.ListChildren(metakvTempAppsPath) {
		if app == appName {
			checkIfDeployed = true
		}
	}

	if !checkIfDeployed {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotDeployed.Code))
		fmt.Fprintf(w, "App: %v not deployed", appName)
		return
	}

	appState := m.superSup.GetAppState(appName)
	if appState != common.AppStateUndeployed {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppNotUndeployed.Code))
		fmt.Fprintf(w, "Skipping delete request from temp store for app: %v as it hasn't been undeployed", appName)
		return
	}

	tempAppList := util.ListChildren(metakvTempAppsPath)

	for _, tempAppName := range tempAppList {
		if appName == tempAppName {
			path := metakvTempAppsPath + tempAppName
			err := util.MetaKvDelete(path, nil)
			if err != nil {
				w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errDelAppTs.Code))
				fmt.Fprintf(w, "Failed to delete from temp store for %v, err: %v", appName, err)
				return
			}

			w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
			fmt.Fprintf(w, "Deleting app: %v in the background", appName)
			return
		}
	}
}

func (m *ServiceMgr) getDebuggerURL(w http.ResponseWriter, r *http.Request) {
	valid := m.validateAuth(w, r, EventingPermissionWrite)
	if !valid {
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

func (m *ServiceMgr) getLocalDebuggerURL(w http.ResponseWriter, r *http.Request) {
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
	valid := m.validateAuth(w, r, EventingPermissionWrite)
	if !valid {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("App: %v got request to start V8 debugger", appName)

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
	valid := m.validateAuth(w, r, EventingPermissionWrite)
	if !valid {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("App: %v got request to stop V8 debugger", appName)

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
	values := r.URL.Query()
	appName := values["name"][0]

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

func (m *ServiceMgr) getAggEventsProcessedPSec(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	appName := values["name"][0]

	util.Retry(util.NewFixedBackoff(time.Second), getEventingNodesAddressesOpCallback, m)

	pStats, err := util.GetAggProcessedPSec(fmt.Sprintf("/getEventsPSec?name=%s", appName), m.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("Failed to processing stats for app: %v, err: %v", appName, err)
		return
	}

	fmt.Fprintf(w, "%v", pStats)
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

func (m *ServiceMgr) getEventProcessingStats(w http.ResponseWriter, r *http.Request) {
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
	valid := m.validateAuth(w, r, EventingPermissionRead)
	if !valid {
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
	valid := m.validateAuth(w, r, EventingPermissionRead)
	if !valid {
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
	valid := m.validateAuth(w, r, EventingPermissionRead)
	if !valid {
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
	valid := m.validateAuth(w, r, EventingPermissionRead)
	if !valid {
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

func (m *ServiceMgr) storeAppSettings(w http.ResponseWriter, r *http.Request) {
	valid := m.validateAuth(w, r, EventingPermissionWrite)
	if !valid {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

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

func (m *ServiceMgr) fetchAppSetup(w http.ResponseWriter, r *http.Request) {
	valid := m.validateAuth(w, r, EventingPermissionRead)
	if !valid {
		return
	}

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

func (m *ServiceMgr) fetchAppTempStore(w http.ResponseWriter, r *http.Request) {
	valid := m.validateAuth(w, r, EventingPermissionRead)
	if !valid {
		return
	}

	tempAppList := util.ListChildren(metakvTempAppsPath)
	respData := make([]application, len(tempAppList))

	for index, appName := range tempAppList {
		path := metakvTempAppsPath + appName
		data, err := util.MetakvGet(path)
		if err == nil {
			var app application
			uErr := json.Unmarshal(data, &app)
			if uErr != nil {
				logging.Errorf("Failed to unmarshal settings data from metakv, err: %v", uErr)
				continue
			}

			respData[index] = app
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

func (m *ServiceMgr) saveAppSetup(w http.ResponseWriter, r *http.Request) {
	valid := m.validateAuth(w, r, EventingPermissionWrite)
	if !valid {
		return
	}

	params := r.URL.Query()
	appName := params["name"][0]

	logging.Infof("Got request to save handlers for: %v", appName)

	path := metakvTempAppsPath + appName
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

	nsServerEndpoint := fmt.Sprintf("127.0.0.1:%s", m.restPort)
	cinfo, err := util.ClusterInfoCache(m.auth, nsServerEndpoint)
	if err != nil {
		logging.Errorf("Failed to initialise cluster info cache, err: %v", err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errConnectNsServer.Code))
		fmt.Fprintf(w, "Failed to connect to cluster manager")
		return
	}

	isMemcached, err := cinfo.IsMemcached(app.DeploymentConfig.SourceBucket)
	if err != nil {
		logging.Errorf("Failed to check bucket type using cluster info cache, err: %v", err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errBucketTypeCheck.Code))
		fmt.Fprintf(w, "Failed to check if source bucket is memcached")
		return
	}

	if isMemcached {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMemcachedBucket.Code))
		fmt.Fprintf(w, "Source bucket is memcached, should be either couchbase or ephemeral")
		return
	}

	err = util.MetakvSet(path, data, nil)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errSaveAppTs.Code))
		fmt.Fprintf(w, "Failed to store handlers for app: %v err: %v", appName, err)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "Stored handlers for app: %v", appName)
}

func (m *ServiceMgr) storeAppSetup(w http.ResponseWriter, r *http.Request) {
	valid := m.validateAuth(w, r, EventingPermissionWrite)
	if !valid {
		return
	}

	values := r.URL.Query()
	appName := values["name"][0]

	if m.checkIfDeployed(appName) {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errAppDeployed.Code))
		fmt.Fprintf(w, "App with same name is already deployed, skipping save request")
		return
	}

	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errString := fmt.Sprintf("App: %s, failed to read content from http request body", appName)
		logging.Errorf("%s, err: %v", errString, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errReadReq.Code))
		fmt.Fprintf(w, "%s\n", errString)
		return
	}

	var app application
	err = json.Unmarshal(content, &app)
	if err != nil {
		errString := fmt.Sprintf("App: %s, Failed to unmarshal payload", appName)
		logging.Errorf("%s, err: %v", errString, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errUnmarshalPld.Code))
		fmt.Fprintf(w, "%s\n", errString)
		return
	}

	if app.DeploymentConfig.SourceBucket == app.DeploymentConfig.MetadataBucket {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errSrcMbSame.Code))
		fmt.Fprintf(w, "Source bucket same as metadata bucket")
		return
	}

	nsServerEndpoint := fmt.Sprintf("127.0.0.1:%s", m.restPort)
	cinfo, err := util.ClusterInfoCache(m.auth, nsServerEndpoint)
	if err != nil {
		logging.Errorf("Failed to initialise cluster info cache, err: %v", err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errConnectNsServer.Code))
		fmt.Fprintf(w, "Failed to connect to cluster manager")
		return
	}

	isMemcached, err := cinfo.IsMemcached(app.DeploymentConfig.SourceBucket)
	if err != nil {
		logging.Errorf("Failed to check bucket type using cluster info cache, err: %v", err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errBucketTypeCheck.Code))
		fmt.Fprintf(w, "Failed to check if source bucket is memcached")
		return
	}

	if isMemcached {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMemcachedBucket.Code))
		fmt.Fprintf(w, "Source bucket is memcached, should be either couchbase or ephemeral")
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
	config := cfg.ConfigEnd(builder)

	builder.Finish(config)

	appContent := builder.FinishedBytes()

	settingsPath := metakvAppSettingsPath + appName
	settings := app.Settings

	mData, mErr := json.Marshal(&settings)
	if mErr != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, "App: %s Failed to marshal settings, err: %v", appName, mErr)
		return
	}

	mkvErr := util.MetakvSet(settingsPath, mData, nil)
	if mkvErr != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errSetSettingsPs.Code))
		fmt.Fprintf(w, "App: %s Failed to store updated settings in metakv, err: %v", appName, mkvErr)
		return
	}

	path := metakvAppsPath + appName
	err = util.MetakvSet(path, appContent, nil)
	if err != nil {
		logging.Errorf("App: %v failed to write to metakv, err: %v", appName, err)
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errSaveAppPs.Code))
		fmt.Fprintf(w, "Failed to write app config to metakv, err: %v", err)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.ok.Code))
	fmt.Fprintf(w, "Stored application config in metakv")
}

func (m *ServiceMgr) getErrCodes(w http.ResponseWriter, r *http.Request) {
	valid := m.validateAuth(w, r, EventingPermissionRead)
	if !valid {
		return
	}

	w.Write(m.statusPayload)
}

func (m *ServiceMgr) getDcpEventsRemaining(w http.ResponseWriter, r *http.Request) {
	valid := m.validateAuth(w, r, EventingPermissionRead)
	if !valid {
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
	valid := m.validateAuth(w, r, EventingPermissionRead)
	if !valid {
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
