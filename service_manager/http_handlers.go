package servicemanager

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/couchbase/eventing/flatbuf/cfg"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
	flatbuffers "github.com/google/flatbuffers/go"
)

func (m *ServiceMgr) clearEventStats(w http.ResponseWriter, r *http.Request) {
	logging.Infof("Got request to clear event stats from host: %v", r.Host)

	m.superSup.ClearEventStats()
}

func (m *ServiceMgr) getNodeUUID(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "%v", m.uuid)
}

func (m *ServiceMgr) deleteApplication(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	appName := values["name"][0]

	appList := m.superSup.DeployedAppList()
	for _, app := range appList {
		if app == appName {
			settingsPath := metakvAppSettingsPath + appName
			err := util.MetaKvDelete(settingsPath, nil)
			if err != nil {
				fmt.Fprintf(w, "Failed to delete setting for app: %v, err: %v", appName, err)
				return
			}

			appsPath := metakvAppsPath + appName
			err = util.MetaKvDelete(appsPath, nil)
			if err != nil {
				fmt.Fprintf(w, "Failed to delete app definition for app: %v, err: %v", appName, err)
				return
			}

			fmt.Fprintf(w, "Deleting app: %v in the background", appName)
			return
		}
	}

	fmt.Fprintf(w, "App: %v not deployed", appName)
}

func (m *ServiceMgr) getDebuggerURL(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("App: %v got request to get V8 debugger url", appName)

	appList := m.superSup.DeployedAppList()
	for _, app := range appList {
		if app == appName {
			debugURL := m.superSup.GetDebuggerURL(appName)

			fmt.Fprintf(w, "%s", debugURL)
			return
		}
	}

	fmt.Fprintf(w, "App: %v not deployed", appName)
}

func (m *ServiceMgr) getLocalDebuggerURL(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("App: %v got request to get local V8 debugger url", appName)

	dir, err := os.Getwd()
	if err != nil {
		logging.Infof("App: %v failed to get current working dir, err: %v", appName, err)
		fmt.Fprintf(w, "")
		return
	}

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
	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("App: %v got request to start V8 debugger", appName)

	appList := m.superSup.DeployedAppList()
	for _, app := range appList {
		if app == appName {
			m.superSup.SignalStartDebugger(appName)

			fmt.Fprintf(w, "App: %v Started Debugger", appName)
			return
		}
	}

	fmt.Fprintf(w, "App: %v not deployed", appName)
}

func (m *ServiceMgr) stopDebugger(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	appName := values["name"][0]

	logging.Infof("App: %v got request to stop V8 debugger", appName)

	appList := m.superSup.DeployedAppList()
	for _, app := range appList {
		if app == appName {
			m.superSup.SignalStopDebugger(appName)

			fmt.Fprintf(w, "App: %v Stopped Debugger", appName)
			return
		}
	}

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

func (m *ServiceMgr) getEventsProcessedPSec(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	appName := values["name"][0]

	producerHostPortAddr := m.superSup.AppProducerHostPortAddr(appName)

	pSec, err := util.GetProcessedPSec("/getEventsPSec", producerHostPortAddr)
	if err != nil {
		logging.Errorf("Failed to capture events processed/sec stat from producer for app: %v on current node, err: %v",
			appName, err)
		return
	}

	fmt.Fprintf(w, "%v", pSec)
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

func (m *ServiceMgr) storeAppSettings(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	appName := params["name"][0]

	path := metakvAppSettingsPath + appName
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintf(w, "Failed to read request body, err: %v", err)
		return
	}

	err = util.MetakvSet(path, data, nil)
	if err != nil {
		fmt.Fprintf(w, "Failed to store setting for app: %v, err: %v", appName, err)
		return
	}

	fmt.Fprintf(w, "stored settings for app: %v", appName)
}

func (m *ServiceMgr) fetchAppSetup(w http.ResponseWriter, r *http.Request) {
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
		fmt.Fprintf(w, "Failed to marshal response for get_application, err: %v", err)
		return
	}
	fmt.Fprintf(w, "%s\n", data)
}

func (m *ServiceMgr) storeAppSetup(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	appName := values["name"][0]

	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errString := fmt.Sprintf("App: %s, failed to read content from http request body", appName)
		logging.Errorf("%s, err: %v", errString, err)
		fmt.Fprintf(w, "%s\n", errString)
		return
	}

	var app application
	err = json.Unmarshal(content, &app)
	if err != nil {
		errString := fmt.Sprintf("App: %s, Failed to unmarshal payload", appName)
		logging.Errorf("%s, err: %v", errString, err)
		fmt.Fprintf(w, "%s\n", errString)
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
		fmt.Fprintf(w, "App: %s Failed to marshal settings, err: %v", appName, mErr)
		return
	}

	mkvErr := util.MetakvSet(settingsPath, mData, nil)
	if mkvErr != nil {
		fmt.Fprintf(w, "App: %s Failed to store updated settings in metakv, err: %v", appName, mkvErr)
		return
	}

	path := metakvAppsPath + appName
	err = util.MetakvSet(path, appContent, nil)
	if err != nil {
		fmt.Fprintf(w, "Failed to write app config to metakv, err: %v", err)
		return
	}

	fmt.Fprintf(w, "Stored application config in metakv")
}
