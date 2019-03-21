package servicemanager

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/gen/flatbuf/cfg"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

func (m *ServiceMgr) checkIfDeployed(appName string) bool {
	deployedApps := m.superSup.DeployedAppList()
	for _, app := range deployedApps {
		if app == appName {
			return true
		}
	}
	return false
}

func decodeRev(b service.Revision) uint64 {
	return binary.BigEndian.Uint64(b)
}

func encodeRev(rev uint64) service.Revision {
	ext := make(service.Revision, 8)
	binary.BigEndian.PutUint64(ext, rev)

	return ext
}

func fillMissingWithDefaults(settings map[string]interface{}) {
	// Handler related configurations
	fillMissingDefault(settings, "checkpoint_interval", float64(60000))
	fillMissingDefault(settings, "cleanup_timers", false)
	fillMissingDefault(settings, "cpp_worker_thread_count", float64(2))
	fillMissingDefault(settings, "deadline_timeout", float64(62))
	fillMissingDefault(settings, "execution_timeout", float64(60))
	fillMissingDefault(settings, "feedback_batch_size", float64(100))
	fillMissingDefault(settings, "feedback_read_buffer_size", float64(65536))
	fillMissingDefault(settings, "idle_checkpoint_interval", float64(30000))
	fillMissingDefault(settings, "lcb_inst_capacity", float64(5))
	fillMissingDefault(settings, "log_level", "INFO")
	fillMissingDefault(settings, "poll_bucket_interval", float64(10))
	fillMissingDefault(settings, "sock_batch_size", float64(100))
	fillMissingDefault(settings, "tick_duration", float64(60000))
	fillMissingDefault(settings, "timer_context_size", float64(1024))
	fillMissingDefault(settings, "undeploy_routine_count", float64(6))
	fillMissingDefault(settings, "worker_count", float64(3))
	fillMissingDefault(settings, "worker_feedback_queue_cap", float64(500))
	fillMissingDefault(settings, "worker_queue_cap", float64(100*1000))
	fillMissingDefault(settings, "worker_queue_mem_cap", float64(1024))
	fillMissingDefault(settings, "worker_response_timeout", float64(300))

	// metastore related configuration
	fillMissingDefault(settings, "execute_timer_routine_count", float64(3))
	fillMissingDefault(settings, "timer_storage_routine_count", float64(3))
	fillMissingDefault(settings, "timer_storage_chan_size", float64(10*1000))
	fillMissingDefault(settings, "timer_queue_mem_cap", float64(50))
	fillMissingDefault(settings, "timer_queue_size", float64(10000))

	// Process related configuration
	fillMissingDefault(settings, "breakpad_on", true)

	// Rebalance related configurations
	fillMissingDefault(settings, "vb_ownership_giveup_routine_count", float64(3))
	fillMissingDefault(settings, "vb_ownership_takeover_routine_count", float64(3))

	// Application logging related configurations
	fillMissingDefault(settings, "app_log_max_size", float64(1024*1024*40))
	fillMissingDefault(settings, "app_log_max_files", float64(10))
	fillMissingDefault(settings, "enable_applog_rotation", true)

	// DCP connection related configurations
	fillMissingDefault(settings, "agg_dcp_feed_mem_cap", float64(1024))
	fillMissingDefault(settings, "data_chan_size", float64(50))
	fillMissingDefault(settings, "dcp_gen_chan_size", float64(10000))
	fillMissingDefault(settings, "dcp_num_connections", float64(1))
}

func fillMissingDefault(settings map[string]interface{}, field string, defaultValue interface{}) {
	if _, ok := settings[field]; !ok {
		settings[field] = defaultValue
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

func (m *ServiceMgr) sendErrorInfo(w http.ResponseWriter, runtimeInfo *runtimeInfo) {
	errInfo := m.errorCodes[runtimeInfo.Code]
	errInfo.RuntimeInfo = *runtimeInfo
	response, err := json.Marshal(errInfo)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, `{"error":"Failed to marshal error info, err: %v"}`, err)
		return
	}

	if runtimeInfo.Code != m.statusCodes.ok.Code {
		w.WriteHeader(m.getDisposition(runtimeInfo.Code))
	}

	w.Header().Add(headerKey, strconv.Itoa(errInfo.Code))
	fmt.Fprintf(w, string(response))
}

func (m *ServiceMgr) sendRuntimeInfo(w http.ResponseWriter, runtimeInfo *runtimeInfo) {
	if runtimeInfo.Code != m.statusCodes.ok.Code {
		m.sendErrorInfo(w, runtimeInfo)
		return
	}

	response, err := json.Marshal(runtimeInfo)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, `{"error":"Failed to marshal error info, err: %v"}`, err)
		return
	}

	fmt.Fprintf(w, string(response))
}

func (m *ServiceMgr) sendRuntimeInfoList(w http.ResponseWriter, runtimeInfoList []*runtimeInfo) {
	response, err := json.Marshal(runtimeInfoList)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, `{"error":"Failed to marshal error info, err: %v"}`, err)
		return
	}

	allOK := true
	allFail := true
	for _, info := range runtimeInfoList {
		allOK = allOK && (info.Code == m.statusCodes.ok.Code)
		allFail = allFail && (info.Code != m.statusCodes.ok.Code)
	}

	if allOK {
		w.WriteHeader(http.StatusOK)
	} else if allFail {
		w.WriteHeader(http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusMultiStatus)
	}

	fmt.Fprintf(w, string(response))
}

func (m *ServiceMgr) unmarshalApp(r *http.Request) (app application, info *runtimeInfo) {
	logPrefix := "ServiceMgr::unmarshalApp"
	info = &runtimeInfo{}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		info.Code = m.statusCodes.errReadReq.Code
		info.Info = fmt.Sprintf("Failed to read request body, err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	err = json.Unmarshal(data, &app)
	if err != nil {
		info.Code = m.statusCodes.errUnmarshalPld.Code
		info.Info = fmt.Sprintf("Failed to unmarshal payload err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	info.Code = m.statusCodes.ok.Code
	info.Info = "OK"
	return
}

// Unmarshals list of application and returns application objects
func (m *ServiceMgr) unmarshalAppList(w http.ResponseWriter, r *http.Request) (appList *[]application, info *runtimeInfo) {
	logPrefix := "ServiceMgr::unmarshalAppList"
	appList = &[]application{}
	info = &runtimeInfo{}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		info.Code = m.statusCodes.errReadReq.Code
		info.Info = fmt.Sprintf("Failed to read request body, err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	err = json.Unmarshal(data, &appList)
	if err != nil {
		info.Code = m.statusCodes.errUnmarshalPld.Code
		info.Info = fmt.Sprintf("Failed to unmarshal payload err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) checkLifeCycleOpsDuringRebalance() (info *runtimeInfo) {
	logPrefix := "ServiceMgr:enableLifeCycleOpsDuringRebalance"

	info = &runtimeInfo{}
	var lifeCycleOpsDuringReb bool

	config, configInfo := m.getConfig()
	if configInfo.Code != m.statusCodes.ok.Code {
		lifeCycleOpsDuringReb = false
	} else {
		if enableVal, exists := config["enable_lifecycle_ops_during_rebalance"]; !exists {
			lifeCycleOpsDuringReb = false
		} else {
			enable, ok := enableVal.(bool)
			if !ok {
				logging.Infof("%s [%d] Supplied enable_lifecycle_ops_during_rebalance value unexpected. Defaulting to false", logPrefix)
				enable = false
			}
			lifeCycleOpsDuringReb = enable
		}
	}

	if rebStatus := m.checkRebalanceStatus(); !lifeCycleOpsDuringReb && rebStatus.Code != m.statusCodes.ok.Code {
		info.Code = rebStatus.Code
		info.Info = rebStatus.Info
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) getSourceBinding(cfg *depCfg) *bucket {
	for _, binding := range cfg.Buckets {
		if binding.BucketName == cfg.SourceBucket && binding.Access == "rw" {
			return &binding
		}
	}
	return nil
}

func (m *ServiceMgr) getSourceBindingFromFlatBuf(config *cfg.DepCfg) *cfg.Bucket {
	binding := new(cfg.Bucket)
	for idx := 0; idx < config.BucketsLength(); idx++ {
		if config.Buckets(binding, idx) {
			if string(binding.BucketName()) == string(config.SourceBucket()) && string(binding.Access()) == "rw" {
				return binding
			}
		}
	}
	return nil
}

func (m *ServiceMgr) isSrcMutationEnabled(cfg *depCfg) bool {
	for _, binding := range cfg.Buckets {
		if binding.BucketName == cfg.SourceBucket && binding.Access == "rw" {
			return true
		}
	}
	return false
}

func (m *ServiceMgr) isAppDeployable(app *application) bool {
	appSrcBinding := m.getSourceBinding(&app.DeploymentConfig)
	if appSrcBinding == nil {
		return true
	}
	for _, appName := range m.superSup.DeployedAppList() {
		if appName == app.Name {
			continue
		}
		data, err := util.ReadAppContent(metakvAppsPath, metakvChecksumPath, appName)
		if err != nil {
			return false
		}
		appdata := cfg.GetRootAsConfig(data, 0)
		config := new(cfg.DepCfg)
		depcfg := appdata.DepCfg(config)
		if app.DeploymentConfig.SourceBucket == string(depcfg.SourceBucket()) {
			binding := m.getSourceBindingFromFlatBuf(depcfg)
			if binding != nil {
				return false
			}
		}
	}
	return true
}

func (m *ServiceMgr) getSourceAndDestinationsFromDepCfg(cfg *depCfg) (src string, dest map[string]struct{}) {
	dest = make(map[string]struct{})
	src = cfg.SourceBucket
	dest[cfg.MetadataBucket] = struct{}{}
	for idx := 0; idx < len(cfg.Buckets); idx++ {
		bucketName := cfg.Buckets[idx].BucketName
		if bucketName != src && cfg.Buckets[idx].Access == "rw" {
			dest[bucketName] = struct{}{}
		}
	}
	return src, dest
}

var metakvSetCallback = func(args ...interface{}) error {
	logPrefix := "ServiceMgr::metakvSetCallback"

	metakvPath := args[0].(string)
	data := args[1].([]byte)

	err := util.MetakvSet(metakvPath, data, nil)
	if err != nil {
		logging.Errorf("%s metakv set failed, err: %v", logPrefix, err)
	}
	return err
}

// GetNodesHostname returns hostnames of all nodes
func GetNodesHostname(data map[string]interface{}) []string {
	hostnames := make([]string, 0)

	nodes, exists := data["nodes"].([]interface{})
	if !exists {
		return hostnames
	}
	for _, value := range nodes {
		nodeInfo := value.(map[string]interface{})
		if hostname, exists := nodeInfo["hostname"].(string); exists {
			hostnames = append(hostnames, hostname)
		}
	}
	return hostnames
}
