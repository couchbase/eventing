package servicemanager

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/gen/flatbuf/cfg"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

func (m *ServiceMgr) checkAppExists(appName string) bool {
	_, info := m.getTempStore(appName)
	if info.Code == m.statusCodes.errAppNotFoundTs.Code {
		return false
	}
	return true
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

func (m *ServiceMgr) checkIfDeployedAndRunning(appName string) bool {
	mhVersion := eventingVerMap["mad-hatter"]
	if m.compareEventingVersion(mhVersion) {
		logPrefix := "ServiceMgr::CheckIfDeployedAndRunning"
		bootstrapStatus, err := util.GetAggBootstrapAppStatus(net.JoinHostPort(util.Localhost(), m.adminHTTPPort), appName)
		if err != nil {
			logging.Errorf("%s %s", logPrefix, err)
			return false
		}

		if bootstrapStatus {
			return false
		}

		return m.superSup.GetAppState(appName) == common.AppStateEnabled
	}
	bootstrappingApps := m.superSup.BootstrapAppList()
	_, isBootstrapping := bootstrappingApps[appName]

	return !isBootstrapping && m.superSup.GetAppState(appName) == common.AppStateEnabled
}

func (m *ServiceMgr) checkCompressHandler() bool {
	mhVersion := eventingVerMap["mad-hatter"]
	config, info := m.getConfig()
	if info.Code != m.statusCodes.ok.Code {
		return m.compareEventingVersion(mhVersion)
	}

	// In Mad-Hatter,eventing handler will be compressed by default
	// It can be turned off by setting force_compress to false
	if val, exists := config["force_compress"]; exists {
		return val.(bool) && m.compareEventingVersion(mhVersion)
	}

	return m.compareEventingVersion(mhVersion)
}

func decodeRev(b service.Revision) uint64 {
	return binary.BigEndian.Uint64(b)
}

func encodeRev(rev uint64) service.Revision {
	ext := make(service.Revision, 8)
	binary.BigEndian.PutUint64(ext, rev)

	return ext
}

func (m *ServiceMgr) fillMissingWithDefaults(appName string, settings map[string]interface{}) {
	// Fill from temp store if available
	app, _ := m.getTempStore(appName)

	// Handler related configurations
	fillMissingDefault(app, settings, "n1ql_prepare_all", true)
	fillMissingDefault(app, settings, "checkpoint_interval", float64(60000))
	fillMissingDefault(app, settings, "cleanup_timers", false)
	fillMissingDefault(app, settings, "cpp_worker_thread_count", float64(2))
	fillMissingDefault(app, settings, "deadline_timeout", float64(62))
	fillMissingDefault(app, settings, "execution_timeout", float64(60))
	fillMissingDefault(app, settings, "feedback_batch_size", float64(100))
	fillMissingDefault(app, settings, "feedback_read_buffer_size", float64(65536))
	fillMissingDefault(app, settings, "idle_checkpoint_interval", float64(30000))
	fillMissingDefault(app, settings, "lcb_inst_capacity", float64(5))
	fillMissingDefault(app, settings, "log_level", "INFO")
	fillMissingDefault(app, settings, "poll_bucket_interval", float64(10))
	fillMissingDefault(app, settings, "sock_batch_size", float64(100))
	fillMissingDefault(app, settings, "tick_duration", float64(60000))
	fillMissingDefault(app, settings, "timer_context_size", float64(1024))
	fillMissingDefault(app, settings, "undeploy_routine_count", float64(6))
	fillMissingDefault(app, settings, "worker_count", float64(3))
	fillMissingDefault(app, settings, "worker_feedback_queue_cap", float64(500))
	fillMissingDefault(app, settings, "worker_queue_cap", float64(100*1000))
	fillMissingDefault(app, settings, "worker_queue_mem_cap", float64(1024))
	fillMissingDefault(app, settings, "worker_response_timeout", float64(3600))

	// metastore related configuration
	fillMissingDefault(app, settings, "execute_timer_routine_count", float64(3))
	fillMissingDefault(app, settings, "timer_storage_routine_count", float64(3))
	fillMissingDefault(app, settings, "timer_storage_chan_size", float64(10*1000))
	fillMissingDefault(app, settings, "timer_queue_mem_cap", float64(50))
	fillMissingDefault(app, settings, "timer_queue_size", float64(10000))

	// Process related configuration
	fillMissingDefault(app, settings, "breakpad_on", true)

	// Rebalance related configurations
	fillMissingDefault(app, settings, "vb_ownership_giveup_routine_count", float64(3))
	fillMissingDefault(app, settings, "vb_ownership_takeover_routine_count", float64(3))

	// Application logging related configurations
	fillMissingDefault(app, settings, "app_log_max_size", float64(1024*1024*40))
	fillMissingDefault(app, settings, "app_log_max_files", float64(10))
	fillMissingDefault(app, settings, "enable_applog_rotation", true)

	// DCP connection related configurations
	fillMissingDefault(app, settings, "agg_dcp_feed_mem_cap", float64(1024))
	fillMissingDefault(app, settings, "data_chan_size", float64(50))
	fillMissingDefault(app, settings, "dcp_gen_chan_size", float64(10000))
	fillMissingDefault(app, settings, "dcp_num_connections", float64(1))

	// N1QL related configuration
	fillMissingDefault(app, settings, "n1ql_consistency", "none")

	// Language related configuration
	fillMissingDefault(app, settings, "language_compatibility", common.LanguageCompatibility[0])
}

func fillMissingDefault(app application, settings map[string]interface{}, field string, defaultValue interface{}) {
	if _, ok := settings[field]; !ok {
		if _, tOk := app.Settings[field]; !tOk {
			settings[field] = defaultValue
			return
		}
		settings[field] = app.Settings[field]
	}
}

func (m *ServiceMgr) sendErrorInfo(w http.ResponseWriter, runtimeInfo *runtimeInfo) {
	errInfo := m.errorCodes[runtimeInfo.Code]
	errInfo.RuntimeInfo = *runtimeInfo
	response, err := json.MarshalIndent(errInfo, "", " ")
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

	response, err := json.MarshalIndent(runtimeInfo, "", " ")
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, `{"error":"Failed to marshal error info, err: %v"}`, err)
		return
	}

	fmt.Fprintf(w, string(response))
}

func (m *ServiceMgr) sendRuntimeInfoList(w http.ResponseWriter, runtimeInfoList []*runtimeInfo) {
	response, err := json.MarshalIndent(runtimeInfoList, "", " ")
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

func (m *ServiceMgr) getSourceBindingFromFlatBuf(config *cfg.DepCfg, appdata *cfg.Config) *cfg.Bucket {
	binding := new(cfg.Bucket)
	for idx := 0; idx < config.BucketsLength(); idx++ {
		if config.Buckets(binding, idx) {
			if string(binding.BucketName()) == string(config.SourceBucket()) && string(appdata.Access(idx)) == "rw" {
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
			binding := m.getSourceBindingFromFlatBuf(depcfg, appdata)
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

func (m *ServiceMgr) UpdateBucketGraphFromMetakv(functionName string) error {
	logPrefix := "ServiceMgr::UpdateBucketGraphFromMektakv"
	appData, err := util.ReadAppContent(metakvAppsPath, metakvChecksumPath, functionName)
	if err != nil {
		logging.Errorf("%s Function read from metakv failed, err: %v", logPrefix, err)
		return err
	}
	app := m.parseFunctionPayload(appData, functionName)
	source, destinations := m.getSourceAndDestinationsFromDepCfg(&app.DeploymentConfig)
	if len(destinations) != 0 {
		m.graph.insertEdges(functionName, source, destinations)
	}
	return nil
}

func (m *ServiceMgr) validateQueryKey(query url.Values) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.ok.Code

	for key := range query {
		if _, found := functionQueryKeys[key]; !found {
			info.Info = "key mismatch error, supported keys in function list query are: source_bucket, function_type, deployed"
			info.Code = m.statusCodes.errReadReq.Code
			return
		}
	}
	return
}

func (m *ServiceMgr) validateQueryBucket(query url.Values) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.ok.Code
	buckets, found := query["source_bucket"]
	if !found {
		return
	}
	bucketLen := len(buckets)
	if bucketLen > 1 {
		info.Info = "more than one bucket name present in function list query"
		info.Code = m.statusCodes.errReadReq.Code
		return
	}
	return
}

func (m *ServiceMgr) validateQueryFunctionType(query url.Values) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.ok.Code

	functionTypes, found := query["function_type"]
	if !found {
		return
	}
	typeLen := len(functionTypes)

	if typeLen == 1 {
		if _, ok := funtionTypes[functionTypes[0]]; !ok {
			info.Info = "invalid function type, supported function types are: sbm, notsbm"
			info.Code = m.statusCodes.errReadReq.Code
			return
		}

	}

	if typeLen > 1 {
		info.Info = "more than one function type present in function list query"
		info.Code = m.statusCodes.errReadReq.Code
		return
	}
	return
}

func (m *ServiceMgr) validateQueryFunctionDeployment(query url.Values) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.ok.Code

	deploymentStatus, found := query["deployed"]
	if !found {
		return
	}
	deployedLen := len(deploymentStatus)

	if deployedLen == 1 {
		if deploymentStatus[0] != "true" && deploymentStatus[0] != "false" {
			info.Info = "invalid deployment status, supported deployment status are: true, false"
			info.Code = m.statusCodes.errReadReq.Code
			return
		}
	}

	if deployedLen > 1 {
		info.Info = "more than one deployment status present in function list query"
		info.Code = m.statusCodes.errReadReq.Code
	}
	return
}

func (m *ServiceMgr) validateFunctionListQuery(query url.Values) (info *runtimeInfo) {
	logPrefix := "ServiceMgr::getFunctionList"

	if info = m.validateQueryKey(query); info.Code != m.statusCodes.ok.Code {
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	if info = m.validateQueryBucket(query); info.Code != m.statusCodes.ok.Code {
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	if info = m.validateQueryFunctionType(query); info.Code != m.statusCodes.ok.Code {
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}

	if info = m.validateQueryFunctionDeployment(query); info.Code != m.statusCodes.ok.Code {
		logging.Errorf("%s %s", logPrefix, info.Info)
		return
	}
	return
}

func (m *ServiceMgr) getFunctionList(query url.Values) (fnlist functionList, info *runtimeInfo) {

	info = m.validateFunctionListQuery(query)
	if info.Code != m.statusCodes.ok.Code {
		return
	}
	m.fnMu.RLock()
	defer m.fnMu.RUnlock()
	bucket := query.Get("source_bucket")
	buckets := make(map[string]struct{})
	if bucket == "" {
		for currBucket := range m.bucketFunctionMap {
			buckets[currBucket] = struct{}{}
		}
	} else {
		buckets[bucket] = struct{}{}
	}

	functionType := query.Get("function_type")
	fnTypes := make(map[string]struct{})
	if functionType == "" {
		fnTypes = funtionTypes
	} else {
		fnTypes[functionType] = struct{}{}
	}

	deployStatus := query.Get("deployed")
	deployStatusList := make(map[bool]struct{})
	if deployStatus == "" {
		deployStatusList[true] = struct{}{}
		deployStatusList[false] = struct{}{}
	} else {
		if deployStatus == "true" {
			deployStatusList[true] = struct{}{}
		} else {
			deployStatusList[false] = struct{}{}
		}
	}

	for currBucket := range buckets {
		functions, ok := m.bucketFunctionMap[currBucket]
		if !ok {
			continue
		}
		for function, meta := range functions {
			if _, ok = fnTypes[meta.fnType]; !ok {
				continue
			}
			if _, ok = deployStatusList[meta.fnDeployed]; !ok {
				continue
			}
			fnlist.Functions = append(fnlist.Functions, function)
		}
	}
	return
}

func (m *ServiceMgr) getStatuses(appName string) (bool, bool, error) {
	logPrefix := "ServiceMgr::getStatuses"

	var sData []byte
	metakvPath := metakvAppSettingsPath + appName
	util.Retry(util.NewFixedBackoff(time.Second), nil, metakvGetCallback, metakvPath, &sData)
	settings := make(map[string]interface{})
	err := json.Unmarshal(sData, &settings)
	if err != nil {
		logging.Errorf("%s Failed to unmarshal settings", logPrefix)
		return false, false, err
	}

	val, ok := settings["deployment_status"]
	if !ok {
		logging.Errorf("%s Missing deployment_status", logPrefix)
		return false, false, fmt.Errorf("missing deployment_status")
	}

	dStatus, ok := val.(bool)
	if !ok {
		logging.Errorf("%s Supplied deployment_status unexpected", logPrefix)
		return false, false, fmt.Errorf("non boolean deployment_status")
	}

	val, ok = settings["processing_status"]
	if !ok {
		logging.Errorf("%s Missing processing_status", logPrefix)
		return false, false, fmt.Errorf("missing processing_status")
	}

	pStatus, ok := val.(bool)
	if !ok {
		logging.Errorf("%s Supplied processing_status unexpected", logPrefix)
		return false, false, fmt.Errorf("non boolean processing_status")
	}

	return dStatus, pStatus, nil
}
