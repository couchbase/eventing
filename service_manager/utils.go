package servicemanager

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/gen/flatbuf/cfg"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/parser"
	"github.com/couchbase/eventing/rbac"
	"github.com/couchbase/eventing/service_manager/response"
	"github.com/couchbase/eventing/util"
)

var dynamicChangedSettings map[string]struct{}

func init() {
	dynamicChangedSettings = map[string]struct{}{"description": struct{}{},
		"log_level":          struct{}{},
		"timer_context_size": struct{}{},
		"deployment_status":  struct{}{},
		"processing_status":  struct{}{},
	}
}

func (m *ServiceMgr) checkAppExists(appLocation string) (*application, *response.RuntimeInfo) {
	app, info := m.getAppFromTempStore(appLocation)
	if info.ErrCode != response.Ok {
		return nil, info
	}
	return app, info
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

func (m *ServiceMgr) checkAndGetAdminPort() string {
	m.configMutex.RLock()
	check := m.clusterEncryptionConfig != nil && m.clusterEncryptionConfig.EncryptData
	m.configMutex.RUnlock()
	if check {
		return m.adminSSLPort
	}
	return m.adminHTTPPort
}

func (m *ServiceMgr) checkIfDeployedAndRunning(appLocation string) bool {
	mhVersion := common.CouchbaseVerMap["mad-hatter"]
	if m.compareEventingVersion(mhVersion) {
		logPrefix := "ServiceMgr::CheckIfDeployedAndRunning"
		id, err := common.GetIdentityFromLocation(appLocation)
		if err != nil {
			return false
		}

		query := fmt.Sprintf("appName=%s&bucket=%s&scope=%s", id.AppName, id.Bucket, id.Scope)
		bootstrapStatus, err := util.GetAggBootstrapAppStatus(net.JoinHostPort(util.Localhost(), m.adminHTTPPort), query, true)
		if err != nil {
			logging.Errorf("%s %s", logPrefix, err)
			return false
		}

		if bootstrapStatus {
			return false
		}

		return m.superSup.GetAppCompositeState(appLocation) == common.AppStateEnabled
	}
	bootstrappingApps := m.superSup.BootstrapAppList()
	_, isBootstrapping := bootstrappingApps[appLocation]

	return !isBootstrapping && m.superSup.GetAppCompositeState(appLocation) == common.AppStateEnabled
}

func (m *ServiceMgr) checkCompressHandler() bool {
	mhVersion := common.CouchbaseVerMap["mad-hatter"]
	config, info := m.getConfig()
	if info.ErrCode != response.Ok {
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
	fillMissingDefault(app, settings, "n1ql_prepare_all", false)
	fillMissingDefault(app, settings, "allow_transaction_mutations", false)
	fillMissingDefault(app, settings, "checkpoint_interval", float64(60000))
	fillMissingDefault(app, settings, "cpp_worker_thread_count", float64(2))
	fillMissingDefault(app, settings, "curl_max_allowed_resp_size", float64(100))
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
	fillMissingDefault(app, settings, "worker_count", float64(1))
	fillMissingDefault(app, settings, "worker_feedback_queue_cap", float64(500))
	fillMissingDefault(app, settings, "worker_queue_cap", float64(100*1000))
	fillMissingDefault(app, settings, "worker_queue_mem_cap", float64(1024))
	fillMissingDefault(app, settings, "worker_response_timeout", float64(3600))
	fillMissingDefault(app, settings, "bucket_cache_size", float64(64*1024*1024))
	fillMissingDefault(app, settings, "bucket_cache_age", float64(1000))

	// metastore related configuration
	fillMissingDefault(app, settings, "timer_queue_mem_cap", float64(50))
	fillMissingDefault(app, settings, "timer_queue_size", float64(10000))

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
	fillMissingDefault(app, settings, "dcp_window_size", float64(20*1024*1024))
	fillMissingDefault(app, settings, "dcp_gen_chan_size", float64(10000))
	fillMissingDefault(app, settings, "dcp_num_connections", float64(1))

	// N1QL related configuration
	fillMissingDefault(app, settings, "n1ql_consistency", "none")

	// Language related configuration
	fillMissingDefault(app, settings, "language_compatibility", common.LanguageCompatibility[0])
	fillMissingDefault(app, settings, "lcb_retry_count", float64(0))

	// Timer parititions related configuration
	fillMissingDefault(app, settings, "num_timer_partitions", float64(defaultNumTimerPartitions))
}

func fillMissingDefault(app *application, settings map[string]interface{}, field string, defaultValue interface{}) {
	if _, ok := settings[field]; !ok {
		if app == nil {
			settings[field] = defaultValue
			return
		}

		if _, tOk := app.Settings[field]; !tOk {
			settings[field] = defaultValue
			return
		}
		settings[field] = app.Settings[field]
	}
}

func (m *ServiceMgr) unmarshalApp(r *http.Request) (app application, info *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::unmarshalApp"
	info = &response.RuntimeInfo{}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		info.ErrCode = response.ErrReadReq
		info.Description = fmt.Sprintf("Failed to read request body, err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	data = bytes.Trim(data, "[]\n ")
	err = json.Unmarshal(data, &app)
	if err != nil {
		info.ErrCode = response.ErrUnmarshalPld
		info.Description = fmt.Sprintf("Failed to unmarshal payload err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	if app.Settings == nil {
		app.Settings = make(map[string]interface{}, 1)
	}

	return
}

// Unmarshals list of application and returns application objects
func (m *ServiceMgr) unmarshalAppList(r *http.Request) (appList *[]application, info *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::unmarshalAppList"
	appList = &[]application{}
	info = &response.RuntimeInfo{}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		info.ErrCode = response.ErrReadReq
		info.Description = fmt.Sprintf("Failed to read request body, err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	// Create array of apps so that passed valid app object not fail during unmarshalling.
	data = bytes.Trim(data, "[]\n ")
	data = append([]byte("["), data...)
	data = append(data, []byte("]")...)

	err = json.Unmarshal(data, &appList)
	if err != nil {
		info.ErrCode = response.ErrUnmarshalPld
		info.Description = fmt.Sprintf("Failed to unmarshal payload err: %v", err)
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	for _, app := range *appList {
		if app.Settings == nil {
			app.Settings = make(map[string]interface{}, 1)
		}
	}

	return
}

func (m *ServiceMgr) checkLifeCycleOpsDuringRebalance() (info *response.RuntimeInfo) {
	logPrefix := "ServiceMgr:enableLifeCycleOpsDuringRebalance"

	info = &response.RuntimeInfo{}
	var lifeCycleOpsDuringReb bool

	config, configInfo := m.getConfig()
	if configInfo.ErrCode != response.Ok {
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

	if rebStatus := m.checkRebalanceStatus(); !lifeCycleOpsDuringReb && rebStatus.ErrCode != response.Ok {
		*info = *rebStatus
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	return
}

func (m *ServiceMgr) getSourceBindingFromFlatBuf(config *cfg.DepCfg, appdata *cfg.Config) *cfg.Bucket {
	binding := new(cfg.Bucket)
	sourceKeyspace := common.Keyspace{
		BucketName:     string(config.SourceBucket()),
		ScopeName:      string(config.SourceScope()),
		CollectionName: string(config.SourceCollection()),
	}

	for idx := 0; idx < config.BucketsLength(); idx++ {
		if config.Buckets(binding, idx) {
			bind := common.Keyspace{
				BucketName:     string(binding.BucketName()),
				ScopeName:      string(binding.ScopeName()),
				CollectionName: string(binding.CollectionName()),
			}

			if sourceKeyspace == bind && string(appdata.Access(idx)) == "rw" {
				return binding
			}
		}
	}
	return nil
}

func (m *ServiceMgr) isSrcMutationEnabled(cfg *depCfg) bool {
	sourceKeyspace := common.Keyspace{
		BucketName:     cfg.SourceBucket,
		ScopeName:      cfg.SourceScope,
		CollectionName: cfg.SourceCollection,
	}
	for _, binding := range cfg.Buckets {
		bind := common.Keyspace{
			BucketName:     binding.BucketName,
			ScopeName:      binding.ScopeName,
			CollectionName: binding.CollectionName,
		}
		if bind == sourceKeyspace && binding.Access == "rw" {
			return true
		}
	}
	return false
}

func (m *ServiceMgr) isAppDeployable(appLocation string, app *application) bool {
	if !m.isSrcMutationEnabled(&app.DeploymentConfig) {
		return true
	}

	sourceKeyspace := common.Keyspace{BucketName: app.DeploymentConfig.SourceBucket,
		ScopeName:      app.DeploymentConfig.SourceScope,
		CollectionName: app.DeploymentConfig.SourceCollection,
	}

	for _, appName := range m.superSup.DeployedAppList() {
		if appName == appLocation || m.superSup.GetAppCompositeState(appName) != common.AppStateEnabled {
			continue
		}
		data, err := util.ReadAppContent(metakvAppsPath, metakvChecksumPath, appName)
		if err != nil {
			return false
		}
		appdata := cfg.GetRootAsConfig(data, 0)
		config := new(cfg.DepCfg)
		depcfg := appdata.DepCfg(config)

		otherKeyspace := common.Keyspace{BucketName: string(depcfg.SourceBucket()),
			ScopeName:      string(depcfg.SourceScope()),
			CollectionName: string(depcfg.SourceCollection()),
		}

		if sourceKeyspace == otherKeyspace {
			binding := m.getSourceBindingFromFlatBuf(depcfg, appdata)
			if binding != nil {
				return false
			}
		}
	}
	return true
}

func (m *ServiceMgr) getSourceAndDestinationsFromDepCfg(cfg *depCfg) (src common.Keyspace, dest map[common.Keyspace]struct{}) {
	dest = make(map[common.Keyspace]struct{})
	src = common.Keyspace{BucketName: cfg.SourceBucket,
		ScopeName:      cfg.SourceScope,
		CollectionName: cfg.SourceCollection,
	}

	metaData := common.Keyspace{BucketName: cfg.MetadataBucket,
		ScopeName:      cfg.MetadataScope,
		CollectionName: cfg.MetadataCollection,
	}
	dest[metaData] = struct{}{}
	for idx := 0; idx < len(cfg.Buckets); idx++ {
		binding := common.Keyspace{BucketName: cfg.Buckets[idx].BucketName,
			ScopeName:      cfg.Buckets[idx].ScopeName,
			CollectionName: cfg.Buckets[idx].CollectionName,
		}

		if binding != src && cfg.Buckets[idx].Access == "rw" {
			dest[binding] = struct{}{}
		}
	}
	return src, dest
}

// GetNodesHostname returns hostnames of all nodes with alternate Addresses if any
func GetNodesHostname(data map[string]interface{}) []string {
	hostnames := make([]string, 0)

	nodes, exists := data["nodes"].([]interface{})
	if !exists {
		return hostnames
	}
	for _, value := range nodes {
		nodeInfo := value.(map[string]interface{})
		if hostname, exists := nodeInfo["hostname"].(string); exists {
			if info, altExists := nodeInfo["alternateAddresses"].(map[string]interface{}); altExists {
				if external, extExists := info["external"].(map[string]interface{}); extExists {
					if extHostname, hExists := external["hostname"].(string); hExists {
						hostname = hostname + "<TOK>" + extHostname
					}
				}
			}
			hostnames = append(hostnames, hostname)
		}
	}
	return hostnames
}

func (m *ServiceMgr) UpdateBucketGraphFromMetakv(functionName string) error {
	logPrefix := "ServiceMgr::UpdateBucketGraphFromMektakv"
	appData, err := util.ReadAppContent(metakvAppsPath, metakvChecksumPath, functionName)
	if err != nil {
		logging.Errorf("%s Function: %v read from metakv failed, err: %v", logPrefix, functionName, err)
		return err
	}
	app := m.parseFunctionPayload(appData, functionName)
	source, destinations := m.getSourceAndDestinationsFromDepCfg(&app.DeploymentConfig)
	_, pinfos := parser.TranspileQueries(app.AppHandlers, "")
	for _, pinfo := range pinfos {
		if pinfo.PInfo.KeyspaceName != "" {
			dest := ConstructKeyspace(pinfo.PInfo.KeyspaceName)
			destinations[dest] = struct{}{}
		}
	}

	logging.Infof("%s inserting edges into graph for function: %v, source: %v destinations: %v", logPrefix, functionName, source, destinations)
	if len(destinations) != 0 {
		m.graph.insertEdges(functionName, source, destinations)
	}
	return nil
}

// block until servicemgr's first callback fires
func (m *ServiceMgr) NotifySupervisorWaitCh() {
	defer func() {
		if err := recover(); err != nil {
			logging.Warnf("While trying to send over the waitch, got error: %v. Skipping sending to channel", err)
		}
	}()

	m.supWaitCh <- true
	close(m.supWaitCh)
}

func (m *ServiceMgr) validateQueryKey(query url.Values) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	for key := range query {
		if _, found := functionQueryKeys[key]; !found {
			info.Description = "key mismatch error, supported keys in function list query are: source_bucket, function_type, deployed"
			info.ErrCode = response.ErrReadReq
			return
		}
	}
	return
}

func (m *ServiceMgr) validateQueryKeyspace(query url.Values) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}
	buckets, bktPresent := query["source_bucket"]

	if bktPresent && len(buckets) > 1 {
		info.Description = "more than one bucket name present in function list query"
		info.ErrCode = response.ErrReadReq
		return
	}

	scopes, scopePresent := query["source_scope"]
	if !bktPresent && scopePresent {
		info.Description = "filter on scope is given without filter on bucket"
		info.ErrCode = response.ErrReadReq
		return
	}

	if scopePresent && len(scopes) > 1 {
		info.Description = "more than one scope name present in function list query"
		info.ErrCode = response.ErrReadReq
		return
	}

	collections, collectionPresent := query["source_collection"]
	if !(scopePresent && bktPresent) && collectionPresent {
		info.Description = "filter on collection is given without filter on bucket or scope"
		info.ErrCode = response.ErrReadReq
		return
	}
	if collectionPresent && len(collections) > 1 {
		info.Description = "more than one collection name present in function list query"
		info.ErrCode = response.ErrReadReq
		return
	}

	return
}

func (m *ServiceMgr) validateQueryFunctionType(query url.Values) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	functionTypes, found := query["function_type"]
	if !found {
		return
	}
	typeLen := len(functionTypes)

	if typeLen == 1 {
		if _, ok := funtionTypes[functionTypes[0]]; !ok {
			info.Description = "invalid function type, supported function types are: sbm, notsbm"
			info.ErrCode = response.ErrReadReq
			return
		}

	}

	if typeLen > 1 {
		info.Description = "more than one function type present in function list query"
		info.ErrCode = response.ErrReadReq
		return
	}
	return
}

func (m *ServiceMgr) validateQueryFunctionDeployment(query url.Values) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	deploymentStatus, found := query["deployed"]
	if !found {
		return
	}
	deployedLen := len(deploymentStatus)

	if deployedLen == 1 {
		if deploymentStatus[0] != "true" && deploymentStatus[0] != "false" {
			info.Description = "invalid deployment status, supported deployment status are: true, false"
			info.ErrCode = response.ErrReadReq
			return
		}
	}

	if deployedLen > 1 {
		info.Description = "more than one deployment status present in function list query"
		info.ErrCode = response.ErrReadReq
	}
	return
}

func (m *ServiceMgr) validateFunctionListQuery(query url.Values) (info *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::getFunctionList"

	if info = m.validateQueryKey(query); info.ErrCode != response.Ok {
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	if info = m.validateQueryKeyspace(query); info.ErrCode != response.Ok {
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	if info = m.validateQueryFunctionType(query); info.ErrCode != response.Ok {
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}

	if info = m.validateQueryFunctionDeployment(query); info.ErrCode != response.Ok {
		logging.Errorf("%s %s", logPrefix, info.Description)
		return
	}
	return
}

func (m *ServiceMgr) getFunctionList(query url.Values) (fnlist functionList, info *response.RuntimeInfo) {

	info = m.validateFunctionListQuery(query)
	if info.ErrCode != response.Ok {
		return
	}

	m.fnMu.RLock()
	defer m.fnMu.RUnlock()
	bucket := query.Get("source_bucket")
	scope := common.CheckAndReturnDefaultForScopeOrCollection(query.Get("source_scope"))
	collection := common.CheckAndReturnDefaultForScopeOrCollection(query.Get("source_collection"))
	keyspace := make(map[common.Keyspace]struct{})
	if bucket == "" {
		for currKeyspace := range m.bucketFunctionMap {
			keyspace[currKeyspace] = struct{}{}
		}
	} else {
		sourceKeyspace := common.Keyspace{BucketName: bucket,
			ScopeName:      scope,
			CollectionName: collection,
		}
		keyspace[sourceKeyspace] = struct{}{}
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

	for currKeyspace := range keyspace {
		functions, ok := m.bucketFunctionMap[currKeyspace]
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

func (m *ServiceMgr) getStatuses(appName string) (dStatus bool, pStatus bool, err error) {
	logPrefix := "ServiceMgr::getStatuses"

	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("%s getStatuses recovered from panic,  stack trace: %rm", logPrefix, string(trace))
			err = fmt.Errorf("%s Error getting statuses for appName: %s, please see logs for more information", logPrefix, appName)
		}
	}()

	var sData []byte
	metakvPath := metakvAppSettingsPath + appName
	util.Retry(util.NewFixedBackoff(time.Second), nil, metakvGetCallback, metakvPath, &sData)
	settings := make(map[string]interface{})
	err = json.Unmarshal(sData, &settings)
	if err != nil {
		logging.Errorf("%s Failed to unmarshal settings", logPrefix)
		return false, false, err
	}

	val, ok := settings["deployment_status"]
	if !ok {
		logging.Errorf("%s Missing deployment_status", logPrefix)
		return false, false, fmt.Errorf("missing deployment_status")
	}

	dStatus, ok = val.(bool)
	if !ok {
		logging.Errorf("%s Supplied deployment_status unexpected", logPrefix)
		return false, false, fmt.Errorf("non boolean deployment_status")
	}

	val, ok = settings["processing_status"]
	if !ok {
		logging.Errorf("%s Missing processing_status", logPrefix)
		return false, false, fmt.Errorf("missing processing_status")
	}

	pStatus, ok = val.(bool)
	if !ok {
		logging.Errorf("%s Supplied processing_status unexpected", logPrefix)
		return false, false, fmt.Errorf("non boolean processing_status")
	}

	return dStatus, pStatus, nil
}

func (m *ServiceMgr) SetFailoverStatus(changeId string) {
	m.failoverMu.Lock()
	defer m.failoverMu.Unlock()

	m.failoverCounter++
	m.failoverNotifTs = time.Now().Unix()
	m.failoverChangeId = changeId

	return
}

func (m *ServiceMgr) ResetFailoverStatus() {
	m.failoverMu.Lock()
	defer m.failoverMu.Unlock()

	if m.failoverCounter > 0 {
		m.failoverCounter--
	}

	if 0 == m.failoverCounter {
		m.failoverNotifTs = 0
		m.failoverChangeId = ""
	}
	return
}

func (m *ServiceMgr) GetFailoverStatus() (failoverNotifTs int64, changeId string) {
	return m.failoverNotifTs, m.failoverChangeId
}

func (m *ServiceMgr) watchFailoverEvents() {
	logPrefix := "ServiceMgr::watchFailoverEvents"

	ticker := time.NewTicker(time.Duration(5) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if m.failoverNotifTs != 0 {
				now := time.Now().Unix()
				if now-m.failoverNotifTs > 5 {
					info := &response.RuntimeInfo{}
					var config common.Config

					if config, info = m.getConfig(); info.ErrCode != response.Ok {
						logging.Errorf("%s getConfig failed: %v", logPrefix, info)
						continue
					}

					autoRedistributeVbsOnFailover := true
					var ok bool
					var flag interface{}
					if flag, ok = config["auto_redistribute_vbs_on_failover"]; ok {
						autoRedistributeVbsOnFailover = flag.(bool)
					}

					if autoRedistributeVbsOnFailover {
						err := m.checkTopologyChangeReadiness(service.TopologyChangeTypeFailover)
						if err == nil {
							path := metakvRebalanceTokenPath + m.failoverChangeId
							value := []byte(startFailover)
							logging.Infof("%s triggering failover processing path: %v, value:%v", logPrefix, path, value)
							m.superSup.TopologyChangeNotifCallback(metakv.KVEntry{Path: path, Value: value, Rev: m.state.rev})
						}
					}
				}
			}

		case <-m.finch:
			return
		}
	}
}

func (m *ServiceMgr) checkTopologyChangeReadiness(changeType service.TopologyChangeType) error {
	logPrefix := "ServiceMgr::checkTopologyChangeReadiness"

	nodeAddrs, err := m.getActiveNodeAddrs()
	logging.Tracef("%s Active Eventing nodes in the cluster: %rs", logPrefix, nodeAddrs)

	if len(nodeAddrs) > 0 && err == nil {

		logging.Infof("%s Querying nodes: %rs for bootstrap status", logPrefix, nodeAddrs)

		// Fail rebalance if some apps are undergoing bootstrap
		mhVersion := common.CouchbaseVerMap["mad-hatter"]
		if !m.compareEventingVersion(mhVersion) {
			appsBootstrapping, err := util.GetAggBootstrappingApps("/getBootstrappingApps", nodeAddrs)
			logging.Infof("%s Status of app bootstrap across all Eventing nodes: %v", logPrefix, appsBootstrapping)
			if err != nil {
				logging.Warnf("%s Some apps are deploying or resuming on some or all Eventing nodes, err: %v", logPrefix, err)
				return err
			}
		} else {
			appsBootstrapStatus, err := util.CheckIfBootstrapOngoing("/getBootstrapStatus", nodeAddrs)
			logging.Infof("%s Bootstrap status across all Eventing nodes: %v err: %v", logPrefix, appsBootstrapStatus, err)
			if err != nil {
				return err
			}
			if appsBootstrapStatus {
				return fmt.Errorf("Some apps are deploying or resuming on some or all Eventing nodes")
			}

			appsPausing, err := util.GetAggPausingApps("/getPausingApps", nodeAddrs)
			logging.Infof("%s Status of pausing apps across all Eventing nodes: %v err: %v", logPrefix, appsPausing, err)
			if err != nil {
				return err
			}
		}
		if changeType == service.TopologyChangeTypeRebalance { // For failover we do not want to wait
			if rebStatus := m.checkRebalanceStatus(); rebStatus.ErrCode != response.Ok {
				return fmt.Errorf("%v", rebStatus.Description)
			}
		}
	}

	if err != nil {
		logging.Warnf("%s Error encountered while fetching active Eventing nodes, err: %v", logPrefix, err)
		return fmt.Errorf("failed to get active eventing nodes in the cluster")
	}

	return nil
}

func (m *ServiceMgr) CheckLifeCycleOpsDuringRebalance() bool {
	rebStatus := m.checkLifeCycleOpsDuringRebalance()
	if rebStatus.ErrCode != response.Ok {
		return true
	}
	return false
}

func (m *ServiceMgr) MaybeEnforceFunctionSchema(app application) *response.RuntimeInfo {
	info := &response.RuntimeInfo{}
	if appData, err := json.Marshal(app); err == nil && app.EnforceSchema == true {
		schemaErr := parser.ValidateHandlerSchema(appData)
		if schemaErr != nil {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("Invalid function configuration, err: %v", schemaErr)
			logging.Errorf("%s\n", info.Description)
			return info
		}
	}
	return info
}

func (m *ServiceMgr) MaybeEnforceSettingsSchema(data []byte) *response.RuntimeInfo {
	info := &response.RuntimeInfo{}
	schemaErr := parser.ValidateSettingsSchema(data)
	if schemaErr != nil {
		info.ErrCode = response.ErrInvalidConfig
		info.Description = fmt.Sprintf("Invalid settings configuration, err: %v", schemaErr)
		logging.Errorf("%s\n", info.Description)
		return info
	}
	return info
}

func (m *ServiceMgr) checkLocalTopologyChangeReadiness() error {
	bootstrapAppList := m.superSup.BootstrapAppList()
	if len(bootstrapAppList) > 0 {
		return fmt.Errorf("Some apps are deploying or resuming on nodeId: %s Apps: %v", m.nodeInfo.NodeID, bootstrapAppList)
	}

	pausingApps := m.superSup.PausingAppList()
	if len(pausingApps) > 0 {
		return fmt.Errorf("Some apps are being paused on nodId: %s Apps: %v", m.nodeInfo.NodeID, pausingApps)
	}

	if m.superSup.RebalanceStatus() {
		return fmt.Errorf("Eventing Rebalance or Failover processing ongoing on nodeId: %s", m.nodeInfo.NodeID)
	}

	return nil
}

func (m *ServiceMgr) getTempStoreAppNames() []string {
	return m.tempAppStore.GetActiveAppsInternalLocation()
}

func (m *ServiceMgr) rbacSupport() bool {
	hostAddress := net.JoinHostPort(util.Localhost(), m.restPort)
	cic, err := util.FetchClusterInfoClient(hostAddress)
	if err != nil {
		return false
	}
	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	version, minVer := cinfo.GetNodeCompatVersion()
	cinfo.RUnlock()

	return version >= 7 && minVer >= 1
}

func (m *ServiceMgr) fetchAppCompositeState(appName string) (int8, *response.RuntimeInfo) {
	app, info := m.getAppFromTempStore(appName)
	if info.ErrCode == response.ErrAppNotFoundTs {
		resInfo := &response.RuntimeInfo{}
		return common.AppStateUndeployed, resInfo
	}

	if info.ErrCode != response.Ok {
		return common.AppStateUndeployed, info
	}

	dStatus, ok := app.Settings["deployment_status"].(bool)
	if !ok {
		dStatus = false
	}
	pStatus, ok := app.Settings["processing_status"].(bool)
	if !ok {
		pStatus = false
	}

	return common.GetCompositeState(dStatus, pStatus), info
}

func (app *application) copy() application {
	copyApp := *app

	settings := make(map[string]interface{})
	for k, v := range app.Settings {
		settings[k] = v
	}
	copyApp.Settings = settings

	metaInfo := make(map[string]interface{})
	for k, v := range app.Metainfo {
		metaInfo[k] = v
	}
	copyApp.Metainfo = metaInfo

	copyApp.DeploymentConfig = app.DeploymentConfig.copy()
	return copyApp
}

func (deploymentCfg depCfg) copy() depCfg {
	copyDepCfg := deploymentCfg
	if len(deploymentCfg.Buckets) > 0 {
		buckets := make([]bucket, 0, len(deploymentCfg.Buckets))
		for _, b := range deploymentCfg.Buckets {
			buckets = append(buckets, b)
		}
		copyDepCfg.Buckets = buckets
	}

	if len(deploymentCfg.Curl) > 0 {
		curl := make([]common.Curl, 0, len(deploymentCfg.Curl))
		for _, c := range deploymentCfg.Curl {
			curl = append(curl, c)
		}
		copyDepCfg.Curl = curl
	}

	if len(deploymentCfg.Constants) > 0 {
		constants := make([]common.Constant, 0, len(deploymentCfg.Constants))
		for _, c := range deploymentCfg.Constants {
			constants = append(constants, c)
		}
		copyDepCfg.Constants = constants
	}
	return copyDepCfg
}

func ConstructKeyspace(keyspace string) common.Keyspace {
	// var namespace string
	scope, collection := "_default", "_default"

	n := strings.IndexByte(keyspace, ':')
	_, keyspace = trim(keyspace, n)
	d := strings.IndexByte(keyspace, '.')
	if d >= 0 {
		keyspace, scope = trim(keyspace, d)
		d = strings.IndexByte(scope, '.')
		if d >= 0 {
			scope, collection = trim(scope, d)
		}
	}

	return common.Keyspace{BucketName: keyspace,
		ScopeName:      scope,
		CollectionName: collection,
	}
}

func CheckIfAppKeyspacesAreSame(app1, app2 application) bool {

	srcKeyspace1 := common.Keyspace{BucketName: app1.DeploymentConfig.SourceBucket,
		ScopeName:      app1.DeploymentConfig.SourceScope,
		CollectionName: app1.DeploymentConfig.SourceCollection}

	metaKeyspace1 := common.Keyspace{BucketName: app1.DeploymentConfig.MetadataBucket,
		ScopeName:      app1.DeploymentConfig.MetadataScope,
		CollectionName: app1.DeploymentConfig.MetadataCollection}

	srcKeyspace2 := common.Keyspace{BucketName: app2.DeploymentConfig.SourceBucket,
		ScopeName:      app2.DeploymentConfig.SourceScope,
		CollectionName: app2.DeploymentConfig.SourceCollection}

	metaKeyspace2 := common.Keyspace{BucketName: app2.DeploymentConfig.MetadataBucket,
		ScopeName:      app2.DeploymentConfig.MetadataScope,
		CollectionName: app2.DeploymentConfig.MetadataCollection}

	return srcKeyspace1 == srcKeyspace2 && metaKeyspace1 == metaKeyspace2
}

func trim(right string, i int) (string, string) {
	var left string

	if i >= 0 {
		left = right[:i]
		if i < len(right)-1 {
			right = right[i+1:]
		} else {
			right = ""
		}
	}
	return left, right
}

func populate(fmtStr, appName, key string, stats []byte, cStats map[string]interface{}) []byte {
	var str string
	if val, ok := cStats[key]; ok {
		str = fmt.Sprintf(fmtStr, METRICS_PREFIX, key, appName, val)
	} else {
		str = fmt.Sprintf(fmtStr, METRICS_PREFIX, key, appName, 0)
	}
	return append(stats, []byte(str)...)
}

func populateUint(fmtStr, appName, key string, stats []byte, cStats map[string]uint64) []byte {
	var str string
	if val, ok := cStats[key]; ok {
		str = fmt.Sprintf(fmtStr, METRICS_PREFIX, key, appName, val)
	} else {
		str = fmt.Sprintf(fmtStr, METRICS_PREFIX, key, appName, 0)
	}
	return append(stats, []byte(str)...)
}

func filterQueryMap(filterString string, include bool) (map[string]bool, error) {
	filterMap := make(map[string]bool)
	filters := strings.Split(filterString, ",")
	for _, keyspace := range filters {
		key := strings.Split(keyspace, ".")
		if len(key) > 0 && len(key) < 4 {
			filterMap[keyspace] = include
			continue
		}
		return nil, fmt.Errorf("Malformed input filter %s", keyspace)
	}
	return filterMap, nil
}

func getRestoreMap(r *http.Request) (map[string]common.Keyspace, error) {
	remap := make(map[string]common.Keyspace)
	remapStr := r.FormValue("remap")
	if len(remapStr) == 0 {
		return remap, nil
	}

	remaps := strings.Split(remapStr, ",")
	for _, rm := range remaps {

		rmp := strings.Split(rm, ":")
		if len(rmp) > 2 || len(rmp) < 2 {
			return nil, fmt.Errorf("Malformed input. Missing source/target in remap %v", remapStr)
		}

		source := rmp[0]
		target := rmp[1]

		src := strings.Split(source, ".")
		tgt := strings.Split(target, ".")

		if len(src) != len(tgt) {
			return nil, fmt.Errorf("Malformed input. source and target in remap should be at same level %v", remapStr)
		}

		switch len(src) {
		case 3:
			remap[source] = common.Keyspace{BucketName: tgt[0], ScopeName: tgt[1], CollectionName: tgt[2]}

		case 2:
			remap[source] = common.Keyspace{BucketName: tgt[0], ScopeName: tgt[1]}

		case 1:
			remap[source] = common.Keyspace{BucketName: tgt[0]}
		default:
			return nil, fmt.Errorf("Malformed input remap %v", remapStr)
		}
	}

	return remap, nil
}

func applyFilter(app application, filterMap map[string]bool, filterType string) bool {
	if filterType == "" {
		return true
	}

	if val, ok := contains(filterMap, app.FunctionScope.BucketName, app.FunctionScope.ScopeName, ""); ok {
		return val
	}

	deploymentConfig := app.DeploymentConfig
	if val, ok := contains(filterMap, deploymentConfig.SourceBucket, deploymentConfig.SourceScope, deploymentConfig.SourceCollection); ok {
		return val
	}

	if val, ok := contains(filterMap, deploymentConfig.MetadataBucket, deploymentConfig.MetadataScope, deploymentConfig.MetadataCollection); ok {
		return val
	}

	for _, keyspace := range deploymentConfig.Buckets {
		if val, ok := contains(filterMap, keyspace.BucketName, keyspace.ScopeName, keyspace.CollectionName); ok {
			return val
		}
	}

	if filterType == "include" {
		return false
	}
	return true
}

func remapContains(remap map[string]common.Keyspace, bucket, scope, collection string) (common.Keyspace, int, bool) {
	if val, ok := remap[fmt.Sprintf("%s.%s.%s", bucket, scope, collection)]; ok {
		return val, 3, true
	}
	if val, ok := remap[fmt.Sprintf("%s.%s", bucket, scope)]; ok {
		return val, 2, true
	}

	if val, ok := remap[fmt.Sprintf("%s", bucket)]; ok {
		return val, 1, true
	}

	return common.Keyspace{}, 0, false
}

func contains(filterMap map[string]bool, bucket, scope, collection string) (val, ok bool) {
	if val, ok = filterMap[fmt.Sprintf("%s.%s.%s", bucket, scope, collection)]; ok {
		return
	}
	if val, ok = filterMap[fmt.Sprintf("%s.%s", bucket, scope)]; ok {
		return
	}

	if val, ok = filterMap[fmt.Sprintf("%s", bucket)]; ok {
		return
	}

	return
}

func applicationAdapter(app *application) (common.Application, error) {
	content, err := json.Marshal(*app)
	if err != nil {
		return common.Application{}, err
	}
	appConverted := common.Application{}
	errConversion := json.Unmarshal(content, &appConverted)
	if errConversion != nil {
		return appConverted, errConversion
	}
	appConverted.Owner = app.Owner
	return appConverted, nil
}

func redactSesitiveData(app *application) {
	for idx, _ := range app.DeploymentConfig.Curl {
		app.DeploymentConfig.Curl[idx].BearerKey = PASSWORD_MASK
		app.DeploymentConfig.Curl[idx].Password = PASSWORD_MASK
	}
	app.Owner = nil
}

func copyPasswords(newApp, oldApp *application) {
	for idx, newBinding := range newApp.DeploymentConfig.Curl {
		for _, binding := range oldApp.DeploymentConfig.Curl {
			if newBinding.Value == binding.Value {
				if newBinding.Password == PASSWORD_MASK {
					newApp.DeploymentConfig.Curl[idx].Password = binding.Password
				}
				if newBinding.BearerKey == PASSWORD_MASK {
					newApp.DeploymentConfig.Curl[idx].BearerKey = binding.BearerKey
				}
			}
		}
	}
}

func isDynamicSetting(setting string) bool {
	_, ok := dynamicChangedSettings[setting]
	return ok
}

func CheckAndGetQueryParam(params map[string][]string, key string) (string, *response.RuntimeInfo) {
	info := &response.RuntimeInfo{}

	appNameList := params[key]
	if len(appNameList) == 0 {
		info.ErrCode = response.ErrInvalidRequest
		info.Description = fmt.Sprintf("Parameter '%s' must be provided", key)
		return "", info
	}

	return appNameList[0], info
}

func getAuthErrorInfo(notAllowed []string, all bool, err error) (runtimeInfo response.RuntimeInfo) {
	runtimeInfo = response.RuntimeInfo{}
	switch err {
	case rbac.ErrAuthentication:
		runtimeInfo.ErrCode = response.ErrUnauthenticated
		runtimeInfo.Description = fmt.Sprintf("%v", err)
	case rbac.ErrAuthorisation:
		runtimeInfo.ErrCode = response.ErrForbidden
		permDescription := "Forbidden. User needs atleast one of the following permissions: %v"
		if all {
			permDescription = "Forbidden. User needs all of the following permissions: %v"
		}
		runtimeInfo.Description = fmt.Sprintf(permDescription, notAllowed)

	case rbac.ErrUserDeleted:
		runtimeInfo.ErrCode = response.ErrForbidden
		runtimeInfo.Description = fmt.Sprintf("Owner of the function doesn't exist")
	default:
		runtimeInfo.ErrCode = response.ErrInternalServer

	}
	return
}

func getBucketScope(params map[string][]string, id *common.Identity) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	bucket, bInfo := CheckAndGetQueryParam(params, "bucket")
	scope, sInfo := CheckAndGetQueryParam(params, "scope")
	if bInfo.ErrCode == response.Ok && sInfo.ErrCode == response.Ok {
		id.Bucket = bucket
		id.Scope = scope
		return
	}

	// If both of them are not provided then its fine
	if bInfo.ErrCode != response.Ok && sInfo.ErrCode != response.Ok {
		id.Bucket = "*"
		id.Scope = "*"
		return
	}

	info.ErrCode = response.ErrInvalidRequest
	info.Description = fmt.Sprintf("Either both 'bucket' and 'scope' should be specified or none")
	return
}

// Create identity of the app
func createIdentity(params map[string][]string) (id common.Identity, info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	name, info := CheckAndGetQueryParam(params, "name")
	if info.ErrCode != response.Ok {
		name, info = CheckAndGetQueryParam(params, "appName")
		if info.ErrCode != response.Ok {
			return id, info
		}
	}
	id.AppName = name
	info = getBucketScope(params, &id)
	return
}

func getAppLocationFromApp(app *application) string {
	if app == nil {
		return ""
	}

	id := common.Identity{
		AppName: app.Name,
		Bucket:  app.FunctionScope.BucketName,
		Scope:   app.FunctionScope.ScopeName,
	}
	return id.ToLocation()
}
