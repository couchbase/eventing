package servicemanager

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/logging"
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
	fillMissingDefault(settings, "cpp_worker_thread_count", float64(2))
	fillMissingDefault(settings, "cron_timers_per_doc", float64(1000))
	fillMissingDefault(settings, "curl_timeout", float64(500))
	fillMissingDefault(settings, "deadline_timeout", float64(4))
	fillMissingDefault(settings, "execution_timeout", float64(2))
	fillMissingDefault(settings, "enable_recursive_mutation", false)
	fillMissingDefault(settings, "execution_timeout", float64(1))
	fillMissingDefault(settings, "feedback_batch_size", float64(100))
	fillMissingDefault(settings, "feedback_read_buffer_size", float64(65536))
	fillMissingDefault(settings, "fuzz_offset", float64(0))
	fillMissingDefault(settings, "lcb_inst_capacity", float64(5))
	fillMissingDefault(settings, "log_level", "INFO")
	fillMissingDefault(settings, "skip_timer_threshold", float64(86400))
	fillMissingDefault(settings, "sock_batch_size", float64(100))
	fillMissingDefault(settings, "tick_duration", float64(60000))
	fillMissingDefault(settings, "timer_processing_tick_interval", float64(500))
	fillMissingDefault(settings, "worker_count", float64(3))
	fillMissingDefault(settings, "worker_feedback_queue_cap", float64(10*1000))
	fillMissingDefault(settings, "worker_queue_cap", float64(100*1000))
	fillMissingDefault(settings, "worker_queue_mem_cap", float64(1024))
	fillMissingDefault(settings, "xattr_doc_timer_entry_prune_threshold", float64(100))

	// Process related configuration
	fillMissingDefault(settings, "breakpad_on", false)

	// Rebalance related configurations
	fillMissingDefault(settings, "vb_ownership_giveup_routine_count", float64(3))
	fillMissingDefault(settings, "vb_ownership_takeover_routine_count", float64(3))

	// Application logging related configurations
	fillMissingDefault(settings, "app_log_max_size", float64(1024*1024*10))
	fillMissingDefault(settings, "app_log_max_files", float64(10))

	// Doc timer configurations for plasma
	fillMissingDefault(settings, "auto_swapper", true)
	fillMissingDefault(settings, "enable_snapshot_smr", false)
	fillMissingDefault(settings, "lss_cleaner_max_threshold", float64(70))
	fillMissingDefault(settings, "lss_cleaner_threshold", float64(30))
	fillMissingDefault(settings, "lss_read_ahead_size", float64(1024*1024))
	fillMissingDefault(settings, "max_delta_chain_len", float64(200))
	fillMissingDefault(settings, "max_page_items", float64(400))
	fillMissingDefault(settings, "min_page_items", float64(50))
	fillMissingDefault(settings, "persist_interval", float64(5000))
	fillMissingDefault(settings, "use_memory_manager", true)

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
	errInfo.RuntimeInfo = runtimeInfo.Info
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

func (m *ServiceMgr) sendRuntimeInfoList(w http.ResponseWriter, runtimeInfoList []*runtimeInfo) {
	response, err := json.Marshal(runtimeInfoList)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		w.WriteHeader(m.getDisposition(m.statusCodes.errMarshalResp.Code))
		fmt.Fprintf(w, `{"error":"Failed to marshal error info, err: %v"}`, err)
		return
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
