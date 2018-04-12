package servicemanager

import (
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

func (m *ServiceMgr) validateApplication(app *application) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	if info = m.validateApplicationName(app.Name); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validateDeploymentConfig(&app.DeploymentConfig); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validateNonEmpty(app.AppHandlers, "Function handler"); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validateSettings(app.Settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) validateAuth(w http.ResponseWriter, r *http.Request, perm string) bool {
	creds, err := cbauth.AuthWebCreds(r)
	if err != nil || creds == nil {
		logging.Warnf("Cannot authenticate request to %rs", r.URL)
		w.WriteHeader(http.StatusUnauthorized)
		return false
	}
	allowed, err := creds.IsAllowed(perm)
	if err != nil || !allowed {
		logging.Warnf("Cannot authorize request to %rs", r.URL)
		w.WriteHeader(http.StatusForbidden)
		return false
	}
	logging.Debugf("Allowing access to %rs", r.URL)
	return true
}

func (m *ServiceMgr) validateAliasName(aliasName string) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	if info = m.validateName(aliasName, "Alias", maxAliasLength); info.Code != m.statusCodes.ok.Code {
		return
	}

	identifier := regexp.MustCompile("^[a-zA-Z_$][a-zA-Z0-9_]*$")
	if !identifier.MatchString(aliasName) {
		info.Code = m.statusCodes.errInvalidConfig.Code
		info.Info = "Alias must be a valid JavaScript variable"
		return
	}

	// Obtained from - https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Lexical_grammar
	jsReservedWords := []string{
		"abstract",
		"await",
		"boolean",
		"break",
		"byte",
		"case",
		"catch",
		"char",
		"class",
		"const",
		"continue",
		"debugger",
		"default",
		"delete",
		"do",
		"double",
		"enum",
		"else",
		"export",
		"extends",
		"final",
		"finally",
		"float",
		"for",
		"function",
		"goto",
		"if",
		"implements",
		"import",
		"interface",
		"in",
		"instanceof",
		"int",
		"let",
		"long",
		"native",
		"new",
		"package",
		"private",
		"protected",
		"public",
		"return",
		"short",
		"static",
		"super",
		"switch",
		"synchronized",
		"this",
		"throw",
		"throws",
		"transient",
		"try",
		"typeof",
		"var",
		"void",
		"volatile",
		"while",
		"with",
		"yield",
	}

	if util.Contains(aliasName, jsReservedWords) {
		info.Code = m.statusCodes.errInvalidConfig.Code
		info.Info = "Alias must not be a JavaScript reserved word"
		return
	}

	// Only a subset of N1QL reserved words
	n1qlReservedWords := []string{
		"alter",
		"build",
		"create",
		"delete",
		"drop",
		"execute",
		"explain",
		"from",
		"grant",
		"infer",
		"insert",
		"merge",
		"prepare",
		"rename",
		"select",
		"revoke",
		"update",
		"upsert",
	}

	if util.ContainsIgnoreCase(aliasName, n1qlReservedWords) {
		info.Code = m.statusCodes.errInvalidConfig.Code
		info.Info = "Alias must not be a N1QL reserved word"
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) validateApplicationName(applicationName string) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	if info = m.validateName(applicationName, "Function", maxApplicationNameLength); info.Code != m.statusCodes.ok.Code {
		return
	}

	appNameRegex := regexp.MustCompile("^[a-zA-Z0-9][a-zA-Z0-9_-]*$")
	if !appNameRegex.MatchString(applicationName) {
		info.Code = m.statusCodes.errInvalidConfig.Code
		info.Info = "Function name can only contain characters in range A-Z, a-z, 0-9 and underscore, hyphen"
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) validateBoolean(field string, settings map[string]interface{}) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	if val, ok := settings[field]; ok {
		if _, ok = val.(bool); !ok {
			info.Info = fmt.Sprintf("%s must be a boolean", field)
			return
		}
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) validateDeploymentConfig(deploymentConfig *depCfg) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	if info = m.validateNonEmpty(deploymentConfig.SourceBucket, "Source bucket name"); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validateNonEmpty(deploymentConfig.MetadataBucket, "Metadata bucket name"); info.Code != m.statusCodes.ok.Code {
		return
	}

	for _, bucket := range deploymentConfig.Buckets {
		if info = m.validateNonEmpty(bucket.BucketName, "Alias bucket name"); info.Code != m.statusCodes.ok.Code {
			return
		}

		if info = m.validateAliasName(bucket.Alias); info.Code != m.statusCodes.ok.Code {
			return
		}
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) validateDirPath(field string, settings map[string]interface{}) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	if val, ok := settings[field]; ok {
		path := val.(string)
		if fileInfo, err := os.Stat(path); err == nil {
			if !fileInfo.IsDir() {
				info.Info = fmt.Sprintf("%s must be a directory", field)
				return
			}

			if fileInfo.Mode().Perm()&(1<<uint(7)) == 0 {
				info.Info = fmt.Sprintf("%s must be writable", field)
				return
			}
		} else {
			info.Info = fmt.Sprintf("%s path does not exist", field)
			return
		}
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) validateLessThan(field1, field2 string, settings map[string]interface{}) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	if _, ok := settings[field1]; !ok {
		info.Info = fmt.Sprintf("%s does not exist", field1)
		return
	}

	if _, ok := settings[field2]; !ok {
		info.Info = fmt.Sprintf("%s does not exist", field2)
		return
	}

	if settings[field1].(float64) >= settings[field2].(float64) {
		info.Info = fmt.Sprintf("%s must be less than %s", field1, field2)
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) validateLocalAuth(w http.ResponseWriter, r *http.Request) bool {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		logging.Warnf("Unable to verify remote in request to %rs: %rs", r.URL, err)
		w.WriteHeader(http.StatusForbidden)
		return false
	}

	pip := net.ParseIP(ip)
	if pip == nil || !pip.IsLoopback() {
		logging.Warnf("Forbidden remote in request to %rs: %rs", r.URL, r)
		w.WriteHeader(http.StatusForbidden)
		return false
	}

	rUsr, rKey, ok := r.BasicAuth()
	if !ok {
		logging.Warnf("No credentials on request to %rs", r.URL)
		w.WriteHeader(http.StatusForbidden)
		return false
	}

	usr, key := util.LocalKey()
	if rUsr != usr || rKey != key {
		logging.Warnf("Cannot authorize request to %rs", r.URL)
		w.WriteHeader(http.StatusForbidden)
		return false
	}

	logging.Debugf("Allowing access to %rs", r.URL)
	return true
}

func (m *ServiceMgr) validateName(name, prefix string, maxLength int) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	if info = m.validateNonEmpty(name, prefix+"name"); info.Code != m.statusCodes.ok.Code {
		info.Info = fmt.Sprintf("%s name should not be empty", prefix)
		return
	}

	if len(name) > maxLength {
		info.Code = m.statusCodes.errInvalidConfig.Code
		info.Info = fmt.Sprintf("%s name length must be less than %d", prefix, maxLength)
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) validateNonEmpty(value, prefix string) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	if value == "" {
		info.Info = fmt.Sprintf("%s should not be empty", prefix)
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) validateNumber(field string, settings map[string]interface{}) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	if val, ok := settings[field]; ok {
		if _, ok = val.(float64); !ok {
			info.Info = fmt.Sprintf("%s must be a number", field)
			return
		}
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) validatePositiveInteger(field string, settings map[string]interface{}) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	if val, ok := settings[field]; ok {
		if info = m.validateNumber(field, settings); info.Code != m.statusCodes.ok.Code {
			return
		}

		info.Code = m.statusCodes.errInvalidConfig.Code
		if val.(float64) <= 0 {
			info.Info = fmt.Sprintf("%s can not be zero or negative", field)
			return
		}

		if math.Trunc(val.(float64)) != val.(float64) {
			info.Info = fmt.Sprintf("%s must be a positive integer", field)
			return
		}
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) validatePossibleValues(field string, settings map[string]interface{}, possibleValues []string) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	if val, ok := settings[field]; ok && !util.Contains(val.(string), possibleValues) {
		info.Info = fmt.Sprintf("Invalid value for %s, possible values are %s", field, strings.Join(possibleValues, ", "))
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) validateSettings(settings map[string]interface{}) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	fillMissingWithDefaults(settings)

	// Handler related configurations
	if info = m.validateBoolean("processing_status", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validateBoolean("deployment_status", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("checkpoint_interval", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validateBoolean("cleanup_timers", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("cpp_worker_thread_count", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("cron_timers_per_doc", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("curl_timeout", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePossibleValues("dcp_stream_boundary", settings, []string{"everything", "from_now"}); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("deadline_timeout", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validateBoolean("enable_recursive_mutation", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("execution_timeout", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validateLessThan("execution_timeout", "deadline_timeout", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("feedback_batch_size", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("feedback_read_buffer_size", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validateZeroOrPositiveInteger("fuzz_offset", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("lcb_inst_capacity", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePossibleValues("log_level", settings, []string{"INFO", "ERROR", "WARNING", "DEBUG", "TRACE"}); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("skip_timer_threshold", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("sock_batch_size", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("tick_duration", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("timer_processing_tick_interval", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("worker_count", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("worker_feedback_queue_cap", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("worker_queue_cap", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("xattr_doc_timer_entry_prune_threshold", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	// Process related configuration
	if info = m.validateBoolean("breakpad_on", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	// Rebalance related configurations
	if info = m.validatePositiveInteger("vb_ownership_giveup_routine_count", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("vb_ownership_takeover_routine_count", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	// Application logging related configurations
	if info = m.validateDirPath("app_log_dir", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("app_log_max_size", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("app_log_max_files", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	// Doc timer configurations for plasma
	if info = m.validateBoolean("auto_swapper", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validateBoolean("enable_snapshot_smr", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("iterator_refresh_counter", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("lss_cleaner_max_threshold", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("lss_cleaner_threshold", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("lss_read_ahead_size", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("max_delta_chain_len", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("max_page_items", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("min_page_items", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("persist_interval", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validateBoolean("use_memory_manager", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	// DCP connection related configurations
	if info = m.validatePositiveInteger("data_chan_size", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("dcp_gen_chan_size", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	if info = m.validatePositiveInteger("dcp_num_connections", settings); info.Code != m.statusCodes.ok.Code {
		return
	}

	info.Code = m.statusCodes.ok.Code
	return
}

func (m *ServiceMgr) validateZeroOrPositiveInteger(field string, settings map[string]interface{}) (info *runtimeInfo) {
	info = &runtimeInfo{}
	info.Code = m.statusCodes.errInvalidConfig.Code

	if val, ok := settings[field]; ok {
		if info = m.validateNumber(field, settings); info.Code != m.statusCodes.ok.Code {
			return
		}

		if val.(float64) < 0 {
			info.Code = m.statusCodes.errInvalidConfig.Code
			info.Info = fmt.Sprintf("%s can not be negative", field)
			return
		}

		if math.Trunc(val.(float64)) != val.(float64) {
			info.Info = fmt.Sprintf("%s must be zero or positive integer", field)
			return
		}
	}

	info.Code = m.statusCodes.ok.Code
	return
}
