package servicemanager

import (
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/common/collections"
	couchbase "github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/parser"
	"github.com/couchbase/eventing/rbac"
	"github.com/couchbase/eventing/service_manager/response"
	"github.com/couchbase/eventing/util"
)

func (m *ServiceMgr) sanitiseApplication(app *application) {
	for idx := 0; idx < len(app.DeploymentConfig.Buckets); idx++ {
		if app.DeploymentConfig.Buckets[idx].Access == "" {
			if app.DeploymentConfig.SourceBucket == app.DeploymentConfig.Buckets[idx].BucketName {
				app.DeploymentConfig.Buckets[idx].Access = "r"
			} else {
				app.DeploymentConfig.Buckets[idx].Access = "rw"
			}
		}
	}
}

func (m *ServiceMgr) validateAppRecursion(app *application) (info *response.RuntimeInfo) {
	logPrefix := "ServiceMgr::validateAppRecursion"

	info = &response.RuntimeInfo{}

	var config common.Config
	if config, info = m.getConfig(); info.ErrCode != response.Ok {
		return
	}

	var allowInterBucketRecursion bool
	if flag, ok := config["allow_interbucket_recursion"]; ok {
		allowInterBucketRecursion = flag.(bool)
	}

	appLocation := getAppLocationFromApp(app)
	if allowInterBucketRecursion == false {
		if appName, deployable := m.isAppDeployable(appLocation, app); !deployable {
			info.ErrCode = response.ErrInterFunctionRecursion
			info.Description = fmt.Sprintf("Inter handler recursion error with app: %s", appName)
			return
		}
	}

	source, destinations := m.getSourceAndDestinationsFromDepCfg(&app.DeploymentConfig)
	_, pinfos := parser.TranspileQueries(app.AppHandlers, "")

	// Prevent deployment of handler with N1QL writing to source
	for _, pinfo := range pinfos {
		if pinfo.PInfo.KeyspaceName != "" {
			dest := ConstructKeyspace(pinfo.PInfo.KeyspaceName)
			if dest == source {
				info.ErrCode = response.ErrInterBucketRecursion
				info.Description = fmt.Sprintf("Function: %s N1QL DML to source keyspace %s", appLocation, pinfo.PInfo.KeyspaceName)
				logging.Errorf("%s %s", logPrefix, info.Description)
				return
			}
			destinations[dest] = struct{}{}
		}
	}

	if !allowInterBucketRecursion && len(destinations) != 0 {
		if possible, path := m.graph.isAcyclicInsertPossible(appLocation, source, destinations); !possible {
			info.ErrCode = response.ErrInterBucketRecursion
			info.Description = fmt.Sprintf("Inter bucket recursion error; function: %s causes a cycle "+
				"involving functions: %v, hence deployment is disallowed", appLocation, path)
			return
		}

		functions := m.graph.getAcyclicInsertSideEffects(destinations)
		if len(functions) > 0 {
			info.WarningInfo = fmt.Sprintf("Function %s will modify source buckets of following functions %v", appLocation, functions)
		}
	}

	return
}

func (m *ServiceMgr) validateApplication(app *application) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}
	logPrefix := "ServiceMgr::validateApplication"

	m.sanitiseApplication(app)

	if info = m.validateApplicationName(app.Name); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateDeploymentConfig(&app.DeploymentConfig); info.ErrCode != response.Ok {
		return
	}

	if _, _, info = m.getBSId(&app.FunctionScope); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateNonEmpty(app.AppHandlers, "Function handler"); info.ErrCode != response.Ok {
		return
	}

	appLocation := getAppLocationFromApp(app)
	if info = m.validateSettings(appLocation, util.DeepCopy(app.Settings)); info.ErrCode != response.Ok {
		return
	}

	if val, ok := app.Settings["processing_status"].(bool); ok && val {
		if info = m.validateAppRecursion(app); info.ErrCode != response.Ok {
			logging.Errorf("%s Function: %s recursion error %d: %s", logPrefix, appLocation, info.ErrCode, info.Description)
			return
		}
	}

	return
}

func (m *ServiceMgr) validateAliasName(aliasName string) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if info = m.validateName(aliasName, "Alias", maxAliasLength); info.ErrCode != response.Ok {
		return
	}

	// Obtained from Variables - https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Grammar_and_types
	identifier := regexp.MustCompile("^[a-zA-Z_$][a-zA-Z0-9_$]*$")
	if !identifier.MatchString(aliasName) {
		info.ErrCode = response.ErrInvalidConfig
		info.Description = "Alias must be a valid JavaScript variable"
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
		info.ErrCode = response.ErrInvalidConfig
		info.Description = "Alias must not be a JavaScript reserved word"
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
		info.ErrCode = response.ErrInvalidConfig
		info.Description = "Alias must not be a N1QL reserved word"
		return
	}

	return
}

func (m *ServiceMgr) validateApplicationName(applicationName string) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if info = m.validateName(applicationName, "Function", maxApplicationNameLength); info.ErrCode != response.Ok {
		return
	}

	appNameRegex := regexp.MustCompile("^[a-zA-Z0-9][a-zA-Z0-9_-]*$")
	if !appNameRegex.MatchString(applicationName) {
		info.ErrCode = response.ErrInvalidConfig
		info.Description = "Function name can only start with characters in range A-Z, a-z, 0-9 and can only contain characters in range A-Z, a-z, 0-9, underscore and hyphen"
		return
	}

	return
}

func (m *ServiceMgr) validateBoolean(field string, isOptional bool, settings map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if val, ok := settings[field]; ok {
		if _, ok = val.(bool); !ok {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s must be a boolean", field)
			return
		}
	} else if !isOptional {
		info.ErrCode = response.ErrInvalidConfig
		info.Description = fmt.Sprintf("%s is required", field)
		return
	}

	return
}

func (m *ServiceMgr) validateKeyspaceExists(bucketName, scopeName, collectionName string, wildcardsAllowed bool) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if scopeName == "*" && collectionName != "*" {
		info.ErrCode = response.ErrInvalidRequest
		info.Description = fmt.Sprintf("Invalid keyspace. collection should be '*' when scope is '*'")
		return
	}

	nsServerEndpoint := net.JoinHostPort(util.Localhost(), m.restPort)
	found, err := util.ValidateAndCheckKeyspaceExist(bucketName, scopeName, collectionName, nsServerEndpoint, wildcardsAllowed)
	if err == util.ErrWildcardNotAllowed {
		info.ErrCode = response.ErrInvalidRequest
		info.Description = fmt.Sprintf("wildcard keyspace not allowed")
		return
	}

	if err != nil {
		info.ErrCode = response.ErrInternalServer
		info.Description = fmt.Sprintf("Failed to get cluster info cache, err: %v", err)
		return
	}

	if !found {
		info.ErrCode = response.ErrCollectionMissing
		info.Description = fmt.Sprintf("bucket: %s scope: %s collection: %s", bucketName, scopeName, collectionName)
		return
	}
	return
}

func (m *ServiceMgr) validateFunctionFeatures(c map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if info = m.validateBoolean(common.DisableCurl, true, c); info.ErrCode != response.Ok {
		return
	}

	return
}

func (m *ServiceMgr) validateConfig(c map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if info = m.validateBoolean("enable_debugger", true, c); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateFunctionFeatures(c); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateNumberRange("ram_quota", c, 256.0, nil); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("function_size", c); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("metakv_max_doc_size", c); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("http_request_timeout", c); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateBoolean("enable_lifecycle_ops_during_rebalance", true, c); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateBoolean("force_compress", true, c); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateBoolean("allow_interbucket_recursion", true, c); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("service_notifier_timeout", c); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateBoolean("auto_redistribute_vbs_on_failover", true, c); info.ErrCode != response.Ok {
		return
	}

	return
}

func (m *ServiceMgr) validateStorageEngine(bucketName string) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	nsServerEndpoint := net.JoinHostPort(util.Localhost(), m.restPort)
	cic, err := util.FetchClusterInfoClient(nsServerEndpoint)
	if err != nil {
		info.ErrCode = response.ErrInternalServer
		info.Description = fmt.Sprintf("Failed to get cluster info cache, err: %v", err)
		return
	}
	clusterInfo := cic.GetClusterInfoCache()
	clusterInfo.RLock()
	defer clusterInfo.RUnlock()

	se, err := clusterInfo.StorageEngine(bucketName)
	if err != nil {
		info.ErrCode = response.ErrBucketTypeCheck
		info.Description = fmt.Sprintf("Failed to check storage backend for bucket: %s err: %v", bucketName, err)
		return
	}

	if se == common.Magma {
		info.ErrCode = response.ErrMagmaStorage
		info.Description = fmt.Sprintf("Magma buckets are not yet supported in Eventing")
		return
	}

	return
}

func (m *ServiceMgr) validateNonMemcached(bucketName string) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	nsServerEndpoint := net.JoinHostPort(util.Localhost(), m.restPort)
	cic, err := util.FetchClusterInfoClient(nsServerEndpoint)
	if err != nil {
		info.ErrCode = response.ErrInternalServer
		info.Description = fmt.Sprintf("Failed to get cluster info cache, err: %v", err)
		return
	}
	clusterInfo := cic.GetClusterInfoCache()
	clusterInfo.RLock()
	defer clusterInfo.RUnlock()

	isMemcached, err := clusterInfo.IsMemcached(bucketName)
	if err != nil {
		info.ErrCode = response.ErrBucketTypeCheck
		info.Description = fmt.Sprintf("Failed to check bucket type using cluster info cache, err: %v", err)
		return
	}

	if isMemcached {
		info.ErrCode = response.ErrMemcachedBucket
		info.Description = fmt.Sprintf("Bucket %s is memcached, should be either couchbase or ephemeral", bucketName)
		return
	}

	return
}

func (m *ServiceMgr) validateBucketAccess(access string) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}
	if access == "r" || access == "rw" {
		return
	}
	info.ErrCode = response.ErrBucketAccess
	info.Description = fmt.Sprintf("Invalid bucket access, should be either \"r\" or \"rw\"")
	return
}

func (m *ServiceMgr) validateDeploymentConfig(deploymentConfig *depCfg) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}
	allowWildcards := true

	if info = m.validateNonEmpty(deploymentConfig.SourceBucket, "Source bucket name"); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateKeyspaceExists(deploymentConfig.SourceBucket, deploymentConfig.SourceScope, deploymentConfig.SourceCollection, allowWildcards); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateNonMemcached(deploymentConfig.SourceBucket); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateNonEmpty(deploymentConfig.MetadataBucket, "Metadata bucket name"); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateKeyspaceExists(deploymentConfig.MetadataBucket, deploymentConfig.MetadataScope, deploymentConfig.MetadataCollection, !allowWildcards); info.ErrCode != response.Ok {
		return
	}

	aliasSet := make(map[string]struct{})
	if info = m.validateBucketBindings(deploymentConfig.Buckets, aliasSet); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateCurlBindings(deploymentConfig.Curl, aliasSet); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateConstantBindings(deploymentConfig.Constants, aliasSet); info.ErrCode != response.Ok {
		return
	}

	return
}

func (m *ServiceMgr) getBSId(fS *common.FunctionScope) (string, uint32, *response.RuntimeInfo) {
	info := &response.RuntimeInfo{}

	if (fS.BucketName == "" && fS.ScopeName == "") || (fS.BucketName == "*" && fS.ScopeName == "*") {
		return "", 0, info
	}

	// Either both should be empty or none
	if info = m.validateNonEmpty(fS.BucketName, "functionScope bucket name"); info.ErrCode != response.Ok {
		return "", 0, info
	}

	if info = m.validateNonEmpty(fS.ScopeName, "functionScope scope name"); info.ErrCode != response.Ok {
		return "", 0, info
	}

	bucketUUID, scopeId, err := util.CheckAndGetBktAndScopeIDs(fS, m.restPort)
	if err == couchbase.ErrBucketNotFound || err == collections.SCOPE_NOT_FOUND {
		info.ErrCode = response.ErrBucketMissing
		info.Description = fmt.Sprintf("Missing Scope %v", err)
		return "", 0, info
	}

	if err != nil {
		info.ErrCode = response.ErrInternalServer
		return "", 0, info
	}

	return bucketUUID, scopeId, info
}

func (m *ServiceMgr) validateBucketBindings(bindings []bucket, existingAliases map[string]struct{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}
	allowWildcards := true

	for _, binding := range bindings {
		if info = m.validateNonEmpty(binding.BucketName, "Bucket name"); info.ErrCode != response.Ok {
			return
		}
		if info = m.validateAliasName(binding.Alias); info.ErrCode != response.Ok {
			return
		}

		if info = m.validateKeyspaceExists(binding.BucketName, binding.ScopeName, binding.CollectionName, allowWildcards); info.ErrCode != response.Ok {
			info.Description = fmt.Sprintf("Keyspace bucket: %s scope: %s collection: %s used for binding: %s doesn't exist", binding.BucketName, binding.ScopeName, binding.CollectionName, binding.Alias)
			return
		}

		//Check for the uniqueness of alias name
		if _, exists := existingAliases[binding.Alias]; exists {
			info.Description = fmt.Sprintf("Bucket alias %s is not unique", binding.Alias)
			info.ErrCode = response.ErrInvalidConfig
			return
		}

		//Update AliasSet
		existingAliases[binding.Alias] = struct{}{}

		if info = m.validateBucketAccess(binding.Access); info.ErrCode != response.Ok {
			return
		}
	}

	return
}

func (m *ServiceMgr) validateCurlBindings(bindings []common.Curl, existingAliases map[string]struct{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	for _, binding := range bindings {
		if info = m.validateNonEmpty(binding.Value, "URL alias name"); info.ErrCode != response.Ok {
			return
		}
		if info = m.validateNonEmpty(binding.Hostname, fmt.Sprintf("URL alias %s hostname", binding.Value)); info.ErrCode != response.Ok {
			return
		}
		if info = m.validateNonEmpty(binding.AuthType, fmt.Sprintf(`URL alias %s "auth type"`, binding.Value)); info.ErrCode != response.Ok {
			return
		}
		if info = m.validateUrl(binding.Hostname); info.ErrCode != response.Ok {
			info.Description = fmt.Sprintf("Invalid URL for URL alias %s : %s", binding.Value, info.Description)
			return
		}
		if !util.Contains(binding.AuthType, []string{"no-auth", "basic", "bearer", "digest"}) {
			info.Description = fmt.Sprintf(`URL alias %s has invalid value for "auth type"`, binding.Value)
			info.ErrCode = response.ErrInvalidConfig
			return
		}
		if info = m.validateAliasName(binding.Value); info.ErrCode != response.Ok {
			return
		}

		if _, exists := existingAliases[binding.Value]; exists {
			info.Description = fmt.Sprintf("URL alias %s is not unique", binding.Value)
			info.ErrCode = response.ErrInvalidConfig
			return
		}
		existingAliases[binding.Value] = struct{}{}
	}
	return
}

func (m *ServiceMgr) validateConstantBindings(bindings []common.Constant, existingAliases map[string]struct{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	for _, binding := range bindings {
		if info = m.validateNonEmpty(binding.Value, "Constant binding alias"); info.ErrCode != response.Ok {
			return
		}
		if info = m.validateNonEmpty(binding.Literal, "Constant binding literal"); info.ErrCode != response.Ok {
			return
		}

		if info = m.validateAliasName(binding.Value); info.ErrCode != response.Ok {
			return
		}

		if _, exists := existingAliases[binding.Value]; exists {
			info.Description = fmt.Sprintf("Constant alias %s is not unique", binding.Value)
			info.ErrCode = response.ErrInvalidConfig
			return
		}
		existingAliases[binding.Value] = struct{}{}
	}
	return
}

func (m *ServiceMgr) validateUrl(u string) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if !(strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://")) {
		info.ErrCode = response.ErrInvalidConfig
		info.Description = fmt.Sprintf("URL starts with invalid scheme type. Please ensure URL starts with http:// or https://")
		return
	}

	_, err := url.ParseRequestURI(u)
	if err != nil {
		info.ErrCode = response.ErrInvalidConfig
		info.Description = fmt.Sprintf("%v", err)
		return
	}

	return
}

func (m *ServiceMgr) validateDirPath(field string, settings map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if val, ok := settings[field]; ok {
		path := val.(string)
		if fileInfo, err := os.Stat(path); err == nil {
			if !fileInfo.IsDir() {
				info.ErrCode = response.ErrInvalidConfig
				info.Description = fmt.Sprintf("%s must be a directory", field)
				return
			}

			if fileInfo.Mode().Perm()&(1<<uint(7)) == 0 {
				info.ErrCode = response.ErrInvalidConfig
				info.Description = fmt.Sprintf("%s must be writable", field)
				return
			}
		} else {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s path does not exist", field)
			return
		}
	}

	return
}

func (m *ServiceMgr) validateLessThan(field1, field2 string, multiplier int, settings map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if _, ok := settings[field1]; !ok {
		info.ErrCode = response.ErrInvalidConfig
		info.Description = fmt.Sprintf("%s does not exist", field1)
		return
	}

	if _, ok := settings[field2]; !ok {
		info.ErrCode = response.ErrInvalidConfig
		info.Description = fmt.Sprintf("%s does not exist", field2)
		return
	}

	if int(settings[field1].(float64)) >= int(settings[field2].(float64))*multiplier {
		info.ErrCode = response.ErrInvalidConfig
		info.Description = fmt.Sprintf("%s must be less than %s", field1, field2)
		return
	}

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

func (m *ServiceMgr) validateName(name, prefix string, maxLength int) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if info = m.validateNonEmpty(name, prefix+"name"); info.ErrCode != response.Ok {
		info.ErrCode = response.ErrInvalidConfig
		info.Description = fmt.Sprintf("%s name should not be empty", prefix)
		return
	}

	if len(name) > maxLength {
		info.ErrCode = response.ErrInvalidConfig
		info.Description = fmt.Sprintf("%s name length must be less than %d", prefix, maxLength)
		return
	}

	return
}

func (m *ServiceMgr) validateNonEmpty(value, prefix string) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if value == "" {
		info.ErrCode = response.ErrInvalidConfig
		info.Description = fmt.Sprintf("%s should not be empty", prefix)
		return
	}

	return
}

func (m *ServiceMgr) validateNumber(field string, settings map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if val, ok := settings[field]; ok {
		if _, ok = val.(float64); !ok {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s must be a number", field)
			return
		}
	}

	return
}

func (m *ServiceMgr) validateStringMustExist(field string, maxLength int, settings map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if val, ok := settings[field]; ok {
		var valStr string
		if valStr, ok = val.(string); !ok {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s must be a string", field)
			return
		}

		if len(valStr) == 0 {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s must not be empty", field)
			return
		}

		if len(valStr) > maxLength {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s must have no more than %d characters", field, maxLength)
			return
		}
	}

	return
}

func (m *ServiceMgr) validatePositiveInteger(field string, settings map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if val, ok := settings[field]; ok {
		if info = m.validateNumber(field, settings); info.ErrCode != response.Ok {
			return
		}

		if val.(float64) <= 0 {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s can not be zero or negative", field)
			return
		}

		if math.Trunc(val.(float64)) != val.(float64) {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s must be a positive integer", field)
			return
		}
	}

	return
}

func (m *ServiceMgr) validateNonNegativeInteger(field string, settings map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if val, ok := settings[field]; ok {
		if info = m.validateNumber(field, settings); info.ErrCode != response.Ok {
			return
		}

		if val.(float64) < 0 {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s can not be negative", field)
			return
		}

		if math.Trunc(val.(float64)) != val.(float64) {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s must be a non negative integer", field)
			return
		}
	}

	return
}

func (m *ServiceMgr) validateTimerContextSize(field string, settings map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if val, ok := settings[field]; ok {
		if val.(float64) > 20*1024*1024 {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s value can not be more than 20MB", field)
			return
		}

		if val.(float64) < 20 {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s value can not be less than 20 bytes", field)
			return
		}
	}

	return
}

func (m *ServiceMgr) validatePossibleValues(field string, settings map[string]interface{}, possibleValues []string) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if val, ok := settings[field]; ok && !util.Contains(val.(string), possibleValues) {
		info.ErrCode = response.ErrInvalidConfig
		info.Description = fmt.Sprintf("Invalid value for %s, possible values are %s", field, strings.Join(possibleValues, ", "))
		return
	}

	return
}

func (m *ServiceMgr) validateTimerPartitions(field string, settings map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if val, ok := settings[field]; ok {
		if val.(float64) < 1 || val.(float64) > 1024 {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s field value must be between 1 and 1024.", field)
			return
		}
	}
	return
}

func (m *ServiceMgr) validateSettings(appName string, settings map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	m.fillMissingWithDefaults(appName, settings)

	// Handler related configurations
	if info = m.validateBoolean("n1ql_prepare_all", false, settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateBoolean("allow_transaction_mutations", true, settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePossibleValues("language_compatibility", settings, common.LanguageCompatibility); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateStringMustExist("user_prefix", maxPrefixLength, settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateBoolean("processing_status", false, settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateBoolean("deployment_status", false, settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("checkpoint_interval", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("cpp_worker_thread_count", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("curl_max_allowed_resp_size", settings); info.ErrCode != response.Ok {
		return
	}

	dcpStreamBoundaryValues := []string{"everything", "from_now"}
	if info = m.validatePossibleValues("dcp_stream_boundary", settings, dcpStreamBoundaryValues); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("execution_timeout", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("feedback_batch_size", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("feedback_read_buffer_size", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateStringArray("handler_headers", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateStringArray("handler_footers", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("idle_checkpoint_interval", settings); info.ErrCode != response.Ok {
		return
	}

	logLevelValues := []string{"INFO", "ERROR", "WARNING", "DEBUG", "TRACE"}
	if info = m.validatePossibleValues("log_level", settings, logLevelValues); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("poll_bucket_interval", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("sock_batch_size", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("timer_context_size", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateTimerContextSize("timer_context_size", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("tick_duration", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("worker_count", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("worker_feedback_queue_cap", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("worker_queue_cap", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("worker_queue_mem_cap", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("worker_response_timeout", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("timer_queue_mem_cap", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("timer_queue_size", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("undeploy_routine_count", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("bucket_cache_size", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("bucket_cache_age", settings); info.ErrCode != response.Ok {
		return
	}

	// Rebalance related configurations
	if info = m.validatePositiveInteger("vb_ownership_giveup_routine_count", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("vb_ownership_takeover_routine_count", settings); info.ErrCode != response.Ok {
		return
	}

	// Application logging related configurations
	if info = m.validateDirPath("app_log_dir", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("app_log_max_size", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("app_log_max_files", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateBoolean("enable_applog_rotation", true, settings); info.ErrCode != response.Ok {
		return
	}

	// DCP connection related configurations
	if info = m.validatePositiveInteger("agg_dcp_feed_mem_cap", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("data_chan_size", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("dcp_window_size", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("dcp_gen_chan_size", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("dcp_num_connections", settings); info.ErrCode != response.Ok {
		return
	}

	// N1QL related configuration
	if info = m.validatePossibleValues("n1ql_consistency", settings, m.consistencyValues); info.ErrCode != response.Ok {
		return
	}

	if info = m.validatePositiveInteger("lcb_inst_capacity", settings); info.ErrCode != response.Ok {
		return
	}

	// libcouchbase configurations
	if info = m.validatePositiveInteger("lcb_timeout", settings); info.ErrCode != response.Ok {
		return
	}

	if info = m.validateNonNegativeInteger("lcb_retry_count", settings); info.ErrCode != response.Ok {
		return
	}

	//  Timer Partitions related configuration
	if info = m.validateTimerPartitions("num_timer_partitions", settings); info.ErrCode != response.Ok {
		return
	}

	return
}

func (m *ServiceMgr) validateStringArray(field string, settings map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if val, ok := settings[field]; ok {
		if values, ok := val.([]interface{}); ok {
			for i, value := range values {
				if _, ok := value.(string); !ok {
					info.ErrCode = response.ErrInvalidConfig
					info.Description = fmt.Sprintf("In %s element at index %d must be a string", field, i)
					return
				}
			}
		} else {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s must be a list of strings", field)
			return
		}
	}

	return
}

func (m *ServiceMgr) validateZeroOrPositiveInteger(field string, settings map[string]interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if val, ok := settings[field]; ok {
		if info = m.validateNumber(field, settings); info.ErrCode != response.Ok {
			return
		}

		if val.(float64) < 0 {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s can not be negative", field)
			return
		}

		if math.Trunc(val.(float64)) != val.(float64) {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s must be zero or positive integer", field)
			return
		}
	}

	return
}

// TODO: Should use internal fields and compare the uuid and cid
func (app *application) functionScopeEquals(tmpApp application) bool {
	fS := app.FunctionScope
	tmpFs := tmpApp.FunctionScope
	if fS.BucketName == "" && fS.ScopeName == "" {
		fS.BucketName = "*"
		fS.ScopeName = "*"
	}

	if tmpFs.BucketName == "" && tmpFs.ScopeName == "" {
		tmpFs.BucketName = "*"
		tmpFs.ScopeName = "*"
	}

	return (fS.BucketName == tmpFs.BucketName) && (fS.ScopeName == tmpFs.ScopeName)
}

func (m *ServiceMgr) checkPermissionWithOwner(app application) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	if app.Owner == nil || (app.Owner.User == "" && app.Owner.Domain == "") {
		return
	}

	fs := app.FunctionScope
	ks := fs.ToKeyspace()

	mPermission := rbac.HandlerManagePermissions(ks)
	sourceKeyspace := &common.Keyspace{BucketName: app.DeploymentConfig.SourceBucket,
		ScopeName:      app.DeploymentConfig.SourceScope,
		CollectionName: app.DeploymentConfig.SourceCollection,
	}

	metadataKeyspace := &common.Keyspace{BucketName: app.DeploymentConfig.MetadataBucket,
		ScopeName:      app.DeploymentConfig.MetadataScope,
		CollectionName: app.DeploymentConfig.MetadataCollection,
	}

	perms := append(mPermission, rbac.HandlerBucketPermissions(sourceKeyspace, metadataKeyspace)...)
	notAllowed, err := rbac.HasPermissions(app.Owner, perms, true)
	if err == rbac.ErrUserDeleted {
		info.ErrCode = response.ErrForbidden
		info.Description = fmt.Sprintf("User who created the function does not exist in the system")
		return
	}

	if err == rbac.ErrAuthorisation {
		info.ErrCode = response.ErrForbidden
		info.Description = fmt.Sprintf("Forbidden. User who created the function is missing all of the following permissions: %v", notAllowed)
		return
	}
	if err != nil {
		*info = getAuthErrorInfo(notAllowed, true, err)
		return
	}
	return
}

// validateNumberRange checks whether the arguement is a number within a given range, pass nil for unbounded
func (m *ServiceMgr) validateNumberRange(field string, settings map[string]interface{}, low interface{}, high interface{}) (info *response.RuntimeInfo) {
	info = &response.RuntimeInfo{}

	lowVal, lOk := low.(float64)
	highVal, hOk := high.(float64)

	if val, ok := settings[field]; ok {
		if info = m.validateNumber(field, settings); info.ErrCode != response.Ok {
			return
		}

		if lOk && val.(float64) < lowVal {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s cannot be less than %v", field, low)
			return
		}

		if hOk && val.(float64) > highVal {
			info.ErrCode = response.ErrInvalidConfig
			info.Description = fmt.Sprintf("%s cannot be greater than %v", field, high)
			return
		}
	}

	return
}
