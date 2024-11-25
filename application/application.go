package application

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	PASSWORD_MASK = "*****"
)

const (
	maxApplicationNameLength = 100

	GlobalValue              = "*"
	DefaultScopeOrCollection = "_default"
)

var (
	ErrInvalidState          = errors.New("invalid state provided")
	ErrStateTransNotPossible = errors.New("state transition not possible")
)

// For backward compatibility
type OldNamespace struct {
	BucketName string `json:"bucket"`
	ScopeName  string `json:"scope"`
}

// Old style application
type OldApplication struct {
	AppCode            string                 `json:"appcode"`
	DeploymentConfig   depCfg                 `json:"depcfg"`
	EventingVersion    string                 `json:"version"`
	EnforceSchema      bool                   `json:"enforce_schema"`
	FunctionID         uint32                 `json:"handleruuid"`
	FunctionInstanceID string                 `json:"function_instance_id"`
	AppName            string                 `json:"appname"`
	Settings           map[string]interface{} `json:"settings"`
	Metainfo           map[string]interface{} `json:"metainfo,omitempty"`
	FunctionScope      OldNamespace           `json:"function_scope"`
	Owner              *Owner                 `json:"owner,omitempty"` // Don't expose. Only used for unmarshaling
}

type depCfg struct {
	Buckets            []*bucket          `json:"buckets,omitempty"`
	Curl               []*CurlBinding     `json:"curl,omitempty"`
	Constants          []*ConstantBinding `json:"constants,omitempty"`
	SourceBucket       string             `json:"source_bucket"`
	SourceScope        string             `json:"source_scope"`
	SourceCollection   string             `json:"source_collection"`
	MetadataBucket     string             `json:"metadata_bucket"`
	MetadataScope      string             `json:"metadata_scope"`
	MetadataCollection string             `json:"metadata_collection"`
}

type bucket struct {
	Alias          string `json:"alias"`
	BucketName     string `json:"bucket_name"`
	ScopeName      string `json:"scope_name"`
	CollectionName string `json:"collection_name"`
	Access         string `json:"access"`
}

type credential struct {
	UserName  string `json:"username"`
	Password  string `json:"password"`
	BearerKey string `json:"bearer_key"`
}

// Implements Namespace and related functions
type Namespace struct {
	BucketName string `json:"bucket_name"`
	ScopeName  string `json:"scope_name"`
}

func NewNamespace(bucketName, scopeName string, wildcardAllowed bool) (Namespace, error) {
	namespace := Namespace{}

	switch bucketName {
	case "", GlobalValue:
		if !wildcardAllowed {
			return namespace, fmt.Errorf("wildcard not allowed")
		}

		namespace.BucketName = GlobalValue
		switch scopeName {
		case "", GlobalValue:
			namespace.ScopeName = GlobalValue
		default:
			return namespace, fmt.Errorf("scope can't be wildcard when bucketname is not wildcard")
		}

	default:
		namespace.BucketName = bucketName
		switch scopeName {
		case "", GlobalValue:
			if !wildcardAllowed {
				return namespace, fmt.Errorf("wildcard not allowed")
			}

			namespace.ScopeName = GlobalValue
		default:
			namespace.ScopeName = scopeName
		}
	}
	return namespace, nil
}

var (
	GlobalNamespace = Namespace{
		BucketName: DefaultScopeOrCollection,
		ScopeName:  DefaultScopeOrCollection,
	}
)

func GetNamespace(params map[string][]string, wildcard bool) (Namespace, error) {
	var bucketName, scopeName string
	val, _ := params["bucket"]
	if len(val) > 0 {
		bucketName = val[0]
	}

	val, _ = params["scope"]
	if len(val) > 0 {
		scopeName = val[0]
	}

	return NewNamespace(bucketName, scopeName, wildcard)
}

func NamespaceInQuery(params map[string][]string) bool {
	if _, ok := params["bucket"]; ok {
		return true
	}

	if _, ok := params["scope"]; ok {
		return true
	}

	return false
}

func (ns Namespace) ToOldNamespace() OldNamespace {
	return OldNamespace{
		BucketName: ns.BucketName,
		ScopeName:  ns.ScopeName,
	}
}

func (ns Namespace) Clone() (clone Namespace) {
	clone.BucketName = ns.BucketName
	clone.ScopeName = ns.ScopeName
	return
}

func (namespace Namespace) String() string {
	if namespace.BucketName == GlobalValue && namespace.ScopeName == GlobalValue {
		return GlobalValue
	}
	return fmt.Sprintf("%s/%s", namespace.BucketName, namespace.ScopeName)
}

func (namespace Namespace) IsWildcard() bool {
	return (namespace.ScopeName == GlobalValue)
}

func (n1 Namespace) ExactEquals(n2 Namespace) bool {
	return (n1.BucketName == n2.BucketName) && (n1.ScopeName == n2.ScopeName)
}

func (n1 Namespace) Match(n2 Namespace) bool {
	if n1.BucketName != n2.BucketName {
		return false
	}

	if n1.IsWildcard() || n2.IsWildcard() {
		return true
	}

	return (n1.ScopeName == n2.ScopeName)
}

// Implement Keyspace and its functions
type source int8

const (
	MetaKvStore source = iota
	RestApi
)

type Keyspace struct {
	Namespace
	CollectionName string `json:"collection_name"`
}

func (k Keyspace) String() string {
	return fmt.Sprintf("%s/%s", k.Namespace.String(), k.CollectionName)
}

func NewKeyspace(bucketName, scopeName, collectionName string, wildcardAllowed bool) (keyspace Keyspace, err error) {
	if bucketName == "" {
		bucketName = GlobalValue
	}

	if scopeName == "" {
		scopeName = DefaultScopeOrCollection
	}

	keyspace.Namespace, err = NewNamespace(bucketName, scopeName, wildcardAllowed)
	if err != nil {
		return
	}

	switch collectionName {
	case GlobalValue:
		if !wildcardAllowed {
			err = fmt.Errorf("wildcard not allowed")
			return
		}
		keyspace.CollectionName = GlobalValue

	default:
		if keyspace.Namespace.BucketName == GlobalValue || keyspace.Namespace.ScopeName == GlobalValue {
			err = fmt.Errorf("invalid use of wildcard")
			return
		}

		if collectionName == "" {
			collectionName = DefaultScopeOrCollection
		}

		keyspace.CollectionName = collectionName
	}

	return
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

func constructKeyspaceFromN1QL(n1qlKeyspace string) (keyspace Keyspace) {
	// var namespace string
	scope, collection := DefaultScopeOrCollection, DefaultScopeOrCollection

	n := strings.IndexByte(n1qlKeyspace, ':')
	_, n1qlKeyspace = trim(n1qlKeyspace, n)
	d := strings.IndexByte(n1qlKeyspace, '.')
	if d >= 0 {
		n1qlKeyspace, scope = trim(n1qlKeyspace, d)
		d = strings.IndexByte(scope, '.')
		if d >= 0 {
			scope, collection = trim(scope, d)
		}
	}

	keyspace.BucketName = n1qlKeyspace
	keyspace.ScopeName = scope
	keyspace.CollectionName = collection
	return
}

func (ks Keyspace) Clone() (clone Keyspace) {
	clone.Namespace = ks.Namespace.Clone()
	clone.CollectionName = ks.CollectionName
	return
}

type requestType uint8

const (
	RequestNone requestType = iota
	RequestBucket
	RequestScope
	RequestCollection
	// Extension: can add queue or http request here
)

func (ks Keyspace) RequestType() requestType {
	if ks.Namespace.IsWildcard() {
		return RequestBucket
	}

	if ks.IsWildcard() {
		return RequestScope
	}

	return RequestCollection
}

func (ks Keyspace) IsWildcard() bool {
	return (ks.Namespace.IsWildcard() || ks.CollectionName == GlobalValue)
}

func (ks1 Keyspace) ExactEquals(ks2 Keyspace) bool {
	return ks1.Namespace.ExactEquals(ks2.Namespace) && (ks1.CollectionName == ks2.CollectionName)
}

func (ks1 Keyspace) Match(ks2 Keyspace) bool {
	if !ks1.Namespace.Match(ks2.Namespace) {
		return false
	}

	if ks1.IsWildcard() || ks2.IsWildcard() {
		return true
	}

	return (ks1.CollectionName == ks2.CollectionName)
}

// Implement AppLocation and its functions
type AppLocation struct {
	Namespace Namespace `json:"function_scope"`
	Appname   string    `json:"app_name"`
}

func (appLocation AppLocation) ToLocationString() string {
	namespaceString := appLocation.Namespace.String()
	if namespaceString == GlobalValue {
		return appLocation.Appname
	}

	return fmt.Sprintf("%s/%s", namespaceString, appLocation.Appname)
}

func (appLocation AppLocation) String() string {
	return appLocation.ToLocationString()
}

func NewAppLocation(appName string, bucketName, scopeName string) (applocation AppLocation, err error) {
	if len(appName) == 0 {
		err = fmt.Errorf("app name should not be empty")
		return
	}

	if len(appName) > maxApplicationNameLength {
		err = fmt.Errorf("%s name length must be less than %d", appName, maxApplicationNameLength)
		return
	}

	if !appNameRegex.MatchString(appName) {
		err = fmt.Errorf("Function name can only start with characters in range A-Z, a-z, 0-9 and can only contain characters in range A-Z, a-z, 0-9, underscore and hyphen")
		return
	}

	applocation.Namespace, err = NewNamespace(bucketName, scopeName, true)
	applocation.Appname = appName
	return
}

func StringToAppLocation(location string) (appLocation AppLocation) {
	loc := strings.Split(location, "/")
	if len(loc) == 3 {
		appLocation.Namespace.BucketName = loc[0]
		appLocation.Namespace.ScopeName = loc[1]
		appLocation.Appname = loc[2]
		return
	}
	appLocation.Namespace.BucketName = GlobalValue
	appLocation.Namespace.ScopeName = GlobalValue
	appLocation.Appname = loc[0]
	return
}

func AppLocationInQuery(params map[string][]string) bool {
	if _, ok := params["appName"]; ok {
		return true
	}

	if _, ok := params["name"]; ok {
		return true
	}

	return NamespaceInQuery(params)
}

func GetLogDirectoryAndFileName(funcDetails *FunctionDetails, eventingDir string) (dirName, fileName string) {
	separator := string(os.PathSeparator)
	namespace := funcDetails.AppLocation.Namespace.String()
	dirName = funcDetails.Settings.AppLogDir
	filename := fmt.Sprintf("%s.log", funcDetails.AppLocation.Appname)

	if dirName == "" {
		dirName = eventingDir
	}
	if namespace != GlobalValue {
		dirName = fmt.Sprintf("%s%sb_%s%ss_%s", dirName, separator,
			funcDetails.AppLocation.Namespace.BucketName, separator, funcDetails.AppLocation.Namespace.ScopeName)
	}

	fileName = filepath.Join(dirName, filename)
	return
}

func QueryMapNamesapce(namespace Namespace) map[string][]string {
	bucketName := namespace.BucketName
	scopeName := namespace.ScopeName

	queryMap := make(map[string][]string)
	queryMap["bucket"] = []string{bucketName}
	queryMap["scope"] = []string{scopeName}

	return queryMap
}

func QueryMap(appLocation AppLocation) map[string][]string {
	queryMap := QueryMapNamesapce(appLocation.Namespace)
	appName := appLocation.Appname

	queryMap["appName"] = []string{appName}
	return queryMap
}

func GetApplocation(params map[string][]string) (appLocation AppLocation) {
	appLocation.Namespace, _ = GetNamespace(params, true)
	val, _ := params["appName"]
	if len(val) > 0 {
		appLocation.Appname = val[0]
	}

	if appLocation.Appname == "" {
		val, _ = params["name"]
		if len(val) > 0 {
			appLocation.Appname = val[0]
		}
	}

	return
}

func (appLocation AppLocation) Clone() (clone AppLocation) {
	clone.Namespace = appLocation.Namespace.Clone()
	clone.Appname = appLocation.Appname
	return
}

// Implement Settings and its functions
type langCompat string

const (
	lang720 = langCompat("7.2.0")
	lang662 = langCompat("6.6.2")
	lang600 = langCompat("6.0.0")
	lang650 = langCompat("6.5.0")
)

type n1qlConsistency string

const (
	NoConsistency = n1qlConsistency("")
	None          = n1qlConsistency("none")
	Request       = n1qlConsistency("request")
)

const (
	executionTimeoutJSON = "execution_timeout"
	onDeployTimeoutJSON  = "on_deploy_timeout"
	langCompatJSON       = "language_compatibility"
	lcbInstCapacityJSON  = "lcb_inst_capacity"
	lcbRetryCountJSON    = "lcb_retry_count"
	lcbTimeoutJSON       = "lcb_timeout"
	n1qlConsistencyJSON  = "n1ql_consistency"
	n1qlPrepareJSON      = "n1ql_prepare_all"
	handlerHeaderJSON    = "handler_headers"
	handlerFooterJSON    = "handler_footers"
	bucketCacheSizeJSON  = "bucket_cache_size"
	bucketCacheAgeJSON   = "bucket_cache_age"
	curlRespSizeJSON     = "curl_max_allowed_resp_size"
)

type LanguageRuntimeSettings struct {
	ExecutionTimeout uint64          `json:"execution_timeout"`
	OnDeployTimeout  uint64          `json:"on_deploy_timeout"`
	LanguageCompat   langCompat      `json:"language_compatibility"`
	LcbInstCapacity  uint32          `json:"lcb_inst_capacity"`
	LcbRetryCount    uint32          `json:"lcb_retry_count"`
	LcbTimeout       uint64          `json:"lcb_timeout"`
	N1qlConsistency  n1qlConsistency `json:"n1ql_consistency"`
	N1qlPrepare      bool            `json:"n1ql_prepare_all"`
	HandlerHeader    []string        `json:"handler_headers"`
	HandlerFooter    []string        `json:"handler_footers"`
	BucketCacheSize  uint64          `json:"bucket_cache_size"`
	BucketCacheAge   uint64          `json:"bucket_cache_age"`
	CurlRespSize     uint64          `json:"curl_max_allowed_resp_size"`
}

type streamBoundary string

const (
	Everyting = streamBoundary("everyting")
	FromNow   = streamBoundary("from_now")
	FromPrior = streamBoundary("from_prior")
)

type logLevel string

const (
	LogInfo  = logLevel("INFO")
	LogError = logLevel("ERROR")
	LogDebug = logLevel("DEBUG")
	LogTrace = logLevel("TRACE")
)

const (
	cppWorkerThreadJSON          = "cpp_worker_thread_count"
	dcpStreamBoundaryJSON        = "dcp_stream_boundary"
	descriptionJSON              = "description"
	logLevelJSON                 = "log_level"
	numTimerPartitionJSON        = "num_timer_partitions"
	statsDurationJSON            = "tick_duration"
	timerContextSizeJSON         = "timer_context_size"
	workerCountJSON              = "worker_count"
	applogDirJSON                = "app_log_dir"
	enableAppRotationJSON        = "enable_applog_rotation"
	appLogMaxSizeJSON            = "app_log_max_size"
	appLogMaxFileJSON            = "app_log_max_files"
	checkpointIntervalJSON       = "checkpoint_interval"
	cursorAwareJSON              = "cursor_aware"
	allowSyncDocumentJSON        = "allow_sync_documents"
	allowTransactionDocumentJSON = "allow_transaction_mutations"
	checkIntervalJSON            = "high_seq_check_interval"
	maxUnackedBytesJSON          = "max_unacked_bytes"
	maxUnackedCountJSON          = "max_unacked_count"
	flushTimerJSON               = "message_flush_time"
	maxParallelVbJSON            = "max_parallel_vb"
)

type InitSettings struct {
	CppWorkerThread          uint32         `json:"cpp_worker_thread_count"`
	DcpStreamBoundary        streamBoundary `json:"dcp_stream_boundary"`
	Description              string         `json:"description"`
	LogLevel                 logLevel       `json:"log_level"`
	NumTimerPartition        uint16         `json:"num_timer_partitions"`
	StatsDuration            uint64         `json:"tick_duration"`
	TimerContextSize         uint32         `json:"timer_context_size"`
	WorkerCount              uint32         `json:"worker_count"`
	AppLogDir                string         `json:"app_log_dir"`
	EnableAppRotation        bool           `json:"enable_applog_rotation"`
	AppLogMaxSize            uint64         `json:"app_log_max_size"`
	AppLogMaxFiles           uint32         `json:"app_log_max_files"`
	CheckpointInterval       uint64         `json:"checkpoint_interval"`
	CursorAware              bool           `json:"cursor_aware"`
	AllowSyncDocuments       bool           `json:"allow_sync_documents"`
	AllowTransactionDocument bool           `json:"allow_transaction_mutations"`

	CheckInterval   uint64  `json:"high_seq_check_interval"`
	MaxUnackedBytes float64 `json:"max_unacked_bytes"`
	MaxUnackedCount float64 `json:"max_unacked_count"`
	FlushTimer      uint64  `json:"message_flush_time"`
	MaxParallelVb   uint16  `json:"max_parallel_vb"`
}

type HandlerSettings struct {
	LanguageRuntimeSettings
	InitSettings
}

func (h HandlerSettings) String() string {
	jsonString, _ := json.Marshal(h)
	return string(jsonString)
}

func (hs *HandlerSettings) UnmarshalJSON(b []byte) error {
	settings := make(map[string]interface{})
	err := json.Unmarshal(b, &settings)
	if err != nil {
		return err
	}

	*hs = DefaultSettings()
	_, err = hs.verifyAndMergeSettings(nil, settings)

	return err
}

func (hs HandlerSettings) Clone() HandlerSettings {
	clonedHS := HandlerSettings{}
	clonedHS.CppWorkerThread = hs.CppWorkerThread
	clonedHS.DcpStreamBoundary = hs.DcpStreamBoundary
	clonedHS.Description = hs.Description
	clonedHS.LogLevel = hs.LogLevel
	clonedHS.NumTimerPartition = hs.NumTimerPartition
	clonedHS.StatsDuration = hs.StatsDuration
	clonedHS.TimerContextSize = hs.TimerContextSize
	clonedHS.WorkerCount = hs.WorkerCount
	clonedHS.AppLogDir = hs.AppLogDir
	clonedHS.EnableAppRotation = hs.EnableAppRotation
	clonedHS.AppLogMaxSize = hs.AppLogMaxSize
	clonedHS.AppLogMaxFiles = hs.AppLogMaxFiles
	clonedHS.CheckpointInterval = hs.CheckpointInterval
	clonedHS.AllowSyncDocuments = hs.AllowSyncDocuments
	clonedHS.AllowTransactionDocument = hs.AllowTransactionDocument
	clonedHS.CursorAware = hs.CursorAware
	clonedHS.CheckInterval = hs.CheckInterval
	clonedHS.MaxUnackedBytes = hs.MaxUnackedBytes
	clonedHS.MaxParallelVb = hs.MaxParallelVb
	clonedHS.MaxUnackedCount = hs.MaxUnackedCount
	clonedHS.FlushTimer = hs.FlushTimer

	clonedHS.ExecutionTimeout = hs.ExecutionTimeout
	clonedHS.OnDeployTimeout = hs.OnDeployTimeout
	clonedHS.LanguageCompat = hs.LanguageCompat
	clonedHS.LcbInstCapacity = hs.LcbInstCapacity
	clonedHS.LcbRetryCount = hs.LcbRetryCount
	clonedHS.LcbTimeout = hs.LcbTimeout
	clonedHS.N1qlConsistency = hs.N1qlConsistency
	clonedHS.N1qlPrepare = hs.N1qlPrepare

	clonedHS.HandlerHeader = make([]string, 0, len(hs.HandlerHeader))
	clonedHS.HandlerHeader = append(clonedHS.HandlerHeader, hs.HandlerHeader...)
	clonedHS.HandlerFooter = make([]string, 0, len(hs.HandlerFooter))
	clonedHS.HandlerFooter = append(clonedHS.HandlerFooter, hs.HandlerFooter...)

	clonedHS.BucketCacheSize = hs.BucketCacheSize
	clonedHS.BucketCacheAge = hs.BucketCacheAge
	clonedHS.CurlRespSize = hs.CurlRespSize

	return clonedHS
}

func DefaultSettings() HandlerSettings {
	hSettings := HandlerSettings{}
	hSettings.CppWorkerThread = 2
	hSettings.DcpStreamBoundary = Everyting
	hSettings.LogLevel = LogInfo
	hSettings.NumTimerPartition = 256
	hSettings.StatsDuration = 1000
	hSettings.TimerContextSize = 1024
	hSettings.WorkerCount = 1
	hSettings.AppLogDir = ""
	hSettings.EnableAppRotation = true
	hSettings.AppLogMaxSize = 41943040
	hSettings.AppLogMaxFiles = 10
	hSettings.CheckpointInterval = 60
	hSettings.AllowSyncDocuments = true
	hSettings.AllowTransactionDocument = false
	hSettings.CursorAware = false
	hSettings.CheckInterval = 50
	hSettings.MaxUnackedBytes = float64(61 * 1024 * 1024)
	hSettings.MaxUnackedCount = float64(100000)
	hSettings.FlushTimer = 50
	hSettings.MaxParallelVb = 8

	// runtime related settings
	hSettings.ExecutionTimeout = 60
	hSettings.OnDeployTimeout = 60
	hSettings.LanguageCompat = lang720
	hSettings.LcbInstCapacity = 10
	hSettings.LcbRetryCount = 0
	hSettings.LcbTimeout = 5
	hSettings.N1qlConsistency = NoConsistency
	hSettings.N1qlPrepare = false
	hSettings.HandlerHeader = []string{"'use strict';"}
	hSettings.HandlerFooter = make([]string, 0)
	hSettings.BucketCacheSize = 67108864
	hSettings.BucketCacheAge = 1000
	hSettings.CurlRespSize = 100

	return hSettings
}

const (
	deploymentStatusJSON = "deployment_status"
	processingStateJSON  = "processing_status"
)

// Implement AppState and its function
type AppState struct {
	ProcessingState bool `json:"processing_status"`
	DeploymentState bool `json:"deployment_status"`
}

func (a AppState) String() string {
	lifeCycleOp := a.GetLifeCycle()
	return lifeCycleOp.String()
}

type LifeCycleOp uint8

const (
	Undeploy      LifeCycleOp = iota //00
	Pause                            // 01
	InvalidState                     // 10
	Deploy                           // 11
	NoLifeCycleOp                    // invalid
)

func (l LifeCycleOp) String() string {
	switch l {
	case Undeploy:
		return "Undeploy"
	case Pause:
		return "Pause"
	case Deploy:
		return "Deploy"
	}
	return "InvalidState"
}

type State uint8

const (
	NoState State = iota
	Undeployed
	Undeploying
	Deployed
	Deploying
	Paused
	Pausing
	Resuming
)

func (s State) String() string {
	switch s {
	case Undeployed:
		return "undeployed"

	case Undeploying:
		return "undeploying"

	case Deployed:
		return "deployed"

	case Deploying:
		return "deploying"

	case Paused:
		return "paused"

	case Pausing:
		return "pausing"

	case Resuming:
		return "resuming"

	case NoState:
		return "not_found"

	}
	return ""
}

func StringToAppState(state string) State {
	switch state {
	case "undeployed":
		return Undeployed
	case "undeploying":
		return Undeploying
	case "deployed":
		return Deployed
	case "deploying":
		return Deploying
	case "paused":
		return Paused
	case "pausing":
		return Pausing
	case "resuming":
		return Resuming
	}
	return NoState
}

func BootstrappingStatus(curr, prev State) State {
	switch curr {
	case Undeployed:
		return Undeploying

	case Paused:
		return Pausing

	case Deployed:
		if prev == Paused {
			return Resuming
		}
		return Deploying
	}

	return curr
}

func (s State) BootstrapingString() string {
	state := BootstrappingStatus(s, NoState)
	return state.String()
}

func (s State) IsDeployed() bool {
	return (s == Deploying || s == Deployed || s == Resuming)
}

func (s State) IsStateTransferDone() bool {
	return (s == Undeployed || s == Deployed || s == Paused)
}

func GetStateFromLifeCycle(op LifeCycleOp) State {
	state := NoState
	switch op {
	case Deploy:
		state = Deployed

	case Pause:
		state = Paused

	case Undeploy:
		state = Undeployed
	}

	return state
}

func (s State) IsBootstrapping() bool {
	return (s == Deploying || s == Undeploying || s == Pausing || s == Resuming)
}

func (as *AppState) UnmarshalJSON(b []byte) error {
	appStateMap := make(map[string]interface{})
	err := json.Unmarshal(b, &appStateMap)
	if err != nil {
		return err
	}

	*as = AppState{}
	_, err = as.VerifyAndMergeAppState(appStateMap)
	return err
}

func (as AppState) Clone() (clone AppState) {
	clone.ProcessingState = as.ProcessingState
	clone.DeploymentState = as.DeploymentState
	return
}

func (e AppState) Equals(e2 AppState) bool {
	return (e.ProcessingState == e2.ProcessingState) &&
		(e.DeploymentState == e2.DeploymentState)
}

func (e AppState) GetLifeCycle() LifeCycleOp {
	var dState, pState uint8
	if e.DeploymentState {
		dState = uint8(1)
	}

	if e.ProcessingState {
		pState = uint8(1)
	}

	return LifeCycleOp(dState | pState<<1)
}

// Implement DepCfg and its function
type DepCfg struct {
	SourceKeyspace Keyspace `json:"source_keyspace"`
	MetaKeyspace   Keyspace `json:"meta_keyspace"`
}

func (d DepCfg) String() string {
	return fmt.Sprintf("{ \"source_keyspace\": %s, \"meta_keyspace\": %s }", d.SourceKeyspace, d.MetaKeyspace)
}

func (depcfg DepCfg) Clone() (clone DepCfg) {
	clone.SourceKeyspace = depcfg.SourceKeyspace.Clone()
	clone.MetaKeyspace = depcfg.MetaKeyspace.Clone()
	return
}

func (depcfg DepCfg) RequestType() requestType {
	return depcfg.SourceKeyspace.RequestType()
}

type bindingType uint8

const (
	Bucket bindingType = iota
	Curl
	Constant
)

type access string

const (
	readWrite = access("rw")
	read      = access("r")
)

type BucketBinding struct {
	Alias      string   `json:"alias"`
	Keyspace   Keyspace `json:"keyspace"`
	AccessType access   `json:"access"`
}

func (b BucketBinding) String() string {
	return fmt.Sprintf("{ \"binding_type\": \"bucketBinding\", \"alias\": %s, \"keyspace\": %s, \"access\": %s }", b.Alias, b.Keyspace, b.AccessType)
}

func NewBucketBinding(alias, bucketName, scopeName, collectionName string, accessType access) (Bindings, error) {
	binding := Bindings{BindingType: Bucket}

	keyspace, err := NewKeyspace(bucketName, scopeName, collectionName, true)
	if err != nil {
		return binding, err
	}

	if accessType != readWrite && accessType != read {
		accessType = readWrite
	}

	err = validateAliasName(alias)
	if err != nil {
		return binding, err
	}

	bb := &BucketBinding{
		Keyspace:   keyspace,
		AccessType: accessType,
		Alias:      alias,
	}
	binding.BucketBinding = bb

	return binding, nil
}

func (bb *BucketBinding) ExactEquals(obb *BucketBinding) bool {
	return bb.Alias == obb.Alias &&
		bb.Keyspace.ExactEquals(obb.Keyspace) &&
		bb.AccessType == obb.AccessType
}

func (bb *BucketBinding) Clone() (clone *BucketBinding) {
	clone = &BucketBinding{}

	clone.Alias = bb.Alias
	clone.Keyspace = bb.Keyspace.Clone()
	clone.AccessType = bb.AccessType

	return
}

type authType string

var (
	NoAuth = authType("no-auth")
	Basic  = authType("basic")
	Beared = authType("bearer")
	Digest = authType("digest")
)

type CurlBinding struct {
	HostName    string   `json:"hostname"`
	Alias       string   `json:"value"`
	AuthType    authType `json:"auth_type"`
	AllowCookie bool     `json:"allow_cookies"`
	ValidateSSL bool     `json:"validate_ssl_certificate"`
	UserName    string   `json:"username"`
	Password    string   `json:"password"`
	BearerKey   string   `json:"bearer_key"`
}

func (c CurlBinding) String() string {
	return fmt.Sprintf("{ \"binding_type\": \"curlBinding\", \"alias\": %s, \"hostname\": %s, \"auth_type\": %s }", c.Alias, c.HostName, c.AuthType)
}

func NewCurlBinding(curlBinding *CurlBinding) (Bindings, error) {
	binding := Bindings{BindingType: Curl}

	err := validateAliasName(curlBinding.Alias)
	if err != nil {
		return binding, err
	}

	if len(curlBinding.HostName) == 0 {
		return binding, fmt.Errorf("URL alias %s hostname is empty", curlBinding.Alias)
	}

	if !(strings.HasPrefix(curlBinding.HostName, "http://") || strings.HasPrefix(curlBinding.HostName, "https://")) {
		return binding, fmt.Errorf("URL starts with invalid scheme type. Please ensure URL starts with http:// or https://")
	}

	_, err = url.ParseRequestURI(curlBinding.HostName)
	if err != nil {
		return binding, fmt.Errorf("%v", err)
	}

	switch curlBinding.AuthType {
	case NoAuth, Basic, Beared, Digest:
		// maybe action can be taken based on auth type
	default:
		return binding, fmt.Errorf("Invalid auth type")
	}

	binding.CurlBinding = curlBinding
	return binding, err
}

func (cb *CurlBinding) ExactEquals(ocb *CurlBinding) bool {
	return cb.HostName == ocb.HostName &&
		cb.Alias == ocb.Alias &&
		cb.AuthType == ocb.AuthType &&
		cb.AllowCookie == ocb.AllowCookie &&
		cb.ValidateSSL == ocb.ValidateSSL &&
		cb.UserName == ocb.UserName &&
		cb.Password == ocb.Password &&
		cb.BearerKey == ocb.BearerKey
}

func (cb *CurlBinding) Clone(redact bool) (clone *CurlBinding) {
	clone = &CurlBinding{}

	clone.HostName = cb.HostName
	clone.Alias = cb.Alias
	clone.AllowCookie = cb.AllowCookie
	clone.ValidateSSL = cb.ValidateSSL
	clone.BearerKey = PASSWORD_MASK
	clone.Password = PASSWORD_MASK

	if !redact {
		clone.AuthType = cb.AuthType
		clone.UserName = cb.UserName
		clone.Password = cb.Password
		clone.BearerKey = cb.BearerKey
	}

	return
}

type ConstantBinding struct {
	Value string `json:"literal"`
	Alias string `json:"value"`
}

func (c ConstantBinding) String() string {
	return fmt.Sprintf("{ \"binding_type\": \"constantBinding\", \"alias\": %s, \"value\": %s }", c.Alias, c.Value)
}

func NewConstantBinding(constant *ConstantBinding) (Bindings, error) {
	binding := Bindings{BindingType: Constant}

	err := validateAliasName(constant.Alias)
	if err != nil {
		return binding, err
	}

	binding.ConstantBinding = constant
	return binding, nil
}

func (cb *ConstantBinding) ExactEquals(ocb *ConstantBinding) bool {
	return (cb.Value == ocb.Value) && (cb.Alias == ocb.Alias)
}

func (cb *ConstantBinding) Clone() (clone *ConstantBinding) {
	clone = &ConstantBinding{}

	clone.Value = cb.Value
	clone.Alias = cb.Alias
	return
}

type Bindings struct {
	BindingType     bindingType      `json:"binding_type"`
	CurlBinding     *CurlBinding     `json:"curl_binding,omitempty"`
	BucketBinding   *BucketBinding   `json:"bucket_binding,omitempty"`
	ConstantBinding *ConstantBinding `json:"constant_binding,omitempty"`
}

func (b Bindings) String() string {
	switch b.BindingType {
	case Bucket:
		return b.BucketBinding.String()

	case Curl:
		return b.CurlBinding.String()

	case Constant:
		return b.ConstantBinding.String()
	}
	return ""
}

func (binding Bindings) Clone(redact bool) (clone Bindings) {
	clone.BindingType = binding.BindingType

	switch clone.BindingType {
	case Bucket:
		clone.BucketBinding = binding.BucketBinding.Clone()
	case Curl:
		clone.CurlBinding = binding.CurlBinding.Clone(redact)
	case Constant:
		clone.ConstantBinding = binding.ConstantBinding.Clone()
	}

	return
}

func (nBinding Bindings) ExactEquals(oBinding Bindings) bool {
	if nBinding.BindingType != oBinding.BindingType {
		return false
	}

	switch oBinding.BindingType {
	case Bucket:
		return nBinding.BucketBinding.ExactEquals(oBinding.BucketBinding)

	case Curl:
		return nBinding.CurlBinding.ExactEquals(oBinding.CurlBinding)

	case Constant:
		return nBinding.ConstantBinding.ExactEquals(oBinding.ConstantBinding)
	}

	return true
}

func createStructFromDepcfg(oldDepcfg depCfg) (newDepcfg DepCfg, bindings []Bindings, err error) {
	newDepcfg.SourceKeyspace, err = NewKeyspace(oldDepcfg.SourceBucket, oldDepcfg.SourceScope, oldDepcfg.SourceCollection, true)
	if err != nil {
		return
	}

	newDepcfg.MetaKeyspace, err = NewKeyspace(oldDepcfg.MetadataBucket, oldDepcfg.MetadataScope, oldDepcfg.MetadataCollection, false)
	if err != nil {
		return
	}

	// Create Bindings
	bindings = make([]Bindings, 0, len(oldDepcfg.Buckets)+len(oldDepcfg.Curl)+len(oldDepcfg.Constants))
	for _, bucketBinding := range oldDepcfg.Buckets {
		binding, bErr := NewBucketBinding(bucketBinding.Alias,
			bucketBinding.BucketName,
			bucketBinding.ScopeName,
			bucketBinding.CollectionName,
			access(bucketBinding.Access))
		if bErr != nil {
			err = bErr
			return
		}
		bindings = append(bindings, binding)
	}

	for _, curlBinding := range oldDepcfg.Curl {
		binding, cErr := NewCurlBinding(curlBinding)
		if cErr != nil {
			err = cErr
			return
		}
		bindings = append(bindings, binding)
	}

	for _, constantBinding := range oldDepcfg.Constants {
		binding, cErr := NewConstantBinding(constantBinding)
		if cErr != nil {
			err = cErr
			return
		}
		bindings = append(bindings, binding)
	}

	return
}

type KeyspaceInfo struct {
	UID          string
	CollectionID string
	ScopeID      string
	BucketID     string
	NumVbuckets  uint16
}

func NewKeyspaceInfo(UID, bucketID, scopeID, collectionID string, numVbuckets uint16) (keyspaceInfo KeyspaceInfo) {
	if UID == GlobalValue {
		UID = ""
	}
	if bucketID == "" {
		bucketID = GlobalValue
	}
	if scopeID == "" {
		scopeID = GlobalValue
	}
	if collectionID == "" {
		collectionID = GlobalValue
	}

	keyspaceInfo.BucketID = bucketID
	keyspaceInfo.ScopeID = scopeID
	keyspaceInfo.CollectionID = collectionID
	keyspaceInfo.UID = UID
	keyspaceInfo.NumVbuckets = numVbuckets
	return
}
func (k KeyspaceInfo) ExactEquals(ok KeyspaceInfo) bool {
	return k.UID == ok.UID &&
		k.CollectionID == ok.CollectionID &&
		k.ScopeID == ok.ScopeID &&
		k.BucketID == ok.BucketID &&
		k.NumVbuckets == ok.NumVbuckets
}

func (k KeyspaceInfo) Match(ok KeyspaceInfo) bool {
	return k.CollectionID == ok.CollectionID &&
		k.ScopeID == ok.ScopeID &&
		k.BucketID == ok.BucketID
}

func (k KeyspaceInfo) Clone() (clone KeyspaceInfo) {
	clone.UID = k.UID
	clone.CollectionID = k.CollectionID
	clone.ScopeID = k.ScopeID
	clone.BucketID = k.BucketID
	clone.NumVbuckets = k.NumVbuckets
	return
}

func (k KeyspaceInfo) String() string {
	if k.BucketID == GlobalValue {
		return GlobalValue // wildcard
	}
	if k.ScopeID == GlobalValue {
		return k.BucketID
	}
	if k.CollectionID == GlobalValue {
		return fmt.Sprintf("%s/%s", k.BucketID, k.ScopeID)
	}
	return fmt.Sprintf("%s/%s/%s", k.BucketID, k.ScopeID, k.CollectionID)
}

func GetKeyspaceInfoFromString(keyspace string) (KeyspaceInfo, error) {
	keyspaceInfo := KeyspaceInfo{}
	if keyspace == GlobalValue {
		return keyspaceInfo, nil
	}

	keyspaceSlice := strings.Split(keyspace, "/")
	if len(keyspaceSlice) > 3 {
		return keyspaceInfo, fmt.Errorf("Invalid keyspace format")
	}

	if len(keyspaceSlice) == 3 {
		keyspaceInfo.CollectionID = keyspaceSlice[2]
	}

	if len(keyspaceSlice) >= 2 {
		keyspaceInfo.ScopeID = keyspaceSlice[1]
	}

	keyspaceInfo.BucketID = keyspaceSlice[0]
	return keyspaceInfo, nil
}

// MetaInfo contains internal info related to function
// This must be populated with appropriate field from the caller
type MetaInfo struct {
	RequestType     requestType
	FunctionScopeID KeyspaceInfo
	MetaID          KeyspaceInfo
	SourceID        KeyspaceInfo

	IsUsingTimer bool
	Seq          uint32
	LastPaused   time.Time

	Sboundary streamBoundary
}

func (m MetaInfo) String() string {
	return fmt.Sprintf("{ \"function_scopeD\": %s, \"source\": %s, \"meta\": %s, \"seq\": %d }", m.FunctionScopeID, m.SourceID, m.MetaID, m.Seq)
}

func (mi MetaInfo) Clone() (clone MetaInfo) {
	clone.Sboundary = mi.Sboundary
	clone.RequestType = mi.RequestType
	clone.FunctionScopeID = mi.FunctionScopeID
	clone.SourceID = mi.SourceID
	clone.MetaID = mi.MetaID
	clone.IsUsingTimer = mi.IsUsingTimer
	clone.Seq = mi.Seq
	clone.LastPaused = mi.LastPaused
	return
}

type Owner struct {
	User   string
	Domain string
	UUID   string
}

func (o Owner) String() string {
	return fmt.Sprintf("{ \"user\": %s, \"domain\": %s, \"uuid\": %s}", o.User, o.Domain, o.UUID)
}

func (o Owner) Clone() (clone Owner) {
	clone.User = o.User
	clone.Domain = o.Domain
	clone.UUID = o.UUID

	return
}

type FunctionDetails struct {
	Version       int8        `json:"version"`
	AppLocation   AppLocation `json:"app_location"`
	AppCode       string      `json:"app_code"`
	AppID         uint32      `json:"function_id"`
	AppInstanceID string      `json:"function_instance_id"`

	Settings HandlerSettings `json:"settings"`
	AppState AppState        `json:"app_state"`

	DeploymentConfig DepCfg     `json:"depcfg"`
	Bindings         []Bindings `json:"bindings"`

	MetaInfo MetaInfo `json:"metainfo"`
	Owner    Owner    `json:"owner,omitempty"`

	marshalled *marshalOldFormat `json:"-"`
}

func (f FunctionDetails) String() string {
	fdclone := f.Clone(true)
	fdJson, _ := json.Marshal(fdclone)
	return string(fdJson)
}

type Writer int8

const (
	WriteBucket Writer = 1 << iota
	WriteN1QL
)

type StorageConfig struct {
	OldStyle    bool
	TryCompress bool
	Extras      []byte
}

type StorageBytes struct {
	Body      []byte
	Sensitive []byte
}

type marshalOldFormat struct {
	marshalled []byte
}

func (m *marshalOldFormat) MarshalJSON() ([]byte, error) {
	return m.marshalled, nil
}

type AppVersion uint8

const (
	// Old format
	Version1 AppVersion = iota

	// New format
	Version2
)

// GetDeploymentConfig will unmarshal the bytes and return deployment config and binding list
// Unmarshaling is of the old format and not the new format
func GetDeploymentConfig(data []byte) (newDepcfg DepCfg, bindings []Bindings, err error) {
	oldDepcfg := depCfg{}
	err = json.Unmarshal(data, &oldDepcfg)
	if err != nil {
		return
	}

	newDepcfg, bindings, err = createStructFromDepcfg(oldDepcfg)
	if err != nil {
		return
	}

	return
}

// "*" means include everything and "-" means except
// If "*" not included then include all the fields
var (
	stateTransition = map[State]map[LifeCycleOp]map[string]struct{}{
		Undeployed: {
			Deploy: {
				"*": struct{}{},
			},
			Undeploy: map[string]struct{}{
				"*": {},
			},
		},

		Deployed: {
			Undeploy: {
				"*": {},
			},

			Pause: {
				"*":                              {},
				"-depcfg.source_keyspace":        {},
				"-depcfg.meta_keyspace":          {},
				"-settings.num_timer_partitions": {},
				"-settings.app_log_dir":          {},
				"-settings.dcp_stream_boundary":  {},
			},

			Deploy: {
				"settings.deployment_status":  {},
				"settings.processing_status":  {},
				"settings.description":        {},
				"settings.log_level":          {},
				"settings.timer_context_size": {},
			},
		},

		Paused: {
			Undeploy: {
				"*": {},
			},

			Deploy: {
				"*":                              {},
				"-depcfg.source_keyspace":        {},
				"-depcfg.meta_keyspace":          {},
				"-settings.num_timer_partitions": {},
				"-settings.app_log_dir":          {},
				"-settings.dcp_stream_boundary":  {},
			},

			Pause: {
				"*":                              {},
				"-depcfg.source_keyspace":        {},
				"-depcfg.meta_keyspace":          {},
				"-settings.num_timer_partitions": {},
				"-settings.app_log_dir":          {},
				"-settings.dcp_stream_boundary":  {},
			},
		},

		NoState: {
			Deploy: {
				"*": {},
			},

			Undeploy: {
				"*": {},
			},
		},
	}
)

func (fd *FunctionDetails) IsSourceMutationPossible() bool {
	srcKeyspace := fd.DeploymentConfig.SourceKeyspace

	bindings := fd.Bindings
	for _, binding := range bindings {
		if binding.BindingType != Bucket {
			continue
		}

		bindingKeyspace := binding.BucketBinding.Keyspace
		if bindingKeyspace.Match(srcKeyspace) {
			return true
		}
	}

	return false
}

func PossibleStateChange(currState string, event AppState) (map[string]struct{}, error) {
	currentState := StringToAppState(currState)

	nextState := event.GetLifeCycle()
	if nextState == InvalidState {
		return nil, ErrInvalidState
	}

	transMap, ok := stateTransition[currentState]
	if !ok {
		return nil, ErrStateTransNotPossible
	}

	changeMap, ok := transMap[nextState]
	if !ok {
		return nil, ErrStateTransNotPossible
	}

	return changeMap, nil
}

// NewApplcation(fBytes []byte, byteSource source)
type Application interface {
	// VerifyAndMergeDepCfg merges when all the checks passed for deployment config
	VerifyAndMergeDepCfg(allowedFields map[string]struct{}, newDepcfg DepCfg, bindings []Bindings) (bool, error)

	// VerifyAndMergeSettings merges when all the checks passed for settings
	VerifyAndMergeSettings(allowedFields map[string]struct{}, settings map[string]interface{}) (bool, error)

	// Returns storage bytes for the Application
	GetStorageBytes(sc StorageConfig) StorageBytes

	// Returns Rest api format as provided by version
	GetAppBytes(v AppVersion) (json.Marshaler, error)

	// GetSourceAndDestinations returns source and destination keyspace
	GetSourceAndDestinations(srcCheck bool) (src Keyspace, dsts map[Keyspace]Writer, err error)

	// GetRequestType returns the kind of source from which function needs to be executed
	GetRequestType() requestType

	// Clone returns duplicate FunctionDetails
	Clone(redact bool) *FunctionDetails

	// ModifyAppCode will modify the code to suitable for running in v8
	ModifyAppCode(modify bool) string

	// IsSourceMutationPossible returns true if source bucket mutation is possible
	IsSourceMutationPossible() bool
}
