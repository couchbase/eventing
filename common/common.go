package common

import (
	"errors"
	"net"

	"github.com/couchbase/cbauth/service"
	couchbase "github.com/couchbase/eventing/dcp"
)

type DcpStreamBoundary string

const (
	DcpEverything = DcpStreamBoundary("everything")
	DcpFromNow    = DcpStreamBoundary("from_now")
	DcpFromPrior  = DcpStreamBoundary("from_prior")
)

var MetakvMaxRetries int64 = 60

type ChangeType string
type StatsData map[string]uint64

type InsightLine struct {
	CallCount      int64   `json:"call_count"`
	CallTime       float64 `json:"call_time"`
	ExceptionCount int64   `json:"error_count"`
	LastException  string  `json:"error_msg"`
	LastLog        string  `json:"last_log"`
}

type Insight struct {
	Script string              `json:"script"`
	Lines  map[int]InsightLine `json:"lines"`
}

type Insights map[string]*Insight

const (
	StartRebalanceCType = ChangeType("start-rebalance")
	StopRebalanceCType  = ChangeType("stop-rebalance")
	StartFailoverCType  = ChangeType("start-failover")
)

type TopologyChangeMsg struct {
	CType     ChangeType
	MsgSource string
}

const (
	AppState int8 = iota
	AppStateUndeployed
	AppStateEnabled
	AppStatePaused
	AppStateUnexpected
)

const (
	WaitingForMutation    = "WaitingForMutation" // Debugger has been started and consumers are waiting to trap
	MutationTrapped       = "MutationTrapped"    // One of the consumers have trapped the mutation
	DebuggerTokenKey      = "debugger"
	MetakvEventingPath    = "/eventing/"
	MetakvDebuggerPath    = MetakvEventingPath + "debugger/"
	MetakvTempAppsPath    = MetakvEventingPath + "tempApps/"
	MetakvCredentialsPath = MetakvEventingPath + "credentials/"
	MetakvConfigPath      = MetakvEventingPath + "settings/config"
)

type DebuggerInstance struct {
	Token           string   `json:"token"`             // An ID for a debugging session
	Host            string   `json:"host"`              // The node where debugger has been spawned
	Status          string   `json:"status"`            // Possible values are WaitingForMutation, MutationTrapped
	URL             string   `json:"url"`               // Chrome-Devtools URL for debugging
	NodesExternalIP []string `json:"nodes_external_ip"` // List of external IP address of the nodes in the cluster
}

type Application struct {
	AppHandlers        string                 `json:"appcode"`
	DeploymentConfig   DepCfg                 `json:"depcfg"`
	EventingVersion    string                 `json:"version"`
	FunctionID         uint32                 `json:"handleruuid"`
	ID                 int                    `json:"id"`
	FunctionInstanceID string                 `json:"function_instance_id"`
	Name               string                 `json:"appname"`
	Settings           map[string]interface{} `json:"settings"`
	UsingTimer         bool                   `json:"using_timer"`
	SrcMutationEnabled bool                   `json:"src_mutation"`
	Metainfo           map[string]interface{} `json:"metainfo,omitempty"`
}

type DepCfg struct {
	Buckets        []Bucket `json:"buckets"`
	Curl           []Curl   `json:"curl"`
	MetadataBucket string   `json:"metadata_bucket"`
	SourceBucket   string   `json:"source_bucket"`
}

type Bucket struct {
	Alias      string `json:"alias"`
	BucketName string `json:"bucket_name"`
	Access     string `json:"access"`
}

type Curl struct {
	Hostname               string `json:"hostname"`
	Value                  string `json:"value"`
	AuthType               string `json:"auth_type"`
	Username               string `json:"username"`
	Password               string `json:"password"`
	BearerKey              string `json:"bearer_key"`
	AllowCookies           bool   `json:"allow_cookies"`
	ValidateSSLCertificate bool   `json:"validate_ssl_certificate"`
}

type Credential struct {
	Username  string `json:"username"`
	Password  string `json:"password"`
	BearerKey string `json:"bearer_key"`
}

var ErrRetryTimeout = errors.New("retry timeout")

// EventingProducer interface to export functions from eventing_producer
type EventingProducer interface {
	AddMetadataPrefix(key string) Key
	Auth() string
	AppendCurlLatencyStats(deltas StatsData)
	AppendLatencyStats(deltas StatsData)
	BootstrapStatus() bool
	CfgData() string
	CheckpointBlobDump() map[string]interface{}
	CleanupMetadataBucket(skipCheckpointBlobs bool) error
	CleanupUDSs()
	ClearEventStats()
	DcpFeedBoundary() string
	GetAppCode() string
	GetAppLog(sz int64) []string
	GetDcpEventsRemainingToProcess() uint64
	GetDebuggerURL() (string, error)
	GetEventingConsumerPids() map[string]int
	GetEventProcessingStats() map[string]uint64
	GetExecutionStats() map[string]interface{}
	GetFailureStats() map[string]interface{}
	GetLatencyStats() StatsData
	GetCurlLatencyStats() StatsData
	GetInsight() *Insight
	GetLcbExceptionsStats() map[string]uint64
	GetMetaStoreStats() map[string]uint64
	GetMetadataPrefix() string
	GetNsServerPort() string
	GetVbOwner(vb uint16) (string, string, error)
	GetSeqsProcessed() map[int]int64
	GetDebuggerToken() string
	InternalVbDistributionStats() map[string]string
	IsEventingNodeAlive(eventingHostPortAddr, nodeUUID string) bool
	IsPlannerRunning() bool
	KillAllConsumers()
	KillAndRespawnEventingConsumer(consumer EventingConsumer)
	KvHostPorts() []string
	LenRunningConsumers() int
	MetadataBucket() string
	NotifyInit()
	NotifyPrepareTopologyChange(ejectNodes, keepNodes []string, changeType service.TopologyChangeType)
	NotifySettingsChange()
	NotifySupervisor()
	NotifyTopologyChange(msg *TopologyChangeMsg)
	NsServerHostPort() string
	NsServerNodeCount() int
	PauseProducer()
	PlannerStats() []*PlannerNodeVbMapping
	ResumeProducer()
	RebalanceStatus() bool
	RebalanceTaskProgress() *RebalanceProgress
	RemoveConsumerToken(workerName string)
	SignalBootstrapFinish()
	SignalStartDebugger(token string) error
	SignalStopDebugger() error
	SetRetryCount(retryCount int64)
	SpanBlobDump() map[string]interface{}
	Serve()
	SourceBucket() string
	Stop(context string)
	StopProducer()
	StopRunningConsumers()
	String() string
	TimerDebugStats() map[int]map[string]interface{}
	IsTrapEvent() bool
	SetTrapEvent(value bool)
	UpdateMemoryQuota(quota int64)
	VbDcpEventsRemainingToProcess() map[int]int64
	VbDistributionStatsFromMetadata() map[string]map[string]string
	VbSeqnoStats() map[int][]map[string]interface{}
	WriteAppLog(log string)
	WriteDebuggerURL(url string)
	WriteDebuggerToken(token string, hostnames []string) error
}

// EventingConsumer interface to export functions from eventing_consumer
type EventingConsumer interface {
	BootstrapStatus() bool
	CheckIfQueuesAreDrained() error
	ClearEventStats()
	CloseAllRunningDcpFeeds()
	ConsumerName() string
	DcpEventsRemainingToProcess() uint64
	EventingNodeUUIDs() []string
	EventsProcessedPSec() *EventProcessingStats
	GetEventProcessingStats() map[string]uint64
	GetExecutionStats() map[string]interface{}
	GetFailureStats() map[string]interface{}
	GetInsight() *Insight
	GetLcbExceptionsStats() map[string]uint64
	GetMetaStoreStats() map[string]uint64
	HandleV8Worker() error
	HostPortAddr() string
	Index() int
	InternalVbDistributionStats() []uint16
	NodeUUID() string
	NotifyClusterChange()
	NotifyRebalanceStop()
	NotifySettingsChange()
	Pid() int
	RebalanceStatus() bool
	RebalanceTaskProgress() *RebalanceProgress
	RemoveSupervisorToken() error
	ResetBootstrapDone()
	Serve()
	SetConnHandle(net.Conn)
	SetFeedbackConnHandle(net.Conn)
	SetRebalanceStatus(status bool)
	GetRebalanceStatus() bool
	GetPrevRebalanceInCompleteStatus() bool
	SignalBootstrapFinish()
	SignalConnected()
	SignalFeedbackConnected()
	SignalStopDebugger() error
	SpawnCompilationWorker(appCode, appContent, appName, eventingPort string, handlerHeaders, handlerFooters []string) (*CompileStatus, error)
	Stop(context string)
	String() string
	TimerDebugStats() map[int]map[string]interface{}
	NotifyPrepareTopologyChange(keepNodes, ejectNodes []string)
	UpdateWorkerQueueMemCap(quota int64)
	VbDcpEventsRemainingToProcess() map[int]int64
	VbEventingNodeAssignMapUpdate(map[uint16]string)
	VbProcessingStats() map[uint16]map[string]interface{}
	VbSeqnoStats() map[int]map[string]interface{}
	WorkerVbMapUpdate(map[string][]uint16)

	SendAssignedVbs()
	PauseConsumer()
	GetAssignedVbs(workerName string) ([]uint16, error)
	NotifyWorker()
}

type EventingSuperSup interface {
	PausingAppList() map[string]string
	BootstrapAppList() map[string]string
	BootstrapAppStatus(appName string) bool
	BootstrapStatus() bool
	CheckpointBlobDump(appName string) (interface{}, error)
	ClearEventStats()
	CleanupProducer(appName string, skipMetaCleanup bool, updateMetakv bool) error
	DcpFeedBoundary(fnName string) (string, error)
	DeployedAppList() []string
	GetEventProcessingStats(appName string) map[string]uint64
	GetAppCode(appName string) string
	GetAppLog(appName string, sz int64) []string
	GetAppState(appName string) int8
	GetDcpEventsRemainingToProcess(appName string) uint64
	GetDebuggerURL(appName string) (string, error)
	GetDeployedApps() map[string]string
	GetEventingConsumerPids(appName string) map[string]int
	GetExecutionStats(appName string) map[string]interface{}
	GetFailureStats(appName string) map[string]interface{}
	GetLatencyStats(appName string) StatsData
	GetCurlLatencyStats(appName string) StatsData
	GetInsight(appName string) *Insight
	GetLcbExceptionsStats(appName string) map[string]uint64
	GetLocallyDeployedApps() map[string]string
	GetMetaStoreStats(appName string) map[string]uint64
	GetBucket(bucketName, appName string) (*couchbase.Bucket, error)
	GetSeqsProcessed(appName string) map[int]int64
	InternalVbDistributionStats(appName string) map[string]string
	KillAllConsumers()
	NotifyPrepareTopologyChange(ejectNodes, keepNodes []string, changeType service.TopologyChangeType)
	TopologyChangeNotifCallback(path string, value []byte, rev interface{}) error
	PlannerStats(appName string) []*PlannerNodeVbMapping
	RebalanceStatus() bool
	RebalanceTaskProgress(appName string) (*RebalanceProgress, error)
	UnwatchBucket(bucketName, appName string)
	RemoveProducerToken(appName string)
	RestPort() string
	SignalStopDebugger(appName string) error
	SpanBlobDump(appName string) (interface{}, error)
	StopProducer(appName string, skipMetaCleanup bool, updateMetakv bool)
	TimerDebugStats(appName string) (map[int]map[string]interface{}, error)
	VbDcpEventsRemainingToProcess(appName string) map[int]int64
	VbDistributionStatsFromMetadata(appName string) map[string]map[string]string
	VbSeqnoStats(appName string) (map[int][]map[string]interface{}, error)
	WriteDebuggerURL(appName, url string)
	WriteDebuggerToken(appName, token string, hostnames []string)
	OptimiseBucketLoading(optimise bool)
}

type EventingServiceMgr interface {
	UpdateBucketGraphFromMetakv(functionName string) error
	ResetFailoverStatus()
	GetFailoverStatus() (failoverNotifTs int64, changeId string)
	OptimiseLoadingCIC(bool) error
}

type Config map[string]interface{}

// AppConfig Application/Event handler configuration
type AppConfig struct {
	AppCode            string
	ParsedAppCode      string
	AppDeployState     string
	AppName            string
	AppState           string
	AppVersion         string
	FunctionID         uint32
	FunctionInstanceID string
	ID                 int
	LastDeploy         string
	Settings           map[string]interface{}
	UsingTimer         bool
	UserPrefix         string
	SrcMutationEnabled bool
}

type RebalanceProgress struct {
	CloseStreamVbsLen     int
	StreamReqVbsLen       int
	VbsRemainingToShuffle int
	VbsOwnedPerPlan       int
	NodeLevelStats        interface{}
}

type EventProcessingStats struct {
	DcpEventsProcessedPSec   int    `json:"dcp_events_processed_psec"`
	TimerEventsProcessedPSec int    `json:"timer_events_processed_psec"`
	Timestamp                string `json:"timestamp"`
}

type CompileStatus struct {
	Area           string `json:"area"`
	Column         int    `json:"column_number"`
	CompileSuccess bool   `json:"compile_success"`
	Description    string `json:"description"`
	Index          int    `json:"index"`
	Language       string `json:"language"`
	Line           int    `json:"line_number"`
}

// PlannerNodeVbMapping captures the vbucket distribution across all
// eventing nodes as per planner
type PlannerNodeVbMapping struct {
	Hostname string `json:"host_name"`
	StartVb  int    `json:"start_vb"`
	VbsCount int    `json:"vb_count"`
}

type HandlerConfig struct {
	N1qlPrepareAll           bool
	LanguageCompatibility    string
	AggDCPFeedMemCap         int64
	CheckpointInterval       int
	IdleCheckpointInterval   int
	CleanupTimers            bool
	CPPWorkerThrCount        int
	ExecuteTimerRoutineCount int
	ExecutionTimeout         int
	FeedbackBatchSize        int
	FeedbackQueueCap         int64
	FeedbackReadBufferSize   int
	HandlerHeaders           []string
	HandlerFooters           []string
	LcbInstCapacity          int
	N1qlConsistency          string
	LogLevel                 string
	SocketWriteBatchSize     int
	SocketTimeout            int
	SourceBucket             string
	StatsLogInterval         int
	StreamBoundary           DcpStreamBoundary
	TimerContextSize         int64
	TimerStorageRoutineCount int
	TimerStorageChanSize     int
	TimerQueueMemCap         uint64
	TimerQueueSize           uint64
	UndeployRoutineCount     int
	UsingTimer               bool
	WorkerCount              int
	WorkerQueueCap           int64
	WorkerQueueMemCap        int64
	WorkerResponseTimeout    int
	LcbRetryCount            int
	NumTimerPartitions       int
}

type ProcessConfig struct {
	BreakpadOn             bool
	DebuggerPort           string
	DiagDir                string
	EventingDir            string
	EventingPort           string
	EventingSSLPort        string
	FeedbackSockIdentifier string
	IPCType                string
	SockIdentifier         string
}

type RebalanceConfig struct {
	VBOwnershipGiveUpRoutineCount   int
	VBOwnershipTakeoverRoutineCount int
}

type Key struct {
	prefix         string
	key            string
	transformedKey string
}

func NewKey(userPrefix, clusterPrefix, key string) Key {
	metadataPrefix := userPrefix + "::" + clusterPrefix
	return Key{metadataPrefix, key, metadataPrefix + "::" + key}
}

func (k Key) Raw() string {
	return k.transformedKey
}

func (k Key) GetPrefix() string {
	return k.prefix
}

func StreamBoundary(boundary string) DcpStreamBoundary {
	switch boundary {
	case "everything":
		return DcpEverything
	case "from_now":
		return DcpFromNow
	case "from_prior":
		return DcpFromPrior
	default:
		return DcpStreamBoundary("")
	}
}

func NewInsight() *Insight {
	return &Insight{Lines: make(map[int]InsightLine)}
}

func NewInsights() *Insights {
	o := make(Insights)
	return &o
}

func (dst *Insights) Accumulate(src *Insights) {
	for app, insight := range *src {
		val := (*dst)[app]
		if val == nil {
			val = NewInsight()
		}
		val.Accumulate(insight)
		(*dst)[app] = val
	}
}

func (dst *Insight) Accumulate(src *Insight) {
	for line, right := range src.Lines {
		left := dst.Lines[line]
		left.CallCount += right.CallCount
		left.CallTime += right.CallTime
		left.ExceptionCount += right.ExceptionCount
		if len(right.LastException) > 0 {
			left.LastException = right.LastException
		}
		if len(right.LastLog) > 0 {
			left.LastLog = right.LastLog
		}
		dst.Lines[line] = left
	}
	if len(src.Script) > 0 {
		dst.Script = src.Script
	}
}

func GetDefaultHandlerHeaders() []string {
	return []string{"'use strict';"}
}
