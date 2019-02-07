package common

import (
	"errors"
	"net"
)

type DcpStreamBoundary string

const (
	DcpEverything = DcpStreamBoundary("everything")
	DcpFromNow    = DcpStreamBoundary("from_now")
	DcpFromPrior  = DcpStreamBoundary("from_prior")
)

type ChangeType string

const (
	StartRebalanceCType = ChangeType("start-rebalance")
	StopRebalanceCType  = ChangeType("stop-rebalance")
)

type TopologyChangeMsg struct {
	CType ChangeType
}

const (
	AppState int8 = iota
	AppStateUndeployed
	AppStateEnabled
	AppStatePaused
	AppStateUnexpected
)

const (
	WaitingForMutation = "WaitingForMutation" // Debugger has been started and consumers are waiting to trap
	MutationTrapped    = "MutationTrapped"    // One of the consumers have trapped the mutation
	DebuggerTokenKey   = "debugger"
	MetakvEventingPath = "/eventing/"
	MetakvDebuggerPath = MetakvEventingPath + "debugger/"
)

type DebuggerInstance struct {
	Token           string   `json:"token"`             // An ID for a debugging session
	Host            string   `json:"host"`              // The node where debugger has been spawned
	Status          string   `json:"status"`            // Possible values are WaitingForMutation, MutationTrapped
	URL             string   `json:"url"`               // Chrome-Devtools URL for debugging
	NodesExternalIP []string `json:"nodes_external_ip"` // List of external IP address of the nodes in the cluster
}

type Curl struct {
	Hostname  string `json:"hostname"`
	Value     string `json:"value"`
	AuthType  string `json:"auth_type"`
	Username  string `json:"username"`
	Password  string `json:"password"`
	BearerKey string `json:"bearer_key"`
	Cookies   string `json:"cookies"`
}

var ErrRetryTimeout = errors.New("retry timeout")

// EventingProducer interface to export functions from eventing_producer
type EventingProducer interface {
	AddMetadataPrefix(key string) Key
	Auth() string
	CfgData() string
	CheckpointBlobDump() map[string]interface{}
	CleanupMetadataBucket(skipCheckpointBlobs bool) error
	CleanupUDSs()
	ClearEventStats()
	DcpFeedBoundary() string
	GetAppCode() string
	GetDcpEventsRemainingToProcess() uint64
	GetDebuggerURL() (string, error)
	GetEventingConsumerPids() map[string]int
	GetEventProcessingStats() map[string]uint64
	GetExecutionStats() map[string]interface{}
	GetFailureStats() map[string]interface{}
	GetHandlerCode() string
	GetLatencyStats() map[string]uint64
	GetCurlLatencyStats() map[string]uint64
	GetLcbExceptionsStats() map[string]uint64
	GetMetaStoreStats() map[string]uint64
	GetMetadataPrefix() string
	GetNsServerPort() string
	GetVbOwner(vb uint16) (string, string, error)
	GetSeqsProcessed() map[int]int64
	GetSourceMap() string
	GetDebuggerToken() string
	InternalVbDistributionStats() map[string]string
	IsEventingNodeAlive(eventingHostPortAddr, nodeUUID string) bool
	IsPlannerRunning() bool
	KillAndRespawnEventingConsumer(consumer EventingConsumer)
	KvHostPorts() []string
	LenRunningConsumers() int
	MetadataBucket() string
	NotifyInit()
	NotifyPrepareTopologyChange(ejectNodes, keepNodes []string)
	NotifySettingsChange()
	NotifySupervisor()
	NotifyTopologyChange(msg *TopologyChangeMsg)
	NsServerHostPort() string
	NsServerNodeCount() int
	PauseProducer()
	PlannerStats() []*PlannerNodeVbMapping
	RebalanceStatus() bool
	RebalanceTaskProgress() *RebalanceProgress
	RemoveConsumerToken(workerName string)
	SignalBootstrapFinish()
	SignalStartDebugger(token string) error
	SignalStopDebugger() error
	SetRetryCount(retryCount int64)
	SpanBlobDump() map[string]interface{}
	Serve()
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
	GetHandlerCode() string
	GetLatencyStats() map[string]uint64
	GetCurlLatencyStats() map[string]uint64
	GetLcbExceptionsStats() map[string]uint64
	GetMetaStoreStats() map[string]uint64
	GetSourceMap() string
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
	ResetBootstrapDone()
	Serve()
	SetConnHandle(net.Conn)
	SetFeedbackConnHandle(net.Conn)
	SetRebalanceStatus(status bool)
	SignalBootstrapFinish()
	SignalConnected()
	SignalFeedbackConnected()
	SignalStopDebugger() error
	SpawnCompilationWorker(appCode, appContent, appName, eventingPort string, handlerHeaders, handlerFooters []string) (*CompileStatus, error)
	Stop(context string)
	String() string
	TimerDebugStats() map[int]map[string]interface{}
	UpdateEventingNodesUUIDs(keepNodes, ejectNodes []string)
	UpdateWorkerQueueMemCap(quota int64)
	VbDcpEventsRemainingToProcess() map[int]int64
	VbEventingNodeAssignMapUpdate(map[uint16]string)
	VbProcessingStats() map[uint16]map[string]interface{}
	VbSeqnoStats() map[int]map[string]interface{}
	WorkerVbMapUpdate(map[string][]uint16)
}

type EventingSuperSup interface {
	BootstrapAppList() map[string]string
	CheckpointBlobDump(appName string) (interface{}, error)
	ClearEventStats()
	CleanupProducer(appName string, skipMetaCleanup bool) error
	DcpFeedBoundary(fnName string) (string, error)
	DeployedAppList() []string
	GetEventProcessingStats(appName string) map[string]uint64
	GetAppCode(appName string) string
	GetAppState(appName string) int8
	GetDcpEventsRemainingToProcess(appName string) uint64
	GetDebuggerURL(appName string) (string, error)
	GetDeployedApps() map[string]string
	GetEventingConsumerPids(appName string) map[string]int
	GetExecutionStats(appName string) map[string]interface{}
	GetFailureStats(appName string) map[string]interface{}
	GetHandlerCode(appName string) string
	GetLatencyStats(appName string) map[string]uint64
	GetCurlLatencyStats(appName string) map[string]uint64
	GetLcbExceptionsStats(appName string) map[string]uint64
	GetLocallyDeployedApps() map[string]string
	GetMetaStoreStats(appName string) map[string]uint64
	GetSeqsProcessed(appName string) map[int]int64
	GetSourceMap(appName string) string
	InternalVbDistributionStats(appName string) map[string]string
	NotifyPrepareTopologyChange(ejectNodes, keepNodes []string)
	PlannerStats(appName string) []*PlannerNodeVbMapping
	RebalanceStatus() bool
	RebalanceTaskProgress(appName string) (*RebalanceProgress, error)
	RemoveProducerToken(appName string)
	RestPort() string
	SignalStopDebugger(appName string) error
	SpanBlobDump(appName string) (interface{}, error)
	StopProducer(appName string, skipMetaCleanup bool)
	TimerDebugStats(appName string) (map[int]map[string]interface{}, error)
	VbDcpEventsRemainingToProcess(appName string) map[int]int64
	VbDistributionStatsFromMetadata(appName string) map[string]map[string]string
	VbSeqnoStats(appName string) (map[int][]map[string]interface{}, error)
	WriteDebuggerURL(appName, url string)
	WriteDebuggerToken(appName, token string, hostnames []string)
}

type EventingServiceMgr interface{}
type Config map[string]interface{}

// AppConfig Application/Event handler configuration
type AppConfig struct {
	AppCode            string
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
	Level          string `json:"level"`
	Line           int    `json:"line_number"`
	UsingTimer     string `json:"using_timer"`
	Version        string `json:"version"`
}

// PlannerNodeVbMapping captures the vbucket distribution across all
// eventing nodes as per planner
type PlannerNodeVbMapping struct {
	Hostname string `json:"host_name"`
	StartVb  int    `json:"start_vb"`
	VbsCount int    `json:"vb_count"`
}

type HandlerConfig struct {
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
