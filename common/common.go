package common

import (
	"errors"
	"net"
)

type DcpStreamBoundary string

const (
	DcpEverything = DcpStreamBoundary("everything")
	DcpFromNow    = DcpStreamBoundary("from_now")
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
	AppStateDisabled
	AppStateUnexpected
)

var ErrRetryTimeout = errors.New("retry timeout")

// EventingProducer interface to export functions from eventing_producer
type EventingProducer interface {
	AddMetadataPrefix(key string) Key
	Auth() string
	CfgData() string
	CheckpointBlobDump() map[string]interface{}
	CleanupMetadataBucket() error
	CleanupUDSs()
	ClearEventStats()
	GetAppCode() string
	GetDcpEventsRemainingToProcess() uint64
	GetDebuggerURL() (string, error)
	GetEventingConsumerPids() map[string]int
	GetEventProcessingStats() map[string]uint64
	GetExecutionStats() map[string]interface{}
	GetFailureStats() map[string]interface{}
	GetHandlerCode() string
	GetLatencyStats() map[string]uint64
	GetLcbExceptionsStats() map[string]uint64
	GetMetaStoreStats() map[string]uint64
	GetNsServerPort() string
	GetVbOwner(vb uint16) (string, string, error)
	GetSeqsProcessed() map[int]int64
	GetSourceMap() string
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
	SignalCheckpointBlobCleanup() error
	SignalStartDebugger() error
	SignalStopDebugger() error
	SetRetryCount(retryCount int64)
	Serve()
	Stop()
	StopProducer()
	StopRunningConsumers()
	String() string
	TimerDebugStats() map[int]map[string]interface{}
	UpdateMemoryQuota(quota int64)
	VbDcpEventsRemainingToProcess() map[int]int64
	VbDistributionStatsFromMetadata() map[string]map[string]string
	VbSeqnoStats() map[int][]map[string]interface{}
	WriteAppLog(log string)
}

// EventingConsumer interface to export functions from eventing_consumer
type EventingConsumer interface {
	ClearEventStats()
	ConsumerName() string
	DcpEventsRemainingToProcess() uint64
	EventingNodeUUIDs() []string
	EventsProcessedPSec() *EventProcessingStats
	GetEventProcessingStats() map[string]uint64
	GetExecutionStats() map[string]interface{}
	GetFailureStats() map[string]interface{}
	GetHandlerCode() string
	GetLatencyStats() map[string]uint64
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
	Serve()
	SetConnHandle(net.Conn)
	SetFeedbackConnHandle(net.Conn)
	SignalBootstrapFinish()
	SignalConnected()
	SignalFeedbackConnected()
	SignalStopDebugger() error
	SpawnCompilationWorker(appCode, appContent, appName, eventingPort string, handlerHeaders, handlerFooters []string) (*CompileStatus, error)
	Stop()
	String() string
	TimerDebugStats() map[int]map[string]interface{}
	UpdateEventingNodesUUIDs(uuids []string)
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
	SignalStartDebugger(appName string) error
	SignalStopDebugger(appName string) error
	StopProducer(appName string, skipMetaCleanup bool)
	TimerDebugStats(appName string) (map[int]map[string]interface{}, error)
	VbDcpEventsRemainingToProcess(appName string) map[int]int64
	VbDistributionStatsFromMetadata(appName string) map[string]map[string]string
	VbSeqnoStats(appName string) (map[int][]map[string]interface{}, error)
}

type EventingServiceMgr interface {
}

// AppConfig Application/Event handler configuration
type AppConfig struct {
	AppCode        string
	AppDeployState string
	AppName        string
	AppState       string
	AppVersion     string
	HandlerUUID    uint32
	ID             int
	LastDeploy     string
	Settings       map[string]interface{}
	UsingTimer     bool
	UserPrefix     string
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

type StartDebugBlob struct {
	StartDebug bool `json:"start_debug"`
}

type StartDebugBlobVer struct {
	StartDebugBlob
	EventingVersion string `json:"version"`
}

type DebuggerInstanceAddrBlob struct {
	ConsumerName string `json:"consumer_name"`
	HostPortAddr string `json:"host_port_addr"`
	NodeUUID     string `json:"uuid"`
}

type DebuggerInstanceAddrBlobVer struct {
	DebuggerInstanceAddrBlob
	EventingVersion string `json:"version"`
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
	CurlTimeout              int64
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
	TimerStorageRoutineCount int
	TimerStorageChanSize     int
	UndeployRoutineCount     int
	UsingTimer               bool
	WorkerCount              int
	WorkerQueueCap           int64
	WorkerQueueMemCap        int64
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
