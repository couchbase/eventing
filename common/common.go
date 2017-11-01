package common

import (
	"net"

	"github.com/couchbase/plasma"
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

// EventingProducer interface to export functions from eventing_producer
type EventingProducer interface {
	Auth() string
	CfgData() string
	CleanupDeadConsumer(consumer EventingConsumer)
	ClearEventStats()
	GetAppCode() string
	GetDcpEventsRemainingToProcess() uint64
	GetDebuggerURL() string
	GetEventingConsumerPids() map[string]int
	GetEventProcessingStats() map[string]uint64
	GetFailureStats() map[string]uint64
	GetHandlerCode() string
	GetLatencyStats() map[string]uint64
	GetNsServerPort() string
	GetSeqsProcessed() map[int]int64
	GetSourceMap() string
	IsEventingNodeAlive(eventingHostPortAddr string) bool
	KvHostPorts() []string
	LenRunningConsumers() int
	MetadataBucket() string
	NotifyInit()
	NotifyPrepareTopologyChange(keepNodes []string)
	NotifySettingsChange()
	NotifySupervisor()
	NotifyTopologyChange(msg *TopologyChangeMsg)
	NsServerHostPort() string
	NsServerNodeCount() int
	PauseProducer()
	RbacUser() string
	RbacPass() string
	SignalBootstrapFinish()
	SignalCheckpointBlobCleanup()
	SignalStartDebugger()
	SignalStopDebugger()
	Serve()
	Stop()
	StopProducer()
	String() string
	TimerTransferHostPortAddrs() map[string]string
	VbEventingNodeAssignMap() map[uint16]string
	WorkerVbMap() map[string][]uint16
}

// EventingConsumer interface to export functions from eventing_consumer
type EventingConsumer interface {
	ClearEventStats()
	ConsumerName() string
	DcpEventsRemainingToProcess() uint64
	EventingNodeUUIDs() []string
	EventsProcessedPSec() *EventProcessingStats
	GetEventProcessingStats() map[string]uint64
	GetFailureStats() map[string]uint64
	GetHandlerCode() string
	GetLatencyStats() map[string]uint64
	GetSeqsProcessed() map[int]int64
	GetSourceMap() string
	HandleV8Worker()
	HostPortAddr() string
	NodeUUID() string
	NotifyClusterChange()
	NotifyRebalanceStop()
	NotifySettingsChange()
	Pid() int
	RebalanceTaskProgress() *RebalanceProgress
	Serve()
	SetConnHandle(net.Conn)
	SignalBootstrapFinish()
	SignalConnected()
	SignalPlasmaClosed(vb uint16)
	SignalPlasmaTransferFinish(vb uint16, store *plasma.Plasma)
	SignalStopDebugger()
	Stop()
	String() string
	TimerTransferHostPortAddr() string
	UpdateEventingNodesUUIDs(uuids []string)
	VbProcessingStats() map[uint16]map[string]interface{}
}

type EventingSuperSup interface {
	AppProducerHostPortAddr(appName string) string
	AppTimerTransferHostPortAddrs(string) (map[string]string, error)
	ClearEventStats()
	DeployedAppList() []string
	GetEventProcessingStats(appName string) map[string]uint64
	GetAppCode(appName string) string
	GetAppState(appName string) int8
	GetDcpEventsRemainingToProcess(appName string) uint64
	GetDebuggerURL(appName string) string
	GetDeployedApps() map[string]string
	GetEventingConsumerPids(appName string) map[string]int
	GetFailureStats(appName string) map[string]uint64
	GetHandlerCode(appName string) string
	GetLatencyStats(appName string) map[string]uint64
	GetSeqsProcessed(appName string) map[int]int64
	GetSourceMap(appName string) string
	NotifyPrepareTopologyChange(keepNodes []string)
	ProducerHostPortAddrs() []string
	RestPort() string
	SignalStartDebugger(appName string)
	SignalStopDebugger(appName string)
}

type EventingServiceMgr interface {
}

// AppConfig Application/Event handler configuration
type AppConfig struct {
	AppName        string
	AppCode        string
	AppDeployState string
	AppState       string
	AppVersion     string
	LastDeploy     string
	ID             int
	Settings       map[string]interface{}
}

type RebalanceProgress struct {
	VbsRemainingToShuffle int
	VbsOwnedPerPlan       int
}

type EventProcessingStats struct {
	DcpEventsProcessedPSec   int    `json:"dcp_events_processed_psec"`
	TimerEventsProcessedPSec int    `json:"timer_events_processed_psec"`
	Timestamp                string `json:"timestamp"`
}

type StartDebugBlob struct {
	StartDebug bool `json:"start_debug"`
}

type DebuggerInstanceAddrBlob struct {
	ConsumerName string `json:"consumer_name"`
	HostPortAddr string `json:"host_port_addr"`
	NodeUUID     string `json:"uuid"`
}
