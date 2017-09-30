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

// EventingProducer interface to export functions from eventing_producer
type EventingProducer interface {
	Auth() string
	CfgData() string
	CleanupDeadConsumer(consumer EventingConsumer)
	ClearEventStats()
	GetAppCode() string
	GetDebuggerURL() string
	GetNsServerPort() string
	GetSeqsProcessed() map[int]int64
	GetSourceMap() string
	GetHandlerCode() string
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
	RbacPass() string
	RbacUser() string
	SignalBootstrapFinish()
	SignalCheckpointBlobCleanup()
	SignalPlasmaClosed(vb uint16)
	SignalPlasmaTransferFinish(vb uint16, store *plasma.Plasma)
	SignalStartDebugger()
	SignalStopDebugger()
	SignalToClosePlasmaStore(vb uint16)
	Serve()
	Stop()
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
	EventsProcessedPSec() *EventProcessingStats
	EventingNodeUUIDs() []string
	GetHandlerCode() string
	GetSeqsProcessed() map[int]int64
	GetSourceMap() string
	HandleV8Worker()
	HostPortAddr() string
	NodeUUID() string
	NotifyClusterChange()
	NotifyRebalanceStop()
	NotifySettingsChange()
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
	GetAppCode(appName string) string
	GetDebuggerURL(appName string) string
	GetDeployedApps() map[string]string
	GetSeqsProcessed(appName string) map[int]int64
	GetSourceMap(appName string) string
	GetHandlerCode(appName string) string
	NotifyPrepareTopologyChange(keepNodes []string)
	ProducerHostPortAddrs() []string
	RestPort() string
	SignalStartDebugger(appName string)
	SignalStopDebugger(appName string)
	SignalToClosePlasmaStore(vb uint16)
	SignalTimerDataTransferStart(vb uint16) bool
	SignalTimerDataTransferStop(vb uint16, store *plasma.Plasma)
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
