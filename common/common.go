package common

import (
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

// EventingProducer interface to export functions from eventing_producer
type EventingProducer interface {
	Auth() string
	CfgData() string
	CleanupDeadConsumer(consumer EventingConsumer)
	GetNsServerPort() string
	IsEventingNodeAlive(eventingHostPortAddr string) bool
	KvHostPorts() []string
	LenRunningConsumers() int
	RbacUser() string
	RbacPass() string
	MetadataBucket() string
	NotifyInit()
	NotifyPrepareTopologyChange(keepNodes []string)
	NotifySettingsChange()
	NotifySupervisor()
	NotifyTopologyChange(msg *TopologyChangeMsg)
	NsServerHostPort() string
	NsServerNodeCount() int
	Serve()
	Stop()
	String() string
	TimerTransferHostPortAddrs() map[string]string
	VbEventingNodeAssignMap() map[uint16]string
	WorkerVbMap() map[string][]uint16
}

// EventingConsumer interface to export functions from eventing_consumer
type EventingConsumer interface {
	ConsumerName() string
	DcpEventsRemainingToProcess() uint64
	EventsProcessedPSec() *EventProcessingStats
	EventingNodeUUIDs() []string
	HandleV8Worker()
	HostPortAddr() string
	NodeUUID() string
	NotifyClusterChange()
	NotifyRebalanceStop()
	NotifySettingsChange()
	RebalanceTaskProgress() *RebalanceProgress
	Serve()
	SetConnHandle(net.Conn)
	SignalConnected()
	Stop()
	String() string
	TimerTransferHostPortAddr() string
	UpdateEventingNodesUUIDs(uuids []string)
	VbProcessingStats() map[uint16]map[string]interface{}
}

type EventingSuperSup interface {
	AppProducerHostPortAddr(appName string) string
	AppTimerTransferHostPortAddrs(string) map[string]string
	NotifyPrepareTopologyChange(keepNodes []string)
	ProducerHostPortAddrs() []string
	RestPort() string
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
	VbsCurrentlyOwned int
	VbsOwnedPerPlan   int
}

type EventProcessingStats struct {
	DcpEventsProcessedPSec   int    `json:"dcp_events_processed_psec"`
	TimerEventsProcessedPSec int    `json:"timer_events_processed_psec"`
	Timestamp                string `json:"timestamp"`
}
