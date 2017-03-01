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
	KvHostPort() []string
	LenRunningConsumers() int
	MetadataBucket() string
	NotifyInit()
	NotifyPrepareTopologyChange(keepNodes []string)
	NotifyStartTopologyChange(msg *TopologyChangeMsg)
	NotifySupervisor()
	NsServerHostPort() string
	NsServerNodeCount() int
	Serve()
	Stop()
	String() string
	VbEventingNodeAssignMap() map[uint16]string
	WorkerVbMap() map[string][]uint16
}

// EventingConsumer interface to export functions from eventing_consumer
type EventingConsumer interface {
	ConsumerName() string
	DcpEventsRemainingToProcess() uint64
	HostPortAddr() string
	NotifyClusterChange()
	Serve()
	SetConnHandle(net.Conn)
	SignalConnected()
	Stop()
	String() string
	RebalanceTaskProgress() float64
	VbProcessingStats() map[uint16]map[string]interface{}
}

type EventingSuperSup interface {
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
