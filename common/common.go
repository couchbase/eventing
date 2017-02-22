package common

import (
	"net"
)

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
	HostPortAddr() string
	NotifyClusterChange()
	Serve()
	SetConnHandle(net.Conn)
	SignalConnected()
	Stop()
	String() string
	VbProcessingStats() map[uint16]map[string]interface{}
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
