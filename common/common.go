package common

import (
	"net"

	"github.com/couchbase/eventing/flatbuf/cfg"
)

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

type AppConfig struct {
	AppName string
	AppCode string
	Depcfg  *cfg.DepCfg
	ID      int
}
