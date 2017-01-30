package common

import (
	"net"
)

type EventingProducer interface {
	Serve()
	Stop()
	String() string
	Auth() string
	LenRunningConsumers() int
	CleanupDeadConsumer(consumer EventingConsumer)
	VbEventingNodeAssignMap() map[uint16]string
	IsEventingNodeAlive(eventingHostPortAddr string) bool
	GetNsServerPort() string
	KvHostPort() []string
	NsServerNodeCount() int
	WorkerVbMap() map[string][]uint16
}

type EventingConsumer interface {
	Serve()
	Stop()
	String() string
	ConsumerName() string
	HostPortAddr() string
	SetConnHandle(net.Conn)
	SignalConnected()
}

type AppConfig struct {
	AppName string      `json:"appname"`
	AppCode string      `json:"appcode"`
	Depcfg  interface{} `json:"depcfg"`
	ID      int         `json:"id"`
}
