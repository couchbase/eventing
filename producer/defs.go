package producer

import (
	"net"

	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/indexing/secondary/dcp"
)

const (
	NUM_VBUCKETS = 1024
)

type Consumer struct {
	app      *AppConfig
	conn     net.Conn
	dcpFeed  *couchbase.DcpFeed
	producer *Producer

	// Populated when C++ v8 worker is spawned
	// correctly and downstream tcp socket is available
	// for sending messages. Unbuffered channel.
	signalConnectedCh chan bool

	// Populated when Cmd.Wait() call returns.
	// Could mean process is dead. Unbuffered channel
	signalCmdWaitExitCh chan bool

	// Populated when downstream tcp socket mapping to
	// C++ v8 worker is down. Buffered channel to avoid deadlock
	stopConsumerCh chan bool
	tcpPort        string
}

type Producer struct {
	App              *AppConfig
	Auth             string
	Bucket           string
	KVHostPort       []string
	NSServerHostPort string
	tcpPort          string
	StopProducerCh   chan bool
	WorkerCount      int

	// Map keeping track of start and end vbucket
	// for each worker
	workerVbucketMap map[int]map[string]interface{}

	// Supervisor of workers responsible for
	// pipelining messages to V8
	workerSupervisor *suptree.Supervisor
}

type DcpEventMetadata struct {
	Cas       uint64 `json:"cas"`
	Expiry    uint32 `json:"expiry"`
	Flag      uint32 `json:"flag"`
	Partition string `json:"partition"`
	Seq       uint64 `json:"seq"`
}

type AppConfig struct {
	AppName    string
	AppCode    string
	BucketName string
}
