package producer

import (
	"net"
	"sync"
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/suptree"
)

const (
	MetaKvEventingPath    = "/eventing/"
	MetaKvAppsPath        = MetaKvEventingPath + "apps/"
	MetaKvAppSettingsPath = MetaKvEventingPath + "settings/"

	DataService = "kv"

	NumVbuckets = 1024

	// WatchClusterChangeInterval - Interval for spawning another routine to keep an eye on cluster state change
	WatchClusterChangeInterval = time.Duration(100) * time.Millisecond
)

type appStatus uint16

const (
	AppUndeployed appStatus = iota
	AppDeployed
)

type Producer struct {
	AppName          string
	cfgData          string
	app              *common.AppConfig
	auth             string
	bucket           string
	metadatabucket   string
	KvPort           string
	kvHostPort       []string
	NsServerPort     string
	nsServerHostPort string
	tcpPort          string
	stopProducerCh   chan bool
	workerCount      int

	// stats gathered from ClusterInfo
	localAddress      string
	eventingNodeAddrs []string
	kvNodeAddrs       []string
	nsServerNodeAddrs []string

	// service.Manager related fields
	nodeInfo *service.NodeInfo

	consumerListeners []net.Listener
	ProducerListener  net.Listener

	// Chan to notify super_supervisor about clean producer shutdown
	NotifySupervisorCh chan bool

	// Chan to notify supervisor about producer initialisation
	NotifyInitCh chan bool

	// Feedback channel to notify change in cluster state
	clusterStateChange chan bool

	// List of running consumers, will be needed if we want to gracefully shut them down
	runningConsumers           []common.EventingConsumer
	consumerSupervisorTokenMap map[common.EventingConsumer]suptree.ServiceToken

	// vbucket to eventing node assignment
	vbEventingNodeAssignMap map[uint16]string

	// copy of KV vbmap, needed while opening up dcp feed
	kvVbMap map[uint16]string

	// time.Ticker duration for dumping consumer stats
	statsTickDuration time.Duration

	// Map keeping track of vbuckets assigned to each worker(consumer)
	workerVbucketMap map[string][]uint16

	// Supervisor of workers responsible for
	// pipelining messages to V8
	workerSupervisor *suptree.Supervisor

	sync.RWMutex
}
