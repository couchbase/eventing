package producer

import (
	"net"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/suptree"
)

const (
	metakvEventingPath    = "/eventing/"
	metakvAppsPath        = metakvEventingPath + "apps/"
	metakvAppSettingsPath = metakvEventingPath + "settings/"
)

const (
	dataService = "kv"

	numVbuckets = 1024

	// WatchClusterChangeInterval - Interval for spawning another routine to keep an eye on cluster state change
	WatchClusterChangeInterval = time.Duration(100) * time.Millisecond
)

type appStatus uint16

const (
	appUndeployed appStatus = iota
	appDeployed
)

// Producer handle - one instance per app per eventing node
type Producer struct {
	listenerHandles        []*abatableListener
	appName                string
	app                    *common.AppConfig
	auth                   string
	bucket                 string
	cfgData                string
	kvPort                 string
	kvHostPorts            []string
	logLevel               string
	metadatabucket         string
	metakvAppHostPortsPath string
	nsServerPort           string
	nsServerHostPort       string
	tcpPort                string
	stopProducerCh         chan bool
	uuid                   string
	workerCount            int

	// Controls start seq no for vb dcp stream
	// currently supports:
	// everything - start from beginning and listen forever
	// from_now - start from current vb seq no and listen forever
	dcpStreamBoundary common.DcpStreamBoundary

	// stats gathered from ClusterInfo
	localAddress      string
	eventingNodeAddrs []string
	kvNodeAddrs       []string
	nsServerNodeAddrs []string
	eventingNodeUUIDs []string

	consumerListeners []net.Listener
	ProducerListener  net.Listener

	// Chan to notify super_supervisor about clean producer shutdown
	notifySupervisorCh chan bool

	// Chan to notify supervisor about producer initialisation
	notifyInitCh chan bool

	// Feedback channel to notify change in cluster state
	clusterStateChange chan bool

	// List of running consumers, will be needed if we want to gracefully shut them down
	runningConsumers           []common.EventingConsumer
	consumerSupervisorTokenMap map[common.EventingConsumer]suptree.ServiceToken

	// vbucket to eventing node assignment
	vbEventingNodeAssignMap map[uint16]string

	// copy of KV vbmap, needed while opening up dcp feed
	kvVbMap map[uint16]string

	// startTopologyChangeCh used by super_supervisor to notify producer
	// about topology change
	startTopologyChangeCh chan *common.TopologyChangeMsg

	// time.Ticker duration for dumping consumer stats
	statsTickDuration time.Duration

	// Map keeping track of vbuckets assigned to each worker(consumer)
	workerVbucketMap map[string][]uint16

	// Supervisor of workers responsible for
	// pipelining messages to V8
	workerSupervisor *suptree.Supervisor

	sync.RWMutex
}
