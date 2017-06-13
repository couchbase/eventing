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

	supervisorTimeout = 60
)

type appStatus uint16

const (
	appUndeployed appStatus = iota
	appDeployed
)

// Producer handle - one instance per app per eventing node
type Producer struct {
	appName                string
	app                    *common.AppConfig
	auth                   string
	bucket                 string
	cleanupTimers          bool
	cfgData                string
	eventingAdminPort      string
	eventingDir            string
	kvPort                 string
	kvHostPorts            []string
	listenerHandles        []*abatableListener
	logLevel               string
	rbacpass               string
	rbacrole               string
	rbacuser               string
	metadatabucket         string
	metakvAppHostPortsPath string
	nsServerPort           string
	nsServerHostPort       string
	tcpPort                string
	stopProducerCh         chan struct{}
	uuid                   string
	workerCount            int

	// N1QL Transpiler related nested iterator config params
	lcbInstIncrSize int
	lcbInstCapacity int

	// Controls start seq no for vb dcp stream
	// currently supports:
	// everything - start from beginning and listen forever
	// from_now - start from current vb seq no and listen forever
	dcpStreamBoundary common.DcpStreamBoundary

	// skipTimerThreshold controls the threshold beyond which if timer event
	// trigger is delayed, it's execution will be skipped
	skipTimerThreshold int

	// Timer event processing worker count per Eventing.Consumer instance
	timerWorkerPoolSize int

	// stats gathered from ClusterInfo
	localAddress      string
	eventingNodeAddrs []string
	kvNodeAddrs       []string
	nsServerNodeAddrs []string
	eventingNodeUUIDs []string

	consumerListeners []net.Listener
	ProducerListener  net.Listener

	// For performance reasons, Golang writes dcp events to tcp socket in batches
	// socketWriteBatchSize controls the batch size
	socketWriteBatchSize int

	// Chan used to signify update of app level settings
	notifySettingsChangeCh chan struct{}

	// Chan to notify super_supervisor about clean producer shutdown
	notifySupervisorCh chan struct{}

	// Chan to notify supervisor about producer initialisation
	notifyInitCh chan struct{}

	// Feedback channel to notify change in cluster state
	clusterStateChange chan struct{}

	// List of running consumers, will be needed if we want to gracefully shut them down
	runningConsumers           []common.EventingConsumer
	consumerSupervisorTokenMap map[common.EventingConsumer]suptree.ServiceToken

	// vbucket to eventing node assignment
	vbEventingNodeAssignMap map[uint16]string

	// copy of KV vbmap, needed while opening up dcp feed
	kvVbMap map[uint16]string

	// topologyChangeCh used by super_supervisor to notify producer
	// about topology change
	topologyChangeCh chan *common.TopologyChangeMsg

	// time.Ticker duration for dumping consumer stats
	statsTickDuration time.Duration

	// Map keeping track of vbuckets assigned to each worker(consumer)
	workerVbucketMap map[string][]uint16

	// Supervisor of workers responsible for
	// pipelining messages to V8
	workerSupervisor *suptree.Supervisor

	sync.RWMutex
}
