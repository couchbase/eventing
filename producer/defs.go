package producer

import (
	"net"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/gocb"
	"github.com/couchbase/plasma"
)

const (
	metakvEventingPath    = "/eventing/"
	metakvAppsPath        = metakvEventingPath + "apps/"
	metakvAppSettingsPath = metakvEventingPath + "settings/"
)

const (
	bucketOpRetryInterval = time.Duration(1000) * time.Millisecond

	dataService = "kv"

	numVbuckets = 1024

	supervisorTimeout = 60

	// KV blob suffixes to assist in choose right consumer instance
	// for instantiating V8 Debugger instance
	startDebuggerFlag    = "startDebugger"
	debuggerInstanceAddr = "debuggerInstAddr"

	// Plasma related constants
	autoLssCleaning  = false
	maxDeltaChainLen = 30
	maxPageItems     = 100
	minPageItems     = 10
)

type appStatus uint16

const (
	appUndeployed appStatus = iota
	appDeployed
)

type startDebugBlob struct {
	StartDebug bool `json:"start_debug"`
}

// Producer handle - one instance per app per eventing node
type Producer struct {
	appName                string
	app                    *common.AppConfig
	auth                   string
	bucket                 string
	cleanupTimers          bool
	cfgData                string
	cppWorkerThrCount      int // No. of worker threads per CPP worker process
	eventingAdminPort      string
	eventingDir            string
	kvPort                 string
	kvHostPorts            []string
	listenerHandles        []net.Listener
	logLevel               string
	metadatabucket         string
	metadataBucketHandle   *gocb.Bucket
	metakvAppHostPortsPath string
	nsServerPort           string
	nsServerHostPort       string
	tcpPort                string
	stopProducerCh         chan struct{}
	superSup               common.EventingSuperSup
	uuid                   string
	workerCount            int

	// Chan used to signal if Eventing.Producer has finished bootstrap
	// i.e. started up all it's child routines
	bootstrapFinishCh chan struct{}

	// Routines to control parallel vbucket ownership transfer
	// during rebalance
	vbOwnershipGiveUpRoutineCount   int
	vbOwnershipTakeoverRoutineCount int

	// N1QL Transpiler related nested iterator config params
	lcbInstCapacity int

	enableRecursiveMutation bool

	// Controls read and write deadline timeout for communication
	// between Go and C++ process
	socketTimeout time.Duration

	// Caps wall clock time allowed for execution of Javascript handler
	// code by V8 runtime(in seconds)
	executionTimeout int

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
	workerNameConsumerMap      map[string]common.EventingConsumer

	// vbucket to eventing node assignment
	vbEventingNodeAssignMap map[uint16]string

	vbPlasmaStore *plasma.Plasma

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
