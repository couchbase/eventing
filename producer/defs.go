package producer

import (
	"io"
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
	metakvAppSettingsPath = metakvEventingPath + "appsettings/"
	metakvConfigKeepNodes = metakvEventingPath + "config/keepNodes" // Store list of eventing keepNodes
)

const (
	bucketOpRetryInterval   = time.Duration(1000) * time.Millisecond
	persistAllTickInterval  = time.Duration(5000) * time.Millisecond
	updateStatsTickInterval = time.Duration(5000) * time.Millisecond

	udsSockPathLimit = 100

	dataService = "kv"

	supervisorTimeout = 60 * time.Second

	// KV blob suffixes to assist in choose right consumer instance
	// for instantiating V8 Debugger instance
	startDebuggerFlag    = "startDebugger"
	debuggerInstanceAddr = "debuggerInstAddr"
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
	breakpadOn             bool // Switch to control whether breakpad must be turned on
	bucket                 string
	cleanupTimers          bool
	cfgData                string
	cppWorkerThrCount      int // No. of worker threads per CPP worker process
	cronTimersPerDoc       int
	curlTimeout            int64 // curl operation timeout in ms
	diagDir                string
	eventingAdminPort      string
	eventingSSLPort        string
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
	numVbuckets            int
	pauseProducerCh        chan struct{}
	persistAllTicker       *time.Ticker
	workerQueueCap         int64
	tcpPort                string
	stopProducerCh         chan struct{}
	superSup               common.EventingSuperSup
	uuid                   string
	workerCount            int

	// DCP config, as they need to be tunable
	dcpConfig map[string]interface{}

	// app log related configs
	appLogPath     string
	appLogMaxSize  int64
	appLogMaxFiles int
	appLogWriter   io.WriteCloser

	// Plasma configs
	lssCleanerMaxThreshold int
	lssCleanerThreshold    int
	lssReadAheadSize       int64
	maxDeltaChainLen       int
	maxPageItems           int
	minPageItems           int

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

	// Needed by cron timers
	// A random number (0 - fuzzOffset) will be added to the epoch time at which the callback function
	// was supposed to be called
	fuzzOffset int

	// stats gathered from ClusterInfo
	localAddress      string
	eventingNodeAddrs []string
	kvNodeAddrs       []string
	nsServerNodeAddrs []string
	eventingNodeUUIDs []string

	consumerListeners []net.Listener
	ProducerListener  net.Listener

	// Threshold post which eventing will try to prune stale xattr records related to
	// doc timer from KV document in source bucket
	xattrEntryPruneThreshold int

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

	plasmaMemQuota int64
	vbPlasmaStore  *plasma.Plasma

	// copy of KV vbmap, needed while opening up dcp feed
	kvVbMap map[uint16]string

	signalStopPersistAllCh chan struct{}

	// topologyChangeCh used by super_supervisor to notify producer
	// about topology change
	topologyChangeCh chan *common.TopologyChangeMsg

	// time.Ticker duration for dumping consumer stats
	statsTickDuration time.Duration

	statsRWMutex *sync.RWMutex

	plannerNodeMappings []*common.PlannerNodeVbMapping // access controlled by statsRWMutex
	seqsNoProcessed     map[int]int64                  // Access controlled by statsRWMutex
	updateStatsTicker   *time.Ticker
	updateStatsStopCh   chan struct{}

	// Captures vbucket assignment to different eventing nodes
	vbEventingNodeMap map[string]map[string]string // access controlled by statsRWMutex

	// Map keeping track of vbuckets assigned to each worker(consumer)
	workerVbucketMap map[string][]uint16

	// Supervisor of workers responsible for
	// pipelining messages to V8
	workerSupervisor *suptree.Supervisor

	sync.RWMutex
}
