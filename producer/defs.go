package producer

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/gocb"
)

const (
	metakvEventingPath    = "/eventing/"
	metakvAppsPath        = metakvEventingPath + "apps/"
	metakvAppSettingsPath = metakvEventingPath + "appsettings/"
	metakvConfigKeepNodes = metakvEventingPath + "config/keepNodes" // Store list of eventing keepNodes
	metakvChecksumPath    = metakvEventingPath + "checksum/"
)

const (
	bucketOpRetryInterval = time.Duration(1000) * time.Millisecond

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
)

type startDebugBlob struct {
	StartDebug bool `json:"start_debug"`
}

// Producer handle - one instance per app per eventing node
type Producer struct {
	appName                string
	app                    *common.AppConfig
	auth                   string
	cfgData                string
	cleanupTimers          bool
	handleV8ConsumerMutex  *sync.Mutex // controls access to Producer.handleV8Consumer
	isBootstrapping        bool
	isPlannerRunning       bool
	isTerminateRunning     bool
	kvPort                 string
	kvHostPorts            []string
	metadatabucket         string
	metadataBucketHandle   *gocb.Bucket
	metakvAppHostPortsPath string
	nsServerPort           string
	nsServerHostPort       string
	numVbuckets            int
	pauseProducerCh        chan struct{}
	pollBucketInterval     time.Duration
	pollBucketTicker       *time.Ticker
	retryCount             int64
	stopCh                 chan struct{}
	stopChClosed           bool
	stopProducerCh         chan struct{}
	superSup               common.EventingSuperSup
	trapEvent              bool
	debuggerToken          string
	uuid                   string
	workerSpawnCounter     uint64

	handlerConfig   *common.HandlerConfig
	processConfig   *common.ProcessConfig
	rebalanceConfig *common.RebalanceConfig

	// DCP config, as they need to be tunable
	dcpConfig map[string]interface{}

	// app log related configs
	appLogPath     string
	appLogMaxSize  int64
	appLogMaxFiles int64
	appLogRotation bool
	appLogWriter   io.WriteCloser

	// Chan used to signal if Eventing.Producer has finished bootstrap
	// i.e. started up all it's child routines
	bootstrapFinishCh chan struct{}

	// stats gathered from ClusterInfo
	localAddress      string
	eventingNodeAddrs []string
	kvNodeAddrs       []string
	nsServerNodeAddrs []string
	ejectNodeUUIDs    []string
	eventingNodeUUIDs []string

	consumerListeners map[common.EventingConsumer]net.Listener // Access controlled by listenerRWMutex
	feedbackListeners map[common.EventingConsumer]net.Listener // Access controlled by listenerRWMutex
	listenerRWMutex   *sync.RWMutex

	// Chan used to signify update of app level settings
	notifySettingsChangeCh chan struct{}

	// Chan to notify super_supervisor about clean producer shutdown
	notifySupervisorCh chan struct{}

	// Chan to notify supervisor about producer initialisation
	notifyInitCh chan struct{}

	// Feedback channel to notify change in cluster state
	clusterStateChange chan struct{}

	// List of running consumers, will be needed if we want to gracefully shut them down
	runningConsumers           []common.EventingConsumer // Access controlled by runningConsumersRWMutex
	runningConsumersRWMutex    *sync.RWMutex
	consumerSupervisorTokenMap map[common.EventingConsumer]suptree.ServiceToken // Access controlled by tokenRWMutex
	tokenRWMutex               *sync.RWMutex

	workerNameConsumerMap        map[string]common.EventingConsumer // Access controlled by workerNameConsumerMapRWMutex
	workerNameConsumerMapRWMutex *sync.RWMutex

	// vbucket to eventing node assignment
	vbEventingNodeAssignMap     map[uint16]string // Access controlled by vbEventingNodeAssignRWMutex
	vbEventingNodeAssignRWMutex *sync.RWMutex

	MemoryQuota int64

	// copy of KV vbmap, needed while opening up dcp feed
	kvVbMap map[uint16]string

	// topologyChangeCh used by super_supervisor to notify producer
	// about topology change
	topologyChangeCh chan *common.TopologyChangeMsg

	statsRWMutex *sync.RWMutex

	plannerNodeMappings        []*common.PlannerNodeVbMapping // Access controlled by plannerNodeMappingsRWMutex
	plannerNodeMappingsRWMutex *sync.RWMutex
	seqsNoProcessed            map[int]int64 // Access controlled by seqsNoProcessedRWMutex
	seqsNoProcessedRWMutex     *sync.RWMutex
	updateStatsTicker          *time.Ticker

	// Captures vbucket assignment to different eventing nodes
	vbEventingNodeMap     map[string]map[string]string // Access controlled by vbEventingNodeRWMutex
	vbEventingNodeRWMutex *sync.RWMutex

	vbMapping        map[uint16]*vbNodeWorkerMapping // Access controlled by vbMappingRWMutex
	vbMappingRWMutex *sync.RWMutex

	// Map keeping track of vbuckets assigned to each worker(consumer)
	workerVbucketMap   map[string][]uint16 // Access controlled by workerVbMapRWMutex
	workerVbMapRWMutex *sync.RWMutex

	// Supervisor of workers responsible for
	// pipelining messages to V8
	workerSupervisor *suptree.Supervisor
}

type vbNodeWorkerMapping struct {
	ownerNode      string
	assignedWorker string
}

type acceptedConn struct {
	conn net.Conn
	err  error
}
