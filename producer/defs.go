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
	cleanupTimers          bool
	cfgData                string
	kvPort                 string
	kvHostPorts            []string
	listenerHandles        []net.Listener
	metadatabucket         string
	metadataBucketHandle   *gocb.Bucket
	metakvAppHostPortsPath string
	nsServerPort           string
	nsServerHostPort       string
	numVbuckets            int
	pauseProducerCh        chan struct{}
	persistAllTicker       *time.Ticker
	statsTicker            *time.Ticker
	stopProducerCh         chan struct{}
	superSup               common.EventingSuperSup
	uuid                   string

	handlerConfig   *common.HandlerConfig
	processConfig   *common.ProcessConfig
	rebalanceConfig *common.RebalanceConfig

	// DCP config, as they need to be tunable
	dcpConfig map[string]interface{}

	// app log related configs
	appLogPath     string
	appLogMaxSize  int64
	appLogMaxFiles int
	appLogWriter   io.WriteCloser

	// Plasma configs
	autoSwapper            bool
	enableSnapshotSMR      bool
	lssCleanerMaxThreshold int
	lssCleanerThreshold    int
	lssReadAheadSize       int64
	maxDeltaChainLen       int
	maxPageItems           int
	minPageItems           int
	persistInterval        int //in ms
	useMemoryMgmt          bool

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

	consumerListeners []net.Listener
	ProducerListener  net.Listener

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
