package supervisor

import (
	"sync"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/suptree"
)

const (
	metakvEventingPath = "/eventing/"

	// MetakvAppsPath refers to path under metakv where app handlers are stored
	MetakvAppsPath = metakvEventingPath + "apps/"

	// MetakvAppSettingsPath refers to path under metakv where app settings are stored
	MetakvAppSettingsPath       = metakvEventingPath + "appsettings/"
	metakvProducerHostPortsPath = metakvEventingPath + "hostports/"

	// MetakvClusterSettings houses global configs related to Eventing
	MetakvClusterSettings = metakvEventingPath + "settings/"

	// MetakvRebalanceTokenPath refers to path under metakv where rebalance tokens are stored
	MetakvRebalanceTokenPath = metakvEventingPath + "rebalanceToken/"
	stopRebalance            = "stopRebalance"
)

const (
	rebalanceRunning = "RebalanceRunning"
	autoLssCleaning  = false
	maxDeltaChainLen = 30
	maxPageItems     = 100
	minPageItems     = 10
	numTimerVbMoves  = 10
	numVbuckets      = 1024
)

const (
	supCmdType int8 = iota
	cmdAppDelete
	cmdAppLoad
	cmdSettingsUpdate
)

type supCmdMsg struct {
	cmd int8
	ctx string
}

// AdminPortConfig captures settings supplied by cluster manager
type AdminPortConfig struct {
	HTTPPort string
	SslPort  string
	CertFile string
	KeyFile  string
}

// SuperSupervisor is responsible for managing/supervising all producer instances
type SuperSupervisor struct {
	auth        string
	CancelCh    chan struct{}
	adminPort   AdminPortConfig
	eventingDir string
	keepNodes   []string
	kvPort      string
	numVbuckets int
	restPort    string
	superSup    *suptree.Supervisor
	supCmdCh    chan supCmdMsg
	uuid        string
	diagDir     string

	appRWMutex *sync.RWMutex

	appDeploymentStatus map[string]bool // Access controlled by appRWMutex
	appProcessingStatus map[string]bool // Access controlled by appRWMutex

	// Captures list of deployed apps and their last deployment time
	deployedApps   map[string]string
	plasmaMemQuota int64 // In MB

	cleanedUpAppMap            map[string]struct{} // Access controlled by default lock
	mu                         *sync.RWMutex
	producerSupervisorTokenMap map[common.EventingProducer]suptree.ServiceToken
	runningProducers           map[string]common.EventingProducer
	vbucketsToOwn              []uint16

	serviceMgr common.EventingServiceMgr
	sync.RWMutex
}

type eventingConfig struct {
	RAMQuota       int64  `json:"ram_quota"`
	MetadataBucket string `json:"metadata_bucket"`
}
