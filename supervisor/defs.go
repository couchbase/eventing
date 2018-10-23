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

	// MetakvAppsRetryPath refers to path where retry counter for bailing out
	// from operations that are retried upon failure
	MetakvAppsRetryPath = metakvEventingPath + "retry/"

	// MetakvAppSettingsPath refers to path under metakv where app settings are stored
	MetakvAppSettingsPath       = metakvEventingPath + "appsettings/"
	metakvProducerHostPortsPath = metakvEventingPath + "hostports/"

	// MetakvClusterSettings houses global configs related to Eventing
	MetakvClusterSettings = metakvEventingPath + "settings/"

	// MetakvRebalanceTokenPath refers to path under metakv where rebalance tokens are stored
	MetakvRebalanceTokenPath = metakvEventingPath + "rebalanceToken/"
	stopRebalance            = "stopRebalance"

	// Store list of eventing keepNodes
	metakvConfigKeepNodes = metakvEventingPath + "config/keepNodes"

	// MetakvChecksumPath within metakv is updated when new function definition is loaded
	MetakvChecksumPath = metakvEventingPath + "checksum/"
)

const (
	numVbuckets = 1024
)

const (
	supCmdType int8 = iota
	cmdAppDelete
	cmdAppLoad
	cmdSettingsUpdate
)

type supCmdMsg struct {
	cleanupTimers bool
	cmd           int8
	ctx           string
}

// AdminPortConfig captures settings supplied by cluster manager
type AdminPortConfig struct {
	DebuggerPort string
	HTTPPort     string
	SslPort      string
	CertFile     string
	KeyFile      string
}

// SuperSupervisor is responsible for managing/supervising all producer instances
type SuperSupervisor struct {
	auth        string
	CancelCh    chan struct{}
	adminPort   AdminPortConfig
	ejectNodes  []string
	eventingDir string
	keepNodes   []string
	kvPort      string
	numVbuckets int
	restPort    string
	retryCount  int64
	superSup    *suptree.Supervisor
	supCmdCh    chan supCmdMsg
	uuid        string
	diagDir     string

	appRWMutex *sync.RWMutex

	appDeploymentStatus map[string]bool // Access controlled by appRWMutex
	appProcessingStatus map[string]bool // Access controlled by appRWMutex

	appListRWMutex    *sync.RWMutex
	bootstrappingApps map[string]string // Captures list of apps undergoing bootstrap, access controlled by appListRWMutex

	// Captures list of deployed apps and their last deployment time. Leveraged to report deployed app status
	// via rest endpoints. Access controlled by appListRWMutex
	deployedApps map[string]string

	// Captures list of deployed apps. Similar to "deployedApps" but it's used internally by Eventing.Consumer
	// to signify app has been undeployed. Access controlled by appListRWMutex
	locallyDeployedApps map[string]string

	// Global config
	memoryQuota int64 // In MB

	cleanedUpAppMap            map[string]struct{} // Access controlled by default lock
	mu                         *sync.RWMutex
	producerSupervisorTokenMap map[common.EventingProducer]suptree.ServiceToken // Access controlled by tokenMapRWMutex
	tokenMapRWMutex            *sync.RWMutex
	runningProducers           map[string]common.EventingProducer // Access controlled by runningProducersRWMutex
	runningProducersRWMutex    *sync.RWMutex
	vbucketsToOwn              []uint16

	serviceMgr common.EventingServiceMgr
	sync.RWMutex
}
