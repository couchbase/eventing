package supervisor

import (
	"errors"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	couchbase "github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb/v2"
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
	startFailover            = "startFailover"

	// Store list of eventing keepNodes
	metakvConfigKeepNodes = metakvEventingPath + "config/keepNodes"

	// MetakvChecksumPath within metakv is updated when new function definition is loaded
	MetakvChecksumPath = metakvEventingPath + "checksum/"
)

const (
	numVbuckets = 1024

	memQuotaThreshold = 0.85
)

// TODO: move it to common package
const bucketOpRetryInterval = time.Duration(1000) * time.Millisecond

var NoBucket = errors.New("Bucket not found")

const (
	supCmdType int8 = iota
	cmdAppDelete
	cmdSettingsUpdate
)

type supCmdMsg struct {
	cmd int8
	ctx string
}

// AdminPortConfig captures settings supplied by cluster manager
type AdminPortConfig struct {
	DebuggerPort string
	HTTPPort     string
	SslPort      string
	CAFile       string
	CertFile     string
	KeyFile      string
}

type bucketWatchStruct struct {
	b    *couchbase.Bucket
	apps map[string]map[common.Keyspace]common.MonitorType
}

type gocbBucketInstance struct {
	apps         map[string]struct{}
	bucketHandle *gocb.Bucket
}

type gocbPool struct {
	sync.RWMutex
	cluster      *gocb.Cluster
	bucketHandle map[string]*gocbBucketInstance
}

type gocbGlobalConfig struct {
	sync.RWMutex
	plaingocbPool     *gocbPool
	encryptedgocbPool *gocbPool
	appEncryptionMap  map[string]bool
	nsServerPort      string
	retrycount        int64
}

// SuperSupervisor is responsible for managing/supervising all producer instances
type SuperSupervisor struct {
	CancelCh    chan struct{}
	adminPort   AdminPortConfig
	ejectNodes  []string
	eventingDir string
	keepNodes   []string
	kvPort      string
	restPort    string
	retryCount  int64
	superSup    *suptree.Supervisor
	supCmdCh    chan supCmdMsg
	uuid        string
	diagDir     string
	hostport    string
	pool        string

	bucketsRWMutex          *sync.RWMutex
	servicesNotifierRetryTm uint
	finch                   chan bool
	buckets                 map[string]*bucketWatchStruct // Access controlled by bucketsRWMutex
	isRebalanceOngoing      int32

	appRWMutex *sync.RWMutex

	appDeploymentStatus map[string]bool // Access controlled by appRWMutex
	appProcessingStatus map[string]bool // Access controlled by appRWMutex

	appListRWMutex    *sync.RWMutex
	bootstrappingApps map[string]string // Captures list of apps undergoing bootstrap, access controlled by appListRWMutex
	pausingApps       map[string]string // Captures list of apps being paused, access controlled by appListRWMutex

	// Captures list of deployed apps and their last deployment time. Leveraged to report deployed app status
	// via rest endpoints. Access controlled by appListRWMutex
	deployedApps map[string]string

	// Captures list of deployed apps. Similar to "deployedApps" but it's used internally by Eventing.Consumer
	// to signify app has been undeployed. Access controlled by appListRWMutex
	locallyDeployedApps map[string]string

	// Count of how many times worker Respawned
	workerRespawnedCount uint32

	// Global config
	memoryQuota int64 // In MB

	cleanedUpAppMap            map[string]struct{} // Access controlled by default lock
	mu                         *sync.RWMutex
	producerSupervisorTokenMap map[common.EventingProducer]suptree.ServiceToken // Access controlled by tokenMapRWMutex
	tokenMapRWMutex            *sync.RWMutex
	runningProducers           map[string]common.EventingProducer // Access controlled by runningProducersRWMutex
	runningProducersRWMutex    *sync.RWMutex
	vbucketsToOwn              []uint16

	scn        *util.ServicesChangeNotifier
	serviceMgr common.EventingServiceMgr

	gocbGlobalConfigHandle *gocbGlobalConfig

	sync.RWMutex

	securitySetting *common.SecuritySetting // access controlled by securityMutex
	securityMutex   *sync.RWMutex

	initEncryptDataMutex     *sync.RWMutex
	initLifecycleEncryptData bool

	featureMatrix uint32

	// -1 means cgroup is not supported
	systemMemLimit float64
}
