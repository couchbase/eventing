package servicemanager

import (
	"sync"
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/util"
)

const (
	MetakvEventingPath       = "/eventing/"
	MetakvAppsPath           = MetakvEventingPath + "apps/"
	MetakvAppSettingsPath    = MetakvEventingPath + "settings/"
	MetakvRebalanceTokenPath = MetakvEventingPath + "rebalanceToken/"
)

const (
	DefaultWorkerCount                  = 3
	DefaultStatsTickDuration            = 10000
	HTTPRequestTimeout                  = time.Duration(1000) * time.Millisecond
	rebalanceProgressUpdateTickInterval = time.Duration(3000) * time.Millisecond
)

// ServiceMgr implements cbauth_service interface
type ServiceMgr struct {
	auth              string
	config            util.ConfigHolder
	eventingNodeAddrs []string
	eventingAdminPort string
	failoverNotif     bool
	mu                *sync.RWMutex
	nodeInfo          *service.NodeInfo
	rebalanceCtx      *rebalanceContext
	rebUpdateTicker   *time.Ticker
	rebalanceRunning  bool
	restPort          string
	servers           []service.NodeID
	state
	superSup common.EventingSuperSup
	waiters  waiters
}

type rebalanceContext struct {
	change service.TopologyChange
	rev    uint64
}

type waiter chan state
type waiters map[waiter]struct{}

type state struct {
	rebalanceID   string
	rebalanceTask *service.Task
	rev           uint64
	servers       []service.NodeID
}

type cleanup struct {
	canceled bool
	f        func()
}

type application struct {
	Name             string `json:"appname"`
	ID               int    `json:"id"`
	DeploymentConfig depCfg `json:"depcfg"`
	AppHandlers      string `json:"appcode"`
}

type depCfg struct {
	Buckets        []bucket `json:"buckets"`
	MetadataBucket string   `json:"metadata_bucket"`
	SourceBucket   string   `json:"source_bucket"`
}

type bucket struct {
	Alias      string `json:"alias"`
	BucketName string `json:"bucket_name"`
}
