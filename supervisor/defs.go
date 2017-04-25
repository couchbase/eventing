package supervisor

import (
	"sync"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/suptree"
)

const (
	metakvEventingPath = "/eventing/"

	// MetakvAppsPath refers to path under metakv where app handlers are stored
	MetakvAppsPath              = metakvEventingPath + "apps/"
	metakvAppSettingsPath       = metakvEventingPath + "settings/"
	metakvProducerHostPortsPath = metakvEventingPath + "hostports/"

	// MetakvRebalanceTokenPath refers to path under metakv where rebalance tokens are stored
	MetakvRebalanceTokenPath = metakvEventingPath + "rebalanceToken/"
	stopRebalance            = "stopRebalance"
)

const (
	rebalanceRunning = "RebalanceRunning"
)

type supCmdMsg struct {
	cmd string
	ctx string
}

// SuperSupervisor is responsible for managing/supervising all producer instances
type SuperSupervisor struct {
	CancelCh          chan struct{}
	eventingAdminPort string
	kvPort            string
	restPort          string
	superSup          *suptree.Supervisor
	supCmdCh          chan supCmdMsg
	uuid              string

	producerSupervisorTokenMap   map[common.EventingProducer]suptree.ServiceToken
	runningProducers             map[string]common.EventingProducer
	runningProducersHostPortAddr map[string]string
	mu                           *sync.RWMutex

	serviceMgr common.EventingServiceMgr
}
