package supervisor

import (
	"sync"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/suptree"
)

const (
	MetakvEventingPath          = "/eventing/"
	MetakvAppsPath              = MetakvEventingPath + "apps/"
	MetakvAppSettingsPath       = MetakvEventingPath + "settings/"
	MetakvProducerHostPortsPath = MetakvEventingPath + "hostports/"
	MetakvRebalanceTokenPath    = MetakvEventingPath + "rebalanceToken/"
)

const (
	RebalanceRunning = "RebalanceRunning"
)

type supCmdMsg struct {
	cmd string
	ctx string
}

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
