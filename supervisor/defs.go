package supervisor

import (
	"sync"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/suptree"
	"github.com/couchbase/nitro/plasma"
)

const (
	metakvEventingPath = "/eventing/"

	// MetakvAppsPath refers to path under metakv where app handlers are stored
	MetakvAppsPath = metakvEventingPath + "apps/"

	// MetakvAppSettingsPath refers to path under metakv where app settings are stored
	MetakvAppSettingsPath       = metakvEventingPath + "settings/"
	metakvProducerHostPortsPath = metakvEventingPath + "hostports/"

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
	numVbuckets      = 1024
	numTimerVbMoves  = 10
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

// SuperSupervisor is responsible for managing/supervising all producer instances
type SuperSupervisor struct {
	auth              string
	CancelCh          chan struct{}
	eventingAdminPort string
	eventingDir       string
	kvPort            string
	restPort          string
	superSup          *suptree.Supervisor
	supCmdCh          chan supCmdMsg
	uuid              string

	mu                           *sync.RWMutex
	plasmaCloseSignalMap         map[uint16]int
	producerSupervisorTokenMap   map[common.EventingProducer]suptree.ServiceToken
	runningProducers             map[string]common.EventingProducer
	runningProducersHostPortAddr map[string]string
	timerDataTransferReq         map[uint16]struct{}
	timerDataTransferReqCh       chan uint16
	vbPlasmaStoreMap             map[uint16]*plasma.Plasma
	vbucketsToOwn                []uint16

	serviceMgr common.EventingServiceMgr
	sync.RWMutex
}
