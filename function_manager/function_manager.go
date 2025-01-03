package functionManager

import (
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	functionHandler "github.com/couchbase/eventing/function_manager/function_handler"
	"github.com/couchbase/gocb/v2"
)

type SystemResourceDetails interface {
	MemRequiredPerThread(application.KeyspaceInfo) float64
}

type InterruptHandler interface {
	// Interrupt called when state changed request done
	StateChangeInterupt(seq uint32, appLocation application.AppLocation)

	// Undeploy due to internal changes
	StopCalledInterupt(seq uint32, msg common.LifecycleMsg)

	// Revert to previous app state
	FailStateInterrupt(seq uint32, appLocation application.AppLocation, msg common.LifecycleMsg)
}

type FunctionManager interface {
	// Stats operation
	GetStats(appLocation application.AppLocation) *common.Stats
	GetInsight(appLocation application.AppLocation) *common.Insight
	GetApplicationLog(appLocation application.AppLocation, size int64) ([]string, error)
	RebalanceProgress(version string, appLocation application.AppLocation, rebalanceProgress *common.AppRebalanceProgress)
	ResetStats(appLocation application.AppLocation)

	// Lifecycle operations
	DeployFunction(fd *application.FunctionDetails)
	PauseFunction(fd *application.FunctionDetails)
	StopFunction(fd *application.FunctionDetails)
	RemoveFunction(fd *application.FunctionDetails) int

	// Notification for changes
	NotifyOwnershipChange()
	NotifyGlobalConfigChange()
	NotifyTlsChanges(cluster *gocb.Cluster)

	// For debugger
	TrapEventOp(trapEvent functionHandler.TrapEventOp, appLocation application.AppLocation, value interface{}) error

	// Close the function manager
	CloseFunctionManager()
}
