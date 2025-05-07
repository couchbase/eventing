package supervisor2

import (
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/service_manager2/response"
	"github.com/couchbase/gocb/v2"
)

type Supervisor2 interface {
	GetStats(application.AppLocation, common.StatsType) (*common.Stats, error)
	ClearStats(application.AppLocation) error

	CompileHandler(*application.FunctionDetails) (*common.CompileStatus, error)
	DebuggerOp(op common.DebuggerOp, funcDetails *application.FunctionDetails, value interface{}) (string, error)

	GetApplicationLog(appLocation application.AppLocation, size int64) ([]string, error)
	GetInsights(appLocation application.AppLocation) *common.Insight
	RebalanceProgress(vbMapVersion string, appLocation application.AppLocation) *common.AppRebalanceProgress
	GetOwnershipDetails() string

	StateChangeInterupt(seq uint32, appLocation application.AppLocation)
	StopCalledInterupt(seq uint32, msg common.LifecycleMsg)

	LifeCycleOperationAllowed() bool

	MemRequiredPerThread(application.KeyspaceInfo) float64
	GetCollectionObject(funcDetails application.Keyspace) (*gocb.Collection, error)

	CreateInitCheckpoint(*response.RuntimeInfo, *application.FunctionDetails)
	PopulateID(response *response.RuntimeInfo, keyspace application.Keyspace) application.KeyspaceInfo
}
