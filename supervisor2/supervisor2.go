package supervisor2

import (
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/gocb/v2"
)

type Supervisor2 interface {
	GetStats(application.AppLocation) (*common.Stats, error)
	ClearStats(application.AppLocation) error

	CreateInitCheckpoint(*application.FunctionDetails) (bool, error)
	CompileHandler(*application.FunctionDetails) (*common.CompileStatus, error)
	DebuggerOp(op common.DebuggerOp, funcDetails *application.FunctionDetails, value interface{}) (string, error)

	GetApplicationLog(appLocation application.AppLocation, size int64) ([]string, error)
	GetInsights(appLocation application.AppLocation) *common.Insight
	RebalanceProgress(vbMapVersion string, appLocation application.AppLocation) *common.AppRebalanceProgress
	GetOwnershipDetails() string

	StateChangeInterupt(seq uint32, appLocation application.AppLocation)
	StopCalledInterupt(seq uint32, msg common.LifecycleMsg)
	FailStateInterrupt(seq uint32, appLocation application.AppLocation, msg common.LifecycleMsg)

	PopulateID(keyspace application.Keyspace) (keyID application.KeyspaceInfo, err error)

	LifeCycleOperationAllowed() bool

	GetCollectionObject(funcDetails application.Keyspace) (*gocb.Collection, error)
}
