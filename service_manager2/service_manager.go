package servicemanager2

import (
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/service_manager2/response"
	"github.com/couchbase/gocb/v2"
)

type CursorRegister interface {
	IsRegisterPossible(k application.Keyspace, funcId string) bool
	Register(k application.Keyspace, funcId string) bool
	Unregister(k application.Keyspace, funcId string)
}

type Supervisor2 interface {
	GetStats(application.AppLocation, common.StatsType) (*common.Stats, error)
	ClearStats(application.AppLocation) error
	CompileHandler(*application.FunctionDetails) (*common.CompileStatus, error)
	DebuggerOp(op common.DebuggerOp, funcDetails *application.FunctionDetails, value interface{}) (string, error)

	GetApplicationLog(appLocation application.AppLocation, size int64) ([]string, error)
	GetInsights(appLocation application.AppLocation) *common.Insight
	RebalanceProgress(vbMapVersion string, appLocation application.AppLocation) *common.AppRebalanceProgress
	GetGlobalRebalanceProgress(changeId string) (float64, error)
	GetOwnershipDetails() string

	LifeCycleOperationAllowed() bool
	AssignOwnership(funcDetails *application.FunctionDetails) error
	GetLeaderNode() string

	GetCollectionObject(keyspace application.Keyspace) (*gocb.Collection, error)
	DeleteOnDeployCheckpoint(funcDetails *application.FunctionDetails, forceDelete bool) error

	CreateInitCheckpoint(*response.RuntimeInfo, *application.FunctionDetails)
	PopulateID(response *response.RuntimeInfo, keyspace application.Keyspace) application.KeyspaceInfo
}

type Config struct {
}

type ServiceManager interface {
}
