package servicemanager2

import (
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/gocb/v2"
)

type CursorRegister interface {
	IsRegisterPossible(k application.Keyspace, funcId string) bool
	Register(k application.Keyspace, funcId string) bool
	Unregister(k application.Keyspace, funcId string)
}

type Supervisor2 interface {
	GetStats(application.AppLocation) (*common.Stats, error)
	ClearStats(application.AppLocation) error
	CreateInitCheckpoint(funcDetails *application.FunctionDetails) (bool, error)
	CompileHandler(*application.FunctionDetails) (*common.CompileStatus, error)
	DebuggerOp(op common.DebuggerOp, funcDetails *application.FunctionDetails, value interface{}) (string, error)

	GetApplicationLog(appLocation application.AppLocation, size int64) ([]string, error)
	GetInsights(appLocation application.AppLocation) *common.Insight
	RebalanceProgress(vbMapVersion string, appLocation application.AppLocation) *common.AppRebalanceProgress
	GetOwnershipDetails() string

	PopulateID(keyspace application.Keyspace) (keyID application.KeyspaceInfo, err error)
	LifeCycleOperationAllowed() bool
	AssignOwnership(funcDetails *application.FunctionDetails) error
	GetLeaderNode() string

	GetCollectionObject(keyspace application.Keyspace) (*gocb.Collection, error)
	DeleteOnDeployCheckpoint(funcDetails *application.FunctionDetails, forceDelete bool) error
}

type Config struct {
}

type ServiceManager interface {
}
