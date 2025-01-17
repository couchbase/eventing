package functionHandler

import (
	"bytes"

	"github.com/couchbase/eventing/application"
	checkpointManager "github.com/couchbase/eventing/checkpoint_manager"
	"github.com/couchbase/eventing/common"
	dcpMessage "github.com/couchbase/eventing/dcp_connection"
	processManager "github.com/couchbase/eventing/process_manager"
)

type RuntimeEnvironment interface {
	InitEvent(version uint32, opcode uint8, handlerID []byte, value interface{})
	LifeCycleOp(version uint32, opcode uint8, handlerID []byte)
	SendControlMessage(version uint32, cmd processManager.Command, opcode uint8, handlerID []byte, key, value interface{})
	GetStats(version uint32, opcode uint8, handlerName []byte)

	WriteDcpMessage(version uint32, buffer *bytes.Buffer, opcode uint8, workerID uint8,
		instanceID []byte, msg *dcpMessage.DcpEvent, internalInfo *checkpointManager.ParsedInternalDetails) int32

	FlushMessage(version uint32, buffer *bytes.Buffer)
	VbSettings(version uint32, opcode uint8, handlerID []byte, key interface{}, value interface{})

	GetProcessDetails() processManager.ProcessDetails
	GetRuntimeStats() common.StatsInterface
}

type UtilityWorker interface {
	CreateUtilityWorker(id string, md processManager.MessageDeliver) processManager.ProcessManager
	GetUtilityWorker() processManager.ProcessManager
	DoneUtilityWorker(id string)
}

type CursorCheckpointHandler interface {
	GetCursors(keyspace application.Keyspace) (map[string]struct{}, bool)
}

// Called when the function asked by caller is done
// Like OwnershipChanges or Stop finished
type InterruptHandler func(id uint16, seq uint32, appLocation application.AppLocation, err error)

type Config struct {
	LeaderHandler bool
}

// Sequence:
// NewFunctionHandler
// SpawnFunction()
// OwnershipChange()
// Stop() // if paused
// Stop() // undeploy
// SpawnFunction()
// OwnershipChange()
// Stop() // If undeployed

type funcHandlerState int8

const (
	TempPause funcHandlerState = iota
	Paused
	pausing
	Undeployed
	undeploying
	Deployed
	running
)

func (fhs funcHandlerState) String() string {
	switch fhs {
	case TempPause:
		return "TemporaryPaused"

	case Paused:
		return "Paused"

	case pausing:
		return "Pausing"

	case Undeployed:
		return "Undeployed"

	case undeploying:
		return "Undeploying"

	case Deployed:
		return "Deployed"

	case running:
		return "Running"
	}

	return ""
}

type runtimeArgs struct {
	currState funcHandlerState
}

type TrapEventOp uint8

const (
	StartTrapEvent TrapEventOp = iota
	StopTrapEvent
)

// manages the function handler
// store all the function related stats
// start dcp connection

// Calling style
// NewFunctionHandler() -> AddFunctionDetails() -> ChangeState(RunApp/PauseApp) -> ChangeState(StopApp) -> AddFunctionDetails()
type FunctionHandler interface {
	AddFunctionDetails(funcDetails *application.FunctionDetails)

	// ChangeState will changes the state of an app
	// It can be called for spawn/pause/undeploy/RuntimeEnvironment crash
	ChangeState(re RuntimeEnvironment, state funcHandlerState)
	TrapEvent(trapEvent TrapEventOp, value interface{}) error

	// ReceiveMessage is when msg is received from the c++ side
	ReceiveMessage(msg *processManager.ResponseMessage)

	// Stats gives the stats back for the app
	Stats(statsType common.StatsType) *common.Stats
	GetInsight() *common.Insight

	GetApplicationLog(size int64) ([]string, error)

	// ResetStats will reset all the stats
	ResetStats()

	// ApplicationLog will write the message to the application log
	ApplicationLog(msg string)

	// NotifyOwnershipChange will be called when new vbs need to be acquired or givenup
	NotifyOwnershipChange()

	// NotifyGlobalConfigChange will be called when global config is changed
	NotifyGlobalConfigChange()

	// GetRebalanceProgress how many vbs need to be claimed
	GetRebalanceProgress(version string, appProgress *common.AppRebalanceProgress) bool

	CloseFunctionHandler()
}

type dummy struct{}

// NewDummyFunctionHandler creates a dummy function handler
func NewDummyFunctionHandler() dummy {
	return dummy{}
}

func (d dummy) AddFunctionDetails(funcDetails *application.FunctionDetails) {
}

func (d dummy) ChangeState(re RuntimeEnvironment, state funcHandlerState) {
}

func (d dummy) TrapEvent(trapEvent TrapEventOp, value interface{}) error {
	return nil
}

func (d dummy) ReceiveMessage(msg *processManager.ResponseMessage) {
}

func (d dummy) Stats(statsType common.StatsType) *common.Stats {
	return nil
}

func (d dummy) GetInsight() *common.Insight {
	return nil
}

func (d dummy) GetApplicationLog(size int64) ([]string, error) {
	return nil, nil
}

func (d dummy) ResetStats() {
}

func (d dummy) ApplicationLog(msg string) {
}

func (d dummy) NotifyOwnershipChange() {
}

func (d dummy) NotifyGlobalConfigChange() {
}

func (d dummy) GetRebalanceProgress(version string, appProgress *common.AppRebalanceProgress) bool {
	return false
}

func (d dummy) CloseFunctionHandler() {
}
