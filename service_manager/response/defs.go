package response

import (
	"fmt"
	"net/http"

	"github.com/couchbase/eventing/gen/auditevent"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/goutils/systemeventlog"
)

type Response interface {
	// Adds request made by the user
	AddRequestData(key string, value interface{})

	// Add the requested event
	SetRequestEvent(event eventCode)

	// Logs if the context is to be logged
	// currently logging to audit and system logs
	// Error will be logged by errCode
	LogAndSend(runtimeInfo *RuntimeInfo)

	Log(runtimeInfo *RuntimeInfo)
}

// Init this with  sel := systemeventlog.NewSystemEventLogger(systemeventlog.SystemEventLoggerConfig{}, baseNsserverURL,
// util.SYSTEM_EVENT_COMPONENT, http.Client{Timeout: util.DEFAULT_TIMEOUT_SECS * time.Second},
// func(message string) {
//         logging.Errorf(message)
//  })
var sel systemeventlog.SystemEventLogger

type WarningsInfo struct {
	Status   string   `json:"status"`
	Warnings []string `json:"warnings"`
}

type errCode int

type RuntimeInfo struct {
	ErrCode     errCode     `json:"code"`
	Description interface{} `json:"info,omitempty"`
	WarningInfo interface{} `json:"warning_info,omitempty"`
	// For system event
	ExtraAttributes interface{} `json:"-"`

	// Some response don't need to be json marshalled
	// If set it to true then sender won't marshal the response and
	// send the description as given by the caller
	SendRawDescription bool `json:"-"`

	// If true then sender will only send the description
	OnlyDescription bool `json:"-"`
	// Defaults to "application/json"
	ContentType string `json:"-"`
}

type ErrorInfo struct {
	httpStatusCode int
	Name           string      `json:"name"`
	Code           int         `json:"code"`
	Attributes     []string    `json:"attributes,omitempty"`
	Description    interface{} `json:"description"`
}

func (e *ErrorInfo) GetStatusCode() int {
	return e.Code
}

func (e *ErrorInfo) GetName() string {
	return e.Name
}

func (e *ErrorInfo) GetDescription() interface{} {
	return e.Description
}

var (
	Ok                        = errCode(0)
	ErrDelAppPs               = errCode(1)
	ErrDelAppTs               = errCode(2)
	ErrSaveAppPs              = errCode(5)
	ErrSaveAppTs              = errCode(6)
	ErrSetSettingsPs          = errCode(7)
	ErrDelAppSettingsPs       = errCode(11)
	ErrAppNotDeployed         = errCode(12)
	ErrAppNotFoundTs          = errCode(13)
	ErrMarshalResp            = errCode(14)
	ErrReadReq                = errCode(15)
	ErrUnmarshalPld           = errCode(16)
	ErrSrcMbSame              = errCode(17)
	ErrAppDeployed            = errCode(20)
	ErrAppNotInit             = errCode(21)
	ErrAppNotUndeployed       = errCode(22)
	ErrStatusesNotFound       = errCode(23)
	ErrBucketTypeCheck        = errCode(25)
	ErrMemcachedBucket        = errCode(26)
	ErrHandlerCompile         = errCode(27)
	ErrAppNameMismatch        = errCode(29)
	ErrSaveConfig             = errCode(33)
	ErrGetConfig              = errCode(34)
	ErrGetRebStatus           = errCode(35)
	ErrRebOrFailoverOngoing   = errCode(36)
	ErrInvalidConfig          = errCode(38)
	ErrAppCodeSize            = errCode(39)
	ErrBucketMissing          = errCode(41)
	ErrClusterVersion         = errCode(42)
	ErrDebuggerDisabled       = errCode(45)
	ErrMixedMode              = errCode(46)
	ErrBucketAccess           = errCode(49)
	ErrInterFunctionRecursion = errCode(50)
	ErrInterBucketRecursion   = errCode(51)
	ErrSyncGatewayEnabled     = errCode(52)
	ErrAppNotFound            = errCode(53)
	ErrMetakvWriteFailed      = errCode(54)
	ErrRequestedOpFailed      = errCode(55)
	ErrCollectionMissing      = errCode(56)
	ErrMagmaStorage           = errCode(58)
	ErrInternalServer         = errCode(59)
	ErrForbidden              = errCode(60)
	ErrUnauthenticated        = errCode(61)
	ErrMethodNotAllowed       = errCode(62)
	ErrInvalidRequest         = errCode(63)
	ErrAppPaused              = errCode(64)
)

// Shouldn't expose internal details
// Keep only those error codes which are relevant to user
var (
	errMap = map[errCode]ErrorInfo{
		Ok:                        ErrorInfo{httpStatusCode: http.StatusOK, Name: "OK", Code: 0},
		ErrDelAppPs:               ErrorInfo{httpStatusCode: http.StatusInternalServerError, Name: "ERR_DEL_APP_PS", Code: 1},
		ErrDelAppTs:               ErrorInfo{httpStatusCode: http.StatusInternalServerError, Name: "ERR_DEL_APP_TS", Code: 2},
		ErrSaveAppPs:              ErrorInfo{httpStatusCode: http.StatusInternalServerError, Name: "ERR_SAVE_APP_PS", Code: 5, Attributes: []string{"retry"}},
		ErrSaveAppTs:              ErrorInfo{httpStatusCode: http.StatusInternalServerError, Name: "ERR_SAVE_APP_TS", Code: 6, Attributes: []string{"retry"}},
		ErrSetSettingsPs:          ErrorInfo{httpStatusCode: http.StatusInternalServerError, Name: "ERR_SET_SETTINGS_PS", Code: 7},
		ErrDelAppSettingsPs:       ErrorInfo{httpStatusCode: http.StatusInternalServerError, Name: "ERR_DEL_APP_SETTINGS_PS", Code: 11},
		ErrAppNotDeployed:         ErrorInfo{httpStatusCode: http.StatusNotAcceptable, Name: "ERR_APP_NOT_DEPLOYED", Code: 12},
		ErrAppNotFoundTs:          ErrorInfo{httpStatusCode: http.StatusNotFound, Name: "ERR_APP_NOT_FOUND_TS", Code: 13},
		ErrMarshalResp:            ErrorInfo{httpStatusCode: http.StatusInternalServerError, Name: "ERR_MARSHAL_RESP", Code: 14},
		ErrReadReq:                ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_READ_REQ", Code: 15},
		ErrUnmarshalPld:           ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_UNMARSHAL_PLD", Code: 16},
		ErrSrcMbSame:              ErrorInfo{httpStatusCode: http.StatusUnprocessableEntity, Name: "ERR_SRC_MB_SAME", Code: 17},
		ErrAppDeployed:            ErrorInfo{httpStatusCode: http.StatusUnprocessableEntity, Name: "ERR_APP_ALREADY_DEPLOYED", Code: 20},
		ErrAppNotInit:             ErrorInfo{httpStatusCode: http.StatusLocked, Name: "ERR_APP_NOT_BOOTSTRAPPED", Code: 21},
		ErrAppNotUndeployed:       ErrorInfo{httpStatusCode: http.StatusUnprocessableEntity, Name: "ERR_APP_NOT_UNDEPLOYED", Code: 22},
		ErrStatusesNotFound:       ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_PROCESSING_OR_DEPLOYMENT_STATUS_NOT_FOUND", Code: 23},
		ErrBucketTypeCheck:        ErrorInfo{httpStatusCode: http.StatusUnprocessableEntity, Name: "ERR_BUCKET_TYPE_CHECK", Code: 25},
		ErrMemcachedBucket:        ErrorInfo{httpStatusCode: http.StatusUnprocessableEntity, Name: "ERR_BUCKET_MEMCACHED", Code: 26},
		ErrHandlerCompile:         ErrorInfo{httpStatusCode: http.StatusUnprocessableEntity, Name: "ERR_HANDLER_COMPILATION", Code: 27},
		ErrAppNameMismatch:        ErrorInfo{httpStatusCode: http.StatusUnprocessableEntity, Name: "ERR_APPNAME_MISMATCH", Code: 29},
		ErrSaveConfig:             ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_SAVE_CONFIG", Code: 33},
		ErrGetConfig:              ErrorInfo{httpStatusCode: http.StatusInternalServerError, Name: "ERR_GET_CONFIG", Code: 34},
		ErrGetRebStatus:           ErrorInfo{httpStatusCode: http.StatusInternalServerError, Name: "ERR_GET_REBALANCE_STATUS", Code: 35},
		ErrRebOrFailoverOngoing:   ErrorInfo{httpStatusCode: http.StatusNotAcceptable, Name: "ERR_REBALANCE_OR_FAILOVER_ONGOING", Code: 36},
		ErrInvalidConfig:          ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_INVALID_CONFIG", Code: 38},
		ErrAppCodeSize:            ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_APPCODE_SIZE", Code: 39},
		ErrBucketMissing:          ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_BUCKET_MISSING", Code: 41},
		ErrClusterVersion:         ErrorInfo{httpStatusCode: http.StatusNotAcceptable, Name: "ERR_CLUSTER_VERSION", Code: 42},
		ErrDebuggerDisabled:       ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_DEBUGGER_DISABLED", Code: 45},
		ErrMixedMode:              ErrorInfo{httpStatusCode: http.StatusNotAcceptable, Name: "ERR_MIXED_MODE", Code: 46},
		ErrBucketAccess:           ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_BUCKET_ACCESS", Code: 49},
		ErrInterFunctionRecursion: ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_INTER_FUNCTION_RECURSION", Code: 50},
		ErrInterBucketRecursion:   ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_INTER_BUCKET_RECURSION", Code: 51},
		ErrSyncGatewayEnabled:     ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_SYNC_GATEWAY_ENABLED", Code: 52},
		ErrAppNotFound:            ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_APP_NOT_FOUND", Code: 53},
		ErrMetakvWriteFailed:      ErrorInfo{httpStatusCode: http.StatusInternalServerError, Name: "ERR_METAKV_WRITE_FAILED", Code: 54},
		ErrRequestedOpFailed:      ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_REQUESTED_OP_FAILED", Code: 55},
		ErrCollectionMissing:      ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_COLLECTION_MISSING", Code: 56},
		ErrMagmaStorage:           ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_MAGMA_BACKEND", Code: 58, Description: "Magma buckets are not yet supported in Eventing"},
		ErrInternalServer:         ErrorInfo{httpStatusCode: http.StatusInternalServerError, Name: "INTERNAL_SERVER_ERROR", Code: 59, Attributes: []string{"retry"}},
		ErrForbidden:              ErrorInfo{httpStatusCode: http.StatusForbidden, Name: "ERR_FORBIDDEN", Code: 60},
		ErrUnauthenticated:        ErrorInfo{httpStatusCode: http.StatusUnauthorized, Name: "ERR_UNAUTHENTICATED", Code: 61, Description: "Unauthenticated User"},
		ErrMethodNotAllowed:       ErrorInfo{httpStatusCode: http.StatusMethodNotAllowed, Name: "ERR_METHOD_NOT_ALLOWED", Code: 62},
		ErrInvalidRequest:         ErrorInfo{httpStatusCode: http.StatusBadRequest, Name: "ERR_INVALID_REQUEST", Code: 63},
		ErrAppPaused:              ErrorInfo{httpStatusCode: http.StatusUnprocessableEntity, Name: "ERR_APP_PAUSED", Code: 64},
	}
)

// For context of the request
type eventCode int

const (
	// System events done through rest
	EventStartTracing eventCode = iota
	EventStopTracing
	EventStartDebugger
	EventStopDebugger
	EventCleanupEventing
	EventDie
	EventTriggerGC
	EventFreeOSMemory
	EventGetUUID
	EventGetVersion
	EventGetErrCodes
	EventGetCpuCount
	EventGetWorkerCount
	EventGetLogFileLocation
	EventSaveConfig
	EventGetConfig

	// Eventing function crud operation
	EventCreateFunction
	EventDeployFunction
	EventPauseFunction
	EventResumeFunction
	EventUndeployFunction
	EventDeleteFunction
	// Includes settings change and function change
	EventUpdateFunction
	EventSetFunctionConfig

	// Function stat event
	EventClearStats
	EventFetchStats
	EventFetchProcessingStats
	EventFetchLatencyStats
	EventFetchExecutionStats
	EventFetchFailureStats

	// Function retrieve
	EventImportFunctions
	EventExportFunctions
	EventGetDeployedApps
	EventGetRunningApps
	EventGetBootstrappingApps
	EventGetPausingApps
	EventGetFunctionStatus
	EventBackupFunctions
	EventRestoreFunctions
	EventGetFunctionDraft
	EventGetInsight
	EventGetAppLog
	EventGetFunctionConfig
	EventListAllfunction
	EventGetAnnotations
	EventGetDebuggerUrl
	EventWriteDebuggerUrl

	// Miscellaneous functions
	EventResdistributeWorkload
	EventGetRebalanceProgress
	EventGetRebalanceStatus
	EventGetBootstrapStatus
	EventGetSeqProcessed
	EventGetDcpEventsRemaining
	EventGetConsumerPids
	EventGetUserInfo
	EventGetRuntimeProfiling

	EventGetAppStats
)

type log uint8

const (
	AuditLog  log = 0b_00000001
	SystemLog     = 0b_00000010
)

type event struct {
	description string
	log         log
	auditLogVal auditevent.AuditEvent
	systemLogId systemeventlog.EventId
}

func (e *event) GetAuditId() (auditevent.AuditEvent, bool) {
	if (e.log & AuditLog) != AuditLog {
		return 0, false
	}
	return e.auditLogVal, true
}

func (e *event) GetSystemLogVal() (systemeventlog.EventId, bool) {
	if (e.log & SystemLog) != SystemLog {
		return 0, false
	}
	return e.systemLogId, true
}

func (e *event) String() string {
	return fmt.Sprintf("%s", e.description)
}

var (
	eventMap = map[eventCode]*event{
		// Audit and system events will be logged by the handler
		EventStartTracing: &event{
			description: "Start tracing",
		},

		EventStopTracing: &event{
			description: "Tracing stopped",
		},

		EventStartDebugger: &event{
			description: "Debugger started",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.StartDebug,
			systemLogId: util.EVENTID_START_DEBUGGER,
		},

		EventStopDebugger: &event{
			description: "Debugger stopped",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.StopDebug,
			systemLogId: util.EVENTID_STOP_DEBUGGER,
		},

		EventCleanupEventing: &event{
			description: "Cleanup Eventing",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.CleanupEventing,
			systemLogId: util.EVENTID_CLEANUP_EVENTING,
		},

		EventDie: &event{
			description: "Killing all eventing consumers and the eventing producer",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.Die,
			systemLogId: util.EVENTID_DIE,
		},

		EventTriggerGC: &event{
			description: "Trigger GC",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.EventingSystemEvent,
			systemLogId: util.EVENTID_TRIGGER_GC,
		},

		EventFreeOSMemory: &event{
			description: "Freeing up memory to OS",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.EventingSystemEvent,
			systemLogId: util.EVENTID_FREE_OS_MEMORY,
		},

		EventGetUUID: &event{
			description: "Get node uuid",
			log:         AuditLog,
			auditLogVal: auditevent.EventingClusterStats,
		},

		EventGetVersion: &event{
			description: "Get node version",
			log:         AuditLog,
			auditLogVal: auditevent.EventingClusterStats,
		},

		EventGetErrCodes: &event{
			description: "Get Eventing error codes",
			log:         AuditLog,
			auditLogVal: auditevent.EventingClusterStats,
		},

		EventGetCpuCount: &event{
			description: "Get cpu count",
			log:         AuditLog,
			auditLogVal: auditevent.EventingClusterStats,
		},

		EventGetWorkerCount: &event{
			description: "Get worker count",
			log:         AuditLog,
			auditLogVal: auditevent.EventingClusterStats,
		},

		EventGetLogFileLocation: &event{
			description: "Get log file location",
			log:         AuditLog,
			auditLogVal: auditevent.EventingClusterStats,
		},

		EventSaveConfig: &event{
			description: "Set global eventing config",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.SaveConfig,
			systemLogId: util.EVENTID_UPDATE_CONFIG,
		},

		EventGetConfig: &event{
			description: "Get global eventing config",
			log:         AuditLog,
			auditLogVal: auditevent.EventingClusterStats,
		},

		EventCreateFunction: &event{
			description: "Create Function",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.CreateFunction,
			systemLogId: util.EVENTID_CREATE_FUNCTION,
		},

		EventDeployFunction: &event{
			description: "Deploy Function",
			log:         AuditLog,
			auditLogVal: auditevent.DeployFunction,
		},

		EventPauseFunction: &event{
			description: "Pause function",
			log:         AuditLog,
			auditLogVal: auditevent.PauseFunction,
		},

		EventResumeFunction: &event{
			description: "Resume function",
			log:         AuditLog,
			auditLogVal: auditevent.ResumeFunction,
		},

		EventUndeployFunction: &event{
			description: "Undeploy function",
			log:         AuditLog,
			auditLogVal: auditevent.UndeployFunction,
		},

		EventDeleteFunction: &event{
			description: "Delete Function",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.DeleteFunction,
			systemLogId: util.EVENTID_DELETE_FUNCTION,
		},

		EventUpdateFunction: &event{
			description: "Function settings or function changed",
			log:         AuditLog,
			auditLogVal: auditevent.SaveDraft,
		},

		EventSetFunctionConfig: &event{
			description: "Set function deployment config",
			log:         AuditLog,
			auditLogVal: auditevent.SaveDraft,
		},

		EventClearStats: &event{
			description: "Request to clear eventing function statistics",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.ClearStats,
			systemLogId: util.EVENTID_CLEAR_STATISTICS,
		},

		EventFetchStats: &event{
			description: "Request to fetch eventing function statistics",
			log:         AuditLog,
			auditLogVal: auditevent.FetchStats,
		},

		EventFetchProcessingStats: &event{
			description: "Request to fetch eventing function processing statistics",
			log:         AuditLog,
			auditLogVal: auditevent.FetchStats,
		},

		EventFetchLatencyStats: &event{
			description: "Request to fetch eventing function latency statistics",
			log:         AuditLog,
			auditLogVal: auditevent.FetchStats,
		},

		EventFetchExecutionStats: &event{
			description: "Request to fetch eventing function execution statistics",
			log:         AuditLog,
			auditLogVal: auditevent.FetchStats,
		},

		EventFetchFailureStats: &event{
			description: "Request to fetch eventing function failure statistics",
			log:         AuditLog,
			auditLogVal: auditevent.FetchStats,
		},

		EventImportFunctions: &event{
			description: "Import Functions",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.ImportFunctions,
			systemLogId: util.EVENTID_IMPORT_FUNCTIONS,
		},

		EventExportFunctions: &event{
			description: "Export Functions",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.ExportFunctions,
			systemLogId: util.EVENTID_EXPORT_FUNCTIONS,
		},
		EventGetDeployedApps: &event{
			description: "Get deployed app list",
			log:         AuditLog,
			auditLogVal: auditevent.ListFunction,
		},

		EventGetRunningApps: &event{
			description: "Get running app list",
			log:         AuditLog,
			auditLogVal: auditevent.ListRunning,
		},

		EventGetBootstrappingApps: &event{
			description: "Get bootstrapping app list",
			log:         AuditLog,
			auditLogVal: auditevent.ListFunction,
		},

		EventGetPausingApps: &event{
			description: "Get pausing app list",
			log:         AuditLog,
			auditLogVal: auditevent.ListFunction,
		},

		EventGetFunctionStatus: &event{
			description: "Get app status",
			log:         AuditLog,
			auditLogVal: auditevent.FunctionStatus,
		},

		EventBackupFunctions: &event{
			description: "Request to backup eventing function",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.BackupFunctions,
			systemLogId: util.EVENTID_BACKUP_FUNCTION,
		},

		EventRestoreFunctions: &event{
			description: "Request to restore eventing function",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.RestoreFunctions,
			systemLogId: util.EVENTID_RESTORE_FUNCTION,
		},

		EventGetFunctionDraft: &event{
			description: "Eventing get function",
			log:         AuditLog,
			auditLogVal: auditevent.FetchDrafts,
		},

		EventGetInsight: &event{
			description: "Get app insights",
			log:         AuditLog,
			auditLogVal: auditevent.FunctionStatus,
		},

		EventGetAppLog: &event{
			description: "Get app logs",
			log:         AuditLog,
			auditLogVal: auditevent.FunctionStatus,
		},

		EventGetFunctionConfig: &event{
			description: "Get function configuration",
			log:         AuditLog,
			auditLogVal: auditevent.FunctionStatus,
		},

		EventGetAnnotations: &event{
			description: "Get app annotations",
			log:         AuditLog,
			auditLogVal: auditevent.FunctionStatus,
		},

		EventGetDebuggerUrl: &event{
			description: "Get Debugger URL",
			log:         AuditLog,
			auditLogVal: auditevent.FunctionStatus,
		},

		EventWriteDebuggerUrl: &event{
			description: "Write Debugger URL",
			log:         AuditLog,
			auditLogVal: auditevent.FunctionStatus,
		},

		EventResdistributeWorkload: &event{
			description: "Re distribution of workload",
			log:         AuditLog | SystemLog,
			auditLogVal: auditevent.EventingClusterStats,
		},

		EventGetRebalanceProgress: &event{
			description: "Get rebalance progress",
			log:         AuditLog,
			auditLogVal: auditevent.EventingClusterStats,
		},

		EventGetRebalanceStatus: &event{
			description: "Get rebalance status",
			log:         AuditLog,
			auditLogVal: auditevent.EventingClusterStats,
		},

		EventGetBootstrapStatus: &event{
			description: "Bootstrap status",
			log:         AuditLog,
			auditLogVal: auditevent.FunctionStatus,
		},

		EventGetSeqProcessed: &event{
			description: "Seq number processed",
			log:         AuditLog,
			auditLogVal: auditevent.FunctionStatus,
		},

		EventGetDcpEventsRemaining: &event{
			description: "Dcp events remaining to process",
			log:         AuditLog,
			auditLogVal: auditevent.FunctionStatus,
		},

		EventGetConsumerPids: &event{
			description: "Get consumer pids",
			log:         AuditLog,
			auditLogVal: auditevent.FunctionStatus,
		},

		EventListAllfunction: &event{
			description: "List all functions",
			log:         AuditLog,
			auditLogVal: auditevent.ListFunction,
		},

		EventGetUserInfo: &event{
			description: "Get user info",
			log:         AuditLog,
			auditLogVal: auditevent.GetUserInfo,
		},

		EventGetRuntimeProfiling: &event{
			description: "Get user info",
			log:         AuditLog,
			auditLogVal: auditevent.GetUserInfo,
		},
	}
)
