package servicemanager

import (
	"encoding/json"
	"net/http"

	"github.com/couchbase/eventing/logging"
)

type statusBase struct {
	Name string
	Code int
}

type statusPayload struct {
	HeaderKey string         `json:"header_key"`
	Version   int            `json:"version"`
	Revision  int            `json:"revision"`
	Errors    []errorPayload `json:"errors"`
}

type errorPayload struct {
	Name        string      `json:"name"`
	Code        int         `json:"code"`
	Description string      `json:"description"`
	Attributes  []string    `json:"attributes"`
	RuntimeInfo runtimeInfo `json:"runtime_info"`
}

type runtimeInfo struct {
	Code int         `json:"code"`
	Info interface{} `json:"info"`
}

type warningsInfo struct {
	Status   string   `json:"status"`
	Warnings []string `json:"warnings"`
}

type statusCodes struct {
	ok                        statusBase
	errDelAppPs               statusBase
	errDelAppTs               statusBase
	errGetAppPs               statusBase
	getAppTs                  statusBase
	errSaveAppPs              statusBase
	errSaveAppTs              statusBase
	errSetSettingsPs          statusBase
	errDelAppSettingsPs       statusBase
	errAppNotDeployed         statusBase
	errAppNotFoundTs          statusBase
	errMarshalResp            statusBase
	errReadReq                statusBase
	errUnmarshalPld           statusBase
	errSrcMbSame              statusBase
	errInvalidExt             statusBase
	errGetVbSeqs              statusBase
	errAppDeployed            statusBase
	errAppNotInit             statusBase
	errAppNotUndeployed       statusBase
	errStatusesNotFound       statusBase
	errConnectNsServer        statusBase
	errBucketTypeCheck        statusBase
	errMemcachedBucket        statusBase
	errHandlerCompile         statusBase
	errRbacCreds              statusBase
	errAppNameMismatch        statusBase
	errSrcBucketMissing       statusBase
	errMetaBucketMissing      statusBase
	errNoEventingNodes        statusBase
	errSaveConfig             statusBase
	errGetConfig              statusBase
	errGetCreds               statusBase
	errGetRebStatus           statusBase
	errRebOngoing             statusBase
	errActiveEventingNodes    statusBase
	errInvalidConfig          statusBase
	errAppCodeSize            statusBase
	errAppRetry               statusBase
	errBucketMissing          statusBase
	errClusterVersion         statusBase
	errUUIDGen                statusBase
	errAppDelete              statusBase
	errDebuggerDisabled       statusBase
	errMixedMode              statusBase
	errFunctionIDGen          statusBase
	errFunctionInstanceIDGen  statusBase
	errBucketAccess           statusBase
	errInterFunctionRecursion statusBase
	errInterBucketRecursion   statusBase
}

func (m *ServiceMgr) getDisposition(code int) int {
	switch code {
	case m.statusCodes.ok.Code:
		return http.StatusOK
	case m.statusCodes.errDelAppPs.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errDelAppTs.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errSaveAppPs.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errSaveAppTs.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errSetSettingsPs.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errDelAppSettingsPs.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errAppNotDeployed.Code:
		return http.StatusNotAcceptable
	case m.statusCodes.errAppNotFoundTs.Code:
		return http.StatusNotFound
	case m.statusCodes.errMarshalResp.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errReadReq.Code:
		return http.StatusBadRequest
	case m.statusCodes.errUnmarshalPld.Code:
		return http.StatusBadRequest
	case m.statusCodes.errSrcMbSame.Code:
		return http.StatusUnprocessableEntity
	case m.statusCodes.errInvalidExt.Code:
		return http.StatusBadRequest
	case m.statusCodes.errGetVbSeqs.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errAppDeployed.Code:
		return http.StatusUnprocessableEntity
	case m.statusCodes.errAppNotInit.Code:
		return http.StatusLocked
	case m.statusCodes.errAppNotUndeployed.Code:
		return http.StatusUnprocessableEntity
	case m.statusCodes.errStatusesNotFound.Code:
		return http.StatusBadRequest
	case m.statusCodes.errConnectNsServer.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errBucketTypeCheck.Code:
		return http.StatusUnprocessableEntity
	case m.statusCodes.errMemcachedBucket.Code:
		return http.StatusUnprocessableEntity
	case m.statusCodes.errHandlerCompile.Code:
		return http.StatusUnprocessableEntity
	case m.statusCodes.errAppNameMismatch.Code:
		return http.StatusUnprocessableEntity
	case m.statusCodes.errSrcBucketMissing.Code:
		return http.StatusGone
	case m.statusCodes.errMetaBucketMissing.Code:
		return http.StatusGone
	case m.statusCodes.errNoEventingNodes.Code:
		return http.StatusBadRequest
	case m.statusCodes.errSaveConfig.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errGetConfig.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errGetRebStatus.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errRebOngoing.Code:
		return http.StatusNotAcceptable
	case m.statusCodes.errInvalidConfig.Code:
		return http.StatusBadRequest
	case m.statusCodes.errAppCodeSize.Code:
		return http.StatusBadRequest
	case m.statusCodes.errAppRetry.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errBucketMissing.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errClusterVersion.Code:
		return http.StatusNotAcceptable
	case m.statusCodes.errUUIDGen.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errAppDelete.Code:
		return http.StatusBadRequest
	case m.statusCodes.errDebuggerDisabled.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errMixedMode.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errFunctionIDGen.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errFunctionInstanceIDGen.Code:
		return http.StatusInternalServerError
	case m.statusCodes.errBucketAccess.Code:
		return http.StatusBadRequest
	case m.statusCodes.errInterFunctionRecursion.Code:
		return http.StatusBadRequest
	case m.statusCodes.errInterBucketRecursion.Code:
		return http.StatusBadRequest
	default:
		logging.Warnf("Unknown status code: %v", code)
		return http.StatusInternalServerError
	}
}

func (m *ServiceMgr) initErrCodes() {
	m.statusCodes = statusCodes{
		ok:                        statusBase{"OK", 0},
		errDelAppPs:               statusBase{"ERR_DEL_APP_PS", 1},
		errDelAppTs:               statusBase{"ERR_DEL_APP_TS", 2},
		errSaveAppPs:              statusBase{"ERR_SAVE_APP_PS", 5},
		errSaveAppTs:              statusBase{"ERR_SAVE_APP_TS", 6},
		errSetSettingsPs:          statusBase{"ERR_SET_SETTINGS_PS", 7},
		errDelAppSettingsPs:       statusBase{"ERR_DEL_APP_SETTINGS_PS", 11},
		errAppNotDeployed:         statusBase{"ERR_APP_NOT_DEPLOYED", 12},
		errAppNotFoundTs:          statusBase{"ERR_APP_NOT_FOUND_TS", 13},
		errMarshalResp:            statusBase{"ERR_MARSHAL_RESP", 14},
		errReadReq:                statusBase{"ERR_READ_REQ", 15},
		errUnmarshalPld:           statusBase{"ERR_UNMARSHAL_PLD", 16},
		errSrcMbSame:              statusBase{"ERR_SRC_MB_SAME", 17},
		errInvalidExt:             statusBase{"ERR_INVALID_EXT", 18},
		errGetVbSeqs:              statusBase{"ERR_GET_VB_SEQS", 19},
		errAppDeployed:            statusBase{"ERR_APP_ALREADY_DEPLOYED", 20},
		errAppNotInit:             statusBase{"ERR_APP_NOT_BOOTSTRAPPED", 21},
		errAppNotUndeployed:       statusBase{"ERR_APP_NOT_UNDEPLOYED", 22},
		errStatusesNotFound:       statusBase{"ERR_PROCESSING_OR_DEPLOYMENT_STATUS_NOT_FOUND", 23},
		errConnectNsServer:        statusBase{"ERR_CONNECT_TO_NS_SERVER", 24},
		errBucketTypeCheck:        statusBase{"ERR_BUCKET_TYPE_CHECK", 25},
		errMemcachedBucket:        statusBase{"ERR_SOURCE_BUCKET_MEMCACHED", 26},
		errHandlerCompile:         statusBase{"ERR_HANDLER_COMPILATION", 27},
		errAppNameMismatch:        statusBase{"ERR_APPNAME_MISMATCH", 29},
		errSrcBucketMissing:       statusBase{"ERR_SRC_BUCKET_MISSING", 30},
		errMetaBucketMissing:      statusBase{"ERR_METADATA_BUCKET_MISSING", 31},
		errNoEventingNodes:        statusBase{"ERR_NO_EVENTING_NODES_FOUND", 32},
		errSaveConfig:             statusBase{"ERR_SAVE_CONFIG", 33},
		errGetConfig:              statusBase{"ERR_GET_CONFIG", 34},
		errGetRebStatus:           statusBase{"ERR_GET_REBALANCE_STATUS", 35},
		errRebOngoing:             statusBase{"ERR_REBALANCE_ONGOING", 36},
		errActiveEventingNodes:    statusBase{"ERR_FETCHING_ACTIVE_EVENTING_NODES", 37},
		errInvalidConfig:          statusBase{"ERR_INVALID_CONFIG", 38},
		errAppCodeSize:            statusBase{"ERR_APPCODE_SIZE", 39},
		errAppRetry:               statusBase{"ERR_APP_RETRY", 40},
		errBucketMissing:          statusBase{"ERR_BUCKET_MISSING", 41},
		errClusterVersion:         statusBase{"ERR_CLUSTER_VERSION", 42},
		errUUIDGen:                statusBase{"ERR_UUID_GEN_FAILED", 43},
		errAppDelete:              statusBase{"ERR_APP_DELETE_NOT_ALLOWED", 44},
		errDebuggerDisabled:       statusBase{"ERR_DEBUGGER_DISABLED", 45},
		errMixedMode:              statusBase{"ERR_MIXED_MODE", 46},
		errFunctionIDGen:          statusBase{"ERR_HANDLER_ID_GEN", 47},
		errFunctionInstanceIDGen:  statusBase{"ERR_INSTANCE_ID_GEN", 48},
		errBucketAccess:           statusBase{"ERR_BUCKET_ACCESS", 49},
		errInterFunctionRecursion: statusBase{"ERR_INTER_FUNCTION_RECURSION", 50},
		errInterBucketRecursion:   statusBase{"ERR_INTER_BUCKET_RECURSION", 51},
	}

	errors := []errorPayload{
		{
			Name:        m.statusCodes.errDelAppPs.Name,
			Code:        m.statusCodes.errDelAppPs.Code,
			Description: "Unable to delete function from primary store",
		},
		{
			Name:        m.statusCodes.errDelAppTs.Name,
			Code:        m.statusCodes.errDelAppTs.Code,
			Description: "Unable to delete function from temporary store",
		},
		{
			Name:        m.statusCodes.errGetAppPs.Name,
			Code:        m.statusCodes.errGetAppPs.Code,
			Description: "Unable to get function from primary store",
			Attributes:  []string{"retry"},
		},
		{
			Name:        m.statusCodes.getAppTs.Name,
			Code:        m.statusCodes.getAppTs.Code,
			Description: "Unable to get function from temporary store",
			Attributes:  []string{"retry"},
		},
		{
			Name:        m.statusCodes.errSaveAppPs.Name,
			Code:        m.statusCodes.errSaveAppPs.Code,
			Description: "Unable to save function to primary store",
		},
		{
			Name:        m.statusCodes.errSaveAppTs.Name,
			Code:        m.statusCodes.errSaveAppTs.Code,
			Description: "Unable to save function to temporary store",
			Attributes:  []string{"retry"},
		},
		{
			Name:        m.statusCodes.errSetSettingsPs.Name,
			Code:        m.statusCodes.errSetSettingsPs.Code,
			Description: "Unable to set function settings in primary store",
		},
		{
			Name:        m.statusCodes.errDelAppSettingsPs.Name,
			Code:        m.statusCodes.errDelAppSettingsPs.Code,
			Description: "Unable to delete function settings",
		},
		{
			Name:        m.statusCodes.errAppNotDeployed.Name,
			Code:        m.statusCodes.errAppNotDeployed.Code,
			Description: "Function not deployed",
		},
		{
			Name:        m.statusCodes.errAppNotFoundTs.Name,
			Code:        m.statusCodes.errAppNotFoundTs.Code,
			Description: "Function not found in temporary store",
		},
		{
			Name:        m.statusCodes.errMarshalResp.Name,
			Code:        m.statusCodes.errMarshalResp.Code,
			Description: "Unable to marshal response",
		},
		{
			Name:        m.statusCodes.errReadReq.Name,
			Code:        m.statusCodes.errReadReq.Code,
			Description: "Unable to read the request body",
		},
		{
			Name:        m.statusCodes.errUnmarshalPld.Name,
			Code:        m.statusCodes.errUnmarshalPld.Code,
			Description: "Unable to unmarshal payload",
		},
		{
			Name:        m.statusCodes.errSrcMbSame.Name,
			Code:        m.statusCodes.errSrcMbSame.Code,
			Description: "Source bucket same as metadata bucket",
		},
		{
			Name:        m.statusCodes.errInvalidExt.Name,
			Code:        m.statusCodes.errInvalidExt.Code,
			Description: "Invalid file extension",
		},
		{
			Name:        m.statusCodes.errGetVbSeqs.Name,
			Code:        m.statusCodes.errGetVbSeqs.Code,
			Description: "Failed to fetch vb sequence processed so far",
		},
		{
			Name:        m.statusCodes.errAppDeployed.Name,
			Code:        m.statusCodes.errAppDeployed.Code,
			Description: "Function already deployed",
		},
		{
			Name:        m.statusCodes.errAppNotInit.Name,
			Code:        m.statusCodes.errAppNotInit.Code,
			Description: "Function hasn't bootstrapped",
		},
		{
			Name:        m.statusCodes.errAppNotUndeployed.Name,
			Code:        m.statusCodes.errAppNotUndeployed.Code,
			Description: "Function hasn't undeployed",
		},
		{
			Name:        m.statusCodes.errStatusesNotFound.Name,
			Code:        m.statusCodes.errStatusesNotFound.Code,
			Description: "Processing or deployment status or both missing from supplied settings",
		},
		{
			Name:        m.statusCodes.errConnectNsServer.Name,
			Code:        m.statusCodes.errConnectNsServer.Code,
			Description: "Failed to connect to cluster manager",
		},
		{
			Name:        m.statusCodes.errBucketTypeCheck.Name,
			Code:        m.statusCodes.errBucketTypeCheck.Code,
			Description: "Failed to check type of source bucket",
		},
		{
			Name:        m.statusCodes.errMemcachedBucket.Name,
			Code:        m.statusCodes.errMemcachedBucket.Code,
			Description: "Source bucket can't be of type memcached",
		},
		{
			Name:        m.statusCodes.errHandlerCompile.Name,
			Code:        m.statusCodes.errHandlerCompile.Code,
			Description: "Function compilation failed",
		},
		{
			Name:        m.statusCodes.errRbacCreds.Name,
			Code:        m.statusCodes.errRbacCreds.Code,
			Description: "RBAC username/password missing",
		},
		{
			Name:        m.statusCodes.errAppNameMismatch.Name,
			Code:        m.statusCodes.errAppNameMismatch.Code,
			Description: "Function names must be same",
		},
		{
			Name:        m.statusCodes.errSrcBucketMissing.Name,
			Code:        m.statusCodes.errSrcBucketMissing.Code,
			Description: "Source bucket missing",
		},
		{
			Name:        m.statusCodes.errMetaBucketMissing.Name,
			Code:        m.statusCodes.errMetaBucketMissing.Code,
			Description: "Metadata bucket missing",
		},
		{
			Name:        m.statusCodes.errNoEventingNodes.Name,
			Code:        m.statusCodes.errNoEventingNodes.Code,
			Description: "No eventing reported from cluster manager",
		},
		{
			Name:        m.statusCodes.errSaveConfig.Name,
			Code:        m.statusCodes.errSaveConfig.Code,
			Description: "Failed to save config to metakv",
		},
		{
			Name:        m.statusCodes.errGetConfig.Name,
			Code:        m.statusCodes.errGetConfig.Code,
			Description: "Failed to get config from metakv",
		},
		{
			Name:        m.statusCodes.errGetCreds.Name,
			Code:        m.statusCodes.errGetCreds.Code,
			Description: "Failed to get credentials from cbauth",
		},
		{
			Name:        m.statusCodes.errGetRebStatus.Name,
			Code:        m.statusCodes.errGetRebStatus.Code,
			Description: "Failed to get rebalance status from eventing nodes",
		},
		{
			Name:        m.statusCodes.errRebOngoing.Name,
			Code:        m.statusCodes.errRebOngoing.Code,
			Description: "Rebalance ongoing on some/all Eventing nodes, creating new functions, deployment or undeployment of existing functions is not allowed",
		},
		{
			Name:        m.statusCodes.errActiveEventingNodes.Name,
			Code:        m.statusCodes.errActiveEventingNodes.Code,
			Description: "Failed to fetch active Eventing nodes",
		},
		{
			Name:        m.statusCodes.errInvalidConfig.Name,
			Code:        m.statusCodes.errInvalidConfig.Code,
			Description: "Invalid configuration",
		},
		{
			Name:        m.statusCodes.errAppCodeSize.Name,
			Code:        m.statusCodes.errAppCodeSize.Code,
			Description: "Function Code size is more than the configured limit",
		},
		{
			Name:        m.statusCodes.errAppRetry.Name,
			Code:        m.statusCodes.errAppRetry.Code,
			Description: "Failed to notify retry to all eventing nodes",
		},
		{
			Name:        m.statusCodes.errBucketMissing.Name,
			Code:        m.statusCodes.errBucketMissing.Code,
			Description: "Bucket does not exist in the cluster",
		},
		{
			Name:        m.statusCodes.errClusterVersion.Name,
			Code:        m.statusCodes.errClusterVersion.Code,
			Description: "This function syntax is unsupported on current cluster version",
		},
		{
			Name:        m.statusCodes.errUUIDGen.Name,
			Code:        m.statusCodes.errUUIDGen.Code,
			Description: "UUID generation failed",
		},
		{
			Name:        m.statusCodes.errAppDelete.Name,
			Code:        m.statusCodes.errAppDelete.Code,
			Description: "Function needs to be undeployed before it can be deleted",
		},
		{
			Name:        m.statusCodes.errDebuggerDisabled.Name,
			Code:        m.statusCodes.errDebuggerDisabled.Code,
			Description: "Unable to start debugger as it has been disabled",
		},
		{
			Name:        m.statusCodes.errMixedMode.Name,
			Code:        m.statusCodes.errMixedMode.Code,
			Description: "Unable to start debugger in mixed mode cluster",
		},
		{
			Name:        m.statusCodes.errFunctionIDGen.Name,
			Code:        m.statusCodes.errFunctionIDGen.Code,
			Description: "Handler ID generation failed",
		},
		{
			Name:        m.statusCodes.errFunctionInstanceIDGen.Name,
			Code:        m.statusCodes.errFunctionInstanceIDGen.Code,
			Description: "Function Instance ID generation failed",
		},
		{
			Name:        m.statusCodes.errBucketAccess.Name,
			Code:        m.statusCodes.errBucketAccess.Code,
			Description: "Invalid bucket access error, access should be either \"r\" or \"w\"",
		},
		{
			Name:        m.statusCodes.errInterFunctionRecursion.Name,
			Code:        m.statusCodes.errInterFunctionRecursion.Code,
			Description: "Inter function recursion error, only one function is allowed to do source bucket mutation/delete on a bucket",
		},
		{
			Name:        m.statusCodes.errInterBucketRecursion.Name,
			Code:        m.statusCodes.errInterBucketRecursion.Code,
			Description: "Inter bucket recursion error, deployment of current handler will cause inter bucket recursion",
		},
	}

	m.errorCodes = make(map[int]errorPayload)
	for _, err := range errors {
		m.errorCodes[err.Code] = err
	}

	statusPayload := statusPayload{
		HeaderKey: headerKey,
		Version:   1,
		Revision:  1,
		Errors:    errors,
	}

	payload, err := json.Marshal(statusPayload)
	if err != nil {
		logging.Errorf("Unable marshal error codes: %v", err)
		return
	}

	m.statusPayload = payload
}
