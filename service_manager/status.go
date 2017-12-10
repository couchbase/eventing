package servicemanager

import (
	"encoding/json"
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
	Name        string   `json:"name"`
	Code        int      `json:"code"`
	Description string   `json:"description"`
	Attributes  []string `json:"attributes"`
	RuntimeInfo string   `json:"runtime_info"`
}

type runtimeInfo struct {
	Code int    `json:"code"`
	Info string `json:"info"`
}

type statusCodes struct {
	ok                   statusBase
	errDelAppPs          statusBase
	errDelAppTs          statusBase
	errGetAppPs          statusBase
	getAppTs             statusBase
	errSaveAppPs         statusBase
	errSaveAppTs         statusBase
	errSetSettingsPs     statusBase
	startDbg             statusBase
	stopDbg              statusBase
	getDbgURL            statusBase
	errDelAppSettingsPs  statusBase
	errAppNotDeployed    statusBase
	errAppNotFoundTs     statusBase
	errMarshalResp       statusBase
	errReadReq           statusBase
	errUnmarshalPld      statusBase
	errSrcMbSame         statusBase
	errInvalidExt        statusBase
	errGetVbSeqs         statusBase
	errAppDeployed       statusBase
	errAppNotInit        statusBase
	errAppNotUndeployed  statusBase
	errStatusesNotFound  statusBase
	errConnectNsServer   statusBase
	errBucketTypeCheck   statusBase
	errMemcachedBucket   statusBase
	errHandlerCompile    statusBase
	errRbacCreds         statusBase
	errAppNameMismatch   statusBase
	errSrcBucketMissing  statusBase
	errMetaBucketMissing statusBase
}

func (m *ServiceMgr) initErrCodes() {
	m.statusCodes = statusCodes{
		ok:                   statusBase{"OK", 0},
		errDelAppPs:          statusBase{"ERR_DEL_APP_PS", 1},
		errDelAppTs:          statusBase{"ERR_DEL_APP_TS", 2},
		errGetAppPs:          statusBase{"ERR_GET_APP_PS", 3},
		getAppTs:             statusBase{"ERR_GET_APP_TS", 4},
		errSaveAppPs:         statusBase{"ERR_SAVE_APP_PS", 5},
		errSaveAppTs:         statusBase{"ERR_SAVE_APP_TS", 6},
		errSetSettingsPs:     statusBase{"ERR_SET_SETTINGS_PS", 7},
		startDbg:             statusBase{"ERR_START_DBG", 8},
		stopDbg:              statusBase{"ERR_STOP_DBG", 9},
		getDbgURL:            statusBase{"ERR_GET_DBG_URL", 10},
		errDelAppSettingsPs:  statusBase{"ERR_DEL_APP_SETTINGS_PS", 11},
		errAppNotDeployed:    statusBase{"ERR_APP_NOT_DEPLOYED", 12},
		errAppNotFoundTs:     statusBase{"ERR_APP_NOT_FOUND_TS", 13},
		errMarshalResp:       statusBase{"ERR_MARSHAL_RESP", 14},
		errReadReq:           statusBase{"ERR_READ_REQ", 15},
		errUnmarshalPld:      statusBase{"ERR_UNMARSHAL_PLD", 16},
		errSrcMbSame:         statusBase{"ERR_SRC_MB_SAME", 17},
		errInvalidExt:        statusBase{"ERR_INVALID_EXT", 18},
		errGetVbSeqs:         statusBase{"ERR_GET_VB_SEQS", 19},
		errAppDeployed:       statusBase{"ERR_APP_ALREADY_DEPLOYED", 20},
		errAppNotInit:        statusBase{"ERR_APP_NOT_BOOTSTRAPPED", 21},
		errAppNotUndeployed:  statusBase{"ERR_APP_NOT_UNDEPLOYED", 22},
		errStatusesNotFound:  statusBase{"ERR_PROCESSING_OR_DEPLOYMENT_STATUS_NOT_FOUND", 23},
		errConnectNsServer:   statusBase{"ERR_CONNECT_TO_NS_SERVER", 24},
		errBucketTypeCheck:   statusBase{"ERR_BUCKET_TYPE_CHECK", 25},
		errMemcachedBucket:   statusBase{"ERR_SOURCE_BUCKET_MEMCACHED", 26},
		errHandlerCompile:    statusBase{"ERR_HANDLER_COMPILATION", 27},
		errRbacCreds:         statusBase{"ERR_INSUFFICIENT_RBAC_CREDS", 28},
		errAppNameMismatch:   statusBase{"ERR_APPNAME_MISMATCH", 29},
		errSrcBucketMissing:  statusBase{"ERR_SRC_BUCKET_MISSING", 30},
		errMetaBucketMissing: statusBase{"ERR_METADATA_BUCKET_MISSING", 31},
	}

	errors := []errorPayload{
		{
			Name:        m.statusCodes.errDelAppPs.Name,
			Code:        m.statusCodes.errDelAppPs.Code,
			Description: "Unable to delete application from primary store",
		},
		{
			Name:        m.statusCodes.errDelAppTs.Name,
			Code:        m.statusCodes.errDelAppTs.Code,
			Description: "Unable to delete application from temporary store",
		},
		{
			Name:        m.statusCodes.errGetAppPs.Name,
			Code:        m.statusCodes.errGetAppPs.Code,
			Description: "Unable to get application from primary store",
			Attributes:  []string{"retry"},
		},
		{
			Name:        m.statusCodes.getAppTs.Name,
			Code:        m.statusCodes.getAppTs.Code,
			Description: "Unable to get application from temporary store",
			Attributes:  []string{"retry"},
		},
		{
			Name:        m.statusCodes.errSaveAppPs.Name,
			Code:        m.statusCodes.errSaveAppPs.Code,
			Description: "Unable to save application to primary store",
		},
		{
			Name:        m.statusCodes.errSaveAppTs.Name,
			Code:        m.statusCodes.errSaveAppTs.Code,
			Description: "Unable to save application to temporary store",
			Attributes:  []string{"retry"},
		},
		{
			Name:        m.statusCodes.errSetSettingsPs.Name,
			Code:        m.statusCodes.errSetSettingsPs.Code,
			Description: "Unable to set application settings in primary store",
		},
		{
			Name:        m.statusCodes.startDbg.Name,
			Code:        m.statusCodes.startDbg.Code,
			Description: "Unable to start debugger",
		},
		{
			Name:        m.statusCodes.stopDbg.Name,
			Code:        m.statusCodes.stopDbg.Code,
			Description: "Unable to stop debugger",
		},
		{
			Name:        m.statusCodes.getDbgURL.Name,
			Code:        m.statusCodes.getDbgURL.Code,
			Description: "Unable to get debugger URL",
		},
		{
			Name:        m.statusCodes.errDelAppSettingsPs.Name,
			Code:        m.statusCodes.errDelAppSettingsPs.Code,
			Description: "Unable to delete app settings",
		},
		{
			Name:        m.statusCodes.errAppNotDeployed.Name,
			Code:        m.statusCodes.errAppNotDeployed.Code,
			Description: "Application not deployed",
		},
		{
			Name:        m.statusCodes.errAppNotFoundTs.Name,
			Code:        m.statusCodes.errAppNotFoundTs.Code,
			Description: "Application not found in temporary store",
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
			Description: "App is already deployed",
		},
		{
			Name:        m.statusCodes.errAppNotInit.Name,
			Code:        m.statusCodes.errAppNotInit.Code,
			Description: "App hasn't bootstrapped",
		},
		{
			Name:        m.statusCodes.errAppNotUndeployed.Name,
			Code:        m.statusCodes.errAppNotUndeployed.Code,
			Description: "App hasn't been undeployed",
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
			Description: "Handler compilation failed",
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
