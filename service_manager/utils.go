package servicemanager

import (
	"encoding/binary"

	"encoding/json"
	"fmt"
	"github.com/couchbase/cbauth/service"
	"net/http"
	"strconv"
)

func decodeRev(b service.Revision) uint64 {
	return binary.BigEndian.Uint64(b)
}

func encodeRev(rev uint64) service.Revision {
	ext := make(service.Revision, 8)
	binary.BigEndian.PutUint64(ext, rev)

	return ext
}

func (m *ServiceMgr) sendErrorInfo(w http.ResponseWriter, runtimeInfo *runtimeInfo) {
	errInfo := m.errorCodes[runtimeInfo.Code]
	errInfo.RuntimeInfo = runtimeInfo.Info
	response, err := json.Marshal(errInfo)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{\"error\":\"Failed to marshal error info, err: %v\"}", err)
		return
	}

	w.Header().Add(headerKey, strconv.Itoa(errInfo.Code))
	fmt.Fprintf(w, string(response))
}

func (m *ServiceMgr) sendRuntimeInfoList(w http.ResponseWriter, runtimeInfoList []*runtimeInfo) {
	response, err := json.Marshal(runtimeInfoList)
	if err != nil {
		w.Header().Add(headerKey, strconv.Itoa(m.statusCodes.errMarshalResp.Code))
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{\"error\":\"Failed to marshal error info, err: %v\"}", err)
		return
	}

	fmt.Fprintf(w, string(response))
}
