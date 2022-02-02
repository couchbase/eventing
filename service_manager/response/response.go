package response

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/couchbase/eventing/audit"
	"github.com/couchbase/eventing/gen/auditevent"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/goutils/systemeventlog"
)

var (
	headerKey = "status"
)

type clientRequest struct {
	event       eventCode
	requestData map[string]interface{}
}

type responseWriter struct {
	writer        http.ResponseWriter
	request       *http.Request
	clientRequest *clientRequest
	runtimeInfo   *RuntimeInfo
}

func NewResponseWriter(w http.ResponseWriter, r *http.Request, event eventCode) *responseWriter {
	clientRequest := &clientRequest{
		event: event,
	}

	res := &responseWriter{
		writer:        w,
		request:       r,
		clientRequest: clientRequest,
	}
	return res
}

func (r *responseWriter) AddRequestData(key string, value interface{}) {
	if r.clientRequest.requestData == nil {
		r.clientRequest.requestData = make(map[string]interface{})
	}
	r.clientRequest.requestData[key] = value
}

func (r *responseWriter) SetRequestEvent(event eventCode) {
	r.clientRequest.event = event
}

func (r *responseWriter) Log(runtimeInfo *RuntimeInfo) {
	r.log(runtimeInfo)
}

func (r *responseWriter) LogAndSend(runtimeInfo *RuntimeInfo) {
	r.log(runtimeInfo)
	r.send(runtimeInfo)
}

func (r *responseWriter) send(runtimeInfo *RuntimeInfo) {
	if runtimeInfo == nil {
		return
	}

	var res interface{}
	httpCode := errMap[Ok].httpStatusCode
	sendRawResponse := runtimeInfo.SendRawDescription

	if runtimeInfo.ErrCode == Ok {
		if runtimeInfo.OnlyDescription {
			res = runtimeInfo.Description
		} else {
			sendRawResponse = false
			res = runtimeInfo
		}
	} else {
		errRes := errMap[runtimeInfo.ErrCode]
		errRes.Description = runtimeInfo.Description

		sendRawResponse = false
		res = errRes
		httpCode = errRes.httpStatusCode
	}

	w := r.writer
	if runtimeInfo.ContentType == "" {
		runtimeInfo.ContentType = "application/json"
	}
	w.Header().Set("Content-Type", runtimeInfo.ContentType)

	if sendRawResponse {
		w.WriteHeader(httpCode)
		fmt.Fprint(w, res)
		return
	}

	response, err := json.MarshalIndent(res, "", " ")
	if err != nil {
		errRes := errMap[ErrMarshalResp]
		httpCode = errRes.httpStatusCode

		w.WriteHeader(httpCode)
		fmt.Fprintf(w, "Failed to marshal response info, err: %v res: %v", err, res)
		return
	}

	w.WriteHeader(httpCode)
	fmt.Fprintf(w, "%s", response)
}

func (r *responseWriter) log(runtimeInfo *RuntimeInfo) {
	if runtimeInfo == nil {
		return
	}

	r.auditLog(runtimeInfo)
	r.systemLog(runtimeInfo)
}

// Logging of the request
func (r *responseWriter) auditLog(runtimeInfo *RuntimeInfo) {
	auditId, ok := isSpecialAuditErrorMessage(runtimeInfo.ErrCode)
	if ok {
		audit.Log(auditId, r.request, nil, nil, nil)
		return
	}

	context := r.clientRequest.event
	event, ok := eventMap[context]
	if !ok {
		return
	}

	auditId, ok = event.GetAuditId()
	if !ok {
		return
	}

	eventDescription := event.description
	request, errRes := r.getAuditRecord(runtimeInfo)
	audit.Log(auditId, r.request, eventDescription, request, errRes)
}

func (r *responseWriter) getAuditRecord(runtimeInfo *RuntimeInfo) (map[string]interface{}, *ErrorInfo) {
	if runtimeInfo.ErrCode == Ok {
		return r.clientRequest.requestData, nil
	}

	errRes := errMap[runtimeInfo.ErrCode]
	errRes.Description = runtimeInfo.Description
	errRes.Attributes = nil

	return r.clientRequest.requestData, &errRes
}

func isSpecialAuditErrorMessage(errCode errCode) (auditEventId auditevent.AuditEvent, audit bool) {
	switch errCode {
	case ErrUnauthenticated:
		auditEventId = auditevent.AuthenticationFailure
		audit = true
	case ErrForbidden:
		auditEventId = auditevent.AuthorizationFailure
		audit = true
	}
	return
}

func (r *responseWriter) systemLog(runtimeInfo *RuntimeInfo) {
	context := r.clientRequest.event
	event, ok := eventMap[context]
	if !ok {
		return
	}

	systemEventId, ok := event.GetSystemLogVal()
	if !ok {
		return
	}

	severity := systemeventlog.SEInfo
	var extraAttributes interface{}

	if runtimeInfo.ErrCode != Ok {
		severity = systemeventlog.SEError
		extraAttributes = runtimeInfo.Description
	} else if runtimeInfo.ExtraAttributes != nil {
		extraAttributes = runtimeInfo.ExtraAttributes
	}
	util.LogSystemEvent(systemEventId, severity, extraAttributes)
}
