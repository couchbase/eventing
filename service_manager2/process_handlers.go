package servicemanager2

import (
	"expvar"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/authenticator"
	"github.com/couchbase/eventing/authenticator/rbac"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/gen/version"
	"github.com/couchbase/eventing/notifier"
	"github.com/couchbase/eventing/service_manager2/response"
)

// pprof index handler
func (m *serviceMgr) indexHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRuntimeProfiling)

	runtimeInfo := &response.RuntimeInfo{}
	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	res.Log(runtimeInfo)
	pprof.Index(w, r)
}

// pprof cmdline handler
func (m *serviceMgr) cmdlineHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRuntimeProfiling)

	runtimeInfo := &response.RuntimeInfo{}
	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	res.Log(runtimeInfo)
	pprof.Cmdline(w, r)
}

// pprof profile handler
func (m *serviceMgr) profileHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRuntimeProfiling)

	runtimeInfo := &response.RuntimeInfo{}
	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	res.Log(runtimeInfo)
	pprof.Profile(w, r)
}

// pprof symbol handler
func (m *serviceMgr) symbolHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRuntimeProfiling)

	runtimeInfo := &response.RuntimeInfo{}
	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	res.Log(runtimeInfo)
	pprof.Symbol(w, r)
}

// pprof trace handler
func (m *serviceMgr) traceHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRuntimeProfiling)
	runtimeInfo := &response.RuntimeInfo{}

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	res.Log(runtimeInfo)
	pprof.Trace(w, r)
}

// expvar handler
func (m *serviceMgr) expvarHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetRuntimeProfiling)
	runtimeInfo := &response.RuntimeInfo{}

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	res.Log(runtimeInfo)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

func (m *serviceMgr) die(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventDie)
	runtimeInfo := &response.RuntimeInfo{}

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		res.LogAndSend(runtimeInfo)
		return
	}

	//TODO: Kill all the processes
	runtimeInfo.Description = "Killing all eventing consumers and the eventing producer"
	res.LogAndSend(runtimeInfo)
	time.Sleep(5 * time.Second)

	os.Exit(-1)
}

func (m *serviceMgr) freeOSMemory(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventFreeOSMemory)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	debug.FreeOSMemory()
	runtimeInfo.Description = "Freed up memory to OS"
}

func (m *serviceMgr) triggerGC(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventTriggerGC)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	runtime.GC()
	runtimeInfo.Description = "Finished GC run"
}

func (m *serviceMgr) getNodeUUID(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetUUID)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.OnlyDescription = true
	runtimeInfo.Description = m.config.UUID
}

func (m *serviceMgr) getNodeVersion(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetVersion)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if notAllowed, err := rbac.IsAllowed(r, rbac.EventingPermissionManage, false); err != nil {
		getAuthErrorInfo(runtimeInfo, notAllowed, false, err)
		return
	}

	runtimeInfo.SendRawDescription = true
	runtimeInfo.OnlyDescription = true
	runtimeInfo.Description = version.EventingVer()
}

func (m *serviceMgr) validateLocalAuth(r *http.Request) bool {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}

	pip := net.ParseIP(ip)
	if pip == nil || !pip.IsLoopback() {
		return false
	}

	rUsr, rKey, ok := r.BasicAuth()
	if !ok {
		return false
	}

	if rUsr != m.config.LocalUsername || rKey != m.config.LocalPassword {
		return false
	}

	return true
}

func (m *serviceMgr) writeDebuggerURLHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventWriteDebuggerUrl)
	runtimeInfo := &response.RuntimeInfo{}

	defer res.LogAndSend(runtimeInfo)

	if ok := m.validateLocalAuth(r); !ok {
		runtimeInfo.ErrCode = response.ErrUnauthenticated
		return
	}

	params := r.URL.Query()
	if !application.AppLocationInQuery(params) {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = "applocation should be provided"
		return
	}
	appLocation := application.GetApplocation(params)
	res.AddRequestData(common.AppLocationTag, appLocation)

	funcDetails, ok := m.appManager.GetApplication(appLocation, false)
	if !ok {
		runtimeInfo.ErrCode = response.ErrAppNotFound
		runtimeInfo.Description = fmt.Sprintf("%s not found", appLocation)
		return
	}

	url, err := io.ReadAll(r.Body)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}

	_, err = m.superSup.DebuggerOp(common.WriteDebuggerURL, funcDetails, string(url))
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		return
	}
	runtimeInfo.ExtraAttributes = params
	runtimeInfo.Description = "Successfully written debugger url"
}

func (m *serviceMgr) getKVNodesAddressesHandler(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetUserInfo)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	if ok := m.validateLocalAuth(r); !ok {
		runtimeInfo.ErrCode = response.ErrUnauthenticated
		return
	}

	kvNodeEvent := notifier.InterestedEvent{
		Event: notifier.EventKVTopologyChanges,
	}

	responseMap := make(map[string]interface{})
	nodesInterface, err := m.observer.GetCurrentState(kvNodeEvent)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		responseMap["is_error"] = true
		responseMap["error"] = "internal server error"
		runtimeInfo.Description = responseMap
		return
	}

	kvNodes := nodesInterface.([]*notifier.Node)

	m.serverConfigMux.RLock()
	encryptData := m.tlsConfig.EncryptData
	isClientAuthMandatory := m.tlsConfig.IsClientAuthMandatory
	m.serverConfigMux.RUnlock()

	port := notifier.DataService
	if encryptData {
		port = notifier.DataServiceSSL
	}
	kvNodeHost := make([]string, 0, len(kvNodes))
	for _, node := range kvNodes {
		kvNodeHost = append(kvNodeHost, fmt.Sprintf("%s:%d", node.HostName, node.Services[port]))
	}

	responseMap["is_error"] = false
	responseMap["kv_nodes"] = kvNodeHost
	responseMap["encrypt_data"] = encryptData
	responseMap["is_client_auth_mandatory"] = isClientAuthMandatory

	runtimeInfo.OnlyDescription = true
	runtimeInfo.Description = responseMap
}

func (m *serviceMgr) getCreds(w http.ResponseWriter, r *http.Request) {
	res := response.NewResponseWriter(w, r, response.EventGetUserInfo)
	runtimeInfo := &response.RuntimeInfo{}

	defer func() {
		res.LogAndSend(runtimeInfo)
	}()

	m.globalStatsCounter.LcbCredsStats.Add(1)
	if ok := m.validateLocalAuth(r); !ok {
		runtimeInfo.ErrCode = response.ErrUnauthenticated
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInvalidRequest
		runtimeInfo.Description = fmt.Sprintf("Failed to read request body, err: %v", err)
		return
	}

	endpoint := string(data)
	username, password, err := authenticator.GetMemcachedServiceAuth(string(data))
	if err != nil {
		runtimeInfo.ErrCode = response.ErrInternalServer
		runtimeInfo.Description = fmt.Sprintf("Failed to get credentials for endpoint: %s, err: %v", endpoint, err)
		return
	}

	response := url.Values{}
	response.Add("username", username)
	response.Add("password", password)

	runtimeInfo.OnlyDescription = true
	runtimeInfo.SendRawDescription = true
	runtimeInfo.Description = fmt.Sprintf("%s", response.Encode())
}
