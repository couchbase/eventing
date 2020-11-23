package servicemanager

import (
	"context"
	"crypto/tls"
	"encoding/json"
	_ "expvar" // For stat collection
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // For debugging
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/parser"
	"github.com/couchbase/eventing/util"
)

func NewState() state {
	return state{
		rev:           0,
		servers:       make([]service.NodeID, 0),
		isBalanced:    true, // Default value of isBalanced should be true we replan vb distribution for all functions on process startup
		rebalanceID:   "",
		rebalanceTask: nil,
	}
}

//NewServiceMgr creates handle for ServiceMgr, which implements cbauth service.Manager
func NewServiceMgr(config util.Config, rebalanceRunning bool, superSup common.EventingSuperSup) *ServiceMgr {

	logging.Infof("ServiceMgr::newServiceMgr config: %rm rebalanceRunning: %v", fmt.Sprintf("%#v", config), rebalanceRunning)

	mu := &sync.RWMutex{}

	mgr := &ServiceMgr{
		consistencyValues: []string{"none", "request"},
		graph:             newBucketMultiDiGraph(),
		fnsInPrimaryStore: make(map[string]depCfg),
		fnsInTempStore:    make(map[string]struct{}),
		bucketFunctionMap: make(map[common.Keyspace]map[string]functionInfo),
		fnMu:              &sync.RWMutex{},
		failoverMu:        &sync.RWMutex{},
		mu:                mu,
		servers:           make([]service.NodeID, 0),
		state:             NewState(),
		statsWritten:      true,
		stopTracerCh:      make(chan struct{}, 1),
		superSup:          superSup,
		finch:             make(chan bool),
	}

	mgr.config.Store(config)
	mgr.nodeInfo = &service.NodeInfo{
		NodeID: service.NodeID(config["uuid"].(string)),
	}

	mgr.rebalanceRunning = rebalanceRunning
	mgr.servers = append(mgr.servers, mgr.nodeInfo.NodeID)
	mgr.waiters = make(waiters)

	mgr.initService()
	return mgr
}

func (m *ServiceMgr) initService() {
	logPrefix := "ServiceMgr::initService"

	cfg := m.config.Load()
	m.adminHTTPPort = cfg["eventing_admin_http_port"].(string)
	m.adminSSLPort = cfg["eventing_admin_ssl_port"].(string)
	m.certFile = cfg["eventing_admin_ssl_cert"].(string)
	m.keyFile = cfg["eventing_admin_ssl_key"].(string)
	m.restPort = cfg["rest_port"].(string)
	m.uuid = cfg["uuid"].(string)
	m.initErrCodes()

	logging.Infof("%s adminHTTPPort: %s adminSSLPort: %s", logPrefix, m.adminHTTPPort, m.adminSSLPort)
	logging.Infof("%s certFile: %s keyFile: %s", logPrefix, m.certFile, m.keyFile)

	util.Retry(util.NewFixedBackoff(time.Second), nil, getHTTPServiceAuth, m)

	go func(m *ServiceMgr) {
		for {
			err := m.registerWithServer()
			if err != nil {
				logging.Infof("%s Retrying to register against cbauth_service", logPrefix)
				time.Sleep(2 * time.Second)
			} else {
				break
			}
		}
	}(m)

	m.disableDebugger()

	mux := http.NewServeMux()

	//pprof REST APIs
	mux.HandleFunc("/debug/pprof/", m.indexHandler)
	mux.HandleFunc("/debug/pprof/cmdline", m.cmdlineHandler)
	mux.HandleFunc("/debug/pprof/profile", m.profileHandler)
	mux.HandleFunc("/debug/pprof/symbol", m.symbolHandler)
	mux.HandleFunc("/debug/pprof/trace", m.traceHandler)

	//expvar REST APIs
	mux.HandleFunc("/debug/vars", m.expvarHandler)

	// Internal REST APIs
	mux.HandleFunc("/cleanupEventing", m.cleanupEventing)
	mux.HandleFunc("/clearEventStats", m.clearEventStats)
	mux.HandleFunc("/die", m.die)
	mux.HandleFunc("/deleteApplication/", m.deletePrimaryStoreHandler)
	mux.HandleFunc("/deleteAppTempStore/", m.deleteTempStoreHandler)
	mux.HandleFunc("/freeOSMemory", m.freeOSMemory)
	mux.HandleFunc("/getAggBootstrappingApps", m.getAggBootstrappingApps)
	mux.HandleFunc("/getAggBootstrapStatus", m.getAggBootstrapStatus)
	mux.HandleFunc("/getAggBootstrapAppStatus", m.getAggBootstrapAppStatus)
	mux.HandleFunc("/getAggEventProcessingStats", m.getAggEventProcessingStats)
	mux.HandleFunc("/getAggRebalanceProgress", m.getAggRebalanceProgress)
	mux.HandleFunc("/getAggRebalanceStatus", m.getAggRebalanceStatus)
	mux.HandleFunc("/getApplication/", m.getPrimaryStoreHandler)
	mux.HandleFunc("/getAnnotations", m.getAnnotations)
	mux.HandleFunc("/getAppTempStore/", m.getTempStoreHandler)
	mux.HandleFunc("/getBootstrappingApps", m.getBootstrappingApps)
	mux.HandleFunc("/getBootstrapStatus", m.getBootstrapStatus)
	mux.HandleFunc("/getBootstrapAppStatus", m.getBootstrapAppStatus)
	mux.HandleFunc("/getPausingApps", m.getPausingApps)
	mux.HandleFunc("/getConsumerPids", m.getEventingConsumerPids)
	mux.HandleFunc("/getCpuCount", m.getCPUCount)
	mux.HandleFunc("/getCreds", m.getCreds)
	mux.HandleFunc("/getDcpEventsRemaining", m.getDcpEventsRemaining)
	mux.HandleFunc("/getDebuggerUrl/", m.getDebuggerURL)
	mux.HandleFunc("/getDeployedApps", m.getDeployedApps)
	mux.HandleFunc("/getErrorCodes", m.getErrCodes)
	mux.HandleFunc("/getEventProcessingStats", m.getEventProcessingStats)
	mux.HandleFunc("/getExecutionStats", m.getExecutionStats)
	mux.HandleFunc("/getFailureStats", m.getFailureStats)
	mux.HandleFunc("/getLatencyStats", m.getLatencyStats)
	mux.HandleFunc("/getLocallyDeployedApps", m.getLocallyDeployedApps)
	mux.HandleFunc("/getAppLog", m.getAppLog)
	mux.HandleFunc("/getRebalanceProgress", m.getRebalanceProgress)
	mux.HandleFunc("/getRebalanceStatus", m.getRebalanceStatus)
	mux.HandleFunc("/getRunningApps", m.getRunningApps)
	mux.HandleFunc("/getSeqsProcessed", m.getSeqsProcessed)
	mux.HandleFunc("/getLocalDebugUrl/", m.getLocalDebugURL)
	mux.HandleFunc("/getWorkerCount", m.getWorkerCount)
	mux.HandleFunc("/getInsight", m.getInsight)
	mux.HandleFunc("/logFileLocation", m.logFileLocation)
	mux.HandleFunc("/saveAppTempStore/", m.saveTempStoreHandler)
	mux.HandleFunc("/setApplication/", m.savePrimaryStoreHandler)
	mux.HandleFunc("/setSettings/", m.setSettingsHandler)
	mux.HandleFunc("/startDebugger/", m.startDebugger)
	mux.HandleFunc("/startTracing", m.startTracing)
	mux.HandleFunc("/triggerGC", m.triggerGC)
	mux.HandleFunc("/stopDebugger/", m.stopDebugger)
	mux.HandleFunc("/stopTracing", m.stopTracing)
	mux.HandleFunc("/uuid", m.getNodeUUID)
	mux.HandleFunc("/version", m.getNodeVersion)
	mux.HandleFunc("/writeDebuggerURL/", m.writeDebuggerURLHandler)
	mux.HandleFunc("/getKVNodesAddresses", m.getKVNodesAddresses)
	mux.HandleFunc("/redistributeworkload", m.triggerInternalRebalance)

	// Public REST APIs
	mux.HandleFunc("/api/v1/status", m.statusHandler)
	mux.HandleFunc("/api/v1/status/", m.statusHandler)
	mux.HandleFunc("/api/v1/stats", m.statsHandler)
	mux.HandleFunc("/api/v1/config", m.configHandler)
	mux.HandleFunc("/api/v1/config/", m.configHandler)
	mux.HandleFunc("/api/v1/functions", m.functionsHandler)
	mux.HandleFunc("/api/v1/functions/", m.functionsHandler)
	mux.HandleFunc("/api/v1/export", m.exportHandler)
	mux.HandleFunc("/api/v1/export/", m.exportHandler)
	mux.HandleFunc("/api/v1/import", m.importHandler)
	mux.HandleFunc("/api/v1/import/", m.importHandler)
	mux.HandleFunc("/api/v1/backup", m.backupHandler)

	mux.HandleFunc("/api/v1/list/functions", m.listFunctions)
	mux.HandleFunc("/api/v1/list/functions/", m.listFunctions)

	mux.HandleFunc("/_prometheusMetrics", m.prometheusLow)
	mux.HandleFunc("/_prometheusMetricsHigh", m.prometheusHigh)

	go func() {
		addr := net.JoinHostPort("", m.adminHTTPPort)

		srv := &http.Server{
			Addr:         addr,
			ReadTimeout:  httpReadTimeOut,
			WriteTimeout: httpWriteTimeOut,
			Handler:      mux,
			ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
				return context.WithValue(ctx, "conn", conn)
			},
		}
		proto := util.GetNetworkProtocol()
		listner, err := net.Listen(proto, addr)
		if err != nil {
			logging.Errorf("Failed to start http service ip family: %v address: %v error: %v", proto, addr, err)
			time.Sleep(1 * time.Second)
			os.Exit(1)
		}

		logging.Infof("%s Admin HTTP server started: %s", logPrefix, addr)
		srv.Serve(listner)
		logging.Fatalf("%s Error in Admin HTTP Server: %v", logPrefix, err)
	}()

	if m.adminSSLPort != "" {
		var reload bool = false
		var sslsrv *http.Server = nil
		sslAddr := net.JoinHostPort("", m.adminSSLPort)

		refresh := func() error {
			if sslsrv != nil {
				reload = true
				sslsrv.Shutdown(context.Background())
			}
			return nil
		}

		go func() {
			for {
				err := cbauth.RegisterTLSRefreshCallback(refresh)
				if err == nil {
					break
				}
				logging.Errorf("%s Unable to register for cert refresh, will retry: %v", logPrefix, err)
				time.Sleep(10 * time.Second)
			}
			for {
				tlscfg, err := m.getTLSConfig(logPrefix)
				if err != nil {
					logging.Errorf("%s Error configuring TLS: %v", logPrefix, err)
					return
				}
				sslsrv = &http.Server{
					Addr:         sslAddr,
					ReadTimeout:  httpReadTimeOut,
					WriteTimeout: httpWriteTimeOut,
					TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0),
					TLSConfig:    tlscfg,
					Handler:      mux,
					ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
						return context.WithValue(ctx, "conn", conn)
					},
				}

				proto := util.GetNetworkProtocol()
				ln, err := net.Listen(proto, sslAddr)
				if err != nil {
					logging.Errorf("Failed to start ssl service ip family: %v address: %v error: %v", proto, sslAddr, err)
					time.Sleep(1 * time.Second)
					os.Exit(1)
				}

				logging.Infof("%s SSL server started: %v", logPrefix, sslsrv)
				tls_ln := tls.NewListener(ln, tlscfg)
				sslsrv.Serve(tls_ln)
			}
		}()
	}

	go func(m *ServiceMgr) {
		cancelCh := make(chan struct{})
		for {
			err := metakv.RunObserveChildren(metakvChecksumPath, m.primaryStoreCsumPathCallback, cancelCh)
			if err != nil {
				logging.Errorf("%s metakv observe error for primary store, err: %v. Retrying...", logPrefix, err)
				time.Sleep(2 * time.Second)
			}
		}
	}(m)

	go func(m *ServiceMgr) {
		cancelCh := make(chan struct{})
		for {
			err := metakv.RunObserveChildren(metakvTempAppsPath, m.tempStoreAppsPathCallback, cancelCh)
			if err != nil {
				logging.Errorf("%s metakv observe error for temp store, err: %v. Retrying...", logPrefix, err)
				time.Sleep(2 * time.Second)
			}
		}
	}(m)

	go func(m *ServiceMgr) {
		cancelCh := make(chan struct{})
		for {
			err := metakv.RunObserveChildren(metakvAppSettingsPath, m.settingChangeCallback, cancelCh)
			if err != nil {
				logging.Errorf("%s metakv observe error for setting store, err: %v. Retrying...", logPrefix, err)
				time.Sleep(2 * time.Second)
			}
		}
	}(m)
	go m.watchFailoverEvents()
}

func (m *ServiceMgr) primaryStoreCsumPathCallback(path string, value []byte, rev interface{}) error {
	logPrefix := "ServiceMgr::primaryStoreCsumPathCallback"

	logging.Infof("%s path: %s encoded value size: %d", logPrefix, path, len(value))

	splitRes := strings.Split(path, "/")
	if len(splitRes) != 4 {
		return nil
	}

	fnName := splitRes[len(splitRes)-1]
	m.fnMu.Lock()
	defer m.fnMu.Unlock()

	if len(value) > 0 {
		//Read application from metakv
		data, err := util.ReadAppContent(metakvAppsPath, metakvChecksumPath, fnName)
		if err != nil {
			logging.Errorf("%s Reading function: %s from metakv failed, err: %v", logPrefix, fnName, err)
			return nil
		}
		app := m.parseFunctionPayload(data, fnName)
		m.fnsInPrimaryStore[fnName] = app.DeploymentConfig
		logging.Infof("%s Added function: %s to fnsInPrimaryStore", logPrefix, fnName)

		if val, ok := app.Settings["processing_status"].(bool); ok && val {
			source, destinations := m.getSourceAndDestinationsFromDepCfg(&app.DeploymentConfig)

			//Find keyspace names from N1QL statements in handler code and add edges
			_, pinfos := parser.TranspileQueries(app.AppHandlers, "")
			for _, pinfo := range pinfos {
				if pinfo.PInfo.KeyspaceName != "" {
					dest := ConstructKeyspace(pinfo.PInfo.KeyspaceName)
					logging.Infof("%s Adding allowed edge label %s, source %s to destination %s",
						logPrefix, fnName, source, pinfo.PInfo.KeyspaceName)
					destinations[dest] = struct{}{}
				}
			}

			logging.Infof("%s inserting edges into graph for function: %v, source: %v destinations: %v", logPrefix, fnName, source, destinations)

			if len(destinations) > 0 {
				m.graph.insertEdges(fnName, source, destinations)
			}
		}

		//Update BucketFunctionMap
		source := common.Keyspace{BucketName: app.DeploymentConfig.SourceBucket,
			ScopeName:      app.DeploymentConfig.SourceScope,
			CollectionName: app.DeploymentConfig.SourceCollection,
		}
		functions, ok := m.bucketFunctionMap[source]
		if !ok {
			functions = make(map[string]functionInfo)
			m.bucketFunctionMap[source] = functions
		}
		funtionType := "notsbm"
		if m.isSrcMutationEnabled(&app.DeploymentConfig) {
			funtionType = "sbm"
		}

		deployed := app.Settings["deployment_status"].(bool)
		functions[app.Name] = functionInfo{fnName: app.Name, fnType: funtionType, fnDeployed: deployed}
	} else {
		cfg := m.fnsInPrimaryStore[fnName]
		m.graph.removeEdges(fnName)
		delete(m.fnsInPrimaryStore, fnName)
		source := common.Keyspace{BucketName: cfg.SourceBucket,
			ScopeName:      cfg.SourceScope,
			CollectionName: cfg.SourceCollection,
		}

		delete(m.bucketFunctionMap[source], fnName)
		if len(m.bucketFunctionMap[source]) == 0 {
			delete(m.bucketFunctionMap, source)
		}
		logging.Infof("%s Deleted function: %s from fnsInPrimaryStore", logPrefix, fnName)
	}

	return nil
}

func (m *ServiceMgr) tempStoreAppsPathCallback(path string, value []byte, rev interface{}) error {
	logPrefix := "ServiceMgr::tempStoreAppsPathCallback"

	logging.Infof("%s path: %s encoded value size: %d", logPrefix, path, len(value))

	splitRes := strings.Split(path, "/")
	if len(splitRes) != 5 {
		return nil
	}

	fnName := splitRes[len(splitRes)-2]
	m.fnMu.Lock()
	defer m.fnMu.Unlock()

	if len(value) > 0 {
		m.fnsInTempStore[fnName] = struct{}{}
		logging.Infof("%s Added function: %s to fnsInTempStore", logPrefix, fnName)
	} else {
		delete(m.fnsInTempStore, fnName)
		logging.Infof("%s Deleted function: %s from fnsInTempStore", logPrefix, fnName)
	}

	return nil
}

func (m *ServiceMgr) settingChangeCallback(path string, value []byte, rev interface{}) error {
	logPrefix := "ServiceMgr::settingChangeCallback"

	logging.Infof("%s path: %s encoded value size: %d", logPrefix, path, len(value))

	pathTokens := strings.Split(path, "/")
	if len(pathTokens) != 4 {
		logging.Errorf("%s Invalid setting path, path: %s", logPrefix, path)
		return nil
	}

	m.fnMu.Lock()
	defer m.fnMu.Unlock()
	functionName := pathTokens[len(pathTokens)-1]
	cfg, ok := m.fnsInPrimaryStore[functionName]

	if !ok {
		return nil
	}

	if value == nil {
		source := common.Keyspace{BucketName: cfg.SourceBucket,
			ScopeName:      cfg.SourceScope,
			CollectionName: cfg.SourceCollection,
		}
		delete(m.bucketFunctionMap[source], functionName)
		if len(m.bucketFunctionMap[source]) == 0 {
			delete(m.bucketFunctionMap, source)
		}

		return nil
	}

	settings := make(map[string]interface{})
	err := json.Unmarshal(value, &settings)
	if err != nil {
		logging.Errorf("%s [%s] Failed to unmarshal settings received from metakv, err: %v",
			logPrefix, functionName, err)
		return nil
	}

	deploymentStatus, ok := settings["deployment_status"].(bool)
	if !ok {
		logging.Errorf("%s [%s] Failed to convert deployment status to boolean from setting",
			logPrefix, functionName)
		return nil
	}

	processingStatus, ok := settings["processing_status"].(bool)
	if !ok {
		logging.Errorf("%s [%s] Failed to convert processing status to boolean from setting",
			logPrefix, functionName)
		return nil
	}

	logging.Infof("%s deploymentStatus: %v, processingStatus: %v", logPrefix, deploymentStatus, processingStatus)
	source, _ := m.getSourceAndDestinationsFromDepCfg(&cfg)
	if processingStatus == false {
		m.graph.removeEdges(functionName)
	} else if deploymentStatus == true && processingStatus == true {
		logging.Infof("%s calling UpdateBucketGraphFromMetakv", logPrefix)
		m.UpdateBucketGraphFromMetakv(functionName)
	}

	//Update BucketFunctionMap
	functions, ok := m.bucketFunctionMap[source]
	if !ok {
		functions = make(map[string]functionInfo)
		m.bucketFunctionMap[source] = functions
	}
	funtionType := "notsbm"
	if m.isSrcMutationEnabled(&cfg) {
		funtionType = "sbm"
	}

	functions[functionName] = functionInfo{fnName: functionName, fnType: funtionType, fnDeployed: deploymentStatus}

	return nil
}

func (m *ServiceMgr) disableDebugger() {
	logPrefix := "ServiceMgr::enableDebugger"

	config, info := m.getConfig()
	if info.Code != m.statusCodes.ok.Code {
		return
	}

	if _, exists := config["enable_debugger"]; exists {
		logging.Tracef("%s enable_debugger field exists , not making any change", logPrefix)
		return
	}

	logging.Tracef("%s enable_debugger field does not exist, enabling it", logPrefix)

	config["enable_debugger"] = false
	if info := m.saveConfig(config); info.Code != m.statusCodes.ok.Code {
		logging.Errorf("Unable to enable debugger by default, err: %v", info.Info)
	}
}

func (m *ServiceMgr) registerWithServer() error {
	cfg := m.config.Load()
	logging.Infof("Registering against cbauth_service, uuid: %v", cfg["uuid"].(string))

	err := service.RegisterManager(m, nil)
	if err != nil {
		logging.Errorf("Failed to register against cbauth_service, err: %v", err)
		return err
	}

	return nil
}

func (m *ServiceMgr) prepareRebalance(change service.TopologyChange) error {

	if isSingleNodeRebal(change) {
		if change.KeepNodes[0].NodeInfo.NodeID == m.nodeInfo.NodeID {
			logging.Infof("ServiceMgr::prepareRebalance - only node in the cluster")
		} else {
			return fmt.Errorf("node receiving prepare request isn't part of the cluster")
		}
	}

	return nil
}

func (m *ServiceMgr) startRebalance(change service.TopologyChange) error {
	logPrefix := "ServiceMgr::startRebalance"

	m.rebalanceCtx = &rebalanceContext{
		change: change,
		rev:    0,
	}

	logging.Infof("%s Garbage collecting old rebalance tokens", logPrefix)
	// Garbage collect old Rebalance Tokens
	util.Retry(util.NewFixedBackoff(time.Second), nil, cleanupEventingMetaKvPath, metakvRebalanceTokenPath)
	logging.Infof("%s Writing rebalance token: %s to metakv", logPrefix, change.ID)
	path := metakvRebalanceTokenPath + change.ID
	util.Retry(util.NewFixedBackoff(time.Second), nil, metaKVSetCallback, path, change.ID)

	m.updateRebalanceProgressLocked(0.0)

	return nil
}

func (m *ServiceMgr) updateRebalanceProgressLocked(progress float64) {
	changeID := m.rebalanceCtx.change.ID
	rev := m.rebalanceCtx.incRev()

	task := &service.Task{
		Rev:          encodeRev(rev),
		ID:           fmt.Sprintf("%s", changeID),
		Type:         service.TaskTypeRebalance,
		Status:       service.TaskStatusRunning,
		IsCancelable: true,
		Progress:     progress,

		Extra: map[string]interface{}{
			"rebalanceID": changeID,
		},
	}

	m.updateStateLocked(func(s *state) {
		s.rebalanceTask = task
	})
}

func (ctx *rebalanceContext) incRev() uint64 {
	curr := ctx.rev
	ctx.rev++

	return curr
}

func (m *ServiceMgr) wait(rev service.Revision, cancel service.Cancel) (state, error) {
	m.mu.Lock()
	unlock := newCleanup(func() {
		m.mu.Unlock()
	})
	defer unlock.run()

	currState := m.copyStateLocked()

	if rev == nil {
		return currState, nil
	}

	haveRev := decodeRev(rev)
	if haveRev != m.rev {
		return currState, nil
	}

	ch := m.addWaiterLocked()
	unlock.run()

	select {
	case <-cancel:
		return state{}, service.ErrCanceled
	case newState := <-ch:
		return newState, nil
	}
}

func stateToTaskList(s state) *service.TaskList {
	tasks := &service.TaskList{}

	tasks.Rev = encodeRev(s.rev)
	tasks.Tasks = make([]service.Task, 0)

	if s.rebalanceTask != nil {
		tasks.Tasks = append(tasks.Tasks, *s.rebalanceTask)
	}

	return tasks
}

func (m *ServiceMgr) stateToTopology(s state) *service.Topology {
	topology := &service.Topology{}

	topology.Rev = encodeRev(s.rev)
	topology.Nodes = append([]service.NodeID(nil), m.servers...)
	topology.IsBalanced = m.isBalanced
	topology.Messages = nil

	return topology
}

func (m *ServiceMgr) addWaiterLocked() waiter {
	ch := make(waiter, 1)
	m.waiters[ch] = struct{}{}

	return ch
}

func (m *ServiceMgr) removeWaiter(w waiter) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.waiters, w)
}

func (m *ServiceMgr) copyStateLocked() state {
	return m.state
}

func (m *ServiceMgr) cancelActualTaskLocked(task *service.Task) error {
	switch task.Type {
	case service.TaskTypeRebalance:
		return m.cancelRebalanceTaskLocked(task)
	default:
		panic("can't happen")
	}
}

func (m *ServiceMgr) cancelRebalanceTaskLocked(task *service.Task) error {
	switch task.Status {
	case service.TaskStatusRunning:
		return m.cancelRunningRebalanceTaskLocked(task)
	case service.TaskStatusFailed:
		return m.cancelFailedRebalanceTaskLocked()
	default:
		panic("can't happen")
	}
}

func (m *ServiceMgr) cancelRunningRebalanceTaskLocked(task *service.Task) error {
	logPrefix := "ServiceMgr::cancelRunningRebalanceTaskLocked"

	m.rebalancer.cancel()
	m.onRebalanceDoneLocked(nil, true)

	util.Retry(util.NewFixedBackoff(time.Second), nil, stopRebalanceCallback, m.rebalancer, task.ID)

	logging.Infof("%s Updated rebalance token: %s in metakv as part of stopping ongoing rebalance", logPrefix, task.ID)

	return nil
}

func (m *ServiceMgr) cancelFailedRebalanceTaskLocked() error {
	m.updateStateLocked(func(s *state) {
		s.rebalanceTask = nil
	})

	return nil
}

func isSingleNodeRebal(change service.TopologyChange) bool {
	if len(change.KeepNodes) == 1 && len(change.EjectNodes) == 0 {
		return true
	}
	return false
}

func (m *ServiceMgr) updateState(body func(state *state)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.updateStateLocked(body)
}

func (m *ServiceMgr) updateStateLocked(body func(state *state)) {
	body(&m.state)
	m.state.rev++

	m.notifyWaitersLocked()
}

func (m *ServiceMgr) notifyWaitersLocked() {
	s := m.copyStateLocked()
	for ch := range m.waiters {
		if ch != nil {
			ch <- s
		}
	}

	m.waiters = make(waiters)
}

func (m *ServiceMgr) runRebalanceCallback(cancel <-chan struct{}, body func()) {

	done := make(chan struct{})

	go func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		select {
		case <-cancel:
			break
		default:
			body()
		}

		close(done)
	}()

	select {
	case <-done:
	case <-cancel:
	}
}

func (m *ServiceMgr) rebalanceProgressCallback(progress float64, cancel <-chan struct{}) {
	m.runRebalanceCallback(cancel, func() {
		m.updateRebalanceProgressLocked(progress)
	})
}

func (m *ServiceMgr) rebalanceDoneCallback(err error, cancel <-chan struct{}) {
	m.runRebalanceCallback(cancel, func() {
		m.onRebalanceDoneLocked(err, false)
	})
}

func (m *ServiceMgr) onRebalanceDoneLocked(err error, cancelRebalance bool) {
	logPrefix := "ServiceMgr::onRebalanceDoneLocked"
	newTask := (*service.Task)(nil)
	if err != nil {
		ctx := m.rebalanceCtx
		rev := ctx.incRev()

		newTask = &service.Task{
			Rev:          encodeRev(rev),
			ID:           fmt.Sprintf("rebalance/%s", ctx.change.ID),
			Type:         service.TaskTypeRebalance,
			Status:       service.TaskStatusFailed,
			IsCancelable: true,

			ErrorMessage: err.Error(),

			Extra: map[string]interface{}{
				"rebalanceId": ctx.change.ID,
			},
		}
		m.isBalanced = false
	} else if cancelRebalance == true {
		m.isBalanced = false
	} else {
		m.isBalanced = true
	}
	logging.Infof("%s updated isBalanced: %v", logPrefix, m.isBalanced)

	m.rebalancer = nil
	m.rebalanceCtx = nil

	m.updateStateLocked(func(s *state) {
		s.rebalanceTask = newTask
		s.rebalanceID = ""
	})
}

func (m *ServiceMgr) getActiveNodeAddrs() ([]string, error) {
	logPrefix := "ServiceMgr::getActiveNodeAddrs"

	util.Retry(util.NewFixedBackoff(time.Second), nil, getEventingNodesAddressesOpCallback, m, true)

	nodeAddrs := make([]string, 0)

	// m.eventingNodeAddrs would capture all online Eventing nodes in the cluster, so it might include
	// Eventing nodes that aren't actually part of cluster yet but were requested as part of KeepNodes
	// from ns_server in PrepareTopologyChange RPC call. So filtering out only the previously existing
	// Eventing nodes to check if any app is undergoing bootstrap is needed.
	addrUUIDMap, err := util.GetNodeUUIDs("/uuid", m.eventingNodeAddrs)
	if err != nil {
		logging.Errorf("%s Failed to get eventing node uuids, err: %v", logPrefix, err)
		return nodeAddrs, err
	}

	var data []byte
	util.Retry(util.NewFixedBackoff(time.Second), nil, metakvGetCallback, metakvConfigKeepNodes, &data)

	if len(data) == 0 {
		return nodeAddrs, nil
	}

	var keepNodes []string
	err = json.Unmarshal(data, &keepNodes)
	if err != nil {
		logging.Warnf("%s Failed to unmarshal keepNodes received from metakv, err: %v", logPrefix, err)
		return nodeAddrs, err
	}

	for _, uuid := range keepNodes {
		if nodeAddr, ok := addrUUIDMap[uuid]; ok {
			nodeAddrs = append(nodeAddrs, nodeAddr)
		}
	}

	logging.Debugf("%s keepNodes from metakv: %v addrUUIDMap: %rm nodeAddrs: %rs",
		logPrefix, keepNodes, addrUUIDMap, nodeAddrs)

	return nodeAddrs, nil
}

func (m *ServiceMgr) compareEventingVersion(need eventingVer) bool {
	logPrefix := "ServiceMgr::compareEventingVersion"

	nodes, err := m.getActiveNodeAddrs()
	if err != nil {
		logging.Errorf("%s failed to get active eventing nodes, err: %v", logPrefix, err)
		return false
	}

	versions, err := util.GetEventingVersion("/version", nodes)
	if err != nil {
		logging.Errorf("%s failed to gather eventing version, err: %v", logPrefix, err)
		return false
	}

	for _, ver := range versions {
		eVer, err := frameEventingVersion(ver)
		if err != nil {
			return false
		}

		if !eVer.compare(need) {
			logging.Infof("%s eventing version for all nodes: %+v need version: %+v", logPrefix, versions, need)
			return false
		}
	}

	return true
}

func (e eventingVer) compare(need eventingVer) bool {
	return (e.major > need.major ||
		e.major == need.major && e.minor > need.minor ||
		e.major == need.major && e.minor == need.minor && e.mpVersion >= need.mpVersion) &&
		(e.isEnterprise == need.isEnterprise)
}

func frameEventingVersion(ver string) (eventingVer, error) {
	var eVer eventingVer

	segs := strings.Split(ver, "-")
	if len(segs) < 4 {
		return eVer, errInvalidVersion
	}

	verSegs := strings.Split(segs[1], ".")
	if len(verSegs) != 3 {
		return eVer, errInvalidVersion
	}

	val, err := strconv.Atoi(verSegs[0])
	if err != nil {
		return eVer, errInvalidVersion
	}
	eVer.major = val

	val, err = strconv.Atoi(verSegs[1])
	if err != nil {
		return eVer, errInvalidVersion
	}
	eVer.minor = val

	val, err = strconv.Atoi(verSegs[2])
	if err != nil {
		return eVer, errInvalidVersion
	}
	eVer.mpVersion = val

	val, err = strconv.Atoi(segs[2])
	if err != nil {
		return eVer, errInvalidVersion
	}
	eVer.build = val

	if segs[len(segs)-1] == "ee" {
		eVer.isEnterprise = true
	}

	return eVer, nil
}
