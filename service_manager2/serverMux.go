package servicemanager2

import (
	"net/http"
)

func (m *serviceMgr) getServerMux() *http.ServeMux {
	mux := http.NewServeMux()

	//pprof REST APIs //DONE
	mux.HandleFunc("/debug/pprof/", m.indexHandler)
	mux.HandleFunc("/debug/pprof/cmdline", m.cmdlineHandler)
	mux.HandleFunc("/debug/pprof/profile", m.profileHandler)
	mux.HandleFunc("/debug/pprof/symbol", m.symbolHandler)
	mux.HandleFunc("/debug/pprof/trace", m.traceHandler)

	mux.HandleFunc("/die", m.die)
	mux.HandleFunc("/freeOSMemory", m.freeOSMemory)
	mux.HandleFunc("/triggerGC", m.triggerGC)
	mux.HandleFunc("/uuid", m.getNodeUUID)
	mux.HandleFunc("/version", m.getNodeVersion)
	mux.HandleFunc("/getKVNodesAndCerts", m.getKVNodesAndCertsHandler)
	mux.HandleFunc("/getUserInfo", m.getUserInfoHandler)
	mux.HandleFunc("/getCreds", m.getCreds)
	mux.HandleFunc("/getCpuCount", m.getCPUCount)

	//expvar REST APIs
	mux.HandleFunc("/debug/vars", m.expvarHandler) //DONE

	// Internal REST APIs
	mux.HandleFunc("/logFileLocation", m.logFileLocation)
	/*
		           mux.HandleFunc("/deleteApplication/", m.deletePrimaryStoreHandler)
		           mux.HandleFunc("/deleteAppTempStore/", m.deleteTempStoreHandler)
		           mux.HandleFunc("/getAggEventProcessingStats", m.getAggEventProcessingStats)
		           mux.HandleFunc("/getAggRebalanceStatus", m.getAggRebalanceStatus)
		           mux.HandleFunc("/getConsumerPids", m.getEventingConsumerPids)
		           mux.HandleFunc("/getDcpEventsRemaining", m.getDcpEventsRemaining)
		           mux.HandleFunc("/getEventProcessingStats", m.getEventProcessingStats)
		           mux.HandleFunc("/getExecutionStats", m.getExecutionStats)
		           mux.HandleFunc("/getFailureStats", m.getFailureStats)
		           mux.HandleFunc("/getLatencyStats", m.getLatencyStats)
		           mux.HandleFunc("/getLocallyDeployedApps", m.getLocallyDeployedApps)
		           mux.HandleFunc("/getRebalanceStatus", m.getRebalanceStatus)
		           mux.HandleFunc("/getRunningApps", m.getRunningApps)
		           mux.HandleFunc("/getSeqsProcessed", m.getSeqsProcessed)
		           mux.HandleFunc("/getLocalDebugUrl/", m.getLocalDebugURL)
		           mux.HandleFunc("/saveAppTempStore/", m.saveTempStoreHandler)
		           mux.HandleFunc("/setApplication/", m.savePrimaryStoreHandler)
		           mux.HandleFunc("/setSettings/", m.setSettingsHandler)
		           mux.HandleFunc("/startTracing", m.startTracing)
		           mux.HandleFunc("/stopTracing", m.stopTracing)
		           mux.HandleFunc("/redistributeworkload", m.triggerInternalRebalance)
				   mux.HandleFunc("/tenantOwnership", m.distributeTenantOwnership)
	*/

	mux.HandleFunc("/getAggBootstrappingApps", m.getAggBootstrapStatusHandler)
	mux.HandleFunc("/getAggBootstrapStatus", m.getAggBootstrapStatusHandler)
	mux.HandleFunc("/getAggBootstrapAppStatus", m.getAggBootstrapAppStatusHandler)
	mux.HandleFunc("/getAggRebalanceProgress", m.getAggRebalanceProgress)
	mux.HandleFunc("/getAggRebalanceStatus", m.getAggRebalanceStatus)
	mux.HandleFunc("/getRunningApps", m.getRunningApps)

	mux.HandleFunc("/getAnnotations", m.getAnnotations)
	mux.HandleFunc("/getApplication/", m.getApplicationHandler)
	mux.HandleFunc("/getAppTempStore/", m.getApplicationHandler)
	mux.HandleFunc("/getAppLog", m.getAppLog)
	mux.HandleFunc("/getInsight", m.getInsight)
	mux.HandleFunc("/getRebalanceProgress", m.getRebalanceProgress)
	mux.HandleFunc("/getOwnershipMap", m.getOwnershipMap)
	mux.HandleFunc("/getOwnedVbsForApp", m.getOwnedVbsForApp)

	mux.HandleFunc("/getBootstrappingApps", m.getBootstrappingAppsHandler)
	mux.HandleFunc("/getBootstrapStatus", m.getBootstrapStatusHandler)
	mux.HandleFunc("/getBootstrapAppStatus", m.getBootstrapAppStatusHandler)
	mux.HandleFunc("/getDeployedApps", m.getAggDeployedAppsHandler)
	mux.HandleFunc("/getLocallyDeployedApps", m.getLocallyDeployedAppsHandler)
	mux.HandleFunc("/getPausingApps", m.getPausingAppsHandler)
	mux.HandleFunc("/getErrorCodes", m.getErrCodes)
	mux.HandleFunc("/getStatus", m.getStatus)
	mux.HandleFunc("/getWorkerCount", m.getWorkerCount)

	// Public REST APIs
	mux.HandleFunc("/api/v1/status", m.statusHandler)
	mux.HandleFunc("/api/v1/status/", m.statusHandler)
	mux.HandleFunc("/api/v1/stats", m.statsHandler)
	mux.HandleFunc("/api/v1/stats/", m.statsHandler)
	mux.HandleFunc("/api/v1/config", m.configHandler)
	mux.HandleFunc("/api/v1/config/", m.configHandler)
	mux.HandleFunc("/api/v1/functions", m.functionsHandler)
	mux.HandleFunc("/api/v1/functions/", m.functionsHandler)
	mux.HandleFunc("/api/v1/export", m.exportHandler)
	mux.HandleFunc("/api/v1/export/", m.exportHandler)
	mux.HandleFunc("/api/v1/import", m.importHandler)
	mux.HandleFunc("/api/v1/import/", m.importHandler)
	mux.HandleFunc("/api/v1/backup", m.backupHandler)
	mux.HandleFunc("/resetStatsCounters", m.clearEventStats)
	mux.HandleFunc("/resetNodeStatsCounters", m.clearNodeEventStats)
	mux.HandleFunc("/cleanupEventing", m.cleanupEventing)
	mux.HandleFunc("/startDebugger/", m.startDebugger)
	mux.HandleFunc("/getDebuggerUrl/", m.getDebuugerUrl)
	mux.HandleFunc("/stopDebugger/", m.stopDebugger)
	mux.HandleFunc("/writeDebuggerURL", m.writeDebuggerURLHandler)

	mux.HandleFunc("/api/v1/list/functions", m.listFunctions)
	mux.HandleFunc("/api/v1/list/functions/", m.listFunctions)

	mux.HandleFunc("/_prometheusMetrics", m.prometheusLow)
	mux.HandleFunc("/_prometheusMetricsHigh", m.prometheusHigh)

	return mux
}
