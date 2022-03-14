package servicemanager

import (
	"fmt"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/rbac"
	"github.com/couchbase/eventing/service_manager/response"
	"github.com/couchbase/eventing/util"
)

type Stats interface {
	AppStats(cred cbauth.Creds, identity common.Identity, fullStats bool) (stats, *response.RuntimeInfo)
}

func (m *ServiceMgr) AppStats(cred cbauth.Creds, identity common.Identity, fullStats bool) (stats, *response.RuntimeInfo) {
	appLocation := identity.ToLocation()
	if !m.checkIfDeployed(appLocation) {
		runtimeInfo := response.RuntimeInfo{
			ErrCode:     response.ErrAppDeployed,
			Description: fmt.Sprintf("%s is not deployed", appLocation),
		}
		return stats{}, &runtimeInfo
	}

	runtimeInfo := m.checkPermissionFromCred(cred, appLocation, rbac.HandlerGetPermissions, false)
	if runtimeInfo.ErrCode != response.Ok {
		return stats{}, runtimeInfo
	}

	var stats stats
	feedBoundary, err := m.superSup.DcpFeedBoundary(appLocation)
	if err == nil {
		stats.DCPFeedBoundary = feedBoundary
	}
	app, runtimeInfo := m.getTempStore(appLocation)
	stats.EventProcessingStats = m.superSup.GetEventProcessingStats(appLocation)
	stats.EventsRemaining = backlogStat{DcpBacklog: m.superSup.GetDcpEventsRemainingToProcess(appLocation)}
	stats.ExecutionStats = m.superSup.GetExecutionStats(appLocation)
	stats.FailureStats = m.superSup.GetFailureStats(appLocation)
	stats.FunctionName = app.Name
	stats.FunctionScope = app.FunctionScope
	stats.GocbCredsRequestCounter = util.GocbCredsRequestCounter
	stats.FunctionID = app.FunctionID
	stats.InternalVbDistributionStats = m.superSup.InternalVbDistributionStats(appLocation)
	stats.LcbCredsRequestCounter = m.lcbCredsCounter
	stats.LcbExceptionStats = m.superSup.GetLcbExceptionsStats(appLocation)
	stats.MetastoreStats = m.superSup.GetMetaStoreStats(appLocation)
	stats.WorkerPids = m.superSup.GetEventingConsumerPids(appLocation)
	stats.PlannerStats = m.superSup.PlannerStats(appLocation)
	stats.VbDistributionStatsFromMetadata = m.superSup.VbDistributionStatsFromMetadata(appLocation)

	latencyStats := m.superSup.GetLatencyStats(appLocation)
	ls := make(map[string]int)
	ls["50"] = percentileN(latencyStats, 50)
	ls["80"] = percentileN(latencyStats, 80)
	ls["90"] = percentileN(latencyStats, 90)
	ls["95"] = percentileN(latencyStats, 95)
	ls["99"] = percentileN(latencyStats, 99)
	ls["100"] = percentileN(latencyStats, 100)
	stats.LatencyPercentileStats = ls

	m.rebalancerMutex.RLock()
	if m.rebalancer != nil {
		rebalanceStats := make(map[string]interface{})
		rebalanceStats["is_leader"] = true
		rebalanceStats["node_level_stats"] = m.rebalancer.NodeLevelStats
		rebalanceStats["rebalance_progress"] = m.rebalancer.RebalanceProgress
		rebalanceStats["rebalance_progress_counter"] = m.rebalancer.RebProgressCounter
		rebalanceStats["rebalance_start_ts"] = m.rebalancer.RebalanceStartTs
		rebalanceStats["total_vbs_to_shuffle"] = m.rebalancer.TotalVbsToShuffle
		rebalanceStats["vbs_remaining_to_shuffle"] = m.rebalancer.VbsRemainingToShuffle

		stats.RebalanceStats = rebalanceStats
	}
	m.rebalancerMutex.RUnlock()

	if fullStats {
		checkpointBlobDump, err := m.superSup.CheckpointBlobDump(appLocation)
		if err == nil {
			stats.CheckpointBlobDump = checkpointBlobDump
		}

		stats.LatencyStats = m.superSup.GetLatencyStats(appLocation)
		stats.CurlLatencyStats = m.superSup.GetCurlLatencyStats(appLocation)
		stats.SeqsProcessed = m.superSup.GetSeqsProcessed(appLocation)

		spanBlobDump, err := m.superSup.SpanBlobDump(appLocation)
		if err == nil {
			stats.SpanBlobDump = spanBlobDump
		}

		stats.VbDcpEventsRemaining = m.superSup.VbDcpEventsRemainingToProcess(appLocation)
		debugStats, err := m.superSup.TimerDebugStats(appLocation)
		if err == nil {
			stats.DocTimerDebugStats = debugStats
		}
		vbSeqnoStats, err := m.superSup.VbSeqnoStats(appLocation)
		if err == nil {
			stats.VbSeqnoStats = vbSeqnoStats
		}
	}

	return stats, &response.RuntimeInfo{ErrCode: response.Ok}
}
