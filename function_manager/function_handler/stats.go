package functionHandler

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/common"
	vbhandler "github.com/couchbase/eventing/function_manager/function_handler/vb_handler"
	"github.com/couchbase/eventing/logging"
	processManager "github.com/couchbase/eventing/process_manager"
)

const (
	printStatsInterval = time.Minute
)

type statsHandler struct {
	sync.RWMutex

	logPrefix      string
	location       application.AppLocation
	stats          *common.Stats
	discardedStats *common.Stats
}

func newStatsHandler(logPrefix string, appLocation application.AppLocation) *statsHandler {
	stats := common.NewStats(true, appLocation.Namespace, appLocation.Appname, common.FullStats)
	discardedStats := common.NewStats(true, appLocation.Namespace, appLocation.Appname, common.PartialStats)
	return &statsHandler{
		logPrefix:      logPrefix,
		location:       appLocation,
		stats:          stats,
		discardedStats: discardedStats,
	}
}

func (sh *statsHandler) getInsight() *common.Insight {
	copyInsight := common.NewInsight()

	sh.RLock()
	defer sh.RUnlock()

	copyInsight.Accumulate(sh.stats.Insight)
	return copyInsight
}

func (sh *statsHandler) getStats(statType common.StatsType) *common.Stats {
	sh.RLock()
	defer sh.RUnlock()

	stats := sh.stats.Sub(sh.discardedStats, statType)
	return stats
}

func (sh *statsHandler) resetStats() {
	logPrefix := fmt.Sprintf("statsHandler::resetStats[%s]", sh.logPrefix)
	sh.Lock()
	defer sh.Unlock()

	sh.discardedStats = sh.stats.Copy(common.PartialStats)
	logging.Infof("%s successfully reset the stats: %s", logPrefix, sh.discardedStats)
}

func (stats *statsHandler) IncrementCountProcessingStats(statName string, count uint64) {
	stats.Lock()
	defer stats.Unlock()

	stats.incrementCountProcessingStatsLocked(statName, count)
}

func (stats *statsHandler) AddExecutionStats(statName string, count interface{}) {
	stats.Lock()
	defer stats.Unlock()

	stats.stats.ExecutionStats[statName] = count
}

func (stats *statsHandler) incrementCountProcessingStatsLocked(statName string, count uint64) {
	stats.stats.EventProcessingStats[statName] += count
}

func (stats *statsHandler) processedSeqEvents(msg *processManager.ResponseMessage) map[uint16][]uint64 {
	extras := msg.Value
	processedSeq := make(map[uint16]uint64)
	copied := make(map[uint16][]uint64)

	for len(extras) > 0 {
		vb := binary.BigEndian.Uint16(extras)
		seq := binary.BigEndian.Uint64(extras[2:])
		vbuuid := binary.BigEndian.Uint64(extras[10:])
		processedSeq[vb] = seq
		copied[vb] = []uint64{seq, vbuuid}
		extras = extras[18:]
	}

	stats.Lock()
	stats.stats.ProcessedSeq = processedSeq
	stats.Unlock()

	return copied
}

func (stats *statsHandler) handleStats(msg *processManager.ResponseMessage) {
	switch msg.Opcode {
	case processManager.FailureStats:
		fStats := make(map[string]interface{})
		err := json.Unmarshal(msg.Value, &fStats)
		if err != nil {
			return
		}

		stats.Lock()
		stats.stats.FailureStats = fStats
		stats.incrementCountProcessingStatsLocked("failure_stats", 1)
		stats.Unlock()

	case processManager.ExecutionStats:
		eStats := make(map[string]interface{})
		err := json.Unmarshal(msg.Value, &eStats)
		if err != nil {
			return
		}

		stats.Lock()
		stats.stats.ExecutionStats = eStats
		stats.stats.EventProcessingStats["timer_responses_received"] = uint64(eStats["timer_create_counter"].(float64))
		stats.stats.EventProcessingStats["timer_events"] = uint64(eStats["timer_msg_counter"].(float64))
		stats.incrementCountProcessingStatsLocked("execution_stats", 1)
		stats.Unlock()

	case processManager.Insight:
		insight := common.NewInsight()
		err := json.Unmarshal([]byte(msg.Value), insight)
		if err != nil {
			return
		}

		if insight == nil {
			insight = common.NewInsight()
		}

		stats.Lock()
		stats.stats.Insight = insight
		stats.incrementCountProcessingStatsLocked("insight_stats", 1)
		stats.Unlock()

	case processManager.LatencyStats:
		latency := make(map[string]uint64)
		err := json.Unmarshal([]byte(msg.Value), &latency)
		if err != nil {
			return
		}

		stats.Lock()
		stats.stats.TempLatencyHistogram.Update(latency)
		stats.incrementCountProcessingStatsLocked("latency_stats", 1)
		stats.Unlock()

	case processManager.CurlLatencyStats:
		latency := make(map[string]uint64)
		err := json.Unmarshal([]byte(msg.Value), &latency)
		if err != nil {
			return
		}

		stats.Lock()
		stats.stats.CurlLatency.Update(latency)
		stats.incrementCountProcessingStatsLocked("curl_latency_stats", 1)
		stats.Unlock()

	case processManager.AllStats:
		aStats := make(map[string]interface{})
		err := json.Unmarshal([]byte(msg.Value), &aStats)
		if err != nil {
			return
		}

		executionStats := make(map[string]interface{})
		err = json.Unmarshal([]byte(aStats["execution_stats"].(string)), &executionStats)
		if err != nil {
			return
		}

		failureStats := make(map[string]interface{})
		err = json.Unmarshal([]byte(aStats["failure_stats"].(string)), &failureStats)
		if err != nil {
			return
		}

		latencyStats := make(map[string]uint64)
		err = json.Unmarshal([]byte(aStats["latency_stats"].(string)), &latencyStats)
		if err != nil {
			return
		}

		curlLatencyStats := make(map[string]uint64)
		err = json.Unmarshal([]byte(aStats["curl_latency_stats"].(string)), &curlLatencyStats)
		if err != nil {
			return
		}

		lcbExceptionStats := make(map[string]uint64)
		err = json.Unmarshal([]byte(aStats["lcb_exception_stats"].(string)), &lcbExceptionStats)
		if err != nil {
			return
		}

		stats.Lock()
		stats.stats.FailureStats = failureStats
		stats.stats.ExecutionStats = executionStats
		stats.stats.EventProcessingStats["timer_responses_received"] = uint64(executionStats["timer_create_counter"].(float64))
		stats.stats.EventProcessingStats["timer_events"] = uint64(executionStats["timer_msg_counter"].(float64))
		stats.incrementCountProcessingStatsLocked("execution_stats", 1)
		stats.incrementCountProcessingStatsLocked("failure_stats", 1)
		stats.incrementCountProcessingStatsLocked("latency_stats", 1)
		stats.incrementCountProcessingStatsLocked("curl_latency_stats", 1)
		stats.incrementCountProcessingStatsLocked("lcb_exception_stats", 1)
		stats.stats.TempLatencyHistogram.Update(latencyStats)
		stats.stats.CurlLatency.Update(curlLatencyStats)
		stats.stats.LCBExceptionStats = lcbExceptionStats
		stats.Unlock()
	}
}

const (
	seqCheckTime = time.Duration(30) * time.Second
)

func (stats *statsHandler) start(ctx context.Context, version uint32, instanceID []byte, re RuntimeEnvironment, vbHandler vbhandler.VbHandler, statsDuration time.Duration) {
	logPrefix := fmt.Sprintf("statsHandler::statsHandler[%s]", stats.logPrefix)
	stats.Lock()
	stats.stats = common.NewStats(true, stats.location.Namespace, stats.location.Appname, common.FullStats)
	stats.discardedStats = common.NewStats(true, stats.location.Namespace, stats.location.Appname, common.PartialStats)
	stats.Unlock()

	tick := time.NewTicker(statsDuration * time.Millisecond)
	seqTick := time.NewTicker(seqCheckTime)
	printStats := time.NewTicker(printStatsInterval)

	defer func() {
		// TODO: Close the seq number manager
		seqTick.Stop()
		printStats.Stop()
		tick.Stop()
	}()

	for {
		select {
		case <-tick.C:
			re.GetStats(version, processManager.StatsAckBytes, instanceID)
			re.GetStats(version, processManager.ProcessedEvents, instanceID)
			re.GetStats(version, processManager.Insight, instanceID)
			re.GetStats(version, processManager.AllStats, instanceID)
			stats.Lock()
			stats.incrementCountProcessingStatsLocked("agg_messages_sent_to_worker", 4)
			stats.Unlock()

		case <-seqTick.C:
			vbToSeq := vbHandler.GetHighSeqNum()
			mutationsRemaining := uint64(0)
			stats.RLock()
			for vb, seq := range vbToSeq {
				if seq > stats.stats.ProcessedSeq[vb] {
					mutationsRemaining += seq - stats.stats.ProcessedSeq[vb]
				}
			}
			stats.RUnlock()

			stats.Lock()
			stats.stats.EventRemaining["dcp_backlog"] = mutationsRemaining
			stats.Unlock()

		case <-printStats.C:
			stats.RLock()
			logging.Infof("%s %s", logPrefix, stats.stats)
			stats.RUnlock()

		case <-ctx.Done():
			return
		}
	}
}
