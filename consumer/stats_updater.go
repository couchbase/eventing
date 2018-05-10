package consumer

import (
	"time"

	"github.com/couchbase/eventing/logging"
)

func newVbProcessingStats(appName string, numVbuckets uint16, uuid, workerName string) vbStats {
	vbsts := make(vbStats, numVbuckets)
	for i := uint16(0); i < numVbuckets; i++ {
		vbsts[i] = &vbStat{
			stats: make(map[string]interface{}),
		}
		vbsts[i].stats["assigned_worker"] = ""
		vbsts[i].stats["dcp_stream_status"] = dcpStreamStopped
		vbsts[i].stats["requesting_worker"] = ""

		vbsts[i].stats["plasma_last_seq_no_stored"] = uint64(0)
		vbsts[i].stats["plasma_last_seq_no_persisted"] = uint64(0)

		vbsts[i].stats["last_doc_timer_feedback_seqno"] = uint64(0)
		vbsts[i].stats["last_processed_seq_no"] = uint64(0)

		vbsts[i].stats["currently_processed_doc_id_timer"] = time.Now().UTC().Format(time.RFC3339)
		vbsts[i].stats["last_cleaned_up_doc_id_timer_event"] = time.Now().UTC().Format(time.RFC3339)
		vbsts[i].stats["last_doc_id_timer_sent_to_worker"] = time.Now().UTC().Format(time.RFC3339)
		vbsts[i].stats["last_processed_doc_id_timer_event"] = time.Now().UTC().Format(time.RFC3339)
		vbsts[i].stats["next_doc_id_timer_to_process"] = time.Now().UTC().Add(time.Second).Format(time.RFC3339)

		vbsts[i].stats["next_cron_timer_to_process"] = time.Now().UTC().Add(time.Second).Format(time.RFC3339)
		vbsts[i].stats["currently_processed_cron_timer"] = time.Now().UTC().Add(time.Second).Format(time.RFC3339)
		vbsts[i].stats["last_processed_cron_timer_event"] = time.Now().UTC().Add(time.Second).Format(time.RFC3339)

		// Doc timer debug stats
		vbsts[i].stats["deleted_during_cleanup_counter"] = uint64(0)
		vbsts[i].stats["removed_during_rebalance_counter"] = uint64(0)
		vbsts[i].stats["sent_to_worker_counter"] = uint64(0)
		vbsts[i].stats["timer_create_counter"] = uint64(0)
		vbsts[i].stats["timers_in_past_counter"] = uint64(0)
		vbsts[i].stats["timers_in_past_from_backfill_counter"] = uint64(0)
		vbsts[i].stats["timers_recreated_from_dcp_backfill"] = uint64(0)

		//Cron timer debug stats
		vbsts[i].stats["processed_crontimer_counter"] = uint64(0)

		// vb seq no stats
		vbsts[i].stats["ever_owned_vb"] = false
		vbsts[i].stats["host_name"] = ""
		vbsts[i].stats["last_checkpointed_seq_no"] = uint64(0)
		vbsts[i].stats["last_read_seq_no"] = uint64(0)
		vbsts[i].stats["node_uuid"] = uuid
		vbsts[i].stats["start_seq_no"] = uint64(0)
		vbsts[i].stats["seq_no_at_stream_end"] = uint64(0)
		vbsts[i].stats["seq_no_after_close_stream"] = uint64(0)
		vbsts[i].stats["timestamp"] = time.Now().UTC().Format(time.RFC3339)
		vbsts[i].stats["worker_name"] = workerName
	}
	return vbsts
}

func newVbBackupStats(numVbuckets uint16) vbStats {
	vbsts := make(vbStats, numVbuckets)
	for i := uint16(0); i < numVbuckets; i++ {
		vbsts[i] = &vbStat{
			stats: make(map[string]interface{}),
		}
		vbsts[i].stats["last_doc_timer_feedback_seqno"] = uint64(0)
		vbsts[i].stats["last_processed_seq_no"] = uint64(0)
		//Doc timer counter
		vbsts[i].stats["sent_to_worker_counter"] = uint64(0)
		//Cron timer counter
		vbsts[i].stats["processed_crontimer_counter"] = uint64(0)
	}
	return vbsts
}

func (vbs vbStats) getVbStat(vb uint16, statName string) interface{} {
	vbstat := vbs[vb]
	vbstat.RLock()
	defer vbstat.RUnlock()
	return vbstat.stats[statName]
}

func (vbs vbStats) updateVbStat(vb uint16, statName string, val interface{}) {
	vbstat := vbs[vb]
	vbstat.Lock()
	defer vbstat.Unlock()
	vbstat.stats[statName] = val
}

func (vbs vbStats) copyVbStats(numVbuckets uint16) vbStats {
	vbsts := make(vbStats, numVbuckets)
	for i := uint16(0); i < numVbuckets; i++ {
		vbsts[i] = &vbStat{
			stats: make(map[string]interface{}),
		}

		// params to copy over
		vbsts[i].stats["assigned_worker"] = vbs.getVbStat(i, "assigned_worker")
		vbsts[i].stats["current_vb_owner"] = vbs.getVbStat(i, "current_vb_owner")
		vbsts[i].stats["currently_processed_doc_id_timer"] = vbs.getVbStat(i, "currently_processed_doc_id_timer")
		vbsts[i].stats["currently_processed_cron_timer"] = vbs.getVbStat(i, "currently_processed_cron_timer")
		vbsts[i].stats["dcp_stream_status"] = vbs.getVbStat(i, "dcp_stream_status")

		vbsts[i].stats["last_cleaned_up_doc_id_timer_event"] = vbs.getVbStat(i, "last_cleaned_up_doc_id_timer_event")
		vbsts[i].stats["last_doc_id_timer_sent_to_worker"] = vbs.getVbStat(i, "last_doc_id_timer_sent_to_worker")
		vbsts[i].stats["last_processed_doc_id_timer_event"] = vbs.getVbStat(i, "last_processed_doc_id_timer_event")
		vbsts[i].stats["next_doc_id_timer_to_process"] = vbs.getVbStat(i, "next_doc_id_timer_to_process")

		vbsts[i].stats["last_processed_seq_no"] = vbs.getVbStat(i, "last_processed_seq_no")
		vbsts[i].stats["last_doc_timer_feedback_seqno"] = vbs.getVbStat(i, "last_doc_timer_feedback_seqno")

		vbsts[i].stats["next_cron_timer_to_process"] = vbs.getVbStat(i, "next_cron_timer_to_process")
		vbsts[i].stats["last_processed_cron_timer_event"] = vbs.getVbStat(i, "last_processed_cron_timer_event")

		vbsts[i].stats["node_uuid"] = vbs.getVbStat(i, "node_uuid")

		vbsts[i].stats["deleted_during_cleanup_counter"] = vbs.getVbStat(i, "deleted_during_cleanup_counter")
		vbsts[i].stats["removed_during_rebalance_counter"] = vbs.getVbStat(i, "removed_during_rebalance_counter")
		vbsts[i].stats["sent_to_worker_counter"] = vbs.getVbStat(i, "sent_to_worker_counter")
		vbsts[i].stats["timer_create_counter"] = vbs.getVbStat(i, "timer_create_counter")
		vbsts[i].stats["timers_in_past_counter"] = vbs.getVbStat(i, "timers_in_past_counter")
		vbsts[i].stats["timers_in_past_from_backfill_counter"] = vbs.getVbStat(i, "timers_in_past_from_backfill_counter")
		vbsts[i].stats["timers_recreated_from_dcp_backfill"] = vbs.getVbStat(i, "timers_recreated_from_dcp_backfill")

	}
	return vbsts
}

func (c *Consumer) updateWorkerStats() {
	logPrefix := "Consumer::updateWorkerStats"

	for {
		select {
		case <-c.updateStatsTicker.C:
			if c.workerExited {
				logging.Infof("%s [%s:%s:%d] Skipping sending worker stat opcode as worker exited",
					logPrefix, c.workerName, c.tcpPort, c.Pid())
				continue
			}

			c.dcpEventsRemainingToProcess()
			c.sendGetExecutionStats(false)
			c.sendGetFailureStats(false)
			c.sendGetLatencyStats(false)
			c.sendGetLcbExceptionStats(false)

		case <-c.updateStatsStopCh:
			logging.Infof("%s [%s:%s:%d] Exiting cpp worker stats updater routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}
