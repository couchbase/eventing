package consumer

import (
	"fmt"
	"time"
)

func newVbProcessingStats(appName string) vbStats {
	vbsts := make(vbStats, numVbuckets)
	for i := uint16(0); i < numVbuckets; i++ {
		vbsts[i] = &vbStat{
			stats: make(map[string]interface{}),
		}
		vbsts[i].stats["last_processed_seq_no"] = uint64(0)
		vbsts[i].stats["dcp_stream_status"] = dcpStreamStopped
		vbsts[i].stats["assigned_worker"] = ""
		vbsts[i].stats["requesting_worker"] = ""

		// Stored in memory but not persisted to disk,
		// persistence to disk takes place periodically. Below 4 stats
		// will be updated by plasma writer routines
		vbsts[i].stats["plasma_last_seq_no_stored"] = uint64(0)
		vbsts[i].stats["plasma_last_seq_no_persisted"] = uint64(0)

		vbsts[i].stats["currently_processed_doc_id_timer"] = time.Now().UTC().Format(time.RFC3339)
		vbsts[i].stats["currently_processed_non_doc_timer"] = fmt.Sprintf("%s::%s", appName, time.Now().UTC().Format(time.RFC3339))
		vbsts[i].stats["last_processed_doc_id_timer_event"] = ""
		vbsts[i].stats["next_doc_id_timer_to_process"] = time.Now().UTC().Add(time.Second).Format(time.RFC3339)
		vbsts[i].stats["next_non_doc_timer_to_process"] = fmt.Sprintf("%s::%s", appName, time.Now().UTC().Add(time.Second).Format(time.RFC3339))
		vbsts[i].stats["doc_id_timer_processing_worker"] = ""
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

func (vbs vbStats) copyVbStats() vbStats {
	vbsts := make(vbStats, numVbuckets)
	for i := uint16(0); i < numVbuckets; i++ {
		vbsts[i] = &vbStat{
			stats: make(map[string]interface{}),
		}

		// params to copy over
		vbsts[i].stats["assigned_worker"] = vbs.getVbStat(i, "assigned_worker")
		vbsts[i].stats["current_vb_owner"] = vbs.getVbStat(i, "current_vb_owner")
		vbsts[i].stats["currently_processed_doc_id_timer"] = vbs.getVbStat(i, "currently_processed_doc_id_timer")
		vbsts[i].stats["currently_processed_non_doc_timer"] = vbs.getVbStat(i, "currently_processed_non_doc_timer")
		vbsts[i].stats["dcp_stream_status"] = vbs.getVbStat(i, "dcp_stream_status")
		vbsts[i].stats["doc_id_timer_processing_worker"] = vbs.getVbStat(i, "doc_id_timer_processing_worker")
		vbsts[i].stats["last_processed_doc_id_timer_event"] = vbs.getVbStat(i, "last_processed_doc_id_timer_event")
		vbsts[i].stats["last_processed_seq_no"] = vbs.getVbStat(i, "last_processed_seq_no")
		vbsts[i].stats["next_doc_id_timer_to_process"] = vbs.getVbStat(i, "next_doc_id_timer_to_process")
		vbsts[i].stats["next_non_doc_timer_to_process"] = vbs.getVbStat(i, "next_non_doc_timer_to_process")
		vbsts[i].stats["node_uuid"] = vbs.getVbStat(i, "node_uuid")
		vbsts[i].stats["plasma_last_persisted_seq_no"] = vbs.getVbStat(i, "plasma_last_persisted_seq_no")
	}
	return vbsts
}
