package consumer

import (
	"time"
)

func newVbProcessingStats() vbStats {
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

		// Below stats will be updated by plasma reader routines, which
		// will process timer events
		vbsts[i].stats["currently_processed_timer"] = time.Now().UTC().Format(time.RFC3339)
		vbsts[i].stats["last_processed_timer_event"] = ""
		vbsts[i].stats["next_timer_to_process"] = time.Now().UTC().Add(time.Second).Format(time.RFC3339)
		vbsts[i].stats["timer_processing_worker"] = ""
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
