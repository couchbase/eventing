package consumer

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
