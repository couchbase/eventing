package producer

func newVbProcessingStats() vbStats {
	vbsts := make(vbStats, NUM_VBUCKETS)
	for i := uint16(0); i < NUM_VBUCKETS; i++ {
		vbsts[i] = &vbStat{
			stats: make(map[string]interface{}),
		}
		vbsts[i].stats["last_processed_seq_no"] = uint64(0)
		vbsts[i].stats["dcp_stream_status"] = DCP_STREAM_STOPPED
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
