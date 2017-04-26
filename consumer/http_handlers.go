package consumer

import (
	cm "github.com/couchbase/eventing/common"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

// RebalanceTaskProgress reports progress to producer
func (c *Consumer) RebalanceTaskProgress() *cm.RebalanceProgress {
	progress := &cm.RebalanceProgress{}

	vbsRemainingToGiveUp := c.getVbRemainingToGiveUp()
	vbsRemainingToOwn := c.getVbRemainingToOwn()

	if len(vbsRemainingToGiveUp) > 0 || len(vbsRemainingToOwn) > 0 {
		vbsOwnedPerPlan := c.getVbsOwned()
		vbsCurrentlyOwned := c.verifyVbsCurrentlyOwned(vbsOwnedPerPlan)

		progress.VbsCurrentlyOwned = len(vbsCurrentlyOwned)
		progress.VbsOwnedPerPlan = len(vbsOwnedPerPlan)
	}

	return progress
}

// DcpEventsProcessedPSec reports dcp events triggered per sec
func (c *Consumer) DcpEventsProcessedPSec() int {
	return c.dcpOpsProcessedPSec
}

// DcpEventsRemainingToProcess reports dcp events remaining to producer
func (c *Consumer) DcpEventsRemainingToProcess() uint64 {
	vbsTohandle := c.vbsToHandle()

	seqNos, err := common.BucketSeqnos(c.producer.NsServerHostPort(), "default", c.bucket)
	if err != nil {
		logging.Errorf("CRVT[%s:%s:%s:%d] Failed to fetch get_all_vb_seqnos, err: %v", c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
		return 0
	}

	var eventsProcessed, totalEvents uint64
	for _, vbno := range vbsTohandle {
		eventsProcessed += c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no").(uint64)
		totalEvents += seqNos[int(vbno)]
	}

	return totalEvents - eventsProcessed
}

// VbProcessingStats exposes consumer vb metadata to producer
func (c *Consumer) VbProcessingStats() map[uint16]map[string]interface{} {
	vbstats := make(map[uint16]map[string]interface{})
	for vbno := range c.vbProcessingStats {
		if _, ok := vbstats[vbno]; !ok {
			vbstats[vbno] = make(map[string]interface{})
		}
		assignedWorker := c.vbProcessingStats.getVbStat(vbno, "assigned_worker")
		owner := c.vbProcessingStats.getVbStat(vbno, "current_vb_owner")
		streamStatus := c.vbProcessingStats.getVbStat(vbno, "dcp_stream_status")
		seqNo := c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no")
		uuid := c.vbProcessingStats.getVbStat(vbno, "node_uuid")

		vbstats[vbno]["assigned_worker"] = assignedWorker
		vbstats[vbno]["current_vb_owner"] = owner
		vbstats[vbno]["node_uuid"] = uuid
		vbstats[vbno]["stream_status"] = streamStatus
		vbstats[vbno]["seq_no"] = seqNo
	}

	return vbstats
}
