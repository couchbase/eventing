package consumer

import (
	cm "github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

// RebalanceTaskProgress reports progress to producer
func (c *Consumer) RebalanceTaskProgress() *cm.RebalanceProgress {
	logPrefix := "Consumer::RebalanceTaskProgress"

	progress := &cm.RebalanceProgress{}

	vbsRemainingToCloseStream := c.getVbRemainingToCloseStream()
	vbsRemainingToStreamReq := c.getVbRemainingToStreamReq()

	logging.Infof("%s [%s:%s:%d] vbsRemainingToCloseStream len: %d dump: %v vbsRemainingToStreamReq len: %d dump: %v",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), len(vbsRemainingToCloseStream),
		util.Condense(vbsRemainingToCloseStream), len(vbsRemainingToStreamReq),
		util.Condense(vbsRemainingToStreamReq))

	if len(vbsRemainingToCloseStream) > 0 || len(vbsRemainingToStreamReq) > 0 {
		vbsOwnedPerPlan := c.getVbsOwned()

		progress.CloseStreamVbsLen = len(vbsRemainingToCloseStream)
		progress.StreamReqVbsLen = len(vbsRemainingToStreamReq)

		progress.VbsOwnedPerPlan = len(vbsOwnedPerPlan)
		progress.VbsRemainingToShuffle = len(vbsRemainingToCloseStream) + len(vbsRemainingToStreamReq)
	}

	if len(vbsRemainingToCloseStream) == 0 && len(vbsRemainingToStreamReq) == 0 && c.vbsStateUpdateRunning {
		c.isRebalanceOngoing = false
		logging.Infof("%s [%s:%s:%d] Updated isRebalanceOngoing to %t",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.isRebalanceOngoing)
	}

	return progress
}

// EventsProcessedPSec reports dcp + timer events triggered per sec
func (c *Consumer) EventsProcessedPSec() *cm.EventProcessingStats {
	pStats := &cm.EventProcessingStats{
		DcpEventsProcessedPSec:   c.dcpOpsProcessedPSec,
		TimerEventsProcessedPSec: c.timerMessagesProcessedPSec,
	}

	return pStats
}

func (c *Consumer) dcpEventsRemainingToProcess() error {
	logPrefix := "Consumer::dcpEventsRemainingToProcess"

	vbsTohandle := c.vbsToHandle()
	if len(vbsTohandle) <= 0 {
		return nil
	}

	c.statsRWMutex.Lock()
	c.vbDcpEventsRemaining = make(map[int]int64)
	c.statsRWMutex.Unlock()

	seqNos, err := util.BucketSeqnos(c.producer.NsServerHostPort(), "default", c.bucket)
	if err != nil {
		logging.Errorf("%s [%s:%s:%d] Failed to fetch get_all_vb_seqnos, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
		c.dcpEventsRemaining = 0
		return err
	}

	var eventsProcessed, totalEvents uint64

	for _, vb := range vbsTohandle {
		seqNo := c.vbProcessingStats.getVbStat(vb, "last_read_seq_no").(uint64)

		if seqNos[int(vb)] > seqNo {
			c.statsRWMutex.Lock()
			c.vbDcpEventsRemaining[int(vb)] = int64(seqNos[int(vb)] - seqNo)
			c.statsRWMutex.Unlock()

			eventsProcessed += seqNo
			totalEvents += seqNos[int(vb)]
		}
	}

	if eventsProcessed > totalEvents {
		c.dcpEventsRemaining = 0
		return nil
	}

	c.dcpEventsRemaining = totalEvents - eventsProcessed
	return nil
}

// DcpEventsRemainingToProcess reports cached value for dcp events remaining to producer
func (c *Consumer) DcpEventsRemainingToProcess() uint64 {
	return c.dcpEventsRemaining
}

// VbDcpEventsRemainingToProcess reports cached dcp events remaining broken down to vbucket level
func (c *Consumer) VbDcpEventsRemainingToProcess() map[int]int64 {
	c.statsRWMutex.RLock()
	defer c.statsRWMutex.RUnlock()
	vbDcpEventsRemaining := make(map[int]int64)

	for vb, count := range c.vbDcpEventsRemaining {
		vbDcpEventsRemaining[vb] = count
	}

	return vbDcpEventsRemaining
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
		currentProcDocIDTimer := c.vbProcessingStats.getVbStat(vbno, "currently_processed_doc_id_timer").(string)
		currentProcCronTimer := c.vbProcessingStats.getVbStat(vbno, "currently_processed_cron_timer").(string)
		lastProcDocIDTimer := c.vbProcessingStats.getVbStat(vbno, "last_processed_doc_id_timer_event").(string)
		nextDocIDTimer := c.vbProcessingStats.getVbStat(vbno, "next_doc_id_timer_to_process").(string)
		nextCronTimer := c.vbProcessingStats.getVbStat(vbno, "next_cron_timer_to_process").(string)
		plasmaLastSeqNoPersist := c.vbProcessingStats.getVbStat(vbno, "plasma_last_seq_no_persisted").(uint64)

		vbstats[vbno]["assigned_worker"] = assignedWorker
		vbstats[vbno]["current_vb_owner"] = owner
		vbstats[vbno]["node_uuid"] = uuid
		vbstats[vbno]["stream_status"] = streamStatus
		vbstats[vbno]["seq_no"] = seqNo
		vbstats[vbno]["currently_processed_doc_id_timer"] = currentProcDocIDTimer
		vbstats[vbno]["currently_processed_cron_timer"] = currentProcCronTimer
		vbstats[vbno]["last_processed_doc_id_timer_event"] = lastProcDocIDTimer
		vbstats[vbno]["next_doc_id_timer_to_process"] = nextDocIDTimer
		vbstats[vbno]["next_cron_timer_to_process"] = nextCronTimer
		vbstats[vbno]["plasma_last_seq_no_persisted"] = plasmaLastSeqNoPersist
	}

	return vbstats
}
