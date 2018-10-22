package consumer

import (
	"errors"
	"math/rand"

	cm "github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/timers"
	"github.com/couchbase/eventing/util"
)

var errTimerQueueNotDrained = errors.New("timer queues are not drained")

// RebalanceTaskProgress reports progress to producer
func (c *Consumer) RebalanceTaskProgress() *cm.RebalanceProgress {
	logPrefix := "Consumer::RebalanceTaskProgress"

	progress := &cm.RebalanceProgress{}

	vbsRemainingToCloseStream := c.getVbRemainingToCloseStream()
	vbsRemainingToStreamReq := c.getVbRemainingToStreamReq()

	logging.Infof("%s [%s:%s:%d] IsBootstrapping: %t vbsRemainingToCloseStream len: %d dump: %v vbsRemainingToStreamReq len: %d dump: %v",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.isBootstrapping, len(vbsRemainingToCloseStream),
		util.Condense(vbsRemainingToCloseStream), len(vbsRemainingToStreamReq),
		util.Condense(vbsRemainingToStreamReq))

	if len(vbsRemainingToCloseStream) > 0 || len(vbsRemainingToStreamReq) > 0 {
		vbsOwnedPerPlan := c.getVbsOwned()

		progress.CloseStreamVbsLen = len(vbsRemainingToCloseStream)
		progress.StreamReqVbsLen = len(vbsRemainingToStreamReq)

		progress.VbsOwnedPerPlan = len(vbsOwnedPerPlan)
		progress.VbsRemainingToShuffle = len(vbsRemainingToCloseStream) + len(vbsRemainingToStreamReq)
	}

	// timerQueuesAreDrained - Primarily to avoid calls to checkIfTimerQueuesAreDrained()
	// multiple times and hence avoid filling up log files
	if !c.timerQueuesAreDrained && len(vbsRemainingToCloseStream) == 0 && c.usingTimer {

		logging.Infof("%s [%s:%s:%d] uuid: %s eject node UUIDs: %+v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.NodeUUID(), c.ejectNodesUUIDs)

		if util.Contains(c.NodeUUID(), c.ejectNodesUUIDs) {
			err := c.CheckIfQueuesAreDrained()
			if err != nil {
				// Faking rebalance progress while timer queues are getting drained
				vbsToMove := rand.Intn(5) + 1
				progress.VbsRemainingToShuffle = vbsToMove
				progress.CloseStreamVbsLen = vbsToMove
				return progress
			}
			c.timerQueuesAreDrained = true
		}
	}

	if len(vbsRemainingToCloseStream) == 0 && len(vbsRemainingToStreamReq) == 0 {
		if c.isRebalanceOngoing {
			logging.Infof("%s [%s:%s:%d] Updated isRebalanceOngoing to %t",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), c.isRebalanceOngoing)
		}
		c.isRebalanceOngoing = false
	}

	if c.isBootstrapping {
		progress.VbsRemainingToShuffle++
	}

	return progress
}

// CheckIfQueuesAreDrained looks at all queues to make sure no events are left,
// to avoid potential loss of events - especially during rebalance out and
// pausing of execution of a function
func (c *Consumer) CheckIfQueuesAreDrained() error {
	logPrefix := "Consumer::CheckIfQueuesAreDrained"

	vbsFilterAckYetToCome := c.getVbsFilterAckYetToCome()
	if len(vbsFilterAckYetToCome) > 0 {
		logging.Infof("%s [%s:%s:%d] vbsFilterAckYetToCome dump: %s len: %d",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), util.Condense(vbsFilterAckYetToCome), len(vbsFilterAckYetToCome))
		return errTimerQueueNotDrained
	}

	if c.cppQueueSizes.AggQueueSize > 0 {
		c.GetExecutionStats()
		logging.Infof("%s [%s:%s:%d] AggQueueSize: %d",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.cppQueueSizes.AggQueueSize)
		return errTimerQueueNotDrained
	}

	if c.usingTimer {
		if c.cppQueueSizes.DocTimerQueueSize > 0 {
			c.GetExecutionStats()
			logging.Infof("%s [%s:%s:%d] DocTimerQueueSize: %d",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), c.cppQueueSizes.DocTimerQueueSize)
			return errTimerQueueNotDrained
		}

		if c.createTimerQueue.Count() > 0 {
			logging.Infof("%s [%s:%s:%d] CreateTimerQueue size: %d",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), c.createTimerQueue.Count())
			return errTimerQueueNotDrained
		}

		var aggStorageQueueCount uint64
		c.timerStorageMetaChsRWMutex.RLock()
		for _, queue := range c.timerStorageQueues {
			aggStorageQueueCount += queue.Count()
		}
		c.timerStorageMetaChsRWMutex.RUnlock()

		if aggStorageQueueCount > 0 {
			logging.Infof("%s [%s:%s:%d] aggStorageQueueCount: %d",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), aggStorageQueueCount)
			return errTimerQueueNotDrained
		}

		if c.fireTimerQueue.Count() > 0 {
			logging.Infof("%s [%s:%s:%d] fireTimerQueue: %d",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), c.fireTimerQueue.Count())
			return errTimerQueueNotDrained
		}

		// All the timer queues are drained and the vb's have received STREAMEND
		// It is not possible for new events to create timers
		// Sync span timers for one last time before rebalancing out
		timers.ForceSpanSync()

		logging.Infof("%s [%s:%s:%d] TimerQueue: %d CreateTimerQueue: %d aggStorageQueue: %d aggQueue: %d fireTimerQueue: %d",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.cppQueueSizes.DocTimerQueueSize,
			c.createTimerQueue.Count(), aggStorageQueueCount, c.cppQueueSizes.AggQueueSize, c.fireTimerQueue.Count())
	}

	return nil
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
