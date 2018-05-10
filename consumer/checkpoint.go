package consumer

import (
	"fmt"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
)

func (c *Consumer) doLastSeqNoCheckpoint() {
	logPrefix := "Consumer::doLastSeqNoCheckpoint"

	c.checkpointTicker = time.NewTicker(c.checkpointInterval)

	var vbBlob vbucketKVBlob
	var cas gocb.Cas
	var isNoEnt bool
	checkpoints := make([]time.Time, 1024)
	for {
		select {
		case <-c.checkpointTicker.C:
			deployedApps := c.superSup.GetLocallyDeployedApps()
			if _, ok := deployedApps[c.app.AppName]; !ok {
				logging.Infof("%s [%s:%s:%d] Returning from checkpoint ticker routine",
					logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			err := util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getEventingNodeAddrOpCallback, c)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			for vbno := range c.vbProcessingStats {

				// only checkpoint stats for vbuckets that the consumer instance owns
				if c.ConsumerName() == c.vbProcessingStats.getVbStat(vbno, "assigned_worker") &&
					c.NodeUUID() == c.vbProcessingStats.getVbStat(vbno, "node_uuid") {

					vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vbno)

					if c.isVbIdle(vbno, &checkpoints[vbno]) {
						continue
					}
					// Metadata blob doesn't exist probably the app is deployed for the first time.
					err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, true, &isNoEnt)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
					}

					if isNoEnt {

						logging.Infof("%s [%s:%s:%d] vb: %d Creating the initial metadata blob entry",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), vbno)

						err = c.updateCheckpointInfo(vbKey, vbno, &vbBlob)
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
							return
						}

						continue
					}

					// Steady state cluster
					if c.NodeUUID() == vbBlob.NodeUUID && vbBlob.DCPStreamStatus == dcpStreamRunning {
						err = c.updateCheckpointInfo(vbKey, vbno, &vbBlob)
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
							return
						}

						continue
					}

					// Needed to handle race between previous owner(another eventing node) and new owner(current node).
					if vbBlob.CurrentVBOwner == "" && c.checkIfCurrentNodeShouldOwnVb(vbno) &&
						c.checkIfCurrentConsumerShouldOwnVb(vbno) && vbBlob.DCPStreamStatus == dcpStreamStopped {

						err = c.updateCheckpointInfo(vbKey, vbno, &vbBlob)
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
							return
						}

						continue
					}
				}
			}

		case <-c.stopCheckpointingCh:
			logging.Infof("%s [%s:%s:%d] Exited checkpointing routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}

func (c *Consumer) updateCheckpointInfo(vbKey string, vbno uint16, vbBlob *vbucketKVBlob) error {
	logPrefix := "Consumer::updateCheckpointInfo"

	c.updateBackupVbStats(vbno)
	vbBlob.AssignedWorker = c.ConsumerName()
	vbBlob.CurrentVBOwner = c.HostPortAddr()
	vbBlob.DCPStreamStatus = c.vbProcessingStats.getVbStat(vbno, "dcp_stream_status").(string)
	vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
	vbBlob.NodeUUID = c.NodeUUID()
	vbBlob.VBId = vbno

	vbBlob.CurrentProcessedDocIDTimer = c.vbProcessingStats.getVbStat(vbno, "currently_processed_doc_id_timer").(string)
	vbBlob.CurrentProcessedCronTimer = c.vbProcessingStats.getVbStat(vbno, "currently_processed_cron_timer").(string)
	vbBlob.LastCleanedUpDocIDTimerEvent = c.vbProcessingStats.getVbStat(vbno, "last_cleaned_up_doc_id_timer_event").(string)
	vbBlob.LastDocIDTimerSentToWorker = c.vbProcessingStats.getVbStat(vbno, "last_doc_id_timer_sent_to_worker").(string)
	vbBlob.LastDocTimerFeedbackSeqNo = c.vbProcessingStats.getVbStat(vbno, "last_doc_timer_feedback_seqno").(uint64)
	vbBlob.LastSeqNoProcessed = c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no").(uint64)
	vbBlob.NextDocIDTimerToProcess = c.vbProcessingStats.getVbStat(vbno, "next_doc_id_timer_to_process").(string)
	vbBlob.NextCronTimerToProcess = c.vbProcessingStats.getVbStat(vbno, "next_cron_timer_to_process").(string)

	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, periodicCheckpointCallback, c, vbKey, vbBlob)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return common.ErrRetryTimeout
	}

	return nil
}

func (c *Consumer) isVbIdle(vbno uint16, checkpointTime *time.Time) bool {
	currentTime := time.Now()
	if checkpointTime.IsZero() == false &&
		currentTime.Sub(*checkpointTime) < c.idleCheckpointInterval &&
		c.backupVbStats.getVbStat(vbno, "last_processed_seq_no").(uint64) == c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no").(uint64) &&
		c.backupVbStats.getVbStat(vbno, "last_doc_timer_feedback_seqno").(uint64) == c.vbProcessingStats.getVbStat(vbno, "last_doc_timer_feedback_seqno").(uint64) &&
		c.backupVbStats.getVbStat(vbno, "sent_to_worker_counter").(uint64) == c.vbProcessingStats.getVbStat(vbno, "sent_to_worker_counter").(uint64) &&
		c.backupVbStats.getVbStat(vbno, "processed_crontimer_counter").(uint64) == c.vbProcessingStats.getVbStat(vbno, "processed_crontimer_counter").(uint64) {
		return true
	}
	*checkpointTime = currentTime
	return false
}

func (c *Consumer) updateBackupVbStats(vbno uint16) {
	bucketopSeqNo := c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no").(uint64)
	doctimerSeqNo := c.vbProcessingStats.getVbStat(vbno, "last_doc_timer_feedback_seqno").(uint64)
	doctimerCount := c.vbProcessingStats.getVbStat(vbno, "sent_to_worker_counter").(uint64)
	crontimerCount := c.vbProcessingStats.getVbStat(vbno, "processed_crontimer_counter").(uint64)
	c.backupVbStats.updateVbStat(vbno, "last_processed_seq_no", bucketopSeqNo)
	c.backupVbStats.updateVbStat(vbno, "last_doc_timer_feedback_seqno", doctimerSeqNo)
	c.backupVbStats.updateVbStat(vbno, "sent_to_worker_counter", doctimerCount)
	c.backupVbStats.updateVbStat(vbno, "processed_crontimer_counter", crontimerCount)
}
