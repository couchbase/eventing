package consumer

import (
	"fmt"
	"sort"
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

			vbs := make([]uint16, 0)
			for vb := range c.vbProcessingStats {
				vbs = append(vbs, vb)
			}

			sort.Sort(util.Uint16Slice(vbs))

			for _, vb := range vbs {

				// only checkpoint stats for vbuckets that the consumer instance owns
				if c.ConsumerName() == c.vbProcessingStats.getVbStat(vb, "assigned_worker") &&
					c.NodeUUID() == c.vbProcessingStats.getVbStat(vb, "node_uuid") {

					vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vb)

					if c.isVbIdle(vb, &checkpoints[vb]) {
						continue
					}
					// Metadata blob doesn't exist probably the app is deployed for the first time.
					err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
						c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, true, &isNoEnt)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
					}

					if isNoEnt {
						logging.Infof("%s [%s:%s:%d] vb: %d Creating the initial metadata blob entry",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

						err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, recreateCheckpointBlobsFromVbStatsCallback, c,
							c.producer.AddMetadataPrefix(vbKey), &vbBlob)
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
							return
						}

						err = c.updateCheckpointInfo(vbKey, vb, &vbBlob)
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
							return
						}

						continue
					}

					// Steady state cluster
					if c.NodeUUID() == vbBlob.NodeUUID && vbBlob.DCPStreamStatus == dcpStreamRunning {
						err = c.updateCheckpointInfo(vbKey, vb, &vbBlob)
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
							return
						}

						continue
					}

					// Needed to handle race between previous owner(another eventing node) and new owner(current node).
					if vbBlob.CurrentVBOwner == "" && c.checkIfCurrentNodeShouldOwnVb(vb) &&
						c.checkIfCurrentConsumerShouldOwnVb(vb) && vbBlob.DCPStreamStatus == dcpStreamStopped {

						err = c.updateCheckpointInfo(vbKey, vb, &vbBlob)
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
							return
						}

						continue
					}
				}
			}

		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Exited checkpointing routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}

func (c *Consumer) updateCheckpointInfo(vbKey string, vb uint16, vbBlob *vbucketKVBlob) error {
	logPrefix := "Consumer::updateCheckpointInfo"

	c.updateBackupVbStats(vb)
	vbBlob.AssignedWorker = c.ConsumerName()
	vbBlob.CurrentVBOwner = c.HostPortAddr()
	vbBlob.NodeUUID = c.NodeUUID()
	vbBlob.VBId = vb

	vbBlob.CurrentProcessedDocIDTimer = c.vbProcessingStats.getVbStat(vb, "currently_processed_doc_id_timer").(string)
	vbBlob.CurrentProcessedCronTimer = c.vbProcessingStats.getVbStat(vb, "currently_processed_cron_timer").(string)
	vbBlob.DCPStreamStatus = c.vbProcessingStats.getVbStat(vb, "dcp_stream_status").(string)
	vbBlob.LastCleanedUpDocIDTimerEvent = c.vbProcessingStats.getVbStat(vb, "last_cleaned_up_doc_id_timer_event").(string)
	vbBlob.LastDocIDTimerSentToWorker = c.vbProcessingStats.getVbStat(vb, "last_doc_id_timer_sent_to_worker").(string)
	vbBlob.LastDocTimerFeedbackSeqNo = c.vbProcessingStats.getVbStat(vb, "last_doc_timer_feedback_seqno").(uint64)
	vbBlob.LastSeqNoProcessed = c.vbProcessingStats.getVbStat(vb, "last_processed_seq_no").(uint64)
	vbBlob.NextDocIDTimerToProcess = c.vbProcessingStats.getVbStat(vb, "next_doc_id_timer_to_process").(string)
	vbBlob.NextCronTimerToProcess = c.vbProcessingStats.getVbStat(vb, "next_cron_timer_to_process").(string)
	vbBlob.VBuuid = c.vbProcessingStats.getVbStat(vb, "vb_uuid").(uint64)

	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, periodicCheckpointCallback,
		c, c.producer.AddMetadataPrefix(vbKey), vbBlob)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	return nil
}

func (c *Consumer) isVbIdle(vbno uint16, checkpointTime *time.Time) bool {
	currentTime := time.Now()
	if checkpointTime.IsZero() == false &&
		currentTime.Sub(*checkpointTime) < c.idleCheckpointInterval &&
		c.backupVbStats.getVbStat(vbno, "last_processed_seq_no").(uint64) == c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no").(uint64) &&
		c.backupVbStats.getVbStat(vbno, "last_doc_timer_feedback_seqno").(uint64) == c.vbProcessingStats.getVbStat(vbno, "last_doc_timer_feedback_seqno").(uint64) &&
		c.backupVbStats.getVbStat(vbno, "sent_to_worker_counter").(uint64) == c.vbProcessingStats.getVbStat(vbno, "sent_to_worker_counter").(uint64) {
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
