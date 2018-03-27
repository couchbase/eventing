package consumer

import (
	"fmt"
	"time"

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

	for {
		select {
		case <-c.checkpointTicker.C:
			deployedApps := c.superSup.GetDeployedApps()
			if _, ok := deployedApps[c.app.AppName]; !ok {
				logging.Infof("%s [%s:%s:%d] Returning from checkpoint ticker routine",
					logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

			for vbno := range c.vbProcessingStats {

				// only checkpoint stats for vbuckets that the consumer instance owns
				if c.ConsumerName() == c.vbProcessingStats.getVbStat(vbno, "assigned_worker") &&
					c.NodeUUID() == c.vbProcessingStats.getVbStat(vbno, "node_uuid") {

					vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vbno)

					// Metadata blob doesn't exist probably the app is deployed for the first time.
					util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, true, &isNoEnt)
					if isNoEnt {

						logging.Infof("%s [%s:%s:%d] vb: %d Creating the initial metadata blob entry",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), vbno)

						c.updateCheckpointInfo(vbKey, vbno, &vbBlob)
						continue
					}

					// Steady state cluster
					if c.NodeUUID() == vbBlob.NodeUUID && vbBlob.DCPStreamStatus == dcpStreamRunning {
						c.updateCheckpointInfo(vbKey, vbno, &vbBlob)
						continue
					}

					// Needed to handle race between previous owner(another eventing node) and new owner(current node).
					if vbBlob.CurrentVBOwner == "" && c.checkIfCurrentNodeShouldOwnVb(vbno) &&
						c.checkIfCurrentConsumerShouldOwnVb(vbno) && vbBlob.DCPStreamStatus == dcpStreamStopped {

						c.updateCheckpointInfo(vbKey, vbno, &vbBlob)
						continue
					}

				}
			}

		case <-c.stopCheckpointingCh:
			return
		}
	}
}

func (c *Consumer) updateCheckpointInfo(vbKey string, vbno uint16, vbBlob *vbucketKVBlob) {

	vbBlob.AssignedWorker = c.ConsumerName()
	vbBlob.CurrentVBOwner = c.HostPortAddr()
	vbBlob.DCPStreamStatus = c.vbProcessingStats.getVbStat(vbno, "dcp_stream_status").(string)
	vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
	vbBlob.NodeUUID = c.NodeUUID()
	vbBlob.VBId = vbno

	vbBlob.CurrentProcessedDocIDTimer = c.vbProcessingStats.getVbStat(vbno, "currently_processed_doc_id_timer").(string)
	vbBlob.CurrentProcessedCronTimer = c.vbProcessingStats.getVbStat(vbno, "currently_processed_cron_timer").(string)
	vbBlob.LastDocTimerFeedbackSeqNo = c.vbProcessingStats.getVbStat(vbno, "last_doc_timer_feedback_seqno").(uint64)
	vbBlob.NextDocIDTimerToProcess = c.vbProcessingStats.getVbStat(vbno, "next_doc_id_timer_to_process").(string)
	vbBlob.NextCronTimerToProcess = c.vbProcessingStats.getVbStat(vbno, "next_cron_timer_to_process").(string)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), periodicCheckpointCallback, c, vbKey, vbBlob)
}
