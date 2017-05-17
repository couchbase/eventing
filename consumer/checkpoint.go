package consumer

import (
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

func (c *Consumer) doLastSeqNoCheckpoint() {
	c.checkpointTicker = time.NewTicker(checkPointInterval)

	for {
		select {
		case <-c.checkpointTicker.C:

			util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

			var vbBlob vbucketKVBlob

			for vbno := range c.vbProcessingStats {

				// only checkpoint stats for vbuckets that the consumer instance owns
				if c.HostPortAddr() == c.vbProcessingStats.getVbStat(vbno, "current_vb_owner") &&
					c.ConsumerName() == c.vbProcessingStats.getVbStat(vbno, "assigned_worker") &&
					c.NodeUUID() == c.vbProcessingStats.getVbStat(vbno, "node_uuid") {

					vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vbno)))

					var cas uint64
					var isNoEnt bool

					//Metadata blob doesn't exist probably the app is deployed for the first time.
					util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, true, &isNoEnt)
					if isNoEnt {

						logging.Infof("CRCH[%s:%s:%s:%d] vb: %d Creating the initial metadata blob entry",
							c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbno)

						c.updateCheckpointInfo(vbKey, vbno, &vbBlob, &cas)
						continue
					}

					// Steady state cluster
					if c.HostPortAddr() == vbBlob.CurrentVBOwner {

						c.updateCheckpointInfo(vbKey, vbno, &vbBlob, &cas)
						continue
					}

					// Needed to handle race between previous owner(another eventing node) and new owner(current node).
					if vbBlob.CurrentVBOwner == "" && c.checkIfCurrentNodeShouldOwnVb(vbno) &&
						c.checkIfCurrentConsumerShouldOwnVb(vbno) && vbBlob.DCPStreamStatus == dcpStreamStopped {

						c.updateCheckpointInfo(vbKey, vbno, &vbBlob, &cas)
						continue
					}

				}
			}

		case <-c.stopCheckpointingCh:
			return
		}
	}
}

func (c *Consumer) updateCheckpointInfo(vbKey string, vbno uint16, vbBlob *vbucketKVBlob, cas *uint64) {

	vbBlob.AssignedTimerWorker = c.vbProcessingStats.getVbStat(vbno, "timer_processing_worker").(string)
	vbBlob.AssignedWorker = c.ConsumerName()
	vbBlob.CurrentVBOwner = c.HostPortAddr()
	vbBlob.DCPStreamStatus = c.vbProcessingStats.getVbStat(vbno, "dcp_stream_status").(string)
	vbBlob.LastCheckpointTime = time.Now().Format(time.RFC3339)
	vbBlob.LastSeqNoProcessed = c.vbProcessingStats.getVbStat(vbno, "last_processed_seq_no").(uint64)
	vbBlob.NodeUUID = c.NodeUUID()
	vbBlob.VBId = vbno

	vbBlob.CurrentProcessedTimer = c.vbProcessingStats.getVbStat(vbno, "currently_processed_timer").(string)
	vbBlob.LastProcessedTimerEvent = c.vbProcessingStats.getVbStat(vbno, "last_processed_timer_event").(string)
	vbBlob.NextTimerToProcess = c.vbProcessingStats.getVbStat(vbno, "next_timer_to_process").(string)
	vbBlob.PlasmaPersistedSeqNo = c.vbProcessingStats.getVbStat(vbno, "plasma_last_seq_no_persisted").(uint64)

	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), casOpCallback, c, vbKey, vbBlob, cas)
}
