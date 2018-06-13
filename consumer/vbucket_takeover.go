package consumer

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
)

var errDcpFeedsClosed = errors.New("dcp feeds are closed")
var errDcpStreamRequested = errors.New("another worker issued STREAMREQ")
var errUnexpectedVbStreamStatus = errors.New("unexpected vbucket stream status")
var errVbOwnedByAnotherWorker = errors.New("vbucket is owned by another worker on same node")
var errVbOwnedByAnotherNode = errors.New("vbucket is owned by another node")

func (c *Consumer) reclaimVbOwnership(vb uint16) error {
	logPrefix := "Consumer::reclaimVbOwnership"

	var vbBlob vbucketKVBlob
	var cas gocb.Cas

	err := c.doVbTakeover(vb)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return common.ErrRetryTimeout
	}

	vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vb)
	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, false)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return common.ErrRetryTimeout
	}

	if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() {
		logging.Debugf("%s [%s:%s:%d] vb: %d successfully reclaimed ownership",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		return nil
	}

	return fmt.Errorf("Failed to reclaim vb ownership")
}

// Vbucket ownership give-up routine
func (c *Consumer) vbGiveUpRoutine(vbsts vbStats, giveupWg *sync.WaitGroup) {
	logPrefix := "Consumer::vbGiveUpRoutine"

	defer giveupWg.Done()

	if len(c.vbsRemainingToGiveUp) == 0 {
		logging.Tracef("%s [%s:%s:%d] No vbuckets remaining to give up",
			logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	vbsDistribution := util.VbucketDistribution(c.vbsRemainingToGiveUp, c.vbOwnershipGiveUpRoutineCount)

	for k, v := range vbsDistribution {
		logging.Infof("%s [%s:%s:%d] vb give up routine id: %d, vbs assigned len: %d dump: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), k, len(v), util.Condense(v))
	}

	var wg sync.WaitGroup
	wg.Add(c.vbOwnershipGiveUpRoutineCount)

	for i := 0; i < c.vbOwnershipGiveUpRoutineCount; i++ {
		go func(c *Consumer, i int, vbsRemainingToGiveUp []uint16, wg *sync.WaitGroup, vbsts vbStats) {

			defer wg.Done()

			var vbBlob vbucketKVBlob
			var cas gocb.Cas

			for _, vb := range vbsRemainingToGiveUp {
				vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vb)
				err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, false)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return
				}

				if vbBlob.NodeUUID != c.NodeUUID() && vbBlob.DCPStreamStatus == dcpStreamRunning {
					logging.Infof("%s [%s:giveup_r_%d:%s:%d] vb: %d metadata node uuid: %d dcp stream status: %d, skipping give up phase",
						logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb, vbBlob.NodeUUID, vbBlob.DCPStreamStatus)

					logging.Infof("%s [%s:giveup_r_%d:%s:%d] vb: %d Issuing dcp close stream",
						logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb)
					c.RLock()
					err := c.vbDcpFeedMap[vb].DcpCloseStream(vb, vb)
					if err != nil {
						logging.Errorf("%s [%s:giveup_r_%d:%s:%d] vb: %d Failed to close dcp stream, err: %v",
							logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb, err)
					}
					c.RUnlock()

					c.vbProcessingStats.updateVbStat(vb, "assigned_worker", "")
					c.vbProcessingStats.updateVbStat(vb, "current_vb_owner", "")
					c.vbProcessingStats.updateVbStat(vb, "dcp_stream_status", dcpStreamStopped)
					c.vbProcessingStats.updateVbStat(vb, "node_uuid", "")

					lastSeqNo := c.vbProcessingStats.getVbStat(uint16(vb), "last_read_seq_no").(uint64)
					c.vbProcessingStats.updateVbStat(vb, "seq_no_after_close_stream", lastSeqNo)
					c.vbProcessingStats.updateVbStat(vb, "timestamp", time.Now().Format(time.RFC3339))

					continue
				}

				logging.Infof("%s [%s:giveup_r_%d:%s:%d] vb: %d uuid: %s vbStat uuid: %s owner node: %rs consumer name: %s",
					logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb, c.NodeUUID(),
					vbsts.getVbStat(vb, "node_uuid"),
					vbsts.getVbStat(vb, "current_vb_owner"),
					vbsts.getVbStat(vb, "assigned_worker"))

				if vbsts.getVbStat(vb, "node_uuid") == c.NodeUUID() &&
					vbsts.getVbStat(vb, "assigned_worker") == c.ConsumerName() {

					c.vbsStreamClosedRWMutex.Lock()
					_, cUpdated := c.vbsStreamClosed[vb]
					if !cUpdated {
						c.vbsStreamClosed[vb] = true
					}
					c.vbsStreamClosedRWMutex.Unlock()

					var dcpStreamToClose *couchbase.DcpFeed
					func() {
						c.RLock()
						dcpStreamToClose = c.vbDcpFeedMap[vb]
						c.RUnlock()
					}()

					// TODO: Retry loop for dcp close stream as it could fail and additional verification checks.
					// Additional check needed to verify if vbBlob.NewOwner is the expected owner
					// as per the vbEventingNodesAssignMap.
					logging.Infof("%s [%s:giveup_r_%d:%s:%d] vb: %d Issuing dcp close stream",
						logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb)
					err := dcpStreamToClose.DcpCloseStream(vb, vb)
					if err != nil {
						logging.Errorf("%s [%s:giveup_r_%d:%s:%d] vb: %d Failed to close dcp stream, err: %v",
							logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb, err)
					}

					lastSeqNo := c.vbProcessingStats.getVbStat(uint16(vb), "last_read_seq_no").(uint64)
					c.vbProcessingStats.updateVbStat(vb, "seq_no_after_close_stream", lastSeqNo)
					c.vbProcessingStats.updateVbStat(vb, "timestamp", time.Now().Format(time.RFC3339))

					if !cUpdated {
						logging.Infof("%s [%s:giveup_r_%d:%s:%d] vb: %d updating metadata about dcp stream close",
							logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb)

						err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, false)
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
							return
						}

						err = c.updateCheckpoint(vbKey, vb, &vbBlob)
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
							return
						}
					}

					// Check if another node has taken up ownership of vbucket for which
					// ownership was given up above. Metadata is updated about ownership give up only after
					// DCP_STREAMEND is received from DCP producer
				retryVbMetaStateCheck:
					err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, false)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
					}

					logging.Infof("%s [%s:giveup_r_%d:%s:%d] vb: %d Metadata check, stream status: %s owner node: %s worker: %s",
						logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus, vbBlob.CurrentVBOwner, vbBlob.AssignedWorker)

					select {
					case <-c.stopVbOwnerGiveupCh:
						logging.Infof("%s [%s:giveup_r_%d:%s:%d] Exiting vb ownership give-up routine, last vb handled: %d",
							logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb)
						return

					default:

						// Retry looking up metadata for vbucket whose ownership has been given up if:
						// (a) DCP stream status isn't running
						// (b) If NodeUUID and AssignedWorker are still mapping to Eventing.Consumer instance that just gave up the
						//     ownership of that vbucket (could happen because metadata is only updated only when actual DCP_STREAMEND
						//     is received). Happens in case of data rollback
						if vbBlob.DCPStreamStatus != dcpStreamRunning || (vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName()) {
							time.Sleep(retryVbMetaStateCheckInterval)

							// Handling the case where KV rollbacks the checkpoint data update post DcpCloseStream
							if vbBlob.DCPStreamStatus == dcpStreamRunning && vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() {

								logging.Infof("%s [%s:giveup_r_%d:%s:%d] vb: %d KV potentially lost checkpoint data update post DcpCloseStream call, rewriting...",
									logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb)
								err = c.updateCheckpoint(vbKey, vb, &vbBlob)
								if err == common.ErrRetryTimeout {
									logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
									return
								}
							}

							goto retryVbMetaStateCheck
						}
						logging.Infof("%s [%s:giveup_r_%d:%s:%d] Gracefully exited vb ownership give-up routine, last handled vb: %d",
							logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb)
					}
				}
			}
		}(c, i, vbsDistribution[i], &wg, vbsts)
	}

	wg.Wait()
}

func (c *Consumer) checkAndUpdateMetadataLoop() {
	logPrefix := "Consumer::checkAndUpdateMetadataLoop"
	c.checkMetadataStateTicker = time.NewTicker(restartVbDcpStreamTickInterval)

	logging.Infof("%s [%s:%s:%d] Started up the routine", logPrefix, c.workerName, c.tcpPort, c.Pid())

	for {
		select {
		case <-c.checkMetadataStateTicker.C:
			if !c.isRebalanceOngoing {
				c.checkMetadataStateTicker.Stop()
				logging.Infof("%s [%s:%s:%d] Exiting the routine", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			c.checkAndUpdateMetadata()
		}
	}
}

func (c *Consumer) checkAndUpdateMetadata() {
	logPrefix := "Consumer::checkAndUpdateMetadata"
	vbsOwned := c.getCurrentlyOwnedVbs()

	var vbBlob vbucketKVBlob
	var cas gocb.Cas

	for _, vb := range vbsOwned {
		vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vb)

		err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, false)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}

		if vbBlob.NodeUUID != c.NodeUUID() || vbBlob.DCPStreamStatus != dcpStreamRunning || vbBlob.AssignedWorker != c.ConsumerName() {
			entry := OwnershipEntry{
				AssignedWorker: c.ConsumerName(),
				CurrentVBOwner: c.HostPortAddr(),
				Operation:      metadataCorrected,
				Timestamp:      time.Now().String(),
			}

			err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, metadataCorrectionCallback, c, vbKey, &entry)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			logging.Infof("%s [%s:%s:%d] vb: %d Checked and updated metadata", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		}
	}
}

func (c *Consumer) vbsStateUpdate() {
	logPrefix := "Consumer::vbsStateUpdate"

	c.vbsStateUpdateRunning = true
	logging.Infof("%s [%s:%s:%d] Updated vbsStateUpdateRunning to %t",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.vbsStateUpdateRunning)

	defer func() {
		c.vbsStateUpdateRunning = false
		logging.Infof("%s [%s:%s:%d] Updated vbsStateUpdateRunning to %t",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.vbsStateUpdateRunning)
	}()

	c.vbsRemainingToGiveUp = c.getVbRemainingToGiveUp()
	c.vbsRemainingToOwn = c.getVbRemainingToOwn()

	if len(c.vbsRemainingToGiveUp) == 0 && len(c.vbsRemainingToOwn) == 0 {
		// reset the flag
		c.isRebalanceOngoing = false

		logging.Infof("%s [%s:%s:%d] Updated isRebalanceOngoing to %t",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), c.isRebalanceOngoing)
		return
	}

	vbsts := c.vbProcessingStats.copyVbStats(uint16(c.numVbuckets))

	var giveupWg sync.WaitGroup
	giveupWg.Add(1)
	go c.vbGiveUpRoutine(vbsts, &giveupWg)

	vbsOwned := c.getCurrentlyOwnedVbs()
	sort.Sort(util.Uint16Slice(vbsOwned))

	logging.Infof("%s [%s:%s:%d] Before vbTakeover, vbsRemainingToOwn => %v vbRemainingToGiveUp => %v Owned len: %d dump: %v",
		logPrefix, c.workerName, c.tcpPort, c.Pid(),
		util.Condense(c.vbsRemainingToOwn), util.Condense(c.vbsRemainingToGiveUp),
		len(vbsOwned), util.Condense(vbsOwned))

	go c.checkAndUpdateMetadataLoop()

retryStreamUpdate:
	vbsDistribution := util.VbucketDistribution(c.vbsRemainingToOwn, c.vbOwnershipTakeoverRoutineCount)

	for k, v := range vbsDistribution {
		logging.Infof("%s [%s:%s:%d] vb takeover routine id: %d, vbs assigned len: %d dump: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), k, len(v), util.Condense(v))
	}

	var wg sync.WaitGroup
	wg.Add(c.vbOwnershipTakeoverRoutineCount)

	for i := 0; i < c.vbOwnershipTakeoverRoutineCount; i++ {
		go func(c *Consumer, i int, vbsRemainingToOwn []uint16, wg *sync.WaitGroup) {

			defer wg.Done()
			for _, vb := range vbsRemainingToOwn {
				if c.dcpFeedsClosed {
					logging.Infof("%s [%s:takeover_r_%d:%s:%d] Exiting vb ownership takeover routine, as dcpFeeds are closed",
						logPrefix, c.workerName, i, c.tcpPort, c.Pid())
					return
				}

				select {
				case <-c.stopVbOwnerTakeoverCh:
					logging.Infof("%s [%s:takeover_r_%d:%s:%d] Exiting vb ownership takeover routine, next vb: %d",
						logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb)
					return
				default:
				}

				c.inflightDcpStreamsRWMutex.RLock()
				if _, ok := c.inflightDcpStreams[vb]; ok {
					logging.Infof("%s [%s:takeover_r_%d:%s:%d] vb: %d skipping vbTakeover as dcp request stream already in flight",
						logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb)
					c.inflightDcpStreamsRWMutex.RUnlock()
					continue
				}
				c.inflightDcpStreamsRWMutex.RUnlock()

				err := util.Retry(util.NewFixedBackoff(vbTakeoverRetryInterval), c.retryCount, vbTakeoverCallback, c, vb)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return
				}
			}

		}(c, i, vbsDistribution[i], &wg)
	}

	wg.Wait()

	c.stopVbOwnerTakeoverCh = make(chan struct{}, c.vbOwnershipTakeoverRoutineCount)

	if c.isRebalanceOngoing {
		c.vbsRemainingToOwn = c.getVbRemainingToOwn()
		vbsRemainingToGiveUp := c.getVbRemainingToGiveUp()

		logging.Tracef("%s [%s:%s:%d] Post vbTakeover job execution, vbsRemainingToOwn => %v vbRemainingToGiveUp => %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(),
			util.Condense(c.vbsRemainingToOwn), util.Condense(vbsRemainingToGiveUp))

		// Retry logic in-case previous attempt to own/start dcp stream didn't succeed
		// because some other node has already opened(or hasn't closed) the vb dcp stream
		if len(c.vbsRemainingToOwn) > 0 && !c.dcpFeedsClosed {
			time.Sleep(dcpStreamRequestRetryInterval)
			goto retryStreamUpdate
		}
	}

	giveupWg.Wait()

	// reset the flag
	c.isRebalanceOngoing = false
	logging.Infof("%s [%s:%s:%d] Updated isRebalanceOngoing to %t",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), c.isRebalanceOngoing)

	c.checkAndUpdateMetadata()
}

func (c *Consumer) doVbTakeover(vb uint16) error {
	logPrefix := "Consumer::doVbTakeover"

	if !c.isRebalanceOngoing {
		logging.Infof("%s [%s:%s:%d] vb: %d Skipping vbTakeover as rebalance has been stopped",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		return nil
	}

	var vbBlob vbucketKVBlob
	var cas gocb.Cas

	vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vb)

	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, false)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return common.ErrRetryTimeout
	}

	switch vbBlob.DCPStreamStatus {
	case dcpStreamRunning:

		logging.Infof("%s [%s:%s:%d] vb: %d dcp stream status: %s curr owner: %rs worker: %v UUID consumer: %s from metadata: %s check if current node should own vb: %t",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus,
			vbBlob.CurrentVBOwner, vbBlob.AssignedWorker, c.NodeUUID(),
			vbBlob.NodeUUID, c.checkIfCurrentNodeShouldOwnVb(vb))

		if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() {
			logging.Infof("%s [%s:%s:%d] vb: %d current consumer and eventing node has already opened dcp stream. Stream status: %s, skipping",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus)
			return nil
		}

		if c.NodeUUID() != vbBlob.NodeUUID &&
			!c.producer.IsEventingNodeAlive(vbBlob.CurrentVBOwner, vbBlob.NodeUUID) && c.checkIfCurrentNodeShouldOwnVb(vb) {

			if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker != c.ConsumerName() {
				return errVbOwnedByAnotherWorker
			}

			logging.Infof("%s [%s:%s:%d] Node: %rs taking ownership of vb: %d old node: %rs isn't alive any more as per ns_server vbuuid: %s vblob.uuid: %s",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), c.HostPortAddr(), vb, vbBlob.CurrentVBOwner,
				c.NodeUUID(), vbBlob.NodeUUID)

			if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() {

				logging.Infof("%s [%s:%s:%d] vb: %d vbblob stream status: %v starting dcp stream",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus)

				return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob, true)
			}
			return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob, true)
		}

		if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker != c.ConsumerName() {
			logging.Infof("%s [%s:%s:%d] vb: %d owned by another worker: %s on same node",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.AssignedWorker)
			return errVbOwnedByAnotherWorker
		}

		logging.Infof("%s [%s:%s:%d] vb: %d owned by node: %s worker: %s",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.CurrentVBOwner, vbBlob.AssignedWorker)
		return errVbOwnedByAnotherNode

	case dcpStreamStopped, dcpStreamUninitialised:

		if vbBlob.DCPStreamRequested {
			if (vbBlob.NodeUUIDRequestedVbStream == c.NodeUUID() && vbBlob.WorkerRequestedVbStream == c.ConsumerName()) ||
				(vbBlob.NodeUUIDRequestedVbStream == "" && vbBlob.WorkerRequestedVbStream == "") {
				return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob, false)
			}

			logging.Infof("%s [%s:%s:%d] vb: %d. STREAMREQ already issued by hostPort: %s worker: %s uuid: %s",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.NodeRequestedVbStream,
				vbBlob.WorkerRequestedVbStream, vbBlob.NodeUUIDRequestedVbStream)
			return errDcpStreamRequested
		}

		logging.Infof("%s [%s:%s:%d] vb: %d vbblob stream status: %s, starting dcp stream",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus)

		return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob, true)

	default:
		return errUnexpectedVbStreamStatus
	}

	return nil
}

func (c *Consumer) checkIfCurrentNodeShouldOwnVb(vb uint16) bool {
	c.vbEventingNodeAssignRWMutex.RLock()
	defer c.vbEventingNodeAssignRWMutex.RUnlock()

	return c.vbEventingNodeAssignMap[vb] == c.HostPortAddr()
}

func (c *Consumer) checkIfCurrentConsumerShouldOwnVb(vb uint16) bool {
	c.workerVbucketMapRWMutex.Lock()
	defer c.workerVbucketMapRWMutex.Unlock()

	for _, v := range c.workerVbucketMap[c.workerName] {
		if vb == v {
			return true
		}
	}
	return false
}

func (c *Consumer) updateVbOwnerAndStartDCPStream(vbKey string, vb uint16, vbBlob *vbucketKVBlob, backfillTimers bool) error {
	logPrefix := "Consumer::updateVbOwnerAndStartDCPStream"

	if c.checkIfVbAlreadyOwnedByCurrConsumer(vb) {
		logging.Infof("%s [%s:%s:%d] vb: %v already owned by Eventing.Consumer, skipping",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		return nil
	}

	if backfillTimers {
		seqNos, err := util.BucketSeqnos(c.producer.NsServerHostPort(), "default", c.bucket)
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Failed to fetch get_all_vb_seqnos, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
			return err
		}

		logging.Infof("%s [%s:%s:%d] vb: %v LastProcessedSeqNo: %d LastDocTimerFeedbackSeqNo: %d high seq no: %d",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.LastSeqNoProcessed, vbBlob.LastDocTimerFeedbackSeqNo, seqNos[int(vb)])

		err = util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getKvNodesFromVbMap, c)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return common.ErrRetryTimeout
		}

		var b *couchbase.Bucket
		var dcpFeed *couchbase.DcpFeed

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, commonConnectBucketOpCallback, c, &b)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return common.ErrRetryTimeout
		}

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, startFeedFromKVNodesCallback, c, &b, vb, &dcpFeed, c.getKvNodes())
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return common.ErrRetryTimeout
		}

		var wg sync.WaitGroup
		wg.Add(1)

		var receivedTillEndSeqNo bool

		go func(dcpFeed *couchbase.DcpFeed, wg *sync.WaitGroup, endSeqNo uint64, receivedTillEndSeqNo *bool) {
			defer wg.Done()

			statsTicker := time.NewTicker(c.statsTickDuration)
			dcpMessagesProcessed := make(map[mcd.CommandCode]uint64)

			defer statsTicker.Stop()
			var seqNoReceived uint64

			for {
				select {
				case e, ok := <-dcpFeed.C:
					if ok == false {
						logging.Infof("%s [%s:%s:%d] vb: %d Exiting doc timer recreate routine",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
						return
					}

					if _, ok := dcpMessagesProcessed[e.Opcode]; !ok {
						dcpMessagesProcessed[e.Opcode] = 0
					}
					dcpMessagesProcessed[e.Opcode]++

					if e.Seqno > seqNoReceived {
						seqNoReceived = e.Seqno
					}

					logging.Tracef("%s [%s:%s:%d] vb: %v Opcode received: %v key: %ru datatype: %v seq no: %d",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, e.Opcode, string(e.Key), e.Datatype, e.Seqno)

					switch e.Opcode {
					case mcd.DCP_STREAMEND:
						logging.Infof("%s [%s:%s:%d] vb: %v Stream end has been received. LastSeqNoReceived: %d endSeqNo: %d",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, seqNoReceived, endSeqNo)

						if seqNoReceived >= endSeqNo {
							*receivedTillEndSeqNo = true
						} else {
							logging.Errorf("%s [%s:%s:%d] vb: %v Received events till end seq no: %d desired: %d",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, seqNoReceived, endSeqNo)
						}

					case mcd.DCP_MUTATION:
						switch e.Datatype {
						case dcpDatatypeJSONXattr:
							totalXattrLen := binary.BigEndian.Uint32(e.Value[0:])
							totalXattrData := e.Value[4 : 4+totalXattrLen-1]

							logging.Tracef("%s [%s:%s:%d] key: %ru totalXattrLen: %v totalXattrData: %v",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key), totalXattrLen, totalXattrData)

							var xMeta xattrMetadata
							var bytesDecoded uint32

							// Try decoding all xattrs defined in io-vector encoding format
							for bytesDecoded < totalXattrLen {
								frameLength := binary.BigEndian.Uint32(totalXattrData)
								bytesDecoded += 4
								frameData := totalXattrData[4 : 4+frameLength-1]
								bytesDecoded += frameLength
								if bytesDecoded < totalXattrLen {
									totalXattrData = totalXattrData[4+frameLength:]
								}

								if len(frameData) > len(xattrPrefix) {
									if bytes.Compare(frameData[:len(xattrPrefix)], []byte(xattrPrefix)) == 0 {
										toParse := frameData[len(xattrPrefix)+1:]

										err := json.Unmarshal(toParse, &xMeta)
										if err != nil {
											logging.Errorf("%s [%s:%s:%d] Failed to unmarshal xattr metadata, err: %v",
												logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
											continue
										}
									}
								}
							}

							for _, timerEntry := range xMeta.Timers {

								data := strings.Split(timerEntry, "::")

								if len(data) == 3 {
									pEntry := &plasmaStoreEntry{
										callbackFn:   data[2],
										fromBackfill: true,
										key:          string(e.Key),
										timerTs:      data[1],
										vb:           e.VBucket,
									}

									c.plasmaStoreCh <- pEntry
									counter := c.vbProcessingStats.getVbStat(e.VBucket, "timers_recreated_from_dcp_backfill").(uint64)
									c.vbProcessingStats.updateVbStat(e.VBucket, "timers_recreated_from_dcp_backfill", counter+1)

									c.timersRecreatedFromDCPBackfill++
								}
							}

							logging.Tracef("%s [%s:%s:%d] Inserting doc timer key: %ru into plasma",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key))

						default:
						}

					default:
						logging.Tracef("%s [%s:%s:%d] vb: %v Got opcode: %v",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, e.Opcode)
					}

					if seqNoReceived >= endSeqNo {
						logging.Infof("%s [%s:%s:%d] vb: %v Exiting as LastSeqNoReceived: %d endSeqNo: %d",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, seqNoReceived, endSeqNo)

						*receivedTillEndSeqNo = true
					}

				case <-statsTicker.C:
					countMsg, _, _ := util.SprintDCPCounts(dcpMessagesProcessed)
					logging.Infof("%s [%s:%s:%d] vb: %d seqNoReceived: %d DCP events: %s",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, seqNoReceived, countMsg)
				}
			}
		}(dcpFeed, &wg, seqNos[vb], &receivedTillEndSeqNo)

		var flogs couchbase.FailoverLog

		// TODO: Can be improved by requesting failover log just for the vbucket for which stream is going to be requested
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getFailoverLogOpAllVbucketsCallback, c, b, &flogs, vb)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return common.ErrRetryTimeout
		}

		start, snapStart, snapEnd := uint64(0), uint64(0), seqNos[int(vb)]
		flags := uint32(0)
		end := seqNos[int(vb)]

		logging.Infof("%s [%s:%s:%d] vb: %v Going to start backfill DCP feed from source bucket: %s start seq no: %d end seq no: %d KV nodes: %rs flog len: %d",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, c.bucket, start, end, c.getKvNodes(), len(flogs))

		if flog, ok := flogs[vb]; ok {
			vbuuid, _, _ := flog.Latest()

			logging.Infof("%s [%s:%s:%d] vb: %v starting DCP feed for timer backfill. Start seq no: %d end seq no: %d",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, start, end)

			opaque := uint16(vb)
			err := dcpFeed.DcpRequestStream(vb, opaque, flags, vbuuid, start, end, snapStart, snapEnd)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] vb: %v Failed to request stream for recreating doc timers, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)

				dcpFeed.Close()
				return err
			}
		} else {
			logging.Errorf("%s [%s:%s:%d] vb: %v Failover log doesn't have entry for it",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		}

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, checkIfReceivedTillEndSeqNoCallback, c, vb, &receivedTillEndSeqNo, dcpFeed)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return common.ErrRetryTimeout
		}

		wg.Wait()

		if !receivedTillEndSeqNo {
			return fmt.Errorf("not received doc timer events till desired end seq no")
		}
	}

	c.vbProcessingStats.updateVbStat(vb, "last_processed_seq_no", vbBlob.LastSeqNoProcessed)
	c.vbProcessingStats.updateVbStat(vb, "last_doc_timer_feedback_seqno", vbBlob.LastDocTimerFeedbackSeqNo)
	c.vbProcessingStats.updateVbStat(vb, "start_seq_no", vbBlob.LastSeqNoProcessed)
	c.vbProcessingStats.updateVbStat(vb, "timestamp", time.Now().Format(time.RFC3339))

	logging.Infof("%s [%s:%s:%d] vb: %d Sending streamRequestInfo size: %d",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, len(c.reqStreamCh))

	c.reqStreamCh <- &streamRequestInfo{
		vb:         vb,
		vbBlob:     vbBlob,
		startSeqNo: vbBlob.LastSeqNoProcessed,
	}

	return nil
}

func (c *Consumer) updateCheckpoint(vbKey string, vb uint16, vbBlob *vbucketKVBlob) error {
	logPrefix := "Consumer::updateCheckpoint"

	vbBlob.AssignedWorker = ""
	vbBlob.CurrentVBOwner = ""
	vbBlob.DCPStreamStatus = dcpStreamStopped
	vbBlob.NodeUUID = ""
	vbBlob.PreviousAssignedWorker = c.ConsumerName()
	vbBlob.PreviousNodeUUID = c.NodeUUID()
	vbBlob.PreviousVBOwner = c.HostPortAddr()

	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, updateCheckpointCallback, c, vbKey, vbBlob)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return common.ErrRetryTimeout
	}

	c.vbProcessingStats.updateVbStat(vb, "assigned_worker", vbBlob.AssignedWorker)
	c.vbProcessingStats.updateVbStat(vb, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vb, "dcp_stream_status", vbBlob.DCPStreamStatus)
	c.vbProcessingStats.updateVbStat(vb, "node_uuid", vbBlob.NodeUUID)

	logging.Tracef("%s [%s:%s:%d] vb: %v Stopped dcp stream, updated checkpoint blob in bucket",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
	return nil
}

func (c *Consumer) checkIfConsumerShouldOwnVb(vb uint16, workerName string) bool {
	c.workerVbucketMapRWMutex.RLock()
	defer c.workerVbucketMapRWMutex.RUnlock()

	for _, v := range c.workerVbucketMap[workerName] {
		if vb == v {
			return true
		}
	}
	return false
}

func (c *Consumer) getConsumerForGivenVbucket(vb uint16) string {
	c.workerVbucketMapRWMutex.RLock()
	defer c.workerVbucketMapRWMutex.RUnlock()

	for workerName, vbs := range c.workerVbucketMap {
		for _, v := range vbs {
			if vb == v {
				return workerName
			}
		}
	}
	return ""
}

func (c *Consumer) checkIfVbAlreadyOwnedByCurrConsumer(vb uint16) bool {
	if c.vbProcessingStats.getVbStat(vb, "node_uuid") == c.uuid &&
		c.vbProcessingStats.getVbStat(vb, "assigned_worker") == c.ConsumerName() &&
		c.vbProcessingStats.getVbStat(vb, "dcp_stream_status") == dcpStreamRunning {
		return true
	}

	return false
}

func (c *Consumer) getVbRemainingToOwn() []uint16 {
	c.vbEventingNodeAssignRWMutex.RLock()
	defer c.vbEventingNodeAssignRWMutex.RUnlock()

	var vbsRemainingToOwn []uint16

	for vb := range c.vbEventingNodeAssignMap {

		if (c.vbProcessingStats.getVbStat(vb, "node_uuid") != c.NodeUUID() ||
			c.vbProcessingStats.getVbStat(vb, "assigned_worker") != c.ConsumerName()) &&
			c.checkIfCurrentConsumerShouldOwnVb(vb) {

			vbsRemainingToOwn = append(vbsRemainingToOwn, vb)
		}
	}

	sort.Sort(util.Uint16Slice(vbsRemainingToOwn))

	return vbsRemainingToOwn
}

// Returns the list of vbs that a given consumer should own as per the producer's plan
func (c *Consumer) getVbsOwned() []uint16 {
	c.vbEventingNodeAssignRWMutex.RLock()
	defer c.vbEventingNodeAssignRWMutex.RUnlock()

	var vbsOwned []uint16

	for vb, v := range c.vbEventingNodeAssignMap {
		if v == c.HostPortAddr() && c.checkIfCurrentNodeShouldOwnVb(vb) &&
			c.checkIfConsumerShouldOwnVb(vb, c.ConsumerName()) {

			vbsOwned = append(vbsOwned, vb)
		}
	}

	sort.Sort(util.Uint16Slice(vbsOwned))
	return vbsOwned
}

func (c *Consumer) getVbRemainingToGiveUp() []uint16 {
	var vbsRemainingToGiveUp []uint16

	for vb := range c.vbProcessingStats {
		if c.ConsumerName() == c.vbProcessingStats.getVbStat(vb, "assigned_worker") &&
			!c.checkIfCurrentConsumerShouldOwnVb(vb) {
			vbsRemainingToGiveUp = append(vbsRemainingToGiveUp, vb)
		}
	}

	sort.Sort(util.Uint16Slice(vbsRemainingToGiveUp))

	return vbsRemainingToGiveUp
}

func (c *Consumer) verifyVbsCurrentlyOwned(vbsToMigrate []uint16) []uint16 {
	var vbsCurrentlyOwned []uint16

	for _, vb := range vbsToMigrate {
		if c.HostPortAddr() == c.vbProcessingStats.getVbStat(vb, "current_vb_owner") &&
			c.ConsumerName() == c.vbProcessingStats.getVbStat(vb, "assigned_worker") {
			vbsCurrentlyOwned = append(vbsCurrentlyOwned, vb)
		}
	}

	return vbsCurrentlyOwned
}

func (c *Consumer) vbsToHandle() []uint16 {
	c.workerVbucketMapRWMutex.RLock()
	defer c.workerVbucketMapRWMutex.RUnlock()

	return c.workerVbucketMap[c.ConsumerName()]
}

func (c *Consumer) doCleanupForPreviouslyOwnedVbs() error {
	logPrefix := "Consumer::doCleanupForPreviouslyOwnedVbs"

	vbuckets := make([]uint16, 0)
	for vb := 0; vb < c.numVbuckets; vb++ {
		vbuckets = append(vbuckets, uint16(vb))
	}

	vbsNotSupposedToOwn := util.Uint16SliceDiff(vbuckets, c.vbnos)

	logging.Infof("%s [%s:%s:%d] vbsNotSupposedToOwn len: %d dump: %s",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), len(vbsNotSupposedToOwn), util.Condense(vbsNotSupposedToOwn))

	for _, vb := range vbsNotSupposedToOwn {

		vbKey := fmt.Sprintf("%s::vb::%v", c.app.AppName, vb)

		var vbBlob vbucketKVBlob
		var cas gocb.Cas

		err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, false)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return common.ErrRetryTimeout
		}

		if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() && vbBlob.DCPStreamStatus == dcpStreamRunning {
			err = c.updateCheckpoint(vbKey, vb, &vbBlob)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return common.ErrRetryTimeout
			}
			logging.Infof("%s [%s:%s:%d] vb: %v Cleaned up ownership", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		}
	}

	return nil
}
