package consumer

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
)

var (
	errDcpFeedsClosed           = errors.New("dcp feeds are closed")
	errDcpStreamRequested       = errors.New("another worker issued STREAMREQ")
	errUnexpectedVbStreamStatus = errors.New("unexpected vbucket stream status")
	errVbOwnedByAnotherWorker   = errors.New("vbucket is owned by another worker on same node")
	errVbOwnedByAnotherNode     = errors.New("vbucket is owned by another node")
)

func (c *Consumer) checkAndUpdateMetadata() {
	logPrefix := "Consumer::checkAndUpdateMetadata"
	vbsOwned := c.getCurrentlyOwnedVbs()

	var vbBlob vbucketKVBlob
	var cas gocb.Cas

	for _, vb := range vbsOwned {
		if !c.checkIfCurrentConsumerShouldOwnVb(vb) {
			continue
		}

		vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vb)

		err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
			c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, false)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}

		if vbBlob.NodeUUID != c.NodeUUID() || vbBlob.DCPStreamStatus != dcpStreamRunning || vbBlob.AssignedWorker != c.ConsumerName() {
			lastSeqNo := c.vbProcessingStats.getVbStat(vb, "last_read_seq_no").(uint64)

			entry := OwnershipEntry{
				AssignedWorker: c.ConsumerName(),
				CurrentVBOwner: c.HostPortAddr(),
				Operation:      metadataCorrected,
				SeqNo:          lastSeqNo,
				Timestamp:      time.Now().String(),
			}

			err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, metadataCorrectionCallback,
				c, c.producer.AddMetadataPrefix(vbKey), &entry)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			logging.Infof("%s [%s:%s:%d] vb: %d Checked and updated metadata", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

			if !c.checkIfCurrentConsumerShouldOwnVb(vb) {
				lastSeqNo := c.vbProcessingStats.getVbStat(vb, "last_read_seq_no").(uint64)

				entry := OwnershipEntry{
					AssignedWorker: c.ConsumerName(),
					CurrentVBOwner: c.HostPortAddr(),
					Operation:      undoMetadataCorrection,
					SeqNo:          lastSeqNo,
					Timestamp:      time.Now().String(),
				}

				err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, undoMetadataCorrectionCallback,
					c, c.producer.AddMetadataPrefix(vbKey), &entry)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return
				}

				logging.Infof("%s [%s:%s:%d] vb: %d Reverted metadata correction", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
			}

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

	vbsOwned := c.getCurrentlyOwnedVbs()
	sort.Sort(util.Uint16Slice(vbsOwned))

	logging.Infof("%s [%s:%s:%d] Before vbTakeover, vbsRemainingToOwn => %v vbRemainingToGiveUp => %v Owned len: %d dump: %v",
		logPrefix, c.workerName, c.tcpPort, c.Pid(),
		util.Condense(c.vbsRemainingToOwn), util.Condense(c.vbsRemainingToGiveUp),
		len(vbsOwned), util.Condense(vbsOwned))

retryStreamUpdate:
	vbsDistribution := util.VbucketDistribution(c.vbsRemainingToOwn, c.vbOwnershipTakeoverRoutineCount)

	for k, v := range vbsDistribution {
		logging.Tracef("%s [%s:%s:%d] vb takeover routine id: %d, vbs assigned len: %d dump: %v",
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
				case _, ok := <-c.stopVbOwnerTakeoverCh:
					if ok == false {
						logging.Infof("%s [%s:takeover_r_%d:%s:%d] Exiting vb ownership takeover routine, next vb: %d",
							logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb)
						return
					}
				default:
				}

				c.inflightDcpStreamsRWMutex.RLock()
				if _, ok := c.inflightDcpStreams[vb]; ok {
					logging.Tracef("%s [%s:takeover_r_%d:%s:%d] vb: %d skipping vbTakeover as dcp request stream already in flight",
						logPrefix, c.workerName, i, c.tcpPort, c.Pid(), vb)
					c.inflightDcpStreamsRWMutex.RUnlock()
					continue
				}
				c.inflightDcpStreamsRWMutex.RUnlock()

				if c.checkIfAlreadyEnqueued(vb) {
					continue
				}

				if !c.checkIfCurrentConsumerShouldOwnVb(vb) {
					continue
				}

				err := util.Retry(util.NewFixedBackoff(vbTakeoverRetryInterval), c.retryCount, vbTakeoverCallback, c, vb)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return
				}
			}

		}(c, i, vbsDistribution[i], &wg)
	}

	wg.Wait()

	c.stopVbOwnerTakeoverCh = make(chan struct{})

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
		c.deleteFromEnqueueMap(vb)
		return nil
	}

	var vbBlob vbucketKVBlob
	var cas gocb.Cas
	var isNoEnt bool

	vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vb)

	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
		c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, true, &isNoEnt, true)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	var possibleConsumers []string
	for i := 0; i < c.workerCount; i++ {
		possibleConsumers = append(possibleConsumers, fmt.Sprintf("worker_%s_%d", c.app.AppName, i))
	}

	switch vbBlob.DCPStreamStatus {
	case dcpStreamRunning:

		logging.Infof("%s [%s:%s:%d] vb: %d dcp stream status: %s curr owner: %rs worker: %v UUID consumer: %s from metadata: %s check if current node should own vb: %t",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus,
			vbBlob.CurrentVBOwner, vbBlob.AssignedWorker, c.NodeUUID(),
			vbBlob.NodeUUID, c.checkIfCurrentNodeShouldOwnVb(vb))

		if vbBlob.NodeUUID != c.NodeUUID() {
			// Case 1a: Some node that isn't part of the cluster has spawned DCP stream for the vbucket.
			//         Hence start the connection from consumer, discarding previous state.
			if !c.producer.IsEventingNodeAlive(vbBlob.CurrentVBOwner, vbBlob.NodeUUID) && c.checkIfCurrentNodeShouldOwnVb(vb) {
				logging.Infof("%s [%s:%s:%d] vb: %d node: %rs taking ownership. Old node: %rs isn't alive any more as per ns_server vbuuid: %s vblob.uuid: %s",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, c.HostPortAddr(), vbBlob.CurrentVBOwner,
					c.NodeUUID(), vbBlob.NodeUUID)
				return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob)
			}

			// Case 1b: Invalid worker on another node is owning up vbucket stream
			if !util.Contains(vbBlob.AssignedWorker, possibleConsumers) {
				return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob)
			}

			// Case 1c: Invalid node uuid is marked as owner of the vbucket
			if !util.Contains(vbBlob.NodeUUID, c.eventingNodeUUIDs) && !util.Contains(vbBlob.NodeUUID, c.ejectNodesUUIDs) {
				return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob)
			}
		}

		if vbBlob.NodeUUID == c.NodeUUID() {
			// Case 2a: Current consumer has already spawned DCP stream for the vbucket
			if vbBlob.AssignedWorker == c.ConsumerName() {
				logging.Infof("%s [%s:%s:%d] vb: %d current consumer and eventing node has already opened dcp stream. Stream status: %s, skipping",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus)
				return nil
			}

			logging.Infof("%s [%s:%s:%d] vb: %d owned by another worker: %s on same node",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.AssignedWorker)

			if !util.Contains(vbBlob.AssignedWorker, possibleConsumers) {
				// Case 2b: Worker who is invalid right now, has the ownership per metadata. Could happen for example:
				//         t1 - Eventing starts off with worker count 10
				//         t2 - Function was paused and resumed with worker count 3
				//         t3 - Eventing rebalance was kicked off and KV rolled back metadata bucket to t1
				//         This would currently cause rebalance to get stuck
				//         In this case, it makes sense to revoke ownership metadata of old owners.
				return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob)
			}

			// Case 2c: An existing & running consumer on current Eventing node  has owned up the vbucket
			return errVbOwnedByAnotherWorker
		}

		// Case 3: Another running Eventing node has the ownership of the vbucket stream
		logging.Infof("%s [%s:%s:%d] vb: %d owned by node: %s worker: %s",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.CurrentVBOwner, vbBlob.AssignedWorker)
		return errVbOwnedByAnotherNode

	case dcpStreamStopped, dcpStreamUninitialised:

		if vbBlob.DCPStreamRequested {
			if (vbBlob.NodeUUIDRequestedVbStream == c.NodeUUID() && vbBlob.WorkerRequestedVbStream == c.ConsumerName()) ||
				(vbBlob.NodeUUIDRequestedVbStream == "" && vbBlob.WorkerRequestedVbStream == "") {
				return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob)
			}

			if vbBlob.NodeUUIDRequestedVbStream != c.NodeUUID() &&
				!c.producer.IsEventingNodeAlive(vbBlob.NodeRequestedVbStream, vbBlob.NodeUUIDRequestedVbStream) {
				logging.Infof("%s [%s:%s:%d] vb: %d node: %rs going to open dcp stream. "+
					"Old node: %rs isn't alive any more as per ns_server vbuuid: %s node requested stream uuid: %s",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), c.HostPortAddr(), vb, vbBlob.NodeRequestedVbStream,
					c.NodeUUID(), vbBlob.NodeUUIDRequestedVbStream)
				return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob)
			}

			logging.Infof("%s [%s:%s:%d] vb: %d. STREAMREQ already issued by hostPort: %s worker: %s uuid: %s",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.NodeRequestedVbStream,
				vbBlob.WorkerRequestedVbStream, vbBlob.NodeUUIDRequestedVbStream)
			return errDcpStreamRequested
		}

		logging.Infof("%s [%s:%s:%d] vb: %d vbblob stream status: %s, starting dcp stream",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.DCPStreamStatus)

		return c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob)

	default:
		return errUnexpectedVbStreamStatus
	}
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

func (c *Consumer) updateVbOwnerAndStartDCPStream(vbKey string, vb uint16, vbBlob *vbucketKVBlob) error {
	logPrefix := "Consumer::updateVbOwnerAndStartDCPStream"

	if c.checkIfVbAlreadyOwnedByCurrConsumer(vb) {
		logging.Infof("%s [%s:%s:%d] vb: %v already owned by Eventing.Consumer, skipping",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		return nil
	}

	if c.checkIfAlreadyEnqueued(vb) {
		return nil
	}
	c.addToEnqueueMap(vb)

	c.vbProcessingStats.updateVbStat(vb, "last_doc_timer_feedback_seqno", vbBlob.LastDocTimerFeedbackSeqNo)
	c.vbProcessingStats.updateVbStat(vb, "last_processed_seq_no", vbBlob.LastSeqNoProcessed)
	c.vbProcessingStats.updateVbStat(vb, "last_read_seq_no", vbBlob.LastSeqNoProcessed)
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

	if c.resetBootstrapDone {
		logging.Infof("%s [%s:%s:%d] vb: %d current BootstrapStreamReqDone flag: %t",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.BootstrapStreamReqDone)
		vbBlob.BootstrapStreamReqDone = false
		logging.Infof("%s [%s:%s:%d] vb: %d updated BootstrapStreamReqDone flag to: %t",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.BootstrapStreamReqDone)
	}

	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, updateCheckpointCallback,
		c, c.producer.AddMetadataPrefix(vbKey), vbBlob)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	c.vbProcessingStats.updateVbStat(vb, "assigned_worker", vbBlob.AssignedWorker)
	c.vbProcessingStats.updateVbStat(vb, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vb, "dcp_stream_status", vbBlob.DCPStreamStatus)
	c.vbProcessingStats.updateVbStat(vb, "node_uuid", vbBlob.NodeUUID)
	c.vbProcessingStats.updateVbStat(vb, "last_processed_seq_no", vbBlob.LastSeqNoProcessed)
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

func (c *Consumer) checkIfVbAlreadyRequestedByCurrConsumer(vb uint16) bool {
	if c.vbProcessingStats.getVbStat(vb, "dcp_stream_requested_node_uuid") == c.NodeUUID() &&
		c.vbProcessingStats.getVbStat(vb, "dcp_stream_requested_worker") == c.ConsumerName() {
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

func (c *Consumer) getVbRemainingToStreamReq() []uint16 {
	c.vbEventingNodeAssignRWMutex.RLock()
	defer c.vbEventingNodeAssignRWMutex.RUnlock()

	var vbRemainingToStreamReq []uint16

	for vb := range c.vbEventingNodeAssignMap {
		if (c.vbProcessingStats.getVbStat(vb, "dcp_stream_requested_node_uuid") != c.NodeUUID() ||
			c.vbProcessingStats.getVbStat(vb, "dcp_stream_requested_worker") != c.ConsumerName()) &&
			c.checkIfCurrentConsumerShouldOwnVb(vb) {
			vbRemainingToStreamReq = append(vbRemainingToStreamReq, vb)
		}
	}

	sort.Sort(util.Uint16Slice(vbRemainingToStreamReq))

	return vbRemainingToStreamReq
}

func (c *Consumer) getVbRemainingToCloseStream() []uint16 {
	var vbsRemainingToCloseStream []uint16

	for vb := range c.vbProcessingStats {
		if c.ConsumerName() == c.vbProcessingStats.getVbStat(vb, "dcp_stream_requested_worker") &&
			!c.checkIfCurrentConsumerShouldOwnVb(vb) {
			vbsRemainingToCloseStream = append(vbsRemainingToCloseStream, vb)
		}
	}

	sort.Sort(util.Uint16Slice(vbsRemainingToCloseStream))

	return vbsRemainingToCloseStream
}

func (c *Consumer) getVbsFilterAckYetToCome() []uint16 {
	var vbsFilterAckYetToCome []uint16

	for vb := range c.vbProcessingStats {
		if !c.vbProcessingStats.getVbStat(vb, "vb_filter_ack_received").(bool) {
			vbsFilterAckYetToCome = append(vbsFilterAckYetToCome, vb)
		}
	}
	sort.Sort(util.Uint16Slice(vbsFilterAckYetToCome))

	return vbsFilterAckYetToCome
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
		err := c.cleanupVbMetadata(vb)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}
	}

	return nil
}

func (c *Consumer) cleanupVbMetadata(vb uint16) error {
	logPrefix := "Consumer::cleanupVbMetadata"

	vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vb)

	var vbBlob vbucketKVBlob
	var cas gocb.Cas

	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
		c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, false)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() && vbBlob.DCPStreamStatus == dcpStreamRunning {
		err = c.updateCheckpoint(vbKey, vb, &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}
		logging.Infof("%s [%s:%s:%d] vb: %d cleaned up ownership", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
	}

	return nil
}

// CloseAllRunningDcpFeeds drops all socket connections to DCP producer
func (c *Consumer) CloseAllRunningDcpFeeds() {
	logPrefix := "Consumer::CloseAllRunningDcpFeeds"

	runningDcpFeeds := make([]*couchbase.DcpFeed, 0)

	c.hostDcpFeedRWMutex.RLock()
	for _, dcpFeed := range c.kvHostDcpFeedMap {
		runningDcpFeeds = append(runningDcpFeeds, dcpFeed)
	}
	c.hostDcpFeedRWMutex.RUnlock()

	c.streamReqRWMutex.Lock()
	defer c.streamReqRWMutex.Unlock()

	logging.Infof("%s [%s:%s:%d] Going to close all active dcp feeds. Active feed count: %d",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), len(runningDcpFeeds))

	for _, dcpFeed := range runningDcpFeeds {
		func() {
			c.cbBucketRWMutex.Lock()
			defer c.cbBucketRWMutex.Unlock()

			err := dcpFeed.Close()
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] DCP feed: %s failed to close connection, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), dcpFeed.GetName(), err)
			} else {
				logging.Infof("%s [%s:%s:%d] DCP feed: %s closed connection",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), dcpFeed.GetName())
			}
		}()
	}

	for vb := range c.vbProcessingStats {
		c.vbProcessingStats.updateVbStat(vb, "dcp_stream_requested", false)
		c.vbProcessingStats.updateVbStat(vb, "dcp_stream_requested_worker", "")
		c.vbProcessingStats.updateVbStat(vb, "dcp_stream_requested_node_uuid", "")
	}

	logging.Infof("%s [%s:%s:%d] Finished reset of vb related stats",
		logPrefix, c.workerName, c.tcpPort, c.Pid())
}
