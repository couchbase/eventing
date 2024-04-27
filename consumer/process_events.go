package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
	couchbase "github.com/couchbase/eventing/dcp"
	mcd "github.com/couchbase/eventing/dcp/transport"
	cb "github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb/v2"
)

func (c *Consumer) processDCPEvents() {
	logPrefix := "Consumer::processDCPEvents"

	functionInstanceID := strconv.Itoa(int(c.app.FunctionID)) + "-" + c.app.FunctionInstanceID

	for {
		if c.cppQueueSizes != nil {
			if c.workerQueueCap < (c.numSentEvents-c.cppQueueSizes.NumProcessedEvents) ||
				c.workerQueueMemCap < (c.sentEventsSize-c.cppQueueSizes.ProcessedEventsSize) {
				logging.Debugf("%s [%s:%s:%d] Throttling, cpp queue sizes: %+v, num sent event: %d, events size: %d",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), c.cppQueueSizes, c.numSentEvents, c.sentEventsSize)

				// avoid throttling when consumer is pausing or terminating
				if !(c.isPausing || atomic.LoadUint32(&c.isTerminateRunning) == 1) {
					time.Sleep(10 * time.Millisecond)
				}

				// If rebalance in ongoing, it's important to read dcp mutations as STREAMBEGIN/END messages could be behind them.
				// And it is also important to not queue up mutations in consumer to contain rss growth when cpp queues are full.
				// So *continue* only when there is no rebalance on going, or we are not pausing or we are not undeploying
				if !(c.isRebalanceOngoing || c.isPausing || atomic.LoadUint32(&c.isTerminateRunning) == 1) {
					continue
				}

			}
		}

		if len(c.reqStreamCh) > 0 || len(c.clusterStateChangeNotifCh) > 0 {
			logging.Debugf("%s [%s:%s:%d] Throttling, len(c.reqStreamCh): %v, len(c.clusterStateChangeNotifCh): %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), len(c.reqStreamCh), len(c.clusterStateChangeNotifCh))
			runtime.Gosched()
		}

		select {
		case e, ok := <-c.aggDCPFeed:
			if ok == false {
				logging.Infof("%s [%s:%s:%d] Closing DCP feed for bucket %q",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), c.sourceKeyspace.BucketName)
				return
			}

			atomic.AddInt64(&c.aggDCPFeedMem, -int64(len(e.Value)))
			c.updateDcpProcessedMsgs(e.Opcode)

			switch e.Opcode {
			case mcd.DCP_MUTATION:
				if c.filterMutations(e) {
					continue
				}

				logging.Tracef("%s [%s:%s:%d] Got DCP_MUTATION for key: %ru datatype: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key), e.Datatype)

				if !c.allowSyncDocuments && c.isSGWMutation(e) {
					c.suppressedDCPMutationCounter++
					continue
				}

				switch e.Datatype {
				case dcpDatatypeJSON:
					c.dcpMutationCounter++
					c.sendEvent(e)

				case dcpDatatypeBinary:
					if c.binaryDocAllowed {
						c.dcpMutationCounter++
						c.sendEvent(e)
					}

				case dcpDatatypeJSONXattr:
					c.sendXattrDoc(e, functionInstanceID)

				case dcpDatatypeBinXattr:
					if c.binaryDocAllowed {
						if !c.allowTransactionMutations && c.isTransactionMutation(e) {
							c.suppressedDCPMutationCounter++
							continue
						}
						c.sendXattrDoc(e, functionInstanceID)
					}

				}

			case mcd.DCP_DELETION:
				if c.filterMutations(e) {
					continue
				}

				logging.Tracef("%s [%s:%s:%d] Got DCP_DELETION for key: %ru datatype: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key), e.Datatype)

				if !c.allowSyncDocuments && c.isSGWMutation(e) {
					c.suppressedDCPDeletionCounter++
					continue
				}

				if c.processAndSendDcpDelOrExpMessage(e, functionInstanceID, true) {
					c.dcpDeletionCounter++
				} else {
					c.suppressedDCPDeletionCounter++
				}

			case mcd.DCP_EXPIRATION:
				if c.filterMutations(e) {
					continue
				}

				logging.Tracef("%s [%s:%s:%d] Got DCP_EXPIRATION for key: %ru datatype: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key), e.Datatype)

				if !c.allowSyncDocuments && c.isSGWMutation(e) {
					c.suppressedDCPExpirationCounter++
					continue
				}

				if c.processAndSendDcpDelOrExpMessage(e, functionInstanceID, false) {
					c.dcpExpiryCounter++
				} else {
					c.suppressedDCPExpirationCounter++
				}

			case mcd.DCP_STREAMEND:
				c.dcpStatsLogger.AddDcpLog(e.VBucket, LogState, string(StateStreamEnd))
				logging.Debugf("%s [%s:%s:%d] vb: %d got STREAMEND", logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket)
				c.vbProcessingStats.updateVbStat(e.VBucket, "vb_stream_request_metadata_updated", false)
				lastReadSeqNo := c.vbProcessingStats.getVbStat(e.VBucket, "last_read_seq_no").(uint64)
				c.vbProcessingStats.updateVbStat(e.VBucket, "seq_no_at_stream_end", lastReadSeqNo)
				c.vbProcessingStats.updateVbStat(e.VBucket, "timestamp", time.Now().Format(time.RFC3339))
				lastSentSeqNo := c.vbProcessingStats.getVbStat(e.VBucket, "last_sent_seq_no").(uint64)
				if lastSentSeqNo == 0 {
					logging.Infof("STREAMEND without streaming any mutation last_read_seqno: %d last_sent_seqno: %d", lastReadSeqNo, lastSentSeqNo)
					c.handleStreamEnd(e.VBucket, lastReadSeqNo)
				} else {
					c.sendVbFilterData(e.VBucket, lastSentSeqNo, false)
				}

			case mcd.DCP_SYSTEM_EVENT:
				c.cidToKeyspaceCache.updateManifest(e)
				if e.EventType == mcd.COLLECTION_DROP || e.EventType == mcd.COLLECTION_FLUSH {
					c.SendDeleteCidMsg(e.CollectionID, e.VBucket, e.Seqno)
				} else {
					c.SendNoOp(e.Seqno, e.VBucket)
				}
				c.vbProcessingStats.updateVbStat(e.VBucket, "last_read_seq_no", e.Seqno)
				c.vbProcessingStats.updateVbStat(e.VBucket, "manifest_id", string(e.ManifestUID))

			case mcd.DCP_SEQNO_ADVANCED:
				c.SendNoOp(e.Seqno, e.VBucket)
				c.vbProcessingStats.updateVbStat(e.VBucket, "last_read_seq_no", e.Seqno)

			default:
			}

		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Exiting processDCPEvents routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}

func (c *Consumer) processStatsEvents() {
	logPrefix := "Consumer::processStatsEvents"

	var timerMsgCounter uint64
	for {
		select {
		case <-c.statsTicker.C:

			vbsOwned := c.getCurrentlyOwnedVbs()

			c.msgProcessedRWMutex.RLock()
			countMsg, dcpOpCount, tStamp := util.SprintDCPCounts(c.dcpMessagesProcessed)

			diff := tStamp.Sub(c.opsTimestamp)

			dcpOpsDiff := dcpOpCount - c.dcpOpsProcessed
			timerOpsDiff := c.timerMessagesProcessed - timerMsgCounter
			timerMsgCounter = c.timerMessagesProcessed

			seconds := int(diff.Nanoseconds() / (1000 * 1000 * 1000))
			if seconds > 0 {
				c.dcpOpsProcessedPSec = int(dcpOpsDiff) / seconds
				c.timerMessagesProcessedPSec = int(timerOpsDiff) / seconds
			}

			logging.Infof("%s [%s:%s:%d] DCP events: %s V8 events: %s Timer events: Doc: %v, vbs owned len: %d vbs owned: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), countMsg, util.SprintV8Counts(c.v8WorkerMessagesProcessed),
				c.timerMessagesProcessed, len(vbsOwned), util.Condense(vbsOwned))

			c.statsRWMutex.RLock()
			estats, eErr := json.Marshal(&c.executionStats)
			fstats, fErr := json.Marshal(&c.failureStats)
			c.statsRWMutex.RUnlock()

			if eErr == nil && fErr == nil {
				logging.Infof("%s [%s:%s:%d] CPP worker stats. Failure stats: %s execution stats: %s",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), string(fstats), string(estats))
			}

			c.opsTimestamp = tStamp
			c.dcpOpsProcessed = dcpOpCount
			c.msgProcessedRWMutex.RUnlock()

		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Exiting processStatsEvents routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}

func (c *Consumer) processFilterEvents() {
	logPrefix := "Consumer::processFilterEvents"

	for {
		select {
		case e, ok := <-c.filterDataCh:
			if ok == false {
				logging.Infof("%s [%s:%s:%d] Closing filterDataCh", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}
			logging.Infof("%s [%s:%s:%d] vb: %d seqNo: %d received on filterDataCh",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), e.Vbucket, e.SeqNo)

			c.handleStreamEnd(e.Vbucket, e.SeqNo)

		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Exiting processFilterEvents routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}

func (c *Consumer) startDcp() error {
	logPrefix := "Consumer::startDcp"

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		return fmt.Errorf("terminate routine is running")
	}

	logging.Infof("%s [%s:%s:%d] no. of vbs owned len: %d dump: %s",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), len(c.vbnos), util.Condense(c.vbnos))

	err := util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getEventingNodeAddrOpCallback, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	var vbSeqnos []uint64
	// Fetch high seq number only if dcp stream boundary is from now
	if c.dcpStreamBoundary == common.DcpFromNow {
		err = util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, util.GetSeqnos, c.producer.NsServerHostPort(),
			"default", c.sourceKeyspace.BucketName, c.srcKeyspaceID, &vbSeqnos, true)
		if err != nil && c.dcpStreamBoundary != common.DcpEverything {
			logging.Errorf("%s [%s:%s:%d] Failed to fetch vb seqnos, err: %v", logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
			return nil
		}
	} else {
		vbSeqnos = make([]uint64, c.numVbuckets)
	}

	c.cidToKeyspaceCache.refreshManifestFromClusterInfo()
	currentManifestUID := "0"
	if c.dcpStreamBoundary == common.DcpFromNow {
		currentManifestUID, _ = c.getManifestUID(c.sourceKeyspace.BucketName)
	}

	logging.Debugf("%s [%s:%s:%d] get_all_vb_seqnos: len => %d dump => %v",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), len(vbSeqnos), vbSeqnos)
	vbs := make([]uint16, 0, len(vbSeqnos))

	var operr error
	for _, vb := range c.vbnos {
		vbKey := common.GetCheckpointKey(c.app, vb, common.Checkpoint)
		var vbBlob vbucketKVBlob
		var cas gocb.Cas
		var isNoEnt bool

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
			c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, &operr, true, &isNoEnt)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		} else if operr == common.ErrEncryptionLevelChanged {
			logging.Errorf("%s [%s:%s:%d] Encryption level changed while accessing metadata bucket", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return operr
		}
		logging.Infof("%s [%s:%s:%d] vb: %d isNoEnt: %t", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, isNoEnt)

		if isNoEnt {
			failoverLog, err := c.getFailoverLog(nil, vb, false)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] vb: %d failed to grab failover log, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
				c.addVbForRestreaming(vb)
				continue
			}

			vbuuid, _, err := failoverLog.Latest()
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] vb: %d failed to grab latest failover log, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
				c.addVbForRestreaming(vb)
				continue
			}

			c.vbProcessingStats.updateVbStat(vb, "bootstrap_stream_req_done", false)

			// Storing vbuuid in metadata bucket, will be required for start
			// stream later on
			vbBlob.VBuuid = vbuuid
			vbBlob.VBId = vb
			vbBlob.AssignedWorker = c.ConsumerName()
			vbBlob.CurrentVBOwner = c.HostPortAddr()

			// Assigning previous owner and worker to current consumer
			vbBlob.PreviousAssignedWorker = c.ConsumerName()
			vbBlob.PreviousNodeUUID = c.NodeUUID()
			vbBlob.PreviousVBOwner = c.HostPortAddr()

			vbBlob.ManifestUID = currentManifestUID

			if c.dcpStreamBoundary == common.DcpFromNow {
				vbBlob.LastSeqNoProcessed = vbSeqnos[int(vb)]
			}

			vbBlob.CurrentProcessedDocIDTimer = time.Now().UTC().Format(time.RFC3339)
			vbBlob.LastProcessedDocIDTimerEvent = time.Now().UTC().Format(time.RFC3339)
			vbBlob.NextDocIDTimerToProcess = time.Now().UTC().Add(time.Second).Format(time.RFC3339)
			vbBlob.FailoverLog = failoverLog

			vbBlobVer := vbucketKVBlobVer{
				vbBlob,
				util.EventingVer(),
			}
			err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, setOpCallback,
				c, c.producer.AddMetadataPrefix(vbKey), &vbBlobVer, &operr)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return err
			} else if operr == common.ErrEncryptionLevelChanged {
				logging.Errorf("%s [%s:%s:%d] Encryption level changed while accessing metadata bucket", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return operr
			}

			logging.Infof("%s [%s:%s:%d] vb: %d Created initial metadata blob", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

			if c.checkAndAddToEnqueueMap(vb) {
				continue
			}

			vbs = append(vbs, vb)
			switch c.dcpStreamBoundary {
			case common.DcpEverything, common.DcpFromPrior:
				logging.Debugf("%s [%s:%s:%d] vb: %d Sending streamRequestInfo size: %d",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, len(c.reqStreamCh))

				c.reqStreamCh <- &streamRequestInfo{
					vb:          vb,
					vbBlob:      &vbBlob,
					startSeqNo:  uint64(0),
					manifestUID: vbBlob.ManifestUID,
				}
				c.vbProcessingStats.updateVbStat(vb, "manifest_id", vbBlob.ManifestUID)
				c.vbProcessingStats.updateVbStat(vb, "start_seq_no", uint64(0))
				c.vbProcessingStats.updateVbStat(vb, "timestamp", time.Now().Format(time.RFC3339))

			case common.DcpFromNow:
				logging.Debugf("%s [%s:%s:%d] vb: %d Sending streamRequestInfo size: %d",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, len(c.reqStreamCh))

				c.reqStreamCh <- &streamRequestInfo{
					vb:          vb,
					vbBlob:      &vbBlob,
					startSeqNo:  vbSeqnos[int(vb)],
					manifestUID: vbBlob.ManifestUID,
				}
				c.vbProcessingStats.updateVbStat(vb, "manifest_id", vbBlob.ManifestUID)
				c.vbProcessingStats.updateVbStat(vb, "start_seq_no", vbSeqnos[int(vb)])
				c.vbProcessingStats.updateVbStat(vb, "timestamp", time.Now().Format(time.RFC3339))
			}
		} else {
			logging.Infof("%s [%s:%s:%d] vb: %d checkpoint blob prexisted, UUID: %s assigned worker: %s",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.NodeUUID, vbBlob.AssignedWorker)

			if vbBlob.ManifestUID == "" {
				vbBlob.ManifestUID = currentManifestUID
			}

			sentToReqStream := false
			if vbBlob.NodeUUID == c.NodeUUID() || vbBlob.NodeUUID == "" {
				// this specifically addresses the corner case described in MB-46092
				c.workerVbucketMapRWMutex.RLock()
				_, consumerPresent := c.workerVbucketMap[vbBlob.AssignedWorker]
				c.workerVbucketMapRWMutex.RUnlock()

				if (vbBlob.AssignedWorker == c.ConsumerName() || vbBlob.AssignedWorker == "") || !consumerPresent {
					if c.checkAndAddToEnqueueMap(vb) {
						continue
					}

					vbs = append(vbs, vb)

					logging.Debugf("%s [%s:%s:%d] vb: %d Sending streamRequestInfo size: %d",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, len(c.reqStreamCh))

					sentToReqStream = true
					if !vbBlob.BootstrapStreamReqDone {

						c.vbProcessingStats.updateVbStat(vb, "bootstrap_stream_req_done", false)

						switch c.dcpStreamBoundary {
						case common.DcpEverything:
							c.reqStreamCh <- &streamRequestInfo{
								vb:          vb,
								vbBlob:      &vbBlob,
								startSeqNo:  0,
								manifestUID: vbBlob.ManifestUID,
							}
							c.vbProcessingStats.updateVbStat(vb, "start_seq_no", 0)

						case common.DcpFromNow:
							c.reqStreamCh <- &streamRequestInfo{
								vb:          vb,
								vbBlob:      &vbBlob,
								startSeqNo:  vbSeqnos[int(vb)],
								manifestUID: vbBlob.ManifestUID,
							}
							c.vbProcessingStats.updateVbStat(vb, "manifest_id", vbBlob.ManifestUID)
							c.vbProcessingStats.updateVbStat(vb, "start_seq_no", vbSeqnos[int(vb)])

						case common.DcpFromPrior:
							c.reqStreamCh <- &streamRequestInfo{
								vb:          vb,
								vbBlob:      &vbBlob,
								startSeqNo:  vbBlob.LastSeqNoProcessed,
								manifestUID: vbBlob.ManifestUID,
							}
							c.vbProcessingStats.updateVbStat(vb, "manifest_id", vbBlob.ManifestUID)
							c.vbProcessingStats.updateVbStat(vb, "start_seq_no", vbBlob.LastSeqNoProcessed)
						default:
							sentToReqStream = false
						}
					} else {
						c.reqStreamCh <- &streamRequestInfo{
							vb:          vb,
							vbBlob:      &vbBlob,
							startSeqNo:  vbBlob.LastSeqNoProcessed,
							manifestUID: vbBlob.ManifestUID,
						}
						c.vbProcessingStats.updateVbStat(vb, "manifest_id", vbBlob.ManifestUID)
						c.vbProcessingStats.updateVbStat(vb, "start_seq_no", vbBlob.LastSeqNoProcessed)
					}

					c.vbProcessingStats.updateVbStat(vb, "timestamp", time.Now().Format(time.RFC3339))
				}
			}
			if !sentToReqStream {
				c.addVbForRestreaming(vb)
			}
		}
	}

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval*5), c.retryCount, checkIfVbStreamsOpenedCallback, c, vbs, &operr)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	} else if operr == common.ErrEncryptionLevelChanged {
		logging.Errorf("%s [%s:%s:%d] Exiting from checkIfVbStreamsOpenedCallback as encryption level has been changed during bootstrap", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return operr
	}

	return nil
}

func (c *Consumer) addToAggChan(dcpFeed *couchbase.DcpFeed) {
	logPrefix := "Consumer::addToAggChan"

	go func(dcpFeed *couchbase.DcpFeed) {
		defer func() {
			if r := recover(); r != nil {
				trace := debug.Stack()
				logging.Errorf("%s [%s:%s:%d] addToAggChan: recover %rm stack trace: %rm",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
			}
		}()

		for {
			if dcpFeed == nil {
				logging.Infof("%s [%s:%s:%d] DCP feed has been closed, bailing out",
					logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			select {
			case e, ok := <-dcpFeed.C:
				if ok == false {
					c.hostDcpFeedRWMutex.Lock()
					for addr, feed := range c.kvHostDcpFeedMap {
						if feed == dcpFeed {
							delete(c.kvHostDcpFeedMap, addr)
							logging.Infof("%s [%s:%s:%d] Closing dcp feed: %v, count: %d for bucket: %s",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), dcpFeed.GetName(),
								len(dcpFeed.C), c.sourceKeyspace.BucketName)
						}
					}
					c.hostDcpFeedRWMutex.Unlock()
					return
				}

				switch e.Opcode {
				case mcd.DCP_STREAMREQ:
					c.updateDcpProcessedMsgs(e.Opcode)
					logging.Debugf("%s [%s:%s:%d] vb: %d got STREAMREQ status: %v",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, e.Status)
					c.dcpStatsLogger.AddDcpLog(e.VBucket, StreamResponse, fmt.Sprintf("%s", e.Status))

				retryCheckMetadataUpdated:
					if metadataUpdated, ok := c.vbProcessingStats.getVbStat(e.VBucket, "vb_stream_request_metadata_updated").(bool); ok {
						logging.Debugf("%s [%s:%s:%d] vb: %d STREAMREQ metadataUpdated: %t",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, metadataUpdated)
						if metadataUpdated {
							c.vbProcessingStats.updateVbStat(e.VBucket, "vb_stream_request_metadata_updated", false)
						} else {
							time.Sleep(100 * time.Millisecond)
							goto retryCheckMetadataUpdated
						}
					} else {
						logging.Infof("%s [%s:%s:%d] vb: %d STREAMREQ metadataUpdated not found",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket)
						time.Sleep(100 * time.Millisecond)
						goto retryCheckMetadataUpdated
					}

					if e.Status == mcd.SUCCESS {
						vbFlog := &vbFlogEntry{statusCode: e.Status, streamReqRetry: false, vb: e.VBucket}

						var vbBlob vbucketKVBlob
						var cas gocb.Cas

						vbKey := common.GetCheckpointKey(c.app, e.VBucket, common.Checkpoint)

						var operr error
						err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
							c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, &operr, false)
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
							continue
						} else if operr == common.ErrEncryptionLevelChanged {
							logging.Errorf("%s [%s:%s:%d] Skipping current STREAMREQ event as change in encryption level was detected during bootstrap", logPrefix, c.workerName, c.tcpPort, c.Pid())
							continue
						}

						vbuuid, seqNo, err := e.FailoverLog.Latest()
						if err != nil {
							logging.Errorf("%s [%s:%s:%d] vb: %d STREAMREQ Inserting entry: %#v to vbFlogChan."+
								" Failure to get latest failover log, err: %v",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, vbFlog, err)
							c.vbFlogChan <- vbFlog
							continue
						}

						c.vbProcessingStats.updateVbStat(e.VBucket, "vb_uuid", vbuuid)

						// Update metadata with latest vbuuid and rollback seq no
						vbBlob.AssignedWorker = c.ConsumerName()
						vbBlob.CurrentVBOwner = c.HostPortAddr()
						vbBlob.DCPStreamStatus = dcpStreamRunning
						vbBlob.LastSeqNoProcessed = seqNo
						vbBlob.NodeUUID = c.uuid
						vbBlob.VBuuid = vbuuid
						vbBlob.FailoverLog = *e.FailoverLog

						var startSeqNo uint64
						if seqNo, ok := c.vbProcessingStats.getVbStat(e.VBucket, "last_processed_seq_no").(uint64); ok {
							startSeqNo = seqNo
						}

						c.sendUpdateProcessedSeqNo(e.VBucket, startSeqNo)

						if val, ok := c.vbProcessingStats.getVbStat(e.VBucket, "bootstrap_stream_req_done").(bool); ok && !val {
							c.vbProcessingStats.updateVbStat(e.VBucket, "bootstrap_stream_req_done", true)
							vbBlob.BootstrapStreamReqDone = true
							logging.Infof("%s [%s:%s:%d] vb: %d updated bootstrap done flag to: %t",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, vbBlob.BootstrapStreamReqDone)
						}

						err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySRSCallback,
							c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &operr)
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
							continue
						} else if operr == common.ErrEncryptionLevelChanged {
							continue
						}

						c.vbProcessingStats.updateVbStat(e.VBucket, "assigned_worker", c.ConsumerName())
						c.vbProcessingStats.updateVbStat(e.VBucket, "current_vb_owner", c.HostPortAddr())
						c.vbProcessingStats.updateVbStat(e.VBucket, "dcp_stream_status", dcpStreamRunning)
						c.vbProcessingStats.updateVbStat(e.VBucket, "node_uuid", c.uuid)

						c.vbProcessingStats.updateVbStat(e.VBucket, "ever_owned_vb", true)
						c.vbProcessingStats.updateVbStat(e.VBucket, "host_name", c.HostPortAddr())
						c.vbProcessingStats.updateVbStat(e.VBucket, "last_checkpointed_seq_no", startSeqNo)
						c.vbProcessingStats.updateVbStat(e.VBucket, "timestamp", time.Now().Format(time.RFC3339))
						c.vbProcessingStats.updateVbStat(e.VBucket, "worker_name", c.ConsumerName())

						c.vbProcessingStats.updateVbStat(e.VBucket, "dcp_stream_requested_node_uuid", c.NodeUUID())
						c.vbProcessingStats.updateVbStat(e.VBucket, "dcp_stream_requested_worker", c.ConsumerName())

						c.vbProcessingStats.updateVbStat(e.VBucket, "vb_filter_ack_received", false)
						c.vbProcessingStats.updateVbStat(e.VBucket, "failover_log", vbBlob.FailoverLog)

						if !c.checkIfCurrentConsumerShouldOwnVb(e.VBucket) {
							c.Lock()
							c.vbsRemainingToClose = append(c.vbsRemainingToClose, e.VBucket)
							c.Unlock()

							c.filterVbEventsRWMutex.Lock()
							c.filterVbEvents[e.VBucket] = struct{}{}
							c.filterVbEventsRWMutex.Unlock()
						}

						logging.Debugf("%s [%s:%s:%d] vb: %d STREAMREQ Inserting entry: %#v to vbFlogChan",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, vbFlog)
						c.vbFlogChan <- vbFlog
						continue
					}

					if e.Status == mcd.KEY_EEXISTS {
						vbFlog := &vbFlogEntry{statusCode: e.Status, streamReqRetry: false, vb: e.VBucket}

						logging.Debugf("%s [%s:%s:%d] vb: %d STREAMREQ Inserting entry: %#v to vbFlogChan",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, vbFlog)
						c.vbFlogChan <- vbFlog
						continue
					}

					if e.Status != mcd.SUCCESS {
						vbFlog := &vbFlogEntry{
							flog:           e.FailoverLog,
							seqNo:          e.Seqno,
							statusCode:     e.Status,
							streamReqRetry: true,
							vb:             e.VBucket,
						}

						vbKey := common.GetCheckpointKey(c.app, e.VBucket, common.Checkpoint)

						var operr error
						err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySRFCallback,
							c, c.producer.AddMetadataPrefix(vbKey), &operr)
						if err == common.ErrRetryTimeout {
							logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
							continue
						} else if operr == common.ErrEncryptionLevelChanged {
							continue
						}

						logging.Debugf("%s [%s:%s:%d] vb: %d STREAMREQ Failed. Inserting entry: %#v to vbFlogChan",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, vbFlog)
						c.vbFlogChan <- vbFlog
						continue
					}
					// already processed, no need to push to aggfeed
					continue
				}

				if c.aggDCPFeedMem > c.aggDCPFeedMemCap {
					time.Sleep(10 * time.Millisecond)
				}

				atomic.AddInt64(&c.aggDCPFeedMem, int64(len(e.Value)))
				select {
				case c.aggDCPFeed <- e:
				case <-c.stopConsumerCh:
					return

				}
			case <-c.stopConsumerCh:
				return
			}
		}
	}(dcpFeed)
}

func (c *Consumer) cleanupStaleDcpFeedHandles() error {
	logPrefix := "Consumer::cleanupStaleDcpFeedHandles"

	kvAddrsPerVbMap := make(map[string]struct{})
	for _, kvAddr := range c.kvVbMap {
		kvAddrsPerVbMap[kvAddr] = struct{}{}
	}

	var kvAddrListPerVbMap []string
	for kvAddr := range kvAddrsPerVbMap {
		kvAddrListPerVbMap = append(kvAddrListPerVbMap, kvAddr)
	}

	var kvHostDcpFeedMapEntries []string
	c.hostDcpFeedRWMutex.RLock()
	for kvAddr := range c.kvHostDcpFeedMap {
		kvHostDcpFeedMapEntries = append(kvHostDcpFeedMapEntries, kvAddr)
	}
	c.hostDcpFeedRWMutex.RUnlock()

	kvAddrDcpFeedsToClose := util.StrSliceDiff(kvHostDcpFeedMapEntries, kvAddrListPerVbMap)

	if len(kvAddrDcpFeedsToClose) > 0 {
		var streamcreateerr error
		err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, populateDcpFeedVbEntriesCallback, c, &streamcreateerr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		} else if streamcreateerr == common.ErrEncryptionLevelChanged {
			logging.Errorf("%s [%s:%s:%d] Exiting as encryption level change during bootstrap", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return streamcreateerr
		}
	}

	for _, kvAddr := range kvAddrDcpFeedsToClose {
		logging.Infof("%s [%s:%s:%d] Going to cleanup kv dcp feed for kvAddr: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), kvAddr)

		c.hostDcpFeedRWMutex.RLock()
		feed, ok := c.kvHostDcpFeedMap[kvAddr]
		if ok && feed != nil {
			feed.Close()
		}
		c.hostDcpFeedRWMutex.RUnlock()

		c.hostDcpFeedRWMutex.Lock()
		vbsMetadataToUpdate := c.dcpFeedVbMap[c.kvHostDcpFeedMap[kvAddr]]
		delete(c.kvHostDcpFeedMap, kvAddr)
		c.hostDcpFeedRWMutex.Unlock()

		for _, vb := range vbsMetadataToUpdate {
			err := c.clearUpOwnershipInfoFromMeta(vb)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return err
			}
			if err == common.ErrEncryptionLevelChanged {
				return err
			}
		}
	}

	return nil
}

func (c *Consumer) clearUpOwnershipInfoFromMeta(vb uint16) error {
	var vbBlob vbucketKVBlob
	var cas gocb.Cas
	logPrefix := "Consumer::clearUpOwnershipInfoFromMeta"
	vbKey := common.GetCheckpointKey(c.app, vb, common.Checkpoint)

	var operr error
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
		c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, &operr, false)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	} else if operr == common.ErrEncryptionLevelChanged {
		logging.Errorf("%s [%s:%s:%d] Encryption due to change in encryption level during bootstrap", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return operr
	}

	vbBlob.AssignedWorker = ""
	vbBlob.CurrentVBOwner = ""
	vbBlob.DCPStreamStatus = dcpStreamStopped
	vbBlob.NodeUUID = ""
	vbBlob.PreviousAssignedWorker = c.ConsumerName()
	vbBlob.PreviousNodeUUID = c.NodeUUID()
	vbBlob.PreviousVBOwner = c.HostPortAddr()

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySECallback,
		c, c.producer.AddMetadataPrefix(vbKey), &operr)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	} else if operr == common.ErrEncryptionLevelChanged {
		return operr
	}

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, updateCheckpointCallback,
		c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &operr)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	} else if operr == common.ErrEncryptionLevelChanged {
		return operr
	}

	c.vbProcessingStats.updateVbStat(vb, "assigned_worker", vbBlob.AssignedWorker)
	c.vbProcessingStats.updateVbStat(vb, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vb, "dcp_stream_status", vbBlob.DCPStreamStatus)
	c.vbProcessingStats.updateVbStat(vb, "node_uuid", vbBlob.NodeUUID)
	return nil
}

func (c *Consumer) dcpRequestStreamHandle(vb uint16, vbBlob *vbucketKVBlob, start uint64, mid string) error {
	logPrefix := "Consumer::dcpRequestStreamHandle"

	defer func() {
		if r := recover(); r != nil {
			trace := debug.Stack()
			logging.Errorf("%s [%s:%s:%d] dcpRequestStreamHandle recover %rm stack trace: %rm",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), r, string(trace))
		}
	}()

	c.streamReqRWMutex.Lock()
	defer c.streamReqRWMutex.Unlock()

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		return nil
	}

	err := util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getKvVbMap, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	vbKvAddr := c.kvVbMap[vb]

	// Closing feeds for KV hosts which are no more present in kv vb map
	err = c.cleanupStaleDcpFeedHandles()
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	} else if err == common.ErrEncryptionLevelChanged {
		return err
	}

	c.hostDcpFeedRWMutex.Lock()
	dcpFeed, ok := c.kvHostDcpFeedMap[vbKvAddr]
	if !ok {
		feedName := couchbase.NewDcpFeedName(fmt.Sprintf("%d_%s_%s_%s", c.app.FunctionID, vbKvAddr, c.workerName, c.HostPortAddr()))
		var operr error
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, startDCPFeedOpCallback, c, feedName, vbKvAddr, &operr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		} else if operr == common.ErrEncryptionLevelChanged {
			c.hostDcpFeedRWMutex.Unlock()
			return operr
		}

		dcpFeed = c.kvHostDcpFeedMap[vbKvAddr]

		c.addToAggChan(dcpFeed)

		logging.Infof("%s [%s:%s:%d] vb: %d kvAddr: %s Started up new dcp feed. Spawned aggChan routine",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbKvAddr)
	}
	c.hostDcpFeedRWMutex.Unlock()

	c.Lock()
	c.vbDcpFeedMap[vb] = dcpFeed
	c.Unlock()

	opaque, flags := uint16(vb), uint32(0)
	end := uint64(0xFFFFFFFFFFFFFFFF)

	snapStart, snapEnd := start, start

	logging.Debugf("%s [%s:%s:%d] vb: %d DCP stream start vbKvAddr: %rs vbuuid: %d startSeq: %d snapshotStart: %d snapshotEnd: %d Manifest id: %s",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbKvAddr, vbBlob.VBuuid, start, snapStart, snapEnd, mid)

	if c.dcpFeedsClosed {
		return errDcpFeedsClosed
	}

	c.vbsStreamRRWMutex.Lock()
	if _, ok := c.vbStreamRequested[vb]; !ok {
		c.vbStreamRequested[vb] = start
		logging.Debugf("%s [%s:%s:%d] vb: %v Going to make DcpRequestStream call",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
	} else {
		c.vbsStreamRRWMutex.Unlock()
		logging.Debugf("%s [%s:%s:%d] vb: %v skipping DcpRequestStream call as one is already in-progress",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		return nil
	}
	c.vbsStreamRRWMutex.Unlock()

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		return fmt.Errorf("function is terminating")
	}

	c.dcpStreamReqCounter++
	hexScopeId, hexColId := getHexKeyspaceIDs(c.srcKeyspaceID)
	if mid == "" {
		manifestID, ok := c.vbProcessingStats.getVbStat(uint16(vb), "manifest_id").(string)
		if !ok || manifestID == "" {
			mid = "0"
		} else {
			mid = manifestID
		}
	}
	requestString := fmt.Sprintf("{seq: %v, uuid: %v, kv_node: %v, mid: %v, scopeID: %v, cid: %v}", start, vbBlob.VBuuid, vbKvAddr, mid, hexScopeId, hexColId)
	c.dcpStatsLogger.AddDcpLog(vb, LogState, requestString)
	err = dcpFeed.DcpRequestStream(vb, opaque, flags, vbBlob.VBuuid, start, end, snapStart, snapEnd, mid, hexScopeId, hexColId)
	if err != nil {
		c.dcpStatsLogger.AddDcpLog(vb, StreamResponse, fmt.Sprintf("%v", err))
		c.dcpStreamReqErrCounter++
		logging.Errorf("%s [%s:%s:%d] vb: %d STREAMREQ call failed on dcpFeed: %v, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, dcpFeed.GetName(), err)

		c.purgeVbStreamRequested(logPrefix, vb)

		if c.checkIfCurrentConsumerShouldOwnVb(vb) {
			c.Lock()
			c.vbsRemainingToRestream = append(c.vbsRemainingToRestream, vb)
			c.Unlock()
		}

		if err == couchbase.ErrorInvalidVbucket {
			return err
		}

		dcpFeed.Close()

		c.hostDcpFeedRWMutex.Lock()
		delete(c.kvHostDcpFeedMap, vbKvAddr)
		c.hostDcpFeedRWMutex.Unlock()

		logging.Infof("%s [%s:%s:%d] vb: %d Closed and deleted dcpfeed mapping to kvAddr: %s",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbKvAddr)
	} else {

		c.vbProcessingStats.updateVbStat(vb, "last_read_seq_no", start)
		c.vbProcessingStats.updateVbStat(vb, "last_processed_seq_no", start)
		c.vbProcessingStats.updateVbStat(vb, "last_sent_seq_no", uint64(0))

		logging.Debugf("%s [%s:%s:%d] vb: %d Adding entry into inflightDcpStreams",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

		c.inflightDcpStreamsRWMutex.Lock()
		c.inflightDcpStreams[vb] = struct{}{}
		c.inflightDcpStreamsRWMutex.Unlock()

		vbKey := common.GetCheckpointKey(c.app, vb, common.Checkpoint)
		var operr error
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySRRCallback,
			c, c.producer.AddMetadataPrefix(vbKey), &operr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		} else if operr == common.ErrEncryptionLevelChanged {
			return operr
		}

		c.vbProcessingStats.updateVbStat(vb, "vb_stream_request_metadata_updated", true)

		c.vbProcessingStats.updateVbStat(vb, "dcp_stream_requested", true)
		c.vbProcessingStats.updateVbStat(vb, "dcp_stream_requested_worker", c.ConsumerName())
		c.vbProcessingStats.updateVbStat(vb, "dcp_stream_requested_node_uuid", c.NodeUUID())

		logging.Debugf("%s [%s:%s:%d] vb: %d Updated checkpoint blob to indicate STREAMREQ was issued",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
	}

	return err
}

func (c *Consumer) handleFailoverLog() {
	logPrefix := "Consumer::handleFailoverLog"

	for {
		select {
		case vbFlog := <-c.vbFlogChan:
			logging.Debugf("%s [%s:%s:%d] vb: %d Got entry from vbFlogChan: %#v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, vbFlog)

			c.inflightDcpStreamsRWMutex.Lock()
			if _, exists := c.inflightDcpStreams[vbFlog.vb]; exists {
				logging.Debugf("%s [%s:%s:%d] vb: %d purging entry from inflightDcpStreams",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb)
				delete(c.inflightDcpStreams, vbFlog.vb)
			}
			c.inflightDcpStreamsRWMutex.Unlock()

			if vbFlog.signalStreamEnd {
				logging.Debugf("%s [%s:%s:%d] vb: %d got STREAMEND", logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb)
				continue
			}

			if !vbFlog.streamReqRetry && vbFlog.statusCode == mcd.SUCCESS {
				logging.Debugf("%s [%s:%s:%d] vb: %d DCP Stream created", logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb)
				continue
			}

			if vbFlog.streamReqRetry {
				vbKey := common.GetCheckpointKey(c.app, vbFlog.vb, common.Checkpoint)
				var vbBlob vbucketKVBlob
				var cas gocb.Cas
				var isNoEnt bool

				var operr error
				err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
					c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, &operr, true, &isNoEnt)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return
				} else if operr == common.ErrEncryptionLevelChanged {
					logging.Errorf("%s [%s:%s:%d] Encryption due to change in encryption level during bootstrap", logPrefix, c.workerName, c.tcpPort, c.Pid())
					continue
				}

				if vbBlob.ManifestUID == "" {
					vbBlob.ManifestUID = "0"
				}

				if vbFlog.statusCode == mcd.ROLLBACK {
					failoverLog, err := c.getFailoverLog(&vbBlob, vbFlog.vb, true)
					if err != nil {
						c.addVbForRestreaming(vbFlog.vb)
						continue
					}

					// This will make sure failover log will be till last seq number processed
					_, _, err = failoverLog.PopTillSeqNo(vbBlob.LastSeqNoProcessed)
					if err != nil {
						c.addVbForRestreaming(vbFlog.vb)
						continue
					}

					// Pop the top most failover log and use its vbuuid and startseq number to avoid rolling back to 0
					vbuuid, startSeqNo, err := failoverLog.PopAndGetLatest()
					if err != nil {
						c.addVbForRestreaming(vbFlog.vb)
						continue
					}

					logging.Debugf("%s [%s:%s:%d] vb: %d rollback requested by DCP. New vbuuid: %d startSeq: %d flog startSeqNo: %d",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, vbuuid, vbFlog.seqNo, startSeqNo)

					// update in-memory stats to reflect rollback seqno so that periodicCheckPoint picks up the latest data
					c.vbProcessingStats.updateVbStat(vbFlog.vb, "last_processed_seq_no", vbFlog.seqNo)
					c.vbProcessingStats.updateVbStat(vbFlog.vb, "vb_uuid", vbuuid)
					c.vbProcessingStats.updateVbStat(vbFlog.vb, "failover log", failoverLog)

					// update check point blob to let a racing doVbTakeover during rebalance try with correct <vbuuid, seqno> on next attempt
					vbBlob.VBuuid = vbuuid
					vbBlob.LastSeqNoProcessed = startSeqNo
					vbBlob.FailoverLog = failoverLog

					err = c.updateCheckpoint(vbKey, vbFlog.vb, &vbBlob)
					if err != nil {
						logging.Errorf("%s [%s:%s:%d] updateCheckpoint failed, err: %v", logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
					}

					c.purgeVbStreamRequested(logPrefix, vbFlog.vb)

					if c.checkAndAddToEnqueueMap(vbFlog.vb) {
						continue
					}

					logging.Debugf("%s [%s:%s:%d] vb: %d Sending streamRequestInfo size: %d",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, len(c.reqStreamCh))

					// Reason for sending this message at the time of sending STREAMREQ request
					// to DCP producer instead of of time when Eventing gets STREAMREQ response
					// from DCP producer is because we don't precisely know the start_seq_no for
					// for stream in later case, unless we maintain another data structure to
					// maintain that information
					c.sendVbFilterData(vbFlog.vb, vbFlog.seqNo, true)
					streamInfo := &streamRequestInfo{
						vb:          vbFlog.vb,
						vbBlob:      &vbBlob,
						startSeqNo:  startSeqNo,
						manifestUID: vbBlob.ManifestUID,
					}

					select {
					case c.reqStreamCh <- streamInfo:
					case <-c.stopConsumerCh:
						return
					}
					c.vbProcessingStats.updateVbStat(vbFlog.vb, "start_seq_no", startSeqNo)
					c.vbProcessingStats.updateVbStat(vbFlog.vb, "timestamp", time.Now().Format(time.RFC3339))
				} else {
					// Issuing high seq nos call to ascertain all vbuckets are back online(i.e. not stuck warm-up, flush etc)
				vbLabel:
					for {
						select {
						case <-c.stopConsumerCh:
							return
						default:
							var vbSeqNos []uint64
							err := util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount,
								util.GetSeqnos, c.producer.NsServerHostPort(), "default",
								c.sourceKeyspace.BucketName, c.srcKeyspaceID, &vbSeqNos, true)
							if err == nil {
								break vbLabel
							}
							logging.Errorf("%s [%s:%s:%d] Failed to fetch get_all_vb_seqnos, len vbSeqNos: %d err: %v",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), len(vbSeqNos), err)
						}
					}

					logging.Debugf("%s [%s:%s:%d] vb: %d Retrying DCP stream start vbuuid: %d vbFlog startSeq: %d last processed seq: %d",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, vbBlob.VBuuid,
						vbFlog.seqNo, vbBlob.LastSeqNoProcessed)

					c.purgeVbStreamRequested(logPrefix, vbFlog.vb)

					if c.checkAndAddToEnqueueMap(vbFlog.vb) {
						continue
					}

					c.vbProcessingStats.updateVbStat(vbFlog.vb, "manifest_id", vbBlob.ManifestUID)
					c.vbProcessingStats.updateVbStat(vbFlog.vb, "start_seq_no", vbBlob.LastSeqNoProcessed)
					c.vbProcessingStats.updateVbStat(vbFlog.vb, "timestamp", time.Now().Format(time.RFC3339))

					c.deleteFromEnqueueMap(vbFlog.vb)
					c.addVbForRestreaming(vbFlog.vb)
				}
			}

		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Exiting failover log handling routine", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}

func (c *Consumer) getCurrentlyOwnedVbs() []uint16 {
	var vbsOwned []uint16

	for vb := 0; vb < c.numVbuckets; vb++ {
		if c.vbProcessingStats.getVbStat(uint16(vb), "assigned_worker") == c.ConsumerName() &&
			c.vbProcessingStats.getVbStat(uint16(vb), "node_uuid") == c.NodeUUID() {

			vbsOwned = append(vbsOwned, uint16(vb))
		}
	}

	sort.Sort(util.Uint16Slice(vbsOwned))

	return vbsOwned
}

// Distribute partitions among cpp worker threads
func (c *Consumer) cppWorkerThrPartitionMap() {
	partitions := make([]uint16, c.numVbuckets)
	for i := 0; i < int(c.numVbuckets); i++ {
		partitions[i] = uint16(i)
	}

	c.cppThrPartitionMap = util.VbucketDistribution(partitions, c.cppWorkerThrCount)
}

func (c *Consumer) sendEvent(e *cb.DcpEvent) error {
	logPrefix := "Consumer::processTrappedEvent"

	mKeyspace, deleted := c.cidToKeyspaceCache.getKeyspaceName(e)
	if deleted {
		// Collection is deleted. Skip this mutation
		return nil
	}

	if !c.producer.IsTrapEvent() {
		c.sendDcpEvent(mKeyspace, e, false)
		return nil
	}

	logging.Debugf("%s [%s:%s:%d] Trying to trap an event", logPrefix, c.workerName, c.tcpPort, c.Pid())

	var success bool
	var instance common.DebuggerInstance

	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount,
		acquireDebuggerTokenCallback, c, c.producer.GetDebuggerToken(), &success, &instance)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	if success {
		c.startDebugger(mKeyspace, e, instance)
	} else {
		c.sendDcpEvent(mKeyspace, e, false)
	}
	return nil
}

func (c *Consumer) SendNoOp(seqNo uint64, partition uint16) {
	if !c.producer.IsTrapEvent() {
		c.sendNoOpEvent(seqNo, partition)
	}
}

func (c *Consumer) SendDeleteCidMsg(cid uint32, partition uint16, seqNo uint64) {
	if !c.producer.IsTrapEvent() {
		c.sendDeleteCidEvent(cid, partition, seqNo)
	}
}

func (c *Consumer) processReqStreamMessages() {
	logPrefix := "Consumer::processReqStreamMessages"

	for {
		select {
		case msg, ok := <-c.reqStreamCh:
			logging.Infof("%s [%s:%s:%d] vb: %d reqStreamCh size: %d msg: %#v Got request to stream",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), msg.vb, len(c.reqStreamCh), msg)

			c.deleteFromEnqueueMap(msg.vb)

			if !ok {
				logging.Infof("%s [%s:%s:%d] Returning streamReq processing routine", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			if !c.checkIfCurrentConsumerShouldOwnVb(msg.vb) {
				logging.Infof("%s [%s:%s:%d] vb: %d Skipping stream request as worker isn't supposed to own it",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg.vb)

				err := c.cleanupVbMetadata(msg.vb)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return
				} else if err == common.ErrEncryptionLevelChanged {
					continue
				}

				continue
			}

			if c.checkIfVbAlreadyOwnedByCurrConsumer(msg.vb) {
				logging.Infof("%s [%s:%s:%d] vb: %d Skipping stream request as vbucket is already owned by worker",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg.vb)
				continue
			}

			if c.producer.IsPlannerRunning() {
				logging.Infof("%s [%s:%s:%d] vb: %d Skipping stream request as planner is running", logPrefix, c.workerName, c.tcpPort, c.Pid(), msg.vb)

				time.Sleep(time.Second)

				c.Lock()
				c.vbsRemainingToRestream = append(c.vbsRemainingToRestream, msg.vb)

				if !util.Contains(msg.vb, c.vbsRemainingToCleanup) {
					c.vbsRemainingToCleanup = append(c.vbsRemainingToCleanup, msg.vb)
				}
				c.Unlock()

				continue
			}

			c.inflightDcpStreamsRWMutex.RLock()
			if _, ok := c.inflightDcpStreams[msg.vb]; ok {
				logging.Infof("%s [%s:%s:%d] vb: %d Skipping stream request as stream req for it is already in-flight",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg.vb)
				c.inflightDcpStreamsRWMutex.RUnlock()
				continue
			}
			c.inflightDcpStreamsRWMutex.RUnlock()

			var streamReqWG sync.WaitGroup
			streamReqWG.Add(1)

			go func(msg *streamRequestInfo, c *Consumer, logPrefix string, streamReqWG *sync.WaitGroup) {
				defer streamReqWG.Done()

				err := c.dcpRequestStreamHandle(msg.vb, msg.vbBlob, msg.startSeqNo, msg.manifestUID)
				if err != nil {
					c.Lock()
					c.vbsRemainingToRestream = append(c.vbsRemainingToRestream, msg.vb)
					c.Unlock()
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
					} else if err == common.ErrEncryptionLevelChanged {
						return
					}
				} else {
					logging.Debugf("%s [%s:%s:%d] vb: %d DCP stream successfully requested", logPrefix, c.workerName, c.tcpPort, c.Pid(), msg.vb)
				}
			}(msg, c, logPrefix, &streamReqWG)

			streamReqWG.Wait()

		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Exiting streamReq processing routine", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}

func (c *Consumer) handleStreamEnd(vBucket uint16, last_processed_seqno uint64) {
	logPrefix := "Consumer::handleStreamEnd"

	shouldOwn := false
	vbFlog := &vbFlogEntry{signalStreamEnd: true, vb: vBucket}
	defer func() {
		c.purgeVbStreamRequested(logPrefix, vBucket)
		c.inflightDcpStreamsRWMutex.Lock()
		if _, exists := c.inflightDcpStreams[vbFlog.vb]; exists {
			logging.Debugf("%s [%s:%s:%d] vb: %d purging entry from inflightDcpStreams",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb)
			delete(c.inflightDcpStreams, vbFlog.vb)
		}
		c.inflightDcpStreamsRWMutex.Unlock()
		if shouldOwn {
			c.Lock()
			c.vbsRemainingToRestream = append(c.vbsRemainingToRestream, vBucket)
			c.Unlock()
		}
	}()

	vbKey := common.GetCheckpointKey(c.app, vBucket, common.Checkpoint)

	var operr error
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySECallback,
		c, c.producer.AddMetadataPrefix(vbKey), &operr)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	} else if operr == common.ErrEncryptionLevelChanged {
		return
	}

	c.filterVbEventsRWMutex.Lock()
	if _, ok := c.filterVbEvents[vBucket]; ok {
		delete(c.filterVbEvents, vBucket)
	}
	c.filterVbEventsRWMutex.Unlock()

	var vbBlob vbucketKVBlob
	var cas gocb.Cas
	c.vbProcessingStats.updateVbStat(vBucket, "last_processed_seq_no", last_processed_seqno)

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
		c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, &operr, false)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	} else if operr == common.ErrEncryptionLevelChanged {
		logging.Errorf("%s [%s:%s:%d] Exiting change in encryption level during bootstrap", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	}

	vbBlob.LastSeqNoProcessed = last_processed_seqno
	err = c.updateCheckpoint(vbKey, vBucket, &vbBlob)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return
	} else if err == common.ErrEncryptionLevelChanged {
		return
	}

	c.vbProcessingStats.updateVbStat(vBucket, "assigned_worker", "")
	c.vbProcessingStats.updateVbStat(vBucket, "current_vb_owner", "")
	c.vbProcessingStats.updateVbStat(vBucket, "dcp_stream_status", dcpStreamStopped)
	c.vbProcessingStats.updateVbStat(vBucket, "node_uuid", "")
	c.vbProcessingStats.updateVbStat(vBucket, "dcp_stream_requested_worker", "")
	c.vbProcessingStats.updateVbStat(vBucket, "vb_filter_ack_received", true)

	shouldOwn = c.checkIfCurrentConsumerShouldOwnVb(vBucket)
	if shouldOwn {
		vbFlog.seqNo = last_processed_seqno
		c.vbFlogChan <- vbFlog

		logging.Infof("%s [%s:%s:%d] vb: %d got STREAMEND, Inserting entry: %#v to vbFlogChan",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vBucket, vbFlog)

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
			c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, &operr, false)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		} else if operr == common.ErrEncryptionLevelChanged {
			logging.Errorf("%s [%s:%s:%d] Exiting change in encryption level during bootstrap", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}

		err = c.updateCheckpoint(vbKey, vBucket, &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		} else if err == common.ErrEncryptionLevelChanged {
			return
		}
	} else {
		logging.Debugf("%s [%s:%s:%d] vb: %d got STREAMEND. Not owned by this node", logPrefix, c.workerName, c.tcpPort, c.Pid(), vBucket)
		c.dcpStatsLogger.DeletePartition(vBucket)
	}
}

// return false if message is supressed else true
func (c *Consumer) processAndSendDcpDelOrExpMessage(e *cb.DcpEvent, functionInstanceID string, checkRecursiveEvent bool) bool {
	logPrefix := "Consumer::processAndSendDcpMessage"
	switch e.Datatype {
	case uint8(cb.IncludeXATTRs):
		if c.producer.SrcMutation() && checkRecursiveEvent {
			if isRecursive, err := c.isRecursiveDCPEvent(e, functionInstanceID); err == nil && isRecursive == true {
				return false
			}
		}
		logging.Tracef("%s [%s:%s:%d] Sending key: %ru to be processed by JS handlers",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key))
		c.sendEvent(e)
	default:
		c.sendEvent(e)
	}
	return true
}

func (c *Consumer) sendXattrDoc(e *cb.DcpEvent, functionInstanceID string) {
	logPrefix := "Consumer::sendXattrDoc"

	if c.producer.SrcMutation() {
		if isRecursive, err := c.isRecursiveDCPEvent(e, functionInstanceID); err == nil && isRecursive == true {
			c.suppressedDCPMutationCounter++
		} else {
			logging.Tracef("%s [%s:%s:%d] No IntraHandlerRecursion, sending key: %ru to be processed by JS handlers",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key))
			c.dcpMutationCounter++
			c.sendEvent(e)
		}
	} else {
		logging.Tracef("%s [%s:%s:%d] Sending key: %ru to be processed by JS handlers",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key))
		c.dcpMutationCounter++
		c.sendEvent(e)
	}
}

// return true if filter event else false
func (c *Consumer) filterMutations(e *cb.DcpEvent) bool {
	c.filterVbEventsRWMutex.RLock()
	if _, ok := c.filterVbEvents[e.VBucket]; ok {
		c.filterVbEventsRWMutex.RUnlock()
		return true
	}
	c.filterVbEventsRWMutex.RUnlock()

	c.vbProcessingStats.updateVbStat(e.VBucket, "last_read_seq_no", e.Seqno)
	return false
}

func (c *Consumer) isTransactionMutation(e *cb.DcpEvent) bool {
	return bytes.HasPrefix(e.Key, cb.TransactionMutationPrefix)
}

func (c *Consumer) isSGWMutation(e *cb.DcpEvent) bool {
	return bytes.HasPrefix(e.Key, cb.SyncGatewayMutationPrefix)
}

// If fetchFresh is true then it will fetch the latest failover log if vbBlob doesn't contain failover log
func (c *Consumer) getFailoverLog(vbBlob *vbucketKVBlob, vb uint16, fetchFresh bool) (cb.FailoverLog, error) {
	logPrefix := "Consumer::getFailoverLog"

	if vbBlob != nil && vbBlob.FailoverLog != nil {
		return vbBlob.FailoverLog, nil
	}

	// If stream boundary is from now or failover log doesn't exist in vbblob then fetch fresh failover log
	if fetchFresh || c.dcpStreamBoundary == common.DcpFromNow {
		var flogs couchbase.FailoverLog
		var operr error
		err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getEFFailoverLogOpAllVbucketsCallback, c, &flogs, vb, &operr)
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil, err
		} else if operr == common.ErrEncryptionLevelChanged {
			logging.Errorf("%s [%s:%s:%d] Encryption due to change in encryption level during bootstrap", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil, operr
		}
		return flogs[vb], nil
	}

	return cb.InitFailoverLog(), nil
}

func (c *Consumer) addVbForRestreaming(vb uint16) {
	logPrefix := "Consumer::addVbForRestreaming"
	c.Lock()
	c.vbsRemainingToRestream = append(c.vbsRemainingToRestream, vb)
	c.Unlock()
	c.purgeVbStreamRequested(logPrefix, vb)
}

func getHexKeyspaceIDs(internal common.KeyspaceID) (scope, collection string) {
	switch internal.StreamType {
	case common.STREAM_BUCKET:
	case common.STREAM_SCOPE:
		scope = common.Uint32ToHex(internal.Sid)
	case common.STREAM_COLLECTION:
		collection = common.Uint32ToHex(internal.Cid)
	}
	return
}
