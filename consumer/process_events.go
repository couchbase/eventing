package consumer

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	mcd "github.com/couchbase/eventing/dcp/transport"
	cb "github.com/couchbase/eventing/dcp/transport/client"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/timers"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
)

func (c *Consumer) processEvents() {
	logPrefix := "Consumer::processEvents"

	functionInstanceID := strconv.Itoa(int(c.app.FunctionID)) + "-" + c.app.FunctionInstanceID

	var timerMsgCounter uint64
	for {
		if c.cppQueueSizes != nil {
			if c.workerQueueCap < c.cppQueueSizes.AggQueueSize ||
				c.feedbackQueueCap < c.cppQueueSizes.DocTimerQueueSize ||
				c.workerQueueMemCap < c.cppQueueSizes.AggQueueMemory {
				logging.Debugf("%s [%s:%s:%d] Throttling, cpp queue sizes: %+v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), c.cppQueueSizes)
				time.Sleep(10 * time.Millisecond)
			}
		}

		select {
		case e, ok := <-c.aggDCPFeed:
			if ok == false {
				logging.Infof("%s [%s:%s:%d] Closing DCP feed for bucket %q",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), c.bucket)
				return
			}

			atomic.AddInt64(&c.aggDCPFeedMem, -int64(len(e.Value)))

			c.msgProcessedRWMutex.Lock()
			if _, ok := c.dcpMessagesProcessed[e.Opcode]; !ok {
				c.dcpMessagesProcessed[e.Opcode] = 0
			}
			c.dcpMessagesProcessed[e.Opcode]++
			c.msgProcessedRWMutex.Unlock()

			switch e.Opcode {
			case mcd.DCP_MUTATION:

				c.filterVbEventsRWMutex.RLock()
				if _, ok := c.filterVbEvents[e.VBucket]; ok {
					c.filterVbEventsRWMutex.RUnlock()
					continue
				}
				c.filterVbEventsRWMutex.RUnlock()

				c.vbProcessingStats.updateVbStat(e.VBucket, "last_read_seq_no", e.Seqno)
				logging.Tracef("%s [%s:%s:%d] Got DCP_MUTATION for key: %ru datatype: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key), e.Datatype)

				switch e.Datatype {
				case dcpDatatypeJSON:
					c.dcpMutationCounter++
					c.sendEvent(e)
				case dcpDatatypeJSONXattr:
					xattrLen := binary.BigEndian.Uint32(e.Value[0:4])
					if c.app.SrcMutationEnabled {
						if isRecursive, err := c.isRecursiveDCPEvent(e, functionInstanceID); err == nil && isRecursive == true {
							c.suppressedDCPMutationCounter++
						} else {
							logging.Tracef("%s [%s:%s:%d] No IntraHandlerRecursion, sending key: %ru to be processed by JS handlers",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key))
							c.dcpMutationCounter++
							e.Value = e.Value[xattrLen+4:]
							c.sendEvent(e)
						}
					} else {
						logging.Tracef("%s [%s:%s:%d] Sending key: %ru to be processed by JS handlers",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key))
						c.dcpMutationCounter++
						e.Value = e.Value[xattrLen+4:]
						c.sendEvent(e)
					}
				}

			case mcd.DCP_DELETION:
				c.filterVbEventsRWMutex.RLock()
				if _, ok := c.filterVbEvents[e.VBucket]; ok {
					c.filterVbEventsRWMutex.RUnlock()
					continue
				}
				c.filterVbEventsRWMutex.RUnlock()

				c.vbProcessingStats.updateVbStat(e.VBucket, "last_read_seq_no", e.Seqno)
				switch e.Datatype {
				case dcpDatatypeJSONXattr:
					xattrLen := binary.BigEndian.Uint32(e.Value[0:4])
					if c.app.SrcMutationEnabled {
						if isRecursive, err := c.isRecursiveDCPEvent(e, functionInstanceID); err == nil && isRecursive == true {
							c.suppressedDCPDeletionCounter++
						} else {
							c.dcpDeletionCounter++
							e.Value = e.Value[xattrLen+4:]
							logging.Tracef("%s [%s:%s:%d] No IntraHandlerRecursion, sending key: %ru to be processed by JS handlers",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key))
							c.sendEvent(e)
						}
					} else {
						c.dcpDeletionCounter++
						e.Value = e.Value[xattrLen+4:]
						logging.Tracef("%s [%s:%s:%d] Sending key: %ru to be processed by JS handlers",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key))
						c.sendEvent(e)
					}
				default:
					c.dcpDeletionCounter++
					c.sendEvent(e)
				}

			case mcd.DCP_STREAMREQ:

				logging.Infof("%s [%s:%s:%d] vb: %d got STREAMREQ status: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, e.Status)

			retryCheckMetadataUpdated:
				if metadataUpdated, ok := c.vbProcessingStats.getVbStat(e.VBucket, "vb_stream_request_metadata_updated").(bool); ok {
					logging.Infof("%s [%s:%s:%d] vb: %d STREAMREQ metadataUpdated: %t",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, metadataUpdated)
					if metadataUpdated {
						c.vbProcessingStats.updateVbStat(e.VBucket, "vb_stream_request_metadata_updated", false)
					} else {
						time.Sleep(time.Second)
						goto retryCheckMetadataUpdated
					}
				} else {
					logging.Infof("%s [%s:%s:%d] vb: %d STREAMREQ metadataUpdated not found",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket)
					time.Sleep(time.Second)
					goto retryCheckMetadataUpdated
				}

				if e.Status == mcd.SUCCESS {

					kvNodes := c.getKvNodes()

					connStr := "couchbase://"
					for index, kvNode := range kvNodes {
						if index != 0 {
							connStr = connStr + ","
						}
						connStr = connStr + kvNode
					}

					if util.IsIPv6() {
						connStr += "?ipv6=allow"
					}

					if c.usingTimer {
						err := timers.Create(c.producer.GetMetadataPrefix(), int(e.VBucket), connStr, c.producer.MetadataBucket())
						if err == common.ErrRetryTimeout {
							logging.Infof("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
							return
						}
						if err != nil {
							logging.Errorf("%s [%s:%s:%d] vb: %d unable to create metastore, err: %v",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, err)
						}
					}

					vbFlog := &vbFlogEntry{statusCode: e.Status, streamReqRetry: false, vb: e.VBucket}

					var vbBlob vbucketKVBlob
					var cas gocb.Cas

					vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, e.VBucket)

					err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
						c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, false)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
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

					var startSeqNo uint64
					if seqNo, ok := c.vbProcessingStats.getVbStat(e.VBucket, "last_processed_seq_no").(uint64); ok {
						startSeqNo = seqNo
					}

					if val, ok := c.vbProcessingStats.getVbStat(e.VBucket, "bootstrap_stream_req_done").(bool); ok && !val {
						c.vbProcessingStats.updateVbStat(e.VBucket, "bootstrap_stream_req_done", true)
						vbBlob.BootstrapStreamReqDone = true
						logging.Infof("%s [%s:%s:%d] vb: %d updated bootstrap done flag to: %t",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, vbBlob.BootstrapStreamReqDone)
					}

					entry := OwnershipEntry{
						AssignedWorker: c.ConsumerName(),
						CurrentVBOwner: c.HostPortAddr(),
						Operation:      dcpStreamRunning,
						SeqNo:          startSeqNo,
						Timestamp:      time.Now().String(),
					}

					err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySRSCallback,
						c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &entry)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
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

					if !c.checkIfCurrentConsumerShouldOwnVb(e.VBucket) {
						c.Lock()
						c.vbsRemainingToClose = append(c.vbsRemainingToClose, e.VBucket)
						c.Unlock()

						c.filterVbEventsRWMutex.Lock()
						c.filterVbEvents[e.VBucket] = struct{}{}
						c.filterVbEventsRWMutex.Unlock()
					}

					logging.Infof("%s [%s:%s:%d] vb: %d STREAMREQ Inserting entry: %#v to vbFlogChan",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, vbFlog)
					c.vbFlogChan <- vbFlog
					continue
				}

				if e.Status == mcd.KEY_EEXISTS {
					vbFlog := &vbFlogEntry{statusCode: e.Status, streamReqRetry: false, vb: e.VBucket}

					logging.Infof("%s [%s:%s:%d] vb: %d STREAMREQ Inserting entry: %#v to vbFlogChan",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, vbFlog)
					c.vbFlogChan <- vbFlog
					continue
				}

				if e.Status != mcd.SUCCESS {

					vbFlog := &vbFlogEntry{
						flog:           e.FailoverLog,
						statusCode:     e.Status,
						streamReqRetry: true,
						vb:             e.VBucket,
					}

					c.vbsStreamRRWMutex.Lock()
					if _, ok := c.vbStreamRequested[e.VBucket]; ok {
						logging.Infof("%s [%s:%s:%d] vb: %d STREAMREQ failed, purging entry from vbStreamRequested",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket)

						delete(c.vbStreamRequested, e.VBucket)
					}
					c.vbsStreamRRWMutex.Unlock()

					vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, e.VBucket)

					entry := OwnershipEntry{
						AssignedWorker: c.ConsumerName(),
						CurrentVBOwner: c.HostPortAddr(),
						Operation:      dcpStreamRequestFailed,
						Timestamp:      time.Now().String(),
					}

					err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySRFCallback,
						c, c.producer.AddMetadataPrefix(vbKey), &entry)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
					}

					logging.Infof("%s [%s:%s:%d] vb: %d STREAMREQ Inserting entry: %#v to vbFlogChan",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, vbFlog)
					c.vbFlogChan <- vbFlog
				}
			case mcd.DCP_STREAMEND:
				logging.Infof("%s [%s:%s:%d] vb: %d got STREAMEND", logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket)

				c.vbProcessingStats.updateVbStat(e.VBucket, "vb_stream_request_metadata_updated", false)
				lastSeqNo := c.vbProcessingStats.getVbStat(e.VBucket, "last_read_seq_no").(uint64)
				c.vbProcessingStats.updateVbStat(e.VBucket, "seq_no_at_stream_end", lastSeqNo)
				c.vbProcessingStats.updateVbStat(e.VBucket, "timestamp", time.Now().Format(time.RFC3339))

				c.sendVbFilterData(e.VBucket, lastSeqNo, false)

			default:
			}

		case e, ok := <-c.filterDataCh:
			if ok == false {
				logging.Infof("%s [%s:%s:%d] Closing filterDataCh", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}
			logging.Infof("%s [%s:%s:%d] vb: %d seqNo: %d received on filterDataCh",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), e.Vbucket, e.SeqNo)

			c.vbsStreamRRWMutex.Lock()
			if _, ok := c.vbStreamRequested[e.Vbucket]; ok {
				logging.Infof("%s [%s:%s:%d] vb: %d purging entry from vbStreamRequested",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), e.Vbucket)
				delete(c.vbStreamRequested, e.Vbucket)
			}
			c.vbsStreamRRWMutex.Unlock()

			c.inflightDcpStreamsRWMutex.Lock()
			if _, exists := c.inflightDcpStreams[e.Vbucket]; exists {
				logging.Infof("%s [%s:%s:%d] vb: %d purging entry from inflightDcpStreams",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), e.Vbucket)
				delete(c.inflightDcpStreams, e.Vbucket)
			}
			c.inflightDcpStreamsRWMutex.Unlock()

			vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, e.Vbucket)

			seqNo := c.vbProcessingStats.getVbStat(e.Vbucket, "last_read_seq_no").(uint64)

			entry := OwnershipEntry{
				AssignedWorker: c.ConsumerName(),
				CurrentVBOwner: c.HostPortAddr(),
				Operation:      dcpStreamStopped,
				SeqNo:          seqNo,
				Timestamp:      time.Now().String(),
			}

			err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySECallback,
				c, c.producer.AddMetadataPrefix(vbKey), &entry)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			c.filterVbEventsRWMutex.Lock()
			delete(c.filterVbEvents, e.Vbucket)
			c.filterVbEventsRWMutex.Unlock()

			var vbBlob vbucketKVBlob
			var cas gocb.Cas
			c.vbProcessingStats.updateVbStat(e.Vbucket, "last_processed_seq_no", e.SeqNo)

			err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
				c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, false)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			vbBlob.LastSeqNoProcessed = e.SeqNo
			err = c.updateCheckpoint(vbKey, e.Vbucket, &vbBlob)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			c.vbProcessingStats.updateVbStat(e.Vbucket, "assigned_worker", "")
			c.vbProcessingStats.updateVbStat(e.Vbucket, "current_vb_owner", "")
			c.vbProcessingStats.updateVbStat(e.Vbucket, "dcp_stream_status", dcpStreamStopped)
			c.vbProcessingStats.updateVbStat(e.Vbucket, "node_uuid", "")
			c.vbProcessingStats.updateVbStat(e.Vbucket, "dcp_stream_requested_worker", "")
			c.vbProcessingStats.updateVbStat(e.Vbucket, "vb_filter_ack_received", true)

			if c.checkIfCurrentConsumerShouldOwnVb(e.Vbucket) {
				logging.Infof("%s [%s:%s:%d] vb: %d got STREAMEND, needs to be reclaimed",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), e.Vbucket)

				vbFlog := &vbFlogEntry{seqNo: e.SeqNo, signalStreamEnd: true, vb: e.Vbucket}
				logging.Infof("%s [%s:%s:%d] vb: %d STREAMEND Inserting entry: %#v to vbFlogChan",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), e.Vbucket, vbFlog)
				c.vbFlogChan <- vbFlog

				err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
					c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, false)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return
				}

				err = c.updateCheckpoint(vbKey, e.Vbucket, &vbBlob)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return
				}

				c.Lock()
				c.vbsRemainingToRestream = append(c.vbsRemainingToRestream, e.Vbucket)
				c.Unlock()
			}

		case <-c.statsTicker.C:

			vbsOwned := c.getCurrentlyOwnedVbs()
			if len(vbsOwned) > 0 {

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

				c.statsRWMutex.Lock()
				estats, eErr := json.Marshal(&c.executionStats)
				fstats, fErr := json.Marshal(&c.failureStats)
				c.statsRWMutex.Unlock()

				if eErr == nil && fErr == nil {
					logging.Infof("%s [%s:%s:%d] CPP worker stats. Failure stats: %s execution stats: %s",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), string(fstats), string(estats))
				}

				c.opsTimestamp = tStamp
				c.dcpOpsProcessed = dcpOpCount
				c.msgProcessedRWMutex.RUnlock()
			}

		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Exiting processEvents routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}

func (c *Consumer) processTimerEvents() {
	logPrefix := "Consumer::processTimerEvents"
	for {
		select {
		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Exiting processTimerEvents routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		default:
			ev, err := c.fireTimerQueue.Pop()
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to pop from fireTimerQueue, err: %v", logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
				return
			}
			timer := ev.(*timerContext)
			c.timerMessagesProcessed++
			c.sendTimerEvent(timer, false)
		}
	}
}

func (c *Consumer) startDcp(flogs couchbase.FailoverLog) error {
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

	vbSeqnos, err := util.BucketSeqnos(c.producer.NsServerHostPort(), "default", c.bucket)
	if err != nil && c.dcpStreamBoundary != common.DcpEverything {
		logging.Errorf("%s [%s:%s:%d] Failed to fetch vb seqnos, err: %v", logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
		return nil
	}

	logging.Debugf("%s [%s:%s:%d] get_all_vb_seqnos: len => %d dump => %v",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), len(vbSeqnos), vbSeqnos)

	flogVbs := make([]uint16, 0)
	vbs := make([]uint16, 0)

	for vb := range flogs {
		flogVbs = append(flogVbs, vb)
	}
	sort.Sort(util.Uint16Slice(flogVbs))

	logging.Infof("%s [%s:%s:%d] flogVbs len: %d dump: %v flogs len: %d dump: %v",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), len(flogVbs), util.Condense(flogVbs), len(flogVbs), flogs)

	for _, vb := range flogVbs {
		flog := flogs[vb]
		vbuuid, _, err := flog.Latest()
		if err != nil {
			logging.Errorf("%s [%s:%s:%d] vb: %d failed to grab latest failover log, err: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
			continue
		}

		logging.Infof("%s [%s:%s:%d] vb: %d vbuuid: %d flog: %v going to start dcp stream",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbuuid, flog)

		vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vb)
		var vbBlob vbucketKVBlob
		var start uint64
		var cas gocb.Cas
		var isNoEnt bool

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
			c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, true, &isNoEnt)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}

		logging.Infof("%s [%s:%s:%d] vb: %d isNoEnt: %t", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, isNoEnt)

		if isNoEnt {

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

			entry := OwnershipEntry{
				AssignedWorker: c.ConsumerName(),
				CurrentVBOwner: c.HostPortAddr(),
				Operation:      dcpStreamBootstrap,
				Timestamp:      time.Now().String(),
			}
			vbBlob.OwnershipHistory = append(vbBlob.OwnershipHistory, entry)

			vbBlob.CurrentProcessedDocIDTimer = time.Now().UTC().Format(time.RFC3339)
			vbBlob.LastProcessedDocIDTimerEvent = time.Now().UTC().Format(time.RFC3339)
			vbBlob.NextDocIDTimerToProcess = time.Now().UTC().Add(time.Second).Format(time.RFC3339)

			vbBlobVer := vbucketKVBlobVer{
				vbBlob,
				util.EventingVer(),
			}
			err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, setOpCallback,
				c, c.producer.AddMetadataPrefix(vbKey), &vbBlobVer)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return err
			}

			logging.Infof("%s [%s:%s:%d] vb: %d Created initial metadata blob", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

			if c.checkIfAlreadyEnqueued(vb) {
				continue
			} else {
				c.addToEnqueueMap(vb)
			}

			vbs = append(vbs, vb)
			switch c.dcpStreamBoundary {
			case common.DcpEverything, common.DcpFromPrior:
				logging.Infof("%s [%s:%s:%d] vb: %d Sending streamRequestInfo size: %d",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, len(c.reqStreamCh))

				c.reqStreamCh <- &streamRequestInfo{
					vb:         vb,
					vbBlob:     &vbBlob,
					startSeqNo: uint64(0),
				}
				c.vbProcessingStats.updateVbStat(vb, "start_seq_no", start)
				c.vbProcessingStats.updateVbStat(vb, "timestamp", time.Now().Format(time.RFC3339))

			case common.DcpFromNow:
				logging.Infof("%s [%s:%s:%d] vb: %d Sending streamRequestInfo size: %d",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, len(c.reqStreamCh))

				c.reqStreamCh <- &streamRequestInfo{
					vb:         vb,
					vbBlob:     &vbBlob,
					startSeqNo: vbSeqnos[int(vb)],
				}
				c.vbProcessingStats.updateVbStat(vb, "start_seq_no", start)
				c.vbProcessingStats.updateVbStat(vb, "timestamp", time.Now().Format(time.RFC3339))
			}
		} else {
			logging.Infof("%s [%s:%s:%d] vb: %d checkpoint blob prexisted, UUID: %s assigned worker: %s",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbBlob.NodeUUID, vbBlob.AssignedWorker)

			if (vbBlob.NodeUUID == c.NodeUUID() || vbBlob.NodeUUID == "") &&
				(vbBlob.AssignedWorker == c.ConsumerName() || vbBlob.AssignedWorker == "") {

				if c.checkIfAlreadyEnqueued(vb) {
					continue
				} else {
					c.addToEnqueueMap(vb)
				}

				vbs = append(vbs, vb)

				logging.Infof("%s [%s:%s:%d] vb: %d Sending streamRequestInfo size: %d",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, len(c.reqStreamCh))

				if !vbBlob.BootstrapStreamReqDone {

					c.vbProcessingStats.updateVbStat(vb, "bootstrap_stream_req_done", false)

					switch c.dcpStreamBoundary {
					case common.DcpEverything:
						c.reqStreamCh <- &streamRequestInfo{
							vb:         vb,
							vbBlob:     &vbBlob,
							startSeqNo: 0,
						}
						c.vbProcessingStats.updateVbStat(vb, "start_seq_no", 0)

					case common.DcpFromNow:
						c.reqStreamCh <- &streamRequestInfo{
							vb:         vb,
							vbBlob:     &vbBlob,
							startSeqNo: vbSeqnos[int(vb)],
						}
						c.vbProcessingStats.updateVbStat(vb, "start_seq_no", vbSeqnos[int(vb)])

					case common.DcpFromPrior:
						c.reqStreamCh <- &streamRequestInfo{
							vb:         vb,
							vbBlob:     &vbBlob,
							startSeqNo: vbBlob.LastSeqNoProcessed,
						}
						c.vbProcessingStats.updateVbStat(vb, "start_seq_no", vbBlob.LastSeqNoProcessed)
					}
				} else {
					c.reqStreamCh <- &streamRequestInfo{
						vb:         vb,
						vbBlob:     &vbBlob,
						startSeqNo: vbBlob.LastSeqNoProcessed,
					}
					c.vbProcessingStats.updateVbStat(vb, "start_seq_no", vbBlob.LastSeqNoProcessed)
				}

				c.vbProcessingStats.updateVbStat(vb, "timestamp", time.Now().Format(time.RFC3339))
			}
		}
	}

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval*5), c.retryCount, checkIfVbStreamsOpenedCallback, c, vbs)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
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
					var kvAddr string
					c.hostDcpFeedRWMutex.RLock()
					for addr, feed := range c.kvHostDcpFeedMap {
						if feed == dcpFeed {
							kvAddr = addr
						}
					}
					c.hostDcpFeedRWMutex.RUnlock()

					logging.Infof("%s [%s:%s:%d] Closing dcp feed: %v, count: %d for bucket: %s",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), dcpFeed.GetName(), len(dcpFeed.C), c.bucket)
					c.hostDcpFeedRWMutex.Lock()
					delete(c.kvHostDcpFeedMap, kvAddr)
					c.hostDcpFeedRWMutex.Unlock()
					return
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
		err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, populateDcpFeedVbEntriesCallback, c)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
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
		}
	}

	return nil
}

func (c *Consumer) clearUpOwnershipInfoFromMeta(vb uint16) error {
	var vbBlob vbucketKVBlob
	var cas gocb.Cas
	logPrefix := "Consumer::clearUpOwnershipInfoFromMeta"
	vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vb)

	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
		c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, false)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	vbBlob.AssignedWorker = ""
	vbBlob.CurrentVBOwner = ""
	vbBlob.DCPStreamStatus = dcpStreamStopped
	vbBlob.NodeUUID = ""
	vbBlob.PreviousAssignedWorker = c.ConsumerName()
	vbBlob.PreviousNodeUUID = c.NodeUUID()
	vbBlob.PreviousVBOwner = c.HostPortAddr()

	entry := OwnershipEntry{
		AssignedWorker: c.ConsumerName(),
		CurrentVBOwner: c.HostPortAddr(),
		Operation:      dcpStreamStopped,
		Timestamp:      time.Now().String(),
	}

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySECallback,
		c, c.producer.AddMetadataPrefix(vbKey), &entry)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, updateCheckpointCallback,
		c, c.producer.AddMetadataPrefix(vbKey), &vbBlob)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return err
	}

	c.vbProcessingStats.updateVbStat(vb, "assigned_worker", vbBlob.AssignedWorker)
	c.vbProcessingStats.updateVbStat(vb, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vb, "dcp_stream_status", vbBlob.DCPStreamStatus)
	c.vbProcessingStats.updateVbStat(vb, "node_uuid", vbBlob.NodeUUID)
	return nil
}

func (c *Consumer) dcpRequestStreamHandle(vb uint16, vbBlob *vbucketKVBlob, start uint64) error {
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

	refreshMap := func() error {
		c.cbBucketRWMutex.Lock()
		defer c.cbBucketRWMutex.Unlock()

		err := c.cbBucket.Refresh()
		if err != nil {
			logging.Infof("%s [%s:%s:%d] vb: %d failed to refresh vbmap",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
			return err
		}
		return nil
	}

	err := refreshMap()
	if err != nil {
		return err
	}

	err = util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getKvVbMap, c)
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
	}

	c.hostDcpFeedRWMutex.Lock()
	dcpFeed, ok := c.kvHostDcpFeedMap[vbKvAddr]
	if !ok {
		feedName := couchbase.NewDcpFeedName(c.HostPortAddr() + "_" + vbKvAddr + "_" + c.workerName)
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, startDCPFeedOpCallback, c, feedName, vbKvAddr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
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

	logging.Infof("%s [%s:%s:%d] vb: %d DCP stream start vbKvAddr: %rs vbuuid: %d startSeq: %d snapshotStart: %d snapshotEnd: %d",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, vbKvAddr, vbBlob.VBuuid, start, snapStart, snapEnd)

	if c.dcpFeedsClosed {
		return errDcpFeedsClosed
	}

	c.vbsStreamRRWMutex.Lock()
	if _, ok := c.vbStreamRequested[vb]; !ok {
		c.vbStreamRequested[vb] = struct{}{}
		logging.Infof("%s [%s:%s:%d] vb: %v Going to make DcpRequestStream call",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
	} else {
		c.vbsStreamRRWMutex.Unlock()
		logging.Infof("%s [%s:%s:%d] vb: %v skipping DcpRequestStream call as one is already in-progress",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
		return nil
	}
	c.vbsStreamRRWMutex.Unlock()

	err = refreshMap()
	if err != nil {
		return err
	}

	if atomic.LoadUint32(&c.isTerminateRunning) == 1 {
		return fmt.Errorf("function is terminating")
	}

	c.dcpStreamReqCounter++
	err = dcpFeed.DcpRequestStream(vb, opaque, flags, vbBlob.VBuuid, start, end, snapStart, snapEnd)
	if err != nil {
		c.dcpStreamReqErrCounter++
		logging.Errorf("%s [%s:%s:%d] vb: %d STREAMREQ call failed on dcpFeed: %v, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, dcpFeed.GetName(), err)

		c.vbsStreamRRWMutex.Lock()
		if _, ok := c.vbStreamRequested[vb]; ok {
			logging.Infof("%s [%s:%s:%d] vb: %d purging entry from vbStreamRequested",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

			delete(c.vbStreamRequested, vb)
		}
		c.vbsStreamRRWMutex.Unlock()

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

		c.sendUpdateProcessedSeqNo(vb, start)
		logging.Infof("%s [%s:%s:%d] vb: %d Adding entry into inflightDcpStreams",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)

		c.inflightDcpStreamsRWMutex.Lock()
		c.inflightDcpStreams[vb] = struct{}{}
		c.inflightDcpStreamsRWMutex.Unlock()

		entry := OwnershipEntry{
			AssignedWorker: c.ConsumerName(),
			CurrentVBOwner: c.HostPortAddr(),
			Operation:      dcpStreamRequested,
			SeqNo:          start,
			Timestamp:      time.Now().String(),
		}

		vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vb)
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySRRCallback,
			c, c.producer.AddMetadataPrefix(vbKey), &entry)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}

		c.vbProcessingStats.updateVbStat(vb, "vb_stream_request_metadata_updated", true)

		c.vbProcessingStats.updateVbStat(vb, "dcp_stream_requested", true)
		c.vbProcessingStats.updateVbStat(vb, "dcp_stream_requested_worker", c.ConsumerName())
		c.vbProcessingStats.updateVbStat(vb, "dcp_stream_requested_node_uuid", c.NodeUUID())

		logging.Infof("%s [%s:%s:%d] vb: %d Updated checkpoint blob to indicate STREAMREQ was issued",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
	}

	return err
}

func (c *Consumer) handleFailoverLog() {
	logPrefix := "Consumer::handleFailoverLog"

	for {
		select {
		case vbFlog := <-c.vbFlogChan:
			logging.Infof("%s [%s:%s:%d] vb: %d Got entry from vbFlogChan: %#v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, vbFlog)

			c.inflightDcpStreamsRWMutex.Lock()
			if _, exists := c.inflightDcpStreams[vbFlog.vb]; exists {
				logging.Infof("%s [%s:%s:%d] vb: %d purging entry from inflightDcpStreams",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb)
				delete(c.inflightDcpStreams, vbFlog.vb)
			}
			c.inflightDcpStreamsRWMutex.Unlock()

			if vbFlog.signalStreamEnd {
				logging.Infof("%s [%s:%s:%d] vb: %d got STREAMEND", logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb)
				continue
			}

			if !vbFlog.streamReqRetry && vbFlog.statusCode == mcd.SUCCESS {
				logging.Infof("%s [%s:%s:%d] vb: %d DCP Stream created", logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb)
				continue
			}

			if vbFlog.streamReqRetry {

				vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vbFlog.vb)
				var vbBlob vbucketKVBlob
				var cas gocb.Cas
				var isNoEnt bool

				err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
					c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, true, &isNoEnt)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return
				}

				var flogs couchbase.FailoverLog
				var startSeqNo uint64
				var vbuuid uint64

				err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getEFFailoverLogOpAllVbucketsCallback, c, &flogs, vbFlog.vb)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return
				}

				if flog, ok := flogs[vbFlog.vb]; ok {
					vbuuid, startSeqNo, err = flog.FetchLogForSeqNo(vbBlob.LastSeqNoProcessed)
					if err != nil {
						c.Lock()
						c.vbsRemainingToRestream = append(c.vbsRemainingToRestream, vbFlog.vb)
						c.Unlock()
						continue
					}

					vbBlob.VBuuid = vbuuid
				}

				if vbFlog.statusCode == mcd.ROLLBACK {
					logging.Infof("%s [%s:%s:%d] vb: %d rollback requested by DCP. Retrying DCP stream start vbuuid: %d startSeq: %d flog startSeqNo: %d",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, vbBlob.VBuuid, vbFlog.seqNo, startSeqNo)

					if c.checkIfAlreadyEnqueued(vbFlog.vb) {
						continue
					} else {
						c.addToEnqueueMap(vbFlog.vb)
					}

					logging.Infof("%s [%s:%s:%d] vb: %d Sending streamRequestInfo size: %d",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, len(c.reqStreamCh))

					// Reason for sending this message at the time of sending STREAMREQ request
					// to DCP producer instead of of time when Eventing gets STREAMREQ response
					// from DCP producer is because we don't precisely know the start_seq_no for
					// for stream in later case, unless we maintain another data structure to
					// maintain that information
					c.sendVbFilterData(vbFlog.vb, vbFlog.seqNo, true)

					streamInfo := &streamRequestInfo{
						vb:         vbFlog.vb,
						vbBlob:     &vbBlob,
						startSeqNo: vbFlog.seqNo,
					}
					select {
					case c.reqStreamCh <- streamInfo:
					case <-c.stopConsumerCh:
						return
					}
					c.vbProcessingStats.updateVbStat(vbFlog.vb, "start_seq_no", vbFlog.seqNo)
					c.vbProcessingStats.updateVbStat(vbFlog.vb, "timestamp", time.Now().Format(time.RFC3339))
				} else {

					// Issuing high seq nos call to ascertain all vbuckets are back online(i.e. not stuck warm-up, flush etc)
				vbLabel:
					for {
						select {
						case <-c.stopConsumerCh:
							return
						default:
							vbSeqNos, err := util.BucketSeqnos(c.producer.NsServerHostPort(), "default", c.bucket)
							if err == nil {
								break vbLabel
							}
							logging.Errorf("%s [%s:%s:%d] Failed to fetch get_all_vb_seqnos, len vbSeqNos: %d err: %v",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), len(vbSeqNos), err)
						}
					}

					filterSeqNum := startSeqNo

					if vbBlob.LastSeqNoProcessed < startSeqNo {
						filterSeqNum = vbBlob.LastSeqNoProcessed
					}
					c.sendVbFilterData(vbFlog.vb, filterSeqNum, true)

					logging.Infof("%s [%s:%s:%d] vb: %d Retrying DCP stream start vbuuid: %d vbFlog startSeq: %d last processed seq: %d",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, vbBlob.VBuuid,
						vbFlog.seqNo, vbBlob.LastSeqNoProcessed)

					if c.checkIfAlreadyEnqueued(vbFlog.vb) {
						continue
					} else {
						c.addToEnqueueMap(vbFlog.vb)
					}

					logging.Infof("%s [%s:%s:%d] vb: %d Sending streamRequestInfo size: %d",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, len(c.reqStreamCh))

					streamInfo := &streamRequestInfo{
						vb:         vbFlog.vb,
						vbBlob:     &vbBlob,
						startSeqNo: filterSeqNum,
					}
					select {
					case c.reqStreamCh <- streamInfo:
					case <-c.stopConsumerCh:
						return
					}
					c.vbProcessingStats.updateVbStat(vbFlog.vb, "start_seq_no", filterSeqNum)
					c.vbProcessingStats.updateVbStat(vbFlog.vb, "timestamp", time.Now().Format(time.RFC3339))
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
	partitions := make([]uint16, cppWorkerPartitionCount)
	for i := 0; i < int(cppWorkerPartitionCount); i++ {
		partitions[i] = uint16(i)
	}

	c.cppThrPartitionMap = util.VbucketDistribution(partitions, c.cppWorkerThrCount)
}

func (c *Consumer) sendEvent(e *cb.DcpEvent) error {
	logPrefix := "Consumer::processTrappedEvent"

	if !c.producer.IsTrapEvent() {
		c.sendDcpEvent(e, false)
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
		c.startDebugger(e, instance)
	} else {
		c.sendDcpEvent(e, false)
	}
	return nil
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
				if !util.Contains(msg.vb, c.vbsRemainingToRestream) {
					c.vbsRemainingToRestream = append(c.vbsRemainingToRestream, msg.vb)
				}

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

				err := c.dcpRequestStreamHandle(msg.vb, msg.vbBlob, msg.startSeqNo)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return
				}

				if err != nil {
					c.Lock()
					if !util.Contains(msg.vb, c.vbsRemainingToRestream) {
						c.vbsRemainingToRestream = append(c.vbsRemainingToRestream, msg.vb)
					}
					c.Unlock()

				} else {
					logging.Infof("%s [%s:%s:%d] vb: %d DCP stream successfully requested", logPrefix, c.workerName, c.tcpPort, c.Pid(), msg.vb)
				}
			}(msg, c, logPrefix, &streamReqWG)

			streamReqWG.Wait()

		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Exiting streamReq processing routine", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}
