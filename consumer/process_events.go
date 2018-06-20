package consumer

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/dcp"
	mcd "github.com/couchbase/eventing/dcp/transport"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
)

func (c *Consumer) processEvents() {
	logPrefix := "Consumer::processEvents"

	var timerMsgCounter uint64
	xattrprefix := strconv.Itoa(int(c.app.HandlerUUID))
	for {

		if c.cppQueueSizes != nil {
			if c.workerQueueCap < c.cppQueueSizes.AggQueueSize || c.feedbackQueueCap < c.cppQueueSizes.DocTimerQueueSize || c.workerQueueMemCap < c.cppQueueSizes.AggQueueMemory {
				logging.Infof("%s [%s:%s:%d] Throttling events to cpp worker, aggregate queue size: %v cap: %v feedback queue size: %v cap: %v aggregate queue memory: %v cap %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), c.cppQueueSizes.AggQueueSize, c.workerQueueCap,
					c.cppQueueSizes.DocTimerQueueSize, c.feedbackQueueCap, c.cppQueueSizes.AggQueueMemory, c.workerQueueMemCap)
				time.Sleep(10 * time.Millisecond)
			}
		}

		select {
		case e, ok := <-c.aggDCPFeed:
			if ok == false {
				logging.Infof("%s [%s:%s:%d] Closing DCP feed for bucket %q",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), c.bucket)

				c.stopCheckpointingCh <- struct{}{}
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

				if c.debuggerState == startDebug {

					c.signalUpdateDebuggerInstBlobCh <- struct{}{}

					select {
					case <-c.signalInstBlobCasOpFinishCh:
						select {
						case <-c.signalStartDebuggerCh:
							go c.startDebuggerServer()
							c.sendMsgToDebugger = true
						default:
						}
					}

					c.debuggerState = stopDebug
				}

				switch e.Datatype {
				case dcpDatatypeJSON:
					if !c.sendMsgToDebugger {
						c.dcpMutationCounter++
						c.sendDcpEvent(e, c.sendMsgToDebugger)
					} else {
						c.dcpMutationCounter++
						go c.sendDcpEvent(e, c.sendMsgToDebugger)
					}
				case dcpDatatypeJSONXattr:
					totalXattrLen := binary.BigEndian.Uint32(e.Value[0:])
					totalXattrData := e.Value[4 : 4+totalXattrLen-1]

					logging.Tracef("%s [%s:%s:%d] key: %ru totalXattrLen: %v totalXattrData: %ru",
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

						if len(frameData) > len(xattrprefix) {
							if bytes.Compare(frameData[:len(xattrprefix)], []byte(xattrprefix)) == 0 {
								toParse := frameData[len(xattrprefix)+1:]

								err := json.Unmarshal(toParse, &xMeta)
								if err != nil {
									continue
								}
							}
						}
					}

					logging.Tracef("%s [%s:%s:%d] Key: %ru xmeta dump: %ru",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key), fmt.Sprintf("%#v", xMeta))

					// Validating for eventing xattr fields
					if xMeta.Cas != "" {
						cas, err := util.ConvertBigEndianToUint64([]byte(xMeta.Cas))
						if err != nil {
							logging.Errorf("%s [%s:%s:%d] Key: %ru failed to convert cas string from kv to uint64, err: %v",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key), err)
							continue
						}

						logging.Tracef("%s [%s:%s:%d] Key: %ru decoded cas: %v dcp cas: %v",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key), cas, e.Cas)

						// Send mutation to V8 CPP worker _only_ when DcpEvent.Cas != Cas field in xattr
						if cas != e.Cas {
							e.Value = e.Value[4+totalXattrLen:]

							if crc32.Update(0, c.crcTable, e.Value) != xMeta.Digest {
								if !c.sendMsgToDebugger {
									logging.Tracef("%s [%s:%s:%d] Sending key: %ru to be processed by JS handlers as cas & crc have mismatched",
										logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key))
									c.dcpMutationCounter++
									c.sendDcpEvent(e, c.sendMsgToDebugger)
								} else {
									c.dcpMutationCounter++
									go c.sendDcpEvent(e, c.sendMsgToDebugger)
								}
							} else {

								logging.Tracef("%s [%s:%s:%d] Sending key: %ru to be stored in plasma",
									logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key))

								// Enabling it until MB-28779 gets resolved
								for _, timerEntry := range xMeta.Timers {

									data := strings.Split(timerEntry, "::")

									if len(data) == 3 {
										pEntry := &plasmaStoreEntry{
											callbackFn: data[2],
											key:        string(e.Key),
											timerTs:    data[1],
											vb:         e.VBucket,
										}

										c.plasmaStoreCh <- pEntry
										logging.Tracef("%s [%s:%s:%d] Sending key: %ru to be stored in plasma, timer entry: %v pEntry: %#v",
											logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key), timerEntry, pEntry)
									}
								}
							}
						} else {

							// Enabling it until MB-28779 gets resolved
							for _, timerEntry := range xMeta.Timers {

								data := strings.Split(timerEntry, "::")

								if len(data) == 3 {
									pEntry := &plasmaStoreEntry{
										callbackFn: data[2],
										key:        string(e.Key),
										timerTs:    data[1],
										vb:         e.VBucket,
									}

									c.plasmaStoreCh <- pEntry
									logging.Tracef("%s [%s:%s:%d] Sending key: %ru to be stored in plasma, timer entry: %v pEntry: %#v",
										logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key), timerEntry, pEntry)
								}
							}

							logging.Tracef("%s [%s:%s:%d] Skipping recursive mutation for key: %ru vb: %v, xmeta: %ru",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key), e.VBucket, fmt.Sprintf("%#v", xMeta))

						}
					} else {
						e.Value = e.Value[4+totalXattrLen:]
						if !c.sendMsgToDebugger {
							logging.Tracef("%s [%s:%s:%d] Sending key: %ru to be processed by JS handlers because no eventing xattrs",
								logPrefix, c.workerName, c.tcpPort, c.Pid(), string(e.Key))
							c.dcpMutationCounter++
							c.sendDcpEvent(e, c.sendMsgToDebugger)
						} else {
							c.dcpMutationCounter++
							go c.sendDcpEvent(e, c.sendMsgToDebugger)
						}
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

				if c.debuggerState == startDebug {

					c.signalUpdateDebuggerInstBlobCh <- struct{}{}

					select {
					case <-c.signalInstBlobCasOpFinishCh:
						select {
						case <-c.signalStartDebuggerCh:
							go c.startDebuggerServer()
							c.sendMsgToDebugger = true
						default:
						}
					}
					c.debuggerState = stopDebug
				}

				if !c.sendMsgToDebugger {
					c.dcpDeletionCounter++
					c.sendDcpEvent(e, c.sendMsgToDebugger)
				} else {
					c.dcpDeletionCounter++
					go c.sendDcpEvent(e, c.sendMsgToDebugger)
				}

			case mcd.DCP_STREAMREQ:

				logging.Infof("%s [%s:%s:%d] vb: %d got STREAMREQ status: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, e.Status)

				if e.Status == mcd.SUCCESS {

					vbFlog := &vbFlogEntry{statusCode: e.Status, streamReqRetry: false, vb: e.VBucket}

					var vbBlob vbucketKVBlob
					var cas gocb.Cas

					vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, e.VBucket)

					err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, false)
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

					// Update metadata with latest vbuuid and rolback seq no.
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

					entry := OwnershipEntry{
						AssignedWorker: c.ConsumerName(),
						CurrentVBOwner: c.HostPortAddr(),
						Operation:      dcpStreamRunning,
						SeqNo:          startSeqNo,
						Timestamp:      time.Now().String(),
					}

					err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySRSCallback, c, vbKey, &vbBlob, &entry)
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
						logging.Infof("%s [%s:%s:%d] vb: %d STREAMREQ failed. Purging entry from vbStreamRequested",
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

					err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySRFCallback, c, vbKey, &entry)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
					}

					logging.Infof("%s [%s:%s:%d] vb: %d STREAMREQ Inserting entry: %#v to vbFlogChan",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, vbFlog)
					c.vbFlogChan <- vbFlog
				}
			case mcd.DCP_STREAMEND:
				// Cleanup entry for vb for which stream_end has been received from vbPorcessingStats
				// which will allow vbTakeOver background routine to start up new stream from
				// new KV node, where the vbucket has been migrated

				logging.Infof("%s [%s:%s:%d] vb: %v, got STREAMEND", logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket)

				c.vbsStreamRRWMutex.Lock()
				if _, ok := c.vbStreamRequested[e.VBucket]; ok {
					logging.Infof("%s [%s:%s:%d] vb: %d Purging entry from vbStreamRequested",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket)

					delete(c.vbStreamRequested, e.VBucket)
				}
				c.vbsStreamRRWMutex.Unlock()

				// Store the latest state of vbucket processing stats in the metadata bucket
				vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, e.VBucket)

				seqNo := c.vbProcessingStats.getVbStat(e.VBucket, "last_read_seq_no").(uint64)

				entry := OwnershipEntry{
					AssignedWorker: c.ConsumerName(),
					CurrentVBOwner: c.HostPortAddr(),
					Operation:      dcpStreamStopped,
					SeqNo:          seqNo,
					Timestamp:      time.Now().String(),
				}

				err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySECallback, c, vbKey, &entry)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return
				}

				c.filterVbEventsRWMutex.Lock()
				delete(c.filterVbEvents, e.VBucket)
				c.filterVbEventsRWMutex.Unlock()

				var vbBlob vbucketKVBlob
				var cas gocb.Cas

				c.vbsStreamClosedRWMutex.Lock()
				_, cUpdated := c.vbsStreamClosed[e.VBucket]
				if !cUpdated {
					c.vbsStreamClosed[e.VBucket] = true
				}
				c.vbsStreamClosedRWMutex.Unlock()

				lastSeqNo := c.vbProcessingStats.getVbStat(e.VBucket, "last_read_seq_no").(uint64)
				c.vbProcessingStats.updateVbStat(e.VBucket, "seq_no_at_stream_end", lastSeqNo)
				c.vbProcessingStats.updateVbStat(e.VBucket, "timestamp", time.Now().Format(time.RFC3339))

				if !cUpdated {
					logging.Infof("%s [%s:%s:%d] vb: %v updating metadata about dcp stream close",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket)

					err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, false)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
					}

					err = c.updateCheckpoint(vbKey, e.VBucket, &vbBlob)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
					}
				}

				if c.checkIfCurrentConsumerShouldOwnVb(e.VBucket) {
					logging.Infof("%s [%s:%s:%d] vb: %v got STREAMEND, needs to be reclaimed",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket)

					vbFlog := &vbFlogEntry{signalStreamEnd: true, vb: e.VBucket}
					logging.Infof("%s [%s:%s:%d] vb: %d STREAMEND Inserting entry: %#v to vbFlogChan",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), e.VBucket, vbFlog)
					c.vbFlogChan <- vbFlog

					err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, false)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
					}

					err = c.updateCheckpoint(vbKey, e.VBucket, &vbBlob)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
					}

					c.Lock()
					c.vbsRemainingToRestream = append(c.vbsRemainingToRestream, e.VBucket)
					c.Unlock()
				}

			default:
			}

		case e, ok := <-c.docTimerEntryCh:
			if ok == false {
				logging.Infof("%s [%s:%s:%d] Closing doc timer chan", logPrefix, c.workerName, c.tcpPort, c.Pid())

				c.stopCheckpointingCh <- struct{}{}
				return
			}

			c.doctimerMessagesProcessed++
			c.sendDocTimerEvent(e, c.sendMsgToDebugger)

		case e, ok := <-c.cronTimerEntryCh:
			if ok == false {
				logging.Infof("%s [%s:%s:%d] Closing non_doc timer chan", logPrefix, c.workerName, c.tcpPort, c.Pid())

				c.stopCheckpointingCh <- struct{}{}
				return
			}

			c.crontimerMessagesProcessed += uint64(e.msgCount)
			c.sendCronTimerEvent(e, c.sendMsgToDebugger)

		case <-c.statsTicker.C:

			vbsOwned := c.getCurrentlyOwnedVbs()
			if len(vbsOwned) > 0 {

				c.msgProcessedRWMutex.RLock()
				countMsg, dcpOpCount, tStamp := util.SprintDCPCounts(c.dcpMessagesProcessed)

				diff := tStamp.Sub(c.opsTimestamp)

				dcpOpsDiff := dcpOpCount - c.dcpOpsProcessed
				timerOpsDiff := (c.doctimerMessagesProcessed + c.crontimerMessagesProcessed) - timerMsgCounter
				timerMsgCounter = (c.doctimerMessagesProcessed + c.crontimerMessagesProcessed)

				seconds := int(diff.Nanoseconds() / (1000 * 1000 * 1000))
				if seconds > 0 {
					c.dcpOpsProcessedPSec = int(dcpOpsDiff) / seconds
					c.timerMessagesProcessedPSec = int(timerOpsDiff) / seconds
				}

				logging.Infof("%s [%s:%s:%d] DCP events: %s V8 events: %s Timer events: Doc: %v Cron: %v, vbs owned len: %d vbs owned: %v Plasma stats: Insert: %v Delete: %v Lookup: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), countMsg, util.SprintV8Counts(c.v8WorkerMessagesProcessed),
					c.doctimerMessagesProcessed, c.crontimerMessagesProcessed, len(vbsOwned), util.Condense(vbsOwned),
					c.plasmaInsertCounter, c.plasmaDeleteCounter, c.plasmaLookupCounter)

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

		case <-c.signalStopDebuggerCh:
			c.debuggerState = stopDebug
			c.consumerSup.Remove(c.debugClientSupToken)

			c.debuggerState = debuggerOpcode

			// Reset debuggerInstanceAddr blob, otherwise next debugger session can't start
			dInstAddrKey := fmt.Sprintf("%s::%s", c.app.AppName, debuggerInstanceAddr)
			dInstAddrBlob := &common.DebuggerInstanceAddrBlobVer{
				common.DebuggerInstanceAddrBlob{},
				util.EventingVer(),
			}
			err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, setOpCallback, c, dInstAddrKey, dInstAddrBlob)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Exiting processEvents routine",
				logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}

func (c *Consumer) startDcp(flogs couchbase.FailoverLog) error {
	logPrefix := "Consumer::startDcp"

	logging.Infof("%s [%s:%s:%d] no. of vbs owned len: %d dump: %s",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), len(c.vbnos), util.Condense(c.vbnos))

	err := util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getEventingNodeAddrOpCallback, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return common.ErrRetryTimeout
	}

	vbSeqnos, err := util.BucketSeqnos(c.producer.NsServerHostPort(), "default", c.bucket)
	if err != nil && c.dcpStreamBoundary != common.DcpEverything {
		logging.Errorf("%s [%s:%s:%d] Failed to fetch vb seqnos, err: %v", logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
		return nil
	}

	logging.Debugf("%s [%s:%s:%d] get_all_vb_seqnos: len => %d dump => %v",
		logPrefix, c.workerName, c.tcpPort, c.Pid(), len(vbSeqnos), vbSeqnos)

	var vbs []uint16

	for vb, flog := range flogs {

		vbuuid, _, _ := flog.Latest()

		vbKey := fmt.Sprintf("%s::vb::%d", c.app.AppName, vb)
		var vbBlob vbucketKVBlob
		var start uint64
		var cas gocb.Cas
		var isNoEnt bool

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, true, &isNoEnt)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return common.ErrRetryTimeout
		}

		if isNoEnt {

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
			err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, setOpCallback, c, vbKey, &vbBlobVer)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return common.ErrRetryTimeout
			}

			vbs = append(vbs, vb)
			switch c.dcpStreamBoundary {
			case common.DcpEverything:
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
			if vbBlob.NodeUUID == c.NodeUUID() && vbBlob.AssignedWorker == c.ConsumerName() {
				vbs = append(vbs, vb)

				logging.Infof("%s [%s:%s:%d] vb: %d Sending streamRequestInfo size: %d",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, len(c.reqStreamCh))

				c.reqStreamCh <- &streamRequestInfo{
					vb:         vb,
					vbBlob:     &vbBlob,
					startSeqNo: vbBlob.LastSeqNoProcessed,
				}
				c.vbProcessingStats.updateVbStat(vb, "start_seq_no", vbBlob.LastSeqNoProcessed)
				c.vbProcessingStats.updateVbStat(vb, "timestamp", time.Now().Format(time.RFC3339))

			}
		}

	}

	err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, checkIfVbStreamsOpenedCallback, c, vbs)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return common.ErrRetryTimeout
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
					logging.Infof("%s [%s:%s:%d] Throttling, aggDCPFeed memory: %d bytes aggDCPFeedMemCap: %d\n",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), c.aggDCPFeedMem, c.aggDCPFeedMemCap)
					time.Sleep(10 * time.Millisecond)
				}

				if !c.isTerminateRunning {
					atomic.AddInt64(&c.aggDCPFeedMem, int64(len(e.Value)))
					c.aggDCPFeed <- e
				}
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
			return common.ErrRetryTimeout
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

		for _, vbno := range vbsMetadataToUpdate {
			err := c.clearUpOwnershipInfoFromMeta(vbno)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return common.ErrRetryTimeout
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

	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, false)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return common.ErrRetryTimeout
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

	c.vbsStreamClosedRWMutex.Lock()
	_, cUpdated := c.vbsStreamClosed[vb]
	if !cUpdated {
		c.vbsStreamClosed[vb] = true
	}
	c.vbsStreamClosedRWMutex.Unlock()

	if !cUpdated {
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySECallback, c, vbKey, &entry)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return common.ErrRetryTimeout
		}

		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, updateCheckpointCallback, c, vbKey, &vbBlob)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return common.ErrRetryTimeout
		}
	}

	c.vbProcessingStats.updateVbStat(vb, "assigned_worker", vbBlob.AssignedWorker)
	c.vbProcessingStats.updateVbStat(vb, "current_vb_owner", vbBlob.CurrentVBOwner)
	c.vbProcessingStats.updateVbStat(vb, "dcp_stream_status", vbBlob.DCPStreamStatus)
	c.vbProcessingStats.updateVbStat(vb, "node_uuid", vbBlob.NodeUUID)
	return nil
}

func (c *Consumer) dcpRequestStreamHandle(vb uint16, vbBlob *vbucketKVBlob, start uint64) error {
	logPrefix := "Consumer::dcpRequestStreamHandle"

	c.cbBucket.Refresh()

	err := util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getKvVbMap, c)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return common.ErrRetryTimeout
	}

	vbKvAddr := c.kvVbMap[vb]

	// Closing feeds for KV hosts which are no more present in kv vb map
	err = c.cleanupStaleDcpFeedHandles()
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
		return common.ErrRetryTimeout
	}

	c.hostDcpFeedRWMutex.Lock()
	dcpFeed, ok := c.kvHostDcpFeedMap[vbKvAddr]
	if !ok {
		feedName := couchbase.NewDcpFeedName(c.HostPortAddr() + "_" + vbKvAddr + "_" + c.workerName)
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, startDCPFeedOpCallback, c, feedName, vbKvAddr)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return common.ErrRetryTimeout
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

	c.dcpStreamReqCounter++
	err = dcpFeed.DcpRequestStream(vb, opaque, flags, vbBlob.VBuuid, start, end, snapStart, snapEnd)
	if err != nil {
		c.dcpStreamReqErrCounter++
		logging.Errorf("%s [%s:%s:%d] vb: %d STREAMREQ call failed on dcpFeed: %v, err: %v",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, dcpFeed.GetName(), err)

		c.vbsStreamRRWMutex.Lock()
		if _, ok := c.vbStreamRequested[vb]; ok {
			logging.Infof("%s [%s:%s:%d] vb: %d Purging entry from vbStreamRequested",
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
		err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, addOwnershipHistorySRRCallback, c, vbKey, &entry)
		if err == common.ErrRetryTimeout {
			logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return err
		}

		c.vbProcessingStats.updateVbStat(vb, "vb_stream_request_metadata_updated", true)

		logging.Infof("%s [%s:%s:%d] vb: %d Updated checkpoint blob to indicate STREAMREQ was issued",
			logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
	}

	return err
}

func (c *Consumer) handleFailoverLog() {
	logPrefix := "Consumer::handlerFailoverLog"

	for {
		select {
		case vbFlog := <-c.vbFlogChan:
			logging.Infof("%s [%s:%s:%d] vb: %d Got entry from vbFlogChan: %#v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, vbFlog)

			c.inflightDcpStreamsRWMutex.Lock()
			if _, exists := c.inflightDcpStreams[vbFlog.vb]; exists {
				logging.Infof("%s [%s:%s:%d] vb: %d Purging entry from inflightDcpStreams",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb)
				delete(c.inflightDcpStreams, vbFlog.vb)
			}
			c.inflightDcpStreamsRWMutex.Unlock()

			if vbFlog.signalStreamEnd {
				logging.Infof("%s [%s:%s:%d] vb: %d Got STREAMEND", logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb)
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

				err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback, c, vbKey, &vbBlob, &cas, true, &isNoEnt)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return
				}

				if vbFlog.statusCode == mcd.ROLLBACK {
					logging.Infof("%s [%s:%s:%d] vb: %v Rollback requested by DCP. Retrying DCP stream start vbuuid: %d startSeq: %d",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, vbBlob.VBuuid, vbFlog.seqNo)

					var flogs couchbase.FailoverLog

					err = util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getFailoverLogOpAllVbucketsCallback, c, c.cbBucket, &flogs, vbFlog.vb)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return
					}

					if flog, ok := flogs[vbFlog.vb]; ok {
						vbuuid, startSeqNo, err := flog.Latest()
						if err != nil {
							c.Lock()
							c.vbsRemainingToRestream = append(c.vbsRemainingToRestream, vbFlog.vb)
							c.Unlock()
							continue
						}

						vbBlob.VBuuid = vbuuid
						logging.Infof("%s [%s:%s:%d] vb: %d Sending streamRequestInfo size: %d",
							logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, len(c.reqStreamCh))

						c.reqStreamCh <- &streamRequestInfo{
							vb:         vbFlog.vb,
							vbBlob:     &vbBlob,
							startSeqNo: startSeqNo,
						}

						c.vbProcessingStats.updateVbStat(vbFlog.vb, "start_seq_no", startSeqNo)
						c.vbProcessingStats.updateVbStat(vbFlog.vb, "timestamp", time.Now().Format(time.RFC3339))
					}
				} else {
					logging.Infof("%s [%s:%s:%d] vb: %v Retrying DCP stream start vbuuid: %d startSeq: %d",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, vbBlob.VBuuid, vbFlog.seqNo)

					logging.Infof("%s [%s:%s:%d] vb: %d Sending streamRequestInfo size: %d",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vbFlog.vb, len(c.reqStreamCh))

					c.reqStreamCh <- &streamRequestInfo{
						vb:         vbFlog.vb,
						vbBlob:     &vbBlob,
						startSeqNo: 0,
					}
					c.vbProcessingStats.updateVbStat(vbFlog.vb, "start_seq_no", 0)
					c.vbProcessingStats.updateVbStat(vbFlog.vb, "timestamp", time.Now().Format(time.RFC3339))
				}

			}

		case <-c.stopHandleFailoverLogCh:
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

func (c *Consumer) processReqStreamMessages() {
	logPrefix := "Consumer::processReqStreamMessages"

	for {
		select {
		case msg, ok := <-c.reqStreamCh:
			logging.Infof("%s [%s:%s:%d] vb: %d reqStreamCh size: %d Got request to stream",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), msg.vb, len(c.reqStreamCh))

			if !ok {
				logging.Infof("%s [%s:%s:%d] Returning streamReq processing routine", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return
			}

			if !c.checkIfCurrentConsumerShouldOwnVb(msg.vb) {
				logging.Infof("%s [%s:%s:%d] vb: %d Skipping stream request as worker isn't supposed to own it",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), msg.vb)
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

		case <-c.stopReqStreamProcessCh:
			logging.Infof("%s [%s:%s:%d] Exiting streamReq processing routine", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return
		}
	}
}
