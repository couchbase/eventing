package consumer

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb/v2"
)

func (c *Consumer) controlRoutine() error {
	logPrefix := "Consumer::controlRoutine"

	defer c.controlRoutineWg.Done()

	for {
		select {
		case <-c.clusterStateChangeNotifCh:

			err := util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), c.retryCount, getEventingNodeAddrOpCallback, c)
			if err == common.ErrRetryTimeout {
				logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
				return err
			}

			// If this node is going out of cluster, we need to pause the c++ consumer altogether (similar to pausing the handler)
			if util.Contains(c.NodeUUID(), c.ejectNodesUUIDs) {
				c.PauseConsumer()
			}

			c.CloseAllRunningDcpFeeds()

			c.stopVbOwnerTakeoverCh = make(chan struct{})

			logging.Infof("%s [%s:%s:%d] Got notification that cluster state has changed",
				logPrefix, c.workerName, c.tcpPort, c.Pid())

			c.vbsStreamClosedRWMutex.Lock()
			c.vbsStreamClosed = make(map[uint16]bool)
			c.vbsStreamClosedRWMutex.Unlock()

			if !c.vbsStateUpdateRunning {
				logging.Infof("%s [%s:%s:%d] Kicking off vbsStateUpdate routine, isRebalanceOngoing %t",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), c.isRebalanceOngoing)
				go c.vbsStateUpdate()
			}

		case <-c.signalSettingsChangeCh:

			logging.Infof("%s [%s:%s:%d] Got notification for settings change",
				logPrefix, c.workerName, c.tcpPort, c.Pid())

			settingsPath := metakvAppSettingsPath + c.app.AppLocation
			sData, err := util.MetakvGet(settingsPath)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to fetch updated settings from metakv, err: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
				continue
			}

			settings := make(map[string]interface{})
			err = json.Unmarshal(sData, &settings)
			if err != nil {
				logging.Errorf("%s [%s:%s:%d] Failed to unmarshal settings received from metakv, err: %ru",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), err)
				continue
			}

			if val, ok := settings["log_level"]; ok {
				c.logLevel = val.(string)
				logging.SetLogLevel(util.GetLogLevel(c.logLevel))
				c.sendLogLevel(c.logLevel, false)
			}

			if val, ok := settings["timer_context_size"]; ok {
				c.timerContextSize = int64(val.(float64))
				c.sendTimerContextSize(c.timerContextSize, false)
			}

			if val, ok := settings["vb_ownership_giveup_routine_count"]; ok {
				c.vbOwnershipGiveUpRoutineCount = int(val.(float64))
			}

			if val, ok := settings["vb_ownership_takeover_routine_count"]; ok {
				c.vbOwnershipTakeoverRoutineCount = int(val.(float64))
			}

		case <-c.restartVbDcpStreamTicker.C:

			c.RLock()
			vbsToRestream := make([]uint16, len(c.vbsRemainingToRestream))
			copy(vbsToRestream, c.vbsRemainingToRestream)

			vbsRemainingToClose := make([]uint16, len(c.vbsRemainingToClose))
			copy(vbsRemainingToClose, c.vbsRemainingToClose)

			vbsRemainingToCleanup := make([]uint16, len(c.vbsRemainingToCleanup))
			copy(vbsRemainingToCleanup, c.vbsRemainingToCleanup)
			c.RUnlock()

			if len(vbsToRestream) == 0 && len(vbsRemainingToClose) == 0 && len(vbsRemainingToCleanup) == 0 {
				continue
			}

			// Verify if the app is deployed or not before trying to reopen vbucket DCP streams
			// for the ones which recently have returned STREAMEND. QE frequently does flush
			// on source bucket right after undeploy
			deployedApps := c.superSup.GetLocallyDeployedApps()
			if _, ok := deployedApps[c.app.AppLocation]; !ok && !c.isBootstrapping {

				c.Lock()
				c.vbsRemainingToRestream = make([]uint16, 0)
				c.vbsRemainingToClose = make([]uint16, 0)
				c.Unlock()

				logging.Infof("%s [%s:%s:%d] Discarding request to restream vbs: %s and vbsRemainingToClose: %s as the app has been undeployed",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), util.Condense(vbsToRestream), util.Condense(vbsRemainingToClose))
				continue
			}

			sort.Sort(util.Uint16Slice(vbsRemainingToCleanup))
			logging.Infof("%s [%s:%s:%d] vbsRemainingToCleanup len: %d dump: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), len(vbsRemainingToCleanup), util.Condense(vbsRemainingToCleanup))

			metadataCorrectedVbs := make([]uint16, 0)
			for _, vb := range vbsRemainingToCleanup {
				if c.producer.IsPlannerRunning() {
					continue
				}

				if !c.checkIfCurrentConsumerShouldOwnVb(vb) {
					err := c.cleanupVbMetadata(vb)
					if err == common.ErrRetryTimeout {
						logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
						return err
					} else if err == common.ErrEncryptionLevelChanged {
						continue
					}

					if err == nil {
						metadataCorrectedVbs = append(metadataCorrectedVbs, vb)
					}
				} else if c.checkIfVbAlreadyOwnedByCurrConsumer(vb) {
					metadataCorrectedVbs = append(metadataCorrectedVbs, vb)
				}
			}

			c.Lock()
			diff := util.VbsSliceDiff(metadataCorrectedVbs, c.vbsRemainingToCleanup)
			c.vbsRemainingToCleanup = make([]uint16, len(diff))
			copy(c.vbsRemainingToCleanup, diff)

			sort.Sort(util.Uint16Slice(c.vbsRemainingToCleanup))
			if len(c.vbsRemainingToCleanup) > 0 {
				logging.Infof("%s [%s:%s:%d] vbsRemainingToCleanup => remaining len: %d dump: %v",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), len(c.vbsRemainingToCleanup), util.Condense(c.vbsRemainingToCleanup))
			}
			c.Unlock()

			sort.Sort(util.Uint16Slice(vbsRemainingToClose))
			logging.Infof("%s [%s:%s:%d] vbsRemainingToClose len: %d dump: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), len(vbsRemainingToClose), util.Condense(vbsRemainingToClose))

			for _, vb := range vbsRemainingToClose {
				if !c.checkIfVbAlreadyOwnedByCurrConsumer(vb) {
					continue
				}

				if c.checkIfCurrentConsumerShouldOwnVb(vb) {
					continue
				}

				logging.Infof("%s [%s:%s:%d] vb: %d Issuing dcp close stream", logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
				c.dcpCloseStreamCounter++

				c.RLock()
				dcpFeed := c.vbDcpFeedMap[vb]
				c.RUnlock()

				err := dcpFeed.DcpCloseStream(vb, vb)
				if err != nil {
					c.dcpCloseStreamErrCounter++
					logging.Errorf("%s [%s:%s:%d] vb: %v Failed to close dcp stream, err: %v",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vb, err)
				} else {
					logging.Infof("%s [%s:%s:%d] vb: %v Issued dcp close stream as current worker isn't supposed to own per plan",
						logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
				}

				lastSeqNo := c.vbProcessingStats.getVbStat(uint16(vb), "last_read_seq_no").(uint64)
				c.vbProcessingStats.updateVbStat(vb, "seq_no_after_close_stream", lastSeqNo)
				c.vbProcessingStats.updateVbStat(vb, "timestamp", time.Now().Format(time.RFC3339))

				var vbBlob vbucketKVBlob
				vbKey := common.GetCheckpointKey(c.app, vb, common.Checkpoint)

				err = c.updateCheckpoint(vbKey, vb, &vbBlob)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return err
				} else if err == common.ErrEncryptionLevelChanged {
					continue
				}
			}

			sort.Sort(util.Uint16Slice(vbsToRestream))
			logging.Infof("%s [%s:%s:%d] vbsToRestream len: %d dump: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), len(vbsToRestream), util.Condense(vbsToRestream))

			var vbsFailedToStartStream []uint16

			for _, vb := range vbsToRestream {
				if c.checkIfVbAlreadyOwnedByCurrConsumer(vb) {
					continue
				}

				// During Eventing+KV swap rebalance:
				// STREAMEND received because of outgoing KV node adds up entries in vbsToRestream,
				// but when eventing node receives rebalance notification it may not need to restream those
				// vbuckets as per the planner's output. Hence additional checking to verify if the worker
				// should own the vbucket stream
				if !c.checkIfCurrentConsumerShouldOwnVb(vb) {
					continue
				}

				var vbBlob vbucketKVBlob
				var cas gocb.Cas
				var isNoEnt bool
				vbKey := common.GetCheckpointKey(c.app, vb, common.Checkpoint)

				logging.Infof("%s [%s:%s:%d] vb: %v, reclaiming it back by restarting dcp stream",
					logPrefix, c.workerName, c.tcpPort, c.Pid(), vb)
				var operr error
				err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), c.retryCount, getOpCallback,
					c, c.producer.AddMetadataPrefix(vbKey), &vbBlob, &cas, &operr, true, &isNoEnt, true)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return err
				} else if operr == common.ErrEncryptionLevelChanged {
					continue
				}

				err = c.updateVbOwnerAndStartDCPStream(vbKey, vb, &vbBlob)
				if err == common.ErrRetryTimeout {
					logging.Errorf("%s [%s:%s:%d] Exiting due to timeout", logPrefix, c.workerName, c.tcpPort, c.Pid())
					return err
				}
				if err != nil {
					c.purgeVbStreamRequested(logPrefix, vb)
					vbsFailedToStartStream = append(vbsFailedToStartStream, vb)
				}
			}

			logging.Infof("%s [%s:%s:%d] vbsFailedToStartStream => len: %v dump: %v",
				logPrefix, c.workerName, c.tcpPort, c.Pid(), len(vbsFailedToStartStream), util.Condense(vbsFailedToStartStream))

			vbsToRestream = util.VbsSliceDiff(vbsFailedToStartStream, vbsToRestream)

			c.Lock()
			diff = util.VbsSliceDiff(vbsToRestream, c.vbsRemainingToRestream)
			c.vbsRemainingToRestream = make([]uint16, len(diff))
			copy(c.vbsRemainingToRestream, diff)
			c.Unlock()

		case <-c.stopConsumerCh:
			logging.Infof("%s [%s:%s:%d] Exiting control routine", logPrefix, c.workerName, c.tcpPort, c.Pid())
			return nil
		}
	}
}
