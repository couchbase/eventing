package consumer

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/gocb"
)

func (c *Consumer) controlRoutine() {
	for {
		select {
		case <-c.clusterStateChangeNotifCh:

			util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

			logging.Infof("CRCR[%s:%s:%s:%d] Got notification that cluster state has changed(could also trigger on app deploy)",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid())

			c.isRebalanceOngoing = true
			go c.vbsStateUpdate()

		case <-c.signalSettingsChangeCh:

			logging.Infof("CRCR[%s:%s:%s:%d] Got notification for settings change",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid())

			settingsPath := metakvAppSettingsPath + c.app.AppName
			sData, err := util.MetakvGet(settingsPath)
			if err != nil {
				logging.Errorf("CRCR[%s:%s:%s:%d] Failed to fetch updated settings from metakv, err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
				continue
			}

			settings := make(map[string]interface{})
			err = json.Unmarshal(sData, &settings)
			if err != nil {
				logging.Errorf("CRCR[%s:%s:%s:%d] Failed to unmarshal settings received from metakv, err: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), err)
				continue
			}

			c.stopCheckpointingCh <- struct{}{}
			c.checkpointInterval = time.Duration(settings["checkpoint_interval"].(float64)) * time.Millisecond
			go c.doLastSeqNoCheckpoint()

			c.logLevel = settings["log_level"].(string)
			logging.SetLogLevel(util.GetLogLevel(c.logLevel))
			c.sendLogLevel(c.logLevel, false)

			c.skipTimerThreshold = int(settings["skip_timer_threshold"].(float64))

			c.timerProcessingTickInterval = time.Duration(settings["timer_processing_tick_interval"].(float64)) * time.Millisecond

			c.timerRWMutex.RLock()
			for k := range c.timerProcessingWorkerSignalCh {
				k.stopCh <- struct{}{}
			}
			c.timerRWMutex.RUnlock()

			c.timerRWMutex.Lock()
			c.timerProcessingWorkerSignalCh = make(map[*timerProcessingWorker]chan struct{})
			c.timerProcessingRunningWorkers = make([]*timerProcessingWorker, 0)
			c.timerRWMutex.Unlock()

			// Spawning DocID based timer processing routines
			c.vbTimerProcessingWorkerAssign(true)

			c.timerRWMutex.RLock()
			for _, r := range c.timerProcessingRunningWorkers {
				go r.processTimerEvents("", "", false)
			}
			c.timerRWMutex.RUnlock()

			c.nonDocTimerStopCh <- struct{}{}
			go c.processNonDocTimerEvents("", "", false)

			if val, ok := settings["deadline_timeout"]; ok {
				c.socketTimeout = time.Duration(val.(float64)) * time.Second
			}

			if val, ok := settings["vb_ownership_giveup_routine_count"]; ok {
				c.vbOwnershipGiveUpRoutineCount = int(val.(float64))
			} else {
				c.vbOwnershipGiveUpRoutineCount = 1
			}

			if val, ok := settings["vb_ownership_takeover_routine_count"]; ok {
				c.vbOwnershipTakeoverRoutineCount = int(val.(float64))
			} else {
				c.vbOwnershipTakeoverRoutineCount = 1
			}

		case <-c.restartVbDcpStreamTicker.C:

		retryVbsRemainingToRestream:
			vbsToRestream := c.vbsRemainingToRestream

			// Verify if the app is deployed or not before trying to reopen vbucket DCP streams
			// for the ones which recently have returned STREAMEND. QE frequently does flush
			// on source bucket right after undeploy
			deployedApps := c.superSup.GetDeployedApps()
			if _, ok := deployedApps[c.app.AppName]; !ok {
				c.vbsRemainingToRestream = make([]uint16, 0)
				logging.Infof("CRCR[%s:%s:%s:%d] Discarding request to restream vbs: %v as the app has been undeployed",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbsToRestream)
				continue
			}

			if len(vbsToRestream) == 0 {
				continue
			}

			sort.Sort(util.Uint16Slice(vbsToRestream))
			logging.Verbosef("CRCR[%s:%s:%s:%d] vbsToRestream len: %v dump: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), len(vbsToRestream), vbsToRestream)

			var vbsFailedToStartStream []uint16

			for _, vbno := range vbsToRestream {
				if c.checkIfVbAlreadyOwnedByCurrConsumer(vbno) {
					continue
				}

				var vbBlob vbucketKVBlob
				var cas gocb.Cas
				vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vbno)))

				logging.Debugf("CRCR[%s:%s:%s:%d] vbno: %v, reclaiming it back by restarting dcp stream",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbno)
				util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

				err := c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, true)
				if err != nil {
					vbsFailedToStartStream = append(vbsFailedToStartStream, vbno)
				}
			}

			logging.Debugf("CRCR[%s:%s:%s:%d] vbsFailedToStartStream => len: %v dump: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid(), len(vbsFailedToStartStream), vbsFailedToStartStream)

			vbsToRestream = util.VbsSliceDiff(vbsFailedToStartStream, vbsToRestream)

			c.Lock()
			diff := util.VbsSliceDiff(vbsToRestream, c.vbsRemainingToRestream)
			c.vbsRemainingToRestream = diff
			vbsRemainingToRestream := len(c.vbsRemainingToRestream)
			c.Unlock()

			sort.Sort(util.Uint16Slice(diff))

			if vbsRemainingToRestream > 0 {
				logging.Verbosef("CRCR[%s:%s:%s:%d] Retrying vbsToRestream, remaining len: %v dump: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbsRemainingToRestream, diff)
				goto retryVbsRemainingToRestream
			}

		case vb := <-c.signalStoreTimerPlasmaCloseCh:
			// Rebalance takeover routine will send signal on this channel to signify
			// stopping of any plasma.Writer instance for a specific vbucket
			c.plasmaStoreRWMutex.Lock()
			delete(c.vbPlasmaWriter, vb)
			c.plasmaStoreRWMutex.Unlock()

			// sends ack message back to rebalance takeover routine, so that it could
			// safely call Close() on vb specific plasma store
			c.signalStoreTimerPlasmaCloseAckCh <- vb

		case <-c.stopControlRoutineCh:
			return
		}
	}
}
