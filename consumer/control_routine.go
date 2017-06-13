package consumer

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

func (c *Consumer) controlRoutine() {
	for {
		select {
		case <-c.clusterStateChangeNotifCh:

			util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

			logging.Infof("CRCR[%s:%s:%s:%d] Got notifcation that cluster state has changed",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid())

			c.isRebalanceOngoing = true
			c.vbsStateUpdate()
			c.isRebalanceOngoing = false

		case <-c.signalSettingsChangeCh:

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
			c.sendLogLevel(c.logLevel)

			c.skipTimerThreshold = int(settings["skip_timer_threshold"].(float64))

			c.timerProcessingTickInterval = time.Duration(settings["timer_processing_tick_interval"].(float64)) * time.Millisecond
			for k := range c.timerProcessingWorkerSignalCh {
				k.stopCh <- struct{}{}
			}

			c.vbTimerProcessingWorkerAssign(true)
			for _, r := range c.timerProcessingRunningWorkers {
				go r.processTimerEvents()
			}

		case <-c.restartVbDcpStreamTicker.C:

		retryVbsRemainingToRestream:
			c.RLock()
			vbsToRestream := c.vbsRemainingToRestream
			c.RUnlock()

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
				var cas uint64
				vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vbno)))

				logging.Infof("CRCR[%s:%s:%s:%d] vbno: %v, reclaiming it back by restarting dcp stream",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbno)
				util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)

				err := c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
				if err != nil {
					vbsFailedToStartStream = append(vbsFailedToStartStream, vbno)
				}
			}

			logging.Infof("CRCR[%s:%s:%s:%d] vbsFailedToStartStream => len: %v dump: %v",
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

		case <-c.stopControlRoutineCh:
			return
		}
	}
}
