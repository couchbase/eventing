package consumer

import (
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

			logging.Infof("CRCR[%s:%s:%s:%d] Got notif that cluster state has changed",
				c.app.AppName, c.workerName, c.tcpPort, c.osPid)

			// Wait till vbs for which STREAMEND has been received because of KV rebalance
			// are reclaimed back by the consumer instance
		retryVbsStateUpdate:
			c.RLock()
			vbsRemainingToRestream := c.vbsRemainingToRestream
			sort.Sort(util.Uint16Slice(vbsRemainingToRestream))
			logging.Infof("CRCR[%s:%s:%s:%d] clusterStateChangeNotif vbsToRestream len: %v dump: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.osPid, len(vbsRemainingToRestream), vbsRemainingToRestream)
			c.RUnlock()

			if len(vbsRemainingToRestream) > 0 {
				time.Sleep(retryVbsStateUpdateInterval)
				goto retryVbsStateUpdate
			}

			c.vbsStateUpdate()

		case <-c.restartVbDcpStreamTicker.C:

		retryVbsRemainingToRestream:
			c.RLock()
			vbsToRestream := c.vbsRemainingToRestream
			c.RUnlock()

			if len(vbsToRestream) == 0 {
				continue
			}

			sort.Sort(util.Uint16Slice(vbsToRestream))
			logging.Infof("CRCR[%s:%s:%s:%d] vbsToRestream len: %v dump: %v",
				c.app.AppName, c.workerName, c.tcpPort, c.osPid, len(vbsToRestream), vbsToRestream)

			for _, vbno := range vbsToRestream {
				var vbBlob vbucketKVBlob
				var cas uint64
				vbKey := fmt.Sprintf("%s_vb_%s", c.app.AppName, strconv.Itoa(int(vbno)))

				logging.Infof("CRCR[%s:%s:%s:%d] vbno: %v, reclaiming it back by restarting dcp stream",
					c.app.AppName, c.workerName, c.tcpPort, c.osPid, vbno)
				util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), getOpCallback, c, vbKey, &vbBlob, &cas, false)
				c.updateVbOwnerAndStartDCPStream(vbKey, vbno, &vbBlob, &cas, true)
			}

			c.Lock()
			diff := util.VbsSliceDiff(vbsToRestream, c.vbsRemainingToRestream)
			c.vbsRemainingToRestream = diff
			vbsRemainingToRestream := len(c.vbsRemainingToRestream)
			c.Unlock()

			sort.Sort(util.Uint16Slice(diff))

			if vbsRemainingToRestream > 0 {
				logging.Infof("CRCR[%s:%s:%s:%d] Retrying vbsToRestream, remaining len: %v dump: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.osPid, vbsRemainingToRestream, diff)
				goto retryVbsRemainingToRestream
			}

		case <-c.stopControlRoutineCh:
			return
		}
	}
}
