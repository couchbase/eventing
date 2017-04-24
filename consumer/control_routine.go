package consumer

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

func (c *Consumer) controlRoutine() {
	for {
		select {
		case <-c.clusterStateChangeNotifCh:

			util.Retry(util.NewFixedBackoff(clusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

			logging.Infof("CRCR[%s:%s:%s:%d] Got notif that cluster state has changed",
				c.app.AppName, c.workerName, c.tcpPort, c.Pid())

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
				logging.Infof("CRCR[%s:%s:%s:%d] Retrying vbsToRestream, remaining len: %v dump: %v",
					c.app.AppName, c.workerName, c.tcpPort, c.Pid(), vbsRemainingToRestream, diff)
				goto retryVbsRemainingToRestream
			}

		case <-c.stopControlRoutineCh:
			return
		}
	}
}
