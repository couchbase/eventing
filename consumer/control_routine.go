package consumer

import (
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

func (c *Consumer) controlRoutine() {
	for {
		select {
		case <-c.clusterStateChangeNotifCh:

			util.Retry(util.NewFixedBackoff(ClusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

			logging.Infof("CRCO[%s:%s:%s:%d] Got notif that cluster state has changed",
				c.app.AppName, c.workerName, c.tcpPort, c.osPid)

			// TODO: stop channel for vb takeover/give up
			c.vbsStateUpdate()

		case <-c.controlRoutineTicker.C:

			util.Retry(util.NewFixedBackoff(ClusterOpRetryInterval), getEventingNodeAddrOpCallback, c)

			logging.Infof("CRCO[%s:%s:%s:%d] Control routine ticker kicked off",
				c.app.AppName, c.workerName, c.tcpPort, c.osPid)

			c.vbsStateUpdate()
		}
	}
}
