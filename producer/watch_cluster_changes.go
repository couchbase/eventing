package producer

import (
	"fmt"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

func (p *Producer) watchClusterChanges() {
	selfRestart := func() {
		logging.Infof("PWCC[%s:%d] Observed change in cluster state", p.AppName, p.lenRunningConsumers())
		time.Sleep(WatchClusterChangeInterval * time.Millisecond)
		go p.watchClusterChanges()
	}

	hostaddr := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)

	localAddress, err := getLocalEventingServiceHost(p.auth, hostaddr)
	if err != nil {
		logging.Errorf("PWCC[%s:%d] Failed to get address for local eventing node, err: %v", p.AppName, p.lenRunningConsumers(), err)
	}

	clusterURL := fmt.Sprintf("http://%s@%s:%s", p.auth, localAddress, p.NsServerPort)

	scn, err := common.NewServicesChangeNotifier(clusterURL, "default")
	if err != nil {
		logging.Errorf("PWCC[%s:%d] Failed to get ChangeNotifier handle, err: %v", p.AppName, p.lenRunningConsumers(), err)
		selfRestart()
		return
	}

	defer scn.Close()

	ch := scn.GetNotifyCh()

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				selfRestart()
				return
			} else {
				logging.Infof("PWCC[%s:%d] Got message that cluster state has changed", p.AppName, p.lenRunningConsumers())
				p.clusterStateChange <- true
				selfRestart()
				return
			}
		}
	}

}
