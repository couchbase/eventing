package producer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/couchbase/indexing/secondary/logging"
)

func (p *Producer) parseDepcfg() {
	logging.Infof("DCFG[%s] Opening up application file", p.AppName)

	depcfgData, err := ioutil.ReadFile(AppsFolder + p.AppName)
	if err == nil {
		err := json.Unmarshal(depcfgData, &p.app)
		if err != nil {
			logging.Errorf("DCFG[%s] Failed to parse depcfg, err: %v", p.AppName, err)
			return
		}

		config := p.app.Depcfg.(map[string]interface{})

		p.auth = config["auth"].(string)
		p.bucket = config["source_bucket"].(string)

		hostaddr := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)

		localAddress, err := getLocalEventingServiceHost(p.auth, hostaddr)
		if err != nil {
			logging.Errorf("DCFG[%s] Failed to get address for local eventing node, err :%v", p.AppName, err)
		}

		p.kvHostPort, err = getKVNodesAddresses(p.auth, hostaddr)
		if err != nil {
			logging.Errorf("DCFG[%s] Failed to get list of kv nodes in the cluster, err: %v", p.AppName, err)
		}

		p.nsServerHostPort = fmt.Sprintf("%s:%s", localAddress, p.NsServerPort)

		p.statsTickDuration = time.Duration(config["tick_duration"].(float64))
		p.workerCount = int(config["worker_count"].(float64))

	} else {
		logging.Errorf("DCFG[%s] Failed to read depcfg, err: %v", p.AppName, err)
	}
}
