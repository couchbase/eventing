package producer

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/flatbuf/cfg"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

func (p *Producer) parseDepcfg() {
	logging.Infof("DCFG[%s] Opening up application file", p.AppName)

	cfgData, err := ioutil.ReadFile(AppsFolder + p.AppName)
	if err == nil {
		config := cfg.GetRootAsConfig(cfgData, 0)

		p.app = new(common.AppConfig)
		p.app.AppCode = string(config.AppCode())
		p.app.AppName = string(config.AppName())
		p.app.ID = int(config.Id())

		d := new(cfg.DepCfg)
		depcfg := config.DepCfg(d)

		p.auth = string(depcfg.Auth())
		p.bucket = string(depcfg.SourceBucket())
		p.cfgData = string(cfgData)
		p.metadatabucket = string(depcfg.MetadataBucket())
		p.statsTickDuration = time.Duration(depcfg.TickDuration())
		p.workerCount = int(depcfg.WorkerCount())

		fmt.Println("wc: ", p.workerCount, " auth: ", p.auth,
			" bucket: ", p.bucket, " statsTickD: ", p.statsTickDuration)

		hostaddr := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)

		localAddress, err := util.LocalEventingServiceHost(p.auth, hostaddr)
		if err != nil {
			logging.Errorf("DCFG[%s] Failed to get address for local eventing node, err :%v", p.AppName, err)
		}

		p.kvHostPort, err = util.KVNodesAddresses(p.auth, hostaddr)
		if err != nil {
			logging.Errorf("DCFG[%s] Failed to get list of kv nodes in the cluster, err: %v", p.AppName, err)
		}

		p.nsServerHostPort = fmt.Sprintf("%s:%s", localAddress, p.NsServerPort)

	} else {
		logging.Errorf("DCFG[%s] Failed to read depcfg, err: %v", p.AppName, err)
	}
}
