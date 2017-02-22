package producer

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/flatbuf/cfg"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

func (p *Producer) parseDepcfg() error {
	logging.Infof("DCFG[%s] Opening up application file", p.AppName)

	path := MetaKvAppsPath + p.AppName
	cfgData, err := util.MetakvGet(path)
	if err == nil {
		config := cfg.GetRootAsConfig(cfgData, 0)

		p.app = new(common.AppConfig)
		p.app.AppCode = string(config.AppCode())
		p.app.AppName = string(config.AppName())
		p.app.AppState = fmt.Sprintf("%v", AppUndeployed)
		p.app.AppVersion = util.MD5hash(p.app.AppCode)
		p.app.LastDeploy = time.Now().UTC().Format("2006-01-02T15:04:05.000000000-0700")
		p.app.ID = int(config.Id())
		p.app.Settings = make(map[string]interface{})

		d := new(cfg.DepCfg)
		depcfg := config.DepCfg(d)

		p.auth = string(depcfg.Auth())
		p.bucket = string(depcfg.SourceBucket())
		p.cfgData = string(cfgData)
		p.metadatabucket = string(depcfg.MetadataBucket())

		settingsPath := MetaKvAppSettingsPath + p.AppName
		sData, sErr := util.MetakvGet(settingsPath)
		if sErr != nil {
			logging.Errorf("DCFG[%s] Failed to fetch settings from metakv, err: %v", p.AppName, sErr)
			return sErr
		}

		settings := make(map[string]interface{})
		uErr := json.Unmarshal(sData, &settings)
		if uErr != nil {
			logging.Errorf("DCFG[%s] Failed to unmarshal settings received from metakv, err: %v", p.AppName, uErr)
			return uErr
		}

		p.statsTickDuration = time.Duration(settings["tick_duration"].(float64))
		p.workerCount = int(settings["worker_count"].(float64))
		p.app.Settings = settings

		logging.Infof("DCFG[%s] Loaded app => wc: %v auth: %v bucket: %v statsTickD: %v",
			p.AppName, p.workerCount, p.auth, p.bucket, p.statsTickDuration)

		if p.workerCount <= 0 {
			return fmt.Errorf("%v", ErrorUnexpectedWorkerCount)
		}

		hostaddr := fmt.Sprintf("127.0.0.1:%s", p.NsServerPort)

		localAddress, err := util.LocalEventingServiceHost(p.auth, hostaddr)
		if err != nil {
			logging.Errorf("DCFG[%s] Failed to get address for local eventing node, err :%v", p.AppName, err)
			return err
		}

		p.kvHostPort, err = util.KVNodesAddresses(p.auth, hostaddr)
		if err != nil {
			logging.Errorf("DCFG[%s] Failed to get list of kv nodes in the cluster, err: %v", p.AppName, err)
			return err
		}

		p.nsServerHostPort = fmt.Sprintf("%s:%s", localAddress, p.NsServerPort)

	} else {
		logging.Errorf("DCFG[%s] Failed to read depcfg, err: %v", p.AppName, err)
		return err
	}
	return nil
}
