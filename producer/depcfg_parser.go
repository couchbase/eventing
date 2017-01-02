package producer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"
)

func (p *Producer) parseDepcfg() {
	log.Println("Opening up application file for app:", p.AppName)

	depcfgData, err := ioutil.ReadFile(APPS_FOLDER + p.AppName)
	if err == nil {
		err := json.Unmarshal(depcfgData, &p.app)
		if err != nil {
			log.Println("Failed to parse depcfg for app", p.AppName,
				"with err:", err.Error())
			return
		}

		config := p.app.Depcfg.(map[string]interface{})

		p.auth = config["auth"].(string)
		p.bucket = config["source_bucket"].(string)
		p.kvHostPort = strings.Split(config["source_node"].(string)+":"+KV_PORT, ",")
		p.nsServerHostPort = fmt.Sprintf("%s:%s", config["source_node"].(string),
			NS_SERVER_PORT)
		p.statsTickDuration = time.Duration(config["tick_duration"].(float64))
		p.workerCount = int(config["worker_count"].(float64))

		return
	} else {
		log.Printf("Encountered err:", err.Error(),
			"while trying to parse depcfg for app:", p.AppName)
	}
}
