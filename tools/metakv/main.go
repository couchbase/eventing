package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/metakv"
)

func main() {
	_, err := cbauth.InternalRetryDefaultInit("http://127.0.0.1:9000", os.Args[1], os.Args[2])
	if err != nil {
		fmt.Printf("Failed to init cbauth, err: %v\n", err)
		return
	}

	appCfgFile := os.Args[3]
	appName := os.Args[4]

	data, err := ioutil.ReadFile("./" + appCfgFile)
	if err != nil {
		fmt.Printf("Failed to read data from appcfg: %s, err: %v\n", appCfgFile, err)
		return
	}

	metakvAppsPath := "/eventing/apps/" + appName
	metakvAppsSettingsPath := "/eventing/settings/" + appName
	err = metakv.Set(metakvAppsPath, data, nil)
	if err != nil {
		fmt.Printf("Path: %s failed to perform metakv set, err: %v\n", metakvAppsPath, err)
		return
	}

	settings := make(map[string]interface{})
	settings["dcp_stream_boundary"] = "everything"
	settings["log_level"] = "INFO"
	settings["sock_batch_size"] = 1
	settings["tick_duration"] = 5000
	settings["checkpoint_interval"] = 20000
	settings["worker_count"] = 3
	settings["timer_worker_pool_size"] = 1
	settings["timer_processing_tick_interval"] = 500
	settings["skip_timer_threshold"] = 600
	settings["lcb_inst_capacity"] = 5
	settings["n1ql_consistency"] = "request"
	settings["enable_recursive_mutation"] = false
	settings["deadline_timeout"] = 2
	settings["execution_timeout"] = 1
	settings["processing_status"] = true

	sData, err := json.Marshal(&settings)
	if err != nil {
		fmt.Printf("Failed to marshal settings, err: %v\n", err)
		return
	}

	err = metakv.Set(metakvAppsSettingsPath, sData, nil)
	if err != nil {
		fmt.Printf("Path: %s failed to store settings, err: %v\n", metakvAppsSettingsPath, err)
		return
	}
}
