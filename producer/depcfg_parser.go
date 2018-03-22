package producer

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/gen/flatbuf/cfg"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
)

func (p *Producer) parseDepcfg() error {
	logPrefix := "Producer::parseDepcfg"

	logging.Infof("%s [%s] Opening up application file", logPrefix, p.appName)

	var cfgData []byte

	// Adding sleep until source of MB-26702 is known
	time.Sleep(5 * time.Second)

	// Keeping metakv lookup in retry loop. There is potential metakv related race between routine that gets notified about updates
	// to metakv path and routine that does metakv lookup
	util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), metakvAppCallback, p, metakvAppsPath, metakvChecksumPath, p.appName, &cfgData)

	config := cfg.GetRootAsConfig(cfgData, 0)

	p.app = new(common.AppConfig)
	p.app.AppCode = string(config.AppCode())
	p.app.AppName = string(config.AppName())
	p.app.AppState = fmt.Sprintf("%v", appUndeployed)
	p.app.AppVersion = util.GetHash(p.app.AppCode)
	p.app.LastDeploy = time.Now().UTC().Format("2006-01-02T15:04:05.000000000-0700")
	p.app.ID = int(config.Id())
	p.app.Settings = make(map[string]interface{})

	d := new(cfg.DepCfg)
	depcfg := config.DepCfg(d)

	var user, password string
	util.Retry(util.NewFixedBackoff(time.Second), getHTTPServiceAuth, p, &user, &password)
	p.auth = fmt.Sprintf("%s:%s", user, password)

	p.handlerConfig.SourceBucket = string(depcfg.SourceBucket())
	p.cfgData = string(cfgData)
	p.metadatabucket = string(depcfg.MetadataBucket())

	settingsPath := metakvAppSettingsPath + p.appName
	sData, sErr := util.MetakvGet(settingsPath)
	if sErr != nil {
		logging.Errorf("%s [%s] Failed to fetch settings from metakv, err: %v", logPrefix, p.appName, sErr)
		return sErr
	}

	settings := make(map[string]interface{})
	uErr := json.Unmarshal(sData, &settings)
	if uErr != nil {
		logging.Errorf("%s [%s] Failed to unmarshal settings received from metakv, err: %v", logPrefix, p.appName, uErr)
		return uErr
	}

	// Handler related configurations

	if val, ok := settings["checkpoint_interval"]; ok {
		p.handlerConfig.CheckpointInterval = int(val.(float64))
	} else {
		p.handlerConfig.CheckpointInterval = 60000
	}

	if val, ok := settings["cleanup_timers"]; ok {
		p.handlerConfig.CleanupTimers = val.(bool)
	} else {
		p.handlerConfig.CleanupTimers = false
	}

	if val, ok := settings["cpp_worker_thread_count"]; ok {
		p.handlerConfig.CPPWorkerThrCount = int(val.(float64))
	} else {
		p.handlerConfig.CPPWorkerThrCount = 2
	}

	if val, ok := settings["cron_timers_per_doc"]; ok {
		p.handlerConfig.CronTimersPerDoc = int(val.(float64))
	} else {
		p.handlerConfig.CronTimersPerDoc = 1000
	}

	if val, ok := settings["curl_timeout"]; ok {
		p.handlerConfig.CurlTimeout = int64(val.(float64))
	} else {
		p.handlerConfig.CurlTimeout = int64(500)
	}

	if val, ok := settings["dcp_stream_boundary"]; ok {
		p.handlerConfig.StreamBoundary = common.DcpStreamBoundary(val.(string))
	} else {
		p.handlerConfig.StreamBoundary = common.DcpStreamBoundary("everything")
	}

	if val, ok := settings["deadline_timeout"]; ok {
		p.handlerConfig.SocketTimeout = int(val.(float64))
	} else {
		p.handlerConfig.SocketTimeout = 2
	}

	if val, ok := settings["enable_recursive_mutation"]; ok {
		p.handlerConfig.EnableRecursiveMutation = val.(bool)
	} else {
		p.handlerConfig.EnableRecursiveMutation = false
	}

	if val, ok := settings["execution_timeout"]; ok {
		p.handlerConfig.ExecutionTimeout = int(val.(float64))
	} else {
		p.handlerConfig.ExecutionTimeout = 1
	}

	if val, ok := settings["feedback_batch_size"]; ok {
		p.handlerConfig.FeedbackBatchSize = int(val.(float64))
	} else {
		p.handlerConfig.FeedbackBatchSize = 100
	}

	if val, ok := settings["feedback_read_buffer_size"]; ok {
		p.handlerConfig.FeedbackReadBufferSize = int(val.(float64))
	} else {
		p.handlerConfig.FeedbackReadBufferSize = 65536
	}

	if val, ok := settings["fuzz_offset"]; ok {
		p.handlerConfig.FuzzOffset = int(val.(float64))
	} else {
		p.handlerConfig.FuzzOffset = 0
	}

	if val, ok := settings["lcb_inst_capacity"]; ok {
		p.handlerConfig.LcbInstCapacity = int(val.(float64))
	} else {
		p.handlerConfig.LcbInstCapacity = 5
	}

	if val, ok := settings["log_level"]; ok {
		p.handlerConfig.LogLevel = val.(string)
	} else {
		p.handlerConfig.LogLevel = "INFO"
	}

	if val, ok := settings["skip_timer_threshold"]; ok {
		p.handlerConfig.SkipTimerThreshold = int(val.(float64))
	} else {
		p.handlerConfig.SkipTimerThreshold = 86400
	}

	if val, ok := settings["sock_batch_size"]; ok {
		p.handlerConfig.SocketWriteBatchSize = int(val.(float64))
	} else {
		p.handlerConfig.SocketWriteBatchSize = 100
	}

	if val, ok := settings["tick_duration"]; ok {
		p.handlerConfig.StatsLogInterval = int(val.(float64))
	} else {
		p.handlerConfig.StatsLogInterval = 300 * 1000
	}

	if val, ok := settings["worker_count"]; ok {
		p.handlerConfig.WorkerCount = int(val.(float64))
	} else {
		p.handlerConfig.WorkerCount = 3
	}

	if val, ok := settings["worker_feedback_queue_cap"]; ok {
		p.handlerConfig.FeedbackQueueCap = int64(val.(float64))
	} else {
		p.handlerConfig.FeedbackQueueCap = int64(10 * 1000)
	}

	if val, ok := settings["worker_queue_cap"]; ok {
		p.handlerConfig.WorkerQueueCap = int64(val.(float64))
	} else {
		p.handlerConfig.WorkerQueueCap = int64(100 * 1000)
	}

	if val, ok := settings["xattr_doc_timer_entry_prune_threshold"]; ok {
		p.handlerConfig.XattrEntryPruneThreshold = int(val.(float64))
	} else {
		p.handlerConfig.XattrEntryPruneThreshold = 100
	}

	// Process related configuration

	if val, ok := settings["breakpad_on"]; ok {
		p.processConfig.BreakpadOn = val.(bool)
	} else {
		p.processConfig.BreakpadOn = false
	}

	// Rebalance related configurations

	if val, ok := settings["vb_ownership_giveup_routine_count"]; ok {
		p.rebalanceConfig.VBOwnershipGiveUpRoutineCount = int(val.(float64))
	} else {
		p.rebalanceConfig.VBOwnershipGiveUpRoutineCount = 3
	}

	if val, ok := settings["vb_ownership_takeover_routine_count"]; ok {
		p.rebalanceConfig.VBOwnershipTakeoverRoutineCount = int(val.(float64))
	} else {
		p.rebalanceConfig.VBOwnershipTakeoverRoutineCount = 3
	}

	// Application logging related configurations

	if val, ok := settings["app_log_dir"]; ok {
		os.MkdirAll(val.(string), 0755)
		p.appLogPath = fmt.Sprintf("%s/%s", val.(string), p.appName)
	} else {
		os.MkdirAll(p.processConfig.EventingDir, 0755)
		p.appLogPath = fmt.Sprintf("%s/%s.log", p.processConfig.EventingDir, p.appName)
	}

	if val, ok := settings["app_log_max_size"]; ok {
		p.appLogMaxSize = int64(val.(float64))
	} else {
		p.appLogMaxSize = 1024 * 1024 * 10
	}

	if val, ok := settings["app_log_max_files"]; ok {
		p.appLogMaxFiles = int(val.(float64))
	} else {
		p.appLogMaxFiles = 10
	}

	// Doc timer configurations for plasma

	if val, ok := settings["auto_swapper"]; ok {
		p.autoSwapper = val.(bool)
	} else {
		p.autoSwapper = true
	}

	if val, ok := settings["enable_snapshot_smr"]; ok {
		p.enableSnapshotSMR = val.(bool)
	} else {
		p.enableSnapshotSMR = false
	}

	if val, ok := settings["lss_cleaner_max_threshold"]; ok {
		p.lssCleanerMaxThreshold = int(val.(float64))
	} else {
		p.lssCleanerMaxThreshold = 70
	}

	if val, ok := settings["lss_cleaner_threshold"]; ok {
		p.lssCleanerThreshold = int(val.(float64))
	} else {
		p.lssCleanerThreshold = 30
	}

	if val, ok := settings["lss_read_ahead_size"]; ok {
		p.lssReadAheadSize = int64(val.(float64))
	} else {
		p.lssReadAheadSize = 1024 * 1024
	}

	if val, ok := settings["max_delta_chain_len"]; ok {
		p.maxDeltaChainLen = int(val.(float64))
	} else {
		p.maxDeltaChainLen = 200
	}

	if val, ok := settings["max_page_items"]; ok {
		p.maxPageItems = int(val.(float64))
	} else {
		p.maxPageItems = 400
	}

	if val, ok := settings["min_page_items"]; ok {
		p.minPageItems = int(val.(float64))
	} else {
		p.minPageItems = 50
	}

	if val, ok := settings["persist_interval"]; ok {
		p.persistInterval = int(val.(float64))
	} else {
		p.persistInterval = 5000
	}

	if val, ok := settings["use_memory_manager"]; ok {
		p.useMemoryMgmt = val.(bool)
	} else {
		p.useMemoryMgmt = true
	}

	// DCP connection related configurations

	if val, ok := settings["data_chan_size"]; ok {
		p.dcpConfig["dataChanSize"] = int(val.(float64))
	} else {
		p.dcpConfig["dataChanSize"] = 10000
	}

	if val, ok := settings["dcp_gen_chan_size"]; ok {
		p.dcpConfig["genChanSize"] = int(val.(float64))
	} else {
		p.dcpConfig["genChanSize"] = 10000
	}

	if val, ok := settings["dcp_num_connections"]; ok {
		p.dcpConfig["numConnections"] = int(val.(float64))
	} else {
		p.dcpConfig["numConnections"] = 1
	}

	p.dcpConfig["activeVbOnly"] = true

	p.app.Settings = settings

	logLevel := settings["log_level"].(string)
	logging.SetLogLevel(util.GetLogLevel(logLevel))

	logging.Infof("%s [%s] Loaded app => wc: %v bucket: %v statsTickD: %v",
		logPrefix, p.appName, p.handlerConfig.WorkerCount, p.handlerConfig.SourceBucket, p.handlerConfig.StatsLogInterval)

	if p.handlerConfig.WorkerCount <= 0 {
		return fmt.Errorf("%v", errorUnexpectedWorkerCount)
	}

	p.nsServerHostPort = net.JoinHostPort(util.Localhost(), p.nsServerPort)

	var err error
	p.kvHostPorts, err = util.KVNodesAddresses(p.auth, p.nsServerHostPort)
	if err != nil {
		logging.Errorf("%s [%s] Failed to get list of kv nodes in the cluster, err: %v", logPrefix, p.appName, err)
		return err
	}

	return nil
}
