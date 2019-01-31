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
	err := util.Retry(util.NewFixedBackoff(bucketOpRetryInterval), &p.retryCount, metakvAppCallback, p, metakvAppsPath, metakvChecksumPath, p.appName, &cfgData)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s] Exiting due to timeout", logPrefix, p.appName)
		return err
	}

	config := cfg.GetRootAsConfig(cfgData, 0)

	p.app = new(common.AppConfig)
	p.app.AppCode = string(config.AppCode())
	p.app.AppName = string(config.AppName())
	p.app.AppState = fmt.Sprintf("%v", appUndeployed)
	p.app.AppVersion = util.GetHash(p.app.AppCode)
	p.app.FunctionID = uint32(config.FunctionID())
	p.app.FunctionInstanceID = string(config.FunctionInstanceID())
	p.app.ID = int(config.Id())
	p.app.LastDeploy = time.Now().UTC().Format("2006-01-02T15:04:05.000000000-0700")
	p.app.Settings = make(map[string]interface{})

	if config.UsingTimer() == 0x1 {
		p.app.UsingTimer = true
	}

	if config.SrcMutationEnabled() == 0x1 {
		p.app.SrcMutationEnabled = true
	}

	d := new(cfg.DepCfg)
	depcfg := config.DepCfg(d)

	var user, password string
	err = util.Retry(util.NewFixedBackoff(time.Second), &p.retryCount, getHTTPServiceAuth, p, &user, &password)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s] Exiting due to timeout", logPrefix, p.appName)
		return err
	}

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

	if val, ok := settings["dcp_stream_boundary"]; ok {
		p.handlerConfig.StreamBoundary = common.DcpStreamBoundary(val.(string))
	} else {
		p.handlerConfig.StreamBoundary = common.DcpStreamBoundary("everything")
	}

	if val, ok := settings["deadline_timeout"]; ok {
		p.handlerConfig.SocketTimeout = int(val.(float64))
	} else {
		p.handlerConfig.SocketTimeout = 62
	}

	if val, ok := settings["execution_timeout"]; ok {
		p.handlerConfig.ExecutionTimeout = int(val.(float64))
	} else {
		p.handlerConfig.ExecutionTimeout = 60
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

	if val, ok := settings["handler_footers"]; ok {
		p.handlerConfig.HandlerFooters = util.ToStringArray(val)
	}

	if val, ok := settings["handler_headers"]; ok {
		p.handlerConfig.HandlerHeaders = util.ToStringArray(val)
	} else {
		p.handlerConfig.HandlerHeaders = []string{"'use strict';"}
	}

	if val, ok := settings["idle_checkpoint_interval"]; ok {
		p.handlerConfig.IdleCheckpointInterval = int(val.(float64))
	} else {
		p.handlerConfig.IdleCheckpointInterval = 30000
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

	if val, ok := settings["poll_bucket_interval"]; ok {
		p.pollBucketInterval = time.Duration(val.(float64)) * time.Second
	} else {
		p.pollBucketInterval = 10 * time.Second
	}

	if val, ok := settings["sock_batch_size"]; ok {
		p.handlerConfig.SocketWriteBatchSize = int(val.(float64))
	} else {
		p.handlerConfig.SocketWriteBatchSize = 100
	}

	if val, ok := settings["tick_duration"]; ok {
		p.handlerConfig.StatsLogInterval = int(val.(float64))
	} else {
		p.handlerConfig.StatsLogInterval = 60 * 1000
	}

	if val, ok := settings["user_prefix"]; ok {
		p.app.UserPrefix = val.(string)
	} else {
		p.app.UserPrefix = "eventing"
	}

	if val, ok := settings["using_timer"]; ok {
		p.handlerConfig.UsingTimer = val.(bool)
	} else {
		p.handlerConfig.UsingTimer = p.app.UsingTimer
	}

	if val, ok := settings["worker_count"]; ok {
		p.handlerConfig.WorkerCount = int(val.(float64))
	} else {
		p.handlerConfig.WorkerCount = 3
	}

	if val, ok := settings["worker_feedback_queue_cap"]; ok {
		p.handlerConfig.FeedbackQueueCap = int64(val.(float64))
	} else {
		p.handlerConfig.FeedbackQueueCap = int64(500)
	}

	if val, ok := settings["worker_queue_cap"]; ok {
		p.handlerConfig.WorkerQueueCap = int64(val.(float64))
	} else {
		p.handlerConfig.WorkerQueueCap = int64(100 * 1000)
	}

	if val, ok := settings["worker_queue_mem_cap"]; ok {
		p.handlerConfig.WorkerQueueMemCap = int64(val.(float64)) * 1024 * 1024
	} else {
		p.handlerConfig.WorkerQueueMemCap = p.consumerMemQuota()
	}

	if val, ok := settings["worker_response_timeout"]; ok {
		p.handlerConfig.WorkerResponseTimeout = int(val.(float64))
	} else {
		p.handlerConfig.WorkerResponseTimeout = 300 // in seconds
	}

	// Metastore related configuration
	if val, ok := settings["execute_timer_routine_count"]; ok {
		p.handlerConfig.ExecuteTimerRoutineCount = int(val.(float64))
	} else {
		p.handlerConfig.ExecuteTimerRoutineCount = 3
	}

	if val, ok := settings["timer_context_size"]; ok {
		p.handlerConfig.TimerContextSize = int64(val.(float64))
	} else {
		p.handlerConfig.TimerContextSize = 1024
	}

	if val, ok := settings["timer_storage_routine_count"]; ok {
		p.handlerConfig.TimerStorageRoutineCount = int(val.(float64))
	} else {
		p.handlerConfig.TimerStorageRoutineCount = 3
	}

	if val, ok := settings["timer_storage_chan_size"]; ok {
		p.handlerConfig.TimerStorageChanSize = int(val.(float64))
	} else {
		p.handlerConfig.TimerStorageChanSize = 10 * 1000
	}

	if val, ok := settings["timer_queue_mem_cap"]; ok {
		p.handlerConfig.TimerQueueMemCap = uint64(val.(float64)) * 1024 * 1024
	} else {
		p.handlerConfig.TimerQueueMemCap = uint64(p.consumerMemQuota())
	}

	if val, ok := settings["timer_queue_size"]; ok {
		p.handlerConfig.TimerQueueSize = uint64(val.(float64))
	} else {
		p.handlerConfig.TimerQueueSize = 10000
	}

	if val, ok := settings["undeploy_routine_count"]; ok {
		p.handlerConfig.UndeployRoutineCount = int(val.(float64))
	} else {
		p.handlerConfig.UndeployRoutineCount = util.CPUCount(true)
	}

	// Process related configuration

	if val, ok := settings["breakpad_on"]; ok {
		p.processConfig.BreakpadOn = val.(bool)
	} else {
		p.processConfig.BreakpadOn = true
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
		p.appLogMaxSize = 1024 * 1024 * 40
	}

	if val, ok := settings["app_log_max_files"]; ok {
		p.appLogMaxFiles = int64(val.(float64))
	} else {
		p.appLogMaxFiles = int64(10)
	}

	if val, ok := settings["enable_applog_rotation"]; ok {
		p.appLogRotation = val.(bool)
	} else {
		p.appLogRotation = true
	}

	// DCP connection related configurations
	if val, ok := settings["agg_dcp_feed_mem_cap"]; ok {
		p.handlerConfig.AggDCPFeedMemCap = int64(val.(float64)) * 1024 * 1024
	} else {
		p.handlerConfig.AggDCPFeedMemCap = p.consumerMemQuota()
	}

	if val, ok := settings["data_chan_size"]; ok {
		p.dcpConfig["dataChanSize"] = int(val.(float64))
	} else {
		p.dcpConfig["dataChanSize"] = 50
	}

	p.dcpConfig["latencyTick"] = p.handlerConfig.StatsLogInterval

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

	var logLevel string
	if val, ok := settings["log_level"]; ok {
		logLevel = val.(string)
	} else {
		logLevel = "INFO"
	}

	logging.SetLogLevel(util.GetLogLevel(logLevel))

	logging.Infof("%s [%s] Loaded function => wc: %v bucket: %v statsTickD: %v",
		logPrefix, p.appName, p.handlerConfig.WorkerCount, p.handlerConfig.SourceBucket, p.handlerConfig.StatsLogInterval)

	if p.handlerConfig.WorkerCount <= 0 {
		return fmt.Errorf("%v", errorUnexpectedWorkerCount)
	}

	p.nsServerHostPort = net.JoinHostPort(util.Localhost(), p.nsServerPort)

	p.kvHostPorts, err = util.KVNodesAddresses(p.auth, p.nsServerHostPort, p.handlerConfig.SourceBucket)
	if err != nil {
		logging.Errorf("%s [%s] Failed to get list of kv nodes in the cluster, err: %v", logPrefix, p.appName, err)
		return err
	}

	logging.Infof("%s [%s] kv nodes from cinfo: %+v", logPrefix, p.appName, p.kvHostPorts)

	return nil
}

func (p *Producer) consumerMemQuota() int64 {
	wc := int64(p.handlerConfig.WorkerCount)
	if wc > 0 {
		// Accounting for memory usage by following queues:
		// (a) dcp feed queue
		// (b) timer_feedback_queue + main_queue on eventing-consumer
		// (c) create timer queue
		// (d) timer store queues for thread pool
		// (e) fire timer queue

		if p.app.UsingTimer {
			return (p.MemoryQuota / (wc * 5)) * 1024 * 1024
		}
		return (p.MemoryQuota / (wc * 2)) * 1024 * 1024
	}
	return 1024 * 1024 * 1024

}
