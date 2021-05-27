package producer

import (
	"encoding/json"
	"fmt"
	"math"
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
	p.app.FunctionID = uint32(config.HandlerUUID())
	p.app.FunctionInstanceID = string(config.FunctionInstanceID())
	p.app.LastDeploy = time.Now().UTC().Format("2006-01-02T15:04:05.000000000-0700")
	lifecycleState := string(config.LifecycleState())
	p.app.Settings = make(map[string]interface{})

	d := new(cfg.DepCfg)
	depcfg := config.DepCfg(d)

	var user, password string
	err = util.Retry(util.NewFixedBackoff(time.Second), &p.retryCount, getHTTPServiceAuth, p, &user, &password)
	if err == common.ErrRetryTimeout {
		logging.Errorf("%s [%s] Exiting due to timeout", logPrefix, p.appName)
		return err
	}

	p.handlerConfig.SourceKeyspace.BucketName = string(depcfg.SourceBucket())
	p.handlerConfig.SourceKeyspace.ScopeName = common.CheckAndReturnDefaultForScopeOrCollection(string(depcfg.SourceScope()))
	p.handlerConfig.SourceKeyspace.CollectionName = common.CheckAndReturnDefaultForScopeOrCollection(string(depcfg.SourceCollection()))
	p.cfgData = string(cfgData)
	p.metadataKeyspace.BucketName = string(depcfg.MetadataBucket())
	p.metadataKeyspace.ScopeName = common.CheckAndReturnDefaultForScopeOrCollection(string(depcfg.MetadataScope()))
	p.metadataKeyspace.CollectionName = common.CheckAndReturnDefaultForScopeOrCollection(string(depcfg.MetadataCollection()))

	p.isSrcMutation = false
	binding := new(cfg.Bucket)
	for idx := 0; idx < depcfg.BucketsLength(); idx++ {
		if depcfg.Buckets(binding, idx) {
			scopeName := common.CheckAndReturnDefaultForScopeOrCollection(string(binding.ScopeName()))
			collectionName := common.CheckAndReturnDefaultForScopeOrCollection(string(binding.CollectionName()))

			if string(binding.BucketName()) == p.handlerConfig.SourceKeyspace.BucketName &&
				scopeName == p.handlerConfig.SourceKeyspace.ScopeName &&
				collectionName == p.handlerConfig.SourceKeyspace.CollectionName &&
				string(config.Access(idx)) == "rw" {
				p.isSrcMutation = true
				break
			}
		}
	}

	p.auth = fmt.Sprintf("%s:%s", user, password)

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

	// N1QL related configuration

	if val, ok := settings["n1ql_consistency"]; ok {
		p.handlerConfig.N1qlConsistency = val.(string)
	} else {
		p.handlerConfig.N1qlConsistency = "none"
	}

	if val, ok := settings["lcb_inst_capacity"]; ok {
		p.handlerConfig.LcbInstCapacity = int(val.(float64))
	} else {
		p.handlerConfig.LcbInstCapacity = 10
	}

	// Handler related configurations
	if val, ok := settings["n1ql_prepare_all"]; ok {
		p.handlerConfig.N1qlPrepareAll = val.(bool)
	} else {
		p.handlerConfig.N1qlPrepareAll = false
	}

	if val, ok := settings["language_compatibility"]; ok {
		p.handlerConfig.LanguageCompatibility = val.(string)
	} else {
		p.handlerConfig.LanguageCompatibility = common.LanguageCompatibility[0]
	}

	if val, ok := settings["checkpoint_interval"]; ok {
		p.handlerConfig.CheckpointInterval = int(val.(float64))
	} else {
		p.handlerConfig.CheckpointInterval = 60000
	}

	if val, ok := settings["cpp_worker_thread_count"]; ok {
		p.handlerConfig.CPPWorkerThrCount = int(val.(float64))
	} else {
		p.handlerConfig.CPPWorkerThrCount = 2
	}

	if lifecycleState == "pause" { // lifecycleState is "" in mixed mode
		// We know that the handler should ways be in from_prior state in pause cycle
		p.handlerConfig.StreamBoundary = common.DcpStreamBoundary("from_prior")
	} else if val, ok := settings["dcp_stream_boundary"]; ok {
		// Leave the settings as it is because it's an upgraded cluster without a life cycle Op yet
		// Likely possible that the handler crashed after the store was changed
		// or the function is in undeploy cycle where we use the value supplied by the user
		p.handlerConfig.StreamBoundary = common.DcpStreamBoundary(val.(string))
	} else {
		p.handlerConfig.StreamBoundary = common.DcpStreamBoundary("everything")
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
		p.handlerConfig.HandlerHeaders = common.GetDefaultHandlerHeaders()
	}

	if val, ok := settings["idle_checkpoint_interval"]; ok {
		p.handlerConfig.IdleCheckpointInterval = int(val.(float64))
	} else {
		p.handlerConfig.IdleCheckpointInterval = 30000
	}

	if val, ok := settings["log_level"]; ok {
		p.handlerConfig.LogLevel = val.(string)
	} else {
		p.handlerConfig.LogLevel = "INFO"
	}

	if val, ok := settings["num_timer_partitions"]; ok {
		p.handlerConfig.NumTimerPartitions = int(math.Min(float64(util.RoundUpToNearestPowerOf2(val.(float64))), float64(p.numVbuckets)))
	} else {
		p.handlerConfig.NumTimerPartitions = p.numVbuckets
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

	if val, ok := settings["worker_count"]; ok {
		p.handlerConfig.WorkerCount = int(val.(float64))
	} else if string(config.Version()) == "" {
		p.handlerConfig.WorkerCount = 3
	} else if handlerVer, err := common.FrameCouchbaseVersion(string(config.Version())); err == nil {
		if currCouchbaseVer, err1 := common.FrameCouchbaseVersionShort("7.0.0"); err1 == nil {
			if currCouchbaseVer.Compare(handlerVer) {
				p.handlerConfig.WorkerCount = 3
			} else {
				p.handlerConfig.WorkerCount = 1
			}
		}
	} else {
		p.handlerConfig.WorkerCount = 1
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
		p.handlerConfig.WorkerResponseTimeout = 5 * 60 // in seconds
	}

	if val, ok := settings["lcb_retry_count"]; ok {
		p.handlerConfig.LcbRetryCount = int(val.(float64))
	} else {
		p.handlerConfig.LcbRetryCount = 0
	}

	if val, ok := settings["lcb_timeout"]; ok {
		p.handlerConfig.LcbTimeout = int(val.(float64))
	} else {
		p.handlerConfig.LcbTimeout = 5
	}

	if val, ok := settings["bucket_cache_size"]; ok {
		p.handlerConfig.BucketCacheSize = int64(val.(float64))
	} else {
		p.handlerConfig.BucketCacheSize = 64 * 1024 * 1024
	}

	if val, ok := settings["bucket_cache_age"]; ok {
		p.handlerConfig.BucketCacheAge = int64(val.(float64))
	} else {
		p.handlerConfig.BucketCacheAge = 1000
	}

	if val, ok := settings["curl_max_allowed_resp_size"]; ok {
		p.handlerConfig.CurlMaxAllowedRespSize = int(val.(float64))
	} else {
		p.handlerConfig.CurlMaxAllowedRespSize = 100
	}

	// Metastore related configuration

	if val, ok := settings["timer_context_size"]; ok {
		p.handlerConfig.TimerContextSize = int64(val.(float64))
	} else {
		p.handlerConfig.TimerContextSize = 1024
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

	if val, ok := settings["allow_transaction_mutations"]; ok {
		p.handlerConfig.AllowTransactionMutations = val.(bool)
	} else {
		p.handlerConfig.AllowTransactionMutations = false
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

	if val, ok := settings["dcp_window_size"]; ok {
		p.dcpConfig["dcpWindowSize"] = uint32(val.(float64))
	} else {
		p.dcpConfig["dcpWindowSize"] = uint32(20 * 1024 * 1024)
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

	logging.Infof("%s [%s] Loaded function => wc: %v bucket: %v Scope: %v Collection: %s statsTickD: %v",
		logPrefix, p.appName, p.handlerConfig.WorkerCount, p.SourceBucket(), p.SourceScope(), p.SourceCollection(),
		p.handlerConfig.StatsLogInterval)

	if p.handlerConfig.WorkerCount <= 0 {
		return fmt.Errorf("%v", errorUnexpectedWorkerCount)
	}

	p.nsServerHostPort = net.JoinHostPort(util.Localhost(), p.nsServerPort)

	p.kvHostPorts, err = util.KVNodesAddresses(p.auth, p.nsServerHostPort, p.SourceBucket())
	if err != nil {
		logging.Errorf("%s [%s] Failed to get list of kv nodes in the cluster, err: %v", logPrefix, p.appName, err)
		return err
	}
	logging.Infof("%s [%s] kv nodes from cinfo: %+v", logPrefix, p.appName, p.kvHostPorts)

	p.dcpConfig["collectionAware"], err = util.CollectionAware(p.auth, p.nsServerHostPort)
	if err != nil {
		logging.Errorf("%s [%s] Failed to cluster collection aware status, err: %v", logPrefix, p.appName, err)
	}
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

		if p.UsingTimer() {
			return (p.MemoryQuota / (wc * 5)) * 1024 * 1024
		}
		return (p.MemoryQuota / (wc * 2)) * 1024 * 1024
	}
	return 1024 * 1024 * 1024

}
