# Eventing Settings

#### Disclaimer: ####

* Most of the settings below are internal settings. They should not be changed
  from default values without consulting with developers or Couchbase Tech Support.
* We've tried limited combinations for different setting parameters. It's best
  to stick with default configuration unless instructed otherwise. In worst case, some
  untested non-default configuration could leave the cluster unusable.

|Field|Default|Description|
|:---|:---|:---
|app_log_dir|Index directory during Couchbase Setup|Function log directory|
|app_log_max_files|10|Rotations of function log files to keep(current plus compressed)
|app_log_max_size|40 MB|Size after which function log files are rotated and compressed|
|checkpoint_interval|60s|Frequency for updating checkpoint blobs in metadata bucket|
|cpp_worker_thread_count|2|V8 sandboxes running within an eventing-consumer process|
|data_chan_size|50|Capacity of queue that buffers dcp events|
|dcp_gen_chan_size|10000|Capacity of queue that buffers dcp related control messages|
|dcp_num_connections|1|Num of dcp connections to open per eventing-consumer per Data service node|
|dcp_stream_boundary|everything|Feed boundary for Function|
|enable_applog_rotation|true|To enable/disable function log file rotation|
|execution_timeout|60s|Timeout for execution of Javascript handler code|
|cursor_checkpoint_timeout|60s|Timeout for execution of checkpointing activity by a cursor_aware function. Defaults to whatever the default for execution_timeout|
|feedback_batch_size|100|Batch size for messages being written from eventing-consumer to eventing-producer|
|feedback_read_buffer_size|65536|Buffer size for reading messages from eventing-consumer|
|lcb_inst_capacity|5|Controls the level of nesting for n1ql iterators|
|log_level|INFO|Log level for Function|
|n1ql_consistency|request|Default consistency level for N1QL statements|
|sock_batch_size|100|Batch size for messages written from eventing-producer to eventing-consumer|
|timer_queue_size|10000|Queue item cap for firing timers|
|undeploy_routine_count|Num of online cpu cores|Size of thread pool to cleanup metadata bucket as par of undeploy|
|user_prefix|eventing|Prefix for eventing system blobs written to metadata bucket|
|vb_ownership_giveup_routine_count|3|Size of thread pool to give up vb ownership during rebalance|
|vb_ownership_takeover_routine_count|3|Size of thread pool to take up vb ownership during rebalance|
|worker_count|1|eventing-consumer instances to spawn for parallelism w.r.t. event processing|
|worker_feedback_queue_cap|500|Capacity of timer feedback queue on eventing-consumer|
|worker_queue_cap|100000|Capacity of queue for main loop queue on eventing-consumer|
|allow_interbucket_recursion|false|Allow deployment of handlers with inter bucket/inter handler recursion|
|bucket_cache_size|64MB|Size to which bucket document cache can grow to before eviction begins|
|bucket_cache_age|1000|Age in milliseconds after which a cached bucket document is considered stale|

