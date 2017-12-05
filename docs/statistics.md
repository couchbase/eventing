# Eventing Stats
Eventing stats can be fetched from each eventing node using REST API bound to localhost. The resulting
stats are local to the node, and suitable for further aggregation across nodes of the cluster.

The following endpoint could be used to get the stats:
```shell
curl http://user:pass@localhost:8096/api/v1/stats?type=full
```
This will return the stats regardings events processing, events remaining, execution, failure, latency, worker PIDs and seq processed.
```json
[
 {
   "function_name": "stock-tracker",
   "event_processing_stats": {
     "DCP_DELETION": 14,
     "DCP_MUTATION": 1,
     "DCP_SNAPSHOT": 15,
     "DCP_STREAMREQ": 1024,
     "DOC_TIMER_EVENTS": 121,
     "CRON_TIMER_EVENTS": 231
   },
   "events_remaining": {
     "dcp_backlog": 33
   },
   "execution_stats": {
     "doc_timer_create_failure": 0,
     "non_doc_timer_create_failure": 0,
     "on_delete_failure": 0,
     "on_delete_success": 14,
     "on_update_failure": 0,
     "on_update_success": 1
   },
   "failure_stats": {
     "bucket_op_exception_count": 0,
     "checkpoint_failure_count": 66,
     "n1ql_op_exception_count": 0,
     "timeout_count": 0
   },
   "latency_stats": {
     "100": 12,
     "1000": 3
   },
   "worker_pids": {
     "worker_h1_0": 28558,
     "worker_h1_1": 28559,
     "worker_h1_2": 28560
   },
   "lcb_exception_stats": {
     "13": 200
   },
   "plasma_stats": {"AllocSz": 96, ...},
   "seqs_processed":{"0":0,"1":0, ...}
 }
]
```
> Omitting the parameter `type=full` will exclude seqs_processed and plasma_stats from the response.

The above stats could be individually obtained through the following endpoints:
```shell
curl http://user:pass@localhost:8096/getExecutionStats?name=function_name
curl http://user:pass@localhost:8096/getLatencyStats?name=function_name
curl http://user:pass@localhost:8096/getFailureStats?name=function_name
```

## Execution stats
This group of counters provide an insight into function execution.

```json 
curl http://user:pass@localhost:8096/getExecutionStats?name=function_name
{
  "doc_timer_create_failure": 0,
  "non_doc_timer_create_failure": 0,
  "on_delete_failure": 5108,
  "on_delete_success": 6400893,
  "on_update_failure": 0,
  "on_update_success": 11510282
}
```

Name|Datatype|Field|Descripton
|:---|:---|:---|:---
Document Timer Creation Retries|uint|`doc_timer_create_failure`|Count of number of times document timers creations that were retried. Retry continues till script timeout.
Cron Timer Creation Retries|uint|`non_doc_timer_create_failure`|Count of number of times cron timers creations that were retried. Retry continues till script timeout.
OnDelete handler failures|uint|`on_delete_failure`|Count of number of delete handler executions that terminated with an uncaught execption.
OnUpdate handler failures|uint|`on_update_failure`|Count of number of update handler executions that terminated with an uncaught execption.
OnDelete handler successful invocations|uint|`on_delete_success`|Counter for number of times OnDelete handler was executed successfully.
OnUpdate handler successful invocations|uint|`on_update_success`|Counter for number of times OnUpdate handler was executed successfully.
 
## Latency Stats
These give latency of handler executions in wall clock time, in aggregate, across all handlers and timers. The returned object has a key which is the latency range in **microseconds** and value which is the count of executions in this range.
 
```json
curl http://user:pass@localhost:8096/getLatencyStats?name=function_name
{
  "1000": 17355495,
  "10000": 2959,
  "100000": 23,
  "101000": 20,
  "102000": 14,
  "103000": 11,
  "104000": 15,
  "105000": 13,
  "106000": 8,
  "107000": 13,
  "108000": 12,
  "109000": 14,
  "11000": 2077,
}
``` 

## DCP Stats
This endpoint returns backlog of events that have occured but are not yet processed by event handlers.
 
```json
curl http://user:pass@localhost:8096/getDcpEventsRemaining?name=function_name
{
  "dcp_backlog": 4808
}
```

## Failure stats
This group of counters provide an insight into failures encountered during function execution.

```json
curl http://user:password@localhost:8096/getFailureStats?name=function_name
{
  "bucket_op_exception_count": 5108,
  "checkpoint_failure_count": 0,
  "n1ql_op_exception_count": 0,
  "timeout_count": 0
}
```

Name|Datatype|Field|Descripton
|:---|:---|:---|:---
Timeout Count|uint|`timeout_count`|Count of number of handler executions that were terminated because the handler ran longer than the configured script timeout
N1QL Operation Failure Count|uint|`n1ql_op_exception_count`|Count of failures encountered when running N1QL queries. Each such failure would result in an exception thrown in JS handler
Bucket Operation Failure Count|uint|`bucket_op_exception_count`|Count of errors encountered during bucket operations. Each of these failures would result in an exception thrown in JS handler. Integer counter.
Checkpoint Failure Count|uint|`checkpoint_failure_count`|Count of failures when checkpointing last processed sequence numbers by v8 worker. Failures are retried using exponential backoff until timeout.

 
