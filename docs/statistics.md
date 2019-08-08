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
     "CRON_TIMER_EVENTS": 231,
     "TIMERS_IN_PAST": 4
   },
   "events_remaining": {
     "dcp_backlog": 33
   },
   "execution_stats": {
     "agg_queue_size": 2,
     "cron_timer_msg_counter": 0,
     "dcp_delete_msg_counter": 14,
     "dcp_mutation_msg_counter": 1,
     "doc_timer_create_failure": 0,
     "doc_timer_msg_counter": 0,
     "messages_parsed": 67,
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
   "curl_latency_stats": {
    "414200": 1,
    "442400": 1,
    "484400": 1,
    "493800": 1,
    "576500": 1,
    "587500": 1,
   },
   "worker_pids": {
     "worker_h1_0": 28558,
     "worker_h1_1": 28559,
     "worker_h1_2": 28560
   },
   "lcb_exception_stats": {
     "13": 200
   },
   "planner_stats": [
    {
      "host_name": "127.0.0.1:9301",
      "start_vb": 0,
      "vb_count": 512
    },
    {
      "host_name": "192.168.0.14:9300",
      "start_vb": 512,
      "vb_count": 512
    }
   ],
   "vb_distribution_stats": {
      "127.0.0.1:9300": {
      "worker_h1_2": "[854-1023]"
    },
    "127.0.0.1:9301": {
      "worker_h1_0": "[0-170]",
      "worker_h1_1": "[171-341]",
      "worker_h1_2": "[342-511]"
    },
    "192.168.0.14:9300": {
      "worker_h1_0": "[512-682]",
      "worker_h1_1": "[683-853]"
    }
   },
   "dcp_event_backlog_per_vb": {"0":1, ...},
   "doc_timer_debug_stats": {"0":{}, ...},
   "plasma_stats": {"AllocSz": 96, ...},
   "seqs_processed":{"0":0,"1":0, ...}
 }
]
```
> Omitting the parameter `type=full` will exclude `dcp_event_backlog_per_vb`, `doc_timer_debug_stats`, `latency_stats`, `plasma_stats` and `seqs_processed` from the response.

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
  "agg_queue_size": 2,
  "cron_timer_msg_counter": 0,
  "dcp_delete_msg_counter": 5108,
  "dcp_mutation_msg_counter": 11510282,
  "doc_timer_create_failure": 0,
  "doc_timer_msg_counter": 0,
  "messages_parsed": 11803452,
  "on_delete_failure": 5108,
  "on_delete_success": 6400893,
  "on_update_failure": 0,
  "on_update_success": 11510282
}
```

Name|Datatype|Field|Descripton
|:---|:---|:---|:---
| Queue Size | int64 | `agg_queue_size` | Count of events that are queued on worker processes, waiting execution. |
| Cron timer counter from eventing-consumer | int64 | `cron_timer_msg_counter`  | Count of Cron timer messages sent to their designated handler for execution  |
| DCP Delete counter from eventing-consumer | int64 | `dcp_delete_msg_counter` | Count of DCP_DELETION messages sent to their designated handler for execution |
| DCP Mutation counter from eventing-consumer | int64 | `dcp_mutation_msg_counter` | Count of DCP_MUTATION messages sent to their designated handler for execution |
| Document Timer Creation Retries | int64 | `doc_timer_create_failure` | Count of number of times document timers creations that were retried. Retry continues till script timeout. |
| Messages parsed counter from eventing-consumer | int64 | `messages_parsed` | Count of flatbuffer encoded messages decoded/parsed by eventing-consumer. |
| OnDelete handler failures | int64 | `on_delete_failure` | Count of number of delete handler executions that terminated with an uncaught exception. |
| OnUpdate handler failures | int64 | `on_update_failure` | Count of number of update handler executions that terminated with an uncaught exception. |
| OnDelete handler successful invocations | int64 | `on_delete_success` | Counter for number of times OnDelete handler was executed successfully. |
| OnUpdate handler successful invocations | int64 | `on_update_success` | Counter for number of times OnUpdate handler was executed successfully. |

## Latency Stats
These give latency of handler executions in wall clock time, in aggregate, across all handlers and timers. The returned object has a key which is the latency range in **microseconds** and value which is the count of executions in this range. `curl_latency_stats` represents the latency (i.e. the time that was spent in transfer of data) of `curl()` calls made in the handler.

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
| Timeout Count | int64 | `timeout_count` | Count of number of handler executions that were terminated because the handler ran longer than the configured script timeout |
| N1QL Operation Failure Count | int64 | `n1ql_op_exception_count` | Count of failures encountered when running N1QL queries. Each such failure would result in an exception thrown in JS handler |
| Bucket Operation Failure Count | int64 | `bucket_op_exception_count` | Count of errors encountered during bucket operations. Each of these failures would result in an exception thrown in JS handler. Integer counter. |
| Checkpoint Failure Count | int64 | `checkpoint_failure_count` | Count of failures when checkpointing last processed sequence numbers by v8 worker. Failures are retried using exponential backoff until timeout. |
