// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

#ifndef V8WORKER_H
#define V8WORKER_H

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <libcouchbase/api3.h>
#include <libcouchbase/couchbase.h>
#include <libplatform/libplatform.h>
#include <list>
#include <map>
#include <regex>
#include <sstream>
#include <stdlib.h>
#include <string>
#include <thread>
#include <time.h>
#include <v8-debug.h>
#include <v8.h>

#include "commands.h"
#include "crc32c.h"
#include "function_templates.h"
#include "histogram.h"
#include "inspector_agent.h"
#include "js_exception.h"
#include "log.h"
#include "n1ql.h"
#include "queue.h"
#include "utils.h"

#include "../../gen/flatbuf/header_generated.h"
#include "../../gen/flatbuf/payload_generated.h"
#include "../../gen/flatbuf/response_generated.h"

#ifndef STANDALONE_BUILD
extern void(assert)(int);
#else
#include <cassert>
#endif

typedef std::chrono::high_resolution_clock Time;
typedef std::chrono::nanoseconds nsecs;

#define SECS_TO_NS 1000 * 1000 * 1000ULL

// Histogram to capture latency stats. Latency buckets have granularity of 1ms,
// starting from 100us to 10s
#define HIST_FROM 100
#define HIST_TILL 1000 * 1000 * 10
#define HIST_WIDTH 1000

#define LCB_OP_RETRY_INTERVAL 100 // in milliseconds
#define LCB_OP_RETRY_COUNTER 3
#define NUM_VBUCKETS 1024

using atomic_ptr_t = std::shared_ptr<std::atomic<int64_t>>;
// Used for checkpointing of vbucket seq nos
typedef std::map<int64_t, atomic_ptr_t> vb_seq_map_t;

typedef struct doc_timer_msg_s {
  std::string
      timer_entry; // <timestamp in GMT>::<callback_func>::<doc_id>::<seq_no>
} doc_timer_msg_t;

// Header frame structure for messages from Go world
typedef struct header_s {
  uint8_t event;
  uint8_t opcode;
  int16_t partition;
  std::string metadata;
} header_t;

// Flatbuffer encoded message from Go world
typedef struct message_s {
  std::string header;
  std::string payload;
} message_t;

// Struct to contain flatbuffer decoded message from Go world
typedef struct worker_msg_s {
  header_t *header;
  message_t *payload;
} worker_msg_t;

typedef struct server_settings_s {
  int checkpoint_interval;
  std::string eventing_dir;
  std::string eventing_port;
  std::string eventing_sslport;
  std::string host_addr;
  std::string kv_host_port;
  std::string rbac_user;
  std::string rbac_pass;
} server_settings_t;

typedef struct handler_config_s {
  std::string app_name;
  int cron_timers_per_doc;
  long curl_timeout;
  std::string dep_cfg;
  int execution_timeout;
  int fuzz_offset;
  int lcb_inst_capacity;
  bool enable_recursive_mutation;
  bool skip_lcb_bootstrap;
} handler_config_t;

class Bucket;
class N1QL;
class ConnectionPool;
class V8Worker;

extern bool debugger_started;

extern bool enable_recursive_mutation;
extern std::atomic<int64_t> bucket_op_exception_count;
extern std::atomic<int64_t> n1ql_op_exception_count;
extern std::atomic<int64_t> timeout_count;
extern std::atomic<int16_t> checkpoint_failure_count;

extern std::atomic<int64_t> on_update_success;
extern std::atomic<int64_t> on_update_failure;
extern std::atomic<int64_t> on_delete_success;
extern std::atomic<int64_t> on_delete_failure;

extern std::atomic<int64_t> doc_timer_create_failure;

extern std::atomic<int64_t> messages_processed_counter;

// DCP or Timer event counter
extern std::atomic<int64_t> cron_timer_msg_counter;
extern std::atomic<int64_t> dcp_delete_msg_counter;
extern std::atomic<int64_t> dcp_mutation_msg_counter;
extern std::atomic<int64_t> doc_timer_msg_counter;

extern std::atomic<int64_t> enqueued_cron_timer_msg_counter;
extern std::atomic<int64_t> enqueued_dcp_delete_msg_counter;
extern std::atomic<int64_t> enqueued_dcp_mutation_msg_counter;
extern std::atomic<int64_t> enqueued_doc_timer_msg_counter;

class V8Worker {
public:
  V8Worker(v8::Platform *platform, handler_config_t *config,
           server_settings_t *settings);
  ~V8Worker();

  void operator()() {

    if (debugger_started)
      return;
    while (!shutdown_terminator) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      if (execute_flag) {
        Time::time_point t = Time::now();
        nsecs ns = std::chrono::duration_cast<nsecs>(t - execute_start_time);

        LOG(logTrace) << "ns.count(): " << ns.count()
                      << "ns, max_task_duration: " << max_task_duration << "ns"
                      << std::endl;
        if (ns.count() > max_task_duration) {
          if (isolate_) {
            LOG(logTrace) << "Task took: " << ns.count()
                          << "ns, terminating it's execution" << std::endl;
            timeout_count++;
            v8::V8::TerminateExecution(isolate_);
            execute_flag = false;
          }
        }
      }
    }
  }

  int V8WorkerLoad(std::string source_s);
  void Checkpoint();
  void RouteMessage();

  int SendUpdate(std::string value, std::string meta, std::string doc_type);
  int SendDelete(std::string meta);
  void SendDocTimer(std::string callback_fn, std::string doc_id,
                    std::string timer_ts, int32_t partition);
  void SendCronTimer(std::string cron_cb_fns, std::string timer_ts,
                     int32_t partition);
  std::string CompileHandler(std::string handler);

  void StartDebugger();
  void StopDebugger();
  bool DebugExecute(const char *func_name, v8::Local<v8::Value> *args,
                    int args_len);

  void Enqueue(header_t *header, message_t *payload);
  void EnqueueDocTimer(header_t *header, message_t *payload);
  int64_t QueueSize();

  void AddLcbException(int err_code);
  void ListLcbExceptions(std::map<int, int64_t> &agg_lcb_exceptions);

  void UpdateHistogram(Time::time_point t);

  v8::Isolate *GetIsolate() { return isolate_; }
  v8::Persistent<v8::Context> context_;

  v8::Persistent<v8::Function> on_update_;
  v8::Persistent<v8::Function> on_delete_;

  v8::Global<v8::ObjectTemplate> worker_template;

  // lcb instances to source and metadata buckets
  lcb_t cb_instance;
  lcb_t meta_cb_instance;
  lcb_t checkpoint_cb_instance; // Separate instance for checkpointing which
                                // writes to metadata bucket. Avoiding sharing
                                // of lcb instance

  std::string app_name_;
  std::string handler_code_;
  std::string script_to_execute_;
  std::string source_map_;

  std::string cb_source_bucket;
  int64_t max_task_duration;

  server_settings_t *settings;

  volatile bool execute_flag;
  volatile bool shutdown_terminator;

  int64_t currently_processed_vb;
  int64_t currently_processed_seqno;
  Time::time_point execute_start_time;

  std::thread checkpointing_thr;
  std::thread processing_thr;
  std::thread *terminator_thr;
  Queue<doc_timer_msg_t> *doc_timer_queue;
  Queue<worker_msg_t> *worker_queue;

  ConnectionPool *conn_pool;
  JsException *js_exception;

  std::mutex lcb_exception_mtx;
  std::map<int, int64_t> lcb_exceptions;

  Histogram *histogram;
  Data data;

private:
  std::string connstr;
  std::string meta_connstr;
  std::string src_path;

  std::mutex doc_timer_mtx;
  std::map<int, std::string>
      doc_timer_checkpoint; // Access controlled by doc_timer_mtx

  std::mutex cron_timer_mtx;
  std::map<int, std::string>
      cron_timer_checkpoint; // Access controlled by cron_timer_mtx

  vb_seq_map_t vb_seq;

  bool ExecuteScript(v8::Local<v8::String> script);
  std::list<Bucket *> bucket_handles;
  N1QL *n1ql_handle;
  std::string last_exception;
  v8::Isolate *isolate_;
  v8::Platform *platform_;
  inspector::Agent *agent;
};

const char *GetUsername(void *cookie, const char *host, const char *port,
                        const char *bucket);
const char *GetPassword(void *cookie, const char *host, const char *port,
                        const char *bucket);
const char *GetUsernameCached(void *cookie, const char *host, const char *port,
                              const char *bucket);
const char *GetPasswordCached(void *cookie, const char *host, const char *port,
                              const char *bucket);
#endif
