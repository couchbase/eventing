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
#include <list>
#include <map>
#include <string>
#include <thread>

#include <libplatform/libplatform.h>
#include <v8-debug.h>
#include <v8.h>

#include <libcouchbase/api3.h>
#include <libcouchbase/couchbase.h>

#include "commands.h"
#include "crc32c.h"
#ifdef ZLIB_FOUND
#include "inspector_agent.h"
#endif
#include "js_exception.h"
#include "log.h"
#include "n1ql.h"
#include "queue.h"

#include "../../flatbuf/include/header_generated.h"
#include "../../flatbuf/include/payload_generated.h"
#include "../../flatbuf/include/response_generated.h"

#ifndef STANDALONE_BUILD
extern void(assert)(int);
#else
#include <cassert>
#endif

typedef std::chrono::high_resolution_clock Time;
typedef std::chrono::nanoseconds nsecs;

#define SECS_TO_NS 1000 * 1000 * 1000ULL
#define LCB_OP_RETRY_INTERVAL 100 // in milliseconds
#define EXCEPTION_STR_SIZE 20
#define CONSOLE_LOG_MAX_ARITY 20

typedef struct header_s {
  uint8_t event;
  uint8_t opcode;
  int16_t partition;
  std::string metadata;
} header_t;

typedef struct message_s {
  std::string header;
  std::string payload;
} message_t;

struct Result {
  lcb_CAS cas;
  lcb_error_t rc;
  std::string value;
  uint32_t exptime;

  Result() : cas(0), rc(LCB_SUCCESS) {}
};

typedef struct worker_msg_s {
  header_t *header;
  message_t *payload;
} worker_msg_t;

typedef struct server_settings_s {
  std::string eventing_port;
  std::string host_addr;
  std::string kv_host_port;
  std::string rbac_pass;
  std::string rbac_user;
} server_settings_t;

typedef struct handler_config_s {
  std::string app_name;
  std::string dep_cfg;
  int execution_timeout;
  int lcb_inst_capacity;
  bool enable_recursive_mutation;
} handler_config_t;

class Bucket;
class ConnectionPool;
class V8Worker;

v8::Local<v8::String> createUtf8String(v8::Isolate *isolate, const char *str);
std::string ObjectToString(v8::Local<v8::Value> value);
std::string ToString(v8::Isolate *isolate, v8::Handle<v8::Value> object);

lcb_t *UnwrapLcbInstance(v8::Local<v8::Object> obj);
lcb_t *UnwrapV8WorkerLcbInstance(v8::Local<v8::Object> obj);
V8Worker *UnwrapV8WorkerInstance(v8::Local<v8::Object> obj);

std::map<std::string, std::string> *UnwrapMap(v8::Local<v8::Object> obj);

int vasprintf(char **strp, const char *fmt, va_list ap);

extern bool enable_recursive_mutation;

class V8Worker {
public:
  V8Worker(v8::Platform *platform, handler_config_t *config,
           server_settings_t *settings);
  ~V8Worker();

  void operator()() const {
    if (debugger_started)
      return;
    while (!shutdown_terminator) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      if (execute_flag) {
        Time::time_point t = Time::now();
        nsecs ns = std::chrono::duration_cast<nsecs>(t - execute_start_time);

        LOG(logTrace) << "ns.count(): " << ns.count()
                      << "ns, max_task_duration: " << max_task_duration << "ns"
                      << '\n';
        if (ns.count() > max_task_duration) {
          if (isolate_) {
            LOG(logTrace) << "Task took: " << ns.count()
                          << "ns, terminating it's execution" << '\n';
            v8::V8::TerminateExecution(isolate_);
          }
        }
      }
    }
  }

  int V8WorkerLoad(std::string source_s);
  void RouteMessage();

  const char *V8WorkerLastException();
  const char *V8WorkerVersion();

  int SendUpdate(std::string value, std::string meta, std::string doc_type);
  int SendDelete(std::string meta);
  void SendDocTimer(std::string doc_id, std::string callback_fn);
  void SendNonDocTimer(std::string doc_ids_cb_fns);

  void StartDebugger();
  void StopDebugger();
  bool DebugExecute(const char *func_name, v8::Local<v8::Value> *args,
                    int args_len);

  void Enqueue(header_t *header, message_t *payload);

  void V8WorkerDispose();
  void V8WorkerTerminateExecution();

  v8::Isolate *GetIsolate() { return isolate_; }
  v8::Persistent<v8::Context> context_;

  v8::Persistent<v8::Function> on_update_;
  v8::Persistent<v8::Function> on_delete_;

  v8::Global<v8::ObjectTemplate> worker_template;

  lcb_t cb_instance;
  lcb_t meta_cb_instance;

  std::string script_to_execute_;
  std::string source_map_;
  std::string handler_code_;
  std::string app_name_;

  std::string curr_eventing_port;
  std::string curr_host;
  std::string cb_kv_endpoint;
  std::string cb_source_bucket;

  volatile bool execute_flag;
  volatile bool shutdown_terminator;
  volatile bool debugger_started;

  Time::time_point execute_start_time;
  uint64_t max_task_duration;

  std::thread *terminator_thr;
  std::thread processing_thr;
  Queue<worker_msg_t> *worker_queue;

  ConnectionPool *conn_pool;
  static JsException exception;

private:
  std::string connstr;
  std::string meta_connstr;
  std::string rbac_pass;
  std::string src_path;

  bool ExecuteScript(v8::Local<v8::String> script);
  std::list<Bucket *> bucket_handles;
  std::string last_exception;
  v8::Isolate *isolate_;
  v8::Platform *platform_;
#ifdef ZLIB_FOUND
  inspector::Agent *agent;
#endif
};

#endif
