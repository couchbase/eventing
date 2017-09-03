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

#include "crc32c.h"
#include "inspector_agent.h"
#include "log.h"
#include "n1ql.h"

#ifndef STANDALONE_BUILD
extern void(assert)(int);
#else
#include <cassert>
#endif

typedef std::chrono::high_resolution_clock Time;
typedef std::chrono::nanoseconds nsecs;

#define SECS_TO_NS 1000 * 1000 * 1000ULL
#define LCB_OP_RETRY_INTERVAL 100 // in milliseconds

struct Result {
  lcb_CAS cas;
  lcb_error_t rc;
  std::string value;
  uint32_t exptime;

  Result() : cas(0), rc(LCB_SUCCESS) {}
};

class Bucket;
class V8Worker;

v8::Local<v8::String> createUtf8String(v8::Isolate *isolate, const char *str);
std::string ObjectToString(v8::Local<v8::Value> value);
std::string ToString(v8::Isolate *isolate, v8::Handle<v8::Value> object);

lcb_t *UnwrapLcbInstance(v8::Local<v8::Object> obj);
lcb_t *UnwrapV8WorkerLcbInstance(v8::Local<v8::Object> obj);
V8Worker *UnwrapV8WorkerInstance(v8::Local<v8::Object> obj);

std::map<std::string, std::string> *UnwrapMap(v8::Local<v8::Object> obj);

extern bool enable_recursive_mutation;

class V8Worker {
public:
  V8Worker(std::string app_name, std::string dep_cfg,
           std::string curr_host_addr, std::string kv_host_port,
           std::string rbac_user, std::string rbac_pass, int lcb_inst_capacity,
           int execution_timeout, bool enable_recursive_mutation);
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
  const char *V8WorkerLastException();
  const char *V8WorkerVersion();

  std::string GetSourceMap();

  int SendUpdate(std::string value, std::string meta, std::string doc_type);
  int SendDelete(std::string meta);
  void SendDocTimer(std::string doc_id, std::string callback_fn);
  void SendNonDocTimer(std::string doc_ids_cb_fns);
  void StartDebugger();
  void StopDebugger();

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
  std::string app_name_;

  std::string curr_host_addr;
  std::string cb_kv_endpoint;
  std::string cb_source_bucket;

  volatile bool execute_flag;
  volatile bool shutdown_terminator;
  volatile bool debugger_started;
  Time::time_point execute_start_time;
  uint64_t max_task_duration;
  std::thread *terminator_thr;

  ConnectionPool *conn_pool;

private:
  std::string connstr;
  std::string meta_connstr;
  std::string rbac_pass;

  bool ExecuteScript(v8::Local<v8::String> script);
  std::list<Bucket *> bucket_handles;
  std::string last_exception;
  v8::Isolate *isolate_;
  v8::Platform *platform;
  inspector::Agent *agent;
};

#endif
