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
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <libcouchbase/couchbase.h>
#include <libplatform/libplatform.h>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <uv.h>
#include <v8.h>
#include <vector>

#include "blocking_deque.h"
#include "bucket.h"
#include "checkpoint_writer.h"
#include "commands.h"
#include "exceptioninsight.h"
#include "histogram.h"
#include "insight.h"
#include "inspector_agent.h"
#include "isolate_data.h"
#include "js_exception.h"
#include "log.h"
#include "parse_deployment.h"
#include "timer_store.h"
#include "utils.h"
#include "v8log.h"

#include "../../gen/flatbuf/header_generated.h"
#include "../../gen/flatbuf/payload_generated.h"
#include "../../gen/flatbuf/response_generated.h"

typedef std::chrono::high_resolution_clock Time;
typedef std::chrono::nanoseconds nsecs;

#define SECS_TO_NS 1000 * 1000 * 1000ULL

extern int64_t timer_context_size;

const int on_update_args_count = 3;
const int on_delete_args_count = 2;
const int timer_callback_args_count = 1;

using atomic_ptr_t = std::shared_ptr<std::atomic<uint64_t>>;
// Used for checkpointing of vbucket seq nos
typedef std::map<int, atomic_ptr_t> vb_seq_map_t;
typedef std::map<int, std::mutex *> vb_lock_map_t;

typedef struct timer_msg_s {
  std::size_t GetSize() const { return timer_entry.length(); }

  std::string timer_entry;
} timer_msg_t;

// Header frame structure for messages from Go world
struct MessageHeader {
  MessageHeader() = default;
  ~MessageHeader() = default;
  MessageHeader(MessageHeader &&other) noexcept
      : event(other.event), opcode(other.opcode), partition(other.partition),
        metadata(std::move(other.metadata)) {}

  MessageHeader &operator=(MessageHeader &&other) noexcept {
    event = other.event;
    opcode = other.opcode;
    partition = other.partition;
    metadata = std::move(other.metadata);
    return *this;
  }
  MessageHeader(const MessageHeader &other) = delete;
  MessageHeader &operator=(const MessageHeader &other) = delete;

  std::size_t GetSize() const {
    return metadata.length() + sizeof(event) + sizeof(opcode) +
           sizeof(partition);
  }

  uint8_t event{0};
  uint8_t opcode{0};
  int16_t partition{0};
  std::string metadata;
};

// Flatbuffer encoded message from Go world
struct MessagePayload {
  MessagePayload() = default;
  ~MessagePayload() = default;
  MessagePayload(MessagePayload &&other) noexcept
      : header(std::move(other.header)), payload(std::move(other.payload)) {}

  MessagePayload &operator=(MessagePayload &&other) noexcept {
    header = std::move(other.header);
    payload = std::move(other.payload);
    return *this;
  }
  MessagePayload(const MessagePayload &other) = delete;
  MessagePayload &operator=(const MessagePayload &other) = delete;

  std::size_t GetSize() const { return header.length() + payload.length(); }

  std::string header;
  std::string payload;
};

// Struct to contain flatbuffer decoded message from Go world
struct WorkerMessage {
  WorkerMessage() = default;
  ~WorkerMessage() = default;
  WorkerMessage(WorkerMessage &&other) noexcept
      : header(std::move(other.header)), payload(std::move(other.payload)) {}

  WorkerMessage &operator=(WorkerMessage &&other) noexcept {
    header = std::move(other.header);
    payload = std::move(other.payload);
    return *this;
  }
  WorkerMessage(const WorkerMessage &other) = delete;
  WorkerMessage &operator=(const WorkerMessage &other) = delete;

  std::size_t GetSize() const { return header.GetSize() + payload.GetSize(); }

  MessageHeader header;
  MessagePayload payload;
};

typedef struct server_settings_s {
  int checkpoint_interval;
  std::string debugger_port;
  std::string eventing_dir;
  std::string eventing_port;
  std::string eventing_sslport;
  std::string host_addr;
  std::string certFile;
} server_settings_t;

typedef struct handler_config_s {
  bool n1ql_prepare_all;
  std::string app_name;
  std::string bucket;
  std::string scope;
  std::string dep_cfg;
  std::string lang_compat;
  int execution_timeout;
  int cursor_checkpoint_timeout;
  int lcb_retry_count;
  int lcb_timeout;
  int lcb_cursor_checkpoint_timeout;
  int lcb_inst_capacity;
  int num_timer_partitions;
  bool skip_lcb_bootstrap;
  bool using_timer;
  int64_t timer_context_size;
  int64_t bucket_cache_size;
  int64_t bucket_cache_age;
  int64_t curl_max_allowed_resp_size;
  std::string n1ql_consistency;
  std::vector<std::string> handler_headers;
  std::vector<std::string> handler_footers;
} handler_config_t;

enum RETURN_CODE {
  kSuccess = 0,
  kFailedToCompileJs,
  kNoHandlersDefined,
  kFailedInitBucketHandle,
  kOnUpdateCallFail,
  kOnDeleteCallFail,
  kOnDeployCallFail,
  kToLocalFailed,
  kJSONParseFailed
};

enum class OnDeployState {
  PENDING,
  FINISHED,
  FAILED
};

class BucketBinding;
class ConnectionPool;
class V8Worker;

struct ParsedMetadata {
  int vb{0};
  uint64_t seq_num{0};
  uint32_t cid{0};

  std::string scope{""};
  std::string collection{""};
  std::string key{""};
  std::string cas{""};
  std::string rootcas{""};
};

class V8Worker {
public:
  V8Worker(v8::Platform *platform, handler_config_t *h_config,
           server_settings_t *server_settings, const std::string &function_name,
           const std::string &function_id,
           const std::string &function_instance_id,
           const std::string &user_prefix, Histogram *latency_stats,
           Histogram *curl_latency_stats, const std::string &ns_server_port,
           const int32_t &num_vbuckets, vb_seq_map_t *vb_seq,
           std::vector<uint64_t> *processed_bucketops, vb_lock_map_t *vb_locks,
           const std::string &user, const std::string &domain);
  ~V8Worker();

  void EnableTracker();
  void DisableTracker();
  int V8WorkerLoad(std::string source_s);
  void RouteMessage();
  void TaskDurationWatcher();

  int SendUpdate(const std::string &value, const std::string &meta,
                 const std::string &xattr, bool is_binary);
  int SendDelete(const std::string &value, const std::string &meta);
  int SendDeploy(const std::string &action, const int64_t &delay);
  void SendTimer(std::string callback, std::string timer_ctx);
  std::string Compile(std::string handler);

  void StartDebugger();
  void StopDebugger();
  bool DebugExecute(const char *func_name, v8::Local<v8::Value> *args,
                    int args_len);

  void PushFront(std::unique_ptr<WorkerMessage> worker_msg);

  void PushBack(std::unique_ptr<WorkerMessage> worker_msg);

  void AddLcbException(int err_code);
  void ListLcbExceptions(std::map<int, int64_t> &agg_lcb_exceptions);

  void UpdateHistogram(Time::time_point t);
  void UpdateCurlLatencyHistogram(const Time::time_point &start);

  void GetBucketOpsMessages(std::vector<uv_buf_t> &messages);

  void UpdateVbFilter(int vb_no, uint64_t seq_no);
  void UpdateDeletedCid(const std::unique_ptr<WorkerMessage> &msg);

  void SetFeatureList(uint32_t disabled_events);

  uint64_t GetVbFilter(int vb_no);

  CodeInsight &GetInsight();

  ExceptionInsight &GetExceptionInsight();

  void EraseVbFilter(int vb_no);

  void UpdateBucketopsSeqnoLocked(int vb_no, uint64_t seq_no);

  uint64_t GetBucketopsSeqno(int vb_no);

  std::unique_lock<std::mutex> GetAndLockBucketOpsLock();

  std::pair<std::optional<ParsedMetadata>, int>
  ParseMetadata(const std::string &metadata) const;

  std::pair<std::optional<ParsedMetadata>, int>
  ParseMetadataWithAck(const std::string &metadata_str, int &skip_ack,
                       const bool ack_check) const;

  void SetThreadExitFlag();

  void StopTimerScan();

  void SetFailFastTimerScans();

  void ResetFailFastTimerScans();

  void UpdatePartitions(const std::unordered_set<int64_t> &vbuckets);

  std::unordered_set<int64_t> GetPartitions() const;

  lcb_STATUS SetTimer(timer::TimerInfo &tinfo);
  lcb_STATUS DelTimer(timer::TimerInfo &tinfo);

  lcb_INSTANCE *GetTimerLcbHandle() const;
  void AddTimerPartition(int vb_no);
  void RemoveTimerPartition(int vb_no);

  inline std::string GetFunctionID() { return function_id_; }

  inline std::string GetFunctionInstanceID() { return function_instance_id_; }

  v8::Isolate *GetIsolate() { return isolate_; }
  v8::Persistent<v8::Context> context_;
  v8::Persistent<v8::Function> on_update_;
  v8::Persistent<v8::Function> on_delete_;
  v8::Persistent<v8::Function> on_deploy_;

  std::string app_name_;
  std::string bucket_;
  std::string scope_;
  std::string script_to_execute_;

  std::string cb_source_bucket_;
  std::string cb_source_scope_;
  std::string cb_source_collection_;
  int64_t max_task_duration_;

  server_settings_t *settings_;

  bool shutdown_terminator_{false};
  static bool debugger_started_;

  uint64_t currently_processed_vb_;
  uint64_t currently_processed_seqno_;
  Time::time_point execute_start_time_;

  std::thread processing_thr_;
  std::thread *terminator_thr_;
  BlockingDeque<std::unique_ptr<WorkerMessage>> *worker_queue_;

  size_t v8_heap_size_;
  std::mutex lcb_exception_mtx_;
  std::atomic<bool> scan_timer_;
  std::atomic<bool> update_v8_heap_;
  std::atomic<bool> run_gc_;
  std::atomic<bool> event_processing_ongoing_;
  std::map<int, int64_t> lcb_exceptions_;
  IsolateData data_;
  int32_t num_vbuckets_{1024};
  int32_t timer_reduction_ratio_{1};

private:
  CompilationInfo CompileHandler(std::string app_name, std::string handler);
  std::string AddHeadersAndFooters(std::string code);

  void UpdateSeqNumLocked(int vb, uint64_t seq_num);
  void HandleDeleteEvent(const std::unique_ptr<WorkerMessage> &msg);
  void HandleMutationEvent(const std::unique_ptr<WorkerMessage> &msg);
  void HandleNoOpEvent(const std::unique_ptr<WorkerMessage> &msg);
  void HandleDeleteCidEvent(const std::unique_ptr<WorkerMessage> &msg);
  void HandleDeployEvent(const std::unique_ptr<WorkerMessage> &msg);
  std::tuple<bool, bool> IsFilteredEventLocked(bool skip_cid_check,
                                               uint32_t cid, int vb,
                                               uint64_t seq_num);
  std::tuple<std::optional<ParsedMetadata>, bool>
  ParseWorkerMessage(const std::unique_ptr<WorkerMessage> &msg) const;
  void SetCouchbaseNamespace();
  v8::Local<v8::ObjectTemplate> NewGlobalObj() const;
  void InstallCurlBindings(const std::vector<CurlBinding> &curl_bindings) const;
  void InstallConstantBindings(
      const std::vector<std::pair<std::string, std::string>> constant_bindings)
      const;
  void InstallBucketBindings(
      const std::unordered_map<
          std::string,
          std::unordered_map<std::string, std::vector<std::string>>> &config);
  void InitializeIsolateData(const server_settings_t *server_settings,
                             const handler_config_t *h_config);

  void
  InitializeCurlBindingValues(const std::vector<CurlBinding> &curl_bindings);
  void FreeCurlBindings();
  std::vector<uv_buf_t>
  BuildResponse(const std::string &payload, resp_msg_type msg_type,
                bucket_ops_response_opcode response_opcode);
  bool ExecuteScript(const v8::Local<v8::String> &script);

  void UpdateV8HeapSize();
  void ForceRunGarbageCollector();
  std::unique_lock<std::mutex> GetAndLockVbLock(int vb_no);
  Histogram *latency_stats_;
  Histogram *curl_latency_stats_;

  std::string src_path_;

  vb_seq_map_t *vb_seq_;
  vb_lock_map_t *vb_locks_;

  std::vector<std::vector<uint64_t>> vbfilter_map_;
  std::map<uint32_t, std::map<uint16_t, uint64_t>> vbfilter_map_for_cid_;
  std::mutex vbfilter_map_for_cid_lock_;
  std::vector<uint64_t> *processed_bucketops_;
  std::mutex bucketops_lock_;

  std::mutex pause_lock_;
  v8::Isolate *isolate_;
  v8::Platform *platform_;
  inspector::Agent *agent_;
  std::string certFile_;
  std::string function_name_;
  std::string function_id_;
  std::string function_instance_id_;
  std::string user_prefix_;
  std::string ns_server_port_;
  std::string user_;
  std::string domain_;
  int64_t cache_size_;
  int64_t cache_expiry_age_;

  timer::TimerStore *timer_store_{nullptr};
  std::atomic<bool> thread_exit_cond_;
  const std::vector<std::string> exception_type_names_;
  std::vector<std::string> curl_binding_values_;
  std::atomic<bool> stop_timer_scan_;
  std::atomic<bool> timed_out_;
  std::atomic<bool> tracker_enabled_;
  std::unordered_set<int64_t> partitions_;
  std::shared_ptr<BucketFactory> bucket_factory_;
  std::unique_ptr<CheckpointWriter> checkpoint_writer_{nullptr};
  std::list<BucketBinding> bucket_bindings_;
  std::vector<std::string> handler_headers_;
  std::vector<std::string> handler_footers_;
};

#endif
