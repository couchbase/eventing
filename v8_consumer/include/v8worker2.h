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

#ifndef V8WORKER2_H
#define V8WORKER2_H

#include "blocking_deque.h"
#include "bucket.h"
#include "checkpoint_writer.h"
#include "exceptioninsight.h"
#include "histogram.h"
#include "insight.h"
#include "inspector_agent.h"
#include "isolate_data.h"
#include "js_exception.h"
#include "messages.h"
#include "settings.h"
#include "stats.h"
#include "timer_store.h"

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

const int on_update_args_count = 3;
const int on_delete_args_count = 2;
const int timer_callback_args_count = 1;

const Time::time_point DefaultTime = {};

class V8Worker2;

class V8Worker2 {
public:
  V8Worker2(int index, std::shared_ptr<RuntimeStats> stats,
            std::shared_ptr<communicator> comm, v8::Platform *platform,
            std::shared_ptr<settings::cluster> cluster_details_,
            std::shared_ptr<settings::global> global_settings_,
            std::shared_ptr<settings::app_details> app_details_);
  V8Worker2(v8::Platform *platform);

  ~V8Worker2();
  int Start();
  void push_msg(std::unique_ptr<messages::worker_request> msg,
                bool priority = false);

  void RouteMessage();
  std::string get_vb_details_seq();
  void update_seq_for_vb(uint16_t vb, uint64_t vbuuid, uint64_t seq);
  std::tuple<uint64_t, uint64_t, bool>
  AddFilterEvent(uint16_t vb, std::vector<uint8_t> payload);
  void AddCidFilterEvent(uint32_t cid);

  // Timer related
  lcb_STATUS SetTimer(timer::TimerInfo &tinfo);
  lcb_STATUS DelTimer(timer::TimerInfo &tinfo);
  lcb_INSTANCE *GetTimerLcbHandle() const;
  void AddTimerPartition(int vb_no);
  void RemoveTimerPartition(int vb_no);

  std::string cb_source_bucket_;
  std::string cb_source_scope_;
  std::string cb_source_collection_;
  int64_t max_task_duration_;

  std::atomic<bool> scan_timer_{false};
  std::atomic<bool> update_v8_heap_{false};
  std::atomic<bool> mem_check_{false};

  size_t v8_heap_size_;

  IsolateData data_;
  int32_t timer_reduction_ratio_{1};
  int16_t num_vbuckets_;

  std::string CompileHandler(std::string area_name, std::string handler);
  inline std::string GetFunctionInstanceID() { return function_instance_id_; }
  void settings_change(const std::unique_ptr<messages::worker_request> &msg);
  inline void AccumulateLog(const std::string &log) {
    stats_->AddCodeInsight(isolate_, log);
  }

  inline std::pair<int32_t, int32_t> GetQueueDetails() {
    return {worker_queue_->GetMemory(), worker_queue_->GetSize()};
  }

  void StopTimerScan() { stop_timer_scan_.store(true); }

  inline int64_t GetMemory() {
    return worker_queue_->GetMemory() + v8_heap_size_;
  }

  inline void set_feature_matrix(uint32_t feature_matrix) {
    data_.feature_matrix = feature_matrix;
  }

  inline void add_unacked_bytes(int32_t size) {
    unacked_bytes_ += size;
    unacked_count_++;
  }

  inline void add_dcp_mutation_done() {
    dcp_message_done_++;
  }

  inline std::tuple<int32_t, int32_t, uint64_t> ack_bytes() {
    auto unacked_bytes = unacked_bytes_.exchange(0);
    auto unacked_count = unacked_count_.exchange(0);
    return {unacked_count, unacked_bytes, dcp_message_done_.load()};
  }

  inline uint64_t processing_seq_num() { return f_map_->processing_seq_num(); }
  void AddLcbException(int err_code) {
    stats_->AddLcbException(err_code);
  }

  std::string GetOnDeployResult(int return_code);

  std::shared_ptr<RuntimeStats> stats_;

private:
  class vb_handler {
  public:
    vb_handler() : mutex_() { current_executing_vb_ = 0xffff; };

    std::tuple<uint64_t, uint64_t, bool> AddFilterEvent(const uint16_t &vb);

    void RemoveFilterEvent(const uint16_t &vb);

    void AddCidFilterEvent(const uint32_t &cid);
    void RemoveCidFilter(const uint32_t &cid);

    bool CheckAndUpdateFilter(const uint32_t &cid, const uint16_t &vb,
                              const uint64_t &vbuuid, const uint64_t &seq,
                              bool skip_cid_check);
    std::string get_vb_details_seq();
    void reset_current_vb();
    void update_seq(uint16_t vb, uint64_t vbuuid, uint64_t seq);
    inline uint64_t processing_seq_num() {
      mutex_.lock();
      auto seq = processed_seq_[current_executing_vb_];
      mutex_.unlock();
      return seq;
    }

  private:
    std::mutex mutex_;
    std::unordered_map<uint16_t, int32_t> vbfilter_map_;
    std::unordered_map<uint32_t, int32_t> vbfilter_map_for_cid_;
    std::unordered_map<uint16_t, uint64_t> processed_seq_;
    std::unordered_map<uint16_t, uint64_t> vbuuid_;
    uint16_t current_executing_vb_;

    std::pair<uint16_t, bool> GetVbFilterLocked(uint16_t vb);
  };

  v8::Local<v8::ObjectTemplate> NewGlobalObj() const;
  void SetCouchbaseNamespace();

  bool ExecuteScript(const v8::Local<v8::String> &script);
  void TaskDurationWatcher();
  bool DebugExecute(const char *func_name, v8::Local<v8::Value> *args,
                    int args_len);

  void HandleDeleteEvent(const std::unique_ptr<messages::worker_request> &msg);
  void
  HandleMutationEvent(const std::unique_ptr<messages::worker_request> &msg);
  void HandleOnDeployEvent(const std::unique_ptr<messages::worker_request> &msg);
  void HandleCollectionDeleteEvent(
      const std::unique_ptr<messages::worker_request> &msg);
  void
  HandleDeleteVbFilter(const std::unique_ptr<messages::worker_request> &msg);
  void HandleNoOpEvent(const std::unique_ptr<messages::worker_request> &msg);

  int SendUpdate(const messages::payload payload, uint64_t cas, uint32_t expiry,
                 uint8_t datatype);
  int SendDelete(const messages::payload payload);
  int SendDeploy(const std::string &action, const int64_t &delay);
  void SendTimer(std::string callback, std::string timer_ctx);

  void InstallCurlBindings(const std::vector<CurlBinding> &curl_bindings) const;
  void InstallConstantBindings(
      const std::vector<std::pair<std::string, std::string>> constant_bindings)
      const;
  void InstallBucketBindings(
      const std::unordered_map<
          std::string,
          std::unordered_map<std::string, std::vector<std::string>>> &config);

  void InitializeIsolateData();
  void CheckAndCreateTracker();
  inline int get_timer_reduction_ratio() {
    return app_details_->num_vbuckets /
           app_details_->settings->num_timer_partitions;
  }

  void
  InitializeCurlBindingValues(const std::vector<CurlBinding> &curl_bindings);
  void FreeCurlBindings();

  BlockingDeque<std::unique_ptr<messages::worker_request>> *worker_queue_;
  v8::Persistent<v8::Context> context_;
  v8::Persistent<v8::Function> on_update_;
  v8::Persistent<v8::Function> on_delete_;
  v8::Persistent<v8::Function> on_deploy_;

  v8::Isolate *isolate_;

  std::shared_ptr<communicator> comm_;
  v8::Platform *platform_;
  const std::shared_ptr<settings::cluster> cluster_details_;
  const std::shared_ptr<settings::global> global_settings_;
  const std::shared_ptr<settings::app_details> app_details_;

  std::unique_ptr<vb_handler> f_map_;
  std::string user_;
  std::string domain_;

  timer::TimerStore *timer_store_{nullptr};
  std::atomic<bool> timer_execution_{false};
  std::atomic<bool> stop_timer_scan_{false};
  std::atomic<bool> shutdown_terminator_{false};

  std::string function_instance_id_;
  const std::vector<std::string> exception_type_names_;
  std::vector<std::string> curl_binding_values_;
  std::shared_ptr<BucketFactory> bucket_factory_;
  std::list<BucketBinding> bucket_bindings_;
  std::thread processing_thr_;
  std::thread *terminator_thr_;

  std::atomic<int32_t> unacked_bytes_{0};
  std::atomic<uint64_t> dcp_message_done_{0};
  std::atomic<int32_t> unacked_count_{0};
  Time::time_point execute_start_time_;

  std::unique_ptr<CheckpointWriter> checkpoint_writer_{nullptr};
  bool tracker_enabled_{false};

  std::string log_prefix_{""};
  inspector::Agent *agent_;
  bool debugger_started_{false};
};

#endif
