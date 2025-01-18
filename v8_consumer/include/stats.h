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

#ifndef STATS_H
#define STATS_H

#include <atomic>
#include <chrono>
#include <map>
#include <string>
#include <v8.h>

#include "exceptioninsight.h"
#include "histogram.h"
#include "insight.h"

typedef std::chrono::high_resolution_clock Time;
typedef std::chrono::nanoseconds nsecs;

extern std::atomic<int64_t> uv_try_write_failure_counter;

class RuntimeStats {
public:
  explicit RuntimeStats(const std::string &instance_id,
                        const std::string &script) {
    code_insight_.Setup(script);
    exception_insight_.Setup(instance_id);

    // pre declare all the possible key to avoid rebalance of hash map
    // execution stats
    execution_stats_["curl.get"] = 0;
    execution_stats_["curl.post"] = 0;
    execution_stats_["curl.delete"] = 0;
    execution_stats_["curl.head"] = 0;
    execution_stats_["curl.put"] = 0;
    execution_stats_["curl_success_count"] = 0;
    execution_stats_["on_update_success"] = 0;
    execution_stats_["on_update_failure"] = 0;
    execution_stats_["on_delete_success"] = 0;
    execution_stats_["on_delete_failure"] = 0;
    execution_stats_["no_op_counter"] = 0;

    execution_stats_["timer_callback_failure"] = 0;
    execution_stats_["timer_callback_success"] = 0;
    execution_stats_["timer_create_counter"] = 0;
    execution_stats_["timer_create_failure"] = 0;
    execution_stats_["timer_cancel_counter"] = 0;
    execution_stats_["timer_msg_counter"] = 0;

    execution_stats_["dcp_delete_msg_counter"] = 0;
    execution_stats_["dcp_mutation_msg_counter"] = 0;
    execution_stats_["filtered_dcp_mutation_counter"] = 0;
    execution_stats_["filtered_dcp_delete_counter"] = 0;
    execution_stats_["enqueued_dcp_mutation_msg_counter"] = 0;
    execution_stats_["enqueued_dcp_delete_msg_counter"] = 0;
    execution_stats_["dcp_mutation_checkpoint_cas_mismatch"] = 0;

    execution_stats_["messages_parsed"] = 0;
    execution_stats_["lcb_retry_failure"] = 0;
    execution_stats_["num_processed_events"] = 0;
    execution_stats_["processed_events_size"] = 0;

    // failure stats
    failure_stats_["curl_failure_count"] = 0;
    failure_stats_["curl_timeout_count"] = 0;
    failure_stats_["curl_non_200_response"] = 0;

    failure_stats_["n1ql_op_exception_count"] = 0;
    failure_stats_["analytics_op_exception_count"] = 0;
    failure_stats_["bucket_op_exception_count"] = 0;
    failure_stats_["bkt_ops_cas_mismatch_count"] = 0;
    failure_stats_["bucket_op_cache_miss_count"] = 0;
    failure_stats_["dcp_mutation_checkpoint_failure"] = 0;
    failure_stats_["timer_callback_missing_counter"] = 0;
    failure_stats_["timeout_count"] = 0;
    failure_stats_["timer_context_size_exceeded_counter"] = 0;
    failure_stats_["bucket_cache_overflow_count"] = 0;
    failure_stats_["debugger_events_lost"] = 0;
    failure_stats_["curl_max_resp_size_exceeded"] = 0;
  }

  ~RuntimeStats() {}

  inline void IncrementExecutionStat(const std::string &key, int64_t value) {
    execution_stats_[key].fetch_add(value);
  }

  inline void IncrementExecutionStat(const std::string &key) {
    execution_stats_[key]++;
  }

  inline int64_t GetExecutionStat(const std::string &key) {
    return execution_stats_[key].load();
  }

  inline void IncrementFailureStat(const std::string &key) {
    failure_stats_[key]++;
  }

  inline int64_t GetFailureStat(const std::string &key) {
    return failure_stats_[key].load();
  }

  inline void AddLcbException(int err_code) {
    lcbMutex.lock();
    lcb_exception_stats[err_code]++;
    lcbMutex.unlock();
  }

  inline void AddException(v8::Isolate *isolate_, v8::TryCatch &try_catch) {
    code_insight_.AccumulateException(isolate_, try_catch);
    exception_insight_.AccumulateException(isolate_, try_catch);
  }

  inline void AddCodeInsight(v8::Isolate *isolate_, const std::string &log) {
    code_insight_.AccumulateLog(isolate_, log);
  }

  void UpdateHistogram(const Time::time_point &start) {
    Time::time_point t = Time::now();
    nsecs ns = std::chrono::duration_cast<nsecs>(t - start);
    latency_stats_.Add(ns.count() / 1000);
  }

  void UpdateCurlLatencyHistogram(const Time::time_point &start) {
    Time::time_point t = Time::now();
    nsecs ns = std::chrono::duration_cast<nsecs>(t - start);
    curl_latency_stats_.Add(ns.count() / 1000);
  }

  std::string GetExecutionStats();
  std::string GetFailureStats();
  std::string GetCodeInsight();
  std::string GetLatencyStats();
  std::string GetCurlLatencyStats();
  std::string GetLcbExceptionStats();

private:
  RuntimeStats(RuntimeStats const &) = delete;
  RuntimeStats &operator=(RuntimeStats const &) = delete;

  std::unordered_map<std::string, std::atomic<int64_t>> execution_stats_;
  std::unordered_map<std::string, std::atomic<int64_t>> failure_stats_;
  CodeInsight code_insight_;
  ExceptionInsight exception_insight_;
  Histogram latency_stats_;
  Histogram curl_latency_stats_;

  std::mutex lcbMutex;
  std::unordered_map<int, int64_t> lcb_exception_stats;
};

#endif
