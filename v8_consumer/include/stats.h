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

class RuntimeStats {
public:
  explicit RuntimeStats(const std::string &instance_id,
                        const std::string &script) {
    code_insight_.Setup(script);
    exception_insight_.Setup(instance_id);
  }

  ~RuntimeStats() {}

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

private:
  RuntimeStats(RuntimeStats const &) = delete;
  RuntimeStats &operator=(RuntimeStats const &) = delete;

  std::map<std::string, std::atomic<int64_t>> execution_stats_;
  std::map<std::string, std::atomic<int64_t>> failure_stats_;
  CodeInsight code_insight_;
  ExceptionInsight exception_insight_;
  Histogram latency_stats_;
  Histogram curl_latency_stats_;
};

#endif
