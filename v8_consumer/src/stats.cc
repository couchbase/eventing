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

#include "stats.h"
#include "utils.h"

#include <nlohmann/json.hpp>

std::string RuntimeStats::GetFailureStats() {
  nlohmann::json fstats;
  fstats["bucket_op_exception_count"] =
      failure_stats_["bucket_op_exception_count"].load();
  fstats["bucket_op_cache_miss_count"] =
      failure_stats_["bucket_op_cache_miss_count"].load();
  fstats["bucket_cache_overflow_count"] =
      failure_stats_["bucket_cache_overflow_count"].load();
  fstats["bkt_ops_cas_mismatch_count"] =
      failure_stats_["bkt_ops_cas_mismatch_count"].load();
  fstats["n1ql_op_exception_count"] =
      failure_stats_["n1ql_op_exception_count"].load();
  fstats["analytics_op_exception_count"] =
      failure_stats_["analytics_op_exception_count"].load();
  fstats["search_op_exception_count"] =
      failure_stats_["search_op_exception_count"].load();
  fstats["timeout_count"] = failure_stats_["timeout_count"].load();
  fstats["debugger_events_lost"] =
      failure_stats_["debugger_events_lost"].load();
  fstats["timer_context_size_exceeded_counter"] =
      failure_stats_["timer_context_size_exceeded_counter"].load();
  fstats["timer_callback_missing_counter"] =
      failure_stats_["timer_callback_missing_counter"].load();
  fstats["dcp_mutation_checkpoint_failure"] =
      failure_stats_["dcp_mutation_checkpoint_failure"].load();

  fstats["curl_non_200_response"] =
      failure_stats_["curl_non_200_response"].load();
  fstats["curl_timeout_count"] = failure_stats_["curl_timeout_count"].load();
  fstats["curl_failure_count"] = failure_stats_["curl_failure_count"].load();
  fstats["curl_max_resp_size_exceeded"] =
      failure_stats_["curl_max_resp_size_exceeded"].load();

  fstats["timestamp"] = GetTimestampNow();
  return fstats.dump();
}

std::string RuntimeStats::GetExecutionStats() {
  nlohmann::json estats;

  estats["on_update_success"] = execution_stats_["on_update_success"].load();
  estats["on_update_failure"] = execution_stats_["on_update_failure"].load();
  estats["on_delete_success"] = execution_stats_["on_delete_success"].load();
  estats["on_delete_failure"] = execution_stats_["on_delete_failure"].load();
  estats["no_op_counter"] = execution_stats_["no_op_counter"].load();
  estats["timer_callback_success"] =
      execution_stats_["timer_callback_success"].load();
  estats["timer_callback_failure"] =
      execution_stats_["timer_callback_failure"].load();
  estats["timer_create_failure"] =
      execution_stats_["timer_create_failure"].load();
  estats["messages_parsed"] = execution_stats_["messages_parsed"].load();
  estats["dcp_delete_msg_counter"] =
      execution_stats_["dcp_delete_msg_counter"].load();
  estats["dcp_mutation_msg_counter"] =
      execution_stats_["dcp_mutation_msg_counter"].load();
  estats["timer_msg_counter"] = execution_stats_["timer_msg_counter"].load();
  estats["timer_create_counter"] =
      execution_stats_["timer_create_counter"].load();
  estats["timer_cancel_counter"] =
      execution_stats_["timer_cancel_counter"].load();
  estats["lcb_retry_failure"] = execution_stats_["lcb_retry_failure"].load();
  estats["filtered_dcp_delete_counter"] =
      execution_stats_["filtered_dcp_delete_counter"].load();
  estats["num_processed_events"] =
      execution_stats_["num_processed_events"].load();
  estats["processed_events_size"] =
      execution_stats_["processed_events_size"].load();
  estats["filtered_dcp_mutation_counter"] =
      execution_stats_["filtered_dcp_mutation_counter"].load();
  estats["dcp_mutation_checkpoint_cas_mismatch"] =
      execution_stats_["dcp_mutation_checkpoint_cas_mismatch"].load();
  estats["enqueued_dcp_mutation_msg_counter"] =
      execution_stats_["enqueued_dcp_mutation_msg_counter"].load();
  estats["enqueued_dcp_delete_msg_counter"] =
      execution_stats_["enqueued_dcp_delete_msg_counter"].load();
  estats["uv_try_write_failure_counter"] = uv_try_write_failure_counter.load();

  estats["curl"]["get"] = execution_stats_["curl.get"].load();
  estats["curl"]["post"] = execution_stats_["curl.post"].load();
  estats["curl"]["delete"] = execution_stats_["curl.delete"].load();
  estats["curl"]["head"] = execution_stats_["curl.head"].load();
  estats["curl"]["put"] = execution_stats_["curl.put"].load();
  estats["curl_success_count"] = execution_stats_["curl_success_count"].load();

  estats["timestamp"] = GetTimestampNow();
  return estats.dump();
}

std::string RuntimeStats::GetCodeInsight() {
  CodeInsight sum;
  sum.Accumulate(code_insight_);
  return sum.ToJSON();
}

std::string RuntimeStats::GetLatencyStats() {
  return latency_stats_.ToString();
}

std::string RuntimeStats::GetCurlLatencyStats() {
  return curl_latency_stats_.ToString();
}

std::string RuntimeStats::GetLcbExceptionStats() {
  lcbMutex.lock();
  nlohmann::json lcb_stats;
  for (const auto &it : lcb_exception_stats) {
    lcb_stats[std::to_string(it.first)] = it.second;
  }
  lcbMutex.unlock();
  return lcb_stats.dump();
}
