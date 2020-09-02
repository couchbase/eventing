// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

#ifndef COUCHBASE_TIMER_STORE_H
#define COUCHBASE_TIMER_STORE_H

#include <libcouchbase/api3.h>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <v8.h>

#include "lcb_utils.h"
#include "timer_defs.h"
#include "timer_iterator.h"
#include "utils.h"
#include "v8worker.h"

namespace timer {
class TimerStore {
public:
  explicit TimerStore(v8::Isolate *isolate, const std::string &prefix,
                      const std::string &metadata_bucket, int32_t num_vbuckets,
                      int32_t timer_reduction_ratio);
  ~TimerStore();

  lcb_error_t SetTimer(TimerInfo &timer, int max_retry_count,
                       uint32_t max_retry_secs);
  lcb_error_t DelTimer(TimerInfo &timer, int max_retry_count,
                       uint32_t max_retry_secs);

  void DeleteTimer(TimerEvent &event);

  Iterator GetIterator();

  void AddPartition(int64_t partition);

  void RemovePartition(int64_t partition);

  void SyncSpan();

  lcb_t GetTimerStoreHandle() const;

private:
  void Connect();
  std::pair<bool, lcb_error_t> InitSpan(int partition);
  std::pair<bool, lcb_error_t> InitSpanLocked(int partition);

  std::pair<bool, lcb_error_t> SyncSpan(int partition);
  std::pair<bool, lcb_error_t> SyncSpanLocked(int partition);
  lcb_error_t MayBeMoveSpanBack(int partition, int64_t due, int max_retry_count,
                                uint32_t max_retry_secs);

  bool ExpandSpan(int64_t partition, int64_t point);

  void ShrinkSpan(int64_t partition, int64_t start);

  bool IsValidTimerPartition(int vb);
  std::pair<lcb_error_t, Result> GetCounter(const std::string &key,
                                            int max_retry_count,
                                            uint32_t max_retry_secs);

  std::pair<lcb_error_t, Result> Insert(const std::string &key,
                                        const nlohmann::json &value,
                                        int max_retry_count,
                                        uint32_t max_retry_secs);

  std::pair<lcb_error_t, Result> Upsert(const std::string &key,
                                        const nlohmann::json &value,
                                        int max_retry_count,
                                        uint32_t max_retry_secs);

  std::pair<lcb_error_t, Result> Replace(const std::string &key,
                                         const nlohmann::json &value,
                                         lcb_CAS cas, int max_retry_count,
                                         uint32_t max_retry_secs);

  lcb_error_t Delete(const std::string &key, uint64_t cas, int max_retry_count,
                     uint32_t max_retry_secs);

  std::pair<lcb_error_t, Result>
  Get(const std::string &key, int max_retry_count, uint32_t max_retry_secs);

  std::pair<lcb_error_t, Result> Lock(const std::string &key,
                                      int max_retry_count,
                                      uint32_t max_retry_secs,
                                      uint32_t max_lock_time);
  std::pair<lcb_error_t, Result> Unlock(const std::string &key,
                                        int max_retry_count,
                                        uint32_t max_retry_secs, lcb_CAS cas);
  v8::Isolate *isolate_;
  std::vector<bool> partitons_;
  std::unordered_map<int64_t, TimerSpan> span_map_;
  std::string prefix_;
  std::string metadata_bucket_;
  lcb_t crud_handle_{nullptr};
  std::mutex store_lock_;
  int32_t num_vbuckets_{1024};
  int32_t timer_reduction_ratio_{1};
  friend class Iterator;
};
} // namespace timer
#endif // COUCHBASE_TIMER_STORE_H
