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

#include <libcouchbase/couchbase.h>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <v8.h>

#include "lcb_utils.h"
#include "timer_defs.h"
#include "timer_iterator.h"
#include "utils.h"

namespace timer {
class TimerStore {
public:
  explicit TimerStore(v8::Isolate *isolate, const std::string &prefix,
                      const std::vector<int64_t> &partitions,
                      const std::string &metadata_bucket,
                      const std::string &metadata_scope,
                      const std::string &metadata_collection,
                      int32_t num_vbuckets);
  ~TimerStore();

  lcb_STATUS SetTimer(TimerInfo &timer, int max_retry_count,
                      uint32_t max_retry_secs);
  lcb_STATUS DelTimer(TimerInfo &timer, int max_retry_count,
                      uint32_t max_retry_secs);

  void DeleteTimer(TimerEvent &event);

  Iterator GetIterator();

  void AddPartition(int64_t partition);

  void RemovePartition(int64_t partition);

  void SyncSpan();

  lcb_INSTANCE *GetTimerStoreHandle() const;

private:
  void Connect();
  std::pair<bool, lcb_STATUS> InitSpan(int partition);
  std::pair<bool, lcb_STATUS> InitSpanLocked(int partition);

  std::pair<bool, lcb_STATUS> SyncSpan(int partition);
  std::pair<bool, lcb_STATUS> SyncSpanLocked(int partition);
  lcb_STATUS MayBeMoveSpanBack(int partition, int64_t due, int max_retry_count,
                               uint32_t max_retry_secs);

  bool ExpandSpan(int64_t partition, int64_t point);

  void ShrinkSpan(int64_t partition, int64_t start);

  std::pair<lcb_STATUS, Result> GetCounter(const std::string &key,
                                           int max_retry_count,
                                           uint32_t max_retry_secs);

  std::pair<lcb_STATUS, Result> Insert(const std::string &key,
                                       const nlohmann::json &value,
                                       int max_retry_count,
                                       uint32_t max_retry_secs);

  std::pair<lcb_STATUS, Result> Upsert(const std::string &key,
                                       const nlohmann::json &value,
                                       int max_retry_count,
                                       uint32_t max_retry_secs);

  std::pair<lcb_STATUS, Result> Replace(const std::string &key,
                                        const nlohmann::json &value,
                                        uint64_t cas, int max_retry_count,
                                        uint32_t max_retry_secs);

  lcb_STATUS Delete(const std::string &key, uint64_t cas, int max_retry_count,
                    uint32_t max_retry_secs);

  std::pair<lcb_STATUS, Result> Get(const std::string &key, int max_retry_count,
                                    uint32_t max_retry_secs);

  std::pair<lcb_STATUS, Result> Lock(const std::string &key,
                                     int max_retry_count,
                                     uint32_t max_retry_secs,
                                     uint32_t max_lock_time);
  std::pair<lcb_STATUS, Result> Unlock(const std::string &key,
                                       int max_retry_count,
                                       uint32_t max_retry_secs, uint64_t cas);
  v8::Isolate *isolate_;
  std::vector<bool> partitons_;
  std::unordered_map<int64_t, TimerSpan> span_map_;
  std::string prefix_;
  std::string metadata_bucket_;
  std::string metadata_scope_;
  std::string metadata_collection_;
  size_t metadata_collection_length_, metadata_scope_length_;
  lcb_INSTANCE *crud_handle_{nullptr};
  std::mutex store_lock_;
  int32_t num_vbuckets_{1024};
  friend class Iterator;
};
} // namespace timer
#endif // COUCHBASE_TIMER_STORE_H
