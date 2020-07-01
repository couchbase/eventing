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

#ifndef COUCHBASE_TIMER_DEFS_H
#define COUCHBASE_TIMER_DEFS_H

#include <libcouchbase/api3.h>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_set>
#include <v8.h>

namespace timer {
static constexpr int resolution = 7;
static constexpr int init_seq = 128;
static constexpr int tail_time = 60;
static constexpr int encode_base = 10;
static const char *const dict =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789*&";

struct EpochInfo {
  EpochInfo(bool is_valid) : is_valid(is_valid), epoch(0) {}

  EpochInfo(bool is_valid, int64_t epoch) : is_valid(is_valid), epoch(epoch) {}

  bool is_valid;
  int64_t epoch;
};

struct TimerInfo {
  TimerInfo() : epoch(0), vb(0), seq_num(0) {}

  int64_t epoch;
  uint64_t vb;
  uint64_t seq_num;
  std::string callback;
  std::string reference;
  std::string context;
};

struct AlarmRecord {
  int64_t alarm_due;
  std::string context_ref;
};

struct ContextRecord {
  nlohmann::json context;
  std::string alarm_ref;
};

struct TimerEvent {
  std::string callback;
  std::string context;
  std::string alarm_key;
  std::string context_key;
  uint64_t alarm_cas;
  uint64_t context_cas;
};

struct TimerSpan {
  int64_t start;
  int64_t stop;
  bool empty{true};
  bool dirty{false};
  uint64_t cas{0};
};

int64_t RoundUp(int64_t val);

int64_t RoundDown(int64_t val);

std::string BuildRootKey(const std::string &prefix, int64_t partition,
                         int64_t due);

std::string BuildAlarmKey(const std::string &prefix, int64_t partition,
                          int64_t due, int64_t seq);

std::string BuildContextKey(const std::string &prefix, int64_t partition,
                            const std::string &ref);

std::string BuildSpanKey(const std::string &prefix, int64_t partition);

} // namespace timer
#endif // COUCHBASE_TIMER_DEFS_H
