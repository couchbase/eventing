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

#ifndef COUCHBASE_TIMER_ITERATOR_H
#define COUCHBASE_TIMER_ITERATOR_H

#include "timer_defs.h"
#include <unordered_set>

namespace timer {
class TimerStore;

class Iterator {
public:
  Iterator(TimerStore *pstore);
  ~Iterator();
  bool GetNext(TimerEvent &tevent);

private:
  std::pair<bool, bool> GetNextTimer(TimerEvent &tevent);
  TimerStore *store_;
  std::unordered_set<int64_t>::iterator partn_iter_;
  int64_t stop_;
  std::string top_key_;
  lcb_CAS top_cas_{0};
  int64_t current_{std::numeric_limits<int64_t>::max()};
  int64_t end_{0};
  int64_t curr_seq_{std::numeric_limits<int64_t>::max()};
  int64_t end_seq_{0};
};
} // namespace timer
#endif // COUCHBASE_TIMER_ITERATOR_H
