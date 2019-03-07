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

#ifndef HISTOGRAM_H
#define HISTOGRAM_H

#include <atomic>
#include <string>

class Histogram {
public:
  void Add(int64_t sample);
  std::string ToString();

private:
  static const int64_t from_ = 100;
  static const int64_t till_ = 1000 * 1000 * 10;
  static const int64_t width_ = 100;
  static constexpr int64_t num_buckets_ = 1 + ((till_ - from_) / width_);
  std::atomic<int64_t> data_[num_buckets_]{};
};

#endif
