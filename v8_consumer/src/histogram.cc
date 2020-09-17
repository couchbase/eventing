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

#include <sstream>

#include "histogram.h"

void Histogram::Add(int64_t sample) {
  if (sample <= from_) {
    ++data_[0];
    return;
  }

  if (sample >= till_) {
    ++data_[num_buckets_ - 1];
    return;
  }

  std::size_t index = ((sample - from_) / width_) + 1;
  if ((index > 0) && (index < (num_buckets_ - 1))) {
    ++data_[index];
  }
}

std::string Histogram::ToString() {
  std::ostringstream out;
  auto separator = "";

  out << "{";
  for (std::size_t i = 0; i < num_buckets_; ++i) {
    auto data = data_[i].exchange(0);
    if (data == 0) {
      continue;
    }
    out << separator << R"(")" << (i == 0 ? from_ : i * width_) << R"(":)"
        << data;
    separator = ",";
  }
  out << "}";

  return out.str();
}
