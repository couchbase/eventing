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

#include <iostream>
#include <vector>

class Histogram {
public:
  Histogram(int64_t f, int64_t t, int64_t w) : from_(f), till_(t), width_(w) {
    buckets_ = 1 + ((till_ - from_) / width_);
    hgram_.assign(buckets_, 0);

    init_ = false;
    sum_ = 0;
    samples_ = 0;
    min_val_ = 0;
    max_val_ = 0;
  }

  ~Histogram() { hgram_.clear(); };

  void Add(int64_t sample);
  int64_t Mean();

  int64_t Buckets() { return buckets_; };
  std::vector<int64_t> Hgram() { return hgram_; };
  int64_t Min() { return min_val_; };
  int64_t Max() { return max_val_; };
  int64_t Samples() { return samples_; };
  int64_t Sum() { return sum_; };

private:
  int64_t min_val_;
  int64_t max_val_;
  std::vector<int64_t> hgram_;

  int64_t buckets_;
  int64_t from_;
  int64_t sum_;
  int64_t till_;
  int64_t width_;
  int64_t samples_;

  bool init_;
};

inline void Histogram::Add(int64_t sample) {
  samples_++;
  sum_ += sample;

  if ((init_ == false) || (sample < min_val_)) {
    min_val_ = sample;
    init_ = true;
  }

  if (max_val_ < sample) {
    max_val_ = sample;
  }

  if (sample < from_) {
    hgram_[0]++;
  } else if (sample >= till_) {
    hgram_[hgram_.size() - 1]++;
  } else {
    size_t index = ((sample - from_) / width_) + 1;
    if ((index > 0) && (index < (hgram_.size() - 1))) {
      hgram_[index]++;
    }
  }
}

inline int64_t Histogram::Mean() {
  if (samples_ == 0) {
    return 0;
  }

  return sum_ / samples_;
}

#endif
