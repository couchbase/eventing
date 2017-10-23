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
  Histogram(int64_t f, int64_t t, int64_t w) : from(f), till(t), width(w) {
    buckets = 1 + ((till - from) / width);
    hgram.assign(buckets, 0);

    init = false;
    sum = 0;
    samples = 0;
    min_val = 0;
    max_val = 0;
  }

  ~Histogram() { hgram.clear(); };

  void Add(int64_t sample);
  int64_t Mean();

  int64_t Buckets() { return buckets; };
  std::vector<int64_t> Hgram() { return hgram; };
  int64_t Min() { return min_val; };
  int64_t Max() { return max_val; };
  int64_t Samples() { return samples; };
  int64_t Sum() { return sum; };

private:
  int64_t min_val;
  int64_t max_val;
  std::vector<int64_t> hgram;

  int64_t buckets;
  int64_t from;
  int64_t sum;
  int64_t till;
  int64_t width;
  int64_t samples;

  bool init;
};

inline void Histogram::Add(int64_t sample) {
  samples++;
  sum += sample;

  if ((init == false) || (sample < min_val)) {
    min_val = sample;
    init = true;
  }

  if (max_val < sample) {
    max_val = sample;
  }

  if (sample < from) {
    hgram[0]++;
  } else if (sample >= till) {
    hgram[hgram.size() - 1]++;
  } else {
    size_t index = ((sample - from) / width) + 1;
    if ((index > 0) && (index < (hgram.size() - 1))) {
      hgram[index]++;
    }
  }
}

inline int64_t Histogram::Mean() {
  if (samples == 0) {
    return 0;
  }

  return sum / samples;
}

#endif
