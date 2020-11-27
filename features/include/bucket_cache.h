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

#ifndef BUCKETCACHE_H
#define BUCKETCACHE_H

#include <map>
#include <random>
#include <shared_mutex>

#include "lcb_utils.h"

class BucketCache {
  using millis = std::chrono::milliseconds;

public:
  static BucketCache &Fetch();

  typedef std::string Key;
  static Key MakeKey(const std::string &bucket, const std::string &scope,
                     const std::string &collection, const std::string &id);

  bool Get(const Key &key, Result &value);
  void Set(const Key &key, const Result &value);

  void Change(const Key &key, const Result &value);
  void Invalidate(const BucketCache::Key &key);

  void SetMaxSize(size_t maxSize);
  void SetMaxAge(millis maxAge);

private:
  struct Value {
    Result data;
    std::chrono::steady_clock::time_point expiry;
  };

  BucketCache(size_t maxSize, millis maxAge);
  ~BucketCache();

  bool Contains(const Key &key);
  void Remove(const Key &key);
  void LockedTrim();

  static size_t SizeOf(const Result &);
  std::unordered_map<Key, Value> cache_;

  std::shared_mutex mutex_;
  size_t size_;
  size_t max_size_;
  millis max_age_;
  std::mt19937 rnd_;

  BucketCache(const BucketCache &) = delete;
  BucketCache &operator=(const BucketCache &) = delete;
};

#endif
