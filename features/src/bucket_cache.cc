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

#include "bucket_cache.h"
#include "log.h"

BucketCache::Key BucketCache::MakeKey(const std::string &bucket,
                                      const std::string &scope,
                                      const std::string &coll,
                                      const std::string &id) {
  std::string key;
  auto sz = bucket.length() + scope.length() + coll.length() + id.length() + 3;
  key.reserve(sz);
  key.append(bucket);
  key.append(1, 0);
  key.append(scope);
  key.append(1, 0);
  key.append(coll);
  key.append(1, 0);
  key.append(id);
  return key;
}

BucketCache &BucketCache::Fetch() {
  static BucketCache cache(64 * 1024 * 1024, millis(1000));
  return cache;
}

BucketCache::BucketCache(size_t maxSize, millis maxAge)
    : mutex_(), size_(0), max_size_(maxSize), max_age_(maxAge),
      rnd_(std::random_device()()) {}

BucketCache::~BucketCache() {}

void BucketCache::SetMaxSize(size_t maxSize) {
  LOG(logInfo) << "Setting bucket cache max size to " << maxSize << std::endl;
  std::unique_lock lock(mutex_);
  max_size_ = maxSize;
  LockedTrim();
}

void BucketCache::SetMaxAge(millis maxAge) {
  LOG(logInfo) << "Setting bucket cache max age to " << maxAge.count()
               << " milliseconds" << std::endl;
  std::unique_lock lock(mutex_);
  max_age_ = maxAge;
  LockedTrim();
}

void BucketCache::Set(const BucketCache::Key &key, const Result &result) {
  std::unique_lock lock(mutex_);
  auto iter = cache_.find(key);
  if (iter != cache_.end()) {
    size_ += BucketCache::SizeOf(result);
    size_ -= BucketCache::SizeOf(iter->second.data);
    auto expiry = std::chrono::steady_clock::now() + max_age_;
    iter->second = {result, expiry};
    iter->second.data.key = key;
  } else {
    LockedTrim(); // trim first to avoid evicting new item
    size_ += BucketCache::SizeOf(result);
    auto expiry = std::chrono::steady_clock::now() + max_age_;
    Value entry = {result, expiry};
    entry.data.key = key;
    cache_[key] = entry;
  }
}

void BucketCache::Remove(const BucketCache::Key &key) {
  std::unique_lock lock(mutex_);
  auto iter = cache_.find(key);
  if (iter != cache_.end()) {
    size_ -= BucketCache::SizeOf(iter->second.data);
    cache_.erase(iter);
  }
}

bool BucketCache::Get(const BucketCache::Key &key, Result &result) {
  std::shared_lock lock(mutex_);
  auto iter = cache_.find(key);
  if (iter == cache_.end()) {
    return false;
  }
  auto now = std::chrono::steady_clock::now();
  if (iter->second.expiry < now) {
    return false;
  }
  result = iter->second.data;
  return true;
}

bool BucketCache::Contains(const BucketCache::Key &key) {
  std::shared_lock lock(mutex_);
  return cache_.find(key) != cache_.end();
}

void BucketCache::Change(const BucketCache::Key &key, const Result &result) {
  // most keys are not cached; so we lookup first as that needs only readlock
  if (Contains(key)) {
    Set(key, result);
  }
}

void BucketCache::Invalidate(const BucketCache::Key &key) {
  // most keys are not cached; so we lookup first as that needs only readlock
  if (Contains(key)) {
    Remove(key);
  }
}

// caller is expected to hold a write lock
void BucketCache::LockedTrim() {
  if (size_ <= max_size_) {
    return;
  }
  bucket_cache_overflow_count_++;
  auto now = std::chrono::steady_clock::now();
  auto iter = cache_.begin();
  while (iter != cache_.end()) {
    if (rnd_() % 3 == 0 || iter->second.expiry < now) {
      size_ -= BucketCache::SizeOf(iter->second.data);
      iter = cache_.erase(iter);
    } else {
      iter++;
    }
  }
}

size_t BucketCache::SizeOf(const Result &result) {
  auto sz = sizeof(Result) + result.key.length() + result.value.length();
  return sz;
}
