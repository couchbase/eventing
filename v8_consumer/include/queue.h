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

#ifndef QUEUE_H
#define QUEUE_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

template <typename T> class Queue {
public:
  Queue() = default;
  Queue(const Queue &) = delete;
  Queue &operator=(const Queue &) = delete;

  T Pop() {
    std::unique_lock<std::mutex> lk(mut_);
    while (data_queue_.empty()) {
      data_cond_.wait(lk);
    }
    auto value = data_queue_.front();
    queue_size_ -= value.GetSize();
    data_queue_.pop();
    entry_count_--;
    return value;
  }

  void Push(const T &item) {
    std::unique_lock<std::mutex> lk(mut_);
    data_queue_.push(item);
    queue_size_ += item.GetSize();
    entry_count_++;
    lk.unlock();
    data_cond_.notify_one();
  }

  int64_t Count() { return entry_count_; }
  std::size_t Size() { return queue_size_; }

private:
  std::queue<T> data_queue_;
  std::mutex mut_;
  std::condition_variable data_cond_;
  std::atomic<std::int64_t> entry_count_ = {0};
  std::atomic<std::size_t> queue_size_ = {0};
};

#endif
