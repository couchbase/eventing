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
    std::unique_lock<std::mutex> lk(mut);
    while (data_queue.empty()) {
      data_cond.wait(lk);
    }
    auto value = data_queue.front();
    queue_size -= value.GetSize();
    data_queue.pop();
    entry_count--;
    return value;
  }

  void Push(const T &item) {
    std::unique_lock<std::mutex> lk(mut);
    data_queue.push(item);
    queue_size += item.GetSize();
    entry_count++;
    lk.unlock();
    data_cond.notify_one();
  }

  int64_t Count() { return entry_count; }
  std::size_t Size() { return queue_size; }

private:
  std::queue<T> data_queue;
  std::mutex mut;
  std::condition_variable data_cond;
  std::atomic<std::int64_t> entry_count = {0};
  std::atomic<std::size_t> queue_size = {0};
};

#endif
