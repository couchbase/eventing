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

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

template <typename T> class Queue {
private:
  std::queue<T> data_queue;
  std::mutex mut;
  std::condition_variable data_cond;

public:
  Queue() = default;
  Queue(const Queue &) = delete;
  Queue &operator=(const Queue &) = delete;

  T pop() {
    std::unique_lock<std::mutex> lk(mut);
    while (data_queue.empty()) {
      data_cond.wait(lk);
    }
    auto value = data_queue.front();
    data_queue.pop();
    return value;
  }

  void pop(T &item) {
    std::unique_lock<std::mutex> lk(mut);
    while (data_queue.empty()) {
      data_cond.wait(lk);
    }
    item = data_queue.front();
    data_queue.pop();
  }

  void push(const T &item) {
    std::unique_lock<std::mutex> lk(mut);
    data_queue.push(item);
    lk.unlock();
    data_cond.notify_one();
  }
};

#endif
