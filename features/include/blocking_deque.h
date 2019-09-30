/// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

#ifndef COUCHBASE_BLOCKING_DEQUE_H
#define COUCHBASE_BLOCKING_DEQUE_H

#include <condition_variable>
#include <deque>
#include <mutex>

template <typename T> class BlockingDeque {
public:
  BlockingDeque() = default;
  BlockingDeque(const BlockingDeque &) = delete;
  BlockingDeque &operator=(const BlockingDeque &) = delete;

  void PushFront(T elem);

  bool PopFront(T &elem);

  void PushBack(T elem);

  bool PopBack(T &elem);

  size_t GetMemory();

  size_t GetSize();

  void Clear();

  void Close();

private:
  std::deque<T> elems_;
  std::mutex lock_;
  std::condition_variable cond_;
  bool closed_{false};
  size_t mem_size_{0};
};

template <typename T> void BlockingDeque<T>::PushFront(T elem) {
  std::unique_lock<std::mutex> lck(lock_);
  mem_size_ += elem->GetSize();
  elems_.push_front(std::move(elem));
  lck.unlock();
  cond_.notify_one();
}

template <typename T> bool BlockingDeque<T>::PopFront(T &elem) {
  std::unique_lock<std::mutex> lck(lock_);
  cond_.wait(lck, [this] { return !elems_.empty() || closed_; });
  if (elems_.empty())
    return false;
  elem = std::move(elems_.front());
  mem_size_ -= elem->GetSize();
  elems_.pop_front();
  return true;
}

template <typename T> void BlockingDeque<T>::PushBack(T elem) {
  std::unique_lock<std::mutex> lck(lock_);
  mem_size_ += elem->GetSize();
  elems_.push_back(std::move(elem));
  lck.unlock();
  cond_.notify_one();
}

template <typename T> bool BlockingDeque<T>::PopBack(T &elem) {
  std::unique_lock<std::mutex> lck(lock_);
  cond_.wait(lck, [this] { return !elems_.empty() || closed_; });
  if (elems_.empty())
    return false;
  elem = std::move(elems_.back());
  mem_size_ -= elem->GetSize();
  elems_.pop_back();
  return true;
}

template <typename T> size_t BlockingDeque<T>::GetMemory() {
  std::lock_guard<std::mutex> lck(lock_);
  return mem_size_;
}

template <typename T> size_t BlockingDeque<T>::GetSize() {
  std::lock_guard<std::mutex> lck(lock_);
  return elems_.size();
}

template <typename T> void BlockingDeque<T>::Clear() {
  std::lock_guard<std::mutex> lck(lock_);
  elems_.clear();
}

template <typename T> void BlockingDeque<T>::Close() {
  std::lock_guard<std::mutex> lck(lock_);
  closed_ = true;
  cond_.notify_all();
}

#endif // COUCHBASE_BLOCKING_DEQUE_H