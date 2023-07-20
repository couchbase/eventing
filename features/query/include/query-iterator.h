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

#ifndef QUERY_ITERATOR_H
#define QUERY_ITERATOR_H

#include <condition_variable>
#include <libcouchbase/couchbase.h>
#include <string>
#include <thread>
#include <utility>
#include <v8.h>

#include "info.h"
#include "query-helper.h"
#include "query-row.h"
#include "query-controllers.h"

namespace Query {
class Iterator;
class Manager;

class Iterator {
  friend N1qlController;
  friend QueryController;

public:
  struct Info : public ::Info {
    Info(bool is_fatal) : ::Info(is_fatal) {}
    Info(bool is_fatal, const std::string &msg) : ::Info(is_fatal, msg) {}
    Info(bool is_fatal, const std::string &msg, bool is_retriable)
        : ::Info(is_fatal, msg, is_retriable) {}
    Info(bool is_fatal, const std::string &msg, bool is_retriable,
         bool is_lcb_special_error)
        : ::Info(is_fatal, msg, is_retriable, is_lcb_special_error) {}

    Info(Iterator *iterator) : ::Info(false), iterator(iterator) {}

    Iterator *iterator{nullptr};
  };

  Iterator(std::unique_ptr<QueryController> query, v8::Isolate *isolate)
      : isolate_(isolate), query_controller_(std::move(query)) {}
  ~Iterator();

  Iterator() = delete;
  Iterator(const Iterator &) = delete;
  Iterator(Iterator &&) = delete;
  Iterator &operator=(const Iterator &) = delete;
  Iterator &operator=(Iterator &&) = delete;

  Query::Row Next();
  Query::Row Peek();
  ::Info Start();
  void Stop();
  ::Info Wait();

  inline void ThrowQueryError(const std::string &err_msg) {
    query_controller_->ThrowQueryError(err_msg);
  }

private:
  struct Cursor {
    Query::Row GetRow() const;
    Query::Row GetRowAsFinal() const;

    enum class ExecutionControl { kV8, kSDK };

    void WaitFor(ExecutionControl control);
    void YieldTo(ExecutionControl control);

    lcb_STATUS client_err_code{LCB_SUCCESS};
    bool is_error{false};
    bool is_client_auth_error{false};
    bool is_client_error{false}; // Error reported by SDK client
    std::string client_error;
    bool is_query_error{false}; // Error reported by Query server
    std::string query_error;
    bool is_last{false};
    std::string data;

  private:
    ExecutionControl control_{ExecutionControl::kV8};
    std::mutex control_sync_;
    std::condition_variable control_signal_;
  };

  class RunnerGuard {
  public:
    explicit RunnerGuard(Cursor &cursor) : cursor_(cursor) {}
    RunnerGuard() = delete;
    RunnerGuard(const RunnerGuard &) = delete;
    RunnerGuard(RunnerGuard &&) = delete;
    RunnerGuard &operator=(const RunnerGuard &) = delete;
    RunnerGuard &operator=(RunnerGuard &&) = delete;
    ~RunnerGuard() { cursor_.YieldTo(Cursor::ExecutionControl::kV8); };

  private:
    Cursor &cursor_;
  };

  enum class State { kIdle, kStarted, kStopped };

  Cursor cursor_;
  bool has_peeked_{false};

  ::Info result_info_{false};
  v8::Isolate *isolate_;
  State state_{State::kIdle};
  std::unique_ptr<QueryController> query_controller_;
  std::thread runner_;
};
} // namespace Query

#endif
