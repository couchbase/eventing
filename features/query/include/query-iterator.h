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
#include <libcouchbase/n1ql.h>
#include <string>
#include <thread>
#include <utility>
#include <v8.h>

#include "info.h"
#include "query-builder.h"
#include "query-helper.h"
#include "query-row.h"

namespace Query {
class Iterator;
class Manager;

class Iterator {
public:
  struct Info : public ::Info {
    Info(bool is_fatal) : ::Info(is_fatal) {}
    Info(bool is_fatal, const std::string &msg) : ::Info(is_fatal, msg) {}
    Info(Iterator *iterator) : ::Info(false), iterator(iterator) {}

    Iterator *iterator{nullptr};
  };

  Iterator(Query::Info query_info, lcb_t instance, v8::Isolate *isolate)
      : connection_(instance), isolate_(isolate),
        builder_(isolate_, std::move(query_info), instance) {}
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

private:
  static void RowCallback(lcb_t connection, int type, const lcb_RESPN1QL *resp);
  static bool IsStatusSuccess(const std::string &row);

  struct Cursor {
    Query::Row GetRow() const;
    Query::Row GetRowAsFinal() const;

    enum class ExecutionControl { kV8, kSDK };

    void WaitFor(ExecutionControl control);
    void YieldTo(ExecutionControl control);

    lcb_error_t client_err_code{LCB_SUCCESS};
    bool is_error{false};
    bool is_client_auth_error{false};
    bool is_client_error{false}; // Error reported by SDK client
    bool is_query_error{false};  // Error reported by Query server
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

  lcb_t connection_;
  ::Info result_info_{false};
  v8::Isolate *isolate_;
  State state_{State::kIdle};
  Query::Builder builder_;
  std::thread runner_;
};
} // namespace Query

#endif
