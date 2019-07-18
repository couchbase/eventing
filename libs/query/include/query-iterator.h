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

// Really MUST ensure that folly libs are #include-ed first
// Otherwise, it'll cause compilation errors
#include <folly/fibers/Baton.h>
#include <folly/fibers/EventBaseLoopController.h>
#include <folly/fibers/FiberManagerMap.h>
#include <folly/fibers/TimedMutex.h>
#include <folly/io/async/EventBase.h>

#include <libcouchbase/couchbase.h>
#include <libcouchbase/error.h>
#include <libcouchbase/n1ql.h>
#include <memory>
#include <string>
#include <v8.h>

#include "info.h"
#include "query-helper.h"

namespace Query {
struct Row {
  Row(bool is_done, bool is_error, bool is_auth_error, lcb_error_t err_code,
      const std::string &data)
      : is_done(is_done), is_error(is_error), is_auth_error(is_auth_error),
        err_code(err_code), data(data) {}

  bool is_done{false};
  bool is_error{false};
  bool is_auth_error{false};
  lcb_error_t err_code{LCB_SUCCESS};
  const std::string &data;
};

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

  Iterator(const Query::Info &query_info, lcb_t instance, v8::Isolate *isolate);

  Row Next();
  Row Peek();
  Iterator::Info Start();
  void Stop();
  ::Info Wait();
  lcb_error_t GetResultCode() const { return result_code_; }

private:
  static void RowCallback(lcb_t connection, int type, const lcb_RESPN1QL *resp);
  static bool IsStatusSuccess(const std::string &row);

  struct Cursor {
    Query::Row GetRow() const;
    Query::Row GetRowAsFinal() const;

    lcb_error_t err_code{LCB_SUCCESS};
    bool is_error{false};
    bool is_auth_error{false};
    bool is_last{false};
    bool is_streaming_started{false};
    std::string data;
    folly::fibers::Baton reader;
    folly::fibers::Baton writer;
  };

  enum class State { kIdle, kStarted, kStopped };

  // TODO : Use move
  const Query::Info query_info_;
  Cursor cursor_;
  bool has_peeked_{false};

  lcb_t connection_;
  lcb_N1QLHANDLE query_handle_{nullptr};
  lcb_error_t result_code_{LCB_SUCCESS};
  folly::EventBase event_base_;
  folly::fibers::FiberManager fiber_mgr_;
  v8::Isolate *isolate_;
  State state_{State::kIdle};
};
} // namespace Query

#endif
