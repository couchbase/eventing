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
#include <memory>
#include <string>
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

  Iterator(const Query::Info &query_info, lcb_t instance, v8::Isolate *isolate,
           lcb_U32 timeout);

  Row Next();
  Row Peek();
  ::Info Start();
  void Stop();
  ::Info Wait();

private:
  static void RowCallback(lcb_t connection, int type, const lcb_RESPN1QL *resp);
  static bool IsStatusSuccess(const std::string &row);

  class BatonGuard {
  public:
    explicit BatonGuard(folly::fibers::Baton &baton) : baton_(baton) {}
    ~BatonGuard() { baton_.post(); }

    BatonGuard() = delete;
    BatonGuard(const BatonGuard &) = delete;
    BatonGuard(BatonGuard &&) = delete;
    BatonGuard &operator=(const BatonGuard &) = delete;
    BatonGuard &operator=(BatonGuard &&) = delete;

  private:
    folly::fibers::Baton &baton_;
  };

  struct Cursor {
    Query::Row GetRow() const;
    Query::Row GetRowAsFinal() const;

    lcb_error_t client_err_code{LCB_SUCCESS};
    bool is_error{false};
    bool is_client_auth_error{false};
    bool is_client_error{false}; // Error reported by SDK client
    bool is_query_error{false};  // Error reported by Query server
    bool is_last{false};
    bool is_streaming_started{false};
    std::string data;
    folly::fibers::Baton reader;
    folly::fibers::Baton writer;
    folly::fibers::Baton started_or_done;
  };

  enum class State { kIdle, kStarted, kStopped };

  // TODO : Use move
  const Query::Info query_info_;
  Cursor cursor_;
  bool has_peeked_{false};

  lcb_t connection_;
  ::Info result_info_{false};
  folly::EventBase event_base_;
  folly::fibers::FiberManager fiber_mgr_;
  v8::Isolate *isolate_;
  State state_{State::kIdle};
  Query::Builder builder_;
};
} // namespace Query

#endif
