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

#include "query-iterator.h"

#include <algorithm>
#include <libcouchbase/couchbase.h>
#include <libcouchbase/error.h>
#include <libcouchbase/n1ql.h>
#include <memory>
#include <regex>
#include <sstream>
#include <string>

#include "info.h"
#include "isolate_data.h"
#include "log.h"
#include "query-mgr.h"

Query::Iterator::Iterator(const Query::Info &query_info, lcb_t instance,
                          v8::Isolate *isolate, const lcb_U32 timeout)
    : query_info_(query_info), connection_(instance),
      fiber_mgr_(std::make_unique<folly::fibers::EventBaseLoopController>()),
      isolate_(isolate), builder_(query_info, instance, timeout) {
  dynamic_cast<folly::fibers::EventBaseLoopController &>(
      fiber_mgr_.loopController())
      .attachEventBase(event_base_);
}

Query::Row Query::Iterator::Next() {
  if (state_ != State::kStarted || cursor_.is_last) {
    return cursor_.GetRowAsFinal();
  }
  if (has_peeked_) {
    has_peeked_ = false;
    return cursor_.GetRow();
  }

  cursor_.writer.post();
  event_base_.loopOnce();
  cursor_.reader.wait();
  cursor_.reader.reset();
  return cursor_.GetRow();
}

::Info Query::Iterator::Start() {
  if (state_ != State::kIdle) {
    return {true, "Unable to start query as it is not in idle state"};
  }

  auto helper = UnwrapData(isolate_)->query_mgr;
  if (auto info = builder_.Build(RowCallback, &cursor_); info.is_fatal) {
    helper->RestoreConnection(connection_);
    return info;
  }

  fiber_mgr_.addTask([this]() {
    BatonGuard guard(cursor_.started_or_done);
    auto helper = UnwrapData(isolate_)->query_helper;

    auto result = lcb_n1ql_query(connection_, nullptr, builder_.GetCmd());
    if (result == LCB_SUCCESS) {
      result = lcb_wait(connection_);
    }
    if (result != LCB_SUCCESS) {
      helper->AccountLCBError(static_cast<int>(result));
      result_info_ = {true, lcb_strerror(connection_, result)};
    }

    auto query_mgr = UnwrapData(isolate_)->query_mgr;
    query_mgr->RestoreConnection(connection_);
  });

  event_base_.loopOnce();
  cursor_.started_or_done.wait();
  state_ = State::kStarted;
  return result_info_;
}

void Query::Iterator::RowCallback(lcb_t connection, int,
                                  const lcb_RESPN1QL *resp) {
  auto cursor =
      static_cast<Cursor *>(const_cast<void *>(lcb_get_cookie(connection)));
  if (!cursor->is_streaming_started) {
    cursor->started_or_done.post();
    cursor->is_streaming_started = true;
  }

  cursor->writer.wait();
  cursor->writer.reset();
  cursor->data.assign(resp->row, resp->nrow);
  cursor->is_last = (resp->rflags & LCB_RESP_F_FINAL) != 0;
  cursor->client_err_code = resp->rc;
  cursor->is_client_error = cursor->is_last && resp->rc != LCB_SUCCESS;
  cursor->is_query_error = cursor->is_last && !IsStatusSuccess(cursor->data);
  cursor->is_error = cursor->is_client_error || cursor->is_query_error;
  cursor->is_client_auth_error =
      cursor->is_error && (resp->rc == LCB_AUTH_ERROR);

  // Ensure that the most specific error is delivered in the case where both
  // client and query errors occur. That is, we expose the client error only
  // when there is no query error
  if (cursor->is_client_error && !cursor->is_query_error) {
    cursor->data = lcb_strerror(connection, resp->rc);
  }

  LOG(logDebug) << "Query::Iterator::RowCallback data : " << RU(cursor->data)
                << " is_last : " << cursor->is_last
                << " is_error : " << cursor->is_error
                << " is_client_auth_error : " << cursor->is_client_auth_error
                << " resp rflags : " << resp->rflags
                << " resp rc : " << resp->rc << " is_last : " << std::endl;

  cursor->reader.post();
}

void Query::Iterator::Stop() {
  if (state_ != State::kStarted) {
    return;
  }

  if (!cursor_.is_last && cursor_.is_streaming_started) {
    // Subsequent RowCallback won't be invoked
    lcb_n1ql_cancel(connection_, builder_.GetHandle());
  }
  cursor_.writer.post();

  // Consume all events
  event_base_.loop();
  state_ = State::kStopped;
}

bool Query::Iterator::IsStatusSuccess(const std::string &row) {
  std::regex re_status_success(R"("status"\s*:\s*"success")");
  return std::regex_search(row, re_status_success);
}

::Info Query::Iterator::Wait() {
  event_base_.loop();
  return result_info_;
}

Query::Row Query::Iterator::Peek() {
  if (has_peeked_) {
    return cursor_.GetRow();
  }
  auto row = Next();
  has_peeked_ = true;
  return row;
}

Query::Row Query::Iterator::Cursor::GetRow() const {
  return {is_last,
          is_error,
          is_client_auth_error,
          is_client_error,
          is_query_error,
          client_err_code,
          data};
}

Query::Row Query::Iterator::Cursor::GetRowAsFinal() const {
  auto row = GetRow();
  row.is_done = true;
  return row;
}
