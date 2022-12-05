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

#include <libcouchbase/couchbase.h>
#include <libcouchbase/n1ql.h>
#include <memory>
#include <mutex>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include "info.h"
#include "isolate_data.h"
#include "lcb_utils.h"
#include "log.h"
#include "query-iterator.h"
#include "query-mgr.h"

Query::Iterator::~Iterator() {
  if (runner_.joinable()) {
    runner_.join();
  }
}

Query::Row Query::Iterator::Next() {
  if (state_ != State::kStarted || cursor_.is_last) {
    return cursor_.GetRowAsFinal();
  }
  if (has_peeked_) {
    has_peeked_ = false;
    return cursor_.GetRow();
  }

  cursor_.YieldTo(Cursor::ExecutionControl::kSDK);
  cursor_.WaitFor(Cursor::ExecutionControl::kV8);
  return cursor_.GetRow();
}

::Info Query::Iterator::Start() {
  if (state_ != State::kIdle) {
    return {true, "Unable to start query as it is not in idle state", false};
  }

  auto helper = UnwrapData(isolate_)->query_mgr;
  if (auto info = builder_.Build(RowCallback, &cursor_); info.is_fatal) {
    helper->RestoreConnection(connection_);
    return {true, info.msg, false};
  }

  cursor_.YieldTo(Cursor::ExecutionControl::kSDK);
  std::thread runner([this]() -> void {
    RunnerGuard guard(cursor_);

    auto result = lcb_n1ql_query(connection_, nullptr, builder_.GetCmd());
    if (result == LCB_SUCCESS) {
      result = lcb_wait(connection_);
    }

    if (result != LCB_SUCCESS) {
      cursor_.is_last = true;
      auto helper = UnwrapData(isolate_)->query_helper;
      helper->AccountLCBError(static_cast<int>(result));
      auto is_retriable = IsRetriable(result);
      auto is_lcb_not_supported = (result == LCB_NOT_SUPPORTED);
      result_info_ = {true, lcb_strerror(connection_, result), is_retriable,
                      is_lcb_not_supported};
    }

    auto query_mgr = UnwrapData(isolate_)->query_mgr;
    query_mgr->RestoreConnection(connection_);
  });
  runner_ = std::move(runner);

  cursor_.WaitFor(Cursor::ExecutionControl::kV8);
  has_peeked_ = true;
  state_ = State::kStarted;
  return result_info_;
}

void Query::Iterator::RowCallback(lcb_t connection, int,
                                  const lcb_RESPN1QL *resp) {
  auto cursor =
      static_cast<Cursor *>(const_cast<void *>(lcb_get_cookie(connection)));

  cursor->data.assign(resp->row, resp->nrow);
  cursor->is_last = (resp->rflags & LCB_RESP_F_FINAL) != 0;
  cursor->client_err_code = resp->rc;
  cursor->is_client_error = cursor->is_last && resp->rc != LCB_SUCCESS;
  cursor->is_query_error = cursor->is_last && !IsStatusSuccess(cursor->data);
  cursor->is_error = cursor->is_client_error || cursor->is_query_error;
  cursor->is_client_auth_error =
      cursor->is_error && (resp->rc == LCB_AUTH_ERROR ||
                          resp->rc == LCB_SSL_CANTVERIFY);

  if (cursor->is_client_error) {
    cursor->client_error = lcb_strerror(connection, resp->rc);
  }
  if (cursor->is_query_error) {
    cursor->query_error = cursor->data;
  }

  LOG(logDebug) << "Query::Iterator::RowCallback data : " << RU(cursor->data)
                << " is_last : " << cursor->is_last
                << " is_error : " << cursor->is_error
                << " is_client_auth_error : " << cursor->is_client_auth_error
                << " resp rflags : " << resp->rflags
                << " resp rc : " << resp->rc << std::endl;

  cursor->YieldTo(Cursor::ExecutionControl::kV8);
  if (!cursor->is_last) {
    cursor->WaitFor(Cursor::ExecutionControl::kSDK);
  }
}

void Query::Iterator::Stop() {
  if (state_ != State::kStarted) {
    return;
  }

  if (!cursor_.is_last) {
    // Subsequent RowCallback won't be invoked
    lcb_n1ql_cancel(connection_, builder_.GetHandle());
    cursor_.YieldTo(Cursor::ExecutionControl::kSDK);
  }

  if (runner_.joinable()) {
    runner_.join();
  }
  state_ = State::kStopped;
}

bool Query::Iterator::IsStatusSuccess(const std::string &row) {
  std::regex re_status_success(R"("status"\s*:\s*"success")");
  return std::regex_search(row, re_status_success);
}

::Info Query::Iterator::Wait() {
  if (runner_.joinable()) {
    runner_.join();
  }
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

void Query::Iterator::Cursor::WaitFor(ExecutionControl control) {
  std::unique_lock<std::mutex> lock(control_sync_);
  control_signal_.wait(
      lock, [this, &control]() -> bool { return control_ == control; });
}

void Query::Iterator::Cursor::YieldTo(const ExecutionControl control) {
  std::lock_guard<std::mutex> lock(control_sync_);
  control_ = control;
  control_signal_.notify_one();
}

Query::Row Query::Iterator::Cursor::GetRow() const {
  return {is_last,         is_error,        is_client_auth_error,
          is_client_error, client_error,    is_query_error,
          query_error,     client_err_code, data};
}

Query::Row Query::Iterator::Cursor::GetRowAsFinal() const {
  auto row = GetRow();
  row.is_done = true;
  return row;
}