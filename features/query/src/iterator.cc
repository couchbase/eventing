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

  if (auto info = query_controller_->build(&cursor_); info.is_fatal) {
    return {true, info.msg, false};
  }

  cursor_.YieldTo(Cursor::ExecutionControl::kSDK);
  std::thread runner([this]() -> void {
    RunnerGuard guard(cursor_);

    auto result = query_controller_->run();
    if (result != LCB_SUCCESS) {
      cursor_.is_last = true;
      auto helper = UnwrapData(isolate_)->query_helper;
      helper->AccountLCBError(static_cast<int>(result));
      auto is_retriable = IsRetriable(result);
      auto is_lcb_not_supported = (result == LCB_ERR_UNSUPPORTED_OPERATION);
      result_info_ = {true, lcb_strerror_short(result), is_retriable,
                      is_lcb_not_supported};
    }
  });
  runner_ = std::move(runner);

  cursor_.WaitFor(Cursor::ExecutionControl::kV8);
  has_peeked_ = true;
  state_ = State::kStarted;
  return result_info_;
}

void Query::Iterator::Stop() {
  if (state_ != State::kStarted) {
    return;
  }

  if (!cursor_.is_last) {
    // Subsequent RowCallback won't be invoked
    query_controller_->cancel();
    cursor_.YieldTo(Cursor::ExecutionControl::kSDK);
  }

  if (runner_.joinable()) {
    runner_.join();
  }
  state_ = State::kStopped;
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
