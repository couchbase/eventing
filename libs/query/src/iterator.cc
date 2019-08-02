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
                          v8::Isolate *isolate)
    : query_info_(query_info), connection_(instance),
      fiber_mgr_(std::make_unique<folly::fibers::EventBaseLoopController>()),
      isolate_(isolate) {
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

Query::Iterator::Info Query::Iterator::Start() {
  if (state_ != State::kIdle) {
    return {true, "Unable to start query as it is not in idle state"};
  }

  lcb_CMDN1QL cmd = {0};
  lcb_N1QLPARAMS *builder = lcb_n1p_new();
  std::stringstream error;
  auto query_mgr = UnwrapData(isolate_)->query_mgr;

  auto result = lcb_n1p_setstmtz(builder, query_info_.query.c_str());
  if (result != LCB_SUCCESS) {
    error << "Unable to set query : " << lcb_strerror(connection_, result)
          << std::endl;
    query_mgr->RestoreConnection(connection_);
    return {true, error.str()};
  }

  for (const auto &[key, value] : query_info_.named_params) {
    result = lcb_n1p_namedparamz(builder, key.c_str(), value.c_str());
    if (result != LCB_SUCCESS) {
      error << "Unable to set named parameter : " << key << " : "
            << lcb_strerror(connection_, result) << std::endl;
      query_mgr->RestoreConnection(connection_);
      return {true, error.str()};
    }
  }

  for (const auto &param : query_info_.pos_params) {
    result = lcb_n1p_posparam(builder, param.c_str(), param.size());
    if (result != LCB_SUCCESS) {
      error << "Unable to set parameter " << param << " : "
            << lcb_strerror(connection_, result) << std::endl;
      query_mgr->RestoreConnection(connection_);
      return {true, error.str()};
    }
  }

  result = lcb_n1p_mkcmd(builder, &cmd);
  if (result != LCB_SUCCESS) {
    error << "Unable to make query cmd : " << lcb_strerror(connection_, result)
          << std::endl;
    query_mgr->RestoreConnection(connection_);
    return {true, error.str()};
  }

  cmd.handle = &query_handle_;
  cmd.callback = RowCallback;
  // Commenting this out until MB-34822 is resolved
  //  cmd.cmdflags |= LCB_CMDN1QL_F_PREPCACHE;
  lcb_set_cookie(connection_, &cursor_);

  auto timeout = UnwrapData(isolate_)->n1ql_timeout;
  result = lcb_cntl(connection_, LCB_CNTL_SET, LCB_CNTL_N1QL_TIMEOUT, &timeout);
  if (result != LCB_SUCCESS) {
    error << "Unable to set timeout for query : "
          << lcb_strerror(connection_, result) << std::endl;
    query_mgr->RestoreConnection(connection_);
    return {true, error.str()};
  }

  result = lcb_n1ql_query(connection_, nullptr, &cmd);
  if (result != LCB_SUCCESS) {
    error << "Unable to schedule query : " << lcb_strerror(connection_, result)
          << std::endl;
    query_mgr->RestoreConnection(connection_);
    return {true, error.str()};
  }

  lcb_n1p_free(builder);

  // TODO : Use promise-future and get rid of Wait
  fiber_mgr_.addTask([this]() {
    result_code_ = lcb_wait(connection_);
    auto query_mgr = UnwrapData(isolate_)->query_mgr;
    query_mgr->RestoreConnection(connection_);
  });
  state_ = State::kStarted;
  return {false};
}

void Query::Iterator::RowCallback(lcb_t connection, int,
                                  const lcb_RESPN1QL *resp) {
  auto cursor =
      static_cast<Cursor *>(const_cast<void *>(lcb_get_cookie(connection)));
  cursor->is_streaming_started = true;
  cursor->writer.wait();
  cursor->writer.reset();
  cursor->data.assign(resp->row, resp->nrow);
  cursor->is_last = (resp->rflags & LCB_RESP_F_FINAL) != 0;
  cursor->is_error = (cursor->is_last) && (resp->rc != LCB_SUCCESS ||
                                           !IsStatusSuccess(cursor->data));
  cursor->err_code = resp->rc;
  cursor->is_auth_error = cursor->is_error && (resp->rc == LCB_AUTH_ERROR);

  if (cursor->is_error && cursor->data.empty()) {
    cursor->data = lcb_strerror(connection, resp->rc);
  }

  LOG(logDebug) << "Query::Iterator::RowCallback data : " << RU(cursor->data)
                << " is_last : " << cursor->is_last
                << " is_error : " << cursor->is_error
                << " is_auth_error : " << cursor->is_auth_error
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
    lcb_n1ql_cancel(connection_, query_handle_);
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
  if (result_code_ != LCB_SUCCESS) {
    std::stringstream error;
    error << "Query did not run successfully : "
          << lcb_strerror(connection_, result_code_) << std::endl;
    return {true, error.str()};
  }
  return {false};
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
  return {is_last, is_error, is_auth_error, err_code, data};
}

Query::Row Query::Iterator::Cursor::GetRowAsFinal() const {
  auto row = GetRow();
  row.is_done = true;
  return row;
}
