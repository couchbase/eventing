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
#include <regex>

#include "js_exception.h"
#include "lcb_utils.h"
#include "query-helper.h"
#include "query-iterator.h"
#include "query-mgr.h"
#include "query-controllers.h"
#include "v8worker2.h"

::Info Query::SearchQuery::build(void *cookie) {
  auto info = do_build(cookie);
  if (info.is_fatal) {
    auto helper = UnwrapData(isolate_)->query_mgr;
    helper->RestoreConnection(connection_);
  }
  return info;
}

::Info Query::SearchQuery::do_build(void *cookie) {
  lcb_cmdsearch_create(&cmd_);
  auto result = lcb_cmdsearch_payload(cmd_, query_info_.query.c_str(),
                                      query_info_.query.size());
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set search query", connection_, result);
  }

  lcb_set_cookie(connection_, cookie);
  lcb_cmdsearch_handle(cmd_, &handle_);
  lcb_cmdsearch_callback(cmd_, RowCallback);

  auto timeout = UnwrapData(isolate_)->search_timeout;
  result = lcb_cmdsearch_timeout(cmd_, timeout);
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set timeout for search query", connection_, result);
  }

  if (on_behalf_of_.size() != 0) {
    lcb_cmdsearch_on_behalf_of(cmd_, on_behalf_of_.c_str(),
                                  on_behalf_of_.size());
  }

  return {false};
}

::Info Query::SearchQuery::ValidateQuery(
    const v8::FunctionCallbackInfo<v8::Value> &args) {
  if (args.Length() < 1) {
    return {true, "Need at least the query"};
  }
  if (!args[0]->IsString()) {
    return {true, "Expecting a string for the first parameter"};
  }
  return {false};
}

lcb_STATUS Query::SearchQuery::run() {
  auto result = lcb_search(connection_, nullptr, GetCmd());
  if (result == LCB_SUCCESS) {
    result = lcb_wait(connection_, LCB_WAIT_DEFAULT);
  }

  auto helper = UnwrapData(isolate_)->query_mgr;
  helper->RestoreConnection(connection_);
  return result;
}

void Query::SearchQuery::ThrowQueryError(const std::string &err_msg) {
  auto v8worker = UnwrapData(isolate_)->v8worker2;
  v8worker->stats_->IncrementFailureStat("search_op_exception_count");
  auto js_exception = UnwrapData(isolate_)->js_exception;
  js_exception->ThrowSearchError(err_msg);
}

void Query::SearchQuery::cancel() {
  lcb_search_cancel(connection_, GetHandle());
}

::Info Query::SearchQuery::ErrorFormat(const std::string &message,
                                       lcb_INSTANCE *connection,
                                       lcb_STATUS error) const {
  auto helper = UnwrapData(isolate_)->query_helper;
  return {true, helper->ErrorFormat(message, connection, error)};
}

bool Query::SearchQuery::IsStatusSuccess(const std::string &row) {
  std::regex re_status_failure(R"("status"\s*:\s*"fail")");
  return !std::regex_search(row, re_status_failure);
}

void Query::SearchQuery::RowCallback(lcb_INSTANCE *connection, int,
                                     const lcb_RESPSEARCH *resp) {
  auto cursor = static_cast<Query::Iterator::Cursor *>(
      const_cast<void *>(lcb_get_cookie(connection)));

  const char *row;
  size_t nrow;
  lcb_respsearch_row(resp, &row, &nrow);

  cursor->data.assign(row, nrow);
  cursor->is_last = lcb_respsearch_is_final(resp);
  cursor->client_err_code = lcb_respsearch_status(resp);
  cursor->is_client_error =
      cursor->is_last && cursor->client_err_code != LCB_SUCCESS;
  cursor->is_query_error = cursor->is_last && !IsStatusSuccess(cursor->data);
  cursor->is_error = cursor->is_client_error || cursor->is_query_error;
  cursor->is_client_auth_error =
      cursor->is_error &&
      (cursor->client_err_code == LCB_ERR_AUTHENTICATION_FAILURE ||
       cursor->client_err_code == LCB_ERR_SSL_CANTVERIFY);

  if (cursor->is_client_error) {
    cursor->client_error = lcb_strerror_short(cursor->client_err_code);
  }
  if (cursor->is_query_error) {
    cursor->query_error = cursor->data;
  }

  LOG(logDebug) << "Query::Iterator::RowCallback data : " << RU(cursor->data)
                << " is_last : " << cursor->is_last
                << " is_error : " << cursor->is_error
                << " is_client_auth_error : " << cursor->is_client_auth_error
                << " resp rc : " << cursor->client_err_code << std::endl;

  cursor->YieldTo(Query::Iterator::Cursor::ExecutionControl::kV8);
  if (!cursor->is_last) {
    cursor->WaitFor(Query::Iterator::Cursor::ExecutionControl::kSDK);
  }
}

Query::Info Query::SearchQuery::GetQueryInfo(
    Query::Helper *helper, const v8::FunctionCallbackInfo<v8::Value> &args) {
  Query::Info query_info;

  query_info.query = helper->GetQueryStatement(args[0]);
  if (args.Length() < 2) {
    return query_info;
  }
  return query_info;
}

void Query::SearchFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  auto start_time = GetUnixTime();
  v8::HandleScope handle_scope(isolate);
  const auto max_timeout = UnwrapData(isolate)->op_timeout;
  auto query_mgr = UnwrapData(isolate)->query_mgr;
  auto helper = UnwrapData(isolate)->query_helper;
  auto js_exception = UnwrapData(isolate)->js_exception;
  auto max_retry = UnwrapData(isolate)->lcb_retry_count;
  auto v8worker = UnwrapData(isolate)->v8worker2;

  auto validation_info = Query::SearchQuery::ValidateQuery(args);
  if (validation_info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("search_op_exception_count");
    js_exception->ThrowSearchError(validation_info.msg);
    return;
  }

  auto conn_refreshed = false;
  auto retry = 0;
  while (true) {
    retry++;
    auto query_info = Query::SearchQuery::GetQueryInfo(helper, args);
    if (query_info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("search_op_exception_count");
      js_exception->ThrowSearchError(query_info.msg);
      return;
    }

    std::unique_ptr<QueryController> qBase;
    qBase = std::make_unique<SearchQuery>(isolate, std::move(query_info));
    auto it_info = query_mgr->NewIterable(std::move(qBase));
    if (it_info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("search_op_exception_count");
      js_exception->ThrowSearchError(it_info.msg);
      return;
    }

    auto iterator = it_info.iterator;
    if (auto start_info = iterator->Start(); start_info.is_fatal) {
      if (!conn_refreshed &&
          (start_info.is_retriable || start_info.is_lcb_special_error)) {
        iterator->Wait();
        query_mgr->RefreshTopConnection();
        conn_refreshed = true;
        continue;
      }

      if (start_info.is_retriable &&
          helper->CheckRetriable(max_retry, max_timeout, retry, start_time)) {
        continue;
      }
      v8worker->stats_->IncrementFailureStat("search_op_exception_count");
      js_exception->ThrowSearchError(start_info.msg);
      return;
    }

    auto first_row = iterator->Peek();
    if (first_row.is_done || first_row.is_error) {
      // Error reported by lcb_wait (coming from LCB client)
      if (auto it_result = iterator->Wait(); it_result.is_fatal) {
        if (!conn_refreshed &&
            (it_result.is_retriable || it_result.is_lcb_special_error)) {
          query_mgr->RefreshTopConnection();
          conn_refreshed = true;
          continue;
        }

        if (it_result.is_retriable &&
            helper->CheckRetriable(max_retry, max_timeout, retry, start_time)) {
          continue;
        }
        v8worker->stats_->IncrementFailureStat("search_op_exception_count");
        js_exception->ThrowSearchError(it_result.msg);
        return;
      }
    }

    auto retriable = IsRetriable(first_row.err_code);
    if (!conn_refreshed && (first_row.is_client_auth_error ||
                            first_row.err_code == LCB_ERR_HTTP || retriable)) {
      query_mgr->RefreshTopConnection();
      conn_refreshed = true;
      continue;
    }

    if (retriable &&
        helper->CheckRetriable(max_retry, max_timeout, retry, start_time)) {
      continue;
    }

    if (first_row.is_error) {
      auto err_msg = helper->RowErrorString(first_row);
      v8worker->stats_->IncrementFailureStat("search_op_exception_count");
      js_exception->ThrowSearchError(err_msg);
      return;
    }

    v8::Locker locker(isolate);
    auto wrapper = new Query::WrapStop(isolate, iterator, it_info.iterable);
    args.GetReturnValue().Set(wrapper->value_.Get(isolate));
    break;
  }
}
