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
#include "query-controllers.h"
#include "query-helper.h"
#include "query-iterator.h"
#include "query-mgr.h"
#include "v8worker2.h"

::Info Query::N1qlController::build(void *cookie) {
  auto info = do_build(cookie);
  if (info.is_fatal) {
    auto helper = UnwrapData(isolate_)->query_mgr;
    helper->RestoreConnection(connection_);
  }
  return info;
}

::Info Query::N1qlController::do_build(void *cookie) {
  lcb_cmdquery_create(&cmd_);
  auto result = lcb_cmdquery_statement(cmd_, query_info_.query.c_str(),
                                       query_info_.query.size());
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set query", connection_, result);
  }

  for (const auto &[key, value] : query_info_.named_params) {
    size_t first = key.find_first_not_of(' ');
    std::string key2 = key.substr(first, key.size() - first);
    first = key2.find_first_not_of('$');
    std::string key3 = key2.substr(first, key2.size() - first);
    result = lcb_cmdquery_named_param(cmd_, key3.c_str(), key3.size(),
                                      value.c_str(), value.size());

    if (result != LCB_SUCCESS) {
      return ErrorFormat("Unable to set named parameter", connection_, result);
    }
  }

  for (const auto &param : query_info_.pos_params) {
    result = lcb_cmdquery_positional_param(cmd_, param.c_str(), param.size());
    if (result != LCB_SUCCESS) {
      return ErrorFormat("Unable to set positional parameter", connection_,
                         result);
    }
  }

  result = lcb_cmdquery_consistency(
      cmd_, query_info_.options.GetOrDefaultN1qlConsistency(isolate_));
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set consistency", connection_, result);
  }

  if (query_info_.options.client_context_id == nullptr) {
    auto stack = v8::StackTrace::CurrentStackTrace(isolate_, 1,
                                                   v8::StackTrace::kLineNumber);
    if (stack->GetFrameCount() > 0) {
      auto frame = stack->GetFrame(isolate_, 0);
      std::string file = "?";
      if (!frame->GetScriptName().IsEmpty()) {
        file = *v8::String::Utf8Value(isolate_, frame->GetScriptName());
      }
      std::string func = "?";
      if (!frame->GetFunctionName().IsEmpty()) {
        func = *v8::String::Utf8Value(isolate_, frame->GetFunctionName());
      }
      int line = frame->GetLineNumber();
      std::ostringstream id;
      id << line << "@" << file << "(" << func << ')';
      query_info_.options.client_context_id =
          std::make_unique<std::string>(id.str());
    }
  }

  if (query_info_.options.client_context_id != nullptr) {
    // n1ql limits to 64 chars, and no \ or " chars
    std::string id = query_info_.options.client_context_id->c_str();
    id.erase(std::remove(id.begin(), id.end(), '\\'), id.end());
    id.erase(std::remove(id.begin(), id.end(), '\"'), id.end());
    if (id.length() > 62) {
      id.erase(62);
    }

    result = lcb_cmdquery_client_context_id(cmd_, id.c_str(), id.size());
    if (result != LCB_SUCCESS) {
      return ErrorFormat("Unable to set clientContextId", connection_, result);
    }
  }

  lcb_cmdquery_handle(cmd_, &handle_);
  lcb_cmdquery_callback(cmd_, RowCallback);
  lcb_cmdquery_adhoc(cmd_,
                     !query_info_.options.GetOrDefaultN1qlIsPrepared(isolate_));

  lcb_set_cookie(connection_, cookie);

  auto timeout = UnwrapData(isolate_)->n1ql_timeout;
  result = lcb_cmdquery_timeout(cmd_, timeout);
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set timeout for query", connection_, result);
  }

  if (on_behalf_of_.size() != 0) {
    lcb_cmdquery_on_behalf_of(cmd_, on_behalf_of_.c_str(),
                              on_behalf_of_.size());
  }

  result = lcb_cntl_setu32(connection_, LCB_CNTL_QUERY_GRACE_PERIOD,
                           Query::n1ql_grace_period);
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set n1ql grace period for query", connection_,
                       result);
  }

  return {false};
}

lcb_STATUS Query::N1qlController::run() {
  auto result = lcb_query(connection_, nullptr, GetCmd());
  if (result == LCB_SUCCESS) {
    result = lcb_wait(connection_, LCB_WAIT_DEFAULT);
  }

  auto helper = UnwrapData(isolate_)->query_mgr;
  helper->RestoreConnection(connection_);
  return result;
}

void Query::N1qlController::cancel() {
  lcb_query_cancel(connection_, GetHandle());
}

::Info Query::N1qlController::ErrorFormat(const std::string &message,
                                          lcb_INSTANCE *connection,
                                          lcb_STATUS error) const {
  auto helper = UnwrapData(isolate_)->query_helper;
  return {true, helper->ErrorFormat(message, connection, error)};
}

bool Query::N1qlController::IsStatusSuccess(const std::string &row) {
  std::regex re_status_success(R"("status"\s*:\s*"success")");
  return std::regex_search(row, re_status_success);
}

void Query::N1qlController::ThrowQueryError(const std::string &err_msg) {
  auto v8worker = UnwrapData(isolate_)->v8worker2;
  v8worker->stats_->IncrementFailureStat("n1ql_op_exception_count");
  auto js_exception = UnwrapData(isolate_)->js_exception;
  js_exception->ThrowN1qlError(err_msg);
}

void Query::N1qlController::RowCallback(lcb_INSTANCE *connection, int,
                                        const lcb_RESPQUERY *resp) {
  auto cursor = static_cast<Query::Iterator::Cursor *>(
      const_cast<void *>(lcb_get_cookie(connection)));

  const char *row;
  size_t nrow;
  lcb_respquery_row(resp, &row, &nrow);

  cursor->data.assign(row, nrow);
  cursor->is_last = lcb_respquery_is_final(resp);
  cursor->client_err_code = lcb_respquery_status(resp);
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

  cursor->YieldTo(Query::Iterator::Cursor::ExecutionControl::kV8);
  if (!cursor->is_last) {
    cursor->WaitFor(Query::Iterator::Cursor::ExecutionControl::kSDK);
  }
}

::Info Query::N1qlController::ValidateQuery(
    const v8::FunctionCallbackInfo<v8::Value> &args) {
  if (args.Length() < 1) {
    return {true, "Need at least the query"};
  }
  if (!args[0]->IsString()) {
    return {true, "Expecting a string for the first parameter"};
  }
  if (args.Length() < 2) {
    return {false};
  }
  if (!(args[1]->IsObject() || args[1]->IsArray())) {
    return {true, "Expecting an object or an array for the second parameter"};
  }
  return {false};
}

Query::Info Query::N1qlController::GetQueryInfo(
    Query::Helper *helper, const v8::FunctionCallbackInfo<v8::Value> &args) {
  Query::Info query_info;

  query_info.query = helper->GetQueryStatement(args[0]);
  if (args.Length() < 2) {
    return query_info;
  }

  if (args[1]->IsArray()) {
    if (auto info = helper->GetN1qlPosParams(args[1]); info.is_fatal) {
      return {true, info.msg};
    } else {
      std::swap(query_info.pos_params, info.pos_params);
    }
  } else if (args[1]->IsObject()) {
    if (auto info = helper->GetN1qlNamedParams(args[1]); info.is_fatal) {
      return {true, info.msg};
    } else {
      std::swap(query_info.named_params, info.named_params);
    }
  }

  if (args.Length() < 3) {
    return query_info;
  }

  Query::Options options;
  if (auto info = helper->opt_extractor_.ExtractN1qlOptions(args[2], options);
      info.is_fatal) {
    return {true, info.msg};
  }

  std::swap(query_info.options, options);
  return query_info;
}

void Query::N1qlFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  auto start_time = GetUnixTime();
  v8::HandleScope handle_scope(isolate);
  const auto max_timeout = UnwrapData(isolate)->op_timeout;
  const auto max_retry = UnwrapData(isolate)->lcb_retry_count;
  auto query_mgr = UnwrapData(isolate)->query_mgr;
  auto helper = UnwrapData(isolate)->query_helper;
  auto js_exception = UnwrapData(isolate)->js_exception;
  auto v8worker = UnwrapData(isolate)->v8worker2;

  auto validation_info = Query::N1qlController::ValidateQuery(args);
  if (validation_info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("n1ql_op_exception_count");
    js_exception->ThrowN1qlError(validation_info.msg);
    return;
  }

  auto conn_refreshed = false;
  auto retry = 0;
  while (true) {
    retry++;
    auto query_info = Query::N1qlController::GetQueryInfo(helper, args);
    if (query_info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("n1ql_op_exception_count");
      js_exception->ThrowN1qlError(query_info.msg);
      return;
    }

    std::unique_ptr<QueryController> query_controller;
    query_controller =
        std::make_unique<N1qlController>(isolate, std::move(query_info));
    auto it_info = query_mgr->NewIterable(std::move(query_controller));
    if (it_info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("n1ql_op_exception_count");
      js_exception->ThrowN1qlError(it_info.msg);
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
      v8worker->stats_->IncrementFailureStat("n1ql_op_exception_count");
      js_exception->ThrowN1qlError(start_info.msg);
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
        v8worker->stats_->IncrementFailureStat("n1ql_op_exception_count");
        js_exception->ThrowN1qlError(it_result.msg);
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
      v8worker->stats_->IncrementFailureStat("n1ql_op_exception_count");
      js_exception->ThrowN1qlError(err_msg);
      return;
    }

    v8::Locker locker(isolate);
    auto wrapper = new Query::WrapStop(isolate, iterator, it_info.iterable);
    args.GetReturnValue().Set(wrapper->value_.Get(isolate));
    break;
  }
}
