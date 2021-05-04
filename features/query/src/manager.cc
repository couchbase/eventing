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

#include <memory>
#include <mutex>
#include <utility>

#include "conn-pool.h"
#include "info.h"
#include "isolate_data.h"
#include "js_exception.h"
#include "lcb_utils.h"
#include "query-helper.h"
#include "query-iterable.h"
#include "query-mgr.h"
#include "utils.h"

extern std::atomic<int64_t> n1ql_op_exception_count;

void Query::Manager::ClearQueries() {
  for (auto &iterator : iterators_) {
    iterator.second->Stop();
  }
  iterators_.clear();
}

Query::Iterable::Info Query::Manager::NewIterable(Query::Info query_info) {
  auto conn_info = conn_pool_.GetConnection();
  if (conn_info.is_fatal) {
    return {true, conn_info.msg};
  }
  auto iterator = std::make_shared<Query::Iterator>(
      std::move(query_info), conn_info.connection, isolate_);
  auto iterator_ptr = iterator.get();
  auto iterable = UnwrapData(isolate_)->query_iterable;
  auto info = iterable->NewObject(iterator_ptr);
  if (info.is_fatal) {
    conn_pool_.RestoreConnection(conn_info.connection);
    return {true, info.msg};
  }
  iterators_[conn_info.connection] = iterator;
  return {iterator, info.object};
}

void QueryFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
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

  auto validation_info = Query::Helper::ValidateQuery(args);
  if (validation_info.is_fatal) {
    ++n1ql_op_exception_count;
    js_exception->ThrowN1QLError(validation_info.msg);
    return;
  }

  auto conn_refreshed = false;
  auto retry = 0;
  while (true) {
    retry++;
    auto query_info = helper->CreateQuery(args);
    if (query_info.is_fatal) {
      ++n1ql_op_exception_count;
      js_exception->ThrowN1QLError(query_info.msg);
      return;
    }

    auto it_info = query_mgr->NewIterable(std::move(query_info));
    if (it_info.is_fatal) {
      ++n1ql_op_exception_count;
      js_exception->ThrowN1QLError(it_info.msg);
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
      ++n1ql_op_exception_count;
      js_exception->ThrowN1QLError(start_info.msg);
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
        ++n1ql_op_exception_count;
        js_exception->ThrowN1QLError(it_result.msg);
        return;
      }
    }

    auto retriable = (first_row.err_code == LCB_ERR_DML_FAILURE) || IsRetriable(first_row.err_code);
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
      helper->HandleRowError(first_row);
      return;
    }

    v8::Locker locker(isolate);
    auto wrapper = new Query::WrapStop(isolate, iterator, it_info.iterable);
    args.GetReturnValue().Set(wrapper->value_.Get(isolate));
    break;
  }
}
