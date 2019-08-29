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

#include "conn-pool.h"
#include "query-iterable.h"

#include <memory>
#include <mutex>

#include "comm.h"
#include "info.h"
#include "isolate_data.h"
#include "js_exception.h"
#include "query-helper.h"
#include "query-mgr.h"
#include "utils.h"

extern std::atomic<int64_t> n1ql_op_exception_count;

void Query::Manager::ClearQueries() {
  for (auto &iterator : iterators_) {
    iterator.second->Stop();
  }
  iterators_.clear();
}

Query::Iterable::Info
Query::Manager::NewIterable(const Query::Info &query_info) {
  auto conn_info = conn_pool_.GetConnection();
  if (conn_info.is_fatal) {
    return {true, conn_info.msg};
  }

  auto iterator = std::make_unique<Query::Iterator>(
      query_info, conn_info.connection, isolate_);
  auto iterator_ptr = iterator.get();

  auto iterable = UnwrapData(isolate_)->query_iterable;
  auto info = iterable->NewObject(iterator_ptr);
  if (info.is_fatal) {
    RestoreConnection(conn_info.connection);
    return {true, info.msg};
  }

  iterators_[conn_info.connection] = std::move(iterator);
  return {iterator_ptr, info.object};
}

void QueryFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  v8::HandleScope handle_scope(isolate);
  auto query_mgr = UnwrapData(isolate)->query_mgr;
  auto helper = UnwrapData(isolate)->query_helper;
  auto js_exception = UnwrapData(isolate)->js_exception;
  auto comm = UnwrapData(isolate)->comm;

  auto validation_info = Query::Helper::ValidateQuery(args);
  if (validation_info.is_fatal) {
    ++n1ql_op_exception_count;
    js_exception->ThrowN1QLError(validation_info.msg);
    return;
  }

  auto query_info = helper->CreateQuery(args);
  if (query_info.is_fatal) {
    ++n1ql_op_exception_count;
    js_exception->ThrowN1QLError(query_info.msg);
    return;
  }

  auto it_info = query_mgr->NewIterable(query_info);
  if (it_info.is_fatal) {
    ++n1ql_op_exception_count;
    js_exception->ThrowN1QLError(it_info.msg);
    return;
  }

  auto &iterator = it_info.iterator;
  if (auto start_info = iterator->Start(); start_info.is_fatal) {
    ++n1ql_op_exception_count;
    js_exception->ThrowN1QLError(start_info.msg);
    return;
  }
  auto first_row = iterator->Peek();
  if (first_row.is_done || first_row.is_error) {
    // Error reported by lcb_wait (coming from LCB client)
    if (auto it_result = iterator->Wait(); it_result.is_fatal) {
      ++n1ql_op_exception_count;
      helper->AccountLCBError(static_cast<int>(iterator->GetResultCode()));
      js_exception->ThrowN1QLError(it_result.msg);
      return;
    }
  }

  if (first_row.is_error) {
    ++n1ql_op_exception_count;
    if (first_row.is_auth_error) {
      comm->Refresh();
    }
    // Error reported by RowCallback (coming from LCB client)
    if (first_row.err_code != LCB_SUCCESS) {
      helper->AccountLCBError(first_row.err_code);
      js_exception->ThrowN1QLError(first_row.data);
      return;
    }
    // Error reported by RowCallback (coming from query server)
    if (auto acc_info = helper->AccountLCBError(first_row.data);
        acc_info.is_fatal) {
      js_exception->ThrowN1QLError(acc_info.msg);
      return;
    }
    js_exception->ThrowN1QLError(first_row.data);
    return;
  }
  args.GetReturnValue().Set(it_info.iterable);
}
