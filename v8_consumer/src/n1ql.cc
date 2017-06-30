// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

#include <include/libplatform/libplatform.h>
#include <include/v8.h>
#include <iostream>
#include <libcouchbase/api3.h>
#include <libcouchbase/couchbase.h>
#include <libcouchbase/n1ql.h>
#include <string>
#include <vector>

#include "../include/n1ql.h"

// Reference to the query engine instantiated by v8worker.
extern N1QL *n1ql_handle;

ConnectionPool::ConnectionPool(int capacity, std::string cb_kv_endpoint,
                               std::string cb_source_bucket,
                               std::string rbac_user, std::string rbac_pass)
    : capacity(capacity), inst_count(0), rbac_pass(rbac_pass) {
  conn_str = "couchbase://" + cb_kv_endpoint + "/" + cb_source_bucket +
             "?username=" + rbac_user + "&select_bucket=true";
}

// Creates and adds one lcb instance into the pool.
void ConnectionPool::AddResource() {
  // Initialization of lcb instances pool.
  lcb_create_st options;
  lcb_error_t err;
  memset(&options, 0, sizeof(options));
  options.version = 3;
  options.v.v3.connstr = conn_str.c_str();
  options.v.v3.type = LCB_TYPE_BUCKET;
  options.v.v3.passwd = rbac_pass.c_str();

  lcb_t instance = nullptr;
  err = lcb_create(&instance, &options);
  if (err != LCB_SUCCESS) {
    Error(instance, "N1QL: unable to create lcb handle", err);
  }

  err = lcb_connect(instance);
  if (err != LCB_SUCCESS) {
    Error(instance, "N1QL: unable to connect to server", err);
  }

  lcb_wait(instance);

  err = lcb_get_bootstrap_status(instance);
  if (err != LCB_SUCCESS) {
    Error(instance, "N1QL: unable to get bootstrap status", err);
  }

  ++inst_count;
  instances.push(instance);
}

lcb_t ConnectionPool::GetResource() {
  // Dynamically expand the pool size if it's within the pool capacity.
  if (instances.empty()) {
    if (inst_count >= capacity) {
      throw "Maximum pool capacity reached";
    } else {
      AddResource();
    }
  }

  lcb_t instance = instances.front();
  instances.pop();
  return instance;
}

void ConnectionPool::Error(lcb_t instance, const char *msg, lcb_error_t err) {
  LOG(logError) << err << " " << lcb_strerror(instance, err) << '\n';
}

ConnectionPool::~ConnectionPool() {
  while (!instances.empty()) {
    lcb_t instance = instances.front();
    if (instance) {
      lcb_destroy(instance);
    }
    instances.pop();
  }
}

void HashedStack::Push(QueryHandler &q_handler) {
  qstack.push(q_handler);
  qmap[q_handler.index_hash] = &q_handler;
}

void HashedStack::Pop() {
  auto it = qmap.find(qstack.top().index_hash);
  qmap.erase(it);
  qstack.pop();
}

template <>
void N1QL::RowCallback<IterQueryHandler>(lcb_t instance, int callback_type,
                                         const lcb_RESPN1QL *resp) {
  QueryHandler q_handler = n1ql_handle->qhandler_stack.Top();

  if (!(resp->rflags & LCB_RESP_F_FINAL)) {
    char *row_str;
    asprintf(&row_str, "%.*s\n", (int)resp->nrow, resp->row);

    v8::Isolate *isolate = q_handler.isolate;
    v8::Local<v8::Value> args[1];
    args[0] = v8::JSON::Parse(v8::String::NewFromUtf8(isolate, row_str));

    // Execute the function callback passed in JavaScript.
    v8::Local<v8::Function> callback = q_handler.iter_handler->callback;
    v8::TryCatch tryCatch(isolate);
    callback->Call(callback, 1, args);
    if (tryCatch.HasCaught()) {
      // Cancel the query if an exception was thrown and re-throw the exception.
      lcb_N1QLHANDLE *handle = (lcb_N1QLHANDLE *)lcb_get_cookie(instance);
      lcb_n1ql_cancel(instance, *handle);
      tryCatch.ReThrow();
    }

    free(row_str);
  } else {
    q_handler.iter_handler->metadata = resp->row;
  }
}

template <>
void N1QL::RowCallback<BlockingQueryHandler>(lcb_t instance, int callback_type,
                                             const lcb_RESPN1QL *resp) {
  QueryHandler q_handler = n1ql_handle->qhandler_stack.Top();

  if (!(resp->rflags & LCB_RESP_F_FINAL)) {
    char *row_str;
    asprintf(&row_str, "%.*s\n", (int)resp->nrow, resp->row);

    // Append the result to the rows vector.
    q_handler.block_handler->rows.push_back(std::string(row_str));

    free(row_str);
  } else {
    q_handler.block_handler->metadata = resp->row;
  }
}

template <typename HandlerType> void N1QL::ExecQuery(QueryHandler &q_handler) {
  q_handler.instance = inst_pool->GetResource();

  // Schedule the data to support query.
  n1ql_handle->qhandler_stack.Push(q_handler);

  lcb_t &instance = q_handler.instance;
  lcb_error_t err;
  lcb_CMDN1QL cmd = {0};
  lcb_N1QLHANDLE handle = NULL;
  cmd.handle = &handle;
  cmd.callback = RowCallback<HandlerType>;

  lcb_N1QLPARAMS *n1ql_params = lcb_n1p_new();
  err = lcb_n1p_setstmtz(n1ql_params, q_handler.query.c_str());
  if (err != LCB_SUCCESS)
    ConnectionPool::Error(instance, "unable to build query string", err);

  lcb_n1p_mkcmd(n1ql_params, &cmd);

  err = lcb_n1ql_query(instance, NULL, &cmd);
  if (err != LCB_SUCCESS)
    ConnectionPool::Error(instance, "unable to query", err);

  lcb_n1p_free(n1ql_params);

  // Set the N1QL handle as cookie for instance - allow for query cancellation.
  lcb_set_cookie(instance, &handle);
  // Run the query.
  lcb_wait(instance);

  // Resource clean-up.
  lcb_set_cookie(instance, NULL);
  n1ql_handle->qhandler_stack.Pop();
  inst_pool->Restore(instance);
}

void IterFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handleScope(isolate);

  // Concatenate stack index with the hash of N1QL instance in JavaScript.
  std::string index_hash = AppendStackIndex(args.This()->GetIdentityHash());
  SetHiddenValue(args, "index_hash", index_hash);

  // Query to run.
  v8::Local<v8::Name> query_name = v8::String::NewFromUtf8(isolate, "query");
  v8::Local<v8::Value> query_value = args.This()->Get(query_name);
  v8::String::Utf8Value query_string(query_value);

  // Callback function to execute.
  v8::Local<v8::Function> func = v8::Local<v8::Function>::Cast(args[0]);

  // Prepare data for query execution.
  IterQueryHandler iter_handler;
  iter_handler.callback = func;
  iter_handler.return_value = v8::String::NewFromUtf8(isolate, "");
  QueryHandler q_handler;
  q_handler.index_hash = index_hash;
  q_handler.query = *query_string;
  q_handler.isolate = args.GetIsolate();
  q_handler.iter_handler = &iter_handler;

  n1ql_handle->ExecQuery<IterQueryHandler>(q_handler);

  // Add query metadata.
  v8::Local<v8::Object> n1ql_obj = args.This();
  AddQueryMetadata(iter_handler, isolate, n1ql_obj);
  args.This() = n1ql_obj;

  args.GetReturnValue().Set(iter_handler.return_value);
}

void StopIterFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::EscapableHandleScope handle_scope(isolate);
  v8::Local<v8::Value> arg = args[0];

  std::string index_hash = GetHiddenValue(args, "index_hash");

  // Cancel the query corresponding to obj_hash.
  QueryHandler *q_handler = n1ql_handle->qhandler_stack.Get(index_hash);
  lcb_t instance = q_handler->instance;
  lcb_N1QLHANDLE *handle = (lcb_N1QLHANDLE *)lcb_get_cookie(instance);
  lcb_n1ql_cancel(instance, *handle);

  // Bubble up the message sent from JavaScript.
  q_handler->iter_handler->return_value = handle_scope.Escape(arg);
}

void ExecQueryFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handleScope(isolate);

  // Concatenate stack index with the hash of N1QL instance in JavaScript.
  std::string index_hash = AppendStackIndex(args.This()->GetIdentityHash());
  SetHiddenValue(args, "index_hash", index_hash);

  // Query to run.
  v8::Local<v8::Name> query_name = v8::String::NewFromUtf8(isolate, "query");
  v8::Local<v8::Value> query_value = args.This()->Get(query_name);
  v8::String::Utf8Value query_string(query_value);

  // Prepare data for query execution.
  BlockingQueryHandler block_handler;
  QueryHandler q_handler;
  q_handler.index_hash = index_hash;
  q_handler.query = *query_string;
  q_handler.isolate = args.GetIsolate();
  q_handler.block_handler = &block_handler;

  n1ql_handle->ExecQuery<BlockingQueryHandler>(q_handler);

  std::vector<std::string> &rows = block_handler.rows;
  v8::Local<v8::Array> result_array =
      v8::Array::New(isolate, static_cast<int>(rows.size()));

  // Populate the result array with the rows of the result.
  for (int i = 0; i < rows.size(); ++i) {
    v8::Local<v8::Value> json_row =
        v8::JSON::Parse(v8::String::NewFromUtf8(isolate, rows[i].c_str()));
    result_array->Set(static_cast<uint32_t>(i), json_row);
  }

  AddQueryMetadata(block_handler, isolate, result_array);

  args.GetReturnValue().Set(result_array);
}

template <typename HandlerType, typename ResultType>
void AddQueryMetadata(HandlerType handler, v8::Isolate *isolate,
                      ResultType &result) {
  if (handler.metadata.length() > 0) {
    // Query metadata.
    v8::Local<v8::String> meta_name =
        v8::String::NewFromUtf8(isolate, "metadata");
    v8::Local<v8::String> meta_str =
        v8::String::NewFromUtf8(isolate, handler.metadata.c_str());
    v8::Local<v8::Value> meta_value = v8::JSON::Parse(meta_str);

    result->Set(meta_name, meta_value);
  }
}

std::string AppendStackIndex(int obj_hash) {
  std::string index_hash = std::to_string(obj_hash);
  index_hash += std::to_string(n1ql_handle->qhandler_stack.Size());

  return index_hash;
}

void SetHiddenValue(const v8::FunctionCallbackInfo<v8::Value> &args,
                    std::string key_str, std::string value_str) {
  v8::Isolate *isolate = args.GetIsolate();
  auto key = v8::Private::ForApi(
      isolate, v8::String::NewFromUtf8(isolate, key_str.c_str()));
  auto value = v8::String::NewFromUtf8(isolate, value_str.c_str());

  args.This()->SetPrivate(isolate->GetCurrentContext(), key, value);
}

std::string GetHiddenValue(const v8::FunctionCallbackInfo<v8::Value> &args,
                           std::string key_str) {
  v8::Isolate *isolate = args.GetIsolate();
  auto key = v8::Private::ForApi(
      isolate, v8::String::NewFromUtf8(isolate, key_str.c_str()));
  v8::MaybeLocal<v8::Value> value =
      args.This()->GetPrivate(isolate->GetCurrentContext(), key);
  v8::String::Utf8Value value_str(value.ToLocalChecked());

  return *value_str;
}
