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

#include <iostream>
#include <libcouchbase/api3.h>
#include <libcouchbase/couchbase.h>
#include <libcouchbase/n1ql.h>
#include <libplatform/libplatform.h>
#include <string>
#include <v8.h>
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
  qmap[q_handler.hash] = &q_handler;
}

void HashedStack::Pop() {
  auto it = qmap.find(qstack.top().hash);
  qmap.erase(it);
  qstack.pop();
}

// Extracts error messages from the metadata JSON.
std::vector<std::string> N1QL::ExtractErrorMsg(const char *metadata,
                                               v8::Isolate *isolate) {
  v8::HandleScope handle_scope(isolate);

  std::vector<std::string> errors;
  auto metadata_v8str = v8::String::NewFromUtf8(isolate, metadata);
  auto metadata_obj = v8::JSON::Parse(metadata_v8str).As<v8::Object>();

  if (!metadata_obj.IsEmpty()) {
    auto errors_v8val =
        metadata_obj->Get(v8::String::NewFromUtf8(isolate, "errors"));
    auto errors_v8arr = errors_v8val.As<v8::Array>();

    for (uint32_t i = 0; i < errors_v8arr->Length(); ++i) {
      auto error = errors_v8arr->Get(i).As<v8::Object>();
      auto msg_v8str = error->Get(v8::String::NewFromUtf8(isolate, "msg"));
      v8::String::Utf8Value msg(msg_v8str);
      errors.push_back(*msg);
    }
  } else {
    LOG(logError) << "Error parsing JSON while extracting N1QL error message"
                  << '\n';
  }

  return errors;
}

// Row-callback for iterator.
template <>
void N1QL::RowCallback<IterQueryHandler>(lcb_t instance, int callback_type,
                                         const lcb_RESPN1QL *resp) {
  QueryHandler q_handler = n1ql_handle->qhandler_stack.Top();
  v8::Isolate *isolate = q_handler.isolate;
  v8::HandleScope handle_scope(isolate);

  if (!(resp->rflags & LCB_RESP_F_FINAL)) {
    char *row_str;

#if defined(_WIN32) || defined(WIN32)
    WinSprintf(&row_str, "%.*s\n", static_cast<int>(resp->nrow), resp->row);
#else
    asprintf(&row_str, "%.*s\n", static_cast<int>(resp->nrow), resp->row);
#endif

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
    if (resp->rc != LCB_SUCCESS) {
      auto errors = n1ql_handle->ExtractErrorMsg(resp->row, isolate);
      V8Worker::exception.Throw(instance, resp->rc, errors);
    }

    q_handler.iter_handler->metadata = resp->row;
  }
}

// Row-callback for blocking query.
template <>
void N1QL::RowCallback<BlockingQueryHandler>(lcb_t instance, int callback_type,
                                             const lcb_RESPN1QL *resp) {
  QueryHandler q_handler = n1ql_handle->qhandler_stack.Top();

  if (!(resp->rflags & LCB_RESP_F_FINAL)) {
    char *row_str;

#if defined(_WIN32) || defined(WIN32)
    WinSprintf(&row_str, "%.*s\n", static_cast<int>(resp->nrow), resp->row);
#else
    asprintf(&row_str, "%.*s\n", static_cast<int>(resp->nrow), resp->row);
#endif

    // Append the result to the rows vector.
    q_handler.block_handler->rows.push_back(std::string(row_str));

    free(row_str);
  } else {
    if (resp->rc != LCB_SUCCESS) {
      auto errors = n1ql_handle->ExtractErrorMsg(resp->row, q_handler.isolate);
      V8Worker::exception.Throw(instance, resp->rc, errors);
    }

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

// iter() function that is exposed to JavaScript.
void IterFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);

  try {
    // Make the hash of N1QL instance in JavaScript unique.
    std::string hash = SetUniqueHash(args);

    // Query to run.
    v8::Local<v8::Name> query_name = v8::String::NewFromUtf8(isolate, "query");
    v8::Local<v8::Value> query_value = args.This()->Get(query_name);
    v8::String::Utf8Value query_string(query_value);

    // Callback function to execute.
    v8::Local<v8::Function> func = v8::Local<v8::Function>::Cast(args[0]);

    // Prepare data for query execution.
    IterQueryHandler iter_handler;
    iter_handler.callback = func;
    QueryHandler q_handler;
    q_handler.hash = hash;
    q_handler.query = *query_string;
    q_handler.isolate = args.GetIsolate();
    q_handler.iter_handler = &iter_handler;

    n1ql_handle->ExecQuery<IterQueryHandler>(q_handler);

    // Add query metadata.
    v8::Local<v8::Object> n1ql_obj = args.This();
    AddQueryMetadata(iter_handler, isolate, n1ql_obj);
    args.This() = n1ql_obj;

    PopScopeStack(args);
  } catch (const char *e) {
    LOG(logError) << "Failed to set unique hash: " << e << '\n';
  }
}

// stopIter() function that is exposed to JavaScript.
void StopIterFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::EscapableHandleScope handle_scope(isolate);
  auto arg = v8::Local<v8::Object>::Cast(args[0]);

  try {
    // Get the unique hash for this object that was set by IterFunction.
    std::string hash = GetUniqueHash(args);

    // Cancel the query corresponding to the unique hash.
    QueryHandler *q_handler = n1ql_handle->qhandler_stack.Get(hash);
    lcb_t instance = q_handler->instance;
    lcb_N1QLHANDLE *handle = (lcb_N1QLHANDLE *)lcb_get_cookie(instance);
    lcb_n1ql_cancel(instance, *handle);

    // Bubble up the message sent from JavaScript.
    SetReturnValue(args, handle_scope.Escape(arg));
  } catch (const char *e) {
    LOG(logError) << "Failed to get unique hash: " << e << '\n';
  }
}

// execQuery() function that is exposed to JavaScript.
void ExecQueryFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handleScope(isolate);

  try {
    // Make the hash of N1QL instance in JavaScript unique.
    std::string hash = SetUniqueHash(args);

    // Query to run.
    v8::Local<v8::Name> query_name = v8::String::NewFromUtf8(isolate, "query");
    v8::Local<v8::Value> query_value = args.This()->Get(query_name);
    v8::String::Utf8Value query_string(query_value);

    // Prepare data for query execution.
    BlockingQueryHandler block_handler;
    QueryHandler q_handler;
    q_handler.hash = hash;
    q_handler.query = *query_string;
    q_handler.isolate = args.GetIsolate();
    q_handler.block_handler = &block_handler;

    n1ql_handle->ExecQuery<BlockingQueryHandler>(q_handler);

    std::vector<std::string> &rows = block_handler.rows;
    auto result_array = v8::Array::New(isolate, static_cast<int>(rows.size()));

    // Populate the result array with the rows of the result.
    for (std::string::size_type i = 0; i < rows.size(); ++i) {
      v8::Local<v8::Value> json_row =
          v8::JSON::Parse(v8::String::NewFromUtf8(isolate, rows[i].c_str()));
      result_array->Set(static_cast<uint32_t>(i), json_row);
    }

    AddQueryMetadata(block_handler, isolate, result_array);

    args.GetReturnValue().Set(result_array);
  } catch (const char *e) {
    LOG(logError) << "Failed to set unique hash: " << e << '\n';
  }
}

// Add return_obj as a private field on iterator.
void SetReturnValue(const v8::FunctionCallbackInfo<v8::Value> &args,
                    v8::Local<v8::Object> return_obj) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  // return_obj contains the following properties.
  const std::vector<std::string> props({"code", "args", "data"});

  for (std::string::size_type i = 0; i < props.size(); ++i) {
    auto key = v8::String::NewFromUtf8(isolate, props[i].c_str());
    auto value = return_obj->Get(key);
    auto private_key = v8::Private::ForApi(isolate, key);
    args.This()->SetPrivate(context, private_key, value);
  }

  // We also save return_obj iteself as a JavaScript object.
  auto key = v8::Private::ForApi(
      isolate, v8::String::NewFromUtf8(isolate, "return_value"));
  args.This()->SetPrivate(context, key, return_obj);
}

// getReturnValue([bool]) function exposed to JavaScript.
void GetReturnValueFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  // Get return_obj from private field.
  auto key = v8::Private::ForApi(
      isolate, v8::String::NewFromUtf8(isolate, "return_value"));
  auto return_value = ToLocal(args.This()->GetPrivate(context, key));

  auto do_concat = v8::Local<v8::Boolean>::Cast(args[0]);
  // Concatenate 'code' and 'args' fields if [bool] is true.
  // Else, return the return_obj as it is.
  if (do_concat->Value()) {
    auto code_key = v8::String::NewFromUtf8(isolate, "code");
    auto code_private_key = v8::Private::ForApi(isolate, code_key);
    auto code_value =
        ToLocal(args.This()->GetPrivate(context, code_private_key));

    auto args_key = v8::String::NewFromUtf8(isolate, "args");
    auto args_private_key = v8::Private::ForApi(isolate, args_key);
    auto args_value =
        ToLocal(args.This()->GetPrivate(context, args_private_key));

    auto return_value =
        v8::String::Concat(v8::Local<v8::String>::Cast(code_value),
                           v8::Local<v8::String>::Cast(args_value));
    args.GetReturnValue().Set(return_value);
  } else {
    args.GetReturnValue().Set(return_value);
  }
}

template <typename HandlerType, typename ResultType>
void AddQueryMetadata(HandlerType handler, v8::Isolate *isolate,
                      ResultType &result) {
  if (handler.metadata.length() > 0) {
    // Query metadata.
    auto metadata_key = v8::String::NewFromUtf8(isolate, "metadata");
    auto metadata = v8::String::NewFromUtf8(isolate, handler.metadata.c_str());
    auto metadata_json = v8::JSON::Parse(metadata);

    result->Set(metadata_key, metadata_json);
  }
}

std::string AppendStackIndex(int obj_hash) {
  std::string index_hash = std::to_string(obj_hash) + '|';
  index_hash += std::to_string(n1ql_handle->qhandler_stack.Size());

  return index_hash;
}

// Every N1qlQuery instance of JavaScript is associated with a private stack.
// This maintains the uniqueness of the hash of instances.
void PushScopeStack(const v8::FunctionCallbackInfo<v8::Value> &args,
                    std::string base_hash_str, std::string unique_hash_str) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  auto base_hash = v8::Private::ForApi(
      isolate, v8::String::NewFromUtf8(isolate, base_hash_str.c_str()));
  auto unique_hash = v8::String::NewFromUtf8(isolate, unique_hash_str.c_str());
  v8::Local<v8::Map> scope_stack;
  bool exists = HasKey(args, base_hash_str);

  // If a base hash exists, then get the stack and push the unique hash onto it.
  if (exists) {
    auto stack = ToLocal(args.This()->GetPrivate(context, base_hash));
    scope_stack = v8::Local<v8::Map>::Cast(stack);
    auto result = scope_stack->Set(
        context, v8::Number::New(isolate, scope_stack->Size()), unique_hash);
  } else {
    // Otherwise, create a new stack and push the base hash onto it.
    scope_stack = v8::Map::New(isolate);
    scope_stack = ToLocal(
        scope_stack->Set(context, v8::Number::New(isolate, 0), unique_hash));
    args.This()->SetPrivate(context, base_hash, scope_stack);
  }
}

// Pop the unique hash associated with the N1qlQuery instance.
void PopScopeStack(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  // Get base hash associated with this instance.
  bool exists;
  std::string base_hash = GetBaseHash(args, exists);

  if (exists) {
    auto hash_key = v8::Private::ForApi(
        isolate, v8::String::NewFromUtf8(isolate, base_hash.c_str()));
    exists = HasKey(args, base_hash);

    // If the base hash exists, then pop the unique hash from top of stack.
    if (exists) {
      auto stack = ToLocal(args.This()->GetPrivate(context, hash_key));
      auto scope_stack = v8::Local<v8::Map>::Cast(stack);
      auto result = scope_stack->Delete(
          context, v8::Number::New(isolate, scope_stack->Size() - 1));
    } else {
      throw "scope stack not set";
    }
  } else {
    throw "base hash not set";
  }
}

// Retrieve the unique hash associated with the N1qlQuery instance.
std::string GetUniqueHash(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  // Get base hash associated with this instance.
  bool exists;
  std::string base_hash_str = GetBaseHash(args, exists);

  if (exists) {
    auto base_hash = v8::Private::ForApi(
        isolate, v8::String::NewFromUtf8(isolate, base_hash_str.c_str()));
    exists = HasKey(args, base_hash_str);

    // If the base hash exists, then return the unique hash from top of stack.
    if (exists) {
      auto stack = ToLocal(args.This()->GetPrivate(context, base_hash));
      auto scope_stack = v8::Local<v8::Map>::Cast(stack);
      auto top_value = ToLocal(scope_stack->Get(
          context, v8::Number::New(isolate, scope_stack->Size() - 1)));
      v8::String::Utf8Value hash(top_value);
      return *hash;
    } else {
      throw "scope stack not set";
    }
  } else {
    throw "base hash not set";
  }

  return "";
}

// Generates and sets a unique hash to a N1qlQuery instance.
std::string SetUniqueHash(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  bool exists;
  std::string base_hash = GetBaseHash(args, exists);
  auto key =
      v8::Private::ForApi(isolate, v8::String::NewFromUtf8(isolate, "hash"));

  // If the base hash exists, then generate unique hash and push it onto stack.
  if (exists) {
    std::string unique_hash = AppendStackIndex(args.This()->GetIdentityHash());
    PushScopeStack(args, base_hash, unique_hash);

    return unique_hash;
  } else {
    // Otherwise, base hash is itself unique.
    base_hash = AppendStackIndex(args.This()->GetIdentityHash());
    auto hash = v8::String::NewFromUtf8(isolate, base_hash.c_str());
    args.This()->SetPrivate(context, key, hash);
    PushScopeStack(args, base_hash, base_hash);

    return base_hash;
  }
}

// Returns base hash from the field.
std::string GetBaseHash(const v8::FunctionCallbackInfo<v8::Value> &args,
                        bool &exists) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  exists = HasKey(args, "hash");
  if (exists) {
    auto key =
        v8::Private::ForApi(isolate, v8::String::NewFromUtf8(isolate, "hash"));
    auto value = ToLocal(args.This()->GetPrivate(context, key));
    v8::String::Utf8Value value_str(value);

    return ToCString(value_str);
  }

  return "";
}

// Check if a key is present in private field.
bool HasKey(const v8::FunctionCallbackInfo<v8::Value> &args,
            std::string key_str) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  auto key = v8::Private::ForApi(
      isolate, v8::String::NewFromUtf8(isolate, key_str.c_str()));
  v8::Maybe<bool> has_key = args.This()->HasPrivate(context, key);

  // Checking for FromJust would crash the v8 process if has_key is empty -
  // https://v8.paulfryzel.com/docs/master/classv8_1_1_maybe.html#a6c35f4870a5b5049d09ba5f13c67ede9
  bool result;
  try {
    result = !has_key.IsNothing() && has_key.IsJust() && has_key.FromJust();
  } catch (...) {
    LOG(logError) << "Key was empty " << '\n';
  }

  return result;
}

// Utility method to convert a MaybeLocal handle to a local handle.
template <typename T> v8::Local<T> ToLocal(const v8::MaybeLocal<T> &handle) {
  if (handle.IsEmpty()) {
    throw "Handle is empty";
  }

  v8::Isolate *isolate = v8::Isolate::GetCurrent();
  v8::EscapableHandleScope handle_scope(isolate);

  v8::Local<T> value;
  auto result = handle.ToLocal(&value);
  return handle_scope.Escape(value);
}
