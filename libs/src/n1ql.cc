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

#include <regex>

#include "isolate_data.h"
#include "js_exception.h"
#include "n1ql.h"
#include "retry_util.h"
#include "utils.h"

std::atomic<int64_t> n1ql_op_exception_count = {0};

const char *GetUsernameCached(void *cookie, const char *host, const char *port,
                              const char *bucket) {
  auto isolate = static_cast<v8::Isolate *>(cookie);
  auto comm = UnwrapData(isolate)->comm;
  auto endpoint = JoinHostPort(host, port);
  auto info = comm->GetCredsCached(endpoint);
  if (!info.is_valid) {
    LOG(logError) << "Failed to get username for " << RS(host) << ":" << port
                  << " err: " << info.msg << std::endl;
  }

  static const char *username = "";
  if (info.username != username) {
    username = strdup(info.username.c_str());
  }

  return username;
}

const char *GetPasswordCached(void *cookie, const char *host, const char *port,
                              const char *bucket) {
  auto isolate = static_cast<v8::Isolate *>(cookie);
  auto comm = UnwrapData(isolate)->comm;
  auto endpoint = JoinHostPort(host, port);
  auto info = comm->GetCredsCached(endpoint);
  if (!info.is_valid) {
    LOG(logError) << "Failed to get password for " << RS(host) << ":" << port
                  << " err: " << info.msg << std::endl;
  }

  static const char *password = "";
  if (info.password != password) {
    password = strdup(info.password.c_str());
  }

  return password;
}

ConnectionPool::ConnectionPool(v8::Isolate *isolate, int capacity,
                               std::string cb_kv_endpoint,
                               std::string cb_source_bucket)
    : capacity_(capacity), inst_count_(0), isolate_(isolate) {

  conn_str_ = "couchbase://" + cb_kv_endpoint + "/" + cb_source_bucket +
              "?select_bucket=true";
  if (IsIPv6()) {
    conn_str_ += "&ipv6=allow";
  }
}

// Creates and adds one lcb instance into the pool.
void ConnectionPool::AddResource() {
  // Initialization of lcb instances pool.
  auto init_success = true;
  lcb_create_st options;
  lcb_error_t err;
  memset(&options, 0, sizeof(options));
  options.version = 3;
  options.v.v3.connstr = conn_str_.c_str();
  options.v.v3.type = LCB_TYPE_BUCKET;

  lcb_t instance = nullptr;
  err = lcb_create(&instance, &options);
  if (err != LCB_SUCCESS) {
    init_success = false;
    Error(instance, "N1QL: Unable to create lcb handle", err);
  }

  bool enableDetailedErrCodes = true;
  err = lcb_cntl(instance, LCB_CNTL_SET, LCB_CNTL_DETAILED_ERRCODES,
                 &enableDetailedErrCodes);
  if (err != LCB_SUCCESS) {
    LOG(logWarning) << "N1QL: Unable to set detailed error codes. Defaulting "
                       "to normal error codes"
                    << std::endl;
  }

  auto auth = lcbauth_new();
  err = lcbauth_set_callbacks(auth, isolate_, GetUsernameCached,
                              GetPasswordCached);
  if (err != LCB_SUCCESS) {
    init_success = false;
    Error(instance, "N1QL: Unable to set auth callbacks", err);
  }

  err = lcbauth_set_mode(auth, LCBAUTH_MODE_DYNAMIC);
  if (err != LCB_SUCCESS) {
    init_success = false;
    Error(instance, "N1QL: Unable to set auth mode to dynamic", err);
  }

  lcb_set_auth(instance, auth);

  err = lcb_connect(instance);
  if (err != LCB_SUCCESS) {
    init_success = false;
    Error(instance, "N1QL: Unable to connect to server", err);
  }

  err = lcb_wait(instance);
  if (err != LCB_SUCCESS) {
    init_success = false;
    Error(instance, "N1QL: Unable to schedule op to connect to server", err);
  }

  err = lcb_get_bootstrap_status(instance);
  if (err != LCB_SUCCESS) {
    init_success = false;
    Error(instance, "N1QL: Unable to get bootstrap status", err);
  }

  ++inst_count_;
  instances_.push(instance);
  if (init_success) {
    LOG(logInfo) << "N1QL: lcb instance successfully initialized for "
                 << RS(conn_str_) << std::endl;
  } else {
    LOG(logError) << "N1QL: Unable to initialize lcb instance for "
                  << RS(conn_str_) << std::endl;
  }
}

lcb_t ConnectionPool::GetResource() {
  // Dynamically expand the pool size if it's within the pool capacity.
  if (instances_.empty()) {
    if (inst_count_ >= capacity_) {
      throw "N1QL: Maximum pool capacity reached";
    } else {
      AddResource();
    }
  }

  lcb_t instance = instances_.front();
  instances_.pop();
  return instance;
}

void ConnectionPool::Error(lcb_t instance, const char *msg, lcb_error_t err) {
  LOG(logError) << msg << " " << lcb_strerror(instance, err) << std::endl;
}

ConnectionPool::~ConnectionPool() {
  while (!instances_.empty()) {
    lcb_t instance = instances_.front();
    if (instance) {
      lcb_destroy(instance);
    }

    instances_.pop();
  }
}

void HashedStack::Push(QueryHandler &q_handler) {
  qstack_.push(q_handler);
  qmap_[q_handler.hash] = &q_handler;
}

void HashedStack::Pop() {
  auto it = qmap_.find(qstack_.top().hash);
  qmap_.erase(it);
  qstack_.pop();
}

// Row-callback for iterator.
template <>
void N1QL::RowCallback<IterQueryHandler>(lcb_t instance, int callback_type,
                                         const lcb_RESPN1QL *resp) {
  auto cookie = (HandlerCookie *)lcb_get_cookie(instance);
  auto isolate = cookie->isolate;
  auto isolate_data = UnwrapData(isolate);
  auto n1ql_handle = isolate_data->n1ql_handle;
  auto q_handler = n1ql_handle->qhandler_stack.Top();
  v8::HandleScope handle_scope(isolate);

  if (!(resp->rflags & LCB_RESP_F_FINAL)) {
    char *row_str;

#if defined(_WIN32) || defined(WIN32)
    WinSprintf(&row_str, "%.*s\n", static_cast<int>(resp->nrow), resp->row);
#else
    asprintf(&row_str, "%.*s\n", static_cast<int>(resp->nrow), resp->row);
#endif

    v8::Local<v8::Value> args[1];
    args[0] = v8::JSON::Parse(v8Str(isolate, row_str));

    // Execute the function callback passed in JavaScript.
    auto callback = q_handler.iter_handler->callback;
    v8::TryCatch try_catch(isolate);
    RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                          IsTerminatingRetriable, IsExecutionTerminating,
                          isolate);

    callback->Call(callback, 1, args);
    if (try_catch.HasCaught()) {
      // Cancel the query if an exception was thrown and re-throw the exception.
      lcb_n1ql_cancel(instance, cookie->handle);
      try_catch.ReThrow();
    }

    free(row_str);
  } else {
    if (resp->rc != LCB_SUCCESS || !IsStatusSuccess(resp->row)) {
      HandleRowCallbackFailure(resp, isolate, cookie);
    }

    q_handler.iter_handler->metadata = resp->row;
  }
}

// Row-callback for blocking query.
template <>
void N1QL::RowCallback<BlockingQueryHandler>(lcb_t instance, int callback_type,
                                             const lcb_RESPN1QL *resp) {
  auto cookie = (HandlerCookie *)lcb_get_cookie(instance);
  auto isolate = cookie->isolate;
  auto isolate_data = UnwrapData(isolate);
  auto n1ql_handle = isolate_data->n1ql_handle;
  auto q_handler = n1ql_handle->qhandler_stack.Top();

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
    if (resp->rc != LCB_SUCCESS || !IsStatusSuccess(resp->row)) {
      HandleRowCallbackFailure(resp, isolate, cookie);
    }

    q_handler.block_handler->metadata = resp->row;
  }
}

void N1QL::HandleRowCallbackFailure(const lcb_RESPN1QL *resp,
                                    v8::Isolate *isolate,
                                    HandlerCookie *cookie) {
  n1ql_op_exception_count++;
  const auto isolate_data = UnwrapData(isolate);
  auto js_exception = isolate_data->js_exception;
  auto codex = isolate_data->n1ql_codex;

  cookie->error = resp->row;
  N1QLErrorExtractor extractor(isolate);
  const auto info = extractor.GetErrorCodes(resp->row);
  if (info.is_fatal) {
    js_exception->ThrowN1QLError(info.msg);
    return;
  }

  for (const auto code : info.errors) {
    if (codex->IsRetriable(code)) {
      cookie->must_retry = true;
    }
    AddLcbException(isolate_data, static_cast<int>(code));
  }

  if (!cookie->must_retry) {
    js_exception->ThrowN1QLError(resp->row);
  }

  if (resp->rc == LCB_AUTH_ERROR || cookie->must_retry) {
    auto comm = isolate_data->comm;
    comm->Refresh();
  }
}

template <typename HandlerType> void N1QL::ExecQuery(QueryHandler &q_handler) {
  q_handler.instance = inst_pool_->GetResource();

  // Schedule the data to support query.
  qhandler_stack.Push(q_handler);

  lcb_t &instance = q_handler.instance;
  lcb_error_t err;

  lcb_N1QLPARAMS *n1ql_params = lcb_n1p_new();
  err = lcb_n1p_setstmtz(n1ql_params, q_handler.query.c_str());
  if (err != LCB_SUCCESS) {
    ConnectionPool::Error(instance, "N1QL: Unable to build query string", err);
  }

  for (const auto &param : *q_handler.named_params) {
    err = lcb_n1p_namedparamz(n1ql_params, param.first.c_str(),
                              param.second.c_str());
    if (err != LCB_SUCCESS) {
      ConnectionPool::Error(instance, "N1QL: Unable to set named parameters",
                            err);
    }
  }

  std::string err_msg;
  auto must_retry = RetryWithFixedBackoff(
      5, 100, [](bool retry) -> bool { return retry; },
      ExecQueryImpl<HandlerType>, isolate_, instance, n1ql_params, err_msg);
  if (must_retry) {
    auto js_exception = UnwrapData(isolate_)->js_exception;
    js_exception->ThrowN1QLError(
        "Query did not succeed even after 5 attempts, error : " + err_msg);
  }

  // Resource clean-up.
  lcb_n1p_free(n1ql_params);
  lcb_set_cookie(instance, nullptr);
  qhandler_stack.Pop();
  inst_pool_->Restore(instance);
}

template <typename HandlerType>
bool N1QL::ExecQueryImpl(v8::Isolate *isolate, lcb_t &instance,
                         lcb_N1QLPARAMS *n1ql_params, std::string &err_out) {
  lcb_CMDN1QL cmd = {0};
  lcb_N1QLHANDLE handle = nullptr;
  cmd.handle = &handle;
  cmd.callback = RowCallback<HandlerType>;

  lcb_n1p_mkcmd(n1ql_params, &cmd);

  auto err = lcb_n1ql_query(instance, nullptr, &cmd);
  if (err != LCB_SUCCESS) {
    // for example: when there is no query node
    ConnectionPool::Error(instance, "N1QL: Unable to schedule N1QL query", err);
    auto js_exception = UnwrapData(isolate)->js_exception;
    js_exception->ThrowN1QLError("N1QL: Unable to schedule N1QL query");
  }

  HandlerCookie cookie;
  cookie.isolate = isolate;
  // Add the N1QL handle as cookie - allow for query cancellation.
  cookie.handle = handle;
  lcb_set_cookie(instance, &cookie);

  cookie.must_retry = false;
  err = lcb_wait(instance);
  if (err != LCB_SUCCESS) {
    ConnectionPool::Error(instance, "N1QL: Query execution failed", err);
  }
  err_out = cookie.error;
  return cookie.must_retry;
}

bool N1QL::IsStatusSuccess(const char *row) {
  std::regex re_status_success(R"("status"\s*:\s*"success")");
  return std::regex_search(row, re_status_success);
}

std::unordered_map<std::string, std::string>
ExtractNamedParams(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  std::unordered_map<std::string, std::string> named_params;
  v8::Local<v8::Value> options_val;
  if (!TO_LOCAL(args.This()->Get(context, v8Str(isolate, "options")),
                &options_val)) {
    return named_params;
  }

  v8::Local<v8::Object> options_obj;
  if (!TO_LOCAL(options_val->ToObject(context), &options_obj)) {
    return named_params;
  }

  v8::Local<v8::Value> named_params_val;
  if (!TO_LOCAL(options_obj->Get(context, v8Str(isolate, "namedParams")),
                &named_params_val)) {
    return named_params;
  }

  v8::Local<v8::Object> named_params_obj;
  if (!TO_LOCAL(named_params_val->ToObject(context), &named_params_obj)) {
    return named_params;
  }

  v8::Local<v8::Array> named_params_arr;
  if (!TO_LOCAL(named_params_obj->GetPropertyNames(context),
                &named_params_arr)) {
    return named_params;
  }

  for (uint32_t i = 0; i < named_params_arr->Length(); ++i) {
    v8::Local<v8::Value> key;
    if (!TO_LOCAL(named_params_arr->Get(context, i), &key)) {
      return named_params;
    }

    v8::Local<v8::Value> value;
    if (!TO_LOCAL(named_params_obj->Get(context, key), &value)) {
      return named_params;
    }

    v8::String::Utf8Value key_utf8(key);
    v8::String::Utf8Value val_utf8(value);
    named_params[*key_utf8] = *val_utf8;
  }

  return named_params;
}

// iter() function that is exposed to JavaScript.
void IterFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::Locker locker(isolate);
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  try {
    // Make the hash of N1QL instance in JavaScript unique.
    auto hash = SetUniqueHash(args);

    // Query to run.
    v8::Local<v8::Value> query;
    if (!TO_LOCAL(args.This()->Get(context, v8Str(isolate, "query")), &query)) {
      return;
    }

    v8::String::Utf8Value query_string(query);
    auto named_params = ExtractNamedParams(args);

    // Callback function to execute.
    auto func = args[0].As<v8::Function>();

    // Prepare data for query execution.
    IterQueryHandler iter_handler;
    iter_handler.callback = func;
    QueryHandler q_handler;
    q_handler.hash = hash;
    q_handler.query = *query_string;
    q_handler.named_params = &named_params;
    q_handler.iter_handler = &iter_handler;

    auto n1ql_handle = UnwrapData(isolate)->n1ql_handle;
    n1ql_handle->ExecQuery<IterQueryHandler>(q_handler);

    // Add query metadata.
    auto n1ql_obj = args.This();
    AddQueryMetadata(iter_handler, isolate, n1ql_obj);
    args.This() = n1ql_obj;

    PopScopeStack(args);
  } catch (const char *e) {
    LOG(logError) << e << std::endl;
    ++n1ql_op_exception_count;
    auto js_exception = UnwrapData(isolate)->js_exception;
    js_exception->ThrowN1QLError(e);
  }
}

// stopIter() function that is exposed to JavaScript.
void StopIterFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::EscapableHandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  v8::Local<v8::Object> arg;
  if (!TO_LOCAL(args[0]->ToObject(context), &arg)) {
    return;
  }

  try {
    // Get the unique hash for this object that was set by
    // IterFunction.
    auto hash = GetUniqueHash(args);

    auto n1ql_handle = UnwrapData(isolate)->n1ql_handle;
    // Cancel the query corresponding to the unique hash.
    auto q_handler = n1ql_handle->qhandler_stack.Get(hash);
    auto instance = q_handler->instance;
    auto cookie = (HandlerCookie *)lcb_get_cookie(instance);
    lcb_n1ql_cancel(instance, cookie->handle);

    // Bubble up the message sent from JavaScript.
    SetReturnValue(args, handle_scope.Escape(arg));
  } catch (const char *e) {
    LOG(logError) << e << std::endl;
    auto js_exception = UnwrapData(isolate)->js_exception;
    js_exception->ThrowN1QLError(e);
  }
}

// execQuery() function that is exposed to JavaScript.
void ExecQueryFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::Locker locker(isolate);
  v8::HandleScope handleScope(isolate);
  auto context = isolate->GetCurrentContext();

  try {
    // Make the hash of N1QL instance in JavaScript unique.
    auto hash = SetUniqueHash(args);

    // Query to run.
    v8::Local<v8::Value> query;
    if (!TO_LOCAL(args.This()->Get(context, v8Str(isolate, "query")), &query)) {
      return;
    }

    v8::String::Utf8Value query_string(query);
    auto named_params = ExtractNamedParams(args);

    // Prepare data for query execution.
    BlockingQueryHandler block_handler;
    QueryHandler q_handler;
    q_handler.hash = hash;
    q_handler.query = *query_string;
    q_handler.named_params = &named_params;
    q_handler.block_handler = &block_handler;

    auto n1ql_handle = UnwrapData(isolate)->n1ql_handle;
    n1ql_handle->ExecQuery<BlockingQueryHandler>(q_handler);

    auto &rows = block_handler.rows;
    auto result_array = v8::Array::New(isolate, static_cast<int>(rows.size()));

    // Populate the result array with the rows of the result.
    for (std::string::size_type i = 0; i < rows.size(); ++i) {
      v8::Local<v8::Value> json_row;
      if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate, rows[i])),
                    &json_row)) {
        return;
      }

      result_array->Set(static_cast<uint32_t>(i), json_row);
    }

    AddQueryMetadata(block_handler, isolate, result_array);

    args.GetReturnValue().Set(result_array);
  } catch (const char *e) {
    LOG(logError) << e << std::endl;
    ++n1ql_op_exception_count;
    auto js_exception = UnwrapData(isolate)->js_exception;
    js_exception->ThrowN1QLError(e);
  }
}

// Add return_obj as a private field on iterator.
// TODO : check if it's safe to turn return_obj into a const reference
inline void SetReturnValue(const v8::FunctionCallbackInfo<v8::Value> &args,
                           v8::Local<v8::Object> return_obj) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  // return_obj contains the following properties.
  const std::vector<std::string> props{"code", "args", "data"};

  for (const auto &prop : props) {
    auto key = v8Str(isolate, prop);
    v8::Local<v8::Value> value;
    if (!TO_LOCAL(return_obj->Get(context, key), &value)) {
      return;
    }

    auto private_key = v8::Private::ForApi(isolate, key);
    args.This()->SetPrivate(context, private_key, value);
  }

  // We also save return_obj iteself as a JavaScript object.
  auto key = v8::Private::ForApi(isolate, v8Str(isolate, "return_value"));
  args.This()->SetPrivate(context, key, return_obj);
}

// getReturnValue([bool]) function exposed to JavaScript.
void GetReturnValueFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  // Get return_obj from private field.
  auto key = v8::Private::ForApi(isolate, v8Str(isolate, "return_value"));
  auto return_value = ToLocal(args.This()->GetPrivate(context, key));

  auto do_concat = args[0].As<v8::Boolean>();
  // Concatenate 'code' and 'args' fields if [bool] is true.
  // Else, return the return_obj as it is.
  if (do_concat->Value()) {
    auto code_key = v8Str(isolate, "code");
    auto code_private_key = v8::Private::ForApi(isolate, code_key);
    auto code_value =
        ToLocal(args.This()->GetPrivate(context, code_private_key));

    auto args_key = v8Str(isolate, "args");
    auto args_private_key = v8::Private::ForApi(isolate, args_key);
    auto args_value =
        ToLocal(args.This()->GetPrivate(context, args_private_key));

    auto return_value = v8::String::Concat(code_value.As<v8::String>(),
                                           args_value.As<v8::String>());
    args.GetReturnValue().Set(return_value);
  } else {
    args.GetReturnValue().Set(return_value);
  }
}

template <typename HandlerType, typename ResultType>
void AddQueryMetadata(HandlerType handler, v8::Isolate *isolate,
                      ResultType &result) {
  if (handler.metadata.length() > 0) {
    v8::HandleScope handle_scope(isolate);
    auto context = isolate->GetCurrentContext();

    // Query metadata.
    v8::Local<v8::Value> metadata_json;
    if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate, handler.metadata)),
                  &metadata_json)) {
      return;
    }

    result->Set(v8Str(isolate, "metadata"), metadata_json);
  }
}

std::string AppendStackIndex(int obj_hash, v8::Isolate *isolate) {
  auto n1ql_handle = UnwrapData(isolate)->n1ql_handle;
  std::string index_hash = std::to_string(obj_hash) + "|";
  index_hash += std::to_string(n1ql_handle->qhandler_stack.Size());

  return index_hash;
}

// Every N1qlQuery instance of JavaScript is associated with a private stack.
// This maintains the uniqueness of the hash of instances.
void PushScopeStack(const v8::FunctionCallbackInfo<v8::Value> &args,
                    std::string base_hash_str, std::string unique_hash_str) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  auto base_hash =
      v8::Private::ForApi(isolate, v8Str(isolate, base_hash_str.c_str()));
  auto unique_hash = v8Str(isolate, unique_hash_str.c_str());
  bool exists = HasKey(args, base_hash_str);

  // If a base hash exists, then get the stack and push the unique hash
  // onto it.
  if (exists) {
    auto stack = ToLocal(args.This()->GetPrivate(context, base_hash));
    auto scope_stack = stack.As<v8::Map>();
    auto result = scope_stack->Set(
        context, v8::Number::New(isolate, scope_stack->Size()), unique_hash);
    if (result.IsEmpty()) {
      throw "Unable to set scope stack";
    }
  } else {
    // Otherwise, create a new stack and push the base hash onto it.
    auto scope_stack = v8::Map::New(isolate);
    scope_stack = ToLocal(
        scope_stack->Set(context, v8::Number::New(isolate, 0), unique_hash));
    args.This()->SetPrivate(context, base_hash, scope_stack);
  }
}

// Pop the unique hash associated with the N1qlQuery instance.
void PopScopeStack(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  // Get base hash associated with this instance.
  bool exists;
  auto base_hash = GetBaseHash(args, exists);

  if (exists) {
    auto hash_key = v8::Private::ForApi(isolate, v8Str(isolate, base_hash));
    exists = HasKey(args, base_hash);

    // If the base hash exists, then pop the unique hash from top of stack.
    if (exists) {
      auto stack = ToLocal(args.This()->GetPrivate(context, hash_key));
      auto scope_stack = stack.As<v8::Map>();
      auto result = scope_stack->Delete(
          context, v8::Number::New(isolate, scope_stack->Size() - 1));
      if (result.IsNothing()) {
        throw "N1QL: Unable to delete from scope stack";
      }
    } else {
      throw "N1QL: Scope stack not set";
    }
  } else {
    throw "N1QL: Base hash not set";
  }
}

// Retrieve the unique hash associated with the N1qlQuery instance.
std::string GetUniqueHash(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  // Get base hash associated with this instance.
  bool exists;
  auto base_hash_str = GetBaseHash(args, exists);

  if (exists) {
    auto base_hash =
        v8::Private::ForApi(isolate, v8Str(isolate, base_hash_str));
    exists = HasKey(args, base_hash_str);

    // If the base hash exists, then return the unique hash from top
    // of stack.
    if (exists) {
      auto stack = ToLocal(args.This()->GetPrivate(context, base_hash));
      auto scope_stack = stack.As<v8::Map>();
      auto top_value = ToLocal(scope_stack->Get(
          context, v8::Number::New(isolate, scope_stack->Size() - 1)));
      v8::String::Utf8Value hash(top_value);
      return *hash;
    } else {
      throw "N1QL: Scope stack not set";
    }
  } else {
    throw "N1QL: Base hash not set";
  }
}

// Generates and sets a unique hash to a N1qlQuery instance.
std::string SetUniqueHash(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  bool exists;
  auto base_hash = GetBaseHash(args, exists);

  // If the base hash exists, then generate unique hash and push it onto
  // stack.
  if (exists) {
    auto unique_hash =
        AppendStackIndex(args.This()->GetIdentityHash(), isolate);
    PushScopeStack(args, base_hash, unique_hash);

    return unique_hash;
  } else {
    // Otherwise, base hash is itself unique.
    base_hash = AppendStackIndex(args.This()->GetIdentityHash(), isolate);
    auto hash = v8Str(isolate, base_hash);
    auto key = v8::Private::ForApi(isolate, v8Str(isolate, "hash"));
    args.This()->SetPrivate(context, key, hash);
    PushScopeStack(args, base_hash, base_hash);

    return base_hash;
  }
}

// Returns base hash from the field.
std::string GetBaseHash(const v8::FunctionCallbackInfo<v8::Value> &args,
                        bool &exists) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  exists = HasKey(args, "hash");
  if (exists) {
    auto key = v8::Private::ForApi(isolate, v8Str(isolate, "hash"));
    auto value = ToLocal(args.This()->GetPrivate(context, key));
    v8::String::Utf8Value value_str(value);
    return *value_str;
  }

  return "";
}

// Check if a key is present in private field.
bool HasKey(const v8::FunctionCallbackInfo<v8::Value> &args,
            std::string key_str) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  auto key = v8::Private::ForApi(isolate, v8Str(isolate, key_str));
  v8::Maybe<bool> has_key = args.This()->HasPrivate(context, key);

  // Checking for FromJust would crash the v8 process if has_key is empty
  // https://v8.paulfryzel.com/docs/master/classv8_1_1_maybe.html#a6c35f4870a5b5049d09ba5f13c67ede9
  if (has_key.IsNothing()) {
    throw "N1QL: Key was empty";
  }

  return !has_key.IsNothing() && has_key.IsJust() && has_key.FromJust();
}

// Utility method to convert a MaybeLocal handle to a local handle.
template <typename T> v8::Local<T> ToLocal(const v8::MaybeLocal<T> &handle) {
  if (handle.IsEmpty()) {
    throw "N1QL: Handle is empty";
  }

  auto isolate = v8::Isolate::GetCurrent();
  v8::EscapableHandleScope handle_scope(isolate);

  v8::Local<T> value;
  auto result = handle.ToLocal(&value);
  if (!result) {
    LOG(logError) << "N1QL: handle.ToLocal failed" << std::endl;
  }

  return handle_scope.Escape(value);
}

N1QLErrorExtractor::N1QLErrorExtractor(v8::Isolate *isolate)
    : isolate_(isolate) {
  v8::HandleScope handle_scope(isolate_);
  auto context = isolate_->GetCurrentContext();
  context_.Reset(isolate_, context);
}

N1QLErrorExtractor::~N1QLErrorExtractor() { context_.Reset(); }

ErrorCodesInfo N1QLErrorExtractor::GetErrorCodes(const char *err_str) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  std::vector<int64_t> errors;

  v8::Local<v8::Value> error_val;
  if (!TO_LOCAL(v8::JSON::Parse(isolate_, v8Str(isolate_, err_str)),
                &error_val)) {
    return {true, "Unable to parse error JSON"};
  }

  v8::Local<v8::Object> error_obj;
  if (!TO_LOCAL(error_val->ToObject(context), &error_obj)) {
    return {true, "Unable to cast error to Object"};
  }

  v8::Local<v8::Value> errors_val;
  if (!TO_LOCAL(error_obj->Get(context, v8Str(isolate_, "errors")),
                &errors_val)) {
    return {true, "Unable to read errors property from message Object"};
  }
  return GetErrorCodes(errors_val);
}

ErrorCodesInfo
N1QLErrorExtractor::GetErrorCodes(const v8::Local<v8::Value> &errors_val) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  std::vector<int64_t> errors;

  auto errors_v8arr = errors_val.As<v8::Array>();
  const auto len = errors_v8arr->Length();
  errors.resize(static_cast<std::size_t>(len));
  for (uint32_t i = 0; i < len; ++i) {
    v8::Local<v8::Value> error_val;
    if (!TO_LOCAL(errors_v8arr->Get(context, i), &error_val)) {
      return {true,
              "Unable to read error Object at index " + std::to_string(i)};
    }

    v8::Local<v8::Object> error_obj;
    if (!TO_LOCAL(error_val->ToObject(context), &error_obj)) {
      return {true, "Unable to cast error at index " + std::to_string(i) +
                        " to Object"};
    }

    v8::Local<v8::Value> code_val;
    if (!TO_LOCAL(error_obj->Get(context, v8Str(isolate_, "code")),
                  &code_val)) {
      return {true, "Unable to get code from error Object at index " +
                        std::to_string(i)};
    }

    v8::Local<v8::Integer> code_v8int;
    if (!TO_LOCAL(code_val->ToInteger(context), &code_v8int)) {
      return {true, "Unable to cast code to integer in error Object at index " +
                        std::to_string(i)};
    }
    errors[static_cast<std::size_t>(i)] = code_v8int->Value();
  }
  return errors;
}
