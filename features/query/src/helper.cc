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

#include <algorithm>
#include <libcouchbase/n1ql.h>
#include <memory>
#include <sstream>
#include <string>
#include <v8.h>
#include <vector>

#include "js_exception.h"
#include "query-helper.h"
#include "utils.h"

extern std::atomic<int64_t> n1ql_op_exception_count;

Query::Helper::Helper(v8::Isolate *isolate,
                      const v8::Local<v8::Context> &context)
    : isolate_(isolate), opt_extractor_(isolate, context) {
  context_.Reset(isolate_, context);
}
Query::Helper::~Helper() { context_.Reset(); }

::Info
Query::Helper::ValidateQuery(const v8::FunctionCallbackInfo<v8::Value> &args) {
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

Query::Info
Query::Helper::CreateQuery(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::HandleScope handle_scope(isolate_);
  Query::Info query_info;

  v8::String::Utf8Value query_utf8(isolate_, args[0]);
  query_info.query = *query_utf8;
  if (args.Length() < 2) {
    return query_info;
  }

  if (args[1]->IsArray()) {
    if (auto info = GetPosParams(args[1]); info.is_fatal) {
      return {true, info.msg};
    } else {
      std::swap(query_info.pos_params, info.pos_params);
    }
  } else if (args[1]->IsObject()) {
    if (auto info = GetNamedParams(args[1]); info.is_fatal) {
      return {true, info.msg};
    } else {
      std::swap(query_info.named_params, info.named_params);
    }
  }

  Query::Options options;
  if (auto info = opt_extractor_.Extract(args, options); info.is_fatal) {
    return {true, info.msg};
  } else {
    std::swap(query_info.options, options);
  }
  return query_info;
}

Query::Helper::NamedParamsInfo
Query::Helper::GetNamedParams(const v8::Local<v8::Value> &arg) const {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  v8::Local<v8::Object> named_params_obj;
  if (!TO_LOCAL(arg->ToObject(context), &named_params_obj)) {
    return {true, "Unable to cast second parameter to object"};
  }

  v8::Local<v8::Array> named_params_keys;
  if (!TO_LOCAL(named_params_obj->GetPropertyNames(context),
                &named_params_keys)) {
    return {true, "Unable to get the key collection in the second parameter"};
  }

  std::unordered_map<std::string, std::string> named_params;
  for (uint32_t i = 0, len = named_params_keys->Length(); i < len; ++i) {
    v8::Local<v8::Value> key;
    if (!TO_LOCAL(named_params_keys->Get(context, i), &key)) {
      return {true, "Unable to get key from the second parameter"};
    }
    if (auto info = Utils::ValidateDataType(key); info.is_fatal) {
      return {true, "Invalid data type for named parameters: " + info.msg};
    }

    v8::Local<v8::Value> value;
    if (!TO_LOCAL(named_params_obj->Get(context, key), &value)) {
      return {true, "Unable to get value from the second parameter"};
    }
    if (auto info = Utils::ValidateDataType(value); info.is_fatal) {
      return {true, "Invalid data type for named parameters: " + info.msg};
    }
    v8::String::Utf8Value key_utf8(isolate_, key);
    named_params[*key_utf8] = JSONStringify(isolate_, value);
  }
  return {named_params};
}

Query::Helper::PosParamsInfo
Query::Helper::GetPosParams(const v8::Local<v8::Value> &arg) const {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  std::stringstream error;

  std::vector<std::string> pos_params;
  auto params_v8arr = arg.As<v8::Array>();
  const auto len = params_v8arr->Length();
  pos_params.reserve(static_cast<std::size_t>(len));

  for (uint32_t i = 0; i < len; ++i) {
    v8::Local<v8::Value> param_val;
    if (!TO_LOCAL(params_v8arr->Get(context, i), &param_val)) {
      error << "Unable to read parameter at index " << i;
      return {true, error.str()};
    }
    if (auto info = Utils::ValidateDataType(param_val); info.is_fatal) {
      error << "Invalid data type at index " << i
            << " for positional parameters: " << info.msg;
      return {true, error.str()};
    }
    pos_params.emplace_back(JSONStringify(isolate_, param_val));
  }
  return {pos_params};
}

::Info Query::Helper::AccountLCBError(const std::string &err_str) {
  auto info = GetErrorCodes(err_str);
  if (info.is_fatal) {
    return {true, info.msg};
  }

  auto isolate_data = UnwrapData(isolate_);
  for (const auto &err_code : info.err_codes) {
    AddLcbException(isolate_data, static_cast<int>(err_code));
  }
  return {false};
}

Query::Helper::ErrorCodesInfo
Query::Helper::GetErrorCodes(const v8::Local<v8::Value> &errors_val) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  std::vector<int64_t> errors;
  std::stringstream error;

  auto errors_v8arr = errors_val.As<v8::Array>();
  const auto len = errors_v8arr->Length();
  errors.resize(static_cast<std::size_t>(len));
  for (uint32_t i = 0; i < len; ++i) {
    v8::Local<v8::Value> error_val;
    if (!TO_LOCAL(errors_v8arr->Get(context, i), &error_val)) {
      error << "Unable to read error Object at index " << i;
      return {true, error.str()};
    }

    v8::Local<v8::Object> error_obj;
    if (!TO_LOCAL(error_val->ToObject(context), &error_obj)) {
      error << "Unable to cast error at index " << i << " to Object";
      return {true, error.str()};
    }

    v8::Local<v8::Value> code_val;
    if (!TO_LOCAL(error_obj->Get(context, v8Str(isolate_, "code")),
                  &code_val)) {
      error << "Unable to get code from error Object at index " << i;
      return {true, error.str()};
    }

    v8::Local<v8::Integer> code_v8int;
    if (!TO_LOCAL(code_val->ToInteger(context), &code_v8int)) {
      error << "Unable to cast code to integer in error Object at index " << i;
      return {true, error.str()};
    }
    errors[static_cast<std::size_t>(i)] = code_v8int->Value();
  }
  return errors;
}

Query::Helper::ErrorCodesInfo
Query::Helper::GetErrorCodes(const std::string &error) {
  if (error.empty()) {
    return {true, "error message is empty"};
  }

  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  std::vector<int64_t> errors;
  std::stringstream msg;

  v8::Local<v8::Value> error_val;
  if (!TO_LOCAL(v8::JSON::Parse(isolate_, v8Str(isolate_, error)),
                &error_val)) {
    msg << "Unable to parse error JSON : " << RU(error);
    return {true, msg.str()};
  }

  v8::Local<v8::Object> error_obj;
  if (!TO_LOCAL(error_val->ToObject(context), &error_obj)) {
    msg << "Unable to cast error to Object : " << RU(error);
    return {true, msg.str()};
  }

  v8::Local<v8::Value> errors_val;
  if (!TO_LOCAL(error_obj->Get(context, v8Str(isolate_, "errors")),
                &errors_val)) {
    msg << "Unable to read errors property from message Object : " << RU(error);
    return {true, msg.str()};
  }
  return GetErrorCodes(errors_val);
}

void Query::Helper::AccountLCBError(int err_code) {
  auto isolate_data = UnwrapData(isolate_);
  AddLcbException(isolate_data, err_code);
}

void Query::Helper::HandleRowError(const Query::Row &row) {
  auto comm = UnwrapData(isolate_)->comm;
  auto js_exception = UnwrapData(isolate_)->js_exception;

  ++n1ql_op_exception_count;
  // TODO : Refresh for server auth error also
  if (row.is_client_auth_error) {
    comm->Refresh();
  }

  std::stringstream err_msg;
  // Error reported by RowCallback (coming from LCB client)
  if (row.is_client_error) {
    AccountLCBError(row.err_code);
    err_msg << "SDK error : " << row.client_error;
  }

  // Error reported by RowCallback (coming from query server)
  if (row.is_query_error) {
    err_msg << " Query error : " << row.query_error;
    if (auto acc_info = AccountLCBError(row.query_error); acc_info.is_fatal) {
      err_msg << " Error accounting LCB : " << acc_info.msg;
    }
  }
  js_exception->ThrowN1QLError(err_msg.str());
}

std::string Query::Helper::ErrorFormat(const std::string &message,
                                       lcb_t connection,
                                       const lcb_error_t error) {
  AccountLCBError(error);
  std::stringstream formatter;
  formatter << message << " : " << lcb_strerror(connection, error);
  return formatter.str();
}

int Query::Helper::GetConsistency(const std::string &consistency) {
  if (consistency == "none") {
    return LCB_N1P_CONSISTENCY_NONE;
  }
  if (consistency == "request") {
    return LCB_N1P_CONSISTENCY_REQUEST;
  }
  return LCB_N1P_CONSISTENCY_NONE;
}

Query::Options::Extractor::Extractor(v8::Isolate *isolate,
                                     const v8::Local<v8::Context> &context)
    : isolate_(isolate) {
  v8::HandleScope handle_scope(isolate_);
  context_.Reset(isolate_, context);
  consistency_property_.Reset(isolate_, v8Str(isolate_, "consistency"));
  client_ctx_id_property_.Reset(isolate_, v8Str(isolate_, "clientContextId"));
  is_prepared_property_.Reset(isolate_, v8Str(isolate_, "isPrepared"));
}

Query::Options::Extractor::~Extractor() {
  context_.Reset();
  consistency_property_.Reset();
  client_ctx_id_property_.Reset();
  is_prepared_property_.Reset();
}

::Info Query::Options::Extractor::Extract(
    const v8::FunctionCallbackInfo<v8::Value> &args, Options &opt_out) const {
  opt_out.consistency = UnwrapData(isolate_)->n1ql_consistency;
  if (args.Length() < 3) {
    return {false};
  }
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  v8::Local<v8::Object> options_obj;
  if (!TO_LOCAL(args[2]->ToObject(context), &options_obj)) {
    return {true, "Unable to read options"};
  }

  auto info = ExtractConsistency(options_obj, opt_out.consistency);
  if (info.is_fatal) {
    return info;
  }
  info = ExtractClientCtxId(options_obj, opt_out.client_context_id);
  if (info.is_fatal) {
    return info;
  }
  return ExtractIsPrepared(options_obj, opt_out.is_prepared);
}

::Info Query::Options::Extractor::ExtractConsistency(
    const v8::Local<v8::Object> &options_obj, int &consistency_out) const {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  consistency_out = UnwrapData(isolate_)->n1ql_consistency;

  auto consistency_property = consistency_property_.Get(isolate_);
  v8::Local<v8::Value> consistency_val;
  if (!TO_LOCAL(options_obj->Get(context, consistency_property),
                &consistency_val)) {
    return {true, "Unable to read consistency value"};
  }
  if (consistency_val->IsUndefined()) {
    return {false};
  }
  if (!consistency_val->IsString()) {
    return {true, "Expecting a string for consistency"};
  }
  v8::String::Utf8Value consistency_utf8(isolate_, consistency_val);
  if (consistencies_.find(*consistency_utf8) == consistencies_.end()) {
    return {true, "consistency must be one of 'none', 'request'"};
  }
  consistency_out = Query::Helper::GetConsistency(*consistency_utf8);
  return {false};
}

::Info Query::Options::Extractor::ExtractClientCtxId(
    const v8::Local<v8::Object> &options_obj,
    std::unique_ptr<std::string> &client_ctx_id_out) const {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  auto client_ctx_id_property = client_ctx_id_property_.Get(isolate_);
  v8::Local<v8::Value> client_ctx_id_val;
  if (!TO_LOCAL(options_obj->Get(context, client_ctx_id_property),
                &client_ctx_id_val)) {
    return {true, "Unable to read clientContextId value"};
  }
  if (auto info = Utils::ValidateDataType(client_ctx_id_val); info.is_fatal) {
    return {true, "Invalid data type for clientContextId: " + info.msg};
  }
  auto client_ctx_id = JSONStringify(isolate_, client_ctx_id_val);
  client_ctx_id_out.reset(new std::string(std::move(client_ctx_id)));
  return {false};
}

::Info Query::Options::Extractor::ExtractIsPrepared(
    const v8::Local<v8::Object> &options_obj,
    std::unique_ptr<bool> &is_prepared_out) const {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  auto is_prepared = UnwrapData(isolate_)->n1ql_prepare_all;
  if (is_prepared) {
    is_prepared_out.reset(new bool(true));
  }

  const auto is_prepared_property = is_prepared_property_.Get(isolate_);
  v8::Local<v8::Value> is_prepared_val;
  if (!TO_LOCAL(options_obj->Get(context, is_prepared_property),
                &is_prepared_val)) {
    return {true, "Unable to read isPrepared value"};
  }
  if (is_prepared_val->IsUndefined()) {
    return {false};
  }
  if (!is_prepared_val->IsBoolean()) {
    return {true, "Expecting a boolean for isPrepared property"};
  }

  if (!TO(is_prepared_val->BooleanValue(context), &is_prepared)) {
    return {true, "Unable to cast isPrepared to boolean"};
  }
  is_prepared_out.reset(new bool(is_prepared));
  return {false};
}
