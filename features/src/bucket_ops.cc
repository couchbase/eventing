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

#include "bucket_ops.h"
#include "bucket.h"
#include "info.h"
#include "isolate_data.h"
#include "js_exception.h"
#include "lcb_utils.h"
#include "utils.h"
#include "v8worker.h"

extern std::atomic<int64_t> bucket_op_exception_count;
extern std::atomic<int64_t> lcb_retry_failure;
std::atomic<int64_t> bkt_ops_cas_mismatch_count = {0};

BucketOps::BucketOps(v8::Isolate *isolate,
                     const v8::Local<v8::Context> &context)
    : isolate_(isolate) {
  context_.Reset(isolate_, context);
  cas_str_ = "cas";
  key_str_ = "id";
  expiry_str_ = "expiry_date";
  data_type_str_ = "datatype";
  key_not_found_str_ = "key_not_found";
  cas_mismatch_str_ = "cas_mismatch";
  key_exist_str_ = "key_already_exists";
  doc_str_ = "doc";
  meta_str_ = "meta";
  cache_str_ = "cache";
  counter_str_ = "count";
  error_str_ = "error";
  success_str_ = "success";
  json_str_ = "json";
  binary_str_ = "binary";
  error_str_ = "error";
  invalid_counter_str_ = "not_number";
}

BucketOps::~BucketOps() { context_.Reset(); }

void BucketOps::HandleBucketOpFailure(lcb_INSTANCE *connection,
                                      lcb_STATUS error) {
  auto isolate_data = UnwrapData(isolate_);
  AddLcbException(isolate_data, error);
  ++bucket_op_exception_count;

  auto js_exception = isolate_data->js_exception;
  js_exception->ThrowKVError(connection, error);
}

Info BucketOps::SetErrorObject(v8::Local<v8::Object> &response_obj,
                               std::string name, std::string desc,
                               lcb_STATUS error, const char *error_type,
                               bool value) {
  auto context = context_.Get(isolate_);
  auto code_value = v8::Number::New(isolate_, lcb_error_flags(error));
  auto name_value = v8Str(isolate_, name);
  auto desc_value = v8Str(isolate_, desc);

  auto error_obj = v8::Object::New(isolate_);
  bool success = false;
  if (!TO(error_obj->Set(context, v8Str(isolate_, "code"), code_value),
          &success) ||
      !success) {
    return {true, "Unable to set code value"};
  }

  if (!TO(error_obj->Set(context, v8Str(isolate_, "name"), name_value),
          &success) ||
      !success) {
    return {true, "Unable to set name value"};
  }

  if (!TO(error_obj->Set(context, v8Str(isolate_, "desc"), desc_value),
          &success) ||
      !success) {
    return {true, "Unable to set desc value"};
  }

  if (!TO(error_obj->Set(context, v8Str(isolate_, error_type),
                         v8::Boolean::New(isolate_, value)),
          &success) ||
      !success) {
    return {true, "Unable to set error_type value"};
  }

  if (!TO(response_obj->Set(context, v8Str(isolate_, error_str_), error_obj),
          &success) ||
      !success) {
    return {true, "Unable to set error object"};
  }

  if (!TO(response_obj->Set(context, v8Str(isolate_, success_str_),
                            v8::Boolean::New(isolate_, false)),
          &success) ||
      !success) {
    return {true, "Unable to set success value"};
  }

  return {false};
}

Info BucketOps::SetCounterData(std::unique_ptr<Result> const &result,
                               v8::Local<v8::Object> &response_obj) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  auto counter = v8::Object::New(isolate_);
  auto success = false;
  if (!TO(counter->Set(context, v8Str(isolate_, counter_str_),
                       v8::Number::New(isolate_, result->subdoc_counter)),
          &success) ||
      !success) {
    return {true, "Unable to set doc body"};
  }

  if (!TO(response_obj->Set(context, v8Str(isolate_, doc_str_), counter),
          &success) ||
      !success) {
    return {true, "Unable to set doc body"};
  }
  return {false};
}

Info BucketOps::SetDocBody(std::unique_ptr<Result> const &result,
                           v8::Local<v8::Object> &response_obj) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  auto utils = UnwrapData(isolate_)->utils;

  v8::Local<v8::Value> doc;

  // doc is json type
  if (result->datatype & JSON_DOC) {
    if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, result->value)),
                  &doc)) {
      return {true, "Unable to parse response body as JSON"};
    }
  } else {
    doc = utils->ToArrayBuffer(static_cast<void *>(result->value.data()),
                               result->value.length());
  }

  auto success = false;
  if (!TO(response_obj->Set(context, v8Str(isolate_, doc_str_), doc),
          &success) ||
      !success) {
    return {true, "Unable to set doc body"};
  }
  return {false};
}

Info BucketOps::SetMetaObject(std::unique_ptr<Result> const &result,
                              v8::Local<v8::Object> &response_obj) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  auto meta_obj = v8::Object::New(isolate_);

  bool success = false;
  if (!TO(meta_obj->Set(context, v8Str(isolate_, key_str_),
                        v8Str(isolate_, result->key)),
          &success) ||
      !success) {
    return {true, "Unable to set document key value in metaObject"};
  }

  if (!TO(meta_obj->Set(context, v8Str(isolate_, cas_str_),
                        v8Str(isolate_, std::to_string(result->cas))),
          &success) ||
      !success) {
    return {true, "Unable to set cas value in metaObject"};
  }

  if (result->exptime) {
    double expiry = static_cast<double>(result->exptime) * 1000;
    if (!TO(meta_obj->Set(context, v8Str(isolate_, expiry_str_),
                          v8::Date::New(context, expiry).ToLocalChecked()),
            &success) ||
        !success) {
      return {true, "Unable to set expiration value in metaObject"};
    }
  }

  auto datatype = json_str_;
  if (!(result->datatype & JSON_DOC)) {
    datatype = binary_str_;
  }

  if (!TO(meta_obj->Set(context, v8Str(isolate_, data_type_str_),
                        v8Str(isolate_, datatype)),
          &success) ||
      !success) {
    return {true, "Unable to set datatype value in metaObject"};
  }

  if (!TO(response_obj->Set(context, v8Str(isolate_, meta_str_), meta_obj),
          &success) ||
      !success) {
    return {true, "Unable to set meta object"};
  }
  return {false};
}

Info BucketOps::ResponseSuccessObject(std::unique_ptr<Result> const &result,
                                      v8::Local<v8::Object> &response_obj,
                                      bool is_doc_needed, bool counter_needed) {

  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  Info info;
  if (is_doc_needed) {
    info = SetDocBody(result, response_obj);
    if (info.is_fatal) {
      return info;
    }
  }

  if (counter_needed) {
    info = SetCounterData(result, response_obj);
    if (info.is_fatal) {
      return info;
    }
  }

  info = SetMetaObject(result, response_obj);
  if (info.is_fatal) {
    return info;
  }

  bool success = false;
  if (!TO(response_obj->Set(context, v8Str(isolate_, success_str_),
                            v8::Boolean::New(isolate_, true)),
          &success) ||
      !success) {
    return {true, "Unable to set success value"};
  }

  return {false};
}

MetaInfo BucketOps::ExtractMetaInfo(v8::Local<v8::Value> meta_object,
                                    bool cas_check, bool expiry_check) {
  auto utils = UnwrapData(isolate_)->utils;
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);

  MetaData meta = {"", 0, 0};

  if (!meta_object->IsObject()) {
    return {false, "2nd argument should be object"};
  }

  v8::Local<v8::Object> req_obj;
  if (!TO_LOCAL(meta_object->ToObject(context), &req_obj)) {
    return {false, "error in casting 2nd argument to Object"};
  }

  v8::Local<v8::Value> key;
  if (req_obj->Has(context, v8Str(isolate_, key_str_)).FromJust()) {
    if (!TO_LOCAL(req_obj->Get(context, v8Str(isolate_, key_str_)), &key)) {
      return {false, "error in reading document key from 2nd argument"};
    }
  } else {
    return {false, "document key is not present in 2nd argument"};
  }

  auto info = Utils::ValidateDataType(key);
  if (info.is_fatal) {
    return {false, "Invalid data type for metaId: " + info.msg};
  }

  meta.key = utils->ToCPPString(key.As<v8::String>());
  if (meta.key == "") {
    return {false, "document key cannot be empty"};
  }

  if (cas_check && req_obj->Has(context, v8Str(isolate_, cas_str_)).FromJust()) {
    v8::Local<v8::Value> cas;
    if (!TO_LOCAL(req_obj->Get(context, v8Str(isolate_, cas_str_)), &cas)) {
      return {false, "error in reading cas"};
    }
    if (!cas->IsString()) {
      return {false, "cas should be a string"};
    }
    auto cas_value = utils->ToCPPString(cas.As<v8::String>());
    meta.cas = std::strtoull(cas_value.c_str(), nullptr, 10);
  }

  if (expiry_check && req_obj->Has(context, v8Str(isolate_, expiry_str_)).FromJust()) {
    v8::Local<v8::Value> expiry;
    if (!TO_LOCAL(req_obj->Get(context, v8Str(isolate_, expiry_str_)),
                  &expiry)) {
      return {false, "error in reading expiration"};
    }

    if (!expiry->IsDate()) {
      return {false, "expiry should be a date object"};
    }

    auto info = Epoch(expiry);
    if (!info.is_valid) {
      return {false, "Unable to compute epoch for the given Date instance"};
    }
    meta.expiry = (uint32_t)info.epoch;
  }
  return {true, meta};
}

OptionsInfo BucketOps::ExtractOptionsInfo(v8::Local<v8::Value> options_object) {
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);

  OptionsData options = {false};

  if (!options_object->IsObject()) {
    return {false, "3rd argument, if present, must be object"};
  }

  v8::Local<v8::Object> req_obj;
  if (!TO_LOCAL(options_object->ToObject(context), &req_obj)) {
    return {false, "error in casting options object to Object"};
  }

  if (req_obj->Has(context, v8Str(isolate_, cache_str_)).FromJust()) {
    v8::Local<v8::Value> cache;
    if (!TO_LOCAL(req_obj->Get(context, v8Str(isolate_, cache_str_)), &cache)) {
      return {false, "error reading 'cache' parameter in options"};
    }
    if (!cache->IsBoolean()) {
      return {false, "the 'cache' parameter in options should be a boolean"};
    }
    auto cache_value = cache.As<v8::Boolean>();
    options.cache = cache_value->Value();
  }

  return {true, options};
}

EpochInfo BucketOps::Epoch(const v8::Local<v8::Value> &date_val) {
  auto utils = UnwrapData(isolate_)->utils;
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  auto get_time_val = utils->GetPropertyFromObject(date_val, "getTime");
  auto get_time_func = get_time_val.As<v8::Function>();

  v8::Local<v8::Value> seconds_v8val;
  if (!TO_LOCAL(get_time_func->Call(context, date_val, 0, nullptr),
                &seconds_v8val)) {
    return {false};
  }

  v8::Local<v8::Number> number_v8val;
  if (!TO_LOCAL(seconds_v8val->ToNumber(context), &number_v8val)) {
    return {false};
  }

  auto epoch = number_v8val->IntegerValue(context).FromMaybe(0) / 1000;
  return {true, epoch};
}

Info BucketOps::VerifyBucketObject(v8::Local<v8::Value> bucket_binding) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  v8::Local<v8::Object> temp_obj;
  if (!TO_LOCAL(bucket_binding->ToObject(context), &temp_obj)) {
    return {true, "1st argument should be object"};
  }

  if (!BucketBinding::IsBucketObject(isolate_, temp_obj)) {
    return {true, "1st argument should be bucket object"};
  }
  return {false};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
BucketOps::Delete(const std::string &key, uint64_t cas, bool is_source_bucket,
                  Bucket *bucket) {
  if (is_source_bucket) {
    return bucket->DeleteWithXattr(key, cas);
  }
  return bucket->DeleteWithoutXattr(key, cas);
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
BucketOps::Counter(const std::string &key, uint64_t cas, lcb_U32 expiry,
                   int64_t delta, bool is_source_bucket, Bucket *bucket) {
  if (is_source_bucket) {
    return bucket->CounterWithXattr(key, cas, expiry, delta);
  }
  return bucket->CounterWithoutXattr(key, cas, expiry, delta);
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
BucketOps::Set(const std::string &key, const std::string &value,
               lcb_STORE_OPERATION op_type, lcb_U32 expiry, uint64_t cas,
               lcb_U32 doc_type, bool is_source_bucket, Bucket *bucket) {
  if (is_source_bucket) {
    lcb_SUBDOC_STORE_SEMANTICS cmd_flag = LCB_SUBDOC_STORE_REPLACE;
    if (op_type == LCB_STORE_UPSERT) {
      cmd_flag = LCB_SUBDOC_STORE_UPSERT;
    } else if (op_type == LCB_STORE_INSERT) {
      cmd_flag = LCB_SUBDOC_STORE_INSERT;
    }

    return bucket->SetWithXattr(key, value, cmd_flag, expiry, cas);
  }
  return bucket->SetWithoutXattr(key, value, op_type, expiry, cas, doc_type);
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
BucketOps::BucketSet(const std::string &key, v8::Local<v8::Value> value,
                     lcb_STORE_OPERATION op_type, lcb_U32 expiry, uint64_t cas,
                     bool is_source_bucket, Bucket *bucket) {
  v8::HandleScope scope(isolate_);
  std::string value_str;
  lcb_U32 doc_type = 0x00000000;
  if (value->IsArrayBuffer()) {
    auto array_buf = value.As<v8::ArrayBuffer>();
    auto contents = array_buf->GetContents();
    value_str.assign(static_cast<const char *>(contents.Data()),
                     contents.ByteLength());
  } else {
    value_str = JSONStringify(isolate_, value);
    doc_type = 0x2000000;
  }
  return Set(key, value_str, op_type, expiry, cas, doc_type, is_source_bucket,
             bucket);
}

void BucketOps::CounterOps(v8::FunctionCallbackInfo<v8::Value> args,
                           int64_t delta) {
  v8::HandleScope handle_scope(isolate_);

  auto isolate_data = UnwrapData(isolate_);
  auto js_exception = isolate_data->js_exception;

  if (args.Length() < 2) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(
        "couchbase.counter requires at least 2 arguments");
    return;
  }

  auto info = VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate_, args[0]);
  if (block_mutation) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  auto meta_info = ExtractMetaInfo(args[1]);
  if (!meta_info.is_valid) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  auto bucket = BucketBinding::GetBucket(isolate_, args[0]);
  auto is_source_bucket = BucketBinding::IsSourceBucket(isolate_, args[0]);

  auto [error, err_code, result] =
      Counter(meta.key, meta.cas, meta.expiry, delta, is_source_bucket, bucket);

  if (error != nullptr) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError(*error);
    return;
  }

  if (*err_code != LCB_SUCCESS) {
    HandleBucketOpFailure(bucket->GetConnection(), *err_code);
    return;
  }

  v8::Local<v8::Object> response_obj = v8::Object::New(isolate_);
  if (result->rc == LCB_ERR_SUBDOC_PATH_MISMATCH) {
    info = SetErrorObject(response_obj, "LCB_DELTA_BADVAL",
                          "counter value cannot be parsed as a number",
                          result->rc, invalid_counter_str_, true);

    if (info.is_fatal) {
      ++bucket_op_exception_count;
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc == LCB_ERR_DOCUMENT_EXISTS) {
    info = SetErrorObject(
        response_obj, "LCB_KEY_EEXISTS",
        "The document key exists with a CAS value different than specified",
        result->rc, cas_mismatch_str_, true);
    if (info.is_fatal) {
      ++bucket_op_exception_count;
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    ++bkt_ops_cas_mismatch_count;
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc != LCB_SUCCESS) {
    HandleBucketOpFailure(bucket->GetConnection(), result->rc);
    return;
  }

  result->key = meta.key;
  result->exptime = meta.expiry;

  info = ResponseSuccessObject(std::move(result), response_obj, false, true);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError(info.msg);
  }
  args.GetReturnValue().Set(response_obj);
}

void BucketOps::GetOp(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  v8::HandleScope handle_scope(isolate);

  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
    return;
  }

  auto js_exception = isolate_data->js_exception;
  auto bucket_ops = isolate_data->bucket_ops;

  if (args.Length() < 2) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError("couchbase.get requires at least 2 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1]);
  if (!meta_info.is_valid) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  OptionsData options = {false};
  if (args.Length() > 2) {
    auto options_info = bucket_ops->ExtractOptionsInfo(args[2]);
    if (!options_info.is_valid) {
      ++bucket_op_exception_count;
      js_exception->ThrowTypeError(options_info.msg);
      return;
    }
    options = options_info.options;
  }

  auto bucket = BucketBinding::GetBucket(isolate, args[0]);
  v8::Local<v8::Object> response_obj = v8::Object::New(isolate);

  if (options.cache) {
    auto result = std::make_unique<Result>();
    auto idx = BucketCache::MakeKey(bucket->BucketName(), bucket->ScopeName(),
                                    bucket->CollectionName(), meta.key);
    auto found = BucketCache::Fetch().Get(idx, *(result.get()));
    if (found) {
      info = bucket_ops->ResponseSuccessObject(std::move(result), response_obj,
                                               true);
      if (info.is_fatal) {
        ++bucket_op_exception_count;
        js_exception->ThrowEventingError(info.msg);
        return;
      }
      args.GetReturnValue().Set(response_obj);
      return;
    }
  }

  auto [error, err_code, result] = bucket->GetWithMeta(meta.key);
  if (error != nullptr) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError(*error);
    return;
  }

  if (*err_code != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), *err_code);
    return;
  }

  if (result->rc == LCB_ERR_DOCUMENT_NOT_FOUND) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_ENOENT",
        "The document key does not exist on the server", result->rc,
        bucket_ops->key_not_found_str_, true);

    if (info.is_fatal) {
      ++bucket_op_exception_count;
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), result->rc);
    return;
  }

  result->key = meta.key;

  if (options.cache) {
    auto idx = BucketCache::MakeKey(bucket->BucketName(), bucket->ScopeName(),
                                    bucket->CollectionName(), meta.key);
    BucketCache::Fetch().Set(idx, *result);
  }

  info =
      bucket_ops->ResponseSuccessObject(std::move(result), response_obj, true);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError(info.msg);
    return;
  }

  args.GetReturnValue().Set(response_obj);
}

void BucketOps::InsertOp(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  v8::HandleScope handle_scope(isolate);

  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
    return;
  }

  auto js_exception = isolate_data->js_exception;
  auto bucket_ops = isolate_data->bucket_ops;

  if (args.Length() < 3) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(
        "couchbase.insert requires at least 3 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if (block_mutation) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], false, true);
  if (!meta_info.is_valid) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }

  info = Utils::ValidateDataType(args[2]);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    auto err_msg = "Invalid data type for 3rd argument: " + info.msg;
    js_exception->ThrowTypeError(err_msg);
    return;
  }

  auto meta = meta_info.meta;

  auto is_source_bucket = BucketBinding::IsSourceBucket(isolate, args[0]);
  auto bucket = BucketBinding::GetBucket(isolate, args[0]);

  auto [error, err_code, result] =
      bucket_ops->BucketSet(meta.key, args[2], LCB_STORE_INSERT, meta.expiry,
                            meta.cas, is_source_bucket, bucket);

  if (error != nullptr) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError(*error);
    return;
  }

  if (*err_code != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), *err_code);
    return;
  }

  v8::Local<v8::Object> response_obj = v8::Object::New(isolate);
  if (result->rc == LCB_ERR_DOCUMENT_EXISTS) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_EEXISTS",
        "The document key already exists in the server.", result->rc,
        bucket_ops->key_exist_str_, true);

    if (info.is_fatal) {
      ++bucket_op_exception_count;
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), result->rc);
    return;
  }

  result->key = meta.key;
  result->exptime = meta.expiry;

  info = bucket_ops->ResponseSuccessObject(std::move(result), response_obj);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError(info.msg);
    return;
  }

  args.GetReturnValue().Set(response_obj);
}

void BucketOps::ReplaceOp(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  v8::HandleScope handle_scope(isolate);

  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
    return;
  }

  auto js_exception = isolate_data->js_exception;
  auto bucket_ops = isolate_data->bucket_ops;

  if (args.Length() < 3) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(
        "couchbase.upsert requires at least 3 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if (block_mutation) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], true, true);
  if (!meta_info.is_valid) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  info = Utils::ValidateDataType(args[2]);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    auto err_msg = "Invalid data type for 3rd argument: " + info.msg;
    js_exception->ThrowTypeError(err_msg);
    return;
  }

  auto is_source_bucket = BucketBinding::IsSourceBucket(isolate, args[0]);
  auto bucket = BucketBinding::GetBucket(isolate, args[0]);

  auto [error, err_code, result] =
      bucket_ops->BucketSet(meta.key, args[2], LCB_STORE_REPLACE, meta.expiry,
                            meta.cas, is_source_bucket, bucket);

  if (error != nullptr) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError(*error);
    return;
  }

  if (*err_code != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), *err_code);
    return;
  }

  v8::Local<v8::Object> response_obj = v8::Object::New(isolate);

  if (result->rc == LCB_ERR_DOCUMENT_EXISTS) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_EEXISTS",
        "The document key exists with a CAS value different than specified",
        result->rc, bucket_ops->cas_mismatch_str_, true);

    if (info.is_fatal) {
      ++bucket_op_exception_count;
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    ++bkt_ops_cas_mismatch_count;
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc == LCB_ERR_DOCUMENT_NOT_FOUND) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_ENOENT",
        "The document key does not exist on the server", result->rc,
        bucket_ops->key_not_found_str_, true);

    if (info.is_fatal) {
      ++bucket_op_exception_count;
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), result->rc);
    return;
  }

  result->key = meta.key;
  result->exptime = meta.expiry;

  info = bucket_ops->ResponseSuccessObject(std::move(result), response_obj);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError(info.msg);
    return;
  }

  args.GetReturnValue().Set(response_obj);
}

void BucketOps::UpsertOp(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  v8::HandleScope handle_scope(isolate);

  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
    return;
  }

  auto js_exception = isolate_data->js_exception;
  auto bucket_ops = isolate_data->bucket_ops;

  if (args.Length() < 3) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(
        "couchbase.upsert requires at least 3 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if (block_mutation) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], false, true);
  if (!meta_info.is_valid) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  info = Utils::ValidateDataType(args[2]);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    auto err_msg = "Invalid data type for 3rd argument: " + info.msg;
    js_exception->ThrowTypeError(err_msg);
    return;
  }

  auto is_source_bucket = BucketBinding::IsSourceBucket(isolate, args[0]);
  auto bucket = BucketBinding::GetBucket(isolate, args[0]);

  auto [error, err_code, result] =
      bucket_ops->BucketSet(meta.key, args[2], LCB_STORE_UPSERT, meta.expiry,
                            meta.cas, is_source_bucket, bucket);

  if (error != nullptr) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError(*error);
    return;
  }

  if (*err_code != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), *err_code);
    return;
  }

  v8::Local<v8::Object> response_obj = v8::Object::New(isolate);
  if (result->rc != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), result->rc);
    return;
  }

  result->key = meta.key;
  result->exptime = meta.expiry;

  info = bucket_ops->ResponseSuccessObject(std::move(result), response_obj);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError(info.msg);
    return;
  }

  args.GetReturnValue().Set(response_obj);
}

void BucketOps::DeleteOp(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  v8::HandleScope handle_scope(isolate);

  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
    return;
  }

  auto bucket_ops = isolate_data->bucket_ops;
  auto js_exception = isolate_data->js_exception;

  if (args.Length() < 2) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(
        "couchbase.delete requires at least 2 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if (block_mutation) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError("Delete from source bucket is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], true);
  if (!meta_info.is_valid) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  auto bucket = BucketBinding::GetBucket(isolate, args[0]);
  auto is_source_bucket = BucketBinding::IsSourceBucket(isolate, args[0]);

  auto [error, err_code, result] =
      bucket_ops->Delete(meta.key, meta.cas, is_source_bucket, bucket);
  if (error != nullptr) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError(*error);
    return;
  }

  if (*err_code != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), *err_code);
    return;
  }

  v8::Local<v8::Object> response_obj = v8::Object::New(isolate);
  if (result->rc == LCB_ERR_DOCUMENT_EXISTS) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_EEXISTS",
        "The document key exists with a CAS value different than specified",
        result->rc, bucket_ops->cas_mismatch_str_, true);

    if (info.is_fatal) {
      ++bucket_op_exception_count;
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    ++bkt_ops_cas_mismatch_count;
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc == LCB_ERR_DOCUMENT_NOT_FOUND) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_ENOENT",
        "The document key does not exist on the server", result->rc,
        bucket_ops->key_not_found_str_, true);

    if (info.is_fatal) {
      ++bucket_op_exception_count;
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), result->rc);
    return;
  }

  result->key = meta.key;
  info = bucket_ops->ResponseSuccessObject(std::move(result), response_obj);
  if (info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError(info.msg);
    return;
  }

  args.GetReturnValue().Set(response_obj);
}

void BucketOps::IncrementOp(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
    return;
  }

  auto bucket_ops = isolate_data->bucket_ops;
  bucket_ops->CounterOps(args, 1);
}

void BucketOps::DecrementOp(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
    return;
  }

  auto bucket_ops = isolate_data->bucket_ops;
  bucket_ops->CounterOps(args, -1);
}
