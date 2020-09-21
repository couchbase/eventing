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
#include "utils.h"
#include "lcb_utils.h"
#include "js_exception.h"
#include "isolate_data.h"
#include "bucket.h"
#include "info.h"
#include "v8worker.h"

extern std::atomic<int64_t> bucket_op_exception_count;
extern std::atomic<int64_t> lcb_retry_failure;
std::atomic<int64_t> bkt_ops_cas_mismatch_count = {0};

BucketOps::BucketOps(v8::Isolate *isolate, const v8::Local<v8::Context> &context)
 : isolate_(isolate) {
  context_.Reset(isolate_, context);
  cas_str_ = "cas";
  key_str_ = "id";
  expiry_str_ = "expiry_date";
  data_type_str_ = "data_type";
  key_not_found_str_ = "key_not_found";
  cas_mismatch_str_ = "cas_mismatch";
  key_exist_str_ = "key_already_exists";
  doc_str_ = "doc";
  meta_str_ = "meta";
  counter_str_ = "count";
  error_str_ = "error";
  success_str_ = "success";
  json_str_ = "json";
  error_str_ = "error";
  invalid_counter_str_ = "not_number";
}

BucketOps::~BucketOps() {
  context_.Reset();
}

void BucketOps::HandleBucketOpFailure(lcb_t connection, lcb_error_t error) {
  auto isolate_data = UnwrapData(isolate_);
  AddLcbException(isolate_data, error);
  ++bucket_op_exception_count;

  auto js_exception = isolate_data->js_exception;
  js_exception->ThrowKVError(connection, error);
}

Info BucketOps::SetErrorObject(v8::Local<v8::Object> &response_obj, std::string name, std::string desc,
                                lcb_error_t error, const char* error_type, bool value) {
  auto context = context_.Get(isolate_);
  auto code_value = v8::Number::New(isolate_, lcb_get_errtype(error));
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

  if (!TO(error_obj->Set(context, v8Str(isolate_, error_type), v8::Boolean::New(isolate_, value)),
          &success) ||
      !success) {
    return {true, "Unable to set error_type value"};
  }

  if (!TO(response_obj->Set(context, v8Str(isolate_, error_str_), error_obj),
          &success) ||
      !success) {
    return {true, "Unable to set error object"};
  }

  if (!TO(response_obj->Set(context, v8Str(isolate_, success_str_), v8::Boolean::New(isolate_, false)),
          &success) ||
      !success) {
    return {true, "Unable to set success value"};
  }

  return {false};
}

Info BucketOps::SetCounterData(std::unique_ptr<Result> const &result, v8::Local<v8::Object> &response_obj) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  auto counter = v8::Object::New(isolate_);
  auto success = false;
  if (!TO(counter->Set(context, v8Str(isolate_, counter_str_),
                            v8::Number::New(isolate_, result->counter)), &success)
        ||!success) {
    return {true, "Unable to set doc body"};
  }

  if (!TO(response_obj->Set(context, v8Str(isolate_, doc_str_), counter),
          &success) ||
      !success) {
    return {true, "Unable to set doc body"};
  }
  return {false};
}

Info BucketOps::SetDocBody(std::unique_ptr<Result> const &result, v8::Local<v8::Object> &response_obj) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  auto utils = UnwrapData(isolate_)->utils;

  v8::Local<v8::Value> doc;

  // doc is json type
  if(result->datatype & 1) {
    if(!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, result->value)),
               &doc)){
      return {true, "Unable to parse response body as JSON"};
    }
  } else {
    doc = utils->ToArrayBuffer(const_cast<void *>(result->binary), result->byteLength);
  }

  auto success = false;
  if (!TO(response_obj->Set(context, v8Str(isolate_, doc_str_), doc),
          &success) ||
      !success) {
    return {true, "Unable to set doc body"};
  }
  return {false};
}

Info BucketOps::SetMetaObject(std::unique_ptr<Result> const &result, v8::Local<v8::Object> &response_obj) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  auto meta_obj = v8::Object::New(isolate_);

  bool success = false;
  if (!TO(meta_obj->Set(context, v8Str(isolate_, key_str_), v8Str(isolate_, result->key)),
          &success) ||
      !success) {
    return {true, "Unable to set document key value in metaObject"};
  }

  if (!TO(meta_obj->Set(context, v8Str(isolate_, cas_str_), v8Str(isolate_, std::to_string(result->cas))),
          &success) ||
      !success) {
    return {true, "Unable to set cas value in metaObject"};
  }

  if(result->exptime) {
    double expiry = static_cast<double>(result->exptime) * 1000;
    if (!TO(meta_obj->Set(context, v8Str(isolate_, expiry_str_), v8::Date::New(isolate_, expiry)),
          &success) ||
      !success) {
      return {true, "Unable to set expiration value in metaObject"};
    }
  }

  if(result->datatype & 1) {
    if (!TO(meta_obj->Set(context, v8Str(isolate_, data_type_str_), v8Str(isolate_, json_str_)),
          &success) ||
      !success) {
      return {true, "Unable to set expiration value in metaObject"};
    }
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
  if(is_doc_needed) {
    info = SetDocBody(result, response_obj);
    if(info.is_fatal) {
      return info;
    }
  }

  if(counter_needed) {
      info = SetCounterData(result, response_obj);
      if(info.is_fatal) {
        return info;
      }
  }

  info = SetMetaObject(result, response_obj);
  if(info.is_fatal) {
    return info;
  }

  bool success = false;
  if (!TO(response_obj->Set(context, v8Str(isolate_, success_str_), v8::Boolean::New(isolate_, true)),
          &success) ||
      !success) {
    return {true, "Unable to set success value"};
  }

  return {false};
}

MetaInfo BucketOps::ExtractMetaInfo(v8::Local<v8::Value> meta_object, bool cas_check, bool expiry_check) {
  auto utils = UnwrapData(isolate_)->utils;
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);

  MetaData meta = {"", 0, 0};

  if(!meta_object->IsObject()) {
    return {false, "2nd argument should be object"};
  }

  v8::Local<v8::Object> req_obj;
  if (!TO_LOCAL(meta_object->ToObject(context), &req_obj)) {
    return {false, "error in casting metadata object to Object"};
  }

  v8::Local<v8::Value> key;
  if(req_obj->Has(v8Str(isolate_, key_str_))) {
    if (!TO_LOCAL(req_obj->Get(context, v8Str(isolate_, key_str_)), &key)) {
      return {false, "error in reading document key"};
    }
  } else {
      return {false, "document key is not present in meta object"};
  }

  auto info = Utils::ValidateDataType(key);
  if(info.is_fatal) {
    return {false, "Invalid data type for metaId: " + info.msg};
  }

  meta.key = utils->ToCPPString(key.As<v8::String>());
  if(meta.key == "") {
    return {false, "document key cannot be empty"};
  }

  if(cas_check && req_obj->Has(v8Str(isolate_, cas_str_))) {
    v8::Local<v8::Value> cas;
    if (!TO_LOCAL(req_obj->Get(context, v8Str(isolate_, cas_str_)), &cas)) {
      return {false, "error in reading cas"};
    }
    if(!cas->IsString()) {
      return {false, "cas should be a string"};
    }
    auto cas_value = utils->ToCPPString(cas.As<v8::String>());
    meta.cas = std::strtoull(cas_value.c_str(), nullptr, 10);
  }

  if(expiry_check && req_obj->Has(v8Str(isolate_, expiry_str_))) {
    v8::Local<v8::Value> expiry;
    if (!TO_LOCAL(req_obj->Get(context, v8Str(isolate_, expiry_str_)), &expiry)) {
      return {false, "error in reading expiration"};
    }

    if(!expiry->IsDate()) {
      return {false, "expiry should be a data object"};
    }

    auto info = Epoch(expiry);
    if(!info.is_valid) {
        return {false, "Unable to compute epoch for the given Date instance"};
    }
    meta.expiry = (uint32_t)info.epoch;
  }
  return {true, meta};
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

std::tuple<Error, std::unique_ptr<lcb_error_t>, std::unique_ptr<Result>>
BucketOps::Delete(const std::string &key, lcb_CAS cas, bool is_source_bucket,
                            Bucket *bucket) {
  if (is_source_bucket) {
    return bucket->DeleteWithXattr(key, cas);
  }
  return bucket->DeleteWithoutXattr(key, cas);
}

std::tuple<Error, std::unique_ptr<lcb_error_t>, std::unique_ptr<Result>>
BucketOps::Counter(const std::string &key, lcb_CAS cas, lcb_U32 expiry, std::string delta,
                   bool is_source_bucket, Bucket *bucket) {
  if (is_source_bucket) {
    return bucket->CounterWithXattr(key, cas, expiry, delta);
  }
  return bucket->CounterWithoutXattr(key, cas, expiry, delta);
}

std::tuple<Error, std::unique_ptr<lcb_error_t>, std::unique_ptr<Result>>
BucketOps::Set(const std::string &key, const void* value, int value_length, lcb_storage_t op_type,
               lcb_U32 expiry, lcb_CAS cas, lcb_U32 doc_type, bool is_source_bucket, Bucket *bucket) {
  if (is_source_bucket) {
    lcb_U32 cmd_flag = 0;
    if(op_type == LCB_SET) {
      cmd_flag = LCB_CMDSUBDOC_F_UPSERT_DOC;
    } else if(op_type == LCB_ADD) {
      cmd_flag = LCB_CMDSUBDOC_F_INSERT_DOC;
    }

    return bucket->SetWithXattr(key, value, value_length, cmd_flag, expiry, cas);
  }
  return bucket->SetWithoutXattr(key, value, value_length, op_type, expiry, cas, doc_type);
}

std::tuple<Error, std::unique_ptr<lcb_error_t>, std::unique_ptr<Result>>
BucketOps::BucketSet(const std::string &key, v8::Local<v8::Value> value,
                     lcb_storage_t op_type, lcb_U32 expiry, lcb_CAS cas,
                     bool is_source_bucket, Bucket *bucket) {
  v8::HandleScope scope(isolate_);

  if(value->IsArrayBuffer()) {
    auto array_buf = value.As<v8::ArrayBuffer>();
    auto contents = array_buf->GetContents();
    auto data = static_cast<const uint8_t *>(contents.Data());
    int data_length = contents.ByteLength();
    lcb_U32  doc_type = 0x00000000;
    return Set(key, data, data_length, op_type, expiry, cas, doc_type, is_source_bucket, bucket);
  }

  auto json_value = JSONStringify(isolate_, value);
  const char* data = json_value.c_str();
  int data_length = strlen(data);
  lcb_U32 doc_type = 0x2000000;
  return Set(key, data, data_length, op_type, expiry, cas, doc_type, is_source_bucket, bucket);
}

void BucketOps::CounterOps(v8::FunctionCallbackInfo<v8::Value> args, std::string delta) {
  v8::HandleScope handle_scope(isolate_);

  auto isolate_data = UnwrapData(isolate_);
  auto js_exception = isolate_data->js_exception;

  if (args.Length() < 2) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError("couchbase.counter requires at least 2 arguments");
    return;
  }

  auto info = VerifyBucketObject(args[0]);
  if(info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate_, args[0]);
  if(block_mutation) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  auto meta_info = ExtractMetaInfo(args[1]);
  if(!meta_info.is_valid) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  auto bucket = BucketBinding::GetBucket(isolate_, args[0]);
  auto is_source_bucket = BucketBinding::IsSourceBucket(isolate_, args[0]);

  auto [error, err_code, result] = Counter(meta.key, meta.cas, meta.expiry, delta, is_source_bucket, bucket);

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
  if (result->rc == LCB_DELTA_BADVAL || result->rc == LCB_SUBDOC_MULTI_FAILURE) {
    info = SetErrorObject(response_obj, "LCB_DELTA_BADVAL", "counter value cannot be parsed as a number",
                           result->rc, invalid_counter_str_, true);

    if(info.is_fatal) {
      ++bucket_op_exception_count;
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc == LCB_KEY_EEXISTS) {
    info = SetErrorObject(response_obj, "LCB_KEY_EEXISTS", "The document key exists with a CAS value different than specified",
                           result->rc, cas_mismatch_str_, true);
    if(info.is_fatal) {
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
  if(info.is_fatal) {
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
  if(info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1]);
  if(!meta_info.is_valid) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  auto bucket = BucketBinding::GetBucket(isolate, args[0]);
  auto [error, err_code, result] = bucket->GetWithMeta(meta.key);
  if (error != nullptr) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError(*error);
    return;
  }

  v8::Local<v8::Object> response_obj = v8::Object::New(isolate);
  if (*err_code != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), *err_code);
    return;
  }

  if (result->rc == LCB_KEY_ENOENT) {
    info = bucket_ops->SetErrorObject(response_obj, "LCB_KEY_ENOENT",
                                       "The document key does not exist on the server",
                                       result->rc, bucket_ops->key_not_found_str_, true);

    if(info.is_fatal) {
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
  info = bucket_ops->ResponseSuccessObject(std::move(result), response_obj, true);
  if(info.is_fatal) {
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
    js_exception->ThrowTypeError("couchbase.insert requires at least 3 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if(info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if(block_mutation) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], false, true);
  if(!meta_info.is_valid) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  auto is_source_bucket = BucketBinding::IsSourceBucket(isolate, args[0]);
  auto bucket = BucketBinding::GetBucket(isolate, args[0]);

  auto [error, err_code, result] = bucket_ops->BucketSet(meta.key, args[2], LCB_ADD, meta.expiry,
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
  if (result->rc == LCB_KEY_EEXISTS) {
    info = bucket_ops->SetErrorObject(response_obj, "LCB_KEY_EEXISTS",
                                       "The document key already exists in the server.",
                                       result->rc, bucket_ops->key_exist_str_, true);

    if(info.is_fatal) {
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
  if(info.is_fatal) {
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
    js_exception->ThrowTypeError("couchbase.upsert requires at least 3 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if(info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if(block_mutation) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], true, true);
  if(!meta_info.is_valid) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  auto is_source_bucket = BucketBinding::IsSourceBucket(isolate, args[0]);
  auto bucket = BucketBinding::GetBucket(isolate, args[0]);

  auto [error, err_code, result] = bucket_ops->BucketSet(meta.key, args[2], LCB_REPLACE, meta.expiry,
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

  if (result->rc == LCB_KEY_EEXISTS) {
    info = bucket_ops->SetErrorObject(response_obj, "LCB_KEY_EEXISTS",
                                       "The document key exists with a CAS value different than specified",
                                       result->rc, bucket_ops->cas_mismatch_str_, true);

    if(info.is_fatal) {
      ++bucket_op_exception_count;
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    ++bkt_ops_cas_mismatch_count;
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc == LCB_KEY_ENOENT) {
    info = bucket_ops->SetErrorObject(response_obj, "LCB_KEY_ENOENT",
                                       "The document key does not exist on the server",
                                       result->rc, bucket_ops->key_not_found_str_, true);

    if(info.is_fatal) {
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
  if(info.is_fatal) {
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
    js_exception->ThrowTypeError("couchbase.upsert requires at least 3 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if(info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if(block_mutation) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], false, true);
  if(!meta_info.is_valid) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  auto is_source_bucket = BucketBinding::IsSourceBucket(isolate, args[0]);
  auto bucket = BucketBinding::GetBucket(isolate, args[0]);

  auto [error, err_code, result] = bucket_ops->BucketSet(meta.key, args[2], LCB_SET, meta.expiry,
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
  if(info.is_fatal) {
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
    js_exception->ThrowTypeError("couchbase.delete requires at least 2 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if(info.is_fatal) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if(block_mutation) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError("Delete from source bucket is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], true);
  if(!meta_info.is_valid) {
    ++bucket_op_exception_count;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  auto bucket = BucketBinding::GetBucket(isolate, args[0]);
  auto is_source_bucket = BucketBinding::IsSourceBucket(isolate, args[0]);

  auto [error, err_code, result] = bucket_ops->Delete(meta.key, meta.cas, is_source_bucket, bucket);
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
  if (result->rc == LCB_KEY_EEXISTS) {
    info = bucket_ops->SetErrorObject(response_obj, "LCB_KEY_EEXISTS",
                                       "The document key exists with a CAS value different than specified",
                                       result->rc, bucket_ops->cas_mismatch_str_, true);

    if(info.is_fatal) {
      ++bucket_op_exception_count;
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    ++bkt_ops_cas_mismatch_count;
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc == LCB_KEY_ENOENT) {
    info = bucket_ops->SetErrorObject(response_obj, "LCB_KEY_ENOENT",
                                       "The document key does not exist on the server",
                                       result->rc, bucket_ops->key_not_found_str_, true);

    if(info.is_fatal) {
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
  if(info.is_fatal) {
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
  bucket_ops->CounterOps(args, "1");
}

void BucketOps::DecrementOp(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
    return;
  }

  auto bucket_ops = isolate_data->bucket_ops;
  bucket_ops->CounterOps(args, "-1");
}
