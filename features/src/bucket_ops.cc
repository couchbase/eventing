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
#include "v8worker2.h"

BucketOps::BucketOps(v8::Isolate *isolate,
                     const v8::Local<v8::Context> &context)
    : isolate_(isolate) {
  context_.Reset(isolate_, context);
  cas_str_ = "cas";
  key_str_ = "id";
  scope_str_ = "scope_name";
  collection_str_ = "collection_name";
  expiry_str_ = "expiry_date";
  data_type_str_ = "datatype";
  key_not_found_str_ = "key_not_found";
  cas_mismatch_str_ = "cas_mismatch";
  key_exist_str_ = "key_already_exists";
  field_not_found_str_ = "field_not_found";
  field_exist_str_ = "field_already_exists";
  doc_str_ = "doc";
  meta_str_ = "meta";
  cache_str_ = "cache";
  spec_type_str_ = "spec_type";
  key_subdoc_str_ = "path";
  value_subdoc_str_ = "value";
  options_subdoc_str_ = "options";
  create_path_type_str_ = "create_path";
  user_xattr_str_ = "xattrs";
  self_recursion_str_ = "self_recursion";
  counter_str_ = "count";
  error_str_ = "error";
  success_str_ = "success";
  json_str_ = "json";
  binary_str_ = "binary";
  invalid_counter_str_ = "not_number";
}

BucketOps::~BucketOps() { context_.Reset(); }

void BucketOps::HandleBucketOpFailure(lcb_INSTANCE *connection,
                                      lcb_STATUS error) {
  auto isolate_data = UnwrapData(isolate_);
  AddLcbException(isolate_data, error);
  const auto v8worker = isolate_data->v8worker2;
  v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");

  auto js_exception = isolate_data->js_exception;
  js_exception->ThrowKVError(connection, error);
}

Info BucketOps::SetErrorObject(v8::Local<v8::Object> &response_obj,
                               std::string name, std::string desc,
                               uint16_t error_code, const char *error_type,
                               bool value) {
  auto context = context_.Get(isolate_);
  auto code_value = v8::Number::New(isolate_, error_code);
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

  if (!(result->datatype & UNKNOWN_TYPE)) {
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
                                      bool is_doc_needed, bool counter_needed,
                                      bool res_size_needed) {

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
  if (res_size_needed) {
    auto size = sizeof(result) + result->key.length() + result->value.length();
    if (!TO(response_obj->Set(context, v8Str(isolate_, "res_size"),
                              v8::Integer::New(isolate_, size)),
            &success) ||
        !success) {
      return {true, "Unable to set size value"};
    }
  }

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

  MetaData meta;

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

  if (cas_check &&
      req_obj->Has(context, v8Str(isolate_, cas_str_)).FromJust()) {
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

  if (expiry_check &&
      req_obj->Has(context, v8Str(isolate_, expiry_str_)).FromJust()) {
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

  v8::Local<v8::Value> keyspaceValue;
  if (req_obj->Has(context, v8Str(isolate_, "keyspace")).FromJust()) {
    if (TO_LOCAL(req_obj->Get(context, v8Str(isolate_, "keyspace")),
                 &keyspaceValue)) {

      v8::Local<v8::Object> keyspace;
      if (!TO_LOCAL(keyspaceValue->ToObject(context), &keyspace)) {
        return {false, "error in casting keyspace to Object"};
      }

      v8::Local<v8::Value> scope;
      if (keyspace->Has(context, v8Str(isolate_, scope_str_)).FromJust()) {
        if (!TO_LOCAL(keyspace->Get(context, v8Str(isolate_, scope_str_)),
                      &scope)) {
          return {false,
                  "error in reading keyspace.scope_name from 2nd argument"};
        }
        meta.scope = utils->ToCPPString(scope.As<v8::String>());
      }

      v8::Local<v8::Value> col;
      if (keyspace->Has(context, v8Str(isolate_, collection_str_)).FromJust()) {
        if (!TO_LOCAL(keyspace->Get(context, v8Str(isolate_, collection_str_)),
                      &col)) {
          return {
              false,
              "error in reading keyspace.collection_name from 2nd argument"};
        }
        meta.collection = utils->ToCPPString(col.As<v8::String>());
      }
    }
  }
  return {true, "", meta};
}

OptionsInfo BucketOps::ExtractOptionsInfo(v8::Local<v8::Value> options_object) {
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);

  OptionsData options = {false, false};

  if (!options_object->IsObject()) {
    return {false, "options argument, if present, must be object"};
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

  if (req_obj->Has(context, v8Str(isolate_, self_recursion_str_)).FromJust()) {
    v8::Local<v8::Value> recursion;
    if (!TO_LOCAL(req_obj->Get(context, v8Str(isolate_, self_recursion_str_)),
                  &recursion)) {
      return {false, "error reading 'recursion' parameter in options"};
    }
    if (!recursion->IsBoolean()) {
      return {false,
              "the 'recursion' parameter in options should be a boolean"};
    }
    auto recursion_value = recursion.As<v8::Boolean>();
    options.self_recursion = recursion_value->Value();
  }

  return {true, options};
}

MutateInSpecsInfo
BucketOps::ExtractMutateInSpecsInfo(v8::Local<v8::Value> mutateinspecs_object) {
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  auto utils = UnwrapData(isolate_)->utils;
  MutateInSpecs specs;

  if (!mutateinspecs_object->IsArray()) {
    return {false, "the 'MutateInSpecs' parameter should be an array"};
  }

  auto req_array = mutateinspecs_object.As<v8::Array>();
  for (uint32_t i = 0; i < req_array->Length(); i++) {
    v8::Local<v8::Value> array_ele;
    if (!TO_LOCAL(req_array->Get(context, i), &array_ele)) {
      return {true, "Unable to read array"};
    }

    if (!array_ele->IsObject()) {
      return {false, "Array elements should be an object"};
    }

    v8::Local<v8::Object> obj = array_ele.As<v8::Object>();
    // Extracting op type
    if (!obj->Has(context, v8Str(isolate_, spec_type_str_)).FromJust()) {
      return {false, "field not provided"};
    }

    v8::Local<v8::Value> spec_v8val;
    if (!TO_LOCAL(obj->Get(context, v8Str(isolate_, spec_type_str_)),
                  &spec_v8val)) {
      return {false, "error in casting spec_type"};
    }

    if (!spec_v8val->IsNumber()) {
      return {false, "'spec_type' should be an integer"};
    }

    v8::Local<v8::Integer> spec_v8int;
    if (!TO_LOCAL(spec_v8val->ToInteger(context), &spec_v8int)) {
      return {false, "error in casting 'spec_type'"};
    }
    auto specType = spec_v8int->Value();

    // Extracting key
    if (!obj->Has(context, v8Str(isolate_, key_subdoc_str_)).FromJust()) {
      return {false, "field not provided"};
    }

    v8::Local<v8::Value> key_v8val;
    if (!TO_LOCAL(obj->Get(context, v8Str(isolate_, key_subdoc_str_)),
                  &key_v8val)) {
      return {false, "error in casting path"};
    }

    v8::String::Utf8Value key_utf8(isolate_, key_v8val);
    std::string key(*key_utf8);

    // Extracting values
    if (!obj->Has(context, v8Str(isolate_, value_subdoc_str_)).FromJust()) {
      return {false, "field not provided"};
    }

    v8::Local<v8::Value> value_v8val;
    if (!TO_LOCAL(obj->Get(context, v8Str(isolate_, value_subdoc_str_)),
                  &value_v8val)) {
      return {false, "error in casting value"};
    }

    v8::String::Utf8Value value_utf8(isolate_, value_v8val);
    std::string value(*value_utf8);

    // Extracting options
    if (!obj->Has(context, v8Str(isolate_, options_subdoc_str_)).FromJust()) {
      specs.emplace_spec(specType, key, value, false, false);
      continue;
    }

    v8::Local<v8::Value> option_v8val;
    if (!TO_LOCAL(obj->Get(context, v8Str(isolate_, options_subdoc_str_)),
                  &option_v8val)) {
      return {false, "error in casting options"};
    }

    if (!option_v8val->IsObject()) {
      return {false, "Option elements should be an object"};
    }

    v8::Local<v8::Object> options_obj = option_v8val.As<v8::Object>();
    bool create_path = false;
    if (options_obj->Has(context, v8Str(isolate_, create_path_type_str_))
            .FromJust()) {
      v8::Local<v8::Value> create_path_v8val;
      if (TO_LOCAL(
              options_obj->Get(context, v8Str(isolate_, create_path_type_str_)),
              &create_path_v8val)) {
        if (!create_path_v8val->IsBoolean()) {
          return {false, "create_path must be a boolean value"};
        }

        auto create_path_v8 = create_path_v8val.As<v8::Boolean>();
        create_path = create_path_v8->Value();
      }
    }

    bool is_user_xattr = false;
    if (options_obj->Has(context, v8Str(isolate_, user_xattr_str_))
            .FromJust()) {
      v8::Local<v8::Value> user_xattr_v8val;
      if (TO_LOCAL(options_obj->Get(context, v8Str(isolate_, user_xattr_str_)),
                   &user_xattr_v8val)) {
        if (!user_xattr_v8val->IsBoolean()) {
          return {false, "xattr must be a boolean value"};
        }
        auto user_xattr_v8 = user_xattr_v8val.As<v8::Boolean>();
        is_user_xattr = user_xattr_v8->Value();
      }
      if (is_user_xattr && !key.empty() && key[0] == '_') {
        return {false, "XATTR path cannot start with underscore"};
      }
      if (is_user_xattr && !utils->ValidateXattrKeyLength(key)) {
        return {false, "XATTR key size must be less than 16 characters"};
      }
    }

    auto valid =
        specs.emplace_spec(specType, key, value, create_path, is_user_xattr);
    if (!valid) {
      return {false, "Invalid specs"};
    }
  }

  return {true, specs};
}

LookupInSpecsInfo
BucketOps::ExtractLookupInSpecsInfo(v8::Local<v8::Value> lookupinspecs_object) {
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  LookupInSpecs specs;

  if (!lookupinspecs_object->IsArray()) {
    return {false, "the 'LookupInSpecs' parameter should be an array"};
  }

  auto req_array = lookupinspecs_object.As<v8::Array>();
  for (uint32_t i = 0; i < req_array->Length(); i++) {
    v8::Local<v8::Value> array_ele;
    if (!TO_LOCAL(req_array->Get(context, i), &array_ele)) {
      return {true, "unable to read array"};
    }

    if (!array_ele->IsObject()) {
      return {false, "array elements should be an object"};
    }

    v8::Local<v8::Object> obj = array_ele.As<v8::Object>();
    // Extracting op type
    if (!obj->Has(context, v8Str(isolate_, spec_type_str_)).FromJust()) {
      return {false, "field not provided"};
    }

    v8::Local<v8::Value> spec_v8val;
    if (!TO_LOCAL(obj->Get(context, v8Str(isolate_, spec_type_str_)),
                  &spec_v8val)) {
      return {false, "error in casting 'spec_type'"};
    }

    if (!spec_v8val->IsNumber()) {
      return {false, "'spec_type' should be an integer"};
    }

    v8::Local<v8::Integer> spec_v8int;
    if (!TO_LOCAL(spec_v8val->ToInteger(context), &spec_v8int)) {
      return {false, "error in casting 'spec_type'"};
    }
    auto specType = spec_v8int->Value();

    // Extracting key
    if (!obj->Has(context, v8Str(isolate_, key_subdoc_str_)).FromJust()) {
      return {false, "field not provided"};
    }

    v8::Local<v8::Value> key_v8val;
    if (!TO_LOCAL(obj->Get(context, v8Str(isolate_, key_subdoc_str_)),
                  &key_v8val)) {
      return {false, "error in casting path"};
    }

    v8::String::Utf8Value key_utf8(isolate_, key_v8val);
    std::string key(*key_utf8);

    // Extracting options
    if (!obj->Has(context, v8Str(isolate_, options_subdoc_str_)).FromJust()) {
      specs.emplace_spec(specType, key, false);
      continue;
    }

    v8::Local<v8::Value> option_v8val;
    if (!TO_LOCAL(obj->Get(context, v8Str(isolate_, options_subdoc_str_)),
                  &option_v8val)) {
      return {false, "error in casting options"};
    }

    if (!option_v8val->IsObject()) {
      return {false, "option elements should be an object"};
    }

    v8::Local<v8::Object> options_obj = option_v8val.As<v8::Object>();
    bool xattr = false;
    if (options_obj->Has(context, v8Str(isolate_, user_xattr_str_))
            .FromJust()) {
      v8::Local<v8::Value> xattr_v8val;
      if (TO_LOCAL(options_obj->Get(context, v8Str(isolate_, user_xattr_str_)),
                   &xattr_v8val)) {
        if (!xattr_v8val->IsBoolean()) {
          return {false, "xattr must be a boolean value"};
        }
        auto xattr_v8 = xattr_v8val.As<v8::Boolean>();
        xattr = xattr_v8->Value();
        if (xattr && !key.empty() && key[0] == '_') {
          return {false, "XATTR path cannot start with underscore"};
        }
      }
    }

    auto valid = specs.emplace_spec(specType, key, xattr);
    if (!valid) {
      return {false, "invalid specs"};
    }
  }

  return {true, specs};
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
BucketOps::Touch(MetaData &meta, bool suppress_recursion, Bucket *bucket) {
  if (suppress_recursion) {
    return bucket->TouchWithXattr(meta);
  }
  return bucket->TouchWithoutXattr(meta);
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
BucketOps::Delete(MetaData &meta, bool suppress_recursion, Bucket *bucket) {
  if (suppress_recursion) {
    return bucket->DeleteWithXattr(meta);
  }
  return bucket->DeleteWithoutXattr(meta);
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
BucketOps::Counter(MetaData &meta, int64_t delta, bool suppress_recursion,
                   Bucket *bucket) {
  if (suppress_recursion) {
    return bucket->CounterWithXattr(meta, delta);
  }
  return bucket->CounterWithoutXattr(meta, delta);
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
BucketOps::Set(MetaData &meta, const std::string &value,
               lcb_STORE_OPERATION op_type, lcb_U32 doc_type,
               bool suppress_recursion, Bucket *bucket) {
  if (suppress_recursion) {
    lcb_SUBDOC_STORE_SEMANTICS cmd_flag = LCB_SUBDOC_STORE_REPLACE;
    if (op_type == LCB_STORE_UPSERT) {
      cmd_flag = LCB_SUBDOC_STORE_UPSERT;
    } else if (op_type == LCB_STORE_INSERT) {
      cmd_flag = LCB_SUBDOC_STORE_INSERT;
    }

    return bucket->SetWithXattr(meta, value, cmd_flag);
  }
  return bucket->SetWithoutXattr(meta, value, op_type, doc_type);
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
BucketOps::BucketSet(MetaData &meta, v8::Local<v8::Value> value,
                     lcb_STORE_OPERATION op_type, bool suppress_recursion,
                     Bucket *bucket) {
  v8::HandleScope scope(isolate_);
  std::string value_str;
  lcb_U32 doc_type = 0x00000000;
  if (value->IsArrayBuffer()) {
    auto array_buf = value.As<v8::ArrayBuffer>();
    auto store = array_buf->GetBackingStore();
    value_str.assign(static_cast<const char *>(store->Data()),
                     store->ByteLength());
  } else {
    value_str = JSONStringify(isolate_, value);
    doc_type = 0x2000000;
  }
  return Set(meta, value_str, op_type, doc_type, suppress_recursion, bucket);
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
BucketOps::BucketMutateIn(MetaData &meta, MutateInSpecs &value,
                          bool suppress_recursion, Bucket *bucket) {
  if (suppress_recursion) {
    return bucket->MutateInWithXattr(meta, value);
  }
  return bucket->MutateInWithoutXattr(meta, value);
}

void BucketOps::Details(v8::FunctionCallbackInfo<v8::Value> args) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  auto isolate_data = UnwrapData(isolate_);
  auto js_exception = isolate_data->js_exception;
  const auto v8worker = isolate_data->v8worker2;

  if (args.Length() < 2) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(
        "couchbase.bindingDetails requires at least 2 argument");
    return;
  }

  auto info = VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate_, args[0]);
  auto access = "rw";
  if (block_mutation) {
    access = "r";
  }

  auto meta_info = ExtractMetaInfo(args[1], false, false);
  if (!meta_info.is_valid) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  auto bucket = BucketBinding::GetBucket(isolate_, args[0]);
  auto [err, scope, collection] = bucket->get_scope_and_collection_names(meta);
  if (err != nullptr) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError(*err);
    return;
  }

  bool success = false;
  auto bucket_name = bucket->BucketName();

  v8::Local<v8::Object> response_obj = v8::Object::New(isolate_);
  v8::Local<v8::Object> meta_obj = v8::Object::New(isolate_);

  if (!TO(meta_obj->Set(context, v8Str(isolate_, "bucket"),
                        v8Str(isolate_, bucket_name)),
          &success) ||
      !success) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError("Temp error");
  }

  if (!TO(meta_obj->Set(context, v8Str(isolate_, "scope"),
                        v8Str(isolate_, scope)),
          &success) ||
      !success) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError("Temp error");
  }

  if (!TO(meta_obj->Set(context, v8Str(isolate_, "collection"),
                        v8Str(isolate_, collection)),
          &success) ||
      !success) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError("Temp error");
  }

  if (!TO(response_obj->Set(context, v8Str(isolate_, "access"),
                            v8Str(isolate_, access)),
          &success) ||
      !success) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError("Temp error");
  }

  if (!TO(response_obj->Set(context, v8Str(isolate_, "keyspace"), meta_obj),
          &success) ||
      !success) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError("Temp error");
  }

  args.GetReturnValue().Set(response_obj);
}

void BucketOps::CounterOps(v8::FunctionCallbackInfo<v8::Value> args,
                           int64_t delta) {
  v8::HandleScope handle_scope(isolate_);

  auto isolate_data = UnwrapData(isolate_);
  auto js_exception = isolate_data->js_exception;
  const auto v8worker = isolate_data->v8worker2;

  if (args.Length() < 2) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError(
        "couchbase.counter requires at least 2 arguments");
    return;
  }

  auto info = VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate_, args[0]);
  if (block_mutation) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  auto meta_info = ExtractMetaInfo(args[1]);
  if (!meta_info.is_valid) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  auto bucket = BucketBinding::GetBucket(isolate_, args[0]);
  auto [err, is_source_mutation] =
      BucketBinding::IsSourceMutation(isolate_, args[0], meta);
  if (err != nullptr) {
    js_exception->ThrowEventingError(*err);
    return;
  }

  auto [error, err_code, result] =
      Counter(meta, delta, is_source_mutation, bucket);

  if (error != nullptr) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
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
                          result->kv_err_code, invalid_counter_str_, true);

    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      ;
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
        result->kv_err_code, cas_mismatch_str_, true);
    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      ;
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    v8worker->stats_->IncrementFailureStat("bkt_ops_cas_mismatch_count");
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
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
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
  const auto v8worker = isolate_data->v8worker2;

  if (args.Length() < 2) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError("couchbase.get requires at least 2 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1]);
  if (!meta_info.is_valid) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  OptionsData options = {false, false};
  if (args.Length() > 2) {
    auto options_info = bucket_ops->ExtractOptionsInfo(args[2]);
    if (!options_info.is_valid) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      js_exception->ThrowTypeError(options_info.msg);
      return;
    }
    options = options_info.options;
  }

  auto bucket = BucketBinding::GetBucket(isolate, args[0]);
  v8::Local<v8::Object> response_obj = v8::Object::New(isolate);
  if (options.cache) {
    v8worker->stats_->IncrementFailureStat("bucket_op_cache_miss_count");
  }

  auto [error, err_code, result] = bucket->GetWithMeta(meta);
  if (error != nullptr) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
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
        "The document key does not exist on the server", result->kv_err_code,
        bucket_ops->key_not_found_str_, true);

    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
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

  info = bucket_ops->ResponseSuccessObject(std::move(result), response_obj,
                                           true, false, true);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
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
  const auto v8worker = isolate_data->v8worker2;

  if (args.Length() < 3) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError(
        "couchbase.insert requires at least 3 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if (block_mutation) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], false, true);
  if (!meta_info.is_valid) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }

  info = Utils::ValidateDataType(args[2]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    auto err_msg = "Invalid data type for 3rd argument: " + info.msg;
    js_exception->ThrowTypeError(err_msg);
    return;
  }

  OptionsData options = {false, false};
  if (args.Length() > 3) {
    auto options_info = bucket_ops->ExtractOptionsInfo(args[3]);
    if (!options_info.is_valid) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      js_exception->ThrowTypeError(options_info.msg);
      return;
    }
    options = options_info.options;
  }

  auto meta = meta_info.meta;

  auto suppress_recursion = !options.self_recursion;
  if (suppress_recursion) {
    auto [err, is_source_mutation] =
        BucketBinding::IsSourceMutation(isolate, args[0], meta);
    if (err != nullptr) {
      js_exception->ThrowEventingError(*err);
      return;
    }
    suppress_recursion = is_source_mutation;
  }

  auto bucket = BucketBinding::GetBucket(isolate, args[0]);

  auto [error, err_code, result] = bucket_ops->BucketSet(
      meta, args[2], LCB_STORE_INSERT, suppress_recursion, bucket);

  if (error != nullptr) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowEventingError(*error);
    return;
  }

  if (*err_code != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), *err_code);
    return;
  }

  v8::Local<v8::Object> response_obj = v8::Object::New(isolate);
  // Cas mismatch check is due to CCBC-1382
  if (result->rc == LCB_ERR_DOCUMENT_EXISTS ||
      result->rc == LCB_ERR_CAS_MISMATCH) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_EEXISTS",
        "The document key already exists in the server.", result->kv_err_code,
        bucket_ops->key_exist_str_, true);

    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      ;
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
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
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
  const auto v8worker = isolate_data->v8worker2;

  if (args.Length() < 3) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError(
        "couchbase.upsert requires at least 3 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if (block_mutation) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], true, true);
  if (!meta_info.is_valid) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  info = Utils::ValidateDataType(args[2]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    auto err_msg = "Invalid data type for 3rd argument: " + info.msg;
    js_exception->ThrowTypeError(err_msg);
    return;
  }

  OptionsData options = {false, false};
  if (args.Length() > 3) {
    auto options_info = bucket_ops->ExtractOptionsInfo(args[3]);
    if (!options_info.is_valid) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      js_exception->ThrowTypeError(options_info.msg);
      return;
    }
    options = options_info.options;
  }

  auto suppress_recursion = !options.self_recursion;
  if (suppress_recursion) {
    auto [err, is_source_mutation] =
        BucketBinding::IsSourceMutation(isolate, args[0], meta);
    if (err != nullptr) {
      js_exception->ThrowEventingError(*err);
      return;
    }
    suppress_recursion = is_source_mutation;
  }

  auto bucket = BucketBinding::GetBucket(isolate, args[0]);

  auto [error, err_code, result] = bucket_ops->BucketSet(
      meta, args[2], LCB_STORE_REPLACE, suppress_recursion, bucket);

  if (error != nullptr) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowEventingError(*error);
    return;
  }

  if (*err_code != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), *err_code);
    return;
  }

  v8::Local<v8::Object> response_obj = v8::Object::New(isolate);

  if (result->rc == LCB_ERR_CAS_MISMATCH) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_EEXISTS",
        "The document key exists with a CAS value different than specified",
        result->kv_err_code, bucket_ops->cas_mismatch_str_, true);

    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      ;
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    v8worker->stats_->IncrementFailureStat("bkt_ops_cas_mismatch_count");
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc == LCB_ERR_DOCUMENT_NOT_FOUND) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_ENOENT",
        "The document key does not exist on the server", result->kv_err_code,
        bucket_ops->key_not_found_str_, true);

    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      ;
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
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
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
  const auto v8worker = isolate_data->v8worker2;

  if (args.Length() < 3) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError(
        "couchbase.upsert requires at least 3 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if (block_mutation) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], false, true);
  if (!meta_info.is_valid) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  info = Utils::ValidateDataType(args[2]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
    auto err_msg = "Invalid data type for 3rd argument: " + info.msg;
    js_exception->ThrowTypeError(err_msg);
    return;
  }

  OptionsData options = {false, false};
  if (args.Length() > 3) {
    auto options_info = bucket_ops->ExtractOptionsInfo(args[3]);
    if (!options_info.is_valid) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      js_exception->ThrowTypeError(options_info.msg);
      return;
    }
    options = options_info.options;
  }

  auto suppress_recursion = !options.self_recursion;
  if (suppress_recursion) {
    auto [err, is_source_mutation] =
        BucketBinding::IsSourceMutation(isolate, args[0], meta);
    if (err != nullptr) {
      js_exception->ThrowEventingError(*err);
      return;
    }
    suppress_recursion = is_source_mutation;
  }

  auto bucket = BucketBinding::GetBucket(isolate, args[0]);
  auto [error, err_code, result] = bucket_ops->BucketSet(
      meta, args[2], LCB_STORE_UPSERT, suppress_recursion, bucket);

  if (error != nullptr) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
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
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    ;
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
  const auto v8worker = isolate_data->v8worker2;

  if (args.Length() < 2) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(
        "couchbase.delete requires at least 2 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if (block_mutation) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError("Delete from source bucket is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], true);
  if (!meta_info.is_valid) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  auto bucket = BucketBinding::GetBucket(isolate, args[0]);
  auto [err, is_source_mutation] =
      BucketBinding::IsSourceMutation(isolate, args[0], meta);
  if (err != nullptr) {
    js_exception->ThrowEventingError(*err);
    return;
  }

  auto [error, err_code, result] =
      bucket_ops->Delete(meta, is_source_mutation, bucket);
  if (error != nullptr) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError(*error);
    return;
  }

  if (*err_code != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), *err_code);
    return;
  }

  v8::Local<v8::Object> response_obj = v8::Object::New(isolate);
  if (result->rc == LCB_ERR_CAS_MISMATCH) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_EEXISTS",
        "The document key exists with a CAS value different than specified",
        result->kv_err_code, bucket_ops->cas_mismatch_str_, true);

    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    v8worker->stats_->IncrementFailureStat("bkt_ops_cas_mismatch_count");
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc == LCB_ERR_DOCUMENT_NOT_FOUND) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_ENOENT",
        "The document key does not exist on the server", result->kv_err_code,
        bucket_ops->key_not_found_str_, true);

    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
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
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
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

void BucketOps::TouchOp(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
    return;
  }

  auto bucket_ops = isolate_data->bucket_ops;
  auto js_exception = isolate_data->js_exception;
  const auto v8worker = isolate_data->v8worker2;

  if (args.Length() < 2) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(
        "couchbase.touch requires at least 2 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if (block_mutation) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError("Touch operation is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], false, true);
  if (!meta_info.is_valid) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  auto bucket = BucketBinding::GetBucket(isolate, args[0]);
  auto [err, is_source_mutation] =
      BucketBinding::IsSourceMutation(isolate, args[0], meta);
  if (err != nullptr) {
    js_exception->ThrowEventingError(*err);
    return;
  }

  auto [error, err_code, result] =
      bucket_ops->Touch(meta, is_source_mutation, bucket);
  if (error != nullptr) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError(*error);
    return;
  }

  result->key = meta.key;
  if (*err_code != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), *err_code);
    return;
  }

  v8::Local<v8::Object> response_obj = v8::Object::New(isolate);
  if (result->rc == LCB_ERR_DOCUMENT_NOT_FOUND) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_ENOENT",
        "The document key does not exist on the server", result->kv_err_code,
        bucket_ops->key_not_found_str_, true);

    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
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

  info = bucket_ops->ResponseSuccessObject(std::move(result), response_obj);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError(info.msg);
    return;
  }

  args.GetReturnValue().Set(response_obj);
}

void BucketOps::BindingDetails(
    const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
    return;
  }

  auto bucket_ops = isolate_data->bucket_ops;
  bucket_ops->Details(args);
}

void BucketOps::MutateInOp(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  v8::HandleScope handle_scope(isolate);

  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
    return;
  }

  auto js_exception = isolate_data->js_exception;
  auto bucket_ops = isolate_data->bucket_ops;
  const auto v8worker = isolate_data->v8worker2;

  if (args.Length() < 3) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(
        "couchbase.MutateIn requires at least 3 arguments");
    return;
  }

  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto block_mutation = BucketBinding::GetBlockMutation(isolate, args[0]);
  if (block_mutation) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], true, true);
  if (!meta_info.is_valid) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  info = Utils::ValidateDataType(args[2]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    auto err_msg = "Invalid data type for 3rd argument: " + info.msg;
    js_exception->ThrowTypeError(err_msg);
    return;
  }

  OptionsData options = {false, false};
  if (args.Length() > 3) {
    auto options_info = bucket_ops->ExtractOptionsInfo(args[3]);
    if (!options_info.is_valid) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      js_exception->ThrowTypeError(options_info.msg);
      return;
    }
    options = options_info.options;
  }

  auto suppress_recursion = !options.self_recursion;
  if (suppress_recursion) {
    auto [err, is_source_mutation] =
        BucketBinding::IsSourceMutation(isolate, args[0], meta);
    if (err != nullptr) {
      js_exception->ThrowEventingError(*err);
      return;
    }
    suppress_recursion = is_source_mutation;
  }

  auto bucket = BucketBinding::GetBucket(isolate, args[0]);
  auto mutateinspecs_info = bucket_ops->ExtractMutateInSpecsInfo(args[2]);
  if (!mutateinspecs_info.is_valid) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(mutateinspecs_info.msg);
    return;
  }

  auto [error, err_code, result] = bucket_ops->BucketMutateIn(
      meta, mutateinspecs_info.specs, suppress_recursion, bucket);

  if (error != nullptr) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError(*error);
    return;
  }

  if (*err_code != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), *err_code);
    return;
  }

  v8::Local<v8::Object> response_obj = v8::Object::New(isolate);
  if (result->rc == LCB_ERR_CAS_MISMATCH) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_EEXISTS",
        "The document key exists with a CAS value different than specified",
        result->kv_err_code, bucket_ops->cas_mismatch_str_, true);

    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    v8worker->stats_->IncrementFailureStat("bkt_ops_cas_mismatch_count");
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc == LCB_ERR_DOCUMENT_NOT_FOUND) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_ENOENT",
        "The document key does not exist on the server", result->kv_err_code,
        bucket_ops->key_not_found_str_, true);

    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc == LCB_ERR_SUBDOC_PATH_EXISTS) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_ERR_SUBDOC_PATH_EXISTS",
        "The document path already exist", result->kv_err_code,
        bucket_ops->field_exist_str_, true);

    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    args.GetReturnValue().Set(response_obj);
    return;
  }

  if (result->rc == LCB_ERR_SUBDOC_PATH_NOT_FOUND) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_ERR_SUBDOC_PATH_NOT_FOUND",
        "The document key does not exist on the server", result->kv_err_code,
        bucket_ops->field_not_found_str_, true);

    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
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
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError(info.msg);
    return;
  }

  args.GetReturnValue().Set(response_obj);
}

void BucketOps::LookupInOp(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  v8::HandleScope handle_scope(isolate);

  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
    return;
  }

  auto js_exception = isolate_data->js_exception;
  auto bucket_ops = isolate_data->bucket_ops;
  const auto v8worker = isolate_data->v8worker2;

  if (args.Length() < 3) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(
        "couchbase.LookupIn requires at least 3 arguments");
    return;
  }

  // Validation and extraction phase
  auto info = bucket_ops->VerifyBucketObject(args[0]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(info.msg);
    return;
  }

  auto meta_info = bucket_ops->ExtractMetaInfo(args[1], true, true);
  if (!meta_info.is_valid) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(meta_info.msg);
    return;
  }
  auto meta = meta_info.meta;

  info = Utils::ValidateDataType(args[2]);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    auto err_msg = "Invalid data type for 3rd argument: " + info.msg;
    js_exception->ThrowTypeError(err_msg);
    return;
  }

  // Setup objects
  auto bucket = BucketBinding::GetBucket(isolate, args[0]);
  auto lookupinspecs_info = bucket_ops->ExtractLookupInSpecsInfo(args[2]);
  if (!lookupinspecs_info.is_valid) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowTypeError(lookupinspecs_info.msg);
    return;
  }
  auto [error, err_code, result] =
      bucket->LookupIn(meta, lookupinspecs_info.specs);
  if (error != nullptr) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError(*error);
    return;
  }

  if (*err_code != LCB_SUCCESS) {
    bucket_ops->HandleBucketOpFailure(bucket->GetConnection(), *err_code);
    return;
  }

  v8::Local<v8::Object> response_obj = v8::Object::New(isolate);
  if (result->rc == LCB_ERR_DOCUMENT_NOT_FOUND) {
    info = bucket_ops->SetErrorObject(
        response_obj, "LCB_KEY_ENOENT",
        "The document key does not exist on the server", result->kv_err_code,
        bucket_ops->key_not_found_str_, true);

    if (info.is_fatal) {
      v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
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
  info =
      bucket_ops->ResponseSuccessObject(std::move(result), response_obj, true);
  if (info.is_fatal) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError(info.msg);
    return;
  }

  args.GetReturnValue().Set(response_obj);
}
