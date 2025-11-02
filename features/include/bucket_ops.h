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

#ifndef BUCKETOPS_H
#define BUCKETOPS_H

#include "bucket.h"
#include "info.h"
#include "lcb_utils.h"
#include <v8.h>

struct OptionsData {
  bool cache;
};

struct MetaInfo {
  MetaInfo() : is_valid(false) {}
  MetaInfo(bool is_valid) : is_valid(is_valid) {}
  MetaInfo(bool is_valid, std::string msg)
      : is_valid(is_valid), msg(std::move(msg)) {}
  MetaInfo(bool is_valid, std::string &&msg, MetaData meta)
      : is_valid(is_valid), msg(std::move(msg)), meta(std::move(meta)) {}

  bool is_valid;
  std::string msg;
  MetaData meta;
};

struct OptionsInfo {
  OptionsInfo() : is_valid(false) {}
  OptionsInfo(bool is_valid) : is_valid(is_valid) {}
  OptionsInfo(bool is_valid, std::string msg)
      : is_valid(is_valid), msg(std::move(msg)) {}
  OptionsInfo(bool is_valid, OptionsData meta)
      : is_valid(is_valid), options(std::move(meta)) {}

  bool is_valid;
  std::string msg;
  OptionsData options;
};

struct EpochInfo {
  EpochInfo(bool is_valid) : is_valid(is_valid), epoch(0) {}

  EpochInfo(bool is_valid, int64_t epoch) : is_valid(is_valid), epoch(epoch) {}

  bool is_valid;
  int64_t epoch;
};

class BucketOps {
public:
  BucketOps(v8::Isolate *isolate, const v8::Local<v8::Context> &context);

  ~BucketOps();
  static void GetOp(const v8::FunctionCallbackInfo<v8::Value> &args);
  static void InsertOp(const v8::FunctionCallbackInfo<v8::Value> &args);
  static void UpsertOp(const v8::FunctionCallbackInfo<v8::Value> &args);
  static void ReplaceOp(const v8::FunctionCallbackInfo<v8::Value> &args);
  static void DeleteOp(const v8::FunctionCallbackInfo<v8::Value> &args);
  static void IncrementOp(const v8::FunctionCallbackInfo<v8::Value> &args);
  static void DecrementOp(const v8::FunctionCallbackInfo<v8::Value> &args);
  static void BindingDetails(const v8::FunctionCallbackInfo<v8::Value> &args);

private:
  EpochInfo Epoch(const v8::Local<v8::Value> &date_val);

  MetaInfo ExtractMetaInfo(v8::Local<v8::Value> meta_object,
                           bool cas_check = false, bool expiry_check = false);

  OptionsInfo ExtractOptionsInfo(v8::Local<v8::Value> options_object);

  Info ResponseSuccessObject(std::unique_ptr<Result> const &result,
                             v8::Local<v8::Object> &response_obj,
                             bool is_doc_needed = false,
                             bool counter_needed = false);

  Info VerifyBucketObject(v8::Local<v8::Value> bucket_binding);

  Info SetDocBody(std::unique_ptr<Result> const &result,
                  v8::Local<v8::Object> &response_obj);
  Info SetMetaObject(std::unique_ptr<Result> const &result,
                     v8::Local<v8::Object> &response_obj);
  Info SetCounterData(std::unique_ptr<Result> const &result,
                      v8::Local<v8::Object> &response_obj);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  Delete(MetaData &meta, bool is_source_bucket, Bucket *bucket);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  Counter(MetaData &meta, int64_t delta, bool is_source_bucket, Bucket *bucket);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  Set(MetaData &meta, const std::string &value, lcb_STORE_OPERATION op_type,
      lcb_U32 doc_type, bool is_source_bucket, Bucket *bucket);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  BucketSet(MetaData &meta, v8::Local<v8::Value> data,
            lcb_STORE_OPERATION op_type, bool is_source_bucket, Bucket *bucket);

  void CounterOps(v8::FunctionCallbackInfo<v8::Value> args, int64_t delta);
  void Details(v8::FunctionCallbackInfo<v8::Value> args);

  void HandleBucketOpFailure(lcb_INSTANCE *connection, lcb_STATUS error);
  Info SetErrorObject(v8::Local<v8::Object> &response_obj, std::string name,
                      std::string desc, uint16_t error_code,
                      const char *error_type, bool value);

  v8::Isolate *isolate_;
  v8::Global<v8::Context> context_;

  const char *cas_str_;
  const char *key_str_;
  const char *expiry_str_;
  const char *collection_str_;
  const char *scope_str_;
  const char *data_type_str_;
  const char *key_not_found_str_;
  const char *cas_mismatch_str_;
  const char *key_exist_str_;
  const char *doc_str_;
  const char *meta_str_;
  const char *counter_str_;
  const char *error_str_;
  const char *success_str_;
  const char *json_str_;
  const char *binary_str_;
  const char *invalid_counter_str_;
  const char *cache_str_;
};

#endif
