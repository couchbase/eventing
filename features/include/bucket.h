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

#ifndef BUCKET_H
#define BUCKET_H

#include <libcouchbase/couchbase.h>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <v8.h>

#include "error.h"
#include "info.h"
#include "isolate_data.h"
#include "lcb_utils.h"

class BucketFactory {
public:
  BucketFactory(v8::Isolate *isolate, const v8::Local<v8::Context> &context);
  ~BucketFactory();

  BucketFactory(const BucketFactory &) = delete;
  BucketFactory(BucketFactory &&) = delete;
  BucketFactory &operator=(const BucketFactory &) = delete;
  BucketFactory &operator=(BucketFactory &&) = delete;

  std::pair<Error, std::unique_ptr<v8::Local<v8::Object>>> NewBucketObj() const;

private:
  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;
  v8::Persistent<v8::ObjectTemplate> bucket_template_;
};

class Bucket {
public:
  Bucket(v8::Isolate *isolate, std::string bucket_name)
      : isolate_(isolate), bucket_name_(std::move(bucket_name)) {}
  ~Bucket();

  Bucket(const Bucket &) = default;
  Bucket(Bucket &&) = delete;
  Bucket &operator=(const Bucket &) = delete;
  Bucket &operator=(Bucket &&) = delete;

  Error Connect();

  std::tuple<Error, std::unique_ptr<lcb_error_t>, std::unique_ptr<Result>>
  Get(const std::string &key);

  std::tuple<Error, std::unique_ptr<lcb_error_t>, std::unique_ptr<Result>>
  SetWithXattr(const std::string &key, const void* value, int value_length,
               lcb_U32 op_type = LCB_CMDSUBDOC_F_UPSERT_DOC, lcb_U32 expiry = 0,
               lcb_CAS cas = 0);

  std::tuple<Error, std::unique_ptr<lcb_error_t>, std::unique_ptr<Result>>
  SetWithoutXattr(const std::string &key, const void* value, int value_length,
                  lcb_storage_t op_type = LCB_SET, lcb_U32 expiry = 0, lcb_CAS cas = 0,
                  lcb_U32 doc_type = 0x2000000);

  std::tuple<Error, std::unique_ptr<lcb_error_t>, std::unique_ptr<Result>>
  DeleteWithXattr(const std::string &key, lcb_CAS cas = 0);

  std::tuple<Error, std::unique_ptr<lcb_error_t>, std::unique_ptr<Result>>
  DeleteWithoutXattr(const std::string &key, lcb_CAS cas = 0);

  std::tuple<Error, std::unique_ptr<lcb_error_t>, std::unique_ptr<Result>>
  GetWithMeta(const std::string &key);

  std::tuple<Error, std::unique_ptr<lcb_error_t>, std::unique_ptr<Result>>
  CounterWithXattr(const std::string &key, lcb_CAS cas, lcb_U32 expiry, std::string delta);

  std::tuple<Error, std::unique_ptr<lcb_error_t>, std::unique_ptr<Result>>
  CounterWithoutXattr(const std::string &key, lcb_CAS cas, lcb_U32 expiry, std::string delta);

  lcb_t GetConnection() const { return connection_; }

private:
  Error FormatErrorAndDestroyConn(const std::string &message,
                                  const lcb_error_t &error) const;

  v8::Isolate *isolate_{nullptr};
  std::string bucket_name_;
  lcb_t connection_{nullptr};
  bool is_connected_{false};
};

class BucketBinding {
  friend BucketFactory;
  friend BucketOps;

public:
  BucketBinding(v8::Isolate *isolate, std::shared_ptr<BucketFactory> factory,
                const std::string &bucket_name, std::string alias,
                bool block_mutation, bool is_source_bucket)
      : block_mutation_(block_mutation), is_source_bucket_(is_source_bucket),
        bucket_name_(bucket_name), bucket_alias_(std::move(alias)),
        factory_(std::move(factory)), bucket_(isolate, bucket_name) {}

  Error InstallBinding(v8::Isolate *isolate,
                       const v8::Local<v8::Context> &context);

  static bool IsBucketObject(v8::Isolate *isolate, const v8::Local<v8::Object> obj);
  static Bucket* GetBucket(v8::Isolate *isolate, const v8::Local<v8::Value> obj);
  static bool GetBlockMutation(v8::Isolate *isolate, const v8::Local<v8::Value> obj);
  static bool IsSourceBucket(v8::Isolate *isolate, const v8::Local<v8::Value> obj);

private:
  static void HandleBucketOpFailure(v8::Isolate *isolate, lcb_t connection,
                                    lcb_error_t error);
  static Info ValidateKey(const v8::Local<v8::Name> &arg);
  static Info ValidateValue(const v8::Local<v8::Value> &arg);
  static Info ValidateKeyValue(const v8::Local<v8::Name> &key,
                               const v8::Local<v8::Value> &value);
  template <typename T> static Info Validate(const v8::Local<T> &arg);

  // Delegate is used to multiplex alphanumeric and numeric accesses on bucket
  // object in JavaScript
  template <typename T>
  static void
  BucketGetDelegate(T name, const v8::PropertyCallbackInfo<v8::Value> &info);
  template <typename T>
  static void
  BucketSetDelegate(T key, v8::Local<v8::Value> value,
                    const v8::PropertyCallbackInfo<v8::Value> &info);
  template <typename T>
  static void
  BucketDeleteDelegate(T key,
                       const v8::PropertyCallbackInfo<v8::Boolean> &info);

  // Specialization functions which will receive the delegate
  // Only one overload performs the actual work of making lcb calls
  // The other overload simply forwards the delegate to the one doing the
  // actual work
  template <typename>
  static void BucketGet(const v8::Local<v8::Name> &key,
                        const v8::PropertyCallbackInfo<v8::Value> &info);
  template <typename>
  static void BucketGet(uint32_t key,
                        const v8::PropertyCallbackInfo<v8::Value> &info);

  template <typename>
  static void BucketSet(const v8::Local<v8::Name> &key,
                        const v8::Local<v8::Value> &value,
                        const v8::PropertyCallbackInfo<v8::Value> &info);
  template <typename>
  static void BucketSet(uint32_t key, const v8::Local<v8::Value> &value,
                        const v8::PropertyCallbackInfo<v8::Value> &info);

  static std::tuple<Error, std::unique_ptr<lcb_error_t>,
                    std::unique_ptr<Result>>
  BucketSet(const std::string &key, const std::string &value,
            bool is_source_bucket, Bucket *bucket);

  template <typename>
  static void BucketDelete(const v8::Local<v8::Name> &key,
                           const v8::PropertyCallbackInfo<v8::Boolean> &info);
  template <typename>
  static void BucketDelete(uint32_t key,
                           const v8::PropertyCallbackInfo<v8::Boolean> &info);

  static std::tuple<Error, std::unique_ptr<lcb_error_t>,
                    std::unique_ptr<Result>>
  BucketDelete(const std::string &key, bool is_source_bucket, Bucket *bucket);

  static void
  BucketDeleteWithXattr(const v8::Local<v8::Name> &key,
                        const v8::PropertyCallbackInfo<v8::Boolean> &info);

  static void
  BucketDeleteWithoutXattr(const v8::Local<v8::Name> &key,
                           const v8::PropertyCallbackInfo<v8::Boolean> &info);

  static void HandleEnoEnt(v8::Isolate *isolate,
                           const v8::PropertyCallbackInfo<v8::Value> &info,
                           lcb_t instance);

  static void HandleEnoEnt(v8::Isolate *isolate, lcb_t instance);

  bool block_mutation_;
  bool is_source_bucket_;
  std::string bucket_name_;
  std::string bucket_alias_;
  std::shared_ptr<BucketFactory> factory_;
  Bucket bucket_;

  enum InternalFields {
    kBucketInstance,
    kBlockMutation,
    kIsSourceBucket,
    kBucketBindingId,
    kInternalFieldsCount
  };
};

// TODO : Must be implemented by the component that wants to use Bucket
void AddLcbException(const IsolateData *isolate_data, lcb_error_t error);
std::string GetFunctionInstanceID(v8::Isolate *isolate);

#endif
