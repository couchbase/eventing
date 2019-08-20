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

#include <libcouchbase/api3.h>
#include <libcouchbase/couchbase.h>
#include <libcouchbase/subdoc.h>
#include <string>
#include <vector>

#include "v8worker.h"

#define LCB_INST_FIELD_NO 0
#define BLOCK_MUTATION_FIELD_NO 1

class Bucket {
public:
  Bucket(V8Worker *w, const char *bname, const char *ep, const char *alias,
         bool block_mutation);
  ~Bucket();

  bool Initialize(V8Worker *w);

  v8::Global<v8::ObjectTemplate> bucket_map_template_;
  lcb_t bucket_lcb_obj_;

private:
  bool InstallMaps();
  static void HandleBucketOpFailure(v8::Isolate *isolate,
                                    lcb_t bucket_lcb_obj_ptr,
                                    lcb_error_t error);
  v8::Local<v8::ObjectTemplate> MakeBucketMapTemplate();

  static Info ValidateKey(const v8::Local<v8::Name> &arg);
  static Info ValidateValue(const v8::Local<v8::Value> &arg);
  static Info ValidateKeyValue(const v8::Local<v8::Name> &key,
                               const v8::Local<v8::Value> &value);
  template <typename T> static Info Validate(const v8::Local<T> &arg);

  // Delegate is used to multiplex alphanumeric and numeric accesses on bucket
  // object in JavaScript
  template <typename T>
  static void
  BucketGetDelegate(T key, const v8::PropertyCallbackInfo<v8::Value> &info);
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

  template <typename>
  static void BucketDelete(v8::Local<v8::Name> key,
                           const v8::PropertyCallbackInfo<v8::Boolean> &info);
  template <typename>
  static void BucketDelete(uint32_t key,
                           const v8::PropertyCallbackInfo<v8::Boolean> &info);

  v8::Local<v8::Object> WrapBucketMap();

  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;

  bool block_mutation_;
  std::string bucket_name_;
  std::string endpoint_;
  std::string bucket_alias_;

  V8Worker *worker_;
};

#endif
