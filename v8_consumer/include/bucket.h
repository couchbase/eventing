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

#include <map>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <vector>

#include <libcouchbase/api3.h>
#include <libcouchbase/couchbase.h>

#include "v8worker.h"

class Bucket {
public:
  Bucket(V8Worker *w, const char *bname, const char *ep, const char *alias,
         std::string rbac_user, std::string rbac_pass);
  ~Bucket();

  virtual bool Initialize(V8Worker *w,
                          std::map<std::string, std::string> *bucket);

  v8::Isolate *GetIsolate() { return isolate_; }
  std::string GetBucketName() { return bucket_name; }
  std::string GetEndPoint() { return endpoint; }

  v8::Global<v8::ObjectTemplate> bucket_map_template_;

  lcb_t bucket_lcb_obj;

private:
  bool InstallMaps(std::map<std::string, std::string> *bucket);

  static v8::Local<v8::ObjectTemplate>
  MakeBucketMapTemplate(v8::Isolate *isolate);

  static void BucketGet(v8::Local<v8::Name> name,
                        const v8::PropertyCallbackInfo<v8::Value> &info);
  static void BucketSet(v8::Local<v8::Name> name, v8::Local<v8::Value> value,
                        const v8::PropertyCallbackInfo<v8::Value> &info);
  static void BucketDelete(v8::Local<v8::Name> name,
                           const v8::PropertyCallbackInfo<v8::Boolean> &info);

  v8::Local<v8::Object>
  WrapBucketMap(std::map<std::string, std::string> *bucket);

  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;

  std::string bucket_name;
  std::string endpoint;
  std::string bucket_alias;

  V8Worker *worker;
};

#endif
