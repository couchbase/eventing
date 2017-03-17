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

#ifndef V8WORKER_H
#define V8WORKER_H

#include <map>
#include <string>

#include <include/libplatform/libplatform.h>
#include <include/v8-debug.h>
#include <include/v8.h>

#include <libcouchbase/api3.h>
#include <libcouchbase/couchbase.h>

#include "log.h"

#ifndef STANDALONE_BUILD
extern void assert(int);
#else
#include <cassert>
#endif

class Bucket;
class V8Worker;

struct Result {
  std::string value;
  lcb_CAS cas;
  lcb_U32 itmflags;
  lcb_error_t status;

  Result() : cas(0), itmflags(0), status(LCB_SUCCESS) {}
};

v8::Local<v8::String> createUtf8String(v8::Isolate *isolate, const char *str);
std::string ObjectToString(v8::Local<v8::Value> value);
std::string ToString(v8::Isolate *isolate, v8::Handle<v8::Value> object);

lcb_t *UnwrapLcbInstance(v8::Local<v8::Object> obj);
lcb_t *UnwrapV8WorkerLcbInstance(v8::Local<v8::Object> obj);
V8Worker *UnwrapV8WorkerInstance(v8::Local<v8::Object> obj);

std::map<std::string, std::string> *UnwrapMap(v8::Local<v8::Object> obj);

class ArrayBufferAllocator : public v8::ArrayBuffer::Allocator {
public:
  virtual void *Allocate(size_t length) {
    void *data = AllocateUninitialized(length);
    return data == NULL ? data : memset(data, 0, length);
  }
  virtual void *AllocateUninitialized(size_t length) { return malloc(length); }
  virtual void Free(void *data, size_t) { free(data); }
};

class V8Worker {
public:
  V8Worker(std::string app_name, std::string dep_cfg, std::string kv_host_port);
  ~V8Worker();

  int V8WorkerLoad(std::string source_s);
  const char *V8WorkerLastException();
  const char *V8WorkerVersion();

  int SendUpdate(std::string value, std::string meta, std::string doc_type);
  int SendDelete(std::string meta);

  void V8WorkerDispose();
  void V8WorkerTerminateExecution();

  v8::Isolate *GetIsolate() { return isolate_; }
  v8::Persistent<v8::Context> context_;

  v8::Persistent<v8::Function> on_update_;
  v8::Persistent<v8::Function> on_delete_;

  v8::Global<v8::ObjectTemplate> worker_template;

  lcb_t cb_instance;

  std::string script_to_execute_;
  std::string app_name_;

  std::string cb_kv_endpoint;
  std::string cb_source_bucket;

private:
  bool ExecuteScript(v8::Local<v8::String> script);

  ArrayBufferAllocator allocator;
  v8::Isolate *isolate_;

  Bucket *bucket_handle;

  std::map<std::string, std::string> bucket;

  std::string last_exception;
};

#endif
