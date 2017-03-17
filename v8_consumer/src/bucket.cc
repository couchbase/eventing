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

#include <arpa/inet.h>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include <include/libplatform/libplatform.h>
#include <include/v8.h>

#include "../include/bucket.h"

static void get_callback(lcb_t, int, const lcb_RESPBASE *rb) {
  const lcb_RESPGET *resp = reinterpret_cast<const lcb_RESPGET *>(rb);
  Result *result = reinterpret_cast<Result *>(rb->cookie);

  result->status = resp->rc;
  result->value.clear();
  if (resp->rc == LCB_SUCCESS) {
    result->value.assign(reinterpret_cast<const char *>(resp->value),
                         resp->nvalue);
  }
}

static void set_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  const lcb_RESPSTORE *resp = reinterpret_cast<const lcb_RESPSTORE *>(rb);
  Result *result = reinterpret_cast<Result *>(rb->cookie);

  result->status = resp->rc;
  result->cas = resp->cas;

  LOG(logTrace) << "LCB_STORE callback "
                << lcb_strerror(instance, result->status) << " cas "
                << resp->cas << '\n';
}

// convert Little endian unsigned int64 to Big endian notation
uint64_t htonll_64(uint64_t value) {
  static const int num = 42;

  if (*reinterpret_cast<const char *>(&num) == num) {
    const uint32_t high_part = htonl(static_cast<uint32_t>(value >> 32));
    const uint32_t low_part =
        htonl(static_cast<uint32_t>(value & 0xFFFFFFFFLL));

    return (static_cast<uint64_t>(low_part) << 32) | high_part;
  } else {
    return value;
  }
}

Bucket::Bucket(V8Worker *w, const char *bname, const char *ep,
               const char *alias) {
  isolate_ = w->GetIsolate();
  context_.Reset(isolate_, w->context_);

  bucket_name.assign(bname);
  endpoint.assign(ep);
  bucket_alias.assign(alias);

  worker = w;

  std::string connstr = "couchbase://" + GetEndPoint() + "/" + GetBucketName();

  // lcb related setup
  lcb_create_st crst;
  memset(&crst, 0, sizeof crst);

  crst.version = 3;
  crst.v.v3.connstr = connstr.c_str();

  lcb_create(&bucket_lcb_obj, &crst);
  lcb_connect(bucket_lcb_obj);
  lcb_wait(bucket_lcb_obj);

  lcb_install_callback3(bucket_lcb_obj, LCB_CALLBACK_GET, get_callback);
  lcb_install_callback3(bucket_lcb_obj, LCB_CALLBACK_STORE, set_callback);
}

Bucket::~Bucket() {
  lcb_destroy(bucket_lcb_obj);
  context_.Reset();
}

bool Bucket::Initialize(V8Worker *w,
                        std::map<std::string, std::string> *bucket) {

  v8::HandleScope handle_scope(GetIsolate());

  v8::Local<v8::Context> context =
      v8::Local<v8::Context>::New(GetIsolate(), w->context_);
  context_.Reset(GetIsolate(), context);

  v8::Context::Scope context_scope(context);

  if (!InstallMaps(bucket))
    return false;

  return true;
}

v8::Local<v8::Object>
Bucket::WrapBucketMap(std::map<std::string, std::string> *obj) {

  v8::EscapableHandleScope handle_scope(GetIsolate());

  if (bucket_map_template_.IsEmpty()) {
    v8::Local<v8::ObjectTemplate> raw_template =
        MakeBucketMapTemplate(GetIsolate());
    bucket_map_template_.Reset(GetIsolate(), raw_template);
  }

  v8::Local<v8::ObjectTemplate> templ =
      v8::Local<v8::ObjectTemplate>::New(GetIsolate(), bucket_map_template_);

  v8::Local<v8::Object> result =
      templ->NewInstance(GetIsolate()->GetCurrentContext()).ToLocalChecked();

  v8::Local<v8::External> map_ptr = v8::External::New(GetIsolate(), obj);

  v8::Local<v8::External> bucket_lcb_obj_ptr =
      v8::External::New(GetIsolate(), &bucket_lcb_obj);

  v8::Local<v8::External> worker_cb_instance =
      v8::External::New(GetIsolate(), &(worker->cb_instance));
  result->SetInternalField(0, map_ptr);
  result->SetInternalField(1, bucket_lcb_obj_ptr);
  result->SetInternalField(2, worker_cb_instance);

  return handle_scope.Escape(result);
}

bool Bucket::InstallMaps(std::map<std::string, std::string> *bucket) {

  v8::HandleScope handle_scope(GetIsolate());

  v8::Local<v8::Object> bucket_obj = WrapBucketMap(bucket);

  v8::Local<v8::Context> context =
      v8::Local<v8::Context>::New(GetIsolate(), context_);

  LOG(logInfo) << "Registering handler for bucket_alias: "
               << bucket_alias.c_str() << '\n';
  // Set the options object as a property on the global object.
  context->Global()
      ->Set(context,
            v8::String::NewFromUtf8(GetIsolate(), bucket_alias.c_str(),
                                    v8::NewStringType::kNormal)
                .ToLocalChecked(),
            bucket_obj)
      .FromJust();

  return true;
}

void Bucket::BucketGet(v8::Local<v8::Name> name,
                       const v8::PropertyCallbackInfo<v8::Value> &info) {

  if (name->IsSymbol())
    return;

  std::string key = ObjectToString(v8::Local<v8::String>::Cast(name));

  lcb_t *bucket_lcb_obj_ptr = UnwrapLcbInstance(info.Holder());

  Result result;
  lcb_CMDGET gcmd = {0};
  LCB_CMD_SET_KEY(&gcmd, key.c_str(), key.length());
  lcb_sched_enter(*bucket_lcb_obj_ptr);
  lcb_get3(*bucket_lcb_obj_ptr, &result, &gcmd);
  lcb_sched_leave(*bucket_lcb_obj_ptr);
  lcb_wait(*bucket_lcb_obj_ptr);

  LOG(logTrace) << "GET call result Key: " << key << " Value: " << result.value
                << '\n';

  const std::string &value = result.value;
  info.GetReturnValue().Set(
      v8::JSON::Parse(v8::String::NewFromUtf8(info.GetIsolate(), value.c_str(),
                                              v8::NewStringType::kNormal,
                                              static_cast<int>(value.length()))
                          .ToLocalChecked()));
}

void Bucket::BucketSet(v8::Local<v8::Name> name, v8::Local<v8::Value> value_obj,
                       const v8::PropertyCallbackInfo<v8::Value> &info) {

  if (name->IsSymbol())
    return;

  std::string key = ObjectToString(v8::Local<v8::String>::Cast(name));
  std::string value = ToString(info.GetIsolate(), value_obj);

  LOG(logTrace) << "Set call Key: " << key << " Value: " << value << '\n';

  lcb_t *bucket_lcb_obj_ptr = UnwrapLcbInstance(info.Holder());
  lcb_t *cb_instance = UnwrapV8WorkerLcbInstance(info.Holder());

  Result result;
  lcb_CMDSTORE scmd = {0};
  LCB_CMD_SET_KEY(&scmd, key.c_str(), key.length());
  LCB_CMD_SET_VALUE(&scmd, value.c_str(), value.length());
  scmd.operation = LCB_SET;
  scmd.flags = 0x2000000;

  lcb_sched_enter(*bucket_lcb_obj_ptr);
  lcb_store3(*bucket_lcb_obj_ptr, &result, &scmd);
  lcb_sched_leave(*bucket_lcb_obj_ptr);
  lcb_wait(*bucket_lcb_obj_ptr);

  info.GetReturnValue().Set(value_obj);
}

void Bucket::BucketDelete(v8::Local<v8::Name> name,
                          const v8::PropertyCallbackInfo<v8::Boolean> &info) {

  if (name->IsSymbol())
    return;

  std::string key = ObjectToString(v8::Local<v8::String>::Cast(name));

  lcb_t *bucket_lcb_obj_ptr = UnwrapLcbInstance(info.Holder());

  lcb_CMDREMOVE rcmd = {0};
  LCB_CMD_SET_KEY(&rcmd, key.c_str(), key.length());

  lcb_sched_enter(*bucket_lcb_obj_ptr);
  lcb_remove3(*bucket_lcb_obj_ptr, NULL, &rcmd);
  lcb_sched_leave(*bucket_lcb_obj_ptr);
  lcb_wait(*bucket_lcb_obj_ptr);

  info.GetReturnValue().Set(true);
}

v8::Local<v8::ObjectTemplate>
Bucket::MakeBucketMapTemplate(v8::Isolate *isolate) {

  v8::EscapableHandleScope handle_scope(isolate);

  v8::Local<v8::ObjectTemplate> result = v8::ObjectTemplate::New(isolate);
  result->SetInternalFieldCount(3);
  result->SetHandler(v8::NamedPropertyHandlerConfiguration(BucketGet, BucketSet,
                                                           NULL, BucketDelete));

  return handle_scope.Escape(result);
}
