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

#define LCB_NO_DEPR_CXX_CTORS
#undef NDEBUG

#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include <libplatform/libplatform.h>
#include <v8.h>

#include "../include/bucket.h"

static void get_callback(lcb_t, int, const lcb_RESPBASE *rb) {
  const lcb_RESPGET *resp = reinterpret_cast<const lcb_RESPGET *>(rb);
  Result *result = reinterpret_cast<Result *>(rb->cookie);

  LOG(logTrace) << "Bucket: LCB_GET callback, res: "
                << lcb_strerror(NULL, rb->rc) << rb->rc << " cas " << rb->cas
                << '\n';

  result->rc = resp->rc;
  result->value.clear();

  if (resp->rc == LCB_SUCCESS) {
    result->value.assign(reinterpret_cast<const char *>(resp->value),
                         static_cast<int>(resp->nvalue));
    LOG(logTrace) << "Value: " << result->value << " flags: " << resp->itmflags
                  << '\n';
  }
}

static void set_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  const lcb_RESPSTORE *resp = reinterpret_cast<const lcb_RESPSTORE *>(rb);
  Result *result = reinterpret_cast<Result *>(rb->cookie);

  result->rc = resp->rc;
  result->cas = resp->cas;

  LOG(logTrace) << "Bucket: LCB_STORE callback "
                << lcb_strerror(instance, result->rc) << " cas " << resp->cas
                << '\n';
}

static void sdmutate_callback(lcb_t, int cbtype, const lcb_RESPBASE *rb) {
  Result *result = reinterpret_cast<Result *>(rb->cookie);
  result->rc = rb->rc;

  LOG(logTrace) << "Bucket: LCB_SDMUTATE callback "
                << lcb_strerror(NULL, result->rc) << '\n';
}

static void del_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  const lcb_RESPREMOVE *resp = reinterpret_cast<const lcb_RESPREMOVE *>(rb);
  Result *result = reinterpret_cast<Result *>(rb->cookie);

  result->rc = resp->rc;
}

Bucket::Bucket(V8Worker *w, const char *bname, const char *ep,
               const char *alias, std::string rbac_user,
               std::string rbac_pass) {
  isolate_ = w->GetIsolate();
  context_.Reset(isolate_, w->context_);

  bucket_name.assign(bname);
  endpoint.assign(ep);
  bucket_alias.assign(alias);

  worker = w;

  std::string connstr = "couchbase://" + GetEndPoint() + "/" + GetBucketName() +
                        "?username=" + rbac_user + "&select_bucket=true";

  LOG(logInfo) << "Bucket: connstr " << connstr << '\n';

  // lcb related setup
  lcb_create_st crst;
  memset(&crst, 0, sizeof crst);

  crst.version = 3;
  crst.v.v3.connstr = connstr.c_str();
  crst.v.v3.type = LCB_TYPE_BUCKET;
  crst.v.v3.passwd = rbac_pass.c_str();

  lcb_create(&bucket_lcb_obj, &crst);
  lcb_connect(bucket_lcb_obj);
  lcb_wait(bucket_lcb_obj);

  lcb_install_callback3(bucket_lcb_obj, LCB_CALLBACK_GET, get_callback);
  lcb_install_callback3(bucket_lcb_obj, LCB_CALLBACK_STORE, set_callback);
  lcb_install_callback3(bucket_lcb_obj, LCB_CALLBACK_SDMUTATE,
                        sdmutate_callback);
  lcb_install_callback3(bucket_lcb_obj, LCB_CALLBACK_REMOVE, del_callback);

  lcb_U32 lcb_timeout = 2500000; // 2.5s
  lcb_cntl(bucket_lcb_obj, LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT, &lcb_timeout);
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

  result->SetInternalField(0, map_ptr);
  result->SetInternalField(1, bucket_lcb_obj_ptr);

  return handle_scope.Escape(result);
}

bool Bucket::InstallMaps(std::map<std::string, std::string> *bucket) {

  v8::HandleScope handle_scope(GetIsolate());

  v8::Local<v8::Object> bucket_obj = WrapBucketMap(bucket);

  v8::Local<v8::Context> context =
      v8::Local<v8::Context>::New(GetIsolate(), context_);

  LOG(logInfo) << "Registering handler for bucket_alias: " << bucket_alias
               << " bucket_name: " << bucket_name << '\n';
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

  v8::String::Utf8Value utf8_key(v8::Local<v8::String>::Cast(name));
  std::string key(*utf8_key);

  lcb_t *bucket_lcb_obj_ptr = UnwrapLcbInstance(info.Holder());

  Result result;
  lcb_CMDGET gcmd = {0};
  LCB_CMD_SET_KEY(&gcmd, key.c_str(), key.length());
  lcb_sched_enter(*bucket_lcb_obj_ptr);
  lcb_get3(*bucket_lcb_obj_ptr, &result, &gcmd);
  lcb_sched_leave(*bucket_lcb_obj_ptr);
  lcb_wait(*bucket_lcb_obj_ptr);

  // Throw an exception in JavaScript if the bucket get call failed.
  if (result.rc != LCB_SUCCESS) {
    bucket_op_exception_count++;
    auto js_exception = UnwrapData(info.GetIsolate())->js_exception;
    js_exception->Throw(*bucket_lcb_obj_ptr, result.rc);
    return;
  }

  LOG(logTrace) << "Get call result Key: " << key << " Value: " << result.value
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

  v8::String::Utf8Value utf8_key(v8::Local<v8::String>::Cast(name));
  std::string key(*utf8_key);
  std::string value = JSONStringify(info.GetIsolate(), value_obj);

  LOG(logTrace) << "Set call Key: " << key << " Value: " << value
                << " enable_recursive_mutation: " << enable_recursive_mutation
                << '\n';

  lcb_t *bucket_lcb_obj_ptr = UnwrapLcbInstance(info.Holder());
  Result sres;

  if (!enable_recursive_mutation) {
    LOG(logTrace) << "Adding macro in xattr to avoid retriggering of handler "
                     "from recursive mutation, enable_recursive_mutation: "
                  << enable_recursive_mutation << '\n';

    while (true) {
      Result gres;
      lcb_CMDGET gcmd = {0};
      LCB_CMD_SET_KEY(&gcmd, key.c_str(), key.length());
      lcb_sched_enter(*bucket_lcb_obj_ptr);
      lcb_get3(*bucket_lcb_obj_ptr, &gres, &gcmd);
      lcb_sched_leave(*bucket_lcb_obj_ptr);
      lcb_wait(*bucket_lcb_obj_ptr);

      switch (gres.rc) {
      case LCB_KEY_ENOENT:
        LOG(logTrace)
            << "Key: " << key
            << " doesn't exist in bucket where mutation has to be written"
            << '\n';
        break;
      case LCB_SUCCESS:
        break;
      default:
        LOG(logTrace) << "Failed to fetch full doc: " << key
                      << " to calculate digest, res: "
                      << lcb_strerror(NULL, gres.rc) << '\n';
        return;
      }

      std::string digest;
      if (gres.rc == LCB_SUCCESS) {
        uint32_t d = crc32c(0, gres.value.c_str(), gres.value.length());
        digest.assign(std::to_string(d));
        LOG(logTrace) << "key: " << key << " digest: " << digest
                      << " value: " << gres.value << '\n';
      }

      lcb_CMDSUBDOC mcmd = {0};
      LCB_CMD_SET_KEY(&mcmd, key.c_str(), key.length());

      lcb_SDSPEC digest_spec, xattr_spec, doc_spec = {0};
      std::vector<lcb_SDSPEC> specs;

      digest_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
      digest_spec.options = LCB_SDSPEC_F_XATTRPATH;

      xattr_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
      xattr_spec.options =
          LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTR_MACROVALUES;

      std::string xattr_cas_path("eventing.cas");
      std::string xattr_digest_path("eventing.digest");
      std::string mutation_cas_macro("\"${Mutation.CAS}\"");

      if (gres.rc == LCB_SUCCESS) {
        LCB_SDSPEC_SET_PATH(&digest_spec, xattr_digest_path.c_str(),
                            xattr_digest_path.size());
        LCB_SDSPEC_SET_VALUE(&digest_spec, digest.c_str(), digest.size());
        specs.push_back(digest_spec);
      }

      LCB_SDSPEC_SET_PATH(&xattr_spec, xattr_cas_path.c_str(),
                          xattr_cas_path.size());
      LCB_SDSPEC_SET_VALUE(&xattr_spec, mutation_cas_macro.c_str(),
                           mutation_cas_macro.size());
      specs.push_back(xattr_spec);

      doc_spec.sdcmd = LCB_SDCMD_SET_FULLDOC;
      LCB_SDSPEC_SET_PATH(&doc_spec, "", 0);
      LCB_SDSPEC_SET_VALUE(&doc_spec, value.c_str(), value.length());
      specs.push_back(doc_spec);

      mcmd.specs = specs.data();
      mcmd.nspecs = specs.size();
      mcmd.cmdflags = LCB_CMDSUBDOC_F_UPSERT_DOC;
      mcmd.cas = gres.cas;

      lcb_subdoc3(*bucket_lcb_obj_ptr, &sres, &mcmd);
      lcb_wait(*bucket_lcb_obj_ptr);

      switch (sres.rc) {
      case LCB_SUCCESS:
        LOG(logTrace) << "Successfully wrote doc:" << key << '\n';
        info.GetReturnValue().Set(value_obj);
        return;
      case LCB_KEY_EEXISTS:
        LOG(logTrace) << "CAS mismatch for doc:" << key << '\n';
        std::this_thread::sleep_for(
            std::chrono::milliseconds(LCB_OP_RETRY_INTERVAL));
        break;
      default:
        LOG(logTrace) << "Encountered error:" << lcb_strerror(NULL, sres.rc)
                      << " for key:" << key << '\n';
        info.GetReturnValue().Set(value_obj);
        return;
      }
    }
  } else {
    LOG(logTrace)
        << "Performing recursive mutation, enable_recursive_mutation: "
        << enable_recursive_mutation << '\n';

    lcb_CMDSTORE scmd = {0};
    LCB_CMD_SET_KEY(&scmd, key.c_str(), key.length());
    LCB_CMD_SET_VALUE(&scmd, value.c_str(), value.length());
    scmd.operation = LCB_SET;
    scmd.flags = 0x2000000;

    lcb_sched_enter(*bucket_lcb_obj_ptr);
    lcb_store3(*bucket_lcb_obj_ptr, &sres, &scmd);
    lcb_sched_leave(*bucket_lcb_obj_ptr);
    lcb_wait(*bucket_lcb_obj_ptr);
  }

  // Throw an exception in JavaScript if the bucket set call failed.
  if (sres.rc != LCB_SUCCESS) {
    bucket_op_exception_count++;
    auto js_exception = UnwrapData(info.GetIsolate())->js_exception;
    js_exception->Throw(*bucket_lcb_obj_ptr, sres.rc);
    return;
  }

  info.GetReturnValue().Set(value_obj);
}

void Bucket::BucketDelete(v8::Local<v8::Name> name,
                          const v8::PropertyCallbackInfo<v8::Boolean> &info) {
  if (name->IsSymbol())
    return;

  v8::String::Utf8Value utf8_key(v8::Local<v8::String>::Cast(name));
  std::string key(*utf8_key);

  lcb_t *bucket_lcb_obj_ptr = UnwrapLcbInstance(info.Holder());

  Result result;
  lcb_CMDREMOVE rcmd = {0};
  LCB_CMD_SET_KEY(&rcmd, key.c_str(), key.length());

  lcb_sched_enter(*bucket_lcb_obj_ptr);
  lcb_remove3(*bucket_lcb_obj_ptr, &result, &rcmd);
  lcb_sched_leave(*bucket_lcb_obj_ptr);
  lcb_wait(*bucket_lcb_obj_ptr);

  // Throw an exception in JavaScript if the bucket delete call failed.
  if (result.rc != LCB_SUCCESS) {
    bucket_op_exception_count++;
    auto js_exception = UnwrapData(info.GetIsolate())->js_exception;
    js_exception->Throw(*bucket_lcb_obj_ptr, result.rc);
    return;
  }

  info.GetReturnValue().Set(true);
}

v8::Local<v8::ObjectTemplate>
Bucket::MakeBucketMapTemplate(v8::Isolate *isolate) {

  v8::EscapableHandleScope handle_scope(isolate);

  v8::Local<v8::ObjectTemplate> result = v8::ObjectTemplate::New(isolate);
  result->SetInternalFieldCount(2);
  result->SetHandler(v8::NamedPropertyHandlerConfiguration(BucketGet, BucketSet,
                                                           NULL, BucketDelete));

  return handle_scope.Escape(result);
}
