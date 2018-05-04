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

#include "bucket.h"

#define LCB_NO_DEPR_CXX_CTORS
#undef NDEBUG

// lcb related callbacks
static void get_callback(lcb_t, int, const lcb_RESPBASE *rb) {
  auto resp = reinterpret_cast<const lcb_RESPGET *>(rb);
  auto result = reinterpret_cast<Result *>(rb->cookie);

  LOG(logTrace) << "Bucket: LCB_GET callback, res: "
                << lcb_strerror(nullptr, rb->rc) << rb->rc << " cas " << rb->cas
                << std::endl;

  result->rc = resp->rc;
  result->value.clear();

  if (resp->rc == LCB_SUCCESS) {
    result->value.assign(reinterpret_cast<const char *>(resp->value),
                         static_cast<int>(resp->nvalue));
    LOG(logTrace) << "Bucket: Value: " << RU(result->value)
                  << " flags: " << resp->itmflags << std::endl;
  }
}

static void set_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  auto resp = reinterpret_cast<const lcb_RESPSTORE *>(rb);
  auto result = reinterpret_cast<Result *>(rb->cookie);

  result->rc = resp->rc;
  result->cas = resp->cas;

  LOG(logTrace) << "Bucket: LCB_STORE callback "
                << lcb_strerror(instance, result->rc) << " cas " << resp->cas
                << std::endl;
}

static void sdmutate_callback(lcb_t, int cbtype, const lcb_RESPBASE *rb) {
  auto result = reinterpret_cast<Result *>(rb->cookie);
  result->rc = rb->rc;

  LOG(logTrace) << "Bucket: LCB_SDMUTATE callback "
                << lcb_strerror(nullptr, result->rc) << std::endl;
}

static void del_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  auto result = reinterpret_cast<Result *>(rb->cookie);
  result->rc = rb->rc;

  LOG(logTrace) << "Bucket: LCB_DEL callback "
                << lcb_strerror(nullptr, result->rc) << std::endl;
}

Bucket::Bucket(V8Worker *w, const char *bname, const char *ep,
               const char *alias, bool block_mutation)
    : block_mutation(block_mutation), bucket_name(bname), endpoint(ep),
      bucket_alias(alias), worker(w) {
  isolate_ = w->GetIsolate();
  context_.Reset(isolate_, w->context_);

  auto init_success = true;
  auto connstr =
      "couchbase://" + endpoint + "/" + bucket_name + "?select_bucket=true";
  if (IsIPv6()) {
    connstr += "&ipv6=allow";
  }

  LOG(logInfo) << "Bucket: connstr " << RS(connstr) << std::endl;

  // lcb related setup
  lcb_create_st crst;
  memset(&crst, 0, sizeof crst);

  crst.version = 3;
  crst.v.v3.connstr = connstr.c_str();
  crst.v.v3.type = LCB_TYPE_BUCKET;

  lcb_create(&bucket_lcb_obj, &crst);

  auto auth = lcbauth_new();
  auto err = lcbauth_set_callbacks(auth, isolate_, GetUsername, GetPassword);
  if (err != LCB_SUCCESS) {
    LOG(logError) << "Bucket: Unable to set auth callbacks" << std::endl;
    init_success = false;
  }

  err = lcbauth_set_mode(auth, LCBAUTH_MODE_DYNAMIC);
  if (err != LCB_SUCCESS) {
    LOG(logError) << "Bucket: Unable to set auth mode to dynamic" << std::endl;
    init_success = false;
  }

  lcb_set_auth(bucket_lcb_obj, auth);

  err = lcb_connect(bucket_lcb_obj);
  if (err != LCB_SUCCESS) {
    LOG(logError) << "Bucket: Unable to connect to bucket" << std::endl;
    init_success = false;
  }

  err = lcb_wait(bucket_lcb_obj);
  if (err != LCB_SUCCESS) {
    LOG(logError) << "Bucket: Unable to schedule call for connect" << std::endl;
    init_success = false;
  }

  lcb_install_callback3(bucket_lcb_obj, LCB_CALLBACK_GET, get_callback);
  lcb_install_callback3(bucket_lcb_obj, LCB_CALLBACK_STORE, set_callback);
  lcb_install_callback3(bucket_lcb_obj, LCB_CALLBACK_SDMUTATE,
                        sdmutate_callback);
  lcb_install_callback3(bucket_lcb_obj, LCB_CALLBACK_REMOVE, del_callback);

  lcb_U32 lcb_timeout = 2500000; // 2.5s
  err =
      lcb_cntl(bucket_lcb_obj, LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT, &lcb_timeout);
  if (err != LCB_SUCCESS) {
    init_success = false;
    LOG(logError) << "Bucket: Unable to set timeout for bucket ops"
                  << std::endl;
  }

  if (init_success) {
    LOG(logInfo) << "Bucket: lcb instance for " << bname
                 << " initialized successfully" << std::endl;
  } else {
    LOG(logError) << "Bucket: Unable to initialize lcb instance for " << bname
                  << std::endl;
  }
}

Bucket::~Bucket() {
  lcb_destroy(bucket_lcb_obj);
  context_.Reset();
}

bool Bucket::Initialize(V8Worker *w) {
  v8::HandleScope handle_scope(isolate_);

  auto context = v8::Local<v8::Context>::New(isolate_, w->context_);
  context_.Reset(isolate_, context);
  v8::Context::Scope context_scope(context);

  return InstallMaps();
}

// Associates the lcb instance with the bucket object and returns it
v8::Local<v8::Object> Bucket::WrapBucketMap() {
  v8::EscapableHandleScope handle_scope(isolate_);

  if (bucket_map_template_.IsEmpty()) {
    auto raw_template = MakeBucketMapTemplate();
    bucket_map_template_.Reset(isolate_, raw_template);
  }

  auto templ =
      v8::Local<v8::ObjectTemplate>::New(isolate_, bucket_map_template_);
  auto context = context_.Get(isolate_);
  auto result = templ->NewInstance(context).ToLocalChecked();
  auto bucket_lcb_obj_ptr = v8::External::New(isolate_, &bucket_lcb_obj);
  auto block_mutation_ptr = v8::External::New(isolate_, &block_mutation);

  result->SetInternalField(LCB_INST_FIELD_NO, bucket_lcb_obj_ptr);
  result->SetInternalField(BLOCK_MUTATION_FIELD_NO, block_mutation_ptr);
  return handle_scope.Escape(result);
}

// Adds the bucket object as a global variable in JavaScript
bool Bucket::InstallMaps() {
  v8::HandleScope handle_scope(isolate_);

  auto bucket_obj = WrapBucketMap();
  auto context = v8::Local<v8::Context>::New(isolate_, context_);

  LOG(logInfo) << "Bucket: Registering handler for bucket_alias: "
               << bucket_alias << " bucket_name: " << bucket_name << std::endl;

  return context->Global()->Set(v8Str(isolate_, bucket_alias.c_str()),
                                bucket_obj);
}

// Performs the lcb related calls when bucket object is accessed
template <>
void Bucket::BucketGet<v8::Local<v8::Name>>(
    const v8::Local<v8::Name> &name,
    const v8::PropertyCallbackInfo<v8::Value> &info) {
  auto isolate = info.GetIsolate();
  if (name->IsSymbol()) {
    auto js_exception = UnwrapData(isolate)->js_exception;
    js_exception->Throw("Symbol data type is not supported");
    ++bucket_op_exception_count;
    return;
  }

  v8::String::Utf8Value utf8_key(v8::Local<v8::String>::Cast(name));
  std::string key(*utf8_key);

  auto bucket_lcb_obj_ptr =
      UnwrapInternalField<lcb_t>(info.Holder(), LCB_INST_FIELD_NO);

  Result result;
  lcb_CMDGET gcmd = {0};
  LCB_CMD_SET_KEY(&gcmd, key.c_str(), key.length());
  lcb_sched_enter(*bucket_lcb_obj_ptr);
  auto err = lcb_get3(*bucket_lcb_obj_ptr, &result, &gcmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_GET: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  lcb_sched_leave(*bucket_lcb_obj_ptr);
  err = lcb_wait(*bucket_lcb_obj_ptr);

  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_GET: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  // Throw an exception in JavaScript if the bucket get call failed.
  if (result.rc != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: LCB_GET call failed: " << result.rc << std::endl;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, result.rc);
    return;
  }

  LOG(logTrace) << "Bucket: Get call result Key: " << RU(key)
                << " Value: " << RU(result.value) << std::endl;

  const std::string &value = result.value;
  auto value_json = v8::JSON::Parse(v8Str(isolate, value.c_str()));
  info.GetReturnValue().Set(value_json);
}

// Performs the lcb related calls when bucket object is accessed
template <>
void Bucket::BucketSet<v8::Local<v8::Name>>(
    const v8::Local<v8::Name> &name, const v8::Local<v8::Value> &value_obj,
    const v8::PropertyCallbackInfo<v8::Value> &info) {
  auto isolate = info.GetIsolate();
  if (name->IsSymbol()) {
    auto js_exception = UnwrapData(isolate)->js_exception;
    js_exception->Throw("Symbol data type is not supported");
    ++bucket_op_exception_count;
    return;
  }

  auto block_mutation =
      UnwrapInternalField<bool>(info.Holder(), BLOCK_MUTATION_FIELD_NO);
  if (*block_mutation) {
    auto js_exception = UnwrapData(info.GetIsolate())->js_exception;
    js_exception->Throw("Writing to source bucket is forbidden");
    ++bucket_op_exception_count;
    return;
  }

  v8::String::Utf8Value utf8_key(v8::Local<v8::String>::Cast(name));
  std::string key(*utf8_key);
  auto value = JSONStringify(isolate, value_obj);

  LOG(logTrace) << "Bucket: Set call Key: " << RU(key)
                << " Value: " << RU(value)
                << " enable_recursive_mutation: " << enable_recursive_mutation
                << std::endl;

  auto bucket_lcb_obj_ptr =
      UnwrapInternalField<lcb_t>(info.Holder(), LCB_INST_FIELD_NO);
  Result sres;

  if (!enable_recursive_mutation) {
    LOG(logTrace)
        << "Bucket: Adding macro in xattr to avoid retriggering of handler "
           "from recursive mutation, enable_recursive_mutation: "
        << enable_recursive_mutation << std::endl;

    while (true) {
      Result gres;
      lcb_CMDGET gcmd = {0};
      LCB_CMD_SET_KEY(&gcmd, key.c_str(), key.length());
      lcb_sched_enter(*bucket_lcb_obj_ptr);
      auto err = lcb_get3(*bucket_lcb_obj_ptr, &gres, &gcmd);
      if (err != LCB_SUCCESS) {
        LOG(logTrace)
            << "Bucket: Unable to get key for recursive_mutation=false"
            << std::endl;
      }

      lcb_sched_leave(*bucket_lcb_obj_ptr);
      err = lcb_wait(*bucket_lcb_obj_ptr);
      if (err != LCB_SUCCESS) {
        LOG(logTrace) << "Bucket: Unable to schedule call to get key for "
                         "recursive_mutation=false"
                      << std::endl;
      }

      switch (gres.rc) {
      case LCB_KEY_ENOENT:
        LOG(logTrace) << "Bucket: Key: " << RU(key)
                      << " doesn't exist in bucket where mutation has "
                         "to be written"
                      << std::endl;
        break;

      case LCB_SUCCESS:
        break;

      default:
        HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, gres.rc);
        LOG(logTrace) << "Bucket: Failed to fetch full doc: " << RU(key)
                      << " to calculate digest, res: "
                      << lcb_strerror(*bucket_lcb_obj_ptr, gres.rc)
                      << std::endl;
        return;
      }

      std::string digest;
      if (gres.rc == LCB_SUCCESS) {
        uint32_t d = crc32c(0, gres.value.c_str(), gres.value.length());
        digest.assign(std::to_string(d));
        LOG(logTrace) << "Bucket: key: " << RU(key) << " digest: " << digest
                      << " value: " << RU(gres.value) << std::endl;
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

      err = lcb_subdoc3(*bucket_lcb_obj_ptr, &sres, &mcmd);
      if (err != LCB_SUCCESS) {
        LOG(logTrace) << "Bucket: Unable to set subdoc op for setting macro"
                      << std::endl;
      }

      err = lcb_wait(*bucket_lcb_obj_ptr);
      if (err != LCB_SUCCESS) {
        LOG(logTrace)
            << "Bucket: Unable to schedule subdoc op for setting macro"
            << std::endl;
      }

      switch (sres.rc) {
      case LCB_SUCCESS:
        LOG(logTrace) << "Bucket: Successfully wrote doc:" << RU(key)
                      << std::endl;
        info.GetReturnValue().Set(value_obj);
        return;

      case LCB_KEY_EEXISTS:
        LOG(logTrace) << "Bucket: CAS mismatch for doc:" << RU(key)
                      << std::endl;
        std::this_thread::sleep_for(
            std::chrono::milliseconds(LCB_OP_RETRY_INTERVAL));
        break;

      default:
        LOG(logTrace) << "Bucket: Encountered error:"
                      << lcb_strerror(*bucket_lcb_obj_ptr, sres.rc)
                      << " for key:" << RU(key) << std::endl;

        HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, gres.rc);
        return;
      }
    }
  } else {
    LOG(logTrace)
        << "Bucket: Performing recursive mutation, enable_recursive_mutation: "
        << enable_recursive_mutation << std::endl;

    lcb_CMDSTORE scmd = {0};
    LCB_CMD_SET_KEY(&scmd, key.c_str(), key.length());
    LCB_CMD_SET_VALUE(&scmd, value.c_str(), value.length());
    scmd.operation = LCB_SET;
    scmd.flags = 0x2000000;

    lcb_sched_enter(*bucket_lcb_obj_ptr);
    auto err = lcb_store3(*bucket_lcb_obj_ptr, &sres, &scmd);
    if (err != LCB_SUCCESS) {
      LOG(logTrace) << "Bucket: Unable to set params for LCB_SET: "
                    << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
      HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
      return;
    }

    lcb_sched_leave(*bucket_lcb_obj_ptr);
    err = lcb_wait(*bucket_lcb_obj_ptr);

    if (err != LCB_SUCCESS) {
      LOG(logTrace) << "Bucket: Unable to schedule LCB_SET: "
                    << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
      HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
      return;
    }
  }

  // Throw an exception in JavaScript if the bucket set call failed.
  if (sres.rc != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: LCB_STORE call failed: " << sres.rc << std::endl;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, sres.rc);
    return;
  }

  info.GetReturnValue().Set(value_obj);
}

// Performs the lcb related calls when bucket object is accessed
template <>
void Bucket::BucketDelete<v8::Local<v8::Name>>(
    v8::Local<v8::Name> name,
    const v8::PropertyCallbackInfo<v8::Boolean> &info) {
  auto isolate = info.GetIsolate();
  if (name->IsSymbol()) {
    auto js_exception = UnwrapData(isolate)->js_exception;
    js_exception->Throw("Symbol data type is not supported");
    ++bucket_op_exception_count;
    return;
  }

  auto block_mutation =
      UnwrapInternalField<bool>(info.Holder(), BLOCK_MUTATION_FIELD_NO);
  if (*block_mutation) {
    auto js_exception = UnwrapData(info.GetIsolate())->js_exception;
    js_exception->Throw("Delete from source bucket is forbidden");
    ++bucket_op_exception_count;
    return;
  }

  v8::String::Utf8Value utf8_key(v8::Local<v8::String>::Cast(name));
  std::string key(*utf8_key);

  auto bucket_lcb_obj_ptr =
      UnwrapInternalField<lcb_t>(info.Holder(), LCB_INST_FIELD_NO);

  Result result;
  lcb_CMDREMOVE rcmd = {0};
  LCB_CMD_SET_KEY(&rcmd, key.c_str(), key.length());

  lcb_sched_enter(*bucket_lcb_obj_ptr);
  auto err = lcb_remove3(*bucket_lcb_obj_ptr, &result, &rcmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_REMOVE: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  lcb_sched_leave(*bucket_lcb_obj_ptr);
  err = lcb_wait(*bucket_lcb_obj_ptr);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_REMOVE: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  // Throw an exception in JavaScript if the bucket delete call failed.
  if (result.rc != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: LCB_REMOVE call failed: " << result.rc
                  << std::endl;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, result.rc);
    return;
  }

  info.GetReturnValue().Set(true);
}

void Bucket::HandleBucketOpFailure(v8::Isolate *isolate,
                                   lcb_t bucket_lcb_obj_ptr,
                                   lcb_error_t error) {
  auto isolate_data = UnwrapData(isolate);
  auto w = isolate_data->v8worker;
  w->AddLcbException(error);
  ++bucket_op_exception_count;

  auto js_exception = isolate_data->js_exception;
  js_exception->Throw(bucket_lcb_obj_ptr, error);
}

// Registers the necessary callbacks to the bucket object in JavaScript
v8::Local<v8::ObjectTemplate> Bucket::MakeBucketMapTemplate() {
  v8::EscapableHandleScope handle_scope(isolate_);

  auto result = v8::ObjectTemplate::New(isolate_);
  // We will store lcb_instance associated with this bucket object in the
  // internal field
  result->SetInternalFieldCount(2);
  // Register corresponding callbacks for alphanumeric accesses on bucket
  // object
  result->SetHandler(v8::NamedPropertyHandlerConfiguration(
      BucketGetDelegate, BucketSetDelegate, nullptr, BucketDeleteDelegate));
  // Register corresponding callbacks for numeric accesses on bucket object
  result->SetIndexedPropertyHandler(
      v8::IndexedPropertyGetterCallback(BucketGetDelegate),
      v8::IndexedPropertySetterCallback(BucketSetDelegate), nullptr,
      v8::IndexedPropertyDeleterCallback(BucketDeleteDelegate));
  return handle_scope.Escape(result);
}

// Delegates to the appropriate type of handler
template <typename T>
void Bucket::BucketGetDelegate(
    T name, const v8::PropertyCallbackInfo<v8::Value> &info) {
  BucketGet<T>(name, info);
}

template <typename T>
void Bucket::BucketSetDelegate(
    T key, v8::Local<v8::Value> value,
    const v8::PropertyCallbackInfo<v8::Value> &info) {
  BucketSet<T>(key, value, info);
}

template <typename T>
void Bucket::BucketDeleteDelegate(
    T key, const v8::PropertyCallbackInfo<v8::Boolean> &info) {
  BucketDelete<T>(key, info);
}

// Specialized templates to forward the delegate to the overload doing the
// acutal work
template <>
void Bucket::BucketGet<uint32_t>(
    uint32_t key, const v8::PropertyCallbackInfo<v8::Value> &info) {
  BucketGet<v8::Local<v8::Name>>(v8Name(info.GetIsolate(), key), info);
}

template <>
void Bucket::BucketSet<uint32_t>(
    uint32_t key, const v8::Local<v8::Value> &value,
    const v8::PropertyCallbackInfo<v8::Value> &info) {
  BucketSet<v8::Local<v8::Name>>(v8Name(info.GetIsolate(), key), value, info);
}

template <>
void Bucket::BucketDelete<uint32_t>(
    uint32_t key, const v8::PropertyCallbackInfo<v8::Boolean> &info) {
  BucketDelete<v8::Local<v8::Name>>(v8Name(info.GetIsolate(), key), info);
}
