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

#include <mutex>
#include <ostream>

#include "bucket.h"
#include "js_exception.h"
#include "lcb_utils.h"
#include "retry_util.h"
#include "utils.h"

#undef NDEBUG

std::atomic<int64_t> bucket_op_exception_count = {0};
std::atomic<int64_t> lcb_retry_failure = {0};

Bucket::Bucket(v8::Isolate *isolate, const v8::Local<v8::Context> &context,
               const std::string &conn_str, const std::string &alias,
               bool block_mutation, bool is_source_bucket)
    : isolate_(isolate), block_mutation_(block_mutation),
      is_source_bucket_(is_source_bucket), conn_str_(conn_str),
      bucket_alias_(alias) {
  context_.Reset(isolate_, context);

  auto init_success = true;
  LOG(logInfo) << "Bucket: connstr " << RS(conn_str_) << std::endl;

  // lcb related setup
  lcb_create_st crst;
  memset(&crst, 0, sizeof crst);

  crst.version = 3;
  crst.v.v3.connstr = conn_str_.c_str();
  crst.v.v3.type = LCB_TYPE_BUCKET;

  lcb_create(&bucket_lcb_obj_, &crst);

  auto err =
      RetryWithFixedBackoff(5, 200, IsRetriable, lcb_cntl, bucket_lcb_obj_,
                            LCB_CNTL_SET, LCB_CNTL_LOGGER, &evt_logger);
  if (err != LCB_SUCCESS) {
    init_success = false;
    LOG(logError) << "Bucket: Unable to set logger hooks, err: " << err
                  << std::endl;
  }

  auto auth = lcbauth_new();
  err = RetryWithFixedBackoff(5, 200, IsRetriable, lcbauth_set_callbacks, auth,
                              isolate_, GetUsername, GetPassword);
  if (err != LCB_SUCCESS) {
    LOG(logError) << "Bucket: Unable to set auth callbacks, err: " << err
                  << std::endl;
    init_success = false;
  }

  err = RetryWithFixedBackoff(5, 200, IsRetriable, lcbauth_set_mode, auth,
                              LCBAUTH_MODE_DYNAMIC);
  if (err != LCB_SUCCESS) {
    LOG(logError) << "Bucket: Unable to set auth mode to dynamic, err: " << err
                  << std::endl;
    init_success = false;
  }

  lcb_set_auth(bucket_lcb_obj_, auth);

  err =
      RetryWithFixedBackoff(5, 200, IsRetriable, lcb_connect, bucket_lcb_obj_);
  if (err != LCB_SUCCESS) {
    LOG(logError) << "Bucket: Unable to connect to bucket, err: " << err
                  << std::endl;
    init_success = false;
  }

  err = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_wait, bucket_lcb_obj_);
  if (err != LCB_SUCCESS) {
    LOG(logError) << "Bucket: Unable to schedule call for connect, err: " << err
                  << std::endl;
    init_success = false;
  }

  lcb_install_callback3(bucket_lcb_obj_, LCB_CALLBACK_GET, get_callback);
  lcb_install_callback3(bucket_lcb_obj_, LCB_CALLBACK_STORE, set_callback);
  lcb_install_callback3(bucket_lcb_obj_, LCB_CALLBACK_SDMUTATE,
                        sdmutate_callback);
  lcb_install_callback3(bucket_lcb_obj_, LCB_CALLBACK_REMOVE, del_callback);

  lcb_U32 lcb_timeout = 2500000; // 2.5s
  err = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_cntl, bucket_lcb_obj_,
                              LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT, &lcb_timeout);
  if (err != LCB_SUCCESS) {
    init_success = false;
    LOG(logError) << "Bucket: Unable to set timeout for bucket ops, err: "
                  << err << std::endl;
  }

  bool enableDetailedErrCodes = true;
  err = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_cntl, bucket_lcb_obj_,
                              LCB_CNTL_SET, LCB_CNTL_DETAILED_ERRCODES,
                              &enableDetailedErrCodes);
  if (err != LCB_SUCCESS) {
    LOG(logWarning) << "Bucket: Unable to set detailed error codes. Defaulting "
                       "to normal error codes, err: "
                    << err << std::endl;
  }

  if (init_success) {
    LOG(logInfo) << "Bucket: lcb instance instantiated successfully with "
                    "connection string : "
                 << RS(conn_str_) << std::endl;
  } else {
    LOG(logError)
        << "Bucket: Unable to initialize lcb instance with connection string : "
        << RS(conn_str_) << std::endl;
  }
}

Bucket::~Bucket() {
  lcb_destroy(bucket_lcb_obj_);
  context_.Reset();
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
  v8::Local<v8::Object> result;
  if (!TO_LOCAL(templ->NewInstance(context), &result)) {
    return handle_scope.Escape(result);
  }

  result->SetInternalField(static_cast<int>(InternalFields::kLcbInstance),
                           v8::External::New(isolate_, &bucket_lcb_obj_));
  result->SetInternalField(static_cast<int>(InternalFields::kBlockMutation),
                           v8::External::New(isolate_, &block_mutation_));
  result->SetInternalField(static_cast<int>(InternalFields::kIsSourceBucket),
                           v8::External::New(isolate_, &is_source_bucket_));
  return handle_scope.Escape(result);
}

// Adds the bucket object as a global variable in JavaScript
bool Bucket::InstallMaps() {
  v8::HandleScope handle_scope(isolate_);

  auto bucket_obj = WrapBucketMap();
  auto context = v8::Local<v8::Context>::New(isolate_, context_);

  LOG(logInfo) << "Bucket: Registering handler for bucket_alias: "
               << bucket_alias_ << " connection string : " << RS(conn_str_)
               << std::endl;

  auto global = context->Global();
  auto install_maps_status = false;

  TO(global->Set(context, v8Str(isolate_, bucket_alias_.c_str()), bucket_obj),
     &install_maps_status);
  return install_maps_status;
}

// Performs the lcb related calls when bucket object is accessed
template <>
void Bucket::BucketGet<v8::Local<v8::Name>>(
    const v8::Local<v8::Name> &name,
    const v8::PropertyCallbackInfo<v8::Value> &info) {
  auto isolate = info.GetIsolate();
  auto js_exception = UnwrapData(isolate)->js_exception;
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  auto validate_info = ValidateKey(name);
  if (validate_info.is_fatal) {
    js_exception->ThrowEventingError(validate_info.msg);
    ++bucket_op_exception_count;
    return;
  }

  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  v8::String::Utf8Value utf8_key(isolate, name.As<v8::String>());
  std::string key(*utf8_key);

  auto bucket_lcb_obj_ptr = UnwrapInternalField<lcb_t>(
      info.Holder(), static_cast<int>(InternalFields::kLcbInstance));

  Result result;
  lcb_CMDGET gcmd = {0};
  LCB_CMD_SET_KEY(&gcmd, key.c_str(), key.length());
  lcb_sched_enter(*bucket_lcb_obj_ptr);
  auto err = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_get3,
                                   *bucket_lcb_obj_ptr, &result, &gcmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_GET: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    lcb_retry_failure++;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  lcb_sched_leave(*bucket_lcb_obj_ptr);

  err =
      RetryWithFixedBackoff(5, 200, IsRetriable, lcb_wait, *bucket_lcb_obj_ptr);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_GET: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    lcb_retry_failure++;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  if (result.rc == LCB_KEY_ENOENT) {
    info.GetReturnValue().Set(v8::Undefined(isolate));
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

  v8::Local<v8::Value> value_json;
  TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate, result.value)), &value_json);
  info.GetReturnValue().Set(value_json);
}

// Performs the lcb related calls when bucket object is accessed
template <>
void Bucket::BucketSet<v8::Local<v8::Name>>(
    const v8::Local<v8::Name> &name, const v8::Local<v8::Value> &value_obj,
    const v8::PropertyCallbackInfo<v8::Value> &info) {
  auto isolate = info.GetIsolate();
  auto js_exception = UnwrapData(isolate)->js_exception;
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  auto validate_info = ValidateKeyValue(name, value_obj);
  if (validate_info.is_fatal) {
    js_exception->ThrowEventingError(validate_info.msg);
    ++bucket_op_exception_count;
    return;
  }

  auto block_mutation = UnwrapInternalField<bool>(
      info.Holder(), static_cast<int>(InternalFields::kBlockMutation));
  if (*block_mutation) {
    js_exception->ThrowKVError("Writing to source bucket is forbidden");
    ++bucket_op_exception_count;
    return;
  }

  auto is_source_bucket = UnwrapInternalField<bool>(
      info.Holder(), static_cast<int>(InternalFields::kIsSourceBucket));
  if (*is_source_bucket) {
    BucketSetWithXattr(name, value_obj, info);
  } else {
    BucketSetWithoutXattr(name, value_obj, info);
  }
}

// Performs the lcb related calls when bucket object is accessed
template <>
void Bucket::BucketDelete<v8::Local<v8::Name>>(
    const v8::Local<v8::Name> &name,
    const v8::PropertyCallbackInfo<v8::Boolean> &info) {
  auto isolate = info.GetIsolate();
  auto js_exception = UnwrapData(isolate)->js_exception;
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  auto validate_info = ValidateKey(name);
  if (validate_info.is_fatal) {
    js_exception->ThrowKVError(validate_info.msg);
    ++bucket_op_exception_count;
    return;
  }

  auto block_mutation = UnwrapInternalField<bool>(
      info.Holder(), static_cast<int>(InternalFields::kBlockMutation));
  if (*block_mutation) {
    js_exception->ThrowKVError("Delete from source bucket is forbidden");
    ++bucket_op_exception_count;
    return;
  }

  auto is_source_bucket = UnwrapInternalField<bool>(
      info.Holder(), static_cast<int>(InternalFields::kIsSourceBucket));
  if (*is_source_bucket) {
    BucketDeleteWithXattr(name, info);
  } else {
    BucketDeleteWithoutXattr(name, info);
  }
}

void Bucket::HandleBucketOpFailure(v8::Isolate *isolate,
                                   lcb_t bucket_lcb_obj_ptr,
                                   lcb_error_t error) {
  auto isolate_data = UnwrapData(isolate);
  AddLcbException(isolate_data, error);
  ++bucket_op_exception_count;

  auto js_exception = isolate_data->js_exception;
  js_exception->ThrowKVError(bucket_lcb_obj_ptr, error);
}

// Registers the necessary callbacks to the bucket object in JavaScript
v8::Local<v8::ObjectTemplate> Bucket::MakeBucketMapTemplate() {
  v8::EscapableHandleScope handle_scope(isolate_);

  auto result = v8::ObjectTemplate::New(isolate_);
  // We will store lcb_instance associated with this bucket object in the
  // internal field
  result->SetInternalFieldCount(
      static_cast<int>(InternalFields::kMaxInternalFields));
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

void Bucket::BucketSetWithXattr(
    const v8::Local<v8::Name> &name, const v8::Local<v8::Value> &value_obj,
    const v8::PropertyCallbackInfo<v8::Value> &info) {
  auto isolate = info.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  v8::String::Utf8Value utf8_key(isolate, name.As<v8::String>());
  std::string key(*utf8_key);
  auto value = JSONStringify(isolate, value_obj);

  LOG(logTrace) << "Bucket: Set call Key: " << RU(key)
                << " Value: " << RU(value) << std::endl;

  auto bucket_lcb_obj_ptr = UnwrapInternalField<lcb_t>(
      info.Holder(), static_cast<int>(InternalFields::kLcbInstance));
  Result mres;

  lcb_SDSPEC function_id_spec = {0};
  std::string function_instance_id = GetFunctionInstanceID(isolate);
  std::string function_instance_id_path("_eventing.fiid");
  function_id_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
  function_id_spec.options =
      LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTRPATH;
  LCB_SDSPEC_SET_PATH(&function_id_spec, function_instance_id_path.c_str(),
                      function_instance_id_path.size());
  LCB_SDSPEC_SET_VALUE(&function_id_spec, function_instance_id.c_str(),
                       function_instance_id.size());

  lcb_SDSPEC dcp_seqno_spec = {0};
  std::string dcp_seqno_path("_eventing.seqno");
  std::string dcp_seqno_macro(R"("${Mutation.seqno}")");
  dcp_seqno_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
  dcp_seqno_spec.options =
      LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTR_MACROVALUES;
  LCB_SDSPEC_SET_PATH(&dcp_seqno_spec, dcp_seqno_path.c_str(),
                      dcp_seqno_path.size());
  LCB_SDSPEC_SET_VALUE(&dcp_seqno_spec, dcp_seqno_macro.c_str(),
                       dcp_seqno_macro.size());

  lcb_SDSPEC value_crc32_spec = {0};
  std::string value_crc32_path("_eventing.crc");
  std::string value_crc32_macro(R"("${Mutation.value_crc32c}")");
  value_crc32_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
  value_crc32_spec.options =
      LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTR_MACROVALUES;
  LCB_SDSPEC_SET_PATH(&value_crc32_spec, value_crc32_path.c_str(),
                      value_crc32_path.size());
  LCB_SDSPEC_SET_VALUE(&value_crc32_spec, value_crc32_macro.c_str(),
                       value_crc32_macro.size());

  lcb_SDSPEC doc_spec = {0};
  doc_spec.sdcmd = LCB_SDCMD_SET_FULLDOC;
  LCB_SDSPEC_SET_PATH(&doc_spec, "", 0);
  LCB_SDSPEC_SET_VALUE(&doc_spec, value.c_str(), value.length());

  std::vector<lcb_SDSPEC> specs = {function_id_spec, dcp_seqno_spec,
                                   value_crc32_spec, doc_spec};
  lcb_CMDSUBDOC mcmd = {0};
  LCB_CMD_SET_KEY(&mcmd, key.c_str(), key.length());
  mcmd.specs = specs.data();
  mcmd.nspecs = specs.size();
  mcmd.cmdflags = LCB_CMDSUBDOC_F_UPSERT_DOC;

  lcb_sched_enter(*bucket_lcb_obj_ptr);
  auto err = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_subdoc3,
                                   *bucket_lcb_obj_ptr, &mres, &mcmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_SET: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    lcb_retry_failure++;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  lcb_sched_leave(*bucket_lcb_obj_ptr);
  err =
      RetryWithFixedBackoff(5, 200, IsRetriable, lcb_wait, *bucket_lcb_obj_ptr);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_SET: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    lcb_retry_failure++;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  // Throw an exception in JavaScript if the bucket set call failed.
  if (mres.rc != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: LCB_STORE call failed: " << mres.rc << std::endl;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, mres.rc);
    return;
  }

  info.GetReturnValue().Set(value_obj);
}

void Bucket::BucketSetWithoutXattr(
    const v8::Local<v8::Name> &name, const v8::Local<v8::Value> &value_obj,
    const v8::PropertyCallbackInfo<v8::Value> &info) {
  auto isolate = info.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  v8::String::Utf8Value utf8_key(isolate, name.As<v8::String>());
  std::string key(*utf8_key);
  auto value = JSONStringify(isolate, value_obj);

  LOG(logTrace) << "Bucket: Set call Key: " << RU(key)
                << " Value: " << RU(value) << std::endl;

  auto bucket_lcb_obj_ptr = UnwrapInternalField<lcb_t>(
      info.Holder(), static_cast<int>(InternalFields::kLcbInstance));
  Result sres;

  lcb_CMDSTORE scmd = {0};
  LCB_CMD_SET_KEY(&scmd, key.c_str(), key.length());
  LCB_CMD_SET_VALUE(&scmd, value.c_str(), value.length());
  scmd.operation = LCB_SET;
  scmd.flags = 0x2000000;

  lcb_sched_enter(*bucket_lcb_obj_ptr);
  auto err = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_store3,
                                   *bucket_lcb_obj_ptr, &sres, &scmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_SET: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    lcb_retry_failure++;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  lcb_sched_leave(*bucket_lcb_obj_ptr);
  err =
      RetryWithFixedBackoff(5, 200, IsRetriable, lcb_wait, *bucket_lcb_obj_ptr);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_SET: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    lcb_retry_failure++;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  // Throw an exception in JavaScript if the bucket set call failed.
  if (sres.rc != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: LCB_STORE call failed: " << sres.rc << std::endl;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, sres.rc);
    return;
  }

  info.GetReturnValue().Set(value_obj);
}

void Bucket::BucketDeleteWithXattr(
    const v8::Local<v8::Name> &name,
    const v8::PropertyCallbackInfo<v8::Boolean> &info) {
  auto isolate = info.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  v8::String::Utf8Value utf8_key(isolate, name.As<v8::String>());
  std::string key(*utf8_key);

  auto bucket_lcb_obj_ptr = UnwrapInternalField<lcb_t>(
      info.Holder(), static_cast<int>(InternalFields::kLcbInstance));

  Result result;
  lcb_SDSPEC function_id_spec = {0};
  std::string function_instance_id = GetFunctionInstanceID(isolate);
  std::string function_instance_id_path("_eventing.fiid");
  function_id_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
  function_id_spec.options =
      LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTRPATH;
  LCB_SDSPEC_SET_PATH(&function_id_spec, function_instance_id_path.c_str(),
                      function_instance_id_path.size());
  LCB_SDSPEC_SET_VALUE(&function_id_spec, function_instance_id.c_str(),
                       function_instance_id.size());

  lcb_SDSPEC dcp_seqno_spec = {0};
  std::string dcp_seqno_path("_eventing.seqno");
  std::string dcp_seqno_macro(R"("${Mutation.seqno}")");
  dcp_seqno_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
  dcp_seqno_spec.options =
      LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTR_MACROVALUES;
  LCB_SDSPEC_SET_PATH(&dcp_seqno_spec, dcp_seqno_path.c_str(),
                      dcp_seqno_path.size());
  LCB_SDSPEC_SET_VALUE(&dcp_seqno_spec, dcp_seqno_macro.c_str(),
                       dcp_seqno_macro.size());

  lcb_SDSPEC value_crc32_spec = {0};
  std::string value_crc32_path("_eventing.crc");
  std::string value_crc32_macro(R"("${Mutation.value_crc32c}")");
  value_crc32_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
  value_crc32_spec.options =
      LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTR_MACROVALUES;
  LCB_SDSPEC_SET_PATH(&value_crc32_spec, value_crc32_path.c_str(),
                      value_crc32_path.size());
  LCB_SDSPEC_SET_VALUE(&value_crc32_spec, value_crc32_macro.c_str(),
                       value_crc32_macro.size());

  lcb_SDSPEC doc_spec = {0};
  doc_spec.sdcmd = LCB_SDCMD_REMOVE_FULLDOC;
  LCB_SDSPEC_SET_PATH(&doc_spec, "", 0);
  LCB_SDSPEC_SET_VALUE(&doc_spec, "", 0);

  std::vector<lcb_SDSPEC> specs = {function_id_spec, dcp_seqno_spec,
                                   value_crc32_spec, doc_spec};
  lcb_CMDSUBDOC mcmd = {0};
  LCB_CMD_SET_KEY(&mcmd, key.c_str(), key.length());
  mcmd.specs = specs.data();
  mcmd.nspecs = specs.size();

  lcb_sched_enter(*bucket_lcb_obj_ptr);
  auto err = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_subdoc3,
                                   *bucket_lcb_obj_ptr, &result, &mcmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_REMOVE: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    lcb_retry_failure++;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  lcb_sched_leave(*bucket_lcb_obj_ptr);
  err =
      RetryWithFixedBackoff(5, 200, IsRetriable, lcb_wait, *bucket_lcb_obj_ptr);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_REMOVE: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    lcb_retry_failure++;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  if (result.rc == LCB_KEY_ENOENT) {
    return;
  }

  // Throw an exception in JavaScript if the bucket delete call failed.
  if (result.rc != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: LCB_REMOVE call failed: " << result.rc
                  << std::endl;
    lcb_retry_failure++;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, result.rc);
    return;
  }

  info.GetReturnValue().Set(true);
}

void Bucket::BucketDeleteWithoutXattr(
    const v8::Local<v8::Name> &name,
    const v8::PropertyCallbackInfo<v8::Boolean> &info) {
  auto isolate = info.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  v8::String::Utf8Value utf8_key(isolate, name.As<v8::String>());
  std::string key(*utf8_key);

  auto bucket_lcb_obj_ptr = UnwrapInternalField<lcb_t>(
      info.Holder(), static_cast<int>(InternalFields::kLcbInstance));

  Result result;
  lcb_CMDREMOVE rcmd = {0};
  LCB_CMD_SET_KEY(&rcmd, key.c_str(), key.length());

  lcb_sched_enter(*bucket_lcb_obj_ptr);
  auto err = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_remove3,
                                   *bucket_lcb_obj_ptr, &result, &rcmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_REMOVE: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    lcb_retry_failure++;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  lcb_sched_leave(*bucket_lcb_obj_ptr);
  err =
      RetryWithFixedBackoff(5, 200, IsRetriable, lcb_wait, *bucket_lcb_obj_ptr);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_REMOVE: "
                  << lcb_strerror(*bucket_lcb_obj_ptr, err) << std::endl;
    lcb_retry_failure++;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, err);
    return;
  }

  if (result.rc == LCB_KEY_ENOENT) {
    return;
  }

  // Throw an exception in JavaScript if the bucket delete call failed.
  if (result.rc != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: LCB_REMOVE call failed: " << result.rc
                  << std::endl;
    lcb_retry_failure++;
    HandleBucketOpFailure(isolate, *bucket_lcb_obj_ptr, result.rc);
    return;
  }

  info.GetReturnValue().Set(true);
}

Info Bucket::ValidateKey(const v8::Local<v8::Name> &arg) {
  auto info = Utils::ValidateDataType(arg);
  if (info.is_fatal) {
    return {true, "Invalid data type for key - " + info.msg};
  }
  return {false};
}

Info Bucket::ValidateValue(const v8::Local<v8::Value> &arg) {
  auto info = Utils::ValidateDataType(arg);
  if (info.is_fatal) {
    return {true, "Invalid data type for value - " + info.msg};
  }
  return {false};
}

Info Bucket::ValidateKeyValue(const v8::Local<v8::Name> &key,
                              const v8::Local<v8::Value> &value) {
  auto info = ValidateKey(key);
  if (info.is_fatal) {
    return info;
  }
  info = ValidateValue(value);
  if (info.is_fatal) {
    return info;
  }
  return {false};
}
