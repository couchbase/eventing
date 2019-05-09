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
#include "js_exception.h"
#include "retry_util.h"
#include "utils.h"

#undef NDEBUG

std::atomic<int64_t> bucket_op_exception_count = {0};
std::atomic<int64_t> lcb_retry_failure = {0};

const char *GetUsername(void *cookie, const char *host, const char *port,
                        const char *bucket) {
  LOG(logDebug) << "Getting username for host " << RS(host) << " port " << port
                << std::endl;

  auto endpoint = JoinHostPort(host, port);
  auto isolate = static_cast<v8::Isolate *>(cookie);
  auto comm = UnwrapData(isolate)->comm;
  auto info = comm->GetCreds(endpoint);
  if (!info.is_valid) {
    LOG(logError) << "Failed to get username for " << RS(host) << ":" << port
                  << " err: " << info.msg << std::endl;
  }

  static const char *username = "";
  if (info.username != username) {
    username = strdup(info.username.c_str());
  }

  return username;
}

const char *GetPassword(void *cookie, const char *host, const char *port,
                        const char *bucket) {
  LOG(logDebug) << "Getting password for host " << RS(host) << " port " << port
                << std::endl;

  auto isolate = static_cast<v8::Isolate *>(cookie);
  auto comm = UnwrapData(isolate)->comm;
  auto endpoint = JoinHostPort(host, port);
  auto info = comm->GetCreds(endpoint);
  if (!info.is_valid) {
    LOG(logError) << "Failed to get password for " << RS(host) << ":" << port
                  << " err: " << info.msg << std::endl;
  }

  static const char *password = "";
  if (info.password != password) {
    password = strdup(info.password.c_str());
  }

  return password;
}

// lcb related callbacks
static void get_callback(lcb_t instance, int, const lcb_RESPBASE *rb) {
  auto resp = reinterpret_cast<const lcb_RESPGET *>(rb);
  auto result = reinterpret_cast<Result *>(rb->cookie);

  LOG(logTrace) << "Bucket: LCB_GET callback, res: "
                << lcb_strerror(nullptr, rb->rc) << rb->rc << " cas " << rb->cas
                << std::endl;

  // TODO : Check if there's a specific error code for missing bucket
  if (rb->rc == LCB_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_GET breaking out" << std::endl;
    lcb_breakout(instance);
  }

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

  if (rb->rc == LCB_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_STORE breaking out" << std::endl;
    lcb_breakout(instance);
  }

  result->rc = resp->rc;
  result->cas = resp->cas;

  LOG(logTrace) << "Bucket: LCB_STORE callback "
                << lcb_strerror(instance, result->rc) << " cas " << resp->cas
                << std::endl;
}

static void sdmutate_callback(lcb_t instance, int cbtype,
                              const lcb_RESPBASE *rb) {
  auto result = reinterpret_cast<Result *>(rb->cookie);
  result->rc = rb->rc;

  if (rb->rc == LCB_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_SDMUTATE breaking out" << std::endl;
    lcb_breakout(instance);
  }

  LOG(logTrace) << "Bucket: LCB_SDMUTATE callback "
                << lcb_strerror(nullptr, result->rc) << std::endl;
}

static void del_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  auto result = reinterpret_cast<Result *>(rb->cookie);
  result->rc = rb->rc;

  if (rb->rc == LCB_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_DEL breaking out" << std::endl;
    lcb_breakout(instance);
  }

  LOG(logTrace) << "Bucket: LCB_DEL callback "
                << lcb_strerror(nullptr, result->rc) << std::endl;
}

#define EVT_LOG_MSG_SIZE 1024

static void evt_log_formatter(char *buf, int buf_size, const char *subsystem,
                              int srcline, unsigned int instance_id,
                              const char *fmt, va_list ap) {
  char msg[EVT_LOG_MSG_SIZE] = {};

  vsnprintf(msg, EVT_LOG_MSG_SIZE, fmt, ap);
  msg[EVT_LOG_MSG_SIZE - 1] = '\0';
  for (int i = 0; i < EVT_LOG_MSG_SIZE; i++) {
    if (msg[i] == '\n') {
      msg[i] = ' ';
    }
  }
  snprintf(buf, buf_size, "[lcb,%s L:%d I:%u] %s", subsystem, srcline,
           instance_id, msg);
}

/**
 * Conversion needed as libcouchbase using ascending order for level, while
 * eventing is using reversed order.
 */
static LogLevel evt_log_map_level(int severity) {
  switch (severity) {
  case LCB_LOG_TRACE:
    return logTrace;
  case LCB_LOG_DEBUG:
    return logDebug;
  case LCB_LOG_INFO:
    return logInfo;
  case LCB_LOG_WARN:
    return logWarning;
  case LCB_LOG_ERROR:
  case LCB_LOG_FATAL:
  default:
    return logError;
  }
}

static bool evt_should_log(int severity, const char *subsys) {
  if (strcmp(subsys, "negotiation") == 0) {
    return false;
  }

  if (evt_log_map_level(severity) <= SystemLog::level_) {
    return true;
  }

  return false;
}

static void evt_log_handler(struct lcb_logprocs_st *procs, unsigned int iid,
                            const char *subsys, int severity,
                            const char *srcfile, int srcline, const char *fmt,
                            va_list ap) {
  if (evt_should_log(severity, subsys)) {
    char buf[EVT_LOG_MSG_SIZE] = {};
    evt_log_formatter(buf, EVT_LOG_MSG_SIZE, subsys, srcline, iid, fmt, ap);
    LOG(evt_log_map_level(severity)) << buf << std::endl;
  }
}

struct lcb_logprocs_st evt_logger = {
    0, /* version */
    {
        {evt_log_handler} /* v1 */
    }                     /* v */
};

Bucket::Bucket(v8::Isolate *isolate, const v8::Local<v8::Context> &context,
               const std::string &bucket_name, const std::string &endpoint,
               const std::string &alias, bool block_mutation,
               bool is_source_bucket)
    : isolate_(isolate), block_mutation_(block_mutation),
      is_source_bucket_(is_source_bucket), bucket_name_(bucket_name),
      endpoint_(endpoint), bucket_alias_(alias) {
  context_.Reset(isolate_, context);

  auto init_success = true;
  auto connstr = GetConnectionStr(endpoint_, bucket_name_);
  LOG(logInfo) << "Bucket: connstr " << RS(connstr) << std::endl;

  // lcb related setup
  lcb_create_st crst;
  memset(&crst, 0, sizeof crst);

  crst.version = 3;
  crst.v.v3.connstr = connstr.c_str();
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
    LOG(logInfo) << "Bucket: lcb instance for " << bucket_name
                 << " initialized successfully" << std::endl;
  } else {
    LOG(logError) << "Bucket: Unable to initialize lcb instance for "
                  << bucket_name << std::endl;
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
               << bucket_alias_ << " bucket_name: " << bucket_name_
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
  if (name->IsSymbol()) {
    auto js_exception = UnwrapData(isolate)->js_exception;
    js_exception->ThrowEventingError("Symbol data type is not supported");
    ++bucket_op_exception_count;
    return;
  }

  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  v8::String::Utf8Value utf8_key(name.As<v8::String>());
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
  if (name->IsSymbol()) {
    auto js_exception = UnwrapData(isolate)->js_exception;
    js_exception->ThrowEventingError("Symbol data type is not supported");
    ++bucket_op_exception_count;
    return;
  }

  auto block_mutation = UnwrapInternalField<bool>(
      info.Holder(), static_cast<int>(InternalFields::kBlockMutation));
  if (*block_mutation) {
    auto js_exception = UnwrapData(info.GetIsolate())->js_exception;
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
  if (name->IsSymbol()) {
    auto js_exception = UnwrapData(isolate)->js_exception;
    js_exception->ThrowKVError("Symbol data type is not supported");
    ++bucket_op_exception_count;
    return;
  }

  auto block_mutation = UnwrapInternalField<bool>(
      info.Holder(), static_cast<int>(InternalFields::kBlockMutation));
  if (*block_mutation) {
    auto js_exception = UnwrapData(info.GetIsolate())->js_exception;
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
  v8::String::Utf8Value utf8_key(name.As<v8::String>());
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
  v8::String::Utf8Value utf8_key(name.As<v8::String>());
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
  v8::String::Utf8Value utf8_key(name.As<v8::String>());
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
  v8::String::Utf8Value utf8_key(name.As<v8::String>());
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
