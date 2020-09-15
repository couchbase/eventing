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

#include <algorithm>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>

#include "bucket.h"
#include "error.h"
#include "js_exception.h"
#include "lang_compat.h"
#include "lcb_utils.h"
#include "retry_util.h"
#include "utils.h"
#include "v8worker.h"

std::atomic<int64_t> bucket_op_exception_count = {0};
std::atomic<int64_t> lcb_retry_failure = {0};

BucketFactory::BucketFactory(v8::Isolate *isolate,
                             const v8::Local<v8::Context> &context)
    : isolate_(isolate) {
  v8::HandleScope handle_scope(isolate_);
  context_.Reset(isolate_, context);

  auto bucket_template = v8::ObjectTemplate::New(isolate_);
  bucket_template->SetInternalFieldCount(BucketBinding::kInternalFieldsCount);

  // Register corresponding callbacks for alphanumeric accesses on bucket
  // object
  bucket_template->SetHandler(v8::NamedPropertyHandlerConfiguration(
      BucketBinding::BucketGetDelegate, BucketBinding::BucketSetDelegate,
      nullptr, BucketBinding::BucketDeleteDelegate));

  // Register corresponding callbacks for numeric accesses on bucket object
  bucket_template->SetIndexedPropertyHandler(
      v8::IndexedPropertyGetterCallback(BucketBinding::BucketGetDelegate),
      v8::IndexedPropertySetterCallback(BucketBinding::BucketSetDelegate),
      nullptr,
      v8::IndexedPropertyDeleterCallback(BucketBinding::BucketDeleteDelegate));
  bucket_template_.Reset(isolate_, bucket_template);
}

BucketFactory::~BucketFactory() {
  context_.Reset();
  bucket_template_.Reset();
}

std::pair<Error, std::unique_ptr<v8::Local<v8::Object>>>
BucketFactory::NewBucketObj() const {
  v8::EscapableHandleScope handle_scope(isolate_);

  const auto context = context_.Get(isolate_);
  auto bucket_template = bucket_template_.Get(isolate_);

  v8::Local<v8::Object> bucket_obj;
  if (!TO_LOCAL(bucket_template->NewInstance(context), &bucket_obj)) {
    return {std::make_unique<std::string>(
                "Unable to create an instance of bucket template"),
            nullptr};
  }
  return {nullptr, std::make_unique<v8::Local<v8::Object>>(
                       handle_scope.Escape(bucket_obj))};
}

Bucket::~Bucket() {
  if (is_connected_) {
    lcb_destroy(connection_);
  }
}

Error Bucket::Connect() {
  LOG(logTrace) << __func__ << " connecting to bucket " << RU(bucket_name_)
                << std::endl;
  if (is_connected_) {
    LOG(logError)
        << __func__
        << " Attempting to connect an already connected bucket instance"
        << std::endl;
    return std::make_unique<std::string>("already connected");
  }

  auto utils = UnwrapData(isolate_)->utils;

  auto conn_str_info = utils->GetConnectionString(bucket_name_);
  if (!conn_str_info.is_valid) {
    return std::make_unique<std::string>(conn_str_info.msg);
  }

  LOG(logInfo) << __func__
               << " connection string : " << RU(conn_str_info.conn_str)
               << std::endl;

  lcb_CREATEOPTS *options;
  lcb_createopts_create(&options, LCB_TYPE_BUCKET);
  lcb_createopts_connstr(options, conn_str_info.conn_str.c_str(),
                         strlen(conn_str_info.conn_str.c_str()));
  lcb_createopts_logger(options, evt_logger.base);

  auto result = lcb_create(&connection_, options);
  if (result != LCB_SUCCESS) {
    return FormatErrorAndDestroyConn("Unable to initialize connection handle",
                                     result);
  }
  lcb_createopts_destroy(options);

  auto auth = lcbauth_new();
  result = RetryWithFixedBackoff(5, 200, IsRetriable, lcbauth_set_callbacks,
                                 auth, isolate_, GetUsername, GetPassword);
  if (result != LCB_SUCCESS) {
    return FormatErrorAndDestroyConn("Unable to set auth callbacks", result);
  }

  result = RetryWithFixedBackoff(5, 200, IsRetriable, lcbauth_set_mode, auth,
                                 LCBAUTH_MODE_DYNAMIC);
  if (result != LCB_SUCCESS) {
    return FormatErrorAndDestroyConn("Unable to set auth mode to dynamic",
                                     result);
  }
  lcb_set_auth(connection_, auth);

  result = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_connect, connection_);
  if (result != LCB_SUCCESS) {
    return FormatErrorAndDestroyConn("Unable to connect to bucket", result);
  }

  result = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_wait, connection_,
                                 LCB_WAIT_DEFAULT);
  if (result != LCB_SUCCESS) {
    return FormatErrorAndDestroyConn("Unable to schedule call for connect",
                                     result);
  }

  lcb_install_callback(connection_, LCB_CALLBACK_GET, GetCallback);
  lcb_install_callback(connection_, LCB_CALLBACK_STORE, SetCallback);
  lcb_install_callback(connection_, LCB_CALLBACK_SDMUTATE, SubDocumentCallback);
  lcb_install_callback(connection_, LCB_CALLBACK_REMOVE, DeleteCallback);
  lcb_install_callback(connection_, LCB_CALLBACK_SDLOOKUP,
                       SubDocumentLookupCallback);

  // TODO : Need to make timeout configurable
  lcb_U32 lcb_timeout = 2500000; // 2.5s
  result =
      RetryWithFixedBackoff(5, 200, IsRetriable, lcb_cntl, connection_,
                            LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT, &lcb_timeout);
  if (result != LCB_SUCCESS) {
    return FormatErrorAndDestroyConn("Unable to set timeout for bucket ops",
                                     result);
  }

  auto enable_detailed_err_codes = true;
  result = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_cntl, connection_,
                                 LCB_CNTL_SET, LCB_CNTL_DETAILED_ERRCODES,
                                 &enable_detailed_err_codes);
  if (result != LCB_SUCCESS) {
    return FormatErrorAndDestroyConn("Unable to set detailed error codes",
                                     result);
  }
  LOG(logTrace) << __func__ << " connected to bucket " << RU(bucket_name_)
                << " successfully" << std::endl;
  is_connected_ = true;
  return nullptr;
}

Error Bucket::FormatErrorAndDestroyConn(const std::string &message,
                                        const lcb_STATUS &error) const {
  std::stringstream err_msg;
  err_msg << message << ", err: " << lcb_strerror_long(error);
  lcb_destroy(connection_);
  LOG(logError) << __func__ << " " << err_msg.str() << std::endl;
  return std::make_unique<std::string>(err_msg.str());
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::Get(const std::string &key) {
  if (!is_connected_) {
    return {std::make_unique<std::string>("Connection is not initialized"),
            nullptr, nullptr};
  }

  lcb_CMDGET *cmd;
  lcb_cmdget_create(&cmd);
  lcb_cmdget_collection(cmd, scope_name_.c_str(), scope_length_,
                        collection_name_.c_str(), collection_length_);
  lcb_cmdget_key(cmd, key.c_str(), key.length());

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  auto [err_code, result] =
      RetryLcbCommand(connection_, *cmd, max_retry, max_timeout, LcbGet);
  if (err_code != LCB_SUCCESS) {
    ++lcb_retry_failure;
    return {nullptr, std::make_unique<lcb_STATUS>(err_code), nullptr};
  }
  return {nullptr, std::make_unique<lcb_STATUS>(err_code),
          std::make_unique<Result>(std::move(result))};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::GetWithMeta(const std::string &key) {
  if (!is_connected_) {
    return {std::make_unique<std::string>("Connection is not initialized"),
            nullptr, nullptr};
  }

  lcb_SUBDOCSPECS *specs;
  lcb_subdocspecs_create(&specs, 3);

  char const *path = "$document.exptime";
  lcb_subdocspecs_get(specs, 0, LCB_SUBDOCSPECS_F_XATTRPATH, path,
                      strlen(path));

  char const *path2 = "$document.datatype";
  lcb_subdocspecs_get(specs, 1, LCB_SUBDOCSPECS_F_XATTRPATH, path2,
                      strlen(path2));

  lcb_subdocspecs_get(specs, 2, 0, "", 0);

  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_specs(cmd, specs);
  lcb_cmdsubdoc_collection(cmd, scope_name_.c_str(), scope_length_,
                           collection_name_.c_str(), collection_length_);
  lcb_cmdsubdoc_key(cmd, key.c_str(), key.length());
  lcb_cmdsubdoc_specs(cmd, specs);

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  auto [err_code, result] =
      RetryLcbCommand(connection_, *cmd, max_retry, max_timeout, LcbSubdocSet);
  if (err_code != LCB_SUCCESS) {
    ++lcb_retry_failure;
    return {nullptr, std::make_unique<lcb_STATUS>(err_code), nullptr};
  }
  return {nullptr, std::make_unique<lcb_STATUS>(err_code),
          std::make_unique<Result>(std::move(result))};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::CounterWithoutXattr(const std::string &key, uint64_t cas,
                            lcb_U32 expiry, int64_t delta) {
  if (!is_connected_) {
    return {std::make_unique<std::string>("Connection is not initialized"),
            nullptr, nullptr};
  }

  lcb_SUBDOCSPECS *spec;
  lcb_subdocspecs_create(&spec, 1);
  lcb_subdocspecs_counter(spec, 0, 0, "count", strlen("count"), delta);

  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_UPSERT);
  lcb_cmdsubdoc_specs(cmd, spec);
  lcb_cmdsubdoc_cas(cmd, cas);
  lcb_cmdsubdoc_expiry(cmd, expiry);
  lcb_cmdsubdoc_collection(cmd, scope_name_.c_str(), scope_length_,
                           collection_name_.c_str(), collection_length_);
  lcb_cmdsubdoc_key(cmd, key.c_str(), key.length());

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  auto [err_code, result] =
      RetryLcbCommand(connection_, *cmd, max_retry, max_timeout, LcbSubdocSet);
  if (err_code != LCB_SUCCESS) {
    ++lcb_retry_failure;
    return {nullptr, std::make_unique<lcb_STATUS>(err_code), nullptr};
  }
  return {nullptr, std::make_unique<lcb_STATUS>(err_code),
          std::make_unique<Result>(std::move(result))};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::CounterWithXattr(const std::string &key, uint64_t cas, lcb_U32 expiry,
                         int64_t delta) {
  if (!is_connected_) {
    return {std::make_unique<std::string>("Connection is not initialized"),
            nullptr, nullptr};
  }

  lcb_SUBDOCSPECS *specs;
  lcb_subdocspecs_create(&specs, 4);

  auto function_instance_id = GetFunctionInstanceID(isolate_);
  std::string function_instance_id_path("_eventing.fiid");
  lcb_subdocspecs_dict_upsert(
      specs, 0, LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTRPATH,
      function_instance_id_path.c_str(), function_instance_id_path.size(),
      function_instance_id.c_str(), function_instance_id.size());

  std::string dcp_seqno_path("_eventing.seqno");
  std::string dcp_seqno_macro(R"("${Mutation.seqno}")");
  lcb_subdocspecs_dict_upsert(specs, 1,
                              LCB_SUBDOCSPECS_F_MKINTERMEDIATES |
                                  LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
                              dcp_seqno_path.c_str(), dcp_seqno_path.size(),
                              dcp_seqno_macro.c_str(), dcp_seqno_macro.size());

  std::string value_crc32_path("_eventing.crc");
  std::string value_crc32_macro(R"("${Mutation.value_crc32c}")");
  lcb_subdocspecs_dict_upsert(
      specs, 2,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      value_crc32_path.c_str(), value_crc32_path.size(),
      value_crc32_macro.c_str(), value_crc32_macro.size());

  lcb_subdocspecs_counter(specs, 3, 0, "count", strlen("count"), delta);

  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_specs(cmd, specs);
  lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_UPSERT);
  lcb_cmdsubdoc_cas(cmd, cas);
  lcb_cmdsubdoc_expiry(cmd, expiry);
  lcb_cmdsubdoc_collection(cmd, scope_name_.c_str(), scope_length_,
                           collection_name_.c_str(), collection_length_);

  lcb_cmdsubdoc_key(cmd, key.c_str(), key.length());

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  auto [err_code, result] =
      RetryLcbCommand(connection_, *cmd, max_retry, max_timeout, LcbSubdocSet);
  if (err_code != LCB_SUCCESS) {
    ++lcb_retry_failure;
    return {nullptr, std::make_unique<lcb_STATUS>(err_code), nullptr};
  }
  return {nullptr, std::make_unique<lcb_STATUS>(err_code),
          std::make_unique<Result>(std::move(result))};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::SetWithXattr(const std::string &key, const char *value,
                     int value_length, lcb_SUBDOC_STORE_SEMANTICS op_type,
                     lcb_U32 expiry, uint64_t cas) {
  if (!is_connected_) {
    return {std::make_unique<std::string>("Connection is not initialized"),
            nullptr, nullptr};
  }

  lcb_SUBDOCSPECS *specs;
  lcb_subdocspecs_create(&specs, 4);
  auto function_instance_id = GetFunctionInstanceID(isolate_);
  std::string function_instance_id_path("_eventing.fiid");
  lcb_subdocspecs_dict_upsert(
      specs, 0, LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTRPATH,
      function_instance_id_path.c_str(), function_instance_id_path.size(),
      function_instance_id.c_str(), function_instance_id.size());

  std::string dcp_seqno_path("_eventing.seqno");
  std::string dcp_seqno_macro(R"("${Mutation.seqno}")");
  lcb_subdocspecs_dict_upsert(specs, 1,
                              LCB_SUBDOCSPECS_F_MKINTERMEDIATES |
                                  LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
                              dcp_seqno_path.c_str(), dcp_seqno_path.size(),
                              dcp_seqno_macro.c_str(), dcp_seqno_macro.size());

  std::string value_crc32_path("_eventing.crc");
  std::string value_crc32_macro(R"("${Mutation.value_crc32c}")");
  lcb_subdocspecs_dict_upsert(
      specs, 2,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      value_crc32_path.c_str(), value_crc32_path.size(),
      value_crc32_macro.c_str(), value_crc32_macro.size());

  lcb_subdocspecs_replace(specs, 3, 0, "", 0, value, value_length);

  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_specs(cmd, specs);
  lcb_cmdsubdoc_cas(cmd, cas);
  lcb_cmdsubdoc_expiry(cmd, expiry);
  lcb_cmdsubdoc_collection(cmd, scope_name_.c_str(), scope_length_,
                           collection_name_.c_str(), collection_length_);
  lcb_cmdsubdoc_key(cmd, key.c_str(), key.length());
  lcb_cmdsubdoc_store_semantics(cmd, op_type);

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  auto [err_code, result] =
      RetryLcbCommand(connection_, *cmd, max_retry, max_timeout, LcbSubdocSet);
  if (err_code != LCB_SUCCESS) {
    ++lcb_retry_failure;
    return {nullptr, std::make_unique<lcb_STATUS>(err_code), nullptr};
  }
  return {nullptr, std::make_unique<lcb_STATUS>(err_code),
          std::make_unique<Result>(std::move(result))};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::SetWithoutXattr(const std::string &key, const char *value,
                        int value_length, lcb_STORE_OPERATION op_type,
                        lcb_U32 expiry, uint64_t cas, lcb_U32 doc_type) {
  if (!is_connected_) {
    return {std::make_unique<std::string>("Connection is not initialized"),
            nullptr, nullptr};
  }

  lcb_CMDSTORE *cmd;
  lcb_cmdstore_create(&cmd, op_type);
  lcb_cmdstore_expiry(cmd, expiry);
  lcb_cmdstore_cas(cmd, cas);
  lcb_cmdstore_collection(cmd, scope_name_.c_str(), scope_length_,
                          collection_name_.c_str(), collection_length_);

  lcb_cmdstore_key(cmd, key.c_str(), key.length());
  lcb_cmdstore_value(cmd, value, value_length);

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  auto [err_code, result] =
      RetryLcbCommand(connection_, *cmd, max_retry, max_timeout, LcbSet);
  if (err_code != LCB_SUCCESS) {
    ++lcb_retry_failure;
    return {nullptr, std::make_unique<lcb_STATUS>(err_code), nullptr};
  }
  return {nullptr, std::make_unique<lcb_STATUS>(err_code),
          std::make_unique<Result>(std::move(result))};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::DeleteWithXattr(const std::string &key, uint64_t cas) {
  if (!is_connected_) {
    return {std::make_unique<std::string>("Connection is not initialized"),
            nullptr, nullptr};
  }

  lcb_SUBDOCSPECS *specs;
  lcb_subdocspecs_create(&specs, 4);

  auto function_instance_id = GetFunctionInstanceID(isolate_);
  std::string function_instance_id_path("_eventing.fiid");
  lcb_subdocspecs_dict_upsert(
      specs, 0, LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTRPATH,
      function_instance_id_path.c_str(), function_instance_id_path.size(),
      function_instance_id.c_str(), function_instance_id.size());

  std::string dcp_seqno_path("_eventing.seqno");
  std::string dcp_seqno_macro(R"("${Mutation.seqno}")");
  lcb_subdocspecs_dict_upsert(specs, 1,
                              LCB_SUBDOCSPECS_F_MKINTERMEDIATES |
                                  LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
                              dcp_seqno_path.c_str(), dcp_seqno_path.size(),
                              dcp_seqno_macro.c_str(), dcp_seqno_macro.size());

  std::string value_crc32_path("_eventing.crc");
  std::string value_crc32_macro(R"("${Mutation.value_crc32c}")");
  lcb_subdocspecs_dict_upsert(
      specs, 2,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      value_crc32_path.c_str(), value_crc32_path.size(),
      value_crc32_macro.c_str(), value_crc32_macro.size());

  lcb_subdocspecs_remove(specs, 3, 0, "", 0);
  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_specs(cmd, specs);
  lcb_cmdsubdoc_cas(cmd, cas);
  lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_REPLACE);

  lcb_cmdsubdoc_collection(cmd, scope_name_.c_str(), scope_length_,
                           collection_name_.c_str(), collection_length_);
  lcb_cmdsubdoc_key(cmd, key.c_str(), key.length());

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  auto [err_code, result] = RetryLcbCommand(connection_, *cmd, max_retry,
                                            max_timeout, LcbSubdocDelete);
  if (err_code != LCB_SUCCESS) {
    ++lcb_retry_failure;
    return {nullptr, std::make_unique<lcb_STATUS>(err_code), nullptr};
  }
  return {nullptr, std::make_unique<lcb_STATUS>(err_code),
          std::make_unique<Result>(std::move(result))};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::DeleteWithoutXattr(const std::string &key, uint64_t cas) {
  if (!is_connected_) {
    return {std::make_unique<std::string>("Connection is not initialized"),
            nullptr, nullptr};
  }

  lcb_CMDREMOVE *cmd;
  lcb_cmdremove_create(&cmd);
  lcb_cmdremove_cas(cmd, cas);
  lcb_cmdremove_collection(cmd, scope_name_.c_str(), scope_length_,
                           collection_name_.c_str(), collection_length_);

  lcb_cmdremove_key(cmd, key.c_str(), key.length());

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  auto [err_code, result] =
      RetryLcbCommand(connection_, *cmd, max_retry, max_timeout, LcbDelete);
  if (err_code != LCB_SUCCESS) {
    ++lcb_retry_failure;
    return {nullptr, std::make_unique<lcb_STATUS>(err_code), nullptr};
  }
  return {nullptr, std::make_unique<lcb_STATUS>(err_code),
          std::make_unique<Result>(std::move(result))};
}

// Performs the lcb related calls when bucket object is accessed
template <>
void BucketBinding::BucketGet<v8::Local<v8::Name>>(
    const v8::Local<v8::Name> &name,
    const v8::PropertyCallbackInfo<v8::Value> &info) {
  auto isolate = info.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  auto js_exception = isolate_data->js_exception;
  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
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

  auto bucket = UnwrapInternalField<Bucket>(info.Holder(),
                                            InternalFields::kBucketInstance);
  auto [error, err_code, result] = bucket->Get(key);
  if (error != nullptr) {
    js_exception->ThrowEventingError(*error);
    return;
  }
  if (*err_code != LCB_SUCCESS) {
    HandleBucketOpFailure(isolate, bucket->GetConnection(), *err_code);
    return;
  }
  if (result->rc == LCB_ERR_DOCUMENT_NOT_FOUND) {
    HandleEnoEnt(isolate, info, bucket->GetConnection());
    return;
  }
  if (result->rc != LCB_SUCCESS) {
    HandleBucketOpFailure(isolate, bucket->GetConnection(), result->rc);
    return;
  }

  v8::Local<v8::Value> value_json;
  TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate, result->value)),
           &value_json);
  // TODO : Log here or throw exception if JSON parse fails
  info.GetReturnValue().Set(value_json);
}

// Performs the lcb related calls when bucket object is accessed
template <>
void BucketBinding::BucketSet<v8::Local<v8::Name>>(
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

  auto block_mutation =
      UnwrapInternalField<bool>(info.Holder(), InternalFields::kBlockMutation);
  if (*block_mutation) {
    ++bucket_op_exception_count;
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  // TODO : Do not cast to v8::String, just use name directly
  v8::String::Utf8Value utf8_key(isolate, name.As<v8::String>());
  std::string key(*utf8_key);
  auto value = JSONStringify(isolate, value_obj);

  auto bucket = UnwrapInternalField<Bucket>(info.Holder(),
                                            InternalFields::kBucketInstance);
  auto is_source_bucket =
      UnwrapInternalField<bool>(info.Holder(), InternalFields::kIsSourceBucket);

  auto [error, err_code, result] =
      BucketSet(key, value, *is_source_bucket, bucket);
  if (error != nullptr) {
    js_exception->ThrowEventingError(*error);
    return;
  }
  if (*err_code != LCB_SUCCESS) {
    HandleBucketOpFailure(isolate, bucket->GetConnection(), *err_code);
    return;
  }
  if (result->rc != LCB_SUCCESS) {
    HandleBucketOpFailure(isolate, bucket->GetConnection(), result->rc);
    return;
  }
  info.GetReturnValue().Set(value_obj);
}

// Performs the lcb related calls when bucket object is accessed
template <>
void BucketBinding::BucketDelete<v8::Local<v8::Name>>(
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
    js_exception->ThrowEventingError("Delete from source bucket is forbidden");
    ++bucket_op_exception_count;
    return;
  }

  v8::String::Utf8Value utf8_key(isolate, name.As<v8::String>());
  std::string key(*utf8_key);

  auto is_source_bucket =
      UnwrapInternalField<bool>(info.Holder(), InternalFields::kIsSourceBucket);
  auto bucket = UnwrapInternalField<Bucket>(info.Holder(),
                                            InternalFields::kBucketInstance);
  auto [error, err_code, result] = BucketDelete(key, is_source_bucket, bucket);
  if (error != nullptr) {
    js_exception->ThrowEventingError(*error);
    return;
  }
  if (*err_code != LCB_SUCCESS) {
    HandleBucketOpFailure(isolate, bucket->GetConnection(), *err_code);
    return;
  }
  if (result->rc == LCB_ERR_DOCUMENT_NOT_FOUND) {
    HandleEnoEnt(isolate, bucket->GetConnection());
    return;
  }
  if (result->rc != LCB_SUCCESS) {
    HandleBucketOpFailure(isolate, bucket->GetConnection(), result->rc);
    return;
  }
  info.GetReturnValue().Set(true);
}

Error BucketBinding::InstallBinding(v8::Isolate *isolate,
                                    const v8::Local<v8::Context> &context) {
  v8::HandleScope handle_scope(isolate);
  auto [err_new_obj, obj] = factory_->NewBucketObj();
  if (err_new_obj != nullptr) {
    return std::move(err_new_obj);
  }

  auto err_connect = bucket_.Connect();
  if (err_connect != nullptr) {
    return err_connect;
  }

  (*obj)->SetInternalField(InternalFields::kBlockMutation,
                           v8::External::New(isolate, &block_mutation_));
  (*obj)->SetInternalField(InternalFields::kBucketInstance,
                           v8::External::New(isolate, &bucket_));
  (*obj)->SetInternalField(InternalFields::kIsSourceBucket,
                           v8::External::New(isolate, &is_source_bucket_));
  (*obj)->SetInternalField(InternalFields::kBucketBindingId,
                           v8Str(isolate, "Bucket-Binding"));

  auto global = context->Global();
  auto result = false;
  if (!TO(global->Set(context, v8Str(isolate, bucket_alias_), *obj), &result) ||
      !result) {
    return std::make_unique<std::string>(
        "Unable to install bucket binding with alias " + bucket_alias_ +
        " to global scope");
  }
  return nullptr;
}

void BucketBinding::HandleBucketOpFailure(v8::Isolate *isolate,
                                          lcb_INSTANCE *connection,
                                          lcb_STATUS error) {
  auto isolate_data = UnwrapData(isolate);
  AddLcbException(isolate_data, error);
  ++bucket_op_exception_count;

  auto js_exception = isolate_data->js_exception;
  js_exception->ThrowKVError(connection, error);
}

// Delegates to the appropriate type of handler
template <typename T>
void BucketBinding::BucketGetDelegate(
    T name, const v8::PropertyCallbackInfo<v8::Value> &info) {
  BucketGet<T>(name, info);
}

template <typename T>
void BucketBinding::BucketSetDelegate(
    T key, v8::Local<v8::Value> value,
    const v8::PropertyCallbackInfo<v8::Value> &info) {
  BucketSet<T>(key, value, info);
}

template <typename T>
void BucketBinding::BucketDeleteDelegate(
    T key, const v8::PropertyCallbackInfo<v8::Boolean> &info) {
  BucketDelete<T>(key, info);
}

// Specialized templates to forward the delegate to the overload doing the
// actual work
template <>
void BucketBinding::BucketGet<uint32_t>(
    uint32_t key, const v8::PropertyCallbackInfo<v8::Value> &info) {
  BucketGet<v8::Local<v8::Name>>(v8Name(info.GetIsolate(), key), info);
}

template <>
void BucketBinding::BucketSet<uint32_t>(
    uint32_t key, const v8::Local<v8::Value> &value,
    const v8::PropertyCallbackInfo<v8::Value> &info) {
  BucketSet<v8::Local<v8::Name>>(v8Name(info.GetIsolate(), key), value, info);
}

template <>
void BucketBinding::BucketDelete<uint32_t>(
    uint32_t key, const v8::PropertyCallbackInfo<v8::Boolean> &info) {
  BucketDelete<v8::Local<v8::Name>>(v8Name(info.GetIsolate(), key), info);
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
BucketBinding::BucketSet(const std::string &key, const std::string &value,
                         bool is_source_bucket, Bucket *bucket) {
  const char *data = value.c_str();
  if (is_source_bucket) {
    return bucket->SetWithXattr(key, data, strlen(data));
  }
  return bucket->SetWithoutXattr(key, data, strlen(data));
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
BucketBinding::BucketDelete(const std::string &key, bool is_source_bucket,
                            Bucket *bucket) {
  if (is_source_bucket) {
    return bucket->DeleteWithXattr(key);
  }
  return bucket->DeleteWithoutXattr(key);
}

void BucketBinding::HandleEnoEnt(
    v8::Isolate *isolate, const v8::PropertyCallbackInfo<v8::Value> &info,
    lcb_INSTANCE *instance) {
  const auto version = UnwrapData(isolate)->lang_compat->version;
  if (version < LanguageCompatibility::Version::k6_5_0) {
    HandleBucketOpFailure(isolate, instance, LCB_ERR_DOCUMENT_NOT_FOUND);
    return;
  }
  info.GetReturnValue().Set(v8::Undefined(isolate));
}

void BucketBinding::HandleEnoEnt(v8::Isolate *isolate, lcb_INSTANCE *instance) {
  const auto version = UnwrapData(isolate)->lang_compat->version;
  if (version < LanguageCompatibility::Version::k6_5_0) {
    HandleBucketOpFailure(isolate, instance, LCB_ERR_DOCUMENT_NOT_FOUND);
  }
}

Info BucketBinding::ValidateKey(const v8::Local<v8::Name> &arg) {
  auto info = Utils::ValidateDataType(arg);
  if (info.is_fatal) {
    return {true, "Invalid data type for key - " + info.msg};
  }
  return {false};
}

Info BucketBinding::ValidateValue(const v8::Local<v8::Value> &arg) {
  auto info = Utils::ValidateDataType(arg);
  if (info.is_fatal) {
    return {true, "Invalid data type for value - " + info.msg};
  }
  return {false};
}

Info BucketBinding::ValidateKeyValue(const v8::Local<v8::Name> &key,
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

bool BucketBinding::IsBucketObject(v8::Isolate *isolate,
                                   const v8::Local<v8::Object> obj) {
  if (obj->InternalFieldCount() != BucketBinding::kInternalFieldsCount) {
    return false;
  }

  v8::HandleScope handle_scope(isolate);

  auto binding_id = obj->GetInternalField(InternalFields::kBucketBindingId);
  if (binding_id.IsEmpty() || binding_id->IsNullOrUndefined() ||
      !binding_id->IsString()) {
    return false;
  }

  v8::String::Utf8Value binding_id_utf8(isolate, binding_id);
  return !strcmp(*binding_id_utf8, "Bucket-Binding");
}

Bucket *BucketBinding::GetBucket(v8::Isolate *isolate,
                                 const v8::Local<v8::Value> obj) {
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();
  v8::Local<v8::Object> local_obj;
  TO_LOCAL(obj->ToObject(context), &local_obj);
  return UnwrapInternalField<Bucket>(local_obj,
                                     InternalFields::kBucketInstance);
}

bool BucketBinding::GetBlockMutation(v8::Isolate *isolate,
                                     const v8::Local<v8::Value> obj) {
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();
  v8::Local<v8::Object> local_obj;
  TO_LOCAL(obj->ToObject(context), &local_obj);
  return *UnwrapInternalField<bool>(local_obj, InternalFields::kBlockMutation);
}

bool BucketBinding::IsSourceBucket(v8::Isolate *isolate,
                                   const v8::Local<v8::Value> obj) {
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();
  v8::Local<v8::Object> local_obj;
  TO_LOCAL(obj->ToObject(context), &local_obj);
  return *UnwrapInternalField<bool>(local_obj, InternalFields::kIsSourceBucket);
}
