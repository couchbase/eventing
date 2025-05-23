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
#include "v8worker2.h"

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

std::tuple<Error, std::unique_ptr<lcb_STATUS>> Bucket::Connect() {
  LOG(logTrace) << __func__ << " connecting to bucket " << RU(bucket_name_)
                << std::endl;
  if (is_connected_) {
    LOG(logError)
        << __func__
        << " Attempting to connect an already connected bucket instance"
        << std::endl;
    return {nullptr, nullptr};
  }

  auto utils = UnwrapData(isolate_)->utils;
  auto logger = UnwrapData(isolate_)->logger;

  auto conn_str_info = utils->GetConnectionString(bucket_name_);
  if (!conn_str_info.is_valid) {
    return {std::make_unique<std::string>(conn_str_info.msg), nullptr};
  }

  LOG(logInfo) << __func__
               << " connection string : " << RU(conn_str_info.conn_str)
               << std::endl;

  lcb_CREATEOPTS *options;
  lcb_createopts_create(&options, LCB_TYPE_BUCKET);
  lcb_createopts_connstr(options, conn_str_info.conn_str.c_str(),
                         strlen(conn_str_info.conn_str.c_str()));
  lcb_createopts_logger(options, logger->base);
  if (conn_str_info.client_key_passphrase != "") {
    lcb_createopts_tls_key_password(options,
                                    conn_str_info.client_key_passphrase.c_str(),
                                    conn_str_info.client_key_passphrase.size());
  }

  auto result = lcb_create(&connection_, options);
  if (result != LCB_SUCCESS) {
    return {FormatErrorAndDestroyConn("Unable to initialize connection handle",
                                      result),
            std::make_unique<lcb_STATUS>(result)};
  }
  lcb_createopts_destroy(options);

  auto auth = lcbauth_new();
  result = RetryWithFixedBackoff(5, 200, IsRetriable, lcbauth_set_callback,
                                 auth, isolate_, GetUsernameAndPassword);
  if (result != LCB_SUCCESS) {
    return {FormatErrorAndDestroyConn("Unable to set auth callbacks", result),
            std::make_unique<lcb_STATUS>(result)};
  }

  result = RetryWithFixedBackoff(5, 200, IsRetriable, lcbauth_set_mode, auth,
                                 LCBAUTH_MODE_DYNAMIC);
  if (result != LCB_SUCCESS) {
    return {
        FormatErrorAndDestroyConn("Unable to set auth mode to dynamic", result),
        std::make_unique<lcb_STATUS>(result)};
  }
  lcb_set_auth(connection_, auth);

  result = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_connect, connection_);
  if (result != LCB_SUCCESS) {
    return {FormatErrorAndDestroyConn("Unable to connect to bucket", result),
            std::make_unique<lcb_STATUS>(result)};
  }

  result = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_wait, connection_,
                                 LCB_WAIT_DEFAULT);
  if (result != LCB_SUCCESS) {
    return {FormatErrorAndDestroyConn("Unable to schedule call for connect",
                                      result),
            std::make_unique<lcb_STATUS>(result)};
  }

  result = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_get_bootstrap_status,
                                 connection_);
  if (result != LCB_SUCCESS) {
    return {
        FormatErrorAndDestroyConn("Unable to bootstrap LCB instance", result),
        std::make_unique<lcb_STATUS>(result)};
  }

  lcb_install_callback(connection_, LCB_CALLBACK_GET, GetCallback);
  lcb_install_callback(connection_, LCB_CALLBACK_STORE, SetCallback);
  lcb_install_callback(connection_, LCB_CALLBACK_SDMUTATE, SubDocumentCallback);
  lcb_install_callback(connection_, LCB_CALLBACK_TOUCH, TouchCallback);
  lcb_install_callback(connection_, LCB_CALLBACK_REMOVE, DeleteCallback);
  lcb_install_callback(connection_, LCB_CALLBACK_SDLOOKUP,
                       SubDocumentLookupCallback);

  lcb_U32 lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  result =
      RetryWithFixedBackoff(5, 200, IsRetriable, lcb_cntl, connection_,
                            LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT, &lcb_timeout);
  if (result != LCB_SUCCESS) {
    return {FormatErrorAndDestroyConn("Unable to set timeout for bucket ops",
                                      result),
            std::make_unique<lcb_STATUS>(result)};
  }

  unsigned int enable_detailed_err_codes = 1;
  result = RetryWithFixedBackoff(5, 200, IsRetriable, lcb_cntl, connection_,
                                 LCB_CNTL_SET, LCB_CNTL_DETAILED_ERRCODES,
                                 &enable_detailed_err_codes);
  if (result != LCB_SUCCESS) {
    return {
        FormatErrorAndDestroyConn("Unable to set detailed error codes", result),
        std::make_unique<lcb_STATUS>(result)};
  }
  LOG(logTrace) << __func__ << " connected to bucket " << RU(bucket_name_)
                << " successfully" << std::endl;
  is_connected_ = true;
  return {nullptr, nullptr};
}

Error Bucket::FormatErrorAndDestroyConn(const std::string &message,
                                        const lcb_STATUS &error) const {
  auto isolate_data = UnwrapData(isolate_);
  AddLcbException(isolate_data, error);
  std::stringstream err_msg;
  err_msg << message << ", err: " << lcb_strerror_long(error);
  lcb_destroy(connection_);
  LOG(logError) << __func__ << " " << err_msg.str() << std::endl;
  return std::make_unique<std::string>(err_msg.str());
}

// returns true if status is auth_failure or cert_verification err AND we were
// able to recreate connection false if either status is not auth failure OR
// status was failure but couldn't recreate
bool Bucket::MaybeRecreateConnOnAuthErr(const lcb_STATUS &status,
                                        bool should_check_autherr) {
  lcb_INSTANCE *tmp_instance = nullptr;
  if ((status == LCB_ERR_AUTHENTICATION_FAILURE ||
       status == LCB_ERR_SSL_CANTVERIFY || status == LCB_ERR_SSL_ERROR) &&
      should_check_autherr) {
    if (is_connected_) {
      LOG(logError) << "Got " << status << " for bucket: " << bucket_name_
                    << " Recreating lcb instance" << std::endl;
      tmp_instance = connection_;
      is_connected_ = false;
      connection_ = nullptr;
    }
    auto [create_err, _] = Connect();
    if (create_err != nullptr) {
      connection_ = tmp_instance;
      if (connection_ != nullptr)
        is_connected_ = true;
      return false;
    }
    if (tmp_instance != nullptr) {
      lcb_destroy(tmp_instance);
    }
    return true;
  }
  return false;
}

std::tuple<Error, std::string, std::string>
Bucket::get_scope_and_collection_names(const MetaData &meta) {
  if ((scope_name_ == "*") && (meta.scope == "")) {
    return {
        std::make_unique<std::string>("Inconsistent wildcard usage. scope_name "
                                      "should be provided in the meta object"),
        "", ""};
  }
  if ((collection_name_ == "*") && (meta.collection == "")) {
    return {std::make_unique<std::string>(
                "Inconsistent wildcard usage. collection_name should be "
                "provided in the meta object"),
            "", ""};
  }

  std::string scope = scope_name_, collection = collection_name_;
  if (scope == "*") {
    scope = meta.scope;
  }

  if (collection == "*") {
    collection = meta.collection;
  }

  return {nullptr, scope, collection};
}

void Bucket::InvalidateCache(const MetaData &meta) {
  if (!meta.invalidate_cache_) {
    return;
  }

  if (invalidate_cache_func_.IsEmpty()) {
    return;
  }

  std::string key;
  auto sz = bucket_name_.length() + meta.scope.length() +
            meta.collection.length() + meta.key.length() + 3;
  key.reserve(sz);
  key.append(bucket_name_);
  key.append("/");
  key.append(meta.scope);
  key.append("/");
  key.append(meta.collection);
  key.append("/");
  key.append(meta.key);

  v8::Local<v8::Value> args[1];

  args[0] = v8::String::NewFromUtf8(isolate_, key.c_str()).ToLocalChecked();
  auto func = invalidate_cache_func_.Get(isolate_);

  v8::HandleScope handle_scope(isolate_);

  auto context = isolate_->GetCurrentContext();
  auto global = context->Global();
  (void)func->Call(context, global, 1, args);
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::WriteCheckpoint(const MetaData &meta, const uint64_t &rootcas,
                        const std::vector<std::string> &cleanup_checkpoints) {

  const auto cursor_checkpoint_timeout =
      UnwrapData(isolate_)->cursor_checkpoint_timeout;
  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  // Setup subdoc specs
  lcb_SUBDOCSPECS *specs;
  auto specs_size = 2;
  if (cleanup_checkpoints.size() > 0) {
    specs_size += cleanup_checkpoints.size();
  }
  lcb_subdocspecs_create(&specs, specs_size);

  auto fiid_ = GetFunctionInstanceID(isolate_);
  fiid_.erase(std::remove(fiid_.begin(), fiid_.end(), '\"'), fiid_.end());

  // Update this function's checkpoint
  std::string checkpoint_cas_path("_checkpoints." + fiid_ + ".cas");
  std::string CAS_macro(R"("${Mutation.CAS}")");
  lcb_subdocspecs_dict_upsert(
      specs, 0,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      checkpoint_cas_path.c_str(), checkpoint_cas_path.size(),
      CAS_macro.c_str(), CAS_macro.size());

  // Note: all cas entries in xattrs are to be stamped with cas bytes swapped
  // to little endian followed by converting to hex string.
  // This is so that this string can then be directly used as a normal string
  // comparison with _mou.pcas which is also written in same format.
  std::string checkpoint_pcas_path("_checkpoints." + fiid_ + ".pcas");
  auto hex_cas = "\"" + to_hex(byte_swap_64(rootcas)) + "\"";
  lcb_subdocspecs_dict_upsert(
      specs, 1, LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTRPATH,
      checkpoint_pcas_path.c_str(), checkpoint_pcas_path.size(),
      hex_cas.c_str(), hex_cas.size());

  // Cleanup dirty checkpoints
  if (cleanup_checkpoints.size() > 0) {
    auto spec_counter = 2;
    for (const std::string &checkpoint_id : cleanup_checkpoints) {
      std::string cleanup_path("_checkpoints." + checkpoint_id);
      lcb_subdocspecs_remove(specs, spec_counter, LCB_SUBDOCSPECS_F_XATTRPATH,
                             cleanup_path.c_str(), cleanup_path.size());
      spec_counter++;
    }
  }

  // Setup command structure
  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_specs(cmd, specs);
  lcb_cmdsubdoc_cas(cmd, meta.cas);
  // lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_UPSERT);
  lcb_cmdsubdoc_collection(cmd, meta.scope.c_str(), meta.scope.size(),
                           meta.collection.c_str(), meta.collection.size());
  lcb_cmdsubdoc_key(cmd, meta.key.c_str(), meta.key.length());
  lcb_cmdsubdoc_timeout(cmd, cursor_checkpoint_timeout);

  // Run command
  auto [err, err_code, result] = TryLcbCmdWithRefreshConnIfNecessary(
      *cmd, max_retry, cursor_checkpoint_timeout, LcbSubdocSet);
  // Destroy command
  lcb_cmdsubdoc_destroy(cmd);
  lcb_subdocspecs_destroy(specs);

  // err handling
  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }
  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }
  return {nullptr, std::move(err_code), std::move(result)};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::Get(MetaData &meta) {
  auto [error, scope, collection] = get_scope_and_collection_names(meta);
  if (error != nullptr) {
    return {std::move(error), nullptr, nullptr};
  }
  meta.scope = scope;
  meta.collection = collection;

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  lcb_CMDGET *cmd;
  lcb_cmdget_create(&cmd);
  lcb_cmdget_collection(cmd, scope.c_str(), scope.size(), collection.c_str(),
                        collection.size());
  lcb_cmdget_key(cmd, meta.key.c_str(), meta.key.length());
  lcb_cmdget_timeout(cmd, lcb_timeout);

  if (!on_behalf_of_.empty()) {
    lcb_cmdget_on_behalf_of(cmd, on_behalf_of_.c_str(), on_behalf_of_.size());
  }

  auto [err, err_code, result] =
      TryLcbCmdWithRefreshConnIfNecessary(*cmd, max_retry, max_timeout, LcbGet);
  lcb_cmdget_destroy(cmd);
  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }
  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }

  InvalidateCache(meta);
  return {nullptr, std::move(err_code), std::move(result)};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::LookupIn(MetaData &meta, LookupInSpecs &specs) {
  auto [error, scope, collection] = get_scope_and_collection_names(meta);
  if (error != nullptr) {
    return {std::move(error), nullptr, nullptr};
  }
  meta.scope = scope;
  meta.collection = collection;

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  lcb_SUBDOCSPECS *lcb_specs;
  lcb_subdocspecs_create(&lcb_specs, specs.get_num_fields() + 2);

  char const *path = "$document.exptime";
  lcb_subdocspecs_get(lcb_specs, 0, LCB_SUBDOCSPECS_F_XATTRPATH, path,
                      strlen(path));

  char const *path2 = "$document.datatype";
  lcb_subdocspecs_get(lcb_specs, 1, LCB_SUBDOCSPECS_F_XATTRPATH, path2,
                      strlen(path2));

  specs.populate_lcb_specs(lcb_specs, 2);

  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_specs(cmd, lcb_specs);
  lcb_cmdsubdoc_collection(cmd, meta.scope.c_str(), meta.scope.size(),
                           meta.collection.c_str(), meta.collection.size());
  lcb_cmdsubdoc_key(cmd, meta.key.c_str(), meta.key.size());
  lcb_cmdsubdoc_timeout(cmd, lcb_timeout);

  if (!on_behalf_of_.empty()) {
    lcb_cmdsubdoc_on_behalf_of(cmd, on_behalf_of_.c_str(),
                               on_behalf_of_.size());
  }
  auto [err, err_code, result] = TryLcbCmdWithRefreshConnIfNecessary(
      *cmd, max_retry, max_timeout, LcbSubdocGet);
  lcb_cmdsubdoc_destroy(cmd);
  lcb_subdocspecs_destroy(lcb_specs);
  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }
  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }

  std::ostringstream oss;
  oss << "[";
  int index = 0;
  for (const auto &entry : result->lookupin_entries) {
    if (index++ > 0) {
      oss << ",";
    }
    if (entry.err_code == LCB_SUCCESS)
      oss << "{\"value\":" << entry.value << ",\"success\":"
          << "true}";
    else
      oss << "{\"success\":"
          << "false}";
  }
  oss << "]";
  result->value = oss.str();

  return {nullptr, std::move(err_code), std::move(result)};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::GetWithMeta(MetaData &meta) {
  auto [error, scope, collection] = get_scope_and_collection_names(meta);
  if (error != nullptr) {
    return {std::move(error), nullptr, nullptr};
  }
  meta.scope = scope;
  meta.collection = collection;

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  lcb_SUBDOCSPECS *lcb_specs;
  lcb_subdocspecs_create(&lcb_specs, 3);

  char const *path = "$document.exptime";
  lcb_subdocspecs_get(lcb_specs, 0, LCB_SUBDOCSPECS_F_XATTRPATH, path,
                      strlen(path));

  char const *path2 = "$document.datatype";
  lcb_subdocspecs_get(lcb_specs, 1, LCB_SUBDOCSPECS_F_XATTRPATH, path2,
                      strlen(path2));

  lcb_subdocspecs_get(lcb_specs, 2, 0, "", 0);

  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_specs(cmd, lcb_specs);
  lcb_cmdsubdoc_collection(cmd, scope.c_str(), scope.size(), collection.c_str(),
                           collection.size());
  lcb_cmdsubdoc_key(cmd, meta.key.c_str(), meta.key.length());
  lcb_cmdsubdoc_specs(cmd, lcb_specs);
  lcb_cmdsubdoc_timeout(cmd, lcb_timeout);

  if (!on_behalf_of_.empty()) {
    lcb_cmdsubdoc_on_behalf_of(cmd, on_behalf_of_.c_str(),
                               on_behalf_of_.size());
  }

  auto [err, err_code, result] = TryLcbCmdWithRefreshConnIfNecessary(
      *cmd, max_retry, max_timeout, LcbSubdocGet);
  lcb_cmdsubdoc_destroy(cmd);
  lcb_subdocspecs_destroy(lcb_specs);
  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }
  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }

  if (result != nullptr && !(result->lookupin_entries.empty())) {
    result->value = result->lookupin_entries[0].value;
  }

  return {nullptr, std::move(err_code), std::move(result)};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::CounterWithoutXattr(MetaData &meta, int64_t delta) {
  auto [error, scope, collection] = get_scope_and_collection_names(meta);
  if (error != nullptr) {
    return {std::move(error), nullptr, nullptr};
  }
  meta.scope = scope;
  meta.collection = collection;

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  lcb_SUBDOCSPECS *lcb_specs;
  lcb_subdocspecs_create(&lcb_specs, 1);
  lcb_subdocspecs_counter(lcb_specs, 0, 0, "count", strlen("count"), delta);

  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_UPSERT);
  lcb_cmdsubdoc_specs(cmd, lcb_specs);
  lcb_cmdsubdoc_cas(cmd, meta.cas);
  lcb_cmdsubdoc_expiry(cmd, meta.expiry);
  lcb_cmdsubdoc_collection(cmd, scope.c_str(), scope.size(), collection.c_str(),
                           collection.size());
  lcb_cmdsubdoc_key(cmd, meta.key.c_str(), meta.key.length());
  lcb_cmdsubdoc_timeout(cmd, lcb_timeout);

  if (!on_behalf_of_.empty()) {
    lcb_cmdsubdoc_on_behalf_of(cmd, on_behalf_of_.c_str(),
                               on_behalf_of_.size());
  }

  auto [err, err_code, result] = TryLcbCmdWithRefreshConnIfNecessary(
      *cmd, max_retry, max_timeout, LcbSubdocSet);
  lcb_cmdsubdoc_destroy(cmd);
  lcb_subdocspecs_destroy(lcb_specs);
  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }
  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }

  return {nullptr, std::move(err_code), std::move(result)};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::CounterWithXattr(MetaData &meta, int64_t delta) {
  auto [error, scope, collection] = get_scope_and_collection_names(meta);
  if (error != nullptr) {
    return {std::move(error), nullptr, nullptr};
  }
  meta.scope = scope;
  meta.collection = collection;

  lcb_SUBDOCSPECS *lcb_specs;
  lcb_subdocspecs_create(&lcb_specs, 5);

  auto function_instance_id = GetFunctionInstanceID(isolate_);
  std::string function_instance_id_path("_eventing.fiid");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 0,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTRPATH,
      function_instance_id_path.c_str(), function_instance_id_path.size(),
      function_instance_id.c_str(), function_instance_id.size());

  std::string dcp_seqno_path("_eventing.seqno");
  std::string dcp_seqno_macro(R"("${Mutation.seqno}")");
  lcb_subdocspecs_dict_upsert(lcb_specs, 1,
                              LCB_SUBDOCSPECS_F_MKINTERMEDIATES |
                                  LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
                              dcp_seqno_path.c_str(), dcp_seqno_path.size(),
                              dcp_seqno_macro.c_str(), dcp_seqno_macro.size());

  std::string CAS_path("_eventing.cas");
  std::string CAS_macro(R"("${Mutation.CAS}")");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 2,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      CAS_path.c_str(), CAS_path.size(), CAS_macro.c_str(), CAS_macro.size());

  std::string value_crc32_path("_eventing.crc");
  std::string value_crc32_macro(R"("${Mutation.value_crc32c}")");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 3,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      value_crc32_path.c_str(), value_crc32_path.size(),
      value_crc32_macro.c_str(), value_crc32_macro.size());
  lcb_subdocspecs_counter(lcb_specs, 4, 0, "count", strlen("count"), delta);

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_specs(cmd, lcb_specs);
  lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_UPSERT);
  lcb_cmdsubdoc_cas(cmd, meta.cas);
  lcb_cmdsubdoc_expiry(cmd, meta.expiry);
  lcb_cmdsubdoc_collection(cmd, scope.c_str(), scope.size(), collection.c_str(),
                           collection.size());

  lcb_cmdsubdoc_key(cmd, meta.key.c_str(), meta.key.length());
  lcb_cmdsubdoc_timeout(cmd, lcb_timeout);

  if (!on_behalf_of_.empty()) {
    lcb_cmdsubdoc_on_behalf_of(cmd, on_behalf_of_.c_str(),
                               on_behalf_of_.size());
    lcb_cmdsubdoc_on_behalf_of_extra_privilege(
        cmd, on_behalf_of_privilege_.c_str(), on_behalf_of_privilege_.size());
  }

  auto [err, err_code, result] = TryLcbCmdWithRefreshConnIfNecessary(
      *cmd, max_retry, max_timeout, LcbSubdocSet);
  lcb_cmdsubdoc_destroy(cmd);
  lcb_subdocspecs_destroy(lcb_specs);
  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }
  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }

  return {nullptr, std::move(err_code), std::move(result)};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::MutateInWithoutXattr(MetaData &meta, MutateInSpecs &specs) {
  auto [error, scope, collection] = get_scope_and_collection_names(meta);
  if (error != nullptr) {
    return {std::move(error), nullptr, nullptr};
  }
  meta.scope = scope;
  meta.collection = collection;

  lcb_SUBDOCSPECS *lcb_specs;
  lcb_subdocspecs_create(&lcb_specs, specs.get_num_fields());

  specs.populate_lcb_specs(lcb_specs, 0);
  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_specs(cmd, lcb_specs);
  lcb_cmdsubdoc_cas(cmd, meta.cas);
  lcb_cmdsubdoc_expiry(cmd, meta.expiry);
  lcb_cmdsubdoc_collection(cmd, scope.c_str(), scope.size(), collection.c_str(),
                           collection.size());
  lcb_cmdsubdoc_key(cmd, meta.key.data(), meta.key.size());
  lcb_cmdsubdoc_timeout(cmd, lcb_timeout);

  if (!on_behalf_of_.empty()) {
    lcb_cmdsubdoc_on_behalf_of(cmd, on_behalf_of_.c_str(),
                               on_behalf_of_.size());
    lcb_cmdsubdoc_on_behalf_of_extra_privilege(
        cmd, on_behalf_of_privilege_.c_str(), on_behalf_of_privilege_.size());
  }

  auto [err, err_code, result] = TryLcbCmdWithRefreshConnIfNecessary(
      *cmd, max_retry, max_timeout, LcbSubdocSet);
  lcb_cmdsubdoc_destroy(cmd);
  lcb_subdocspecs_destroy(lcb_specs);

  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }
  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }

  return {nullptr, std::move(err_code), std::move(result)};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::MutateInWithXattr(MetaData &meta, MutateInSpecs &specs) {
  auto [error, scope, collection] = get_scope_and_collection_names(meta);
  if (error != nullptr) {
    return {std::move(error), nullptr, nullptr};
  }
  meta.scope = scope;
  meta.collection = collection;

  lcb_SUBDOCSPECS *lcb_specs;
  lcb_subdocspecs_create(&lcb_specs, specs.get_num_fields() + 4);

  auto function_instance_id = GetFunctionInstanceID(isolate_);
  std::string function_instance_id_path("_eventing.fiid");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 0,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTRPATH,
      function_instance_id_path.c_str(), function_instance_id_path.size(),
      function_instance_id.c_str(), function_instance_id.size());

  std::string dcp_seqno_path("_eventing.seqno");
  std::string dcp_seqno_macro(R"("${Mutation.seqno}")");
  lcb_subdocspecs_dict_upsert(lcb_specs, 1,
                              LCB_SUBDOCSPECS_F_MKINTERMEDIATES |
                                  LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
                              dcp_seqno_path.c_str(), dcp_seqno_path.size(),
                              dcp_seqno_macro.c_str(), dcp_seqno_macro.size());

  std::string CAS_path("_eventing.cas");
  std::string CAS_macro(R"("${Mutation.CAS}")");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 2,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      CAS_path.c_str(), CAS_path.size(), CAS_macro.c_str(), CAS_macro.size());

  std::string value_crc32_path("_eventing.crc");
  std::string value_crc32_macro(R"("${Mutation.value_crc32c}")");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 3,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      value_crc32_path.c_str(), value_crc32_path.size(),
      value_crc32_macro.c_str(), value_crc32_macro.size());

  specs.populate_lcb_specs(lcb_specs, 4);

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_specs(cmd, lcb_specs);
  lcb_cmdsubdoc_cas(cmd, meta.cas);
  lcb_cmdsubdoc_expiry(cmd, meta.expiry);
  lcb_cmdsubdoc_collection(cmd, scope.c_str(), scope.size(), collection.c_str(),
                           collection.size());
  lcb_cmdsubdoc_key(cmd, meta.key.data(), meta.key.size());
  lcb_cmdsubdoc_timeout(cmd, lcb_timeout);

  if (!on_behalf_of_.empty()) {
    lcb_cmdsubdoc_on_behalf_of(cmd, on_behalf_of_.c_str(),
                               on_behalf_of_.size());
    lcb_cmdsubdoc_on_behalf_of_extra_privilege(
        cmd, on_behalf_of_privilege_.c_str(), on_behalf_of_privilege_.size());
  }

  auto [err, err_code, result] = TryLcbCmdWithRefreshConnIfNecessary(
      *cmd, max_retry, max_timeout, LcbSubdocSet);
  lcb_cmdsubdoc_destroy(cmd);
  lcb_subdocspecs_destroy(lcb_specs);

  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }
  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }

  return {nullptr, std::move(err_code), std::move(result)};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::SetWithXattr(MetaData &meta, const std::string &value,
                     lcb_SUBDOC_STORE_SEMANTICS op_type) {
  auto [error, scope, collection] = get_scope_and_collection_names(meta);
  if (error != nullptr) {
    return {std::move(error), nullptr, nullptr};
  }
  meta.scope = scope;
  meta.collection = collection;

  lcb_SUBDOCSPECS *lcb_specs;
  lcb_subdocspecs_create(&lcb_specs, 5);

  auto function_instance_id = GetFunctionInstanceID(isolate_);
  std::string function_instance_id_path("_eventing.fiid");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 0,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTRPATH,
      function_instance_id_path.c_str(), function_instance_id_path.size(),
      function_instance_id.c_str(), function_instance_id.size());

  std::string dcp_seqno_path("_eventing.seqno");
  std::string dcp_seqno_macro(R"("${Mutation.seqno}")");
  lcb_subdocspecs_dict_upsert(lcb_specs, 1,
                              LCB_SUBDOCSPECS_F_MKINTERMEDIATES |
                                  LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
                              dcp_seqno_path.c_str(), dcp_seqno_path.size(),
                              dcp_seqno_macro.c_str(), dcp_seqno_macro.size());

  std::string CAS_path("_eventing.cas");
  std::string CAS_macro(R"("${Mutation.CAS}")");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 2,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      CAS_path.c_str(), CAS_path.size(), CAS_macro.c_str(), CAS_macro.size());

  std::string value_crc32_path("_eventing.crc");
  std::string value_crc32_macro(R"("${Mutation.value_crc32c}")");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 3,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      value_crc32_path.c_str(), value_crc32_path.size(),
      value_crc32_macro.c_str(), value_crc32_macro.size());

  lcb_subdocspecs_replace(lcb_specs, 4, 0, "", 0, value.data(), value.size());

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_specs(cmd, lcb_specs);
  lcb_cmdsubdoc_cas(cmd, meta.cas);
  lcb_cmdsubdoc_expiry(cmd, meta.expiry);
  lcb_cmdsubdoc_collection(cmd, scope.c_str(), scope.size(), collection.c_str(),
                           collection.size());
  lcb_cmdsubdoc_key(cmd, meta.key.data(), meta.key.size());
  lcb_cmdsubdoc_store_semantics(cmd, op_type);
  lcb_cmdsubdoc_timeout(cmd, lcb_timeout);

  if (!on_behalf_of_.empty()) {
    lcb_cmdsubdoc_on_behalf_of(cmd, on_behalf_of_.c_str(),
                               on_behalf_of_.size());
    lcb_cmdsubdoc_on_behalf_of_extra_privilege(
        cmd, on_behalf_of_privilege_.c_str(), on_behalf_of_privilege_.size());
  }

  auto [err, err_code, result] = TryLcbCmdWithRefreshConnIfNecessary(
      *cmd, max_retry, max_timeout, LcbSubdocSet);
  lcb_cmdsubdoc_destroy(cmd);
  lcb_subdocspecs_destroy(lcb_specs);

  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }
  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }

  InvalidateCache(meta);
  return {nullptr, std::move(err_code), std::move(result)};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::SetWithoutXattr(MetaData &meta, const std::string &value,
                        lcb_STORE_OPERATION op_type, lcb_U32 doc_type) {
  auto [error, scope, collection] = get_scope_and_collection_names(meta);
  if (error != nullptr) {
    return {std::move(error), nullptr, nullptr};
  }

  meta.scope = scope;
  meta.collection = collection;

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  lcb_CMDSTORE *cmd;
  lcb_cmdstore_create(&cmd, op_type);
  lcb_cmdstore_expiry(cmd, meta.expiry);
  lcb_cmdstore_cas(cmd, meta.cas);

  lcb_cmdstore_collection(cmd, scope.c_str(), scope.size(), collection.c_str(),
                          collection.size());

  lcb_cmdstore_key(cmd, meta.key.data(), meta.key.size());
  lcb_cmdstore_value(cmd, value.data(), value.size());
  lcb_cmdstore_timeout(cmd, lcb_timeout);

  if (!on_behalf_of_.empty()) {
    lcb_cmdstore_on_behalf_of(cmd, on_behalf_of_.c_str(), on_behalf_of_.size());
  }

  auto [err, err_code, result] =
      TryLcbCmdWithRefreshConnIfNecessary(*cmd, max_retry, max_timeout, LcbSet);
  lcb_cmdstore_destroy(cmd);
  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }

  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }

  InvalidateCache(meta);
  return {nullptr, std::move(err_code), std::move(result)};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::DeleteWithXattr(MetaData &meta) {
  auto [error, scope, collection] = get_scope_and_collection_names(meta);
  if (error != nullptr) {
    return {std::move(error), nullptr, nullptr};
  }
  meta.scope = scope;
  meta.collection = collection;

  lcb_SUBDOCSPECS *lcb_specs;
  lcb_subdocspecs_create(&lcb_specs, 5);

  auto function_instance_id = GetFunctionInstanceID(isolate_);
  std::string function_instance_id_path("_eventing.fiid");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 0,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTRPATH,
      function_instance_id_path.c_str(), function_instance_id_path.size(),
      function_instance_id.c_str(), function_instance_id.size());

  std::string dcp_seqno_path("_eventing.seqno");
  std::string dcp_seqno_macro(R"("${Mutation.seqno}")");
  lcb_subdocspecs_dict_upsert(lcb_specs, 1,
                              LCB_SUBDOCSPECS_F_MKINTERMEDIATES |
                                  LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
                              dcp_seqno_path.c_str(), dcp_seqno_path.size(),
                              dcp_seqno_macro.c_str(), dcp_seqno_macro.size());

  std::string CAS_path("_eventing.cas");
  std::string CAS_macro(R"("${Mutation.CAS}")");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 2,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      CAS_path.c_str(), CAS_path.size(), CAS_macro.c_str(), CAS_macro.size());

  std::string value_crc32_path("_eventing.crc");
  std::string value_crc32_macro(R"("${Mutation.value_crc32c}")");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 3,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      value_crc32_path.c_str(), value_crc32_path.size(),
      value_crc32_macro.c_str(), value_crc32_macro.size());

  lcb_subdocspecs_remove(lcb_specs, 4, 0, "", 0);

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_specs(cmd, lcb_specs);
  lcb_cmdsubdoc_cas(cmd, meta.cas);
  lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_REPLACE);

  lcb_cmdsubdoc_collection(cmd, scope.c_str(), scope.size(), collection.c_str(),
                           collection.size());
  lcb_cmdsubdoc_key(cmd, meta.key.c_str(), meta.key.length());
  lcb_cmdsubdoc_timeout(cmd, lcb_timeout);

  if (!on_behalf_of_.empty()) {
    lcb_cmdsubdoc_on_behalf_of(cmd, on_behalf_of_.c_str(),
                               on_behalf_of_.size());
    lcb_cmdsubdoc_on_behalf_of_extra_privilege(
        cmd, on_behalf_of_privilege_.c_str(), on_behalf_of_privilege_.size());
  }

  auto [err, err_code, result] = TryLcbCmdWithRefreshConnIfNecessary(
      *cmd, max_retry, max_timeout, LcbSubdocDelete);
  lcb_cmdsubdoc_destroy(cmd);
  lcb_subdocspecs_destroy(lcb_specs);

  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }
  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }

  InvalidateCache(meta);
  return {nullptr, std::move(err_code), std::move(result)};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::DeleteWithoutXattr(MetaData &meta) {
  auto [error, scope, collection] = get_scope_and_collection_names(meta);
  if (error != nullptr) {
    return {std::move(error), nullptr, nullptr};
  }
  meta.scope = scope;
  meta.collection = collection;

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  lcb_CMDREMOVE *cmd;
  lcb_cmdremove_create(&cmd);
  lcb_cmdremove_cas(cmd, meta.cas);
  lcb_cmdremove_collection(cmd, scope.c_str(), scope.size(), collection.c_str(),
                           collection.size());

  lcb_cmdremove_key(cmd, meta.key.c_str(), meta.key.length());
  lcb_cmdremove_timeout(cmd, lcb_timeout);

  if (!on_behalf_of_.empty()) {
    lcb_cmdremove_on_behalf_of(cmd, on_behalf_of_.c_str(),
                               on_behalf_of_.size());
  }

  auto [err, err_code, result] = TryLcbCmdWithRefreshConnIfNecessary(
      *cmd, max_retry, max_timeout, LcbDelete);
  lcb_cmdremove_destroy(cmd);
  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }
  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }

  InvalidateCache(meta);
  return {nullptr, std::move(err_code), std::move(result)};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::TouchWithXattr(MetaData &meta) {
  auto [error, scope, collection] = get_scope_and_collection_names(meta);
  if (error != nullptr) {
    return {std::move(error), nullptr, nullptr};
  }
  meta.scope = scope;
  meta.collection = collection;

  lcb_SUBDOCSPECS *lcb_specs;
  lcb_subdocspecs_create(&lcb_specs, 4);

  auto function_instance_id = GetFunctionInstanceID(isolate_);
  std::string function_instance_id_path("_eventing.fiid");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 0,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTRPATH,
      function_instance_id_path.c_str(), function_instance_id_path.size(),
      function_instance_id.c_str(), function_instance_id.size());

  std::string dcp_seqno_path("_eventing.seqno");
  std::string dcp_seqno_macro(R"("${Mutation.seqno}")");
  lcb_subdocspecs_dict_upsert(lcb_specs, 1,
                              LCB_SUBDOCSPECS_F_MKINTERMEDIATES |
                                  LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
                              dcp_seqno_path.c_str(), dcp_seqno_path.size(),
                              dcp_seqno_macro.c_str(), dcp_seqno_macro.size());

  std::string CAS_path("_eventing.cas");
  std::string CAS_macro(R"("${Mutation.CAS}")");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 2,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      CAS_path.c_str(), CAS_path.size(), CAS_macro.c_str(), CAS_macro.size());

  std::string value_crc32_path("_eventing.crc");
  std::string value_crc32_macro(R"("${Mutation.value_crc32c}")");
  lcb_subdocspecs_dict_upsert(
      lcb_specs, 3,
      LCB_SUBDOCSPECS_F_MKINTERMEDIATES | LCB_SUBDOCSPECS_F_XATTR_MACROVALUES,
      value_crc32_path.c_str(), value_crc32_path.size(),
      value_crc32_macro.c_str(), value_crc32_macro.size());

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  lcb_CMDSUBDOC *cmd;
  lcb_cmdsubdoc_create(&cmd);
  lcb_cmdsubdoc_specs(cmd, lcb_specs);
  lcb_cmdsubdoc_collection(cmd, scope.c_str(), scope.size(), collection.c_str(),
                           collection.size());
  lcb_cmdsubdoc_expiry(cmd, meta.expiry);
  lcb_cmdsubdoc_key(cmd, meta.key.c_str(), meta.key.length());
  lcb_cmdsubdoc_timeout(cmd, lcb_timeout);

  if (!on_behalf_of_.empty()) {
    lcb_cmdsubdoc_on_behalf_of(cmd, on_behalf_of_.c_str(),
                               on_behalf_of_.size());
    lcb_cmdsubdoc_on_behalf_of_extra_privilege(
        cmd, on_behalf_of_privilege_.c_str(), on_behalf_of_privilege_.size());
  }

  auto [err, err_code, result] = TryLcbCmdWithRefreshConnIfNecessary(
      *cmd, max_retry, max_timeout, LcbSubdocSet);
  lcb_cmdsubdoc_destroy(cmd);
  lcb_subdocspecs_destroy(lcb_specs);

  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }
  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }

  return {nullptr, std::move(err_code), std::move(result)};
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
Bucket::TouchWithoutXattr(MetaData &meta) {
  auto [error, scope, collection] = get_scope_and_collection_names(meta);
  if (error != nullptr) {
    return {std::move(error), nullptr, nullptr};
  }

  meta.scope = scope;
  meta.collection = collection;

  const auto max_retry = UnwrapData(isolate_)->lcb_retry_count;
  const auto lcb_timeout = UnwrapData(isolate_)->lcb_timeout;
  const auto max_timeout = UnwrapData(isolate_)->op_timeout;
  const auto v8worker = UnwrapData(isolate_)->v8worker2;

  lcb_CMDTOUCH *cmd;
  lcb_cmdtouch_create(&cmd);
  lcb_cmdtouch_expiry(cmd, meta.expiry);

  lcb_cmdtouch_collection(cmd, scope.c_str(), scope.size(), collection.c_str(),
                          collection.size());

  lcb_cmdtouch_key(cmd, meta.key.data(), meta.key.size());
  lcb_cmdtouch_timeout(cmd, lcb_timeout);

  if (!on_behalf_of_.empty()) {
    lcb_cmdtouch_on_behalf_of(cmd, on_behalf_of_.c_str(), on_behalf_of_.size());
  }

  auto [err, err_code, result] = TryLcbCmdWithRefreshConnIfNecessary(
      *cmd, max_retry, max_timeout, LcbTouch);
  lcb_cmdtouch_destroy(cmd);
  if (err != nullptr) {
    return {std::move(err), nullptr, nullptr};
  }

  if (err_code != nullptr && *err_code != LCB_SUCCESS) {
    v8worker->stats_->IncrementExecutionStat("lcb_retry_failure");
    return {nullptr, std::move(err_code), nullptr};
  }

  return {nullptr, std::move(err_code), std::move(result)};
}

// Performs the lcb related calls when bucket object is accessed
template <>
void BucketBinding::BucketGet<v8::Local<v8::Name>>(
    const v8::Local<v8::Name> &name,
    const v8::PropertyCallbackInfo<v8::Value> &info) {
  auto isolate = info.GetIsolate();
  auto isolate_data = UnwrapData(isolate);
  auto js_exception = isolate_data->js_exception;
  auto utils = isolate_data->utils;
  const auto v8worker = UnwrapData(isolate)->v8worker2;

  std::lock_guard<std::mutex> guard(isolate_data->termination_lock_);
  if (!isolate_data->is_executing_) {
    return;
  }

  MetaData metadata(true);
  auto validate_info = ValidateKey(name);
  if (validate_info.is_fatal) {
    js_exception->ThrowEventingError(validate_info.msg);
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    return;
  }

  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  v8::String::Utf8Value utf8_key(isolate, name.As<v8::String>());
  std::string key(*utf8_key);

  auto bucket = UnwrapInternalField<Bucket>(info.Holder(),
                                            InternalFields::kBucketInstance);
  metadata.key = key;
  auto [err, scope, collection] =
      bucket->get_scope_and_collection_names(metadata);
  if (err != nullptr) {
    js_exception->ThrowEventingError(
        "Map accessor is not allowed for wild card keyspace binding. Please "
        "use advance keyspace accessor");
    return;
  }

  auto [error, err_code, result] = bucket->Get(metadata);
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

  v8::Local<v8::Value> doc;
  // doc is json type
  if (result->datatype & JSON_DOC) {
    if (!(TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate, result->value)),
                   &doc))) {
      js_exception->ThrowEventingError("Unable to parse response body as JSON");
      return;
    }
  } else {
    doc = utils->ToArrayBuffer(static_cast<void *>(result->value.data()),
                               result->value.length());
  }

  info.GetReturnValue().Set(doc);
}

// Performs the lcb related calls when bucket object is accessed
template <>
void BucketBinding::BucketSet<v8::Local<v8::Name>>(
    const v8::Local<v8::Name> &name, const v8::Local<v8::Value> &value_obj,
    const v8::PropertyCallbackInfo<v8::Value> &info) {
  auto isolate = info.GetIsolate();
  auto js_exception = UnwrapData(isolate)->js_exception;
  std::string value_str;
  const auto v8worker = UnwrapData(isolate)->v8worker2;

  v8::Local<v8::ArrayBuffer> array_buf;
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  MetaData metadata(true);
  auto validate_info = ValidateKeyValue(name, value_obj);
  if (validate_info.is_fatal) {
    js_exception->ThrowEventingError(validate_info.msg);
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    return;
  }

  auto block_mutation =
      UnwrapInternalField<bool>(info.Holder(), InternalFields::kBlockMutation);
  if (*block_mutation) {
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    js_exception->ThrowEventingError("Writing to source bucket is forbidden");
    return;
  }

  // TODO : Do not cast to v8::String, just use name directly
  v8::String::Utf8Value utf8_key(isolate, name.As<v8::String>());
  std::string key(*utf8_key);
  if (value_obj->IsArrayBuffer()) {
    array_buf = value_obj.As<v8::ArrayBuffer>();
    auto store = array_buf->GetBackingStore();
    value_str.assign(static_cast<const char *>(store->Data()),
                     store->ByteLength());
  } else {
    value_str = JSONStringify(isolate, value_obj);
  }

  auto bucket = UnwrapInternalField<Bucket>(info.Holder(),
                                            InternalFields::kBucketInstance);
  auto [err, is_source_mutation] =
      IsSourceMutation(isolate, info.Holder(), metadata);
  if (err != nullptr) {
    js_exception->ThrowEventingError(
        "Map accessor is not allowed for wild card keyspace binding. Please "
        "use advance keyspace accessor");
    return;
  }

  metadata.key = key;
  auto [error, err_code, result] =
      BucketSet(metadata, value_str, is_source_mutation, bucket);
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
  const auto v8worker = UnwrapData(isolate)->v8worker2;

  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  MetaData metadata(true);
  auto validate_info = ValidateKey(name);
  if (validate_info.is_fatal) {
    js_exception->ThrowKVError(validate_info.msg);
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    return;
  }

  auto block_mutation = UnwrapInternalField<bool>(
      info.Holder(), static_cast<int>(InternalFields::kBlockMutation));
  if (*block_mutation) {
    js_exception->ThrowEventingError("Delete from source bucket is forbidden");
    v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");
    return;
  }

  v8::String::Utf8Value utf8_key(isolate, name.As<v8::String>());
  std::string key(*utf8_key);

  auto [err, is_source_mutation] =
      IsSourceMutation(isolate, info.Holder(), metadata);
  if (err != nullptr) {
    js_exception->ThrowEventingError(
        "Map accessor is not allowed for wild card keyspace binding. Please "
        "use advance keyspace accessor");
    return;
  }

  auto bucket = UnwrapInternalField<Bucket>(info.Holder(),
                                            InternalFields::kBucketInstance);
  metadata.key = key;
  auto [error, err_code, result] =
      BucketDelete(metadata, is_source_mutation, bucket);
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

  auto [err_connect, _] = bucket_.Connect();
  if (err_connect != nullptr) {
    LOG(logWarning) << __func__ << " Unable to initialise bucket binding: "
                    << RU(bucket_alias_) << " alias of keyspace: "
                    << RU(bucket_.BucketName() + "." + bucket_.ScopeName() +
                          "." + bucket_.CollectionName())
                    << ", during installation because: " << *err_connect
                    << std::endl;
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
  bucket_.SetupCacheInvalidateFunc();
  return nullptr;
}

void BucketBinding::HandleBucketOpFailure(v8::Isolate *isolate,
                                          lcb_INSTANCE *connection,
                                          lcb_STATUS error) {
  auto isolate_data = UnwrapData(isolate);
  const auto v8worker = isolate_data->v8worker2;
  AddLcbException(isolate_data, error);
  v8worker->stats_->IncrementFailureStat("bucket_op_exception_count");

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
BucketBinding::BucketSet(MetaData &meta, const std::string &value,
                         bool is_source_mutation, Bucket *bucket) {
  if (is_source_mutation) {
    return bucket->SetWithXattr(meta, value);
  }
  return bucket->SetWithoutXattr(meta, value);
}

std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
BucketBinding::BucketDelete(MetaData &meta, bool is_source_mutation,
                            Bucket *bucket) {
  if (is_source_mutation) {
    return bucket->DeleteWithXattr(meta);
  }
  return bucket->DeleteWithoutXattr(meta);
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

  auto binding_id =
      obj->GetInternalField(InternalFields::kBucketBindingId).As<v8::Value>();
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

std::tuple<Error, bool>
BucketBinding::IsSourceMutation(v8::Isolate *isolate,
                                const v8::Local<v8::Value> obj,
                                const MetaData &meta) {
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();
  v8::Local<v8::Object> local_obj;
  TO_LOCAL(obj->ToObject(context), &local_obj);

  const auto bucket =
      UnwrapInternalField<Bucket>(local_obj, InternalFields::kBucketInstance);

  auto [err, scope, collection] = bucket->get_scope_and_collection_names(meta);
  if (err != nullptr) {
    return {std::move(err), false};
  }
  auto isSourceBucket =
      *UnwrapInternalField<bool>(local_obj, InternalFields::kIsSourceBucket);
  if (!isSourceBucket) {
    return {nullptr, false};
  }

  const auto v8Worker = UnwrapData(isolate)->v8worker2;
  const auto cb_scope = v8Worker->cb_source_scope_;
  const auto cb_collection = v8Worker->cb_source_collection_;

  auto source_mutation =
      ((cb_scope == "*") || (cb_scope == scope)) &&
      ((cb_collection == "*") || (cb_collection == collection));
  return {nullptr, source_mutation};
}
