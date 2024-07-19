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

#include <libcouchbase/couchbase.h>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <v8.h>

#include "bucket_cache.h"
#include "error.h"
#include "info.h"
#include "isolate_data.h"
#include "lcb_utils.h"

struct MetaData {
  std::string scope;
  std::string collection;
  std::string key;
  uint64_t cas;
  uint32_t expiry;

  bool invalidate_cache_;

  MetaData()
      : scope(""), collection(""), key(""), cas(0), expiry(0),
        invalidate_cache_(false) {}
  MetaData(bool invalidate)
      : scope(""), collection(""), key(""), cas(0), expiry(0),
        invalidate_cache_(invalidate) {}
  MetaData(const std::string& scope, const std::string& collection,
    const std::string& key, const uint64_t& cas)
      : scope(scope), collection(collection), key(key), cas(cas),
        expiry(0), invalidate_cache_(false) {}
};

struct MutateInSpecs {
  enum spec_type {
    oBaseOp,
    oInsert,
    oUpsert,
    oReplace,
    oRemove,
    oArrayAppend,
    oArrayPrepend,
    oArrayInsert,
    oArrayAddUnique,
    oInvalidOp
  };

  struct spec {
    spec_type specType_;
    std::string key_;
    std::string value_;
    uint32_t flags = 0;

    spec(spec_type specType, std::string key, std::string value, bool create_path, bool is_user_xattr)
        : specType_(specType), key_(key), value_(value) {
      if (create_path && allow_create_path(specType)) {
        flags |= LCB_SUBDOCSPECS_F_MKINTERMEDIATES;
      }
      if (is_user_xattr) {
        flags |= LCB_SUBDOCSPECS_F_XATTRPATH;
      }
    }

    inline bool allow_create_path(spec_type specType) {
      return specType != oReplace && specType != oRemove && specType != oArrayInsert;
    }
  };

  std::vector<spec> specs;

  int get_num_fields() const { return specs.size(); }

  bool emplace_spec(int spec, std::string key, std::string value,
                         bool create_path, bool is_user_xattr) {
    if (spec <= oBaseOp || spec >= oInvalidOp) {
      return false;
    }
    specs.emplace_back(spec_type(spec), key, value, create_path, is_user_xattr);
    return true;
  }

  void populate_lcb_specs(lcb_SUBDOCSPECS *lcb_specs, int start_idx) {
    auto index = start_idx;
    for (std::vector<spec>::const_iterator it = specs.begin();
         it != specs.end(); it++) {
      std::string key = it->key_;
      std::string value = it->value_;

      switch (it->specType_) {
      case oInsert: {
        lcb_subdocspecs_dict_add(lcb_specs, index, it->flags, key.c_str(),
                                 key.size(), value.c_str(), value.size());
      } break;

      case oUpsert: {
        lcb_subdocspecs_dict_upsert(lcb_specs, index, it->flags, key.c_str(),
                                    key.size(), value.c_str(), value.size());
      } break;

      case oReplace: {
        lcb_subdocspecs_replace(lcb_specs, index, it->flags, key.c_str(), key.size(),
                                value.c_str(), value.size());
      } break;

      case oRemove: {
        lcb_subdocspecs_remove(lcb_specs, index, it->flags, key.c_str(), key.size());
      } break;

      case oArrayAppend: {
        lcb_subdocspecs_array_add_last(lcb_specs, index, it->flags, key.c_str(),
                                       key.size(), value.c_str(), value.size());
      } break;

      case oArrayPrepend: {
        lcb_subdocspecs_array_add_first(lcb_specs, index, it->flags, key.c_str(),
                                        key.size(), value.c_str(),
                                        value.size());
      } break;

      case oArrayInsert: {
        lcb_subdocspecs_array_insert(lcb_specs, index, it->flags, key.c_str(), key.size(),
                                     value.c_str(), value.size());
      } break;

      case oArrayAddUnique: {
        lcb_subdocspecs_array_add_unique(lcb_specs, index, it->flags, key.c_str(),
                                         key.size(), value.c_str(),
                                         value.size());
      } break;

      default:
        break;
      }
      index++;
    }
  }
};

struct LookupInSpecs {
  enum class spec_type {
    oBaseOp,
    oGet,
    oInvalidOp
  };

  struct spec {
    spec_type specType_;
    std::string key_;
    uint32_t flags = 0;

    spec(spec_type specType, std::string key, bool is_user_xattr)
        : specType_(specType), key_(key) {
      if (is_user_xattr) {
        flags |= LCB_SUBDOCSPECS_F_XATTRPATH;
      }
    }
  };

  std::vector<spec> specs;

  int get_num_fields() const { return specs.size(); }

  bool emplace_spec(int spec_opcode, std::string key, bool is_user_xattr) {
    auto spec = spec_type(spec_opcode);
    if (spec <= spec_type::oBaseOp || spec >= spec_type::oInvalidOp) {
      return false;
    }
    specs.emplace_back(spec, key, is_user_xattr);
    return true;
  }

  void populate_lcb_specs(lcb_SUBDOCSPECS *lcb_specs, int start_idx) {
    auto index = start_idx;
    for (auto it = specs.cbegin(); it != specs.cend(); it++) {
      std::string key = it->key_;

      switch (it->specType_) {
      case spec_type::oGet:
        lcb_subdocspecs_get(lcb_specs, index, it->flags, key.c_str(), key.size());
        break;

      default:
        break;
      }
      index++;
    }
  }
};

class BucketFactory {
public:
  BucketFactory(v8::Isolate *isolate, const v8::Local<v8::Context> &context);
  ~BucketFactory();

  BucketFactory(const BucketFactory &) = delete;
  BucketFactory(BucketFactory &&) = delete;
  BucketFactory &operator=(const BucketFactory &) = delete;
  BucketFactory &operator=(BucketFactory &&) = delete;

  std::pair<Error, std::unique_ptr<v8::Local<v8::Object>>> NewBucketObj() const;

private:
  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;
  v8::Persistent<v8::ObjectTemplate> bucket_template_;
};

class Bucket {
public:
  Bucket(v8::Isolate *isolate, std::string bucket_name, std::string scope_name,
         std::string collection_name, const std::string &user,
         const std::string &domain)
      : isolate_(isolate), bucket_name_(std::move(bucket_name)),
        scope_name_(std::move(scope_name)),
        collection_name_(std::move(collection_name)) {

    on_behalf_of_privilege_ = "SystemXattrWrite";

    if (domain == "external") {
      on_behalf_of_ = "^" + user;
    } else {
      on_behalf_of_ = std::move(user);
    }
  }
  ~Bucket();

  Bucket(const Bucket &) = default;
  Bucket(Bucket &&) = delete;
  Bucket &operator=(const Bucket &) = delete;
  Bucket &operator=(Bucket &&) = delete;

  std::tuple<Error, std::unique_ptr<lcb_STATUS>> Connect();

  void SetupCacheInvalidateFunc() {
    auto context = isolate_->GetCurrentContext();
    auto global = context->Global();
    std::string objString = "couchbase";
    std::string fnString = "invalidateKey";

    v8::Local<v8::Object> object;
    v8::Local<v8::Value> obj_val =
        global->Get(context, v8Str(isolate_, objString)).ToLocalChecked();
    if (!TO_LOCAL(obj_val->ToObject(context), &object)) {
      return;
    }

    v8::Local<v8::Value> invalidate_func;
    if (!TO_LOCAL(object->Get(context, v8Str(isolate_, fnString)),
                  &invalidate_func)) {
      return;
    }

    auto func = invalidate_func.As<v8::Function>();
    invalidate_cache_func_.Reset(isolate_, func);
  }

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  WriteCheckpoint(const MetaData &meta, const uint64_t& rootcas, const std::vector<std::string>& cleanup_cursors);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  Get(MetaData &meta);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  MutateInWithoutXattr(MetaData &meta, MutateInSpecs &specs);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  MutateInWithXattr(MetaData &meta, MutateInSpecs &specs);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  SetWithXattr(MetaData &meta, const std::string &value,
               lcb_SUBDOC_STORE_SEMANTICS op_type = LCB_SUBDOC_STORE_UPSERT);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  SetWithoutXattr(MetaData &meta, const std::string &value,
                  lcb_STORE_OPERATION op_type = LCB_STORE_UPSERT,
                  lcb_U32 doc_type = 0x2000000);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  TouchWithXattr(MetaData &meta);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  TouchWithoutXattr(MetaData &meta);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  DeleteWithXattr(MetaData &meta);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  DeleteWithoutXattr(MetaData &meta);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  LookupIn(MetaData &meta, LookupInSpecs &specs);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  GetWithMeta(MetaData &meta);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  CounterWithXattr(MetaData &meta, int64_t delta);

  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  CounterWithoutXattr(MetaData &meta, int64_t delta);

  lcb_INSTANCE *GetConnection() const { return connection_; }

  const std::string &BucketName() const { return bucket_name_; };
  const std::string &ScopeName() const { return scope_name_; };
  const std::string &CollectionName() const { return collection_name_; };

  std::tuple<Error, std::string, std::string>
  get_scope_and_collection_names(const MetaData &meta);

private:
  Error FormatErrorAndDestroyConn(const std::string &message,
                                  const lcb_STATUS &error) const;

  bool MaybeRecreateConnOnAuthErr(const lcb_STATUS &status,
                                  bool should_check_autherr);
  void InvalidateCache(const MetaData &meta);
  template <typename CmdType, typename Callable>
  std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  TryLcbCmdWithRefreshConnIfNecessary(CmdType &cmd, int max_retry_count,
                                      uint32_t max_retry_secs,
                                      Callable &&callable) {
    if (!is_connected_) {
      Error create_err = nullptr;
      std::unique_ptr<lcb_STATUS> lcb_status = nullptr;
      int retry_count = 0;
      const auto start_time = std::chrono::system_clock::now();
      while (true) {
        std::tie(create_err, lcb_status) = Connect();
        is_connected_ = create_err == nullptr;

        const auto isExceededRetries =
            max_retry_count && retry_count++ >= max_retry_count;
        const auto isRetryableLCBError =
            (lcb_status != nullptr && *lcb_status != LCB_SUCCESS &&
             IsRetriable(*lcb_status));
        const auto isLCBRetryNeeded =
            create_err != nullptr &&
            (lcb_status == nullptr || isRetryableLCBError);
        if (isExceededRetries || create_err == nullptr || !isLCBRetryNeeded) {
          break;
        }

        const auto elapsed_time = std::chrono::system_clock::now() - start_time;
        if (max_retry_secs &&
            elapsed_time >= std::chrono::seconds(max_retry_secs)) {
          break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      }
      if (!is_connected_) {
        return {std::move(create_err), std::move(lcb_status), nullptr};
      }
    }

    auto [internal_err, err_code, result] =
        TryLcbCmd(cmd, max_retry_count, max_retry_secs, callable);
    if (internal_err != nullptr) {
      return {std::move(internal_err), std::make_unique<lcb_STATUS>(err_code),
              std::make_unique<Result>(result)};
    }

    if (err_code != LCB_SUCCESS) {
      return {nullptr, std::make_unique<lcb_STATUS>(err_code),
              std::make_unique<Result>(result)};
    }

    if (!MaybeRecreateConnOnAuthErr(result.rc, true)) {
      return {nullptr, std::make_unique<lcb_STATUS>(err_code),
              std::make_unique<Result>(result)};
    }

    std::tie(internal_err, err_code, result) =
        TryLcbCmd(cmd, max_retry_count, max_retry_secs, callable);
    return {std::move(internal_err), std::make_unique<lcb_STATUS>(err_code),
            std::make_unique<Result>(result)};
  }

  template <typename CmdType, typename Callable>
  std::tuple<Error, lcb_STATUS, Result>
  TryLcbCmd(CmdType &cmd, int max_retry_count, uint32_t max_retry_secs,
            Callable &&callable) {
    auto [err_code, result] = RetryLcbCommand(connection_, cmd, max_retry_count,
                                              max_retry_secs, callable);
    if (err_code != LCB_SUCCESS) {
      return {nullptr, err_code, result};
    }

    if (result.kv_err_code == UNKNOWN_SCOPE) {
      return {std::make_unique<std::string>("Scope doesn't exist"), err_code,
              result};
    }

    if (result.kv_err_code == UNKNOWN_COLLECTION) {
      return {std::make_unique<std::string>("Collection doesn't exist"),
              err_code, result};
    }

    return {nullptr, err_code, result};
  }

  v8::Isolate *isolate_{nullptr};
  v8::Persistent<v8::Function> invalidate_cache_func_;
  std::string bucket_name_;
  std::string scope_name_;
  std::string collection_name_;
  lcb_INSTANCE *connection_{nullptr};
  bool is_connected_{false};
  std::string on_behalf_of_;
  std::string on_behalf_of_privilege_;
};

class BucketBinding {
  friend BucketFactory;
  friend BucketOps;

public:
  BucketBinding(v8::Isolate *isolate, std::shared_ptr<BucketFactory> factory,
                const std::string &bucket_name, const std::string &scope_name,
                const std::string &collection_name, std::string alias,
                bool block_mutation, bool is_source_bucket,
                const std::string &user, const std::string &domain)
      : block_mutation_(block_mutation), is_source_bucket_(is_source_bucket),
        bucket_name_(bucket_name), bucket_alias_(std::move(alias)),
        factory_(std::move(factory)), bucket_(isolate, bucket_name, scope_name,
                                              collection_name, user, domain) {}

  Error InstallBinding(v8::Isolate *isolate,
                       const v8::Local<v8::Context> &context);

  static bool IsBucketObject(v8::Isolate *isolate,
                             const v8::Local<v8::Object> obj);
  static Bucket *GetBucket(v8::Isolate *isolate,
                           const v8::Local<v8::Value> obj);
  static bool GetBlockMutation(v8::Isolate *isolate,
                               const v8::Local<v8::Value> obj);
  static std::tuple<Error, bool>
  IsSourceMutation(v8::Isolate *isolate, const v8::Local<v8::Value> obj,
                   const MetaData &meta);

private:
  static void HandleBucketOpFailure(v8::Isolate *isolate,
                                    lcb_INSTANCE *connection, lcb_STATUS error);
  static Info ValidateKey(const v8::Local<v8::Name> &arg);
  static Info ValidateValue(const v8::Local<v8::Value> &arg);
  static Info ValidateKeyValue(const v8::Local<v8::Name> &key,
                               const v8::Local<v8::Value> &value);
  template <typename T> static Info Validate(const v8::Local<T> &arg);

  // Delegate is used to multiplex alphanumeric and numeric accesses on bucket
  // object in JavaScript
  template <typename T>
  static void
  BucketGetDelegate(T name, const v8::PropertyCallbackInfo<v8::Value> &info);
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

  static std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  BucketSet(MetaData &metadata, const std::string &value, bool is_source_bucket,
            Bucket *bucket);

  template <typename>
  static void BucketDelete(const v8::Local<v8::Name> &key,
                           const v8::PropertyCallbackInfo<v8::Boolean> &info);
  template <typename>
  static void BucketDelete(uint32_t key,
                           const v8::PropertyCallbackInfo<v8::Boolean> &info);

  static std::tuple<Error, std::unique_ptr<lcb_STATUS>, std::unique_ptr<Result>>
  BucketDelete(MetaData &meta, bool is_source_bucket, Bucket *bucket);

  static void
  BucketDeleteWithXattr(const v8::Local<v8::Name> &key,
                        const v8::PropertyCallbackInfo<v8::Boolean> &info);

  static void
  BucketDeleteWithoutXattr(const v8::Local<v8::Name> &key,
                           const v8::PropertyCallbackInfo<v8::Boolean> &info);

  static void HandleEnoEnt(v8::Isolate *isolate,
                           const v8::PropertyCallbackInfo<v8::Value> &info,
                           lcb_INSTANCE *instance);

  static void HandleEnoEnt(v8::Isolate *isolate, lcb_INSTANCE *instance);

  bool block_mutation_;
  bool is_source_bucket_;
  std::string bucket_name_;
  std::string bucket_alias_;
  std::shared_ptr<BucketFactory> factory_;
  Bucket bucket_;

  enum InternalFields {
    kBucketInstance,
    kBlockMutation,
    kIsSourceBucket,
    kBucketBindingId,
    kInternalFieldsCount
  };
};

// TODO : Must be implemented by the component that wants to use Bucket
void AddLcbException(const IsolateData *isolate_data, lcb_STATUS error);
std::string GetFunctionInstanceID(v8::Isolate *isolate);

#endif
