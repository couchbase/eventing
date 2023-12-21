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
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>

#include "bucket.h"
#include "bucket_cache.h"
#include "bucket_ops.h"
#include "curl.h"
#include "exceptioninsight.h"
#include "insight.h"
#include "lang_compat.h"
#include "lcb_utils.h"
#include "query-helper.h"
#include "query-iterable.h"
#include "query-mgr.h"
#include "retry_util.h"
#include "timer.h"
#include "utils.h"
#include "v8worker.h"

bool V8Worker::debugger_started_ = false;

std::atomic<int64_t> timeout_count = {0};
std::atomic<int16_t> checkpoint_failure_count = {0};

std::atomic<int64_t> on_update_success = {0};
std::atomic<int64_t> on_update_failure = {0};
std::atomic<int64_t> on_delete_success = {0};
std::atomic<int64_t> on_delete_failure = {0};
std::atomic<int64_t> no_op_counter = {0};
std::atomic<int64_t> timer_callback_success = {0};
std::atomic<int64_t> timer_callback_failure = {0};
std::atomic<int64_t> timer_create_failure = {0};

std::atomic<int64_t> messages_processed_counter = {0};
std::atomic<int64_t> processed_events_size = {0};
std::atomic<int64_t> num_processed_events = {0};

std::atomic<int64_t> dcp_delete_msg_counter = {0};
std::atomic<int64_t> dcp_mutation_msg_counter = {0};
std::atomic<int64_t> dcp_delete_parse_failure = {0};
std::atomic<int64_t> dcp_mutation_parse_failure = {0};
std::atomic<int64_t> filtered_dcp_delete_counter = {0};
std::atomic<int64_t> filtered_dcp_mutation_counter = {0};
std::atomic<int64_t> dcp_mutation_checkpoint_cas_mismatch = {0};
std::atomic<int64_t> dcp_delete_checkpoint_cas_mismatch = {0};
std::atomic<int64_t> dcp_mutation_checkpoint_failure = {0};
std::atomic<int64_t> dcp_delete_checkpoint_failure = {0};
std::atomic<int64_t> timer_msg_counter = {0};
std::atomic<int64_t> timer_create_counter = {0};
std::atomic<int64_t> timer_cancel_counter = {0};

std::atomic<int64_t> enqueued_dcp_delete_msg_counter = {0};
std::atomic<int64_t> enqueued_dcp_mutation_msg_counter = {0};
std::atomic<int64_t> enqueued_timer_msg_counter = {0};

std::atomic<int64_t> timer_callback_missing_counter = {0};

std::atomic<OnDeployState> on_deploy_stat = {OnDeployState::PENDING};

void V8Worker::SetCouchbaseNamespace() {
  v8::EscapableHandleScope handle_scope(isolate_);

  v8::Local<v8::FunctionTemplate> function_template =
      v8::FunctionTemplate::New(isolate_);
  function_template->SetClassName(
      v8::String::NewFromUtf8(isolate_, "couchbase").ToLocalChecked());

  v8::Local<v8::ObjectTemplate> proto_t =
      function_template->PrototypeTemplate();

  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "getInternal").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, BucketOps::GetOp));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "insertInternal").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, BucketOps::InsertOp));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "upsertInternal").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, BucketOps::UpsertOp));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "replaceInternal").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, BucketOps::ReplaceOp));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "deleteInternal").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, BucketOps::DeleteOp));
  proto_t->Set(v8::String::NewFromUtf8(isolate_, "increment").ToLocalChecked(),
               v8::FunctionTemplate::New(isolate_, BucketOps::IncrementOp));
  proto_t->Set(v8::String::NewFromUtf8(isolate_, "decrement").ToLocalChecked(),
               v8::FunctionTemplate::New(isolate_, BucketOps::DecrementOp));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "touchInternal").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, BucketOps::TouchOp));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "bindingDetails").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, BucketOps::BindingDetails));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "mutateInInternal").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, BucketOps::MutateInOp));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "lookupInInternal").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, BucketOps::LookupInOp));
  proto_t->Set(v8::String::NewFromUtf8(isolate_, "n1qlQuery").ToLocalChecked(),
               v8::FunctionTemplate::New(isolate_, Query::N1qlFunction));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "analyticsQuery").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, Query::AnalyticsFunction));
  proto_t->Set(v8::String::NewFromUtf8(isolate_, "curl").ToLocalChecked(),
               v8::FunctionTemplate::New(isolate_, CurlFunction));
  proto_t->Set(v8::String::NewFromUtf8(isolate_, "log").ToLocalChecked(),
               v8::FunctionTemplate::New(isolate_, Log));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "createTimer").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, CreateTimer));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "cancelTimer").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, CancelTimer));
  proto_t->Set(v8::String::NewFromUtf8(isolate_, "crc64").ToLocalChecked(),
               v8::FunctionTemplate::New(isolate_, Crc64Function));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "crc_64_go_iso").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, Crc64GoIsoFunction));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "base64Encode").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, Base64EncodeFunction));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "base64Decode").ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, Base64DecodeFunction));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "base64Float64ArrayEncode")
          .ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, Base64Float64EncodeFunction));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "base64Float64ArrayDecode")
          .ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, Base64Float64DecodeFunction));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "base64Float32ArrayEncode")
          .ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, Base64Float32EncodeFunction));
  proto_t->Set(
      v8::String::NewFromUtf8(isolate_, "base64Float32ArrayDecode")
          .ToLocalChecked(),
      v8::FunctionTemplate::New(isolate_, Base64Float32DecodeFunction));

  auto context = context_.Get(isolate_);
  v8::Local<v8::Object> cb_obj;
  if (!TO_LOCAL(proto_t->NewInstance(context), &cb_obj)) {
    return;
  }

  auto global_object = context->Global();
  auto result =
      global_object->Set(context, v8Str(isolate_, "couchbase"), cb_obj);

  if (!result.FromJust()) {
    LOG(logError) << "Failed to set the global namespace couchbase"
                  << std::endl;
  }

  v8::TryCatch try_catch(isolate_);
  v8::Local<v8::String> couchbase_script_name = v8Str(isolate_, "couchbase.js");
  std::string wrapper_function2 = R"(
    couchbase.mapkeys = new Map();
    couchbase.map_key_size = 0;

    couchbase.get = function(bucket, meta, options) {
      if (!options || !options.cache) {
        return couchbase.getInternal(bucket, meta);
      }

      // TODO: In serverless Date object will have granularity of 1second.
      var currentTime = Date.now();
      var details = couchbase.bindingDetails(bucket, meta);
      const key = couchbase.getCachedKey(meta.id, details);
      var res = couchbase.mapkeys.get(key);
      if (res) {
        if (currentTime - res.time < couchbase.max_expiry) {
          return res.data;
        }
        couchbase.mapkeys.delete(key);
        couchbase.map_key_size = couchbase.map_key_size - res.data.res_size;
      }

      var res = couchbase.getInternal(bucket, meta, options);
      if (!res.success) {
        return res;
      }

      var cache_res = {
        "data": res,
        "time": Date.now()
      };
      if (couchbase.map_key_size + res.res_size > couchbase.max_size) {
        couchbase.ejectKeys(currentTime);
      }
      couchbase.mapkeys.set(key, cache_res);
      couchbase.map_key_size += res.res_size;
      return res;
    };

    couchbase.insert = function(bucket, meta, doc, options) {
      var details = couchbase.bindingDetails(bucket, meta);
      var key = couchbase.getCachedKey(meta.id, details);

      this.invalidateKey(key);
      if(!options) {
        options = {};
      }
      return couchbase.insertInternal(bucket, meta, doc, options);
    };

    couchbase.upsert = function(bucket, meta, doc, options) {
      var details = couchbase.bindingDetails(bucket, meta);
      var key = couchbase.getCachedKey(meta.id, details);

      this.invalidateKey(key);
      if(!options) {
        options = {};
      }
      return couchbase.upsertInternal(bucket, meta, doc, options);
    };

    couchbase.replace = function(bucket, meta, doc, options) {
      var details = couchbase.bindingDetails(bucket, meta);
      var key = couchbase.getCachedKey(meta.id, details);

      this.invalidateKey(key);
      if(!options) {
        options = {};
      }
      return couchbase.replaceInternal(bucket, meta, doc, options);
    };

    couchbase.MutateInSpec = {};

    Object.defineProperty(couchbase.MutateInSpec, "create", {
        enumerable: false,
        value: function(specType, path, value, specOptions) {
                 var spec = {"spec_type": specType};
                 spec.path = path;
                 spec.value = JSON.stringify(value);

                 if(specOptions != undefined) {
                   spec.options = specOptions;
                 }
                 return spec;
               }
        });

    couchbase.MutateInSpec.insert = function(path, value, specOptions) {
      return couchbase.MutateInSpec.create(1, path, value, specOptions);
    };

    couchbase.MutateInSpec.upsert = function(path, value, specOptions) {
      return couchbase.MutateInSpec.create(2, path, value, specOptions);
    };

    couchbase.MutateInSpec.replace = function(path, value, specOptions) {
      return couchbase.MutateInSpec.create(3, path, value, specOptions);
    };

    couchbase.MutateInSpec.remove = function(path, specOptions) {
      return couchbase.MutateInSpec.create(4, path, undefined, specOptions);
    };

    couchbase.MutateInSpec.arrayAppend = function(path, value, specOptions) {
      return couchbase.MutateInSpec.create(5, path, value, specOptions);
    };

    couchbase.MutateInSpec.arrayPrepend = function(path, value, specOptions) {
      return couchbase.MutateInSpec.create(6, path, value, specOptions);
    };

    couchbase.MutateInSpec.arrayInsert = function(path, value, specOptions) {
      return couchbase.MutateInSpec.create(7, path, value, specOptions);
    };

    couchbase.MutateInSpec.arrayAddUnique = function(path, value, specOptions) {
      return couchbase.MutateInSpec.create(8, path, value, specOptions);
    };

    couchbase.mutateIn = function(bucket, meta, op_array, options) {
      if(!op_array) {
        return {"success": true};
      }

      var details = couchbase.bindingDetails(bucket, meta);
      var key = couchbase.getCachedKey(meta.id, details);

      this.invalidateKey(key);
      if(!options) {
        options = {};
      }
      return couchbase.mutateInInternal(bucket, meta, op_array, options);
    };

    couchbase.LookupInSpec = {};

    Object.defineProperty(couchbase.LookupInSpec, "create", {
        enumerable: false,
        value: function(specType, path, specOptions) {
                 var spec = {"spec_type": specType};
                 spec.path = path;

                 if(specOptions != undefined) {
                   spec.options = specOptions;
                 }
                 return spec;
               }
        });

    couchbase.LookupInSpec.get = function(path, specOptions) {
      return couchbase.LookupInSpec.create(1, path, specOptions);
    };

    couchbase.lookupIn = function(bucket, meta, op_array, options) {
      if(!op_array) {
        return {"success": true};
      }

      var details = couchbase.bindingDetails(bucket, meta);
      var key = couchbase.getCachedKey(meta.id, details);

      this.invalidateKey(key);
      if(!options) {
        options = {};
      }
      return couchbase.lookupInInternal(bucket, meta, op_array, options);
    };

    couchbase.delete = function(bucket, meta) {
      var details = couchbase.bindingDetails(bucket, meta);
      var key = couchbase.getCachedKey(meta.id, details);

      this.invalidateKey(key);
      return couchbase.deleteInternal(bucket, meta);
    };

    couchbase.touch = function(bucket, meta) {
      var details = couchbase.bindingDetails(bucket, meta);
      var key = couchbase.getCachedKey(meta.id, details);

      this.invalidateKey(key);
      return couchbase.touchInternal(bucket, meta);
    };

    couchbase.ejectKeys = function(currtime) {
      var freed_size = 0;
      for (let [key, val] of couchbase.mapkeys) {
        if ((Math.floor(Math.random() * 8) % 3) == 0 || (currtime - val.time) > couchbase.max_expiry) {
          couchbase.mapkeys.delete(key);
          couchbase.map_key_size = couchbase.map_key_size - val.data.res_size;
        }
      }
    };

    couchbase.invalidateKey = function(key) {
      var result = couchbase.mapkeys.get(key);
      if (!result) {
        return;
      }
      var size = result.data.res_size;
      couchbase.mapkeys.delete(key);
      couchbase.map_key_size = couchbase.map_key_size - size;
    };

    couchbase.getCachedKey = function(id, details) {
      return details.keyspace.bucket + "/" + details.keyspace.scope + "/" + details.keyspace.collection + "/" + id;
    };)"
                                  R"(couchbase.max_expiry=)";

  wrapper_function2.append(std::to_string(cache_expiry_age_));
  wrapper_function2.append(";couchbase.max_size=");
  wrapper_function2.append(std::to_string(cache_size_));

  auto wrapper_source2 =
      v8::String::NewFromUtf8(isolate_, wrapper_function2.c_str())
          .ToLocalChecked();
  v8::ScriptOrigin origin2(isolate_, couchbase_script_name);
  v8::Local<v8::Script> compiled_script2;

  if (!TO_LOCAL(v8::Script::Compile(context, wrapper_source2, &origin2),
                &compiled_script2)) {
    LOG(logError) << "Exception logged:"
                  << ExceptionString(isolate_, context, &try_catch)
                  << std::endl;
  }

  v8::Local<v8::Value> result_wrapper2;
  if (!TO_LOCAL(compiled_script2->Run(context), &result_wrapper2)) {
    LOG(logError) << "Unable to run the injected couchbase.js script: "
                  << ExceptionString(isolate_, context, &try_catch)
                  << std::endl;
  }

  return;
}

v8::Local<v8::ObjectTemplate> V8Worker::NewGlobalObj() const {
  v8::EscapableHandleScope handle_scope(isolate_);

  auto global = v8::ObjectTemplate::New(isolate_);

  global->Set(v8::String::NewFromUtf8(isolate_, "curl").ToLocalChecked(),
              v8::FunctionTemplate::New(isolate_, CurlFunction));
  global->Set(v8::String::NewFromUtf8(isolate_, "log").ToLocalChecked(),
              v8::FunctionTemplate::New(isolate_, Log));
  global->Set(v8::String::NewFromUtf8(isolate_, "createTimer").ToLocalChecked(),
              v8::FunctionTemplate::New(isolate_, CreateTimer));
  global->Set(v8::String::NewFromUtf8(isolate_, "cancelTimer").ToLocalChecked(),
              v8::FunctionTemplate::New(isolate_, CancelTimer));
  global->Set(v8::String::NewFromUtf8(isolate_, "crc64").ToLocalChecked(),
              v8::FunctionTemplate::New(isolate_, Crc64Function));
  global->Set(v8::String::NewFromUtf8(isolate_, "N1QL").ToLocalChecked(),
              v8::FunctionTemplate::New(isolate_, Query::N1qlFunction));

  for (const auto &type_name : exception_type_names_) {
    global->Set(
        v8::String::NewFromUtf8(isolate_, type_name.c_str()).ToLocalChecked(),
        v8::FunctionTemplate::New(isolate_, CustomErrorCtor));
  }
  return handle_scope.Escape(global);
}

void V8Worker::InstallCurlBindings(
    const std::vector<CurlBinding> &curl_bindings) const {
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  for (const auto &binding : curl_bindings) {
    binding.InstallBinding(isolate_, context);
  }
}

void V8Worker::InstallConstantBindings(
    const std::vector<std::pair<std::string, std::string>> constant_bindings)
    const {

  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  v8::TryCatch try_catch(isolate_);
  v8::Local<v8::String> script_name = v8Str(isolate_, "constants.js");
  std::ostringstream oss;
  for (auto item : constant_bindings) {
    oss << "const " << item.first << " = " << item.second << ";\n";
  }

  std::string injection_code = oss.str();
  auto injection_source =
      v8::String::NewFromUtf8(isolate_, injection_code.c_str())
          .ToLocalChecked();
  v8::ScriptOrigin origin(isolate_, script_name);
  v8::Local<v8::Script> compiled_script;

  if (!TO_LOCAL(v8::Script::Compile(context, injection_source, &origin),
                &compiled_script)) {
    assert(try_catch.HasCaught());
    LOG(logError) << "Exception logged:"
                  << ExceptionString(isolate_, context, &try_catch)
                  << std::endl;
  }

  v8::Local<v8::Value> result_wrapper;
  if (!TO_LOCAL(compiled_script->Run(context), &result_wrapper)) {
    LOG(logError) << "Unable to inject constant bindings into the script"
                  << std::endl;
  }
}

void V8Worker::InstallBucketBindings(
    const std::unordered_map<
        std::string, std::unordered_map<std::string, std::vector<std::string>>>
        &config) {
  auto buckets_it = config.find("buckets");
  if (buckets_it == config.end()) {
    return;
  }

  for (const auto &[bucket_alias, bucket_info] : buckets_it->second) {
    // bucket_info -> {bucketName, scopeName, collectionName, alias, access}
    const auto &bucket_name = bucket_info[0];
    const auto &scope_name = bucket_info[1];
    const auto &collection_name = bucket_info[2];
    const auto &bucket_access = bucket_info[4];
    auto is_source_bucket = (bucket_name == cb_source_bucket_);
    bucket_bindings_.emplace_back(
        isolate_, bucket_factory_, bucket_name, scope_name, collection_name,
        bucket_alias, bucket_access == "r", is_source_bucket, user_, domain_);
  }
}

void V8Worker::InitializeIsolateData(const server_settings_t *server_settings,
                                     const handler_config_t *h_config) {
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  data_.v8worker = this;
  data_.utils = new Utils(isolate_, context, certFile_);
  data_.js_exception = new JsException(isolate_);
  auto key = GetLocalKey();
  data_.comm = new Communicator(
      server_settings->host_addr, server_settings->eventing_port, key.first,
      key.second, false, app_name_, bucket_, scope_, isolate_);
  data_.timer = new Timer(isolate_, context, this->timer_reduction_ratio_);
  // TODO : Need to make HEAD call to all the bindings to establish TCP
  // Connections
  data_.curl_factory = new CurlFactory(isolate_, context);
  data_.req_builder = new CurlRequestBuilder(isolate_, context);
  data_.resp_builder = new CurlResponseBuilder(
      isolate_, context, h_config->curl_max_allowed_resp_size);
  data_.custom_error = new CustomError(isolate_, context);
  data_.curl_codex = new CurlCodex;
  data_.code_insight = new CodeInsight(isolate_);
  data_.exception_insight = new ExceptionInsight(isolate_);
  data_.query_mgr = new Query::Manager(
      isolate_, cb_source_bucket_,
      static_cast<std::size_t>(h_config->lcb_inst_capacity), user_, domain_);
  data_.query_iterable = new Query::Iterable(isolate_, context);
  data_.query_iterable_impl = new Query::IterableImpl(isolate_, context);
  data_.query_iterable_result = new Query::IterableResult(isolate_, context);
  data_.query_helper = new Query::Helper(isolate_, context);

  // execution_timeout is in seconds
  // query_timeout is expected in micro seconds
  // Setting a lower timeout to allow adequate time for the exception
  // to get thrown
  data_.n1ql_timeout =
      static_cast<lcb_U32>(h_config->execution_timeout < 3
                               ? 500000
                               : (h_config->execution_timeout - 2) * 1000000);
  data_.analytics_timeout =
      static_cast<lcb_U32>(h_config->execution_timeout < 3
                               ? 500000
                               : (h_config->execution_timeout - 2) * 1000000);
  data_.op_timeout = h_config->execution_timeout < 5
                         ? h_config->execution_timeout
                         : h_config->execution_timeout - 2;
  data_.cursor_checkpoint_timeout = h_config->cursor_checkpoint_timeout > 0
                                        ? h_config->cursor_checkpoint_timeout
                                        : data_.op_timeout;
  data_.n1ql_consistency =
      Query::Helper::GetN1qlConsistency(h_config->n1ql_consistency);
  data_.n1ql_prepare_all = h_config->n1ql_prepare_all;
  data_.lang_compat = new LanguageCompatibility(h_config->lang_compat);
  data_.lcb_retry_count = h_config->lcb_retry_count;
  data_.lcb_timeout = ConvertSecondsToMicroSeconds(h_config->lcb_timeout);
  data_.lcb_cursor_checkpoint_timeout =
      ConvertSecondsToMicroSeconds(data_.cursor_checkpoint_timeout);
  data_.insight_line_offset = h_config->handler_headers.size();

  data_.bucket_ops = new BucketOps(isolate_, context);
  data_.feature_matrix = 0;
}

void V8Worker::InitializeCurlBindingValues(
    const std::vector<CurlBinding> &curl_bindings) {
  for (const auto &curl_binding : curl_bindings) {
    curl_binding_values_.emplace_back(curl_binding.value);
  }
}

V8Worker::V8Worker(
    v8::Platform *platform, handler_config_t *h_config,
    server_settings_t *server_settings, const std::string &function_name,
    const std::string &function_id, const std::string &function_instance_id,
    const std::string &user_prefix, Histogram *latency_stats,
    Histogram *curl_latency_stats, const std::string &ns_server_port,
    const int32_t &num_vbuckets, vb_seq_map_t *vb_seq,
    std::vector<uint64_t> *processed_bucketops,
    // TODO: put user and domain into Owner class and call
    // .ForKv() and .ForQuery()
    vb_lock_map_t *vb_locks, const std::string &user, const std::string &domain)
    : app_name_(h_config->app_name), bucket_(h_config->bucket),
      scope_(h_config->scope), settings_(server_settings),
      num_vbuckets_(num_vbuckets),
      timer_reduction_ratio_(
          int(num_vbuckets / h_config->num_timer_partitions)),
      latency_stats_(latency_stats), curl_latency_stats_(curl_latency_stats),
      vb_seq_(vb_seq), vb_locks_(vb_locks),
      processed_bucketops_(processed_bucketops), platform_(platform),
      certFile_(server_settings->certFile), function_name_(function_name),
      function_id_(function_id), user_prefix_(user_prefix),
      ns_server_port_(ns_server_port), user_(user), domain_(domain),
      exception_type_names_({"KVError", "EventingError", "CurlError",
                             "TypeError", "N1QLError", "AnalyticsError"}),
      handler_headers_(h_config->handler_headers) {
  auto config = ParseDeployment(h_config->dep_cfg.c_str());
  cb_source_bucket_.assign(config->source_bucket);
  cb_source_scope_.assign(config->source_scope);
  cb_source_collection_.assign(config->source_collection);

  std::ostringstream oss;
  oss << "\"" << function_id << "-" << function_instance_id << "\"";
  function_instance_id_.assign(oss.str());
  thread_exit_cond_.store(false);
  stop_timer_scan_.store(false);
  timed_out_.store(false);
  tracker_enabled_.store(false);
  event_processing_ongoing_.store(false);
  scan_timer_.store(false);
  update_v8_heap_.store(false);
  run_gc_.store(false);
  vbfilter_map_ = std::vector<std::vector<uint64_t>>(num_vbuckets_);

  v8::Isolate::CreateParams create_params;
  create_params.array_buffer_allocator =
      v8::ArrayBuffer::Allocator::NewDefaultAllocator();

  isolate_ = v8::Isolate::New(create_params);
  isolate_->SetData(IsolateData::index, &data_);
  isolate_->SetCaptureStackTraceForUncaughtExceptions(true);

  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto global = NewGlobalObj();
  auto context = v8::Context::New(isolate_, nullptr, global);
  context_.Reset(isolate_, context);

  v8::Context::Scope context_scope(context);

  lcb_logger_create(&evt_logger.base, &evt_logger);
  lcb_logger_callback(evt_logger.base, evt_log_handler);

  InitializeIsolateData(server_settings, h_config);
  InstallCurlBindings(config->curl_bindings);
  InstallConstantBindings(config->constant_bindings);
  InitializeCurlBindingValues(config->curl_bindings);

  bucket_factory_ = std::make_shared<BucketFactory>(isolate_, context);
  if (!h_config->skip_lcb_bootstrap) {
    InstallBucketBindings(config->component_configs);
  }

  execute_start_time_ = Time::now();
  max_task_duration_ = SECS_TO_NS * h_config->execution_timeout;

  timer_context_size = h_config->timer_context_size;

  cache_size_ = h_config->bucket_cache_size;
  cache_expiry_age_ = h_config->bucket_cache_age;

  LOG(logInfo) << "Initialised V8Worker handle, app_name: "
               << h_config->app_name << " under " << bucket_ << ":" << scope_
               << " debugger port: " << RS(settings_->debugger_port)
               << " curr_host: " << RS(settings_->host_addr)
               << " curr_eventing_port: " << RS(settings_->eventing_port)
               << " curr_eventing_sslport: " << RS(settings_->eventing_sslport)
               << " lcb_cap: " << h_config->lcb_inst_capacity
               << " n1ql_consistency: " << h_config->n1ql_consistency
               << " execution_timeout: " << h_config->execution_timeout
               << " timer_context_size: " << h_config->timer_context_size
               << " ns_server_port: " << ns_server_port_
               << " language compatibility: " << h_config->lang_compat
               << " version: " << EventingVer()
               << " n1ql_prepare_all: " << h_config->n1ql_prepare_all
               << " num_vbuckets: " << num_vbuckets_
               << " num_timer_partitions: " << h_config->num_timer_partitions
               << " bucket_cache_size: " << h_config->bucket_cache_size
               << " bucket_cache_age: " << h_config->bucket_cache_age
               << std::endl;

  src_path_ = settings_->eventing_dir + "/" + app_name_ + ".t.js";

  if (h_config->using_timer) {
    auto prefix = user_prefix + "::" + function_id;
    timer_store_ = new timer::TimerStore(
        isolate_, prefix, config->metadata_bucket, config->metadata_scope,
        config->metadata_collection, num_vbuckets_, timer_reduction_ratio_,
        user_, domain_);
  }
  delete config;
  this->worker_queue_ = new BlockingDeque<std::unique_ptr<WorkerMessage>>();

  std::thread r_thr(&V8Worker::RouteMessage, this);
  processing_thr_ = std::move(r_thr);
}

V8Worker::~V8Worker() {
  if (processing_thr_.joinable()) {
    processing_thr_.join();
  }

  FreeCurlBindings();

  auto data = UnwrapData(isolate_);
  delete data->custom_error;
  delete data->comm;
  delete data->utils;
  delete data->timer;
  delete data->js_exception;
  delete data->curl_factory;
  delete data->req_builder;
  delete data->resp_builder;
  delete data->curl_codex;
  delete data->exception_insight;
  delete data->query_mgr;
  delete data->query_iterable;
  delete data->query_iterable_impl;
  delete data->query_iterable_result;
  delete data->query_helper;
  delete data->lang_compat;

  context_.Reset();
  on_update_.Reset();
  on_delete_.Reset();
  on_deploy_.Reset();
  delete settings_;
  delete worker_queue_;
  delete timer_store_;
}

struct DebugExecuteGuard {
  DebugExecuteGuard(v8::Isolate *isolate) : isolate_(isolate) {
    UnwrapData(isolate_)->is_executing_ = true;
  }

  ~DebugExecuteGuard() { UnwrapData(isolate_)->is_executing_ = false; }

  DebugExecuteGuard(const DebugExecuteGuard &) = delete;
  DebugExecuteGuard(DebugExecuteGuard &&) = delete;
  DebugExecuteGuard &operator=(const DebugExecuteGuard &) = delete;
  DebugExecuteGuard &operator=(DebugExecuteGuard &&) = delete;

private:
  v8::Isolate *isolate_;
};

// Re-compile and execute handler code for debugger
bool V8Worker::DebugExecute(const char *func_name, v8::Local<v8::Value> *args,
                            int args_len) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  v8::TryCatch try_catch(isolate_);

  // Need to construct origin for source-map to apply
  auto origin_v8_str = v8Str(isolate_, src_path_);
  v8::ScriptOrigin origin(isolate_, origin_v8_str);

  v8::Local<v8::Function> console_log_func;
  if (!TO_LOCAL(
          v8::FunctionTemplate::New(isolate_, ConsoleLog)->GetFunction(context),
          &console_log_func)) {
    return false;
  }

  // Replace the usual log function with console.log
  auto global = context->Global();
  CHECK_SUCCESS(global->Set(context, v8Str(isolate_, "log"), console_log_func));

  auto source = v8Str(isolate_, script_to_execute_);
  v8::Local<v8::Script> script;
  if (!TO_LOCAL(v8::Script::Compile(context, source, &origin), &script)) {
    return false;
  }

  v8::Local<v8::Value> result;
  if (!TO_LOCAL(script->Run(context), &result)) {
    return false;
  }

  auto func_ref = global->Get(context, v8Str(isolate_, func_name));
  auto func = v8::Local<v8::Function>::Cast(func_ref.ToLocalChecked());
  RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                        IsTerminatingRetriable, IsExecutionTerminating,
                        isolate_);
  DebugExecuteGuard guard(isolate_);
  if (!TO_LOCAL(func->Call(context, v8::Null(isolate_), args_len, args),
                &result)) {
    return false;
  }

  if (try_catch.HasCaught()) {
    agent_->FatalException(try_catch.Exception(), try_catch.Message());
  }

  return true;
}

void V8Worker::EnableTracker() {
  if (tracker_enabled_) {
    return;
  }
  auto fiid_ = GetFunctionInstanceID();
  checkpoint_writer_ = std::make_unique<CheckpointWriter>(
      isolate_, fiid_, cb_source_bucket_, cb_source_scope_,
      cb_source_collection_);
  checkpoint_writer_->Connect();
  tracker_enabled_ = true;
}

void V8Worker::DisableTracker() {
  return; // [TODO]: Implement
}

int V8Worker::V8WorkerLoad(std::string script_to_execute) {
  LOG(logInfo) << "Eventing dir: " << RS(settings_->eventing_dir) << std::endl;
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  SetCouchbaseNamespace();
  for (const auto &type_name : exception_type_names_) {
    DeriveFromError(isolate_, context, type_name);
  }

  auto final_code = AddHeadersAndFooters(script_to_execute);
  script_to_execute = final_code + '\n';
  LOG(logTrace) << "script to execute: " << RM(script_to_execute) << std::endl;
  script_to_execute_ = script_to_execute;

  CodeInsight::Get(isolate_).Setup(script_to_execute);

  auto source = v8Str(isolate_, script_to_execute);
  if (!ExecuteScript(source)) {
    return kFailedToCompileJs;
  }

  ExceptionInsight::Get(isolate_).Setup(function_name_);

  auto global = context->Global();
  v8::Local<v8::Value> on_update_def;
  if (!TO_LOCAL(global->Get(context, v8Str(isolate_, "OnUpdate")),
                &on_update_def)) {
    return kToLocalFailed;
  }

  v8::Local<v8::Value> on_delete_def;
  if (!TO_LOCAL(global->Get(context, v8Str(isolate_, "OnDelete")),
                &on_delete_def)) {
    return kToLocalFailed;
  }

  v8::Local<v8::Value> on_deploy_def;
  if (!TO_LOCAL(global->Get(context, v8Str(isolate_, "OnDeploy")),
                  &on_deploy_def)) {
    return kToLocalFailed;
  }

  if (!on_update_def->IsFunction() && !on_delete_def->IsFunction() && !on_deploy_def->IsFunction()) {
    return kNoHandlersDefined;
  }

  if (on_update_def->IsFunction()) {
    auto on_update_fun = on_update_def.As<v8::Function>();
    on_update_.Reset(isolate_, on_update_fun);
  }

  if (on_delete_def->IsFunction()) {
    auto on_delete_fun = on_delete_def.As<v8::Function>();
    on_delete_.Reset(isolate_, on_delete_fun);
  }

  if (on_deploy_def->IsFunction()) {
    auto on_deploy_fun = on_deploy_def.As<v8::Function>();
    on_deploy_.Reset(isolate_, on_deploy_fun);
  }

  for (auto &binding : bucket_bindings_) {
    auto error = binding.InstallBinding(isolate_, context);
    if (error != nullptr) {
      LOG(logError) << "Unable to install bucket binding, err : " << *error
                    << std::endl;
    }
  }

  v8::TryCatch try_catch(isolate_);
  v8::Local<v8::String> script_name = v8Str(isolate_, "N1qlQuery.js");
  std::string wrapper_function = R"(function N1qlQuery(stmt, params) {
      this.statement = stmt;
      this.named = {};
      this.options = {'consistency': 'none'};
      if (params) {
          Object.assign(this.named, params['namedParams']);
          if (params.hasOwnProperty('consistency'))
              this.options['consistency'] = params['consistency'];
          }
      this.execQuery = function() {
          let iter = N1QL(this.statement, this.named, this.options);
          let result = Array.from(iter);
          iter.close();
          return result;
      };
  })";

  auto wrapper_source =
      v8::String::NewFromUtf8(isolate_, wrapper_function.c_str())
          .ToLocalChecked();
  v8::ScriptOrigin origin(isolate_, script_name);
  v8::Local<v8::Script> compiled_script;

  if (!TO_LOCAL(v8::Script::Compile(context, wrapper_source, &origin),
                &compiled_script)) {
    assert(try_catch.HasCaught());
    LOG(logError) << "Exception logged:"
                  << ExceptionString(isolate_, context, &try_catch)
                  << std::endl;
  }

  v8::Local<v8::Value> result_wrapper;
  if (!TO_LOCAL(compiled_script->Run(context), &result_wrapper)) {
    LOG(logError) << "Unable to run the injected N1qlQuery script" << std::endl;
  }

  // Spawning terminator thread to monitor the wall clock time for execution
  // of javascript code isn't going beyond max_task_duration
  terminator_thr_ = new std::thread(&V8Worker::TaskDurationWatcher, this);
  return kSuccess;
}

void V8Worker::RouteMessage() {
  std::string val, context, callback;
  while (!thread_exit_cond_.load()) {
    std::unique_ptr<WorkerMessage> msg;
    if (!worker_queue_->PopFront(msg)) {
      continue;
    }

    LOG(logTrace) << " event: " << static_cast<int16_t>(msg->header.event)
                  << " opcode: " << static_cast<int16_t>(msg->header.opcode)
                  << " metadata: " << RU(msg->header.metadata)
                  << " partition: " << msg->header.partition << std::endl;

    event_processing_ongoing_.store(true);
    const auto evt = getEvent(msg->header.event);
    switch (evt) {
    case event_type::eOnDeploy:
      HandleDeployEvent(msg);
      break;
    case event_type::eDCP: {
      const auto dcpOpcode = getDCPOpcode(msg->header.opcode);
      switch (dcpOpcode) {
      case dcp_opcode::oDelete:
        HandleDeleteEvent(msg);
        break;

      case dcp_opcode::oMutation:
        HandleMutationEvent(msg);
        break;

      case dcp_opcode::oNoOp:
        HandleNoOpEvent(msg);
        break;

      case dcp_opcode::oDeleteCid:
        HandleDeleteCidEvent(msg);
        break;

      default:
        // SNH: Any invalid `uint8_t` to `enum class dcp_opcode` conversion will
        // get caught by the `assert` in the function `getDCPOpcode` during
        // testing and development.
        // This `default` case is only for preventing unnecessary compiler
        // warnings
        LOG(logError) << "Received invalid DCP opcode "
                      << static_cast<int>(dcpOpcode) << std::endl;
        break;
      }
      processed_events_size += msg->payload.GetSize();
      num_processed_events++;
      break;
    }

    case event_type::eInternal: {
      const auto internalOpcode = getInternalOpcode(msg->header.opcode);
      switch (internalOpcode) {
      case internal_opcode::oScanTimer: {
        auto iter = timer_store_->GetIterator();
        timer::TimerEvent evt;
        while (!stop_timer_scan_.load() && iter.GetNext(evt)) {
          ++timer_msg_counter;
          this->SendTimer(evt.callback, evt.context);
          timer_store_->DeleteTimer(evt);
        }
        if (stop_timer_scan_.load()) {
          timer_store_->SyncSpan();
        }
        scan_timer_.store(false);
        break;
      }
      case internal_opcode::oUpdateV8HeapSize: {
        UpdateV8HeapSize();
        update_v8_heap_.store(false);
        break;
      }
      case internal_opcode::oRunGc: {
        ForceRunGarbageCollector();
        run_gc_.store(false);
        break;
      }
      default:
        // SNH: Any invalid `uint8_t` to `enum class internal_opcode` conversion
        // will get caught by the `assert` in the function `getDCPOpcode` during
        // testing and development.
        // This `default` case is only for preventing unnecessary compiler
        // warnings
        LOG(logError) << "Received invalid internal opcode "
                      << static_cast<int>(internalOpcode) << std::endl;
        break;
      }
      break;
    }

    case event_type::eDebugger: {
      const auto debuggerOpcode = getDebuggerOpcode(msg->header.opcode);
      switch (debuggerOpcode) {
      case debugger_opcode::oDebuggerStart:
        this->StartDebugger();
        break;

      case debugger_opcode::oDebuggerStop:
        this->StopDebugger();
        break;

      default:
        // SNH: Any invalid `uint8_t` to `enum class debugger_opcode` conversion
        // will get caught by the `assert` in the function `getDCPOpcode` during
        // testing and development.
        // This `default` case is only for preventing unnecessary compiler
        // warnings
        LOG(logError) << "Received invalid debugger opcode "
                      << static_cast<int>(debuggerOpcode) << std::endl;
        break;
      }
      break;
    }

    case event_type::eConfigChange: {
      const auto configOpcode = getConfigOpcode(msg->header.opcode);
      switch (configOpcode) {
      case config_opcode::oUpdateDisableFeatureList: {
        int32_t feature_matrix =
            static_cast<uint32_t>(std::stoul(msg->header.metadata));
        this->SetFeatureList(feature_matrix);
        break;
      }

      default:
        // SNH: Any invalid `uint8_t` to `enum class config_opcode` conversion
        // will get caught by the `assert` in the function `getDCPOpcode` during
        // testing and development.
        // This `default` case is only for preventing unnecessary compiler
        // warnings
        LOG(logError) << "Received invalid config opcode "
                      << static_cast<int>(configOpcode) << std::endl;
        break;
      }
      break;
    }

    default:
      // SNH: Any invalid `uint8_t` to `enum class event_type` conversion will
      // get caught by the `assert` in the function `getDCPOpcode` during
      // testing and development.
      // This `default` case is only for preventing unnecessary compiler
      // warnings
      LOG(logError) << "Received unsupported event " << static_cast<int>(evt)
                    << std::endl;
      break;
    }

    ++messages_processed_counter;
    event_processing_ongoing_.store(false);
  }
}

void V8Worker::UpdateSeqNumLocked(const int vb, const uint64_t seq_num) {
  auto lock = GetAndLockVbLock(vb);
  currently_processed_vb_ = vb;
  currently_processed_seqno_ = seq_num;
  (*vb_seq_)[vb]->store(seq_num, std::memory_order_seq_cst);
  (*processed_bucketops_)[vb] = seq_num;
  lock.unlock();
}

void V8Worker::HandleDeleteEvent(const std::unique_ptr<WorkerMessage> &msg) {

  ++dcp_delete_msg_counter;
  auto [parsed_meta, is_valid] = ParseWorkerMessage(msg);
  if (!is_valid || !parsed_meta.has_value()) {
    ++dcp_delete_parse_failure;
    return;
  }

  {
    std::lock_guard<std::mutex> guard(bucketops_lock_);
    auto [filter, update] = IsFilteredEventLocked(
        false, parsed_meta->cid, parsed_meta->vb, parsed_meta->seq_num);
    if (update) {
      UpdateSeqNumLocked(parsed_meta->vb, parsed_meta->seq_num);
    }
    if (filter) {
      return;
    }
  }

  const auto options = flatbuf::payload::GetPayload(
      static_cast<const void *>(msg->payload.payload.c_str()));
  SendDelete(options->value()->str(), msg->header.metadata);
}

void V8Worker::HandleMutationEvent(const std::unique_ptr<WorkerMessage> &msg) {

  ++dcp_mutation_msg_counter;
  auto [parsed_meta, is_valid] = ParseWorkerMessage(msg);
  if (!is_valid || !parsed_meta.has_value()) {
    ++dcp_mutation_parse_failure;
    return;
  }

  {
    std::lock_guard<std::mutex> guard(bucketops_lock_);
    auto [filter, update] = IsFilteredEventLocked(
        false, parsed_meta->cid, parsed_meta->vb, parsed_meta->seq_num);
    if (update) {
      UpdateSeqNumLocked(parsed_meta->vb, parsed_meta->seq_num);
    }
    if (filter) {
      return;
    }
  }

  const auto doc = flatbuf::payload::GetPayload(
      static_cast<const void *>(msg->payload.payload.c_str()));
  {
    if (tracker_enabled_) {
      uint64_t cas =
          static_cast<uint64_t>(std::stoull(parsed_meta->cas, nullptr, 10));
      uint64_t rootcas =
          static_cast<uint64_t>(std::stoull(parsed_meta->rootcas, nullptr, 10));
      auto cursors = doc->cursors()->str();
      std::vector<std::string> cursors_arr;
      if (cursors.size() > 0) {
        splitString(cursors, cursors_arr, ',');
      }
      auto [client_err, err_code] = checkpoint_writer_->Write(
          MetaData(parsed_meta->scope, parsed_meta->collection,
                   parsed_meta->key, cas),
          rootcas, cursors_arr);
      if (err_code != LCB_SUCCESS) {
        if (err_code == LCB_ERR_CAS_MISMATCH) {
          ++dcp_mutation_checkpoint_cas_mismatch;
        } else {
          ++dcp_mutation_checkpoint_failure;
          auto err_cstr = lcb_strerror_short(err_code);
          if (client_err.length() > 0) {
            err_cstr = client_err.c_str();
          }
          // TODO : Handle scope deletion, collection deletion, document
          // deletion
          APPLOG << "cursor progression failed for document: "
                 << parsed_meta->scope << "/" << parsed_meta->collection << "/"
                 << parsed_meta->key << " rootcas: " << parsed_meta->rootcas
                 << " error: " << err_cstr << std::endl;
        }
        return;
      }
    }
  }

  SendUpdate(doc->value()->str(), msg->header.metadata, doc->xattr()->str(),
             doc->is_binary());
}

void V8Worker::HandleDeployEvent(const std::unique_ptr<WorkerMessage> &msg) {
  const auto payload = flatbuf::payload::GetPayload(
      static_cast<const void *>(msg->payload.payload.c_str()));
  int return_code = SendDeploy(payload->action()->str(), payload->delay());
  if(return_code == kSuccess)
    on_deploy_stat = OnDeployState::FINISHED;
  else
    on_deploy_stat = OnDeployState::FAILED;
}

void V8Worker::HandleNoOpEvent(const std::unique_ptr<WorkerMessage> &msg) {
  auto [parsed_meta, is_valid] = ParseWorkerMessage(msg);
  if (!is_valid || !parsed_meta.has_value()) {
    return;
  }

  {
    std::lock_guard<std::mutex> guard(bucketops_lock_);
    auto [filter, update] = IsFilteredEventLocked(
        true, parsed_meta->cid, parsed_meta->vb, parsed_meta->seq_num);
    if (update) {
      UpdateSeqNumLocked(parsed_meta->vb, parsed_meta->seq_num);
    }
    if (filter) {
      return;
    }
  }
  no_op_counter++;
}

void V8Worker::HandleDeleteCidEvent(const std::unique_ptr<WorkerMessage> &msg) {
  auto [parsed_meta, is_valid] = ParseWorkerMessage(msg);
  if (!is_valid || !parsed_meta.has_value()) {
    return;
  }

  std::lock_guard<std::mutex> guard(vbfilter_map_for_cid_lock_);
  auto lock = GetAndLockVbLock(parsed_meta->vb);
  auto cidIt = vbfilter_map_for_cid_.find(parsed_meta->cid);
  if (cidIt == vbfilter_map_for_cid_.end()) {
    return;
  }

  cidIt->second.erase(parsed_meta->vb);
  if (cidIt->second.size() == 0) {
    vbfilter_map_for_cid_.erase(parsed_meta->cid);
  }
}

std::tuple<std::optional<ParsedMetadata>, bool>
V8Worker::ParseWorkerMessage(const std::unique_ptr<WorkerMessage> &msg) const {
  auto result = ParseMetadata(msg->header.metadata);
  return {result.first, result.second == kSuccess};
}

std::tuple<bool, bool> V8Worker::IsFilteredEventLocked(bool skip_cid_check,
                                                       const uint32_t cid,
                                                       const int vb,
                                                       const uint64_t seq_num) {
  const auto filter_seq_no = GetVbFilter(vb);
  if (filter_seq_no > 0 && seq_num <= filter_seq_no) {
    // Skip filtered event
    ++filtered_dcp_mutation_counter;
    if (seq_num == filter_seq_no) {
      EraseVbFilter(vb);
    }
    return {true, false};
  }

  if (skip_cid_check) {
    return {false, true};
  }

  std::lock_guard<std::mutex> guard(vbfilter_map_for_cid_lock_);
  auto lock = GetAndLockVbLock(vb);
  auto cidIt = vbfilter_map_for_cid_.find(cid);
  if (cidIt == vbfilter_map_for_cid_.end()) {
    return {false, true};
  }

  auto vbIt = cidIt->second.find(vb);
  if (vbIt == cidIt->second.end()) {
    return {false, true};
  }

  return {true, true};
}

bool V8Worker::ExecuteScript(const v8::Local<v8::String> &script) {
  v8::HandleScope handle_scope(isolate_);
  v8::TryCatch try_catch(isolate_);

  auto context = context_.Get(isolate_);
  auto script_name = v8Str(isolate_, app_name_ + ".js");
  v8::ScriptOrigin origin(isolate_, script_name);

  v8::Local<v8::Script> compiled_script;
  if (!v8::Script::Compile(context, script, &origin)
           .ToLocal(&compiled_script)) {
    assert(try_catch.HasCaught());
    LOG(logError) << "Exception logged:"
                  << ExceptionString(isolate_, context, &try_catch)
                  << std::endl;
    // The script failed to compile; bail out.
    return false;
  }

  v8::Local<v8::Value> result;
  if (!compiled_script->Run(context).ToLocal(&result)) {
    assert(try_catch.HasCaught());
    LOG(logError) << "Exception logged:"
                  << ExceptionString(isolate_, context, &try_catch)
                  << std::endl;
    // Running the script failed; bail out.
    return false;
  }

  return true;
}

void V8Worker::AddLcbException(int err_code) {
  std::lock_guard<std::mutex> lock(lcb_exception_mtx_);
  lcb_exceptions_[err_code]++;
}

void V8Worker::ListLcbExceptions(std::map<int, int64_t> &agg_lcb_exceptions) {
  std::lock_guard<std::mutex> lock(lcb_exception_mtx_);
  for (auto const &entry : lcb_exceptions_) {
    agg_lcb_exceptions[entry.first] += entry.second;
  }
}

void V8Worker::UpdateHistogram(Time::time_point start_time) {
  Time::time_point t = Time::now();
  nsecs ns = std::chrono::duration_cast<nsecs>(t - start_time);
  latency_stats_->Add(ns.count() / 1000);
}

void V8Worker::UpdateCurlLatencyHistogram(const Time::time_point &start) {
  Time::time_point t = Time::now();
  nsecs ns = std::chrono::duration_cast<nsecs>(t - start);
  curl_latency_stats_->Add(ns.count() / 1000);
}

int V8Worker::SendUpdate(const std::string &value, const std::string &meta,
                         const std::string &xattr, bool is_binary) {
  const auto start_time = Time::now();

  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  LOG(logTrace) << "value: " << RU(value) << " meta: " << RU(meta) << RU(xattr)
                << std::endl;
  v8::TryCatch try_catch(isolate_);

  v8::Local<v8::Value> args[on_update_args_count];
  if (is_binary) {
    auto utils = UnwrapData(isolate_)->utils;
    args[0] = utils->ToArrayBuffer(value.c_str(), value.length());
  } else {
    if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, value)), &args[0])) {
      return kToLocalFailed;
    }
  }

  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, xattr)), &args[2])) {
    return kToLocalFailed;
  }

  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, meta)), &args[1])) {
    return kToLocalFailed;
  }

  auto js_meta = args[1].As<v8::Object>();
  auto dcp_expiry = v8::Local<v8::Number>::Cast(
      js_meta->Get(context, v8Str(isolate_, "expiration")).ToLocalChecked());
  if (!dcp_expiry.IsEmpty()) {
    auto expiry = dcp_expiry->Value() * 1000;
    if (expiry != 0) {
      auto js_expiry = v8::Date::New(context, expiry).ToLocalChecked();
      auto r = js_meta->Set(context, v8Str(isolate_, "expiry_date"), js_expiry);
      if (!r.FromMaybe(true)) {
        LOG(logWarning) << "Create expiry_date failed in OnUpdate" << std::endl;
      }
    }
  }

  if (on_update_.IsEmpty()) {
    UpdateHistogram(start_time);
    return kOnUpdateCallFail;
  }

  if (try_catch.HasCaught()) {
    auto emsg = ExceptionString(isolate_, context, &try_catch);
    LOG(logDebug) << "OnUpdate Exception: " << emsg << std::endl;
    CodeInsight::Get(isolate_).AccumulateException(try_catch);
    ExceptionInsight::Get(isolate_).AccumulateException(try_catch);
  }

  if (debugger_started_) {
    if (!agent_->IsStarted()) {
      agent_->Start(isolate_, platform_, src_path_.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    return DebugExecute("OnUpdate", args, on_update_args_count)
               ? kSuccess
               : kOnUpdateCallFail;
  }

  RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                        IsTerminatingRetriable, IsExecutionTerminating,
                        isolate_);

  v8::Handle<v8::Value> result;
  timed_out_ = false;
  auto on_doc_update = on_update_.Get(isolate_);
  execute_start_time_ = Time::now();
  UnwrapData(isolate_)->is_executing_ = true;
  if (!TO_LOCAL(on_doc_update->Call(context, context->Global(),
                                    on_update_args_count, args),
                &result)) {
    LOG(logError) << "Error executing on_doc_update \n";
  }
  UnwrapData(isolate_)->is_executing_ = false;
  auto query_mgr = UnwrapData(isolate_)->query_mgr;
  query_mgr->ClearQueries();

  if (try_catch.HasCaught()) {
    UpdateHistogram(start_time);
    on_update_failure++;
    auto emsg = ExceptionString(isolate_, context, &try_catch, timed_out_);
    LOG(logDebug) << "OnUpdate Exception: " << emsg << std::endl;
    CodeInsight::Get(isolate_).AccumulateException(try_catch, timed_out_);
    ExceptionInsight::Get(isolate_).AccumulateException(try_catch, timed_out_);
    return kOnUpdateCallFail;
  }

  on_update_success++;
  UpdateHistogram(start_time);
  return kSuccess;
}

int V8Worker::SendDelete(const std::string &options, const std::string &meta) {
  const auto start_time = Time::now();

  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  LOG(logTrace) << " meta: " << RU(meta) << std::endl;
  v8::TryCatch try_catch(isolate_);

  v8::Local<v8::Value> args[on_delete_args_count];
  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, meta)), &args[0])) {
    return kToLocalFailed;
  }

  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, options)), &args[1])) {
    return kToLocalFailed;
  }

  // DCP always sends 0 as expiration. Until that changes, below won't run
  auto js_meta = args[0].As<v8::Object>();
  auto dcp_expiry = v8::Local<v8::Number>::Cast(
      js_meta->Get(context, v8Str(isolate_, "expiration")).ToLocalChecked());
  if (!dcp_expiry.IsEmpty()) {
    auto expiry = dcp_expiry->Value() * 1000;
    if (expiry != 0) {
      auto js_expiry = v8::Date::New(context, expiry).ToLocalChecked();
      auto r = js_meta->Set(context, v8Str(isolate_, "expiry_date"), js_expiry);
      if (!r.FromMaybe(true)) {
        LOG(logWarning) << "Create expiry_date failed in OnDelete" << std::endl;
      }
    }
  }

  if (on_delete_.IsEmpty()) {
    UpdateHistogram(start_time);
    return kOnDeleteCallFail;
  }

  if (try_catch.HasCaught()) {
    auto emsg = ExceptionString(isolate_, context, &try_catch);
    LOG(logDebug) << "OnDelete Exception: " << emsg << std::endl;
    CodeInsight::Get(isolate_).AccumulateException(try_catch);
    ExceptionInsight::Get(isolate_).AccumulateException(try_catch);
  }

  if (debugger_started_) {
    if (!agent_->IsStarted()) {
      agent_->Start(isolate_, platform_, src_path_.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    return DebugExecute("OnDelete", args, on_delete_args_count)
               ? kSuccess
               : kOnDeleteCallFail;
  }

  RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                        IsTerminatingRetriable, IsExecutionTerminating,
                        isolate_);

  v8::Handle<v8::Value> result;
  auto on_doc_delete = on_delete_.Get(isolate_);
  execute_start_time_ = Time::now();
  timed_out_ = false;
  UnwrapData(isolate_)->is_executing_ = true;
  if (!TO_LOCAL(on_doc_delete->Call(context, context->Global(),
                                    on_delete_args_count, args),
                &result)) {
    LOG(logError) << "Error running the on_doc_delete \n";
  }
  UnwrapData(isolate_)->is_executing_ = false;
  auto query_mgr = UnwrapData(isolate_)->query_mgr;
  query_mgr->ClearQueries();

  if (try_catch.HasCaught()) {
    // TODO: timed_out_ check this and send the timeout message
    LOG(logDebug) << "OnDelete Exception: "
                  << ExceptionString(isolate_, context, &try_catch, timed_out_)
                  << std::endl;
    UpdateHistogram(start_time);
    CodeInsight::Get(isolate_).AccumulateException(try_catch, timed_out_);
    ExceptionInsight::Get(isolate_).AccumulateException(try_catch, timed_out_);

    ++on_delete_failure;
    return kOnDeleteCallFail;
  }

  UpdateHistogram(start_time);
  on_delete_success++;
  return kSuccess;
}

int V8Worker::SendDeploy(const std::string &reason, const int64_t &delay) {
  const auto start_time = Time::now();

  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  v8::TryCatch try_catch(isolate_);

  v8::Local<v8::Value> args[1];

  v8::Local<v8::Object> action_object = v8::Object::New(isolate_);
  v8::Local<v8::String> reason_v8str_value = v8Str(isolate_, reason.c_str());
  v8::Local<v8::Number> delay_v8_value = v8::Number::New(isolate_, static_cast<double>(delay));

  bool success = false;
  if(!TO(action_object->Set(context, v8Str(isolate_, "reason"), reason_v8str_value), &success) || !success) {
    LOG(logError) << "Failed to add reason field in action object for OnDeploy" << std::endl;
    return kToLocalFailed;
  }

  if(!TO(action_object->Set(context, v8Str(isolate_, "delay"), delay_v8_value), &success) || !success) {
    LOG(logError) << "Failed to add delay field in action object for OnDeploy" << std::endl;
    return kToLocalFailed;
  }

  args[0] = action_object;

  if (on_deploy_.IsEmpty()) {
    LOG(logInfo) << "Content for OnDeploy handler is empty, returning success" << std::endl;
    return kSuccess;
  }

  if (try_catch.HasCaught()) {
    auto emsg = ExceptionString(isolate_, context, &try_catch);
    LOG(logError) << "OnDeploy Exception: " << emsg << std::endl;
    CodeInsight::Get(isolate_).AccumulateException(try_catch);
    ExceptionInsight::Get(isolate_).AccumulateException(try_catch);
  }

  if (debugger_started_) {
    if (!agent_->IsStarted()) {
      agent_->Start(isolate_, platform_, src_path_.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    return DebugExecute("OnDeploy", args, 1) ? kSuccess : kOnDeployCallFail;
  }

  v8::Handle<v8::Value> result;
  timed_out_ = false;
  auto on_deploy_fun = on_deploy_.Get(isolate_);
  execute_start_time_ = Time::now();
  UnwrapData(isolate_)->is_executing_ = true;
  if (!TO_LOCAL(on_deploy_fun->Call(context, context->Global(), 1, args),
                &result)) {
    LOG(logError) << "Error executing on_deploy_fun\n";
  }
  UnwrapData(isolate_)->is_executing_ = false;
  auto query_mgr = UnwrapData(isolate_)->query_mgr;
  query_mgr->ClearQueries();

  if (try_catch.HasCaught()) {
    auto emsg = ExceptionString(isolate_, context, &try_catch, false, timed_out_);
    LOG(logError) << "OnDeploy Exception: " << emsg << std::endl;
    UpdateHistogram(execute_start_time_);
    CodeInsight::Get(isolate_).AccumulateException(try_catch, false, timed_out_);
    ExceptionInsight::Get(isolate_).AccumulateException(try_catch, false, timed_out_);
    return kOnDeployCallFail;
  }

  UpdateHistogram(start_time);
  return kSuccess;
}

void V8Worker::SendTimer(std::string callback, std::string timer_ctx) {
  LOG(logTrace) << "Got timer event, context:" << RU(timer_ctx)
                << " callback:" << callback << std::endl;

  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);
  v8::TryCatch try_catch(isolate_);

  v8::Local<v8::Value> timer_ctx_val;
  v8::Local<v8::Value> arg[timer_callback_args_count];

  if (timer_ctx == "undefined") {
    arg[0] = v8::Undefined(isolate_);
  } else {
    if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, timer_ctx)),
                  &timer_ctx_val)) {
      return;
    }
    arg[0] = timer_ctx_val;
  }

  auto utils = UnwrapData(isolate_)->utils;
  auto callback_func_val = utils->GetPropertyFromGlobal(callback);
  if (!utils->IsFuncGlobal(callback_func_val)) {
    timer_callback_missing_counter++;
    return;
  }
  auto callback_func = callback_func_val.As<v8::Function>();
  v8::Handle<v8::Value> result;

  if (try_catch.HasCaught()) {
    auto emsg = ExceptionString(isolate_, context, &try_catch);
    LOG(logDebug) << "Timer callback Exception: " << emsg << std::endl;
    CodeInsight::Get(isolate_).AccumulateException(try_catch);
    ExceptionInsight::Get(isolate_).AccumulateException(try_catch);
  }

  if (debugger_started_) {
    if (!agent_->IsStarted()) {
      agent_->Start(isolate_, platform_, src_path_.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    DebugExecute(callback.c_str(), arg, timer_callback_args_count);
  }

  RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                        IsTerminatingRetriable, IsExecutionTerminating,
                        isolate_);
  timed_out_ = false;
  execute_start_time_ = Time::now();
  UnwrapData(isolate_)->is_executing_ = true;
  if (!TO_LOCAL(callback_func->Call(context, callback_func_val,
                                    timer_callback_args_count, arg),
                &result)) {
    LOG(logError) << "Error executing the callback function \n";
  }
  UnwrapData(isolate_)->is_executing_ = false;

  auto query_mgr = UnwrapData(isolate_)->query_mgr;
  query_mgr->ClearQueries();

  if (try_catch.HasCaught()) {
    UpdateHistogram(execute_start_time_);
    timer_callback_failure++;
    auto emsg = ExceptionString(isolate_, context, &try_catch, timed_out_);
    LOG(logDebug) << "Timer callback Exception: " << emsg << std::endl;
    CodeInsight::Get(isolate_).AccumulateException(try_catch, timed_out_);
    ExceptionInsight::Get(isolate_).AccumulateException(try_catch, timed_out_);

    return;
  }

  timer_callback_success++;
}

void V8Worker::StartDebugger() {
  if (debugger_started_) {
    LOG(logError) << "Debugger already started" << std::endl;
    return;
  }

  int port = 0;
  try {
    port = std::stoi(settings_->debugger_port);
  } catch (const std::exception &e) {
    LOG(logError) << "Invalid port : " << e.what() << std::endl;
    LOG(logWarning) << "Starting debugger with an ephemeral port" << std::endl;
  }

  LOG(logInfo) << "Starting debugger on port: " << RS(port) << std::endl;
  debugger_started_ = true;
  auto on_connect = [this](const std::string &url) -> void {
    auto comm = UnwrapData(isolate_)->comm;
    comm->WriteDebuggerURL(url);
  };

  std::string host_addr_ = "0.0.0.0";
  if (IsIPv6()) {
    host_addr_ = "::";
  }
  agent_ = new inspector::Agent(host_addr_, settings_->host_addr,
                                settings_->eventing_dir + "/" + app_name_ +
                                    "_frontend.url",
                                port, on_connect);
}

void V8Worker::StopDebugger() {
  if (!debugger_started_) {
    LOG(logError) << "Debugger wasn't started" << std::endl;
    return;
  }

  LOG(logInfo) << "Stopping Debugger" << std::endl;
  debugger_started_ = false;
  agent_->Stop();
  delete agent_;
}

void V8Worker::PushFront(std::unique_ptr<WorkerMessage> worker_msg) {
  LOG(logTrace) << "Inserting event at front: "
                << static_cast<int16_t>(worker_msg->header.event) << " opcode: "
                << static_cast<int16_t>(worker_msg->header.opcode)
                << " partition: " << worker_msg->header.partition
                << " metadata: " << RU(worker_msg->header.metadata)
                << std::endl;
  worker_queue_->PushFront(std::move(worker_msg));
}

void V8Worker::PushBack(std::unique_ptr<WorkerMessage> worker_msg) {
  LOG(logTrace) << "Inserting event at back: "
                << static_cast<int16_t>(worker_msg->header.event) << " opcode: "
                << static_cast<int16_t>(worker_msg->header.opcode)
                << " partition: " << worker_msg->header.partition
                << " metadata: " << RU(worker_msg->header.metadata)
                << std::endl;
  worker_queue_->PushBack(std::move(worker_msg));
}

CompilationInfo V8Worker::CompileHandler(std::string area_name,
                                         std::string handler) {
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  v8::TryCatch try_catch(isolate_);
  auto context = context_.Get(isolate_);

  v8::Context::Scope context_scope(context);

  auto script_name = v8Str(isolate_, area_name);
  v8::ScriptOrigin origin(isolate_, script_name);

  v8::Local<v8::Script> compiled_script;

  auto source = v8Str(isolate_, handler);
  if (!v8::Script::Compile(context, source, &origin)
           .ToLocal(&compiled_script)) {
    assert(try_catch.HasCaught());

    LOG(logError) << "Exception logged:"
                  << ExceptionString(isolate_, context, &try_catch)
                  << std::endl;
    return BuildCompileInfo(isolate_, context, &try_catch);
  }

  v8::Local<v8::Value> result;
  if (!compiled_script->Run(context).ToLocal(&result)) {
    assert(try_catch.HasCaught());
    LOG(logError) << "Exception logged:"
                  << ExceptionString(isolate_, context, &try_catch)
                  << std::endl;
    return BuildCompileInfo(isolate_, context, &try_catch);
  }

  CompilationInfo info;
  info.compile_success = true;
  info.description = "Compilation success";
  info.language = "Javascript";
  return info;
}

std::string V8Worker::Compile(std::string handler) {
  std::string header_code;
  int header_index_length = 0;
  for (const auto &header : handler_headers_) {
    header_code.append(header.c_str());
    header_index_length = header.length();
    header_code.append("\n");
  }
  CompilationInfo info = CompileHandler("handlerHeaders", header_code);

  if (!info.compile_success) {
    return CompileInfoToString(info);
  }

  std::string footer_code;
  for (const auto &footer : handler_footers_) {
    footer_code.append(footer.c_str());
    footer_code.append("\n");
  }
  info = CompileHandler("handlerFooters", footer_code);

  if (!info.compile_success) {
    return CompileInfoToString(info);
  }
  auto appCode = AddHeadersAndFooters(handler);
  info = CompileHandler("handlerCode", appCode);
  if (!info.compile_success) {
    info.index -= header_index_length;
    info.line_no -= handler_headers_.size();
  }

  return CompileInfoToString(info);
}

void V8Worker::GetBucketOpsMessages(std::vector<uv_buf_t> &messages) {
  for (int vb = 0; vb < num_vbuckets_; ++vb) {
    auto lock = GetAndLockVbLock(vb);
    auto seq = (*vb_seq_)[vb].get()->load(std::memory_order_seq_cst);
    if (seq > 0) {
      std::string seq_no = std::to_string(vb) + "::" + std::to_string(seq);
      auto curr_messages =
          BuildResponse(seq_no, resp_msg_type::mBucket_Ops_Response,
                        bucket_ops_response_opcode::checkpointResponse);
      for (auto &msg : curr_messages) {
        messages.push_back(msg);
      }
      // Reset the seq no of checkpointed vb to 0
      (*vb_seq_)[vb].get()->compare_exchange_strong(seq, 0);
    }
    lock.unlock();
  }
}

std::vector<uv_buf_t>
V8Worker::BuildResponse(const std::string &payload, resp_msg_type msg_type,
                        bucket_ops_response_opcode response_opcode) {
  std::vector<uv_buf_t> messages;
  flatbuffers::FlatBufferBuilder builder;
  auto msg_offset = builder.CreateString(payload);
  auto r = flatbuf::response::CreateResponse(
      builder, static_cast<int8_t>(msg_type),
      static_cast<int8_t>(response_opcode), msg_offset);
  builder.Finish(r);
  uint32_t length = builder.GetSize();

  char *header_buffer = new char[sizeof(uint32_t)];
  char *length_ptr = (char *)&length;
  std::copy(length_ptr, length_ptr + sizeof(uint32_t), header_buffer);
  messages.emplace_back(uv_buf_init(header_buffer, sizeof(uint32_t)));

  char *response = reinterpret_cast<char *>(builder.GetBufferPointer());
  char *msg = new char[length];
  std::copy(response, response + length, msg);
  messages.emplace_back(uv_buf_init(msg, length));

  return messages;
}

std::pair<std::optional<ParsedMetadata>, int>
V8Worker::ParseMetadata(const std::string &metadata) const {
  int skip_ack;
  return ParseMetadataWithAck(metadata, skip_ack, false);
}

std::pair<std::optional<ParsedMetadata>, int>
V8Worker::ParseMetadataWithAck(const std::string &metadata_str, int &skip_ack,
                               const bool ack_check) const {
  try {
    auto metadata = nlohmann::json::parse(metadata_str, nullptr, false);
    ParsedMetadata pmeta;
    if (metadata.contains("keyspace")) {
      pmeta.scope = metadata["keyspace"]["scope_name"].get<std::string>();
      pmeta.collection =
          metadata["keyspace"]["collection_name"].get<std::string>();
    }
    if (metadata.contains("cid")) {
      pmeta.cid = metadata["cid"].get<uint32_t>();
    }
    if (ack_check) {
      skip_ack = metadata["skip_ack"].get<int>();
    }
    if (metadata.contains("id")) {
      pmeta.key = metadata["id"].get<std::string>();
    }
    if (metadata.contains("cas")) {
      pmeta.cas = metadata["cas"].get<std::string>();
    }
    if (metadata.contains("rootcas")) {
      pmeta.rootcas = metadata["rootcas"].get<std::string>();
    }
    pmeta.vb = metadata["vb"].get<int>();
    pmeta.seq_num = metadata["seq"].get<uint64_t>();
    return {pmeta, kSuccess};
  } catch (nlohmann::json::parse_error &ex) {
  }
  return {std::nullopt, kJSONParseFailed};
}

void V8Worker::UpdateVbFilter(int vb_no, uint64_t seq_no) {
  auto lock = GetAndLockVbLock(vb_no);
  vbfilter_map_[vb_no].push_back(seq_no);
  lock.unlock();
}

void V8Worker::UpdateDeletedCid(const std::unique_ptr<WorkerMessage> &msg) {
  auto [parsed_meta, is_valid] = ParseWorkerMessage(msg);
  if (!is_valid || !parsed_meta.has_value()) {
    return;
  }

  std::lock_guard<std::mutex> guard(vbfilter_map_for_cid_lock_);
  auto lock = GetAndLockVbLock(parsed_meta->vb);
  vbfilter_map_for_cid_[parsed_meta->cid][parsed_meta->vb] =
      parsed_meta->seq_num;
  lock.unlock();
}

uint64_t V8Worker::GetVbFilter(int vb_no) {
  auto lock = GetAndLockVbLock(vb_no);
  auto &filters = vbfilter_map_[vb_no];
  lock.unlock();
  if (filters.empty())
    return 0;
  return filters.front();
}

void V8Worker::EraseVbFilter(int vb_no) {
  auto lock = GetAndLockVbLock(vb_no);
  auto &filters = vbfilter_map_[vb_no];
  lock.unlock();
  if (!filters.empty()) {
    filters.erase(filters.begin());
  }
}

void V8Worker::UpdateBucketopsSeqnoLocked(int vb_no, uint64_t seq_no) {
  auto lock = GetAndLockVbLock(vb_no);
  (*processed_bucketops_)[vb_no] = seq_no;
  lock.unlock();
}

void V8Worker::RemoveTimerPartition(int vb_no) {
  if (timer_store_) {
    timer_store_->RemovePartition(vb_no);
  }
}

void V8Worker::AddTimerPartition(int vb_no) {
  if (timer_store_) {
    timer_store_->AddPartition(vb_no);
  }
}

uint64_t V8Worker::GetBucketopsSeqno(int vb_no) {
  auto lock = GetAndLockVbLock(vb_no);
  // Reset the seq no of checkpointed vb to 0
  (*vb_seq_)[vb_no]->store(0, std::memory_order_seq_cst);
  auto return_val = (*processed_bucketops_)[vb_no];
  lock.unlock();
  return return_val;
}

std::unique_lock<std::mutex> V8Worker::GetAndLockVbLock(int vb_no) {
  return std::unique_lock<std::mutex>(*(*vb_locks_)[vb_no]);
}

void V8Worker::SetThreadExitFlag() {
  thread_exit_cond_.store(true);
  worker_queue_->Close();
}

// Must be called only in destructor
void V8Worker::FreeCurlBindings() {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  auto utils = UnwrapData(isolate_)->utils;

  for (const auto &binding : curl_binding_values_) {
    auto binding_val = utils->GetPropertyFromGlobal(binding);
    auto info = CurlBinding::GetCurlInstance(isolate_, context, binding_val);
    if (info.is_fatal) {
      continue;
    }

    delete info.curl;
  }
}

CodeInsight &V8Worker::GetInsight() {
  auto &insight = CodeInsight::Get(isolate_);
  return insight;
}

ExceptionInsight &V8Worker::GetExceptionInsight() {
  auto &exception_insight = ExceptionInsight::Get(isolate_);
  return exception_insight;
}

// Watches the duration of the event and terminates its execution if it goes
// beyond max_task_duration_
void V8Worker::TaskDurationWatcher() {
  if (debugger_started_) {
    return;
  }

  while (!shutdown_terminator_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    if (!UnwrapData(isolate_)->is_executing_) {
      continue;
    }

    Time::time_point t = Time::now();
    auto duration =
        std::chrono::duration_cast<nsecs>(t - execute_start_time_).count();

    LOG(logTrace) << "ns.count(): " << duration
                  << "ns, max_task_duration: " << max_task_duration_ << "ns"
                  << std::endl;

    if (duration <= max_task_duration_) {
      continue;
    }

    LOG(logInfo) << "Task took: " << duration
                 << "ns, trying to obtain lock to terminate its execution"
                 << std::endl;

    {
      // By holding this lock here, we ensure that the execution control is in
      // the realm of JavaScript and therefore, the call to
      // V8::TerminateExecution done below succeeds
      std::lock_guard<std::mutex> guard(
          UnwrapData(isolate_)->termination_lock_);

      timed_out_ = true;
      timeout_count++;
      isolate_->TerminateExecution();
      UnwrapData(isolate_)->is_executing_ = false;
    }

    LOG(logInfo) << "Task took: " << duration << "ns, terminated its execution"
                 << std::endl;
  }
}

void V8Worker::UpdatePartitions(const std::unordered_set<int64_t> &vbuckets) {
  partitions_ = vbuckets;
}

std::unordered_set<int64_t> V8Worker::GetPartitions() const {
  return partitions_;
}

lcb_STATUS V8Worker::SetTimer(timer::TimerInfo &tinfo) {
  if (timer_store_)
    return timer_store_->SetTimer(tinfo, data_.lcb_retry_count,
                                  data_.op_timeout);
  return LCB_SUCCESS;
}

lcb_STATUS V8Worker::DelTimer(timer::TimerInfo &tinfo) {
  if (timer_store_)
    return timer_store_->DelTimer(tinfo, data_.lcb_retry_count,
                                  data_.op_timeout);
  return LCB_SUCCESS;
}

lcb_INSTANCE *V8Worker::GetTimerLcbHandle() const {
  return timer_store_->GetTimerStoreHandle();
}

std::unique_lock<std::mutex> V8Worker::GetAndLockBucketOpsLock() {
  return std::unique_lock<std::mutex>(bucketops_lock_);
}

void V8Worker::StopTimerScan() { stop_timer_scan_.store(true); }

void V8Worker::SetFailFastTimerScans() {
  if (timer_store_ != nullptr) {
    timer_store_->SetFailFastTimerScans();
  }
}

void V8Worker::ResetFailFastTimerScans() {
  if (timer_store_ != nullptr) {
    timer_store_->ResetFailFastTimerScans();
  }
}

// TODO : Remove this when stats variables are handled properly
void AddLcbException(const IsolateData *isolate_data, const int code) {
  auto w = isolate_data->v8worker;
  w->AddLcbException(code);
}

void AddLcbException(const IsolateData *isolate_data, lcb_STATUS error) {
  auto w = isolate_data->v8worker;
  w->AddLcbException(static_cast<int>(error));
}

std::string GetFunctionInstanceID(v8::Isolate *isolate) {
  auto w = UnwrapData(isolate)->v8worker;
  return w->GetFunctionInstanceID();
}

void UpdateCurlLatencyHistogram(
    v8::Isolate *isolate,
    const std::chrono::high_resolution_clock::time_point &start) {
  auto w = UnwrapData(isolate)->v8worker;
  w->UpdateCurlLatencyHistogram(start);
}

void V8Worker::UpdateV8HeapSize() {
  v8::HeapStatistics stats;
  v8::Locker locker(isolate_);
  isolate_->GetHeapStatistics(&stats);
  v8_heap_size_ = stats.total_heap_size();
}

void V8Worker::ForceRunGarbageCollector() {
  v8::Locker locker(isolate_);
  isolate_->LowMemoryNotification();
}

std::string V8Worker::AddHeadersAndFooters(std::string code) {
  std::string final_code;
  for (const auto &header_code : handler_headers_) {
    final_code.append(header_code.c_str());
    final_code.append("\n");
  }
  final_code.append(code.c_str());
  final_code.append("\n");

  for (const auto &footer_code : handler_footers_) {
    final_code.append(footer_code.c_str());
    final_code.append("\n");
  }

  return final_code;
}

void V8Worker::SetFeatureList(uint32_t feature_matrix) {
  data_.feature_matrix = feature_matrix;
}
