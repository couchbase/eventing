// Copyright (c) 201.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

#include <sstream>

#include "curl.h"
#include "exceptioninsight.h"
#include "insight.h"
#include "lang_compat.h"
#include "query-helper.h"
#include "query-iterable.h"
#include "query-mgr.h"
#include "retry_util.h"
#include "utils.h"

#include "bucket.h"
#include "bucket_ops.h"
#include "isolate_data.h"
#include "lcb_utils.h"
#include "parse_deployment.h"
#include "query-helper.h"
#include "query-iterable.h"
#include "query-mgr.h"
#include "timer.h"
#include "v8log.h"
#include "v8worker2.h"

#include "../gen/flatbuf/header_v2_generated.h"

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
bool V8Worker2::DebugExecute(const char *func_name, v8::Local<v8::Value> *args,
                             int args_len) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  v8::TryCatch try_catch(isolate_);

  // Need to construct origin for source-map to apply
  auto origin_v8_str = v8Str(isolate_, app_details_->app_instance_id + ".js");
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

  auto source = v8Str(isolate_, app_details_->app_code);
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

void V8Worker2::SetCouchbaseNamespace() {
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
    LOG(logError) << log_prefix_
                  << "Failed to set the global namespace couchbase"
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
        log("cache expired item: ", couchbase.max_expiry)
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

  wrapper_function2.append(
      std::to_string(app_details_->settings->bucket_cache_age));
  wrapper_function2.append(";couchbase.max_size=");
  wrapper_function2.append(
      std::to_string(app_details_->settings->bucket_cache_size));

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

v8::Local<v8::ObjectTemplate> V8Worker2::NewGlobalObj() const {
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

void V8Worker2::InstallCurlBindings(
    const std::vector<CurlBinding> &curl_bindings) const {
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  for (const auto &binding : curl_bindings) {
    binding.InstallBinding(isolate_, context);
  }
}

void V8Worker2::InstallConstantBindings(
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

void V8Worker2::InitializeIsolateData() {
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  data_.v8worker2 = this;
  data_.js_exception = new JsException(isolate_);
  data_.instance_id = app_details_->app_instance_id;

  auto key = GetLocalKey();
  data_.utils = new Utils(isolate_, context, cluster_details_->cert_file_);
  data_.comm = new Communicator(
      cluster_details_->local_address_, cluster_details_->eventing_port_,
      key.first, key.second, false, app_details_->location->app_name,
      app_details_->location->bucket_name, app_details_->location->scope_name,
      isolate_);

  data_.timer = new Timer(isolate_, context, get_timer_reduction_ratio());

  // TODO : Need to make HEAD call to all the bindings to establish TCP
  // Connections
  data_.curl_factory = new CurlFactory(isolate_, context);
  data_.req_builder = new CurlRequestBuilder(isolate_, context);
  data_.resp_builder = new CurlResponseBuilder(
      isolate_, context, app_details_->settings->curl_max_allowed_resp_size);
  data_.custom_error = new CustomError(isolate_, context);
  data_.curl_codex = new CurlCodex;
  data_.query_mgr = new Query::Manager(
      isolate_, cb_source_bucket_,
      static_cast<std::size_t>(app_details_->settings->lcb_inst_capacity),
      user_, domain_);
  data_.query_iterable = new Query::Iterable(isolate_, context);
  data_.query_iterable_impl = new Query::IterableImpl(isolate_, context);
  data_.query_iterable_result = new Query::IterableResult(isolate_, context);
  data_.query_helper = new Query::Helper(isolate_, context);

  // execution_timeout is in seconds
  // n1ql_timeout is expected in micro seconds
  // Setting a lower timeout to allow adequate time for the exception
  // to get thrown
  data_.n1ql_timeout = static_cast<lcb_U32>(
      app_details_->settings->timeout < 3
          ? 500000
          : (app_details_->settings->timeout - 2) * 1000000);
  data_.op_timeout = app_details_->settings->timeout < 5
                         ? app_details_->settings->timeout
                         : app_details_->settings->timeout - 2;
  data_.n1ql_consistency = Query::Helper::GetN1qlConsistency(
      app_details_->settings->n1ql_consistency);
  data_.n1ql_prepare_all = app_details_->settings->n1ql_prepare_all;
  data_.analytics_timeout = static_cast<lcb_U32>(
      app_details_->settings->timeout < 3
          ? 500000
          : (app_details_->settings->timeout - 2) * 1000000);
  data_.lang_compat =
      new LanguageCompatibility(app_details_->settings->lang_compatibility);
  data_.lcb_retry_count = app_details_->settings->lcb_retry_count;
  data_.lcb_timeout =
      ConvertSecondsToMicroSeconds(app_details_->settings->lcb_timeout);
  data_.insight_line_offset = app_details_->settings->handler_headers.size();

  data_.bucket_ops = new BucketOps(isolate_, context);
  data_.feature_matrix = global_settings_->feature_matrix_.load();
  data_.timer_context_size = app_details_->settings->timer_context_size;
}

void V8Worker2::InstallBucketBindings(
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
        // TODO: Fix user_ and domain_
        bucket_alias, bucket_access == "r", is_source_bucket, user_, domain_);
  }
}

void V8Worker2::CheckAndCreateTracker() {
  if (!app_details_->settings->cursor_aware) {
    return;
  }
  auto fiid_ = GetFunctionInstanceID();
  checkpoint_writer_ = std::make_unique<CheckpointWriter>(
      isolate_, fiid_, cb_source_bucket_, cb_source_scope_,
      cb_source_collection_);
  checkpoint_writer_->Connect();
  tracker_enabled_ = true;
}

void V8Worker2::InitializeCurlBindingValues(
    const std::vector<CurlBinding> &curl_bindings) {
  for (const auto &curl_binding : curl_bindings) {
    curl_binding_values_.emplace_back(curl_binding.value);
  }
}

V8Worker2::V8Worker2(int index, std::shared_ptr<RuntimeStats> stats,
                     std::shared_ptr<communicator> comm, v8::Platform *platform,
                     std::shared_ptr<settings::cluster> cluster_details,
                     std::shared_ptr<settings::global> global_Settings,
                     std::shared_ptr<settings::app_details> app_details)
    : stats_(stats), comm_(comm), platform_(platform),
      cluster_details_(cluster_details), global_settings_(global_Settings),
      app_details_(app_details),
      exception_type_names_(
          {"KVError", "N1QLError", "EventingError", "CurlError", "TypeError"}) {
  cb_source_bucket_.assign(app_details->dConfig->source_bucket);
  cb_source_scope_.assign(app_details->dConfig->source_scope);
  cb_source_collection_.assign(app_details->dConfig->source_collection);

  log_prefix_.assign("[" + app_details_->location->bucket_name + "/" +
                     app_details_->location->scope_name + "/" +
                     app_details_->location->app_name + ":" +
                     std::to_string(index) + "]");
  user_ = app_details_->app_owner->user_;
  domain_ = app_details_->app_owner->domain_;
  num_vbuckets_ = app_details_->num_vbuckets;
  stop_timer_scan_.store(false);
  timer_execution_.store(false);
  scan_timer_.store(false);
  update_v8_heap_.store(false);
  mem_check_.store(false);

  v8_heap_size_ = 0;
  std::ostringstream oss;
  oss << "\"" << app_details_->app_instance_id << "\"";
  function_instance_id_.assign(oss.str());

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

  lcb_logger_create(&evt_logger.base, &evt_logger);
  lcb_logger_callback(evt_logger.base, evt_log_handler);

  v8::Context::Scope context_scope(context);

  SetCouchbaseNamespace();
  InitializeIsolateData();
  InstallCurlBindings(app_details->dConfig->curl_bindings);
  InstallConstantBindings(app_details->dConfig->constant_bindings);
  InitializeCurlBindingValues(app_details->dConfig->curl_bindings);
  CheckAndCreateTracker();

  bucket_factory_ = std::make_shared<BucketFactory>(isolate_, context);
  InstallBucketBindings(app_details->dConfig->component_configs);
  for (auto &binding : bucket_bindings_) {
    auto error = binding.InstallBinding(isolate_, context);
    if (error != nullptr) {
      // TODO: Return appropriate error or lazy retry during runtime of function
      LOG(logError) << log_prefix_
                    << "Unable to install bucket binding, err : " << *error
                    << std::endl;
    }
  }

  if (app_details_->usingTimer) {
    auto prefix = "eventing::" + std::to_string(app_details_->app_id);
    timer_store_ = new timer::TimerStore(
        isolate_, prefix, app_details->dConfig->metadata_bucket,
        app_details->dConfig->metadata_scope,
        app_details->dConfig->metadata_collection, app_details_->num_vbuckets,
        get_timer_reduction_ratio(), user_, domain_);
  }

  max_task_duration_ = SECS_TO_NS * app_details_->settings->timeout;

  f_map_ = std::unique_ptr<vb_handler>(new vb_handler);
  worker_queue_ =
      new BlockingDeque<std::unique_ptr<messages::worker_request>>();
}

V8Worker2::V8Worker2(v8::Platform *platform) {
  v8::Isolate::CreateParams create_params;
  create_params.array_buffer_allocator =
      v8::ArrayBuffer::Allocator::NewDefaultAllocator();

  isolate_ = v8::Isolate::New(create_params);

  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto global = NewGlobalObj();
  auto context = v8::Context::New(isolate_, nullptr, global);
  context_.Reset(isolate_, context);
}

int V8Worker2::Start() {
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  for (const auto &type_name : exception_type_names_) {
    DeriveFromError(isolate_, context, type_name);
  }

  auto source = v8Str(isolate_, app_details_->app_code);
  if (!ExecuteScript(source)) {
    return kFailedToCompileJs;
  }

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
    LOG(logError) << log_prefix_ << "Exception logged:"
                  << ExceptionString(isolate_, context, &try_catch)
                  << std::endl;
  }

  v8::Local<v8::Value> result_wrapper;
  if (!TO_LOCAL(compiled_script->Run(context), &result_wrapper)) {
    LOG(logError) << log_prefix_ << "Compile error" << std::endl;
  }

  std::thread r_thr(&V8Worker2::RouteMessage, this);
  processing_thr_ = std::move(r_thr);

  // Spawning terminator thread to monitor the wall clock time for execution
  // of javascript code isn't going beyond max_task_duration
  terminator_thr_ = new std::thread(&V8Worker2::TaskDurationWatcher, this);
  return kSuccess;
}

V8Worker2::~V8Worker2() {}

void V8Worker2::RouteMessage() {
  std::string val, context, callback;
  while (true) {
    std::unique_ptr<messages::worker_request> msg;
    if (!worker_queue_->PopFront(msg)) {
      continue;
    }

    switch (msg->event) {
    case messages::eDcpEvent: {
      auto size = msg->GetSize();
      add_unacked_bytes(size);

      switch (msg->opcode) {
      case messages::eDcpMutation:
        HandleMutationEvent(msg);
        break;

      case messages::eDcpDeletion:
        HandleDeleteEvent(msg);
        break;

      case messages::eDcpNoOp:
        HandleNoOpEvent(msg);
        break;

      case messages::eDcpCollectionDelete:
        HandleCollectionDeleteEvent(msg);
        break;
      }
      stats_->IncrementExecutionStat("num_processed_events");
    } break;

    case messages::eInternal:
      switch (msg->opcode) {
      case messages::eScanTimer: {
        auto iter = timer_store_->GetIterator();
        timer::TimerEvent evt;

        timer_execution_.store(true);
        while (!stop_timer_scan_.load() && iter.GetNext(evt)) {
          stats_->IncrementExecutionStat("timer_msg_counter");
          this->SendTimer(evt.callback, evt.context);
          timer_store_->DeleteTimer(evt);
        }

        if (stop_timer_scan_.load()) {
          timer_store_->SyncSpan();
        }

        timer_execution_.store(false);
        scan_timer_.store(false);
        break;
      }

      case messages::eUpdateV8HeapSize: {
        v8::HeapStatistics stats;
        v8::Locker locker(isolate_);
        isolate_->GetHeapStatistics(&stats);
        v8_heap_size_ = stats.total_heap_size();
        update_v8_heap_.store(false);
        break;
      }

      case messages::eMemCheck: {
        v8::Locker locker(isolate_);
        isolate_->LowMemoryNotification();
        mem_check_.store(false);
        break;
      }

      case messages::eDeleteFilter:
        HandleDeleteVbFilter(msg);
        break;
      }
      break;

    case messages::eLifeCycleChange:
      switch (msg->opcode) {
      case messages::eDestroy: {
        shutdown_terminator_ = true;
        // TODO: Cleanup all the things
        LOG(logInfo) << log_prefix_ << " closing all resources " << std::endl;
        return;
      } break;
      }
      break;

    case messages::eVbSettings: {
      switch (msg->opcode) {
      case messages::eFilterVb: {
        std::ostringstream ss;
        ss << msg->payload[0] << msg->payload[1];
        ss << msg->payload[2] << msg->payload[3] << msg->payload[4]
           << msg->payload[5] << msg->payload[6] << msg->payload[7]
           << msg->payload[8] << msg->payload[9];
        ss << msg->payload[10] << msg->payload[11] << msg->payload[12]
           << msg->payload[13] << msg->payload[14] << msg->payload[15]
           << msg->payload[16] << msg->payload[17];

        flatbuffers::FlatBufferBuilder builder;
        auto msg_offset = builder.CreateString(ss.str());
        auto r = flatbuf::header_v2::CreateHeaderV2(builder, msg_offset, 0, 0);
        builder.Finish(r);

        uint32_t size = builder.GetSize();
        std::string sMsg((const char *)builder.GetBufferPointer(), size);

        uint64_t meta = messages::getMetadata(msg, size);
        LOG(logInfo) << log_prefix_ << " done execution. Sending ack message for vb " << std::endl;
        comm_->send_message(meta, msg->identifier, sMsg.c_str(), size);
      } break;

      case messages::eDeleteFilter: {
        HandleDeleteVbFilter(msg);
      }
      }
    } break;

    case messages::eInitEvent: {
      switch (msg->opcode) {
      case messages::eOnDeployHandler:
        HandleOnDeployEvent(msg);
        break;

      case messages::eDebugHandlerStart: {
        int port = 0;
        try {
          port = std::stoi(cluster_details_->debugger_port_);
        } catch (const std::exception &e) {
          LOG(logError) << log_prefix_ << "Invalid port : " << e.what()
                        << std::endl;
          break;
        }

        LOG(logInfo) << log_prefix_ << "Starting debugger on port: " << RS(port)
                     << std::endl;
        auto on_connect = [this](const std::string &url) -> void {
          auto comm = UnwrapData(isolate_)->comm;
          comm->WriteDebuggerURL(url);
        };

        auto payload = messages::get_payload_msg(msg->payload);
        auto value = nlohmann::json::parse(payload.value, nullptr, false);
        auto given_address = value["host_addr"].get<std::string>();
        auto isIpv4 = value["isIpv4"].get<bool>();
        auto dir = value["dir"].get<std::string>();
        std::string host_addr_ = "0.0.0.0";
        if (!isIpv4) {
          host_addr_ = "::";
        }
        agent_ = new inspector::Agent(
            host_addr_, given_address,
            dir + "/" + app_details_->app_instance_id + "_frontend.url", port,
            on_connect);
        debugger_started_ = true;
      } break;

      case messages::eDebugHandlerStop:
        if (debugger_started_) {
          agent_->Stop();
          delete agent_;
        }
        debugger_started_ = false;
      }
    } break;

    case messages::eHandlerDynamicSettings: {
      switch (msg->opcode) {
      case messages::eLogLevelChange:
        break;

      case messages::eTimeContextSize:
        data_.timer_context_size = app_details_->settings->timer_context_size;
        break;
      }
    } break;

    case messages::eGlobalConfigChange:
      switch (msg->opcode) {
      case messages::eFeatureMatrix: {
        data_.feature_matrix = global_settings_->feature_matrix_.load();
        break;
      }
      }
      break;

    default:
      break;
    }
  }
}

bool V8Worker2::ExecuteScript(const v8::Local<v8::String> &script) {
  v8::HandleScope handle_scope(isolate_);
  v8::TryCatch try_catch(isolate_);

  auto context = context_.Get(isolate_);
  auto script_name = v8Str(isolate_, app_details_->app_instance_id + ".js");
  v8::ScriptOrigin origin(isolate_, script_name);

  v8::Local<v8::Script> compiled_script;
  if (!v8::Script::Compile(context, script, &origin)
           .ToLocal(&compiled_script)) {
    assert(try_catch.HasCaught());
    LOG(logError) << log_prefix_ << "Exception logged:"
                  << ExceptionString(isolate_, context, &try_catch)
                  << std::endl;
    // The script failed to compile; bail out.
    return false;
  }

  v8::Local<v8::Value> result;
  if (!compiled_script->Run(context).ToLocal(&result)) {
    assert(try_catch.HasCaught());
    LOG(logError) << log_prefix_ << "Exception logged:"
                  << ExceptionString(isolate_, context, &try_catch)
                  << std::endl;
    // Running the script failed; bail out.
    return false;
  }

  return true;
}

void V8Worker2::HandleDeleteEvent(
    const std::unique_ptr<messages::worker_request> &msg) {
  auto payload = messages::get_payload_msg(msg->payload);
  auto meta_info = messages::get_extras_dcp(msg->identifier, payload.extras);

  stats_->IncrementExecutionStat("dcp_delete_msg_counter");
  auto filter_event = f_map_->CheckAndUpdateFilter(
      meta_info.cid, meta_info.vb, meta_info.vbuuid, meta_info.seq, false);
  if (filter_event) {
    stats_->IncrementExecutionStat("filtered_dcp_delete_counter");
    return;
  }

  stats_->IncrementExecutionStat("messages_parsed");
  SendDelete(payload);
  add_dcp_mutation_done();
  f_map_->reset_current_vb();

  if (debugger_started_) {
    comm_->send_message(msg, "", "", "");
  }
}

void V8Worker2::HandleMutationEvent(
    const std::unique_ptr<messages::worker_request> &msg) {
  auto payload = messages::get_payload_msg(msg->payload);
  auto meta_info = messages::get_extras_dcp(msg->identifier, payload.extras);
  stats_->IncrementExecutionStat("dcp_mutation_msg_counter");

  auto filter_event = f_map_->CheckAndUpdateFilter(
      meta_info.cid, meta_info.vb, meta_info.vbuuid, meta_info.seq, false);
  if (filter_event) {
    stats_->IncrementExecutionStat("filtered_dcp_mutation_counter");
    return;
  }

  if (tracker_enabled_) {
    auto [client_err, err_code] = checkpoint_writer_->Write(
        MetaData(meta_info.scope_name, meta_info.collection_name, meta_info.key,
                 meta_info.cas),
        meta_info.root_cas, meta_info.stale_cursors);
    if (err_code) {
      if (err_code == LCB_ERR_CAS_MISMATCH) {
        stats_->IncrementExecutionStat("dcp_mutation_checkpoint_cas_mismatch");
      } else {
        stats_->IncrementExecutionStat("dcp_mutation_checkpoint_failure");
        auto err_cstr = lcb_strerror_short(err_code);
        if (client_err.length() > 0) {
          err_cstr = client_err.c_str();
        }
        // TODO : Handle scope deletion, collection deletion, document
        // deletion
        APPLOG << "cursor progression failed for document: "
               << meta_info.scope_name << "/" << meta_info.collection_name
               << "/" << meta_info.key << " rootcas: " << meta_info.root_cas
               << " error: " << err_cstr << std::endl;
      }
    }
    return;
  }
  stats_->IncrementExecutionStat("messages_parsed");
  SendUpdate(payload, meta_info.expiry, meta_info.datatype);
  add_dcp_mutation_done();
  f_map_->reset_current_vb();

  if (debugger_started_) {
    comm_->send_message(msg, "", "", "");
  }
}

void V8Worker2::HandleOnDeployEvent(
    const std::unique_ptr<messages::worker_request> &msg) {
  auto value = messages::get_value(msg->payload);
  auto action_object = nlohmann::json::parse(value, nullptr, false);
  int return_code = SendDeploy(action_object["reason"], action_object["delay"]);
  auto result = GetOnDeployResult(return_code);
  comm_->send_message(msg, "", "", result);
}

void V8Worker2::HandleNoOpEvent(
    const std::unique_ptr<messages::worker_request> &msg) {
  auto meta_info = messages::get_extras_dcp(msg->identifier, "");

  stats_->IncrementExecutionStat("no_op_counter");
  f_map_->CheckAndUpdateFilter(meta_info.cid, meta_info.vb, meta_info.vbuuid,
                               meta_info.seq, true);
}

void V8Worker2::HandleCollectionDeleteEvent(
    const std::unique_ptr<messages::worker_request> &msg) {
  auto meta_info = messages::get_extras_dcp(msg->identifier, "");
  f_map_->RemoveCidFilter(meta_info.cid);
}

void V8Worker2::HandleDeleteVbFilter(
    const std::unique_ptr<messages::worker_request> &msg) {
  auto extras = messages::get_extras(msg->payload);
  auto vb = (uint16_t) static_cast<unsigned char>(extras[0]) << 8 |
            (uint16_t) static_cast<unsigned char>(extras[1]);
  f_map_->RemoveFilterEvent(vb);
}

int V8Worker2::SendUpdate(const messages::payload payload, uint32_t expiry,
                          uint8_t datatype) {
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);
  v8::TryCatch try_catch(isolate_);

  if (on_update_.IsEmpty()) {
    return kOnUpdateCallFail;
  }

  v8::Local<v8::Value> args[on_update_args_count];
  switch (datatype) {
  case messages::tJson: {
    if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, payload.value)),
                  &args[0])) {
      LOG(logError) << log_prefix_ << "Value parse error: " << payload.value
                    << std::endl;
      return kToLocalFailed;
    }
  } break;
  case messages::tRaw: {
    auto utils = UnwrapData(isolate_)->utils;
    args[0] =
        utils->ToArrayBuffer(payload.value.c_str(), payload.value.length());
  } break;

  default:
    LOG(logError) << log_prefix_ << "Unknown datatype passed " << payload.value
                  << " dataType: " << datatype << std::endl;
    return kToLocalFailed;
  }

  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, payload.meta)),
                &args[1])) {
    LOG(logError) << log_prefix_ << "Meta parse error" << payload.meta
                  << std::endl;
    return kToLocalFailed;
  }

  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, payload.xattr)),
                &args[2])) {
    LOG(logError) << log_prefix_ << "xattr parse error" << payload.xattr
                  << std::endl;
    return kToLocalFailed;
  }

  if (expiry != 0) {
    uint64_t expiryInMiliseconds = uint64_t(expiry) * 1000;
    auto js_meta = args[1].As<v8::Object>();
    auto js_expiry =
        v8::Date::New(context, expiryInMiliseconds).ToLocalChecked();
    auto r = js_meta->Set(context, v8Str(isolate_, "expiry_date"), js_expiry);
    if (!r.FromMaybe(true)) {
      LOG(logWarning) << log_prefix_
                      << "Error Creating expiry_date failed in OnUpdate"
                      << std::endl;
    }
  }

  if (debugger_started_) {
    if (!agent_->IsStarted()) {
      auto src_path = app_details_->app_instance_id + ".js";
      agent_->Start(isolate_, platform_, src_path.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    return DebugExecute("OnUpdate", args, on_update_args_count)
               ? kSuccess
               : kOnUpdateCallFail;
  }

  UnwrapData(isolate_)->termination_lock_.lock();
  UnwrapData(isolate_)->is_executing_ = true;
  execute_start_time_ = Time::now();
  UnwrapData(isolate_)->termination_lock_.unlock();

  v8::Handle<v8::Value> result;
  auto on_doc_update = on_update_.Get(isolate_);
  if (!TO_LOCAL(on_doc_update->Call(context, context->Global(),
                                    on_update_args_count, args),
                &result)) {
    LOG(logError) << log_prefix_ << "Error executing OnUpdate" << std::endl;
  }
  UnwrapData(isolate_)->termination_lock_.lock();
  stats_->UpdateHistogram(execute_start_time_);
  execute_start_time_ = DefaultTime;
  UnwrapData(isolate_)->is_executing_ = false;
  UnwrapData(isolate_)->termination_lock_.unlock();

  auto query_mgr = UnwrapData(isolate_)->query_mgr;
  query_mgr->ClearQueries();

  if (try_catch.HasCaught()) {
    stats_->IncrementExecutionStat("on_update_failure");
    stats_->AddException(isolate_, try_catch);
    return kOnUpdateCallFail;
  }

  stats_->IncrementExecutionStat("on_update_success");
  return kSuccess;
}

int V8Worker2::SendDelete(const messages::payload payload) {
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  v8::TryCatch try_catch(isolate_);

  v8::Local<v8::Value> args[on_delete_args_count];
  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, payload.meta)),
                &args[0])) {
    return kToLocalFailed;
  }

  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, payload.value)),
                &args[1])) {
    return kToLocalFailed;
  }

  if (on_delete_.IsEmpty()) {
    return kOnDeleteCallFail;
  }

  if (debugger_started_) {
    if (!agent_->IsStarted()) {
      auto src_path = app_details_->app_instance_id + ".js";
      agent_->Start(isolate_, platform_, src_path.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    return DebugExecute("OnDelete", args, on_delete_args_count)
               ? kSuccess
               : kOnDeleteCallFail;
  }

  UnwrapData(isolate_)->termination_lock_.lock();
  UnwrapData(isolate_)->is_executing_ = true;
  execute_start_time_ = Time::now();
  UnwrapData(isolate_)->termination_lock_.unlock();

  v8::Handle<v8::Value> result;
  auto on_doc_delete = on_delete_.Get(isolate_);
  if (!TO_LOCAL(on_doc_delete->Call(context, context->Global(),
                                    on_delete_args_count, args),
                &result)) {
    LOG(logError) << log_prefix_ << " Error executing OnDelete" << std::endl;
  }

  UnwrapData(isolate_)->termination_lock_.lock();
  stats_->UpdateHistogram(execute_start_time_);
  execute_start_time_ = DefaultTime;
  UnwrapData(isolate_)->is_executing_ = false;
  UnwrapData(isolate_)->termination_lock_.unlock();

  auto query_mgr = UnwrapData(isolate_)->query_mgr;
  query_mgr->ClearQueries();

  if (try_catch.HasCaught()) {
    stats_->IncrementExecutionStat("on_delete_failure");
    stats_->AddException(isolate_, try_catch);
    return kOnDeleteCallFail;
  }

  stats_->IncrementExecutionStat("on_delete_success");
  return kSuccess;
}

int V8Worker2::SendDeploy(const std::string &reason, const int64_t &delay) {
    v8::Locker locker(isolate_);
    v8::Isolate::Scope isolate_scope(isolate_);
    v8::HandleScope handle_scope(isolate_);

    auto context = context_.Get(isolate_);
    v8::Context::Scope context_scope(context);
    v8::TryCatch try_catch(isolate_);

    if (on_deploy_.IsEmpty()) {
        LOG(logError) << "OnDeploy handler is not defined" << std::endl;
        return kOnDeployCallFail;
    }

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

    UnwrapData(isolate_)->termination_lock_.lock();
    UnwrapData(isolate_)->is_executing_ = true;
    execute_start_time_ = Time::now();
    UnwrapData(isolate_)->termination_lock_.unlock();

    v8::Handle<v8::Value> result;
    auto on_deploy_fun = on_deploy_.Get(isolate_);
    if (!TO_LOCAL(on_deploy_fun->Call(context, context->Global(), 1, args),
                  &result)) {
        LOG(logError) << "Error executing OnDeploy" << std::endl;
    }

    UnwrapData(isolate_)->termination_lock_.lock();
    stats_->UpdateHistogram(execute_start_time_);
    execute_start_time_ = DefaultTime;
    UnwrapData(isolate_)->is_executing_ = false;
    UnwrapData(isolate_)->termination_lock_.unlock();

    auto query_mgr = UnwrapData(isolate_)->query_mgr;
    query_mgr->ClearQueries();

    if (try_catch.HasCaught()) {
        stats_->AddException(isolate_, try_catch);
        return kOnDeployCallFail;
    }

    return kSuccess;
}

void V8Worker2::SendTimer(std::string callback, std::string timer_ctx) {
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
    stats_->IncrementFailureStat("timer_callback_missing_counter");
    return;
  }

  UnwrapData(isolate_)->termination_lock_.lock();
  UnwrapData(isolate_)->is_executing_ = true;
  execute_start_time_ = Time::now();
  UnwrapData(isolate_)->termination_lock_.unlock();

  auto callback_func = callback_func_val.As<v8::Function>();
  v8::Handle<v8::Value> result;

  if (!TO_LOCAL(callback_func->Call(context, callback_func_val,
                                    timer_callback_args_count, arg),
                &result)) {
    LOG(logError) << log_prefix_ << " Error executing the callback function \n";
  }

  UnwrapData(isolate_)->termination_lock_.lock();
  stats_->UpdateHistogram(execute_start_time_);
  execute_start_time_ = DefaultTime;
  UnwrapData(isolate_)->is_executing_ = false;
  UnwrapData(isolate_)->termination_lock_.unlock();

  auto query_mgr = UnwrapData(isolate_)->query_mgr;
  query_mgr->ClearQueries();

  if (try_catch.HasCaught()) {
    stats_->IncrementExecutionStat("timer_callback_failure");
    stats_->AddException(isolate_, try_catch);
    return;
  }

  stats_->IncrementExecutionStat("timer_callback_success");
}

void V8Worker2::settings_change(
    const std::unique_ptr<messages::worker_request> &msg) {
  // TODO: take action appropriately
}

std::string V8Worker2::CompileHandler(std::string area_name,
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
    return CompileInfoToString(BuildCompileInfo(isolate_, context, &try_catch));
  }

  v8::Local<v8::Value> result;
  if (!compiled_script->Run(context).ToLocal(&result)) {
    return CompileInfoToString(BuildCompileInfo(isolate_, context, &try_catch));
  }

  CompilationInfo info;
  info.compile_success = true;
  info.description = "Compilation success";
  info.language = "Javascript";
  return CompileInfoToString(info);
}

void V8Worker2::push_msg(std::unique_ptr<messages::worker_request> msg,
                         bool priority) {
  if (priority) {
    worker_queue_->PushFront(std::move(msg));
    return;
  }
  worker_queue_->PushBack(std::move(msg));
}

void V8Worker2::update_seq_for_vb(uint16_t vb, uint64_t vbuuid, uint64_t seq) {
  AddTimerPartition(vb);
  f_map_->update_seq(vb, vbuuid, seq);
}

std::tuple<uint64_t, uint64_t, bool>
V8Worker2::AddFilterEvent(uint16_t vb, std::vector<uint8_t> payload) {
  RemoveTimerPartition(vb);

  auto [seq, vbuuid, executing] = f_map_->AddFilterEvent(vb);
  std::unique_ptr<messages::worker_request> filter_msg(
      new messages::worker_request);
  filter_msg->event = messages::eInternal;
  filter_msg->opcode = messages::eDeleteFilter;
  filter_msg->payload = std::move(payload);
  push_msg(std::move(filter_msg));

  return {seq, vbuuid, (executing || timer_execution_.load())};
}

void V8Worker2::AddCidFilterEvent(uint32_t cid) {
  f_map_->AddCidFilterEvent(cid);
}

std::string V8Worker2::get_vb_details_seq() {
  return f_map_->get_vb_details_seq();
}

void V8Worker2::TaskDurationWatcher() {

  while (!shutdown_terminator_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    UnwrapData(isolate_)->termination_lock_.lock();
    auto execute_start_time = execute_start_time_;
    UnwrapData(isolate_)->termination_lock_.unlock();

    if (execute_start_time == DefaultTime) {
      continue;
    }

    Time::time_point t = Time::now();
    auto duration =
        std::chrono::duration_cast<nsecs>(t - execute_start_time).count();
    if (duration <= max_task_duration_) {
      continue;
    }

    {
      // By holding this lock here, we ensure that the execution control is in
      // the realm of JavaScript and therefore, the call to
      // V8::TerminateExecution done below succeeds
      std::lock_guard<std::mutex> guard(
          UnwrapData(isolate_)->termination_lock_);

      // Checking again to makeing sure that correct runner is being terminated
      if (execute_start_time_ == DefaultTime) {
        continue;
      }

      t = Time::now();
      auto duration =
          std::chrono::duration_cast<nsecs>(t - execute_start_time_).count();

      if (duration <= max_task_duration_) {
        continue;
      }

      stats_->IncrementFailureStat("timeout_count");
      isolate_->TerminateExecution();
    }

    LOG(logInfo) << log_prefix_ << "Task took: " << duration
                 << "ns, terminated its execution" << std::endl;
  }
}

// Internal function for vb_handler
std::tuple<uint64_t, uint64_t, bool>
V8Worker2::vb_handler::AddFilterEvent(const uint16_t &vb) {
  auto executing = false;
  uint64_t processed_seq = 0;
  uint64_t vbuuid = 0;

  mutex_.lock();
  vbfilter_map_[vb]++;
  processed_seq = processed_seq_[vb];
  vbuuid = vbuuid_[vb];
  if (current_executing_vb_ == vb) {
    executing = true;
  }
  processed_seq_.erase(vb);
  vbuuid_.erase(vb);

  mutex_.unlock();
  return {processed_seq, vbuuid, executing};
}

void V8Worker2::vb_handler::RemoveFilterEvent(const uint16_t &vb) {
  mutex_.lock();
  auto count = vbfilter_map_.find(vb);
  if (count == vbfilter_map_.end()) {
    mutex_.unlock();
    return;
  }

  vbfilter_map_[vb]--;
  if (vbfilter_map_[vb] == 0) {
    vbfilter_map_.erase(vb);
  }
  mutex_.unlock();
}

void V8Worker2::vb_handler::AddCidFilterEvent(const uint32_t &cid) {
  mutex_.lock();
  vbfilter_map_for_cid_[cid]++;
  mutex_.unlock();
}

void V8Worker2::vb_handler::RemoveCidFilter(const uint32_t &cid) {
  mutex_.lock();
  auto count = vbfilter_map_for_cid_.find(cid);
  if (count == vbfilter_map_for_cid_.end()) {
    mutex_.unlock();
    return;
  }

  vbfilter_map_for_cid_[cid]--;
  if (vbfilter_map_for_cid_[cid] == 0) {
    vbfilter_map_for_cid_.erase(cid);
  }

  mutex_.unlock();
}

bool V8Worker2::vb_handler::CheckAndUpdateFilter(const uint32_t &cid,
                                                 const uint16_t &vb,
                                                 const uint64_t &vbuuid,
                                                 const uint64_t &seq_num,
                                                 bool skip_cid_check) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto vb_filter = vbfilter_map_.find(vb);
  if (vb_filter != vbfilter_map_.end()) {
    return true;
  }

  processed_seq_[vb] = seq_num;
  vbuuid_[vb] = vbuuid;
  // This is for noop event
  if (skip_cid_check) {
    return false;
  }

  auto cidIt = vbfilter_map_for_cid_.find(cid);
  if (cidIt != vbfilter_map_for_cid_.end()) {
    return true;
  }

  current_executing_vb_ = vb;
  return false;
}

void V8Worker2::vb_handler::update_seq(uint16_t vb, uint64_t vbuuid,
                                       uint64_t seq) {
  mutex_.lock();
  processed_seq_[vb] = seq;
  vbuuid_[vb] = vbuuid;
  mutex_.unlock();
}

void V8Worker2::vb_handler::reset_current_vb() {
  mutex_.lock();
  current_executing_vb_ = 0xffff;
  mutex_.unlock();
}

std::string V8Worker2::vb_handler::get_vb_details_seq() {
  mutex_.lock();
  std::ostringstream oss;
  for (auto it = processed_seq_.begin(); it != processed_seq_.end(); it++) {
    uint16_t vb = it->first;
    uint64_t seq = it->second;
    auto vbuuid = vbuuid_[vb];
    oss << static_cast<uint8_t>(vb >> 8) << static_cast<uint8_t>(vb);
    oss << static_cast<uint8_t>(seq >> 56) << static_cast<uint8_t>(seq >> 48)
        << static_cast<uint8_t>(seq >> 40) << static_cast<uint8_t>(seq >> 32)
        << static_cast<uint8_t>(seq >> 24) << static_cast<uint8_t>(seq >> 16)
        << static_cast<uint8_t>(seq >> 8) << static_cast<uint8_t>(seq);
    oss << static_cast<uint8_t>(vbuuid >> 56)
        << static_cast<uint8_t>(vbuuid >> 48)
        << static_cast<uint8_t>(vbuuid >> 40)
        << static_cast<uint8_t>(vbuuid >> 32)
        << static_cast<uint8_t>(vbuuid >> 24)
        << static_cast<uint8_t>(vbuuid >> 16)
        << static_cast<uint8_t>(vbuuid >> 8) << static_cast<uint8_t>(vbuuid);
  }
  mutex_.unlock();

  return oss.str();
}

// Timer related:
lcb_STATUS V8Worker2::SetTimer(timer::TimerInfo &tinfo) {
  if (timer_store_) {
    return timer_store_->SetTimer(tinfo, data_.lcb_retry_count,
                                  data_.op_timeout);
  }
  return LCB_SUCCESS;
}

lcb_STATUS V8Worker2::DelTimer(timer::TimerInfo &tinfo) {
  if (timer_store_)
    return timer_store_->DelTimer(tinfo, data_.lcb_retry_count,
                                  data_.op_timeout);
  return LCB_SUCCESS;
}

lcb_INSTANCE *V8Worker2::GetTimerLcbHandle() const {
  return timer_store_->GetTimerStoreHandle();
}

void V8Worker2::AddTimerPartition(int vb_no) {
  if (timer_store_) {
    timer_store_->AddPartition(vb_no);
  }
}

void V8Worker2::RemoveTimerPartition(int vb_no) {
  if (timer_store_) {
    timer_store_->RemovePartition(vb_no);
  }
}

std::string GetFunctionInstanceID(v8::Isolate *isolate) {
  auto w = UnwrapData(isolate)->v8worker2;
  return w->GetFunctionInstanceID();
}

void AddLcbException(const IsolateData *isolate_data, const int code) {
  auto w = isolate_data->v8worker2;
  w->AddLcbException(code);
}

std::string V8Worker2::GetOnDeployResult(int return_code) {
  nlohmann::json result;
  std::string on_deploy_status = "Finished";
  if(return_code != kSuccess) {
    on_deploy_status = "Failed";
  }
  std::cerr << "v8Worker::GetOnDeployResult The status for OnDeploy: " << on_deploy_status << std::endl;
  result["on_deploy_status"] = on_deploy_status;
  return result.dump();
}
