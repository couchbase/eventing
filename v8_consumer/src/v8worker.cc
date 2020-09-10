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
#include "bucket_ops.h"
#include "curl.h"
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
std::atomic<int64_t> timer_msg_counter = {0};
std::atomic<int64_t> timer_create_counter = {0};
std::atomic<int64_t> timer_cancel_counter = {0};

std::atomic<int64_t> enqueued_dcp_delete_msg_counter = {0};
std::atomic<int64_t> enqueued_dcp_mutation_msg_counter = {0};
std::atomic<int64_t> enqueued_timer_msg_counter = {0};

std::atomic<int64_t> timer_callback_missing_counter = {0};

v8::Local<v8::Object> V8Worker::NewCouchbaseNameSpace() {
  v8::EscapableHandleScope handle_scope(isolate_);

  v8::Local<v8::FunctionTemplate> function_template =
      v8::FunctionTemplate::New(isolate_);
  function_template->SetClassName(
      v8::String::NewFromUtf8(isolate_, "couchbase"));

  v8::Local<v8::ObjectTemplate> proto_t =
      function_template->PrototypeTemplate();

  proto_t->Set(v8::String::NewFromUtf8(isolate_, "get"),
               v8::FunctionTemplate::New(isolate_, BucketOps::GetOp));
  proto_t->Set(v8::String::NewFromUtf8(isolate_, "insert"),
               v8::FunctionTemplate::New(isolate_, BucketOps::InsertOp));
  proto_t->Set(v8::String::NewFromUtf8(isolate_, "upsert"),
               v8::FunctionTemplate::New(isolate_, BucketOps::UpsertOp));
  proto_t->Set(v8::String::NewFromUtf8(isolate_, "replace"),
               v8::FunctionTemplate::New(isolate_, BucketOps::ReplaceOp));
  proto_t->Set(v8::String::NewFromUtf8(isolate_, "delete"),
               v8::FunctionTemplate::New(isolate_, BucketOps::DeleteOp));
  proto_t->Set(v8::String::NewFromUtf8(isolate_, "increment"),
               v8::FunctionTemplate::New(isolate_, BucketOps::IncrementOp));
  proto_t->Set(v8::String::NewFromUtf8(isolate_, "decrement"),
               v8::FunctionTemplate::New(isolate_, BucketOps::DecrementOp));

  auto context = context_.Get(isolate_);
  v8::Local<v8::Object> cb_obj;
  if (!TO_LOCAL(proto_t->NewInstance(context), &cb_obj)) {
    return v8::Local<v8::Object>();
  }

  return handle_scope.Escape(cb_obj);
}

v8::Local<v8::ObjectTemplate> V8Worker::NewGlobalObj() const {
  v8::EscapableHandleScope handle_scope(isolate_);

  auto global = v8::ObjectTemplate::New(isolate_);

  global->Set(v8::String::NewFromUtf8(isolate_, "curl"),
              v8::FunctionTemplate::New(isolate_, CurlFunction));
  global->Set(v8::String::NewFromUtf8(isolate_, "log"),
              v8::FunctionTemplate::New(isolate_, Log));
  global->Set(v8::String::NewFromUtf8(isolate_, "createTimer"),
              v8::FunctionTemplate::New(isolate_, CreateTimer));
  global->Set(v8::String::NewFromUtf8(isolate_, "cancelTimer"),
              v8::FunctionTemplate::New(isolate_, CancelTimer));
  global->Set(v8::String::NewFromUtf8(isolate_, "crc64"),
              v8::FunctionTemplate::New(isolate_, Crc64Function));
  global->Set(v8::String::NewFromUtf8(isolate_, "N1QL"),
              v8::FunctionTemplate::New(isolate_, QueryFunction));

  for (const auto &type_name : exception_type_names_) {
    global->Set(v8::String::NewFromUtf8(isolate_, type_name.c_str()),
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
    auto source_mutation = bucket_name == cb_source_bucket_ &&
                           scope_name == cb_source_scope_ &&
                           collection_name == cb_source_collection_;
    bucket_bindings_.emplace_back(isolate_, bucket_factory_, bucket_name,
                                  scope_name, collection_name, bucket_alias,
                                  bucket_access == "r", source_mutation);
  }
}

void V8Worker::InitializeIsolateData(const server_settings_t *server_settings,
                                     const handler_config_t *h_config) {
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  data_.v8worker = this;
  data_.utils = new Utils(isolate_, context);
  data_.js_exception = new JsException(isolate_);
  auto key = GetLocalKey();
  data_.comm = new Communicator(server_settings->host_addr,
                                server_settings->eventing_port, key.first,
                                key.second, false, app_name_, isolate_);
  data_.timer = new Timer(isolate_, context);
  // TODO : Need to make HEAD call to all the bindings to establish TCP
  // Connections
  data_.curl_factory = new CurlFactory(isolate_, context);
  data_.req_builder = new CurlRequestBuilder(isolate_, context);
  data_.resp_builder = new CurlResponseBuilder(isolate_, context);
  data_.custom_error = new CustomError(isolate_, context);
  data_.curl_codex = new CurlCodex;
  data_.code_insight = new CodeInsight(isolate_);
  data_.query_mgr =
      new Query::Manager(isolate_, cb_source_bucket_,
                         static_cast<std::size_t>(h_config->lcb_inst_capacity));
  data_.query_iterable = new Query::Iterable(isolate_, context);
  data_.query_iterable_impl = new Query::IterableImpl(isolate_, context);
  data_.query_iterable_result = new Query::IterableResult(isolate_, context);
  data_.query_helper = new Query::Helper(isolate_, context);

  // execution_timeout is in seconds
  // n1ql_timeout is expected in micro seconds
  // Setting a lower timeout to allow adequate time for the exception
  // to get thrown
  data_.n1ql_timeout =
      static_cast<lcb_U32>(h_config->execution_timeout < 3
                               ? 500000
                               : (h_config->execution_timeout - 2) * 1000000);
  data_.op_timeout = h_config->execution_timeout < 5
                         ? h_config->execution_timeout
                         : h_config->execution_timeout - 2;
  data_.n1ql_consistency =
      Query::Helper::GetConsistency(h_config->n1ql_consistency);
  data_.n1ql_prepare_all = h_config->n1ql_prepare_all;
  data_.lang_compat = new LanguageCompatibility(h_config->lang_compat);
  data_.lcb_retry_count = h_config->lcb_retry_count;

  data_.bucket_ops = new BucketOps(isolate_, context);
}

void V8Worker::InitializeCurlBindingValues(
    const std::vector<CurlBinding> &curl_bindings) {
  for (const auto &curl_binding : curl_bindings) {
    curl_binding_values_.emplace_back(curl_binding.value);
  }
}

V8Worker::V8Worker(v8::Platform *platform, handler_config_t *h_config,
                   server_settings_t *server_settings,
                   const std::string &function_name,
                   const std::string &function_id,
                   const std::string &function_instance_id,
                   const std::string &user_prefix, Histogram *latency_stats,
                   Histogram *curl_latency_stats,
                   const std::string &ns_server_port,
                   const int32_t &num_vbuckets)
    : app_name_(h_config->app_name), settings_(server_settings),
      num_vbuckets_(num_vbuckets), latency_stats_(latency_stats),
      curl_latency_stats_(curl_latency_stats), platform_(platform),
      function_name_(function_name), function_id_(function_id),
      user_prefix_(user_prefix), ns_server_port_(ns_server_port),
      exception_type_names_(
          {"KVError", "N1QLError", "EventingError", "CurlError", "TypeError"}),
      handler_headers_(h_config->handler_headers),
      handler_footers_(h_config->handler_footers) {
  auto config = ParseDeployment(h_config->dep_cfg.c_str());
  cb_source_bucket_.assign(config->source_bucket);
  cb_source_scope_.assign(config->source_scope);
  cb_source_collection_.assign(config->source_collection);

  std::ostringstream oss;
  oss << "\"" << function_id << "-" << function_instance_id << "\"";
  function_instance_id_.assign(oss.str());
  thread_exit_cond_.store(false);
  stop_timer_scan_.store(false);
  scan_timer_.store(false);
  update_v8_heap_.store(false);
  run_gc_.store(false);
  for (int i = 0; i < num_vbuckets_; i++) {
    vb_seq_[i] = atomic_ptr_t(new std::atomic<uint64_t>(0));
  }
  vbfilter_map_.resize(num_vbuckets_);
  processed_bucketops_.resize(num_vbuckets_, 0);

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
  InitializeCurlBindingValues(config->curl_bindings);

  bucket_factory_ = std::make_shared<BucketFactory>(isolate_, context);
  if (!h_config->skip_lcb_bootstrap) {
    InstallBucketBindings(config->component_configs);
  }

  execute_start_time_ = Time::now();
  max_task_duration_ = SECS_TO_NS * h_config->execution_timeout;

  timer_context_size = h_config->timer_context_size;

  LOG(logInfo) << "Initialised V8Worker handle, app_name: "
               << h_config->app_name
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
               << " num_vbuckets: " << num_vbuckets_ << std::endl;

  src_path_ = settings_->eventing_dir + "/" + app_name_ + ".t.js";

  if (h_config->using_timer) {
    std::vector<int64_t> partitions;
    auto prefix = user_prefix + "::" + function_id;
    timer_store_ = new timer::TimerStore(
        isolate_, prefix, partitions, config->metadata_bucket,
        config->metadata_scope, config->metadata_collection, num_vbuckets_);
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
  delete data->query_mgr;
  delete data->query_iterable;
  delete data->query_iterable_impl;
  delete data->query_iterable_result;
  delete data->query_helper;
  delete data->lang_compat;

  context_.Reset();
  on_update_.Reset();
  on_delete_.Reset();
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
  v8::ScriptOrigin origin(origin_v8_str);

  v8::Local<v8::Function> console_log_func;
  if (!TO_LOCAL(
          v8::FunctionTemplate::New(isolate_, ConsoleLog)->GetFunction(context),
          &console_log_func)) {
    return false;
  }

  // Replace the usual log function with console.log
  auto global = context->Global();
  global->Set(v8Str(isolate_, "log"), console_log_func);

  auto source = v8Str(isolate_, script_to_execute_);
  v8::Local<v8::Script> script;
  if (!TO_LOCAL(v8::Script::Compile(context, source, &origin), &script)) {
    return false;
  }

  v8::Local<v8::Value> result;
  if (!TO_LOCAL(script->Run(context), &result)) {
    return false;
  }

  auto func_ref = global->Get(v8Str(isolate_, func_name));
  auto func = func_ref.As<v8::Function>();
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

int V8Worker::V8WorkerLoad(std::string script_to_execute) {
  LOG(logInfo) << "Eventing dir: " << RS(settings_->eventing_dir) << std::endl;
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  auto global_object = context->Global();
  auto function_template = NewCouchbaseNameSpace();
  auto result = global_object->Set(context, v8Str(isolate_, "couchbase"),
                                   function_template);

  if (!result.FromJust()) {
    LOG(logInfo) << "Failed to set the global namespace couchbase" << std::endl;
    return kToLocalFailed;
  }

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

  if (!on_update_def->IsFunction() && !on_delete_def->IsFunction()) {
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
      v8::String::NewFromUtf8(isolate_, wrapper_function.c_str());
  v8::ScriptOrigin origin(script_name);
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

    auto evt = getEvent(msg->header.event);
    switch (evt) {
    case eDCP:
      switch (getDCPOpcode(msg->header.opcode)) {
      case oDelete:
        HandleDeleteEvent(msg);
        break;

      case oMutation:
        HandleMutationEvent(msg);
        break;

      default:
        LOG(logError) << "Received invalid DCP opcode" << std::endl;
        break;
      }
      processed_events_size += msg->payload.GetSize();
      num_processed_events++;
      break;

    case eInternal:
      switch (msg->header.opcode) {
      case oScanTimer: {
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
      case oUpdateV8HeapSize: {
        UpdateV8HeapSize();
        update_v8_heap_.store(false);
        break;
      }
      case oRunGc: {
        ForceRunGarbageCollector();
        run_gc_.store(false);
        break;
      }
      default:
        LOG(logError) << "Received invalid internal opcode" << std::endl;
        break;
      }
      break;
    case eDebugger:
      switch (getDebuggerOpcode(msg->header.opcode)) {
      case oDebuggerStart:
        this->StartDebugger();
        break;

      case oDebuggerStop:
        this->StopDebugger();
        break;

      default:
        LOG(logError) << "Received invalid debugger opcode" << std::endl;
        break;
      }
      break;
    default:
      LOG(logError) << "Received unsupported event " << evt << std::endl;
      break;
    }

    ++messages_processed_counter;
  }
}

void V8Worker::UpdateSeqNumLocked(const int vb, const uint64_t seq_num) {
  currently_processed_vb_ = vb;
  currently_processed_seqno_ = seq_num;
  vb_seq_[vb]->store(seq_num, std::memory_order_seq_cst);
  processed_bucketops_[vb] = seq_num;
}

void V8Worker::HandleDeleteEvent(const std::unique_ptr<WorkerMessage> &msg) {

  ++dcp_delete_msg_counter;
  auto [vb, seq_num, is_valid] = GetVbAndSeqNum(msg);
  if (!is_valid) {
    ++dcp_delete_parse_failure;
    return;
  }

  {
    std::lock_guard<std::mutex> guard(bucketops_lock_);
    if (IsFilteredEventLocked(vb, seq_num)) {
      return;
    }
    UpdateSeqNumLocked(vb, seq_num);
  }

  const auto options = flatbuf::payload::GetPayload(
      static_cast<const void *>(msg->payload.payload.c_str()));
  SendDelete(options->value()->str(), msg->header.metadata);
}

void V8Worker::HandleMutationEvent(const std::unique_ptr<WorkerMessage> &msg) {

  ++dcp_mutation_msg_counter;
  auto [vb, seq_num, is_valid] = GetVbAndSeqNum(msg);
  if (!is_valid) {
    ++dcp_mutation_parse_failure;
    return;
  }

  {
    std::lock_guard<std::mutex> guard(bucketops_lock_);
    if (IsFilteredEventLocked(vb, seq_num)) {
      return;
    }
    UpdateSeqNumLocked(vb, seq_num);
  }

  const auto doc = flatbuf::payload::GetPayload(
      static_cast<const void *>(msg->payload.payload.c_str()));
  SendUpdate(doc->value()->str(), msg->header.metadata);
}

std::tuple<int, uint64_t, bool>
V8Worker::GetVbAndSeqNum(const std::unique_ptr<WorkerMessage> &msg) const {
  auto vb = 0;
  uint64_t seq_num = 0;
  auto result = ParseMetadata(msg->header.metadata, vb, seq_num);
  return {vb, seq_num, result == kSuccess};
}

bool V8Worker::IsFilteredEventLocked(const int vb, const uint64_t seq_num) {
  const auto filter_seq_no = GetVbFilter(vb);
  if (filter_seq_no > 0 && seq_num <= filter_seq_no) {
    // Skip filtered event
    ++filtered_dcp_mutation_counter;
    if (seq_num == filter_seq_no) {
      EraseVbFilter(vb);
    }
    return true;
  }
  return false;
}

bool V8Worker::ExecuteScript(const v8::Local<v8::String> &script) {
  v8::HandleScope handle_scope(isolate_);
  v8::TryCatch try_catch(isolate_);

  auto context = context_.Get(isolate_);
  auto script_name = v8Str(isolate_, app_name_ + ".js");
  v8::ScriptOrigin origin(script_name);

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

int V8Worker::SendUpdate(const std::string &value, const std::string &meta) {
  const auto start_time = Time::now();

  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  LOG(logTrace) << "value: " << RU(value) << " meta: " << RU(meta) << std::endl;
  v8::TryCatch try_catch(isolate_);

  v8::Local<v8::Value> args[2];
  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, value)), &args[0])) {
    return kToLocalFailed;
  }
  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, meta)), &args[1])) {
    return kToLocalFailed;
  }

  auto js_meta = args[1].As<v8::Object>();
  auto dcp_expiry =
      js_meta->Get(v8Str(isolate_, "expiration"))->ToNumber(context);
  if (!dcp_expiry.IsEmpty()) {
    auto expiry = dcp_expiry.ToLocalChecked()->Value() * 1000;
    if (expiry != 0) {
      auto js_expiry = v8::Date::New(isolate_, expiry);
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
  }

  if (debugger_started_) {
    if (!agent_->IsStarted()) {
      agent_->Start(isolate_, platform_, src_path_.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    return DebugExecute("OnUpdate", args, 2) ? kSuccess : kOnUpdateCallFail;
  }

  RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                        IsTerminatingRetriable, IsExecutionTerminating,
                        isolate_);

  auto on_doc_update = on_update_.Get(isolate_);
  execute_start_time_ = Time::now();
  UnwrapData(isolate_)->is_executing_ = true;
  on_doc_update->Call(context->Global(), 2, args);
  UnwrapData(isolate_)->is_executing_ = false;
  auto query_mgr = UnwrapData(isolate_)->query_mgr;
  query_mgr->ClearQueries();

  if (try_catch.HasCaught()) {
    UpdateHistogram(start_time);
    on_update_failure++;
    auto emsg = ExceptionString(isolate_, context, &try_catch);
    LOG(logDebug) << "OnUpdate Exception: " << emsg << std::endl;
    CodeInsight::Get(isolate_).AccumulateException(try_catch);
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

  v8::Local<v8::Value> args[2];
  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, meta)), &args[0])) {
    return kToLocalFailed;
  }

  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, options)), &args[1])) {
    return kToLocalFailed;
  }

  // DCP always sends 0 as expiration. Until that changes, below won't run
  auto js_meta = args[0].As<v8::Object>();
  auto dcp_expiry =
      js_meta->Get(v8Str(isolate_, "expiration"))->ToNumber(context);
  if (!dcp_expiry.IsEmpty()) {
    auto expiry = dcp_expiry.ToLocalChecked()->Value() * 1000;
    if (expiry != 0) {
      auto js_expiry = v8::Date::New(isolate_, expiry);
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

  assert(!try_catch.HasCaught());

  if (debugger_started_) {
    if (!agent_->IsStarted()) {
      agent_->Start(isolate_, platform_, src_path_.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    return DebugExecute("OnDelete", args, 2) ? kSuccess : kOnDeleteCallFail;
  }

  RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                        IsTerminatingRetriable, IsExecutionTerminating,
                        isolate_);

  auto on_doc_delete = on_delete_.Get(isolate_);
  execute_start_time_ = Time::now();
  UnwrapData(isolate_)->is_executing_ = true;
  on_doc_delete->Call(context->Global(), 2, args);
  UnwrapData(isolate_)->is_executing_ = false;
  auto query_mgr = UnwrapData(isolate_)->query_mgr;
  query_mgr->ClearQueries();

  if (try_catch.HasCaught()) {
    LOG(logDebug) << "OnDelete Exception: "
                  << ExceptionString(isolate_, context, &try_catch)
                  << std::endl;
    UpdateHistogram(start_time);
    ++on_delete_failure;
    return kOnDeleteCallFail;
  }

  UpdateHistogram(start_time);
  on_delete_success++;
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
  v8::Local<v8::Value> arg[1];

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

  if (try_catch.HasCaught()) {
    auto emsg = ExceptionString(isolate_, context, &try_catch);
    LOG(logDebug) << "Timer callback Exception: " << emsg << std::endl;
    CodeInsight::Get(isolate_).AccumulateException(try_catch);
  }

  if (debugger_started_) {
    if (!agent_->IsStarted()) {
      agent_->Start(isolate_, platform_, src_path_.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    DebugExecute(callback.c_str(), arg, 1);
  }

  RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                        IsTerminatingRetriable, IsExecutionTerminating,
                        isolate_);
  execute_start_time_ = Time::now();
  UnwrapData(isolate_)->is_executing_ = true;
  callback_func->Call(callback_func_val, 1, arg);
  UnwrapData(isolate_)->is_executing_ = false;

  auto query_mgr = UnwrapData(isolate_)->query_mgr;
  query_mgr->ClearQueries();

  if (try_catch.HasCaught()) {
    UpdateHistogram(execute_start_time_);
    timer_callback_failure++;
    auto emsg = ExceptionString(isolate_, context, &try_catch);
    LOG(logDebug) << "Timer callback Exception: " << emsg << std::endl;
    CodeInsight::Get(isolate_).AccumulateException(try_catch);
  }
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

  agent_ = new inspector::Agent("0.0.0.0", settings_->host_addr,
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
  v8::ScriptOrigin origin(script_name);

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
    auto seq = vb_seq_[vb].get()->load(std::memory_order_seq_cst);
    if (seq > 0) {
      std::string seq_no = std::to_string(vb) + "::" + std::to_string(seq);
      auto curr_messages =
          BuildResponse(seq_no, mBucket_Ops_Response, checkpointResponse);
      for (auto &msg : curr_messages) {
        messages.push_back(msg);
      }
      // Reset the seq no of checkpointed vb to 0
      vb_seq_[vb].get()->compare_exchange_strong(seq, 0);
    }
  }
}

std::vector<uv_buf_t> V8Worker::BuildResponse(const std::string &payload,
                                              int8_t msg_type,
                                              int8_t response_opcode) {
  std::vector<uv_buf_t> messages;
  flatbuffers::FlatBufferBuilder builder;
  auto msg_offset = builder.CreateString(payload);
  auto r = flatbuf::response::CreateResponse(builder, msg_type, response_opcode,
                                             msg_offset);
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

int V8Worker::ParseMetadata(const std::string &metadata, int &vb_no,
                            uint64_t &seq_no) const {
  int skip_ack;
  return ParseMetadataWithAck(metadata, vb_no, seq_no, skip_ack, false);
}

int V8Worker::ParseMetadataWithAck(const std::string &metadata_str, int &vb_no,
                                   uint64_t &seq_no, int &skip_ack,
                                   const bool ack_check) const {
  auto metadata = nlohmann::json::parse(metadata_str, nullptr, false);
  if (metadata.is_discarded()) {
    return kJSONParseFailed;
  }

  seq_no = metadata["seq"].get<uint64_t>();
  vb_no = metadata["vb"].get<int>();
  if (ack_check) {
    skip_ack = metadata["skip_ack"].get<int>();
  }
  return kSuccess;
}

void V8Worker::UpdateVbFilter(int vb_no, uint64_t seq_no) {
  vbfilter_map_[vb_no].push_back(seq_no);
}

uint64_t V8Worker::GetVbFilter(int vb_no) {
  auto &filters = vbfilter_map_[vb_no];
  if (filters.empty())
    return 0;
  return filters.front();
}

void V8Worker::EraseVbFilter(int vb_no) {
  auto &filters = vbfilter_map_[vb_no];
  if (!filters.empty()) {
    filters.erase(filters.begin());
  }
}

void V8Worker::UpdateBucketopsSeqnoLocked(int vb_no, uint64_t seq_no) {
  processed_bucketops_[vb_no] = seq_no;
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
  // Reset the seq no of checkpointed vb to 0
  vb_seq_[vb_no]->store(0, std::memory_order_seq_cst);
  return processed_bucketops_[vb_no];
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

std::unique_lock<std::mutex> V8Worker::GetAndLockFilterLock() {
  return std::unique_lock<std::mutex>(bucketops_lock_);
}

void V8Worker::StopTimerScan() { stop_timer_scan_.store(true); }

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
