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

#include "v8worker.h"
#include "bucket.h"
#include "curl.h"
#include "retry_util.h"
#include "timer.h"
#include "transpiler.h"
#include "utils.h"

#include "../../gen/js/builtin.h"

bool V8Worker::debugger_started_ = false;

std::atomic<int64_t> timeout_count = {0};
std::atomic<int16_t> checkpoint_failure_count = {0};

std::atomic<int64_t> on_update_success = {0};
std::atomic<int64_t> on_update_failure = {0};
std::atomic<int64_t> on_delete_success = {0};
std::atomic<int64_t> on_delete_failure = {0};

std::atomic<int64_t> timer_create_failure = {0};

std::atomic<int64_t> messages_processed_counter = {0};

std::atomic<int64_t> dcp_delete_msg_counter = {0};
std::atomic<int64_t> dcp_mutation_msg_counter = {0};
std::atomic<int64_t> timer_msg_counter = {0};

std::atomic<int64_t> enqueued_dcp_delete_msg_counter = {0};
std::atomic<int64_t> enqueued_dcp_mutation_msg_counter = {0};
std::atomic<int64_t> enqueued_timer_msg_counter = {0};

std::atomic<int64_t> timer_callback_missing_counter = {0};

v8::Local<v8::ObjectTemplate> V8Worker::NewGlobalObj() const {
  v8::EscapableHandleScope handle_scope(isolate_);

  auto global = v8::ObjectTemplate::New(isolate_);
  global->Set(v8::String::NewFromUtf8(isolate_, "curl"),
              v8::FunctionTemplate::New(isolate_, CurlFunction));
  global->Set(v8::String::NewFromUtf8(isolate_, "log"),
              v8::FunctionTemplate::New(isolate_, Log));
  global->Set(v8::String::NewFromUtf8(isolate_, "iter"),
              v8::FunctionTemplate::New(isolate_, IterFunction));
  global->Set(v8::String::NewFromUtf8(isolate_, "stopIter"),
              v8::FunctionTemplate::New(isolate_, StopIterFunction));
  global->Set(v8::String::NewFromUtf8(isolate_, "execQuery"),
              v8::FunctionTemplate::New(isolate_, ExecQueryFunction));
  global->Set(v8::String::NewFromUtf8(isolate_, "getReturnValue"),
              v8::FunctionTemplate::New(isolate_, GetReturnValueFunction));
  global->Set(v8::String::NewFromUtf8(isolate_, "createTimer"),
              v8::FunctionTemplate::New(isolate_, CreateTimer));
  global->Set(v8::String::NewFromUtf8(isolate_, "urlEncode"),
              v8::FunctionTemplate::New(isolate_, UrlEncodeFunction));
  global->Set(v8::String::NewFromUtf8(isolate_, "urlDecode"),
              v8::FunctionTemplate::New(isolate_, UrlDecodeFunction));

  for (const auto &type_name : exception_type_names_) {
    global->Set(v8::String::NewFromUtf8(isolate_, type_name.c_str()),
                v8::FunctionTemplate::New(isolate_, CustomErrorCtor));
  }
  return handle_scope.Escape(global);
}

// TODO : Use vector
void V8Worker::InstallCurlBindings(
    const std::vector<CurlBinding> &curl_bindings) const {
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  for (const auto &binding : curl_bindings) {
    binding.InstallBinding(isolate_, context);
  }
}

void V8Worker::InitializeIsolateData(const server_settings_t *server_settings,
                                     const handler_config_t *h_config,
                                     const std::string &source_bucket) {
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  data_.v8worker = this;
  data_.utils = new Utils(isolate_, context);
  data_.js_exception = new JsException(isolate_);
  auto key = GetLocalKey();
  data_.comm = new Communicator(server_settings->host_addr,
                                server_settings->eventing_port, key.first,
                                key.second, false, app_name_, isolate_);
  data_.transpiler =
      new Transpiler(isolate_, GetTranspilerSrc(), h_config->handler_headers,
                     h_config->handler_footers, source_bucket);
  data_.timer = new Timer(isolate_, context);
  // TODO : Need to make HEAD call to all the bindings to establish TCP
  // Connections
  data_.curl_factory = new CurlFactory(isolate_, context);
  data_.req_builder = new CurlRequestBuilder(isolate_, context);
  data_.resp_builder = new CurlResponseBuilder(isolate_, context);
  data_.custom_error = new CustomError(isolate_, context);
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
                   const std::string &user_prefix)
    : app_name_(h_config->app_name), settings_(server_settings),
      platform_(platform), function_name_(function_name),
      function_id_(function_id), user_prefix_(user_prefix),
      exception_type_names_(
          {"KVError", "N1QLError", "EventingError", "CurlError"}) {
  auto config = ParseDeployment(h_config->dep_cfg.c_str());
  std::ostringstream oss;
  oss << "\"" << function_id << "-" << function_instance_id << "\"";
  function_instance_id_.assign(oss.str());
  histogram_ = new Histogram(HIST_FROM, HIST_TILL, HIST_WIDTH);
  curl_latency_ = new Histogram(HIST_FROM, HIST_TILL, HIST_WIDTH);
  thread_exit_cond_.store(false);
  for (int i = 0; i < NUM_VBUCKETS; i++) {
    vb_seq_[i] = atomic_ptr_t(new std::atomic<int64_t>(0));
  }
  vbfilter_map_.resize(NUM_VBUCKETS, -1);
  processed_bucketops_.resize(NUM_VBUCKETS, 0);
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
  InitializeIsolateData(server_settings, h_config, config->source_bucket);
  InstallCurlBindings(config->curl_bindings);
  InitializeCurlBindingValues(config->curl_bindings);

  execute_start_time_ = Time::now();
  cb_source_bucket_.assign(config->source_bucket);

  Bucket *bucket_handle = nullptr;
  max_task_duration_ = SECS_TO_NS * h_config->execution_timeout;
  timer_context_size = h_config->timer_context_size;

  if (!h_config->skip_lcb_bootstrap) {
    for (auto it = config->component_configs.begin();
         it != config->component_configs.end(); it++) {
      if (it->first == "buckets") {
        auto bucket = config->component_configs["buckets"].begin();
        for (; bucket != config->component_configs["buckets"].end(); bucket++) {
          std::string bucket_alias = bucket->first;
          std::string bucket_name =
              config->component_configs["buckets"][bucket_alias][0];
          std::string bucket_access =
              config->component_configs["buckets"][bucket_alias][2];
          bucket_handle = new Bucket(isolate_, context, bucket_name,
                                     settings_->kv_host_port, bucket_alias,
                                     bucket_access == "r",
                                     bucket_name == config->source_bucket);

          bucket_handles_.push_back(bucket_handle);
        }
      }
    }
  }

  LOG(logInfo) << "Initialised V8Worker handle, app_name: "
               << h_config->app_name
               << " debugger port: " << RS(settings_->debugger_port)
               << " curr_host: " << RS(settings_->host_addr)
               << " curr_eventing_port: " << RS(settings_->eventing_port)
               << " curr_eventing_sslport: " << RS(settings_->eventing_sslport)
               << " kv_host_port: " << RS(settings_->kv_host_port)
               << " lcb_cap: " << h_config->lcb_inst_capacity
               << " execution_timeout: " << h_config->execution_timeout
               << " timer_context_size: " << h_config->timer_context_size
               << " version: " << EventingVer() << std::endl;

  connstr_ = "couchbase://" + settings_->kv_host_port + "/" +
             cb_source_bucket_ + "?select_bucket=true";
  meta_connstr_ = "couchbase://" + settings_->kv_host_port + "/" +
                  config->metadata_bucket + "?select_bucket=true";

  if (IsIPv6()) {
    connstr_ += "&ipv6=allow";
    meta_connstr_ += "&ipv6=allow";
  }

  if (!h_config->skip_lcb_bootstrap) {
    conn_pool_ = new ConnectionPool(isolate_, h_config->lcb_inst_capacity,
                                    settings_->kv_host_port, cb_source_bucket_);
  }
  src_path_ = settings_->eventing_dir + "/" + app_name_ + ".t.js";

  delete config;

  this->timer_queue_ = new Queue<timer_msg_t>();
  this->worker_queue_ = new Queue<worker_msg_t>();

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
  delete data->transpiler;
  delete data->utils;
  delete data->timer;
  delete data->js_exception;
  delete data->curl_factory;
  delete data->req_builder;
  delete data->resp_builder;

  context_.Reset();
  on_update_.Reset();
  on_delete_.Reset();
  delete conn_pool_;
  delete n1ql_handle_;
  delete settings_;
  delete histogram_;
  delete curl_latency_;
  delete timer_queue_;
  delete worker_queue_;
}

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
  auto transpiler = UnwrapData(isolate_)->transpiler;
  v8::Context::Scope context_scope(context);

  for (const auto &type_name : exception_type_names_) {
    DeriveFromError(isolate_, context, type_name);
  }

  auto uniline_info = transpiler->UniLineN1QL(script_to_execute);
  LOG(logTrace) << "code after Unilining N1QL: "
                << RM(uniline_info.handler_code) << std::endl;
  if (uniline_info.code != kOK) {
    LOG(logError) << "failed to uniline N1QL" << std::endl;
    return uniline_info.code;
  }

  auto jsify_info = Jsify(script_to_execute, true, cb_source_bucket_);
  LOG(logTrace) << "jsified code: " << RM(jsify_info.handler_code) << std::endl;
  if (jsify_info.code != kOK) {
    LOG(logError) << "failed to jsify" << std::endl;
    return jsify_info.code;
  }

  // TODO : Move n1ql_handle_ initialization to InitializeIsolateData()
  n1ql_handle_ = new N1QL(conn_pool_, isolate_);
  UnwrapData(isolate_)->n1ql_handle = n1ql_handle_;

  script_to_execute =
      transpiler->Transpile(jsify_info.handler_code, app_name_ + ".js",
                            uniline_info.handler_code) +
      '\n';
  script_to_execute += std::string((const char *)js_builtin) + '\n';

  auto source = v8Str(isolate_, script_to_execute);
  script_to_execute_ = script_to_execute;
  LOG(logTrace) << "script to execute: " << RM(script_to_execute) << std::endl;

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

  if (!bucket_handles_.empty()) {
    auto bucket_handle = bucket_handles_.begin();

    for (; bucket_handle != bucket_handles_.end(); bucket_handle++) {
      if (*bucket_handle) {
        if (!(*bucket_handle)->InstallMaps()) {
          LOG(logError) << "Error initializing bucket handle" << std::endl;
          return kFailedInitBucketHandle;
        }
      }
    }
  }

  // Spawning terminator thread to monitor the wall clock time for execution
  // of javascript code isn't going beyond max_task_duration. Passing
  // reference to current object instead of having terminator thread make a
  // copy of the object. Spawned thread will execute the terminator loop logic
  // in function call operator() for V8Worker class
  terminator_thr_ = new std::thread(std::ref(*this));

  return kSuccess;
}

void V8Worker::RouteMessage() {
  const flatbuf::payload::Payload *payload;
  std::string val, context, callback;

  while (!thread_exit_cond_.load()) {
    worker_msg_t msg;
    if (!worker_queue_->Pop(msg)) {
      continue;
    }
    payload = flatbuf::payload::GetPayload(
        (const void *)msg.payload->payload.c_str());

    LOG(logTrace) << " event: " << static_cast<int16_t>(msg.header->event)
                  << " opcode: " << static_cast<int16_t>(msg.header->opcode)
                  << " metadata: " << RU(msg.header->metadata)
                  << " partition: " << msg.header->partition << std::endl;

    int vb_no = 0;
    int64_t seq_no = 0;

    switch (getEvent(msg.header->event)) {
    case eDCP:
      switch (getDCPOpcode(msg.header->opcode)) {
      case oDelete:
        dcp_delete_msg_counter++;
        if (kSuccess == ParseMetadata(msg.header->metadata, vb_no, seq_no)) {
          auto filter_seq_no = GetVbFilter(vb_no);
          if (filter_seq_no != -1 && seq_no <= filter_seq_no) {
            if (seq_no == filter_seq_no) {
              EraseVbFilter(vb_no);
            }
          } else {
            this->SendDelete(msg.header->metadata, vb_no, seq_no);
          }
        }
        break;
      case oMutation:
        payload = flatbuf::payload::GetPayload(
            (const void *)msg.payload->payload.c_str());
        val.assign(payload->value()->str());
        dcp_mutation_msg_counter++;
        if (kSuccess == ParseMetadata(msg.header->metadata, vb_no, seq_no)) {
          auto filter_seq_no = GetVbFilter(vb_no);
          if (filter_seq_no != -1 && seq_no <= filter_seq_no) {
            if (seq_no == filter_seq_no) {
              EraseVbFilter(vb_no);
            }
          } else {
            this->SendUpdate(val, msg.header->metadata, vb_no, seq_no, "json");
          }
        }
        break;
      default:
        break;
      }
      break;
    case eTimer:
      switch (getTimerOpcode(msg.header->opcode)) {
      case oTimer:
        payload = flatbuf::payload::GetPayload(
            (const void *)msg.payload->payload.c_str());
        callback.assign(payload->callback_fn()->str());
        context.assign(payload->context()->str());
        timer_msg_counter++;
        this->SendTimer(callback, context);
        break;
      default:
        break;
      }
      break;
    case eDebugger:
      switch (getDebuggerOpcode(msg.header->opcode)) {
      case oDebuggerStart:
        this->StartDebugger();
        break;
      case oDebuggerStop:
        this->StopDebugger();
        break;
      default:
        break;
      }
    default:
      break;
    }

    delete msg.header;
    delete msg.payload;

    messages_processed_counter++;
  }
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
                  << ExceptionString(isolate_, &try_catch) << std::endl;
    // The script failed to compile; bail out.
    return false;
  }

  v8::Local<v8::Value> result;
  if (!compiled_script->Run(context).ToLocal(&result)) {
    assert(try_catch.HasCaught());
    LOG(logError) << "Exception logged:"
                  << ExceptionString(isolate_, &try_catch) << std::endl;
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
  histogram_->Add(ns.count() / 1000);
}

void V8Worker::UpdateCurlLatencyHistogram(const Time::time_point &start) {
  Time::time_point t = Time::now();
  nsecs ns = std::chrono::duration_cast<nsecs>(t - start);
  curl_latency_->Add(ns.count() / 1000);
}

int V8Worker::SendUpdate(std::string value, std::string meta, int vb_no,
                         int64_t seq_no, std::string doc_type) {
  Time::time_point start_time = Time::now();

  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  LOG(logTrace) << "value: " << RU(value) << " meta: " << RU(meta)
                << " doc_type: " << doc_type << std::endl;
  v8::TryCatch try_catch(isolate_);

  v8::Local<v8::Value> args[2];
  if (doc_type == "json") {
    if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, value)), &args[0])) {
      return kToLocalFailed;
    }
  } else {
    args[0] = v8Str(isolate_, value);
  }

  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, meta)), &args[1])) {
    return kToLocalFailed;
  }

  currently_processed_vb_ = vb_no;
  currently_processed_seqno_ = seq_no;
  vb_seq_[vb_no]->store(seq_no, std::memory_order_seq_cst);
  UpdateBucketopsSeqno(vb_no, seq_no);
  if (on_update_.IsEmpty()) {
    UpdateHistogram(start_time);
    return kOnUpdateCallFail;
  }

  if (try_catch.HasCaught()) {
    LOG(logDebug) << "OnUpdate Exception: "
                  << ExceptionString(isolate_, &try_catch) << std::endl;
  }

  if (debugger_started_) {
    if (!agent_->IsStarted()) {
      agent_->Start(isolate_, platform_, src_path_.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    return DebugExecute("OnUpdate", args, 2) ? kSuccess : kOnUpdateCallFail;
  }

  auto on_doc_update = on_update_.Get(isolate_);
  execute_flag_ = true;
  execute_start_time_ = Time::now();
  RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                        IsTerminatingRetriable, IsExecutionTerminating,
                        isolate_);

  on_doc_update->Call(context->Global(), 2, args);
  execute_flag_ = false;
  if (try_catch.HasCaught()) {
    LOG(logDebug) << "OnUpdate Exception: "
                  << ExceptionString(isolate_, &try_catch) << std::endl;
    UpdateHistogram(start_time);
    on_update_failure++;
    return kOnUpdateCallFail;
  }

  on_update_success++;
  UpdateHistogram(start_time);
  return kSuccess;
}

int V8Worker::SendDelete(std::string meta, int vb_no, int64_t seq_no) {
  Time::time_point start_time = Time::now();

  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  LOG(logTrace) << " meta: " << RU(meta) << std::endl;
  v8::TryCatch try_catch(isolate_);

  v8::Local<v8::Value> args[1];
  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, meta)), &args[0])) {
    return kToLocalFailed;
  }

  currently_processed_vb_ = vb_no;
  currently_processed_seqno_ = seq_no;
  vb_seq_[vb_no]->store(seq_no, std::memory_order_seq_cst);
  UpdateBucketopsSeqno(vb_no, seq_no);
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
    return DebugExecute("OnDelete", args, 1) ? kSuccess : kOnDeleteCallFail;
  }

  auto on_doc_delete = on_delete_.Get(isolate_);

  execute_flag_ = true;
  execute_start_time_ = Time::now();
  RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                        IsTerminatingRetriable, IsExecutionTerminating,
                        isolate_);

  on_doc_delete->Call(context->Global(), 1, args);
  execute_flag_ = false;
  if (try_catch.HasCaught()) {
    LOG(logDebug) << "OnDelete Exception: "
                  << ExceptionString(isolate_, &try_catch) << std::endl;
    UpdateHistogram(start_time);
    on_delete_failure++;
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

  if (debugger_started_) {
    if (!agent_->IsStarted()) {
      agent_->Start(isolate_, platform_, src_path_.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    DebugExecute(callback.c_str(), arg, 1);
  }

  execute_flag_ = true;
  execute_start_time_ = Time::now();
  RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                        IsTerminatingRetriable, IsExecutionTerminating,
                        isolate_);

  callback_func->Call(callback_func_val, 1, arg);
  execute_flag_ = false;
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

void V8Worker::Enqueue(header_t *h, message_t *p) {
  std::string key, val;

  worker_msg_t msg;
  msg.header = h;
  msg.payload = p;
  LOG(logTrace) << "Inserting event: " << static_cast<int16_t>(h->event)
                << " opcode: " << static_cast<int16_t>(h->opcode)
                << " partition: " << h->partition
                << " metadata: " << RU(h->metadata) << std::endl;
  worker_queue_->Push(msg);
}

std::string V8Worker::CompileHandler(std::string handler) {
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);
  auto info_obj = v8::Object::New(isolate_);

  CompilationInfo info;

  try {
    auto transpiler = UnwrapData(isolate_)->transpiler;
    info = transpiler->Compile(handler);
    Transpiler::LogCompilationInfo(info);

    info_obj->Set(v8Str(isolate_, "language"), v8Str(isolate_, info.language));
    info_obj->Set(v8Str(isolate_, "compile_success"),
                  v8::Boolean::New(isolate_, info.compile_success));
    info_obj->Set(v8Str(isolate_, "index"),
                  v8::Int32::New(isolate_, info.index));
    info_obj->Set(v8Str(isolate_, "line_number"),
                  v8::Int32::New(isolate_, info.line_no));
    info_obj->Set(v8Str(isolate_, "column_number"),
                  v8::Int32::New(isolate_, info.col_no));
    info_obj->Set(v8Str(isolate_, "description"),
                  v8Str(isolate_, info.description));
    info_obj->Set(v8Str(isolate_, "area"), v8Str(isolate_, info.area));
  } catch (const char *e) {
    LOG(logError) << e << std::endl;
    return "";
  }

  if (info.compile_success) {
    try {
      auto ident = IdentifyVersion(handler);
      info_obj->Set(v8Str(isolate_, "version"), v8Str(isolate_, ident.version));
      info_obj->Set(v8Str(isolate_, "level"), v8Str(isolate_, ident.level));
      info_obj->Set(v8Str(isolate_, "using_timer"),
                    v8Str(isolate_, ident.using_timer));
    } catch (const char *e) {
      LOG(logError) << "Unable to identify version, ignoring:" << e
                    << std::endl;
    }
  }

  return JSONStringify(isolate_, info_obj);
}

CodeVersion V8Worker::IdentifyVersion(std::string handler) {
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  auto transpiler = UnwrapData(isolate_)->transpiler;
  v8::Context::Scope context_scope(context);

  auto uniline_info = transpiler->UniLineN1QL(handler);
  if (uniline_info.code != Jsify::kOK) {
    throw "Unline N1QL failed when trying to identify version";
  }

  auto jsify_info = Jsify(handler, true, cb_source_bucket_);
  if (jsify_info.code != Jsify::kOK) {
    throw "Jsify failed when trying to identify version";
  }

  auto script_to_execute =
      transpiler->Transpile(jsify_info.handler_code, app_name_ + ".js",
                            uniline_info.handler_code) +
      '\n';
  script_to_execute += std::string((const char *)js_builtin) + '\n';

  auto ver = transpiler->GetCodeVersion(script_to_execute);
  return ver;
}

void V8Worker::GetTimerMessages(std::vector<uv_buf_t> &messages,
                                size_t window_size) {
  int64_t timer_count =
      std::min(timer_queue_->Count(), static_cast<int64_t>(window_size));

  for (int64_t idx = 0; idx < timer_count; ++idx) {
    timer_msg_t timer_msg;
    if (!timer_queue_->Pop(timer_msg))
      break;
    auto curr_messages =
        BuildResponse(timer_msg.timer_entry, mTimer_Response, timerResponse);
    for (auto &msg : curr_messages) {
      messages.push_back(msg);
    }
  }
}

void V8Worker::GetBucketOpsMessages(std::vector<uv_buf_t> &messages) {
  for (int vb = 0; vb < NUM_VBUCKETS; ++vb) {
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
                            int64_t &seq_no) {
  int skip_ack;
  return ParseMetadataWithAck(metadata, vb_no, seq_no, skip_ack, false);
}

int V8Worker::ParseMetadataWithAck(const std::string &metadata, int &vb_no,
                                   int64_t &seq_no, int &skip_ack,
                                   bool ack_check) {
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  v8::Local<v8::Value> metadata_val;
  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, metadata)),
                &metadata_val)) {
    return kToLocalFailed;
  }

  v8::Local<v8::Object> metadata_obj;

  if (!TO_LOCAL(metadata_val->ToObject(context), &metadata_obj)) {
    return kToLocalFailed;
  }

  v8::Local<v8::Value> seq_val;
  if (!TO_LOCAL(metadata_obj->Get(context, v8Str(isolate_, "seq")), &seq_val)) {
    return kToLocalFailed;
  }

  v8::Local<v8::Value> vb_val;
  if (!TO_LOCAL(metadata_obj->Get(context, v8Str(isolate_, "vb")), &vb_val)) {
    return kToLocalFailed;
  }

  v8::Local<v8::Value> skip_ack_val;
  if (ack_check) {
    if (!TO_LOCAL(metadata_obj->Get(context, v8Str(isolate_, "skip_ack")),
                  &skip_ack_val)) {
      return kToLocalFailed;
    }
  }

  if (seq_val->IsNumber() && vb_val->IsNumber()) {
    v8::Local<v8::Integer> vb_val_int;
    if (!TO_LOCAL(vb_val->ToInteger(context), &vb_val_int)) {
      return kToLocalFailed;
    }

    v8::Local<v8::Integer> seq_val_int;
    if (!TO_LOCAL(seq_val->ToInteger(context), &seq_val_int)) {
      return kToLocalFailed;
    }

    v8::Local<v8::Integer> skip_ack_int;
    if (ack_check) {
      if (!skip_ack_val->IsNumber()) {
        return kToLocalFailed;
      }

      if (!TO_LOCAL(skip_ack_val->ToInteger(context), &skip_ack_int)) {
        return kToLocalFailed;
      }
    }

    vb_no = vb_val_int->Value();
    seq_no = seq_val_int->Value();

    if (ack_check) {
      skip_ack = skip_ack_int->Value();
    }

    return kSuccess;
  }

  return kToLocalFailed;
}

int V8Worker::UpdateVbFilter(const std::string &metadata) {
  int vb_no = 0;
  int64_t seq_no = 0;
  auto update_status = ParseMetadata(metadata, vb_no, seq_no);
  if (update_status != kSuccess) {
    return update_status;
  }
  std::lock_guard<std::mutex> lock(vbfilter_lock_);
  vbfilter_map_[vb_no] = seq_no;
  return kSuccess;
}

int64_t V8Worker::GetVbFilter(int vb_no) {
  std::lock_guard<std::mutex> lock(vbfilter_lock_);
  return vbfilter_map_[vb_no];
}

void V8Worker::EraseVbFilter(int vb_no) {
  std::lock_guard<std::mutex> lock(vbfilter_lock_);
  vbfilter_map_[vb_no] = -1;
}

void V8Worker::UpdateBucketopsSeqno(int vb_no, int64_t seq_no) {
  std::lock_guard<std::mutex> lock(bucketops_lock_);
  processed_bucketops_[vb_no] = seq_no;
}

int64_t V8Worker::GetBucketopsSeqno(int vb_no) {
  // Reset the seq no of checkpointed vb to 0
  vb_seq_[vb_no]->store(0, std::memory_order_seq_cst);
  std::lock_guard<std::mutex> lock(bucketops_lock_);
  return processed_bucketops_[vb_no];
}

void V8Worker::SetThreadExitFlag() {
  thread_exit_cond_.store(true);
  timer_queue_->Close();
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

// TODO : Remove this when stats variables are handled properly
void AddLcbException(const IsolateData *isolate_data,
                     const lcb_RESPN1QL *resp) {
  auto w = isolate_data->v8worker;
  w->AddLcbException(static_cast<int>(resp->rc));
}

void AddLcbException(const IsolateData *isolate_data, lcb_error_t error) {
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