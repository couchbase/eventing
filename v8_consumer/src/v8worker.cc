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
#include "parse_deployment.h"
#include "retry_util.h"
#include "timer.h"
#include "utils.h"

#include "../../gen/js/builtin.h"

bool V8Worker::debugger_started_ = false;

std::atomic<int64_t> bucket_op_exception_count = {0};
std::atomic<int64_t> n1ql_op_exception_count = {0};
std::atomic<int64_t> timeout_count = {0};
std::atomic<int16_t> checkpoint_failure_count = {0};

std::atomic<int64_t> on_update_success = {0};
std::atomic<int64_t> on_update_failure = {0};
std::atomic<int64_t> on_delete_success = {0};
std::atomic<int64_t> on_delete_failure = {0};

std::atomic<int64_t> timer_create_failure = {0};
std::atomic<int64_t> timer_alarm_delete_failure = {0};
std::atomic<int64_t> timer_context_delete_failure = {0};
std::atomic<int64_t> lcb_retry_failure = {0};

std::atomic<int64_t> messages_processed_counter = {0};

std::atomic<int64_t> dcp_delete_msg_counter = {0};
std::atomic<int64_t> dcp_mutation_msg_counter = {0};
std::atomic<int64_t> timer_msg_counter = {0};

std::atomic<int64_t> enqueued_dcp_delete_msg_counter = {0};
std::atomic<int64_t> enqueued_dcp_mutation_msg_counter = {0};
std::atomic<int64_t> enqueued_timer_msg_counter = {0};

const char *GetUsernameCbBucket(void *cookie, const char *host,
                                const char *port, const char *bucket) {
  LOG(logDebug) << "Getting username for host " << RS(host) << " port " << port
                << std::endl;

  auto comm = static_cast<Communicator *>(cookie);
  auto endpoint = JoinHostPort(host, port);
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

const char *GetPasswordCbBucket(void *cookie, const char *host,
                                const char *port, const char *bucket) {
  LOG(logDebug) << "Getting password for host " << RS(host) << " port " << port
                << std::endl;

  auto comm = static_cast<Communicator *>(cookie);
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

const char *GetUsernameCached(void *cookie, const char *host, const char *port,
                              const char *bucket) {
  auto isolate = static_cast<v8::Isolate *>(cookie);
  auto comm = UnwrapData(isolate)->comm;
  auto endpoint = JoinHostPort(host, port);
  auto info = comm->GetCredsCached(endpoint);
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

const char *GetPasswordCached(void *cookie, const char *host, const char *port,
                              const char *bucket) {
  auto isolate = static_cast<v8::Isolate *>(cookie);
  auto comm = UnwrapData(isolate)->comm;
  auto endpoint = JoinHostPort(host, port);
  auto info = comm->GetCredsCached(endpoint);
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

V8Worker::V8Worker(v8::Platform *platform, handler_config_t *h_config,
                   server_settings_t *server_settings,
                   const std::string &handler_name,
                   const std::string &handler_uuid,
                   const std::string &user_prefix, CbBucket *metadata_bucket)
    : app_name_(h_config->app_name), settings_(server_settings),
      vb_seq_(NUM_VBUCKETS, AtomicInt64(0)),
      vb_seq_validity_(NUM_VBUCKETS, AtomicBool(false)),
      bucketop_filters_(NUM_VBUCKETS, AtomicInt64(0)),
      bucketop_filters_validity_(NUM_VBUCKETS, AtomicBool(false)),
      processed_bucketops_(NUM_VBUCKETS, AtomicInt64(0)),
      timer_filters_(NUM_VBUCKETS, AtomicBool(false)), platform_(platform),
      handler_name_(handler_name), handler_uuid_(handler_uuid),
      user_prefix_(user_prefix), metadata_bucket_(metadata_bucket) {
  deployment_config *config = ParseDeployment(h_config->dep_cfg.c_str());
  curl_timeout = h_config->curl_timeout;
  histogram_ = new Histogram(HIST_FROM, HIST_TILL, HIST_WIDTH);
  thread_exit_cond_.store(false);
  v8::Isolate::CreateParams create_params;
  create_params.array_buffer_allocator =
      v8::ArrayBuffer::Allocator::NewDefaultAllocator();

  isolate_ = v8::Isolate::New(create_params);
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  isolate_->SetData(DATA_SLOT, &data_);
  isolate_->SetCaptureStackTraceForUncaughtExceptions(true);
  data_.v8worker = this;

  curl_global_init(CURL_GLOBAL_ALL);
  CURL *curl = curl_easy_init();
  if (curl) {
    UnwrapData(isolate_)->curl_handle = curl;
  }

  auto global = v8::ObjectTemplate::New(isolate_);
  v8::TryCatch try_catch;

  global->Set(v8::String::NewFromUtf8(isolate_, "curl"),
              v8::FunctionTemplate::New(isolate_, Curl));
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

  if (try_catch.HasCaught()) {
    LOG(logError) << "Exception logged:"
                  << ExceptionString(isolate_, &try_catch) << std::endl;
  }

  auto context = v8::Context::New(isolate_, nullptr, global);
  context_.Reset(isolate_, context);
  js_exception_ = new JsException(isolate_);
  data_.js_exception = js_exception_;

  auto ssl = false;
  auto port = server_settings->eventing_port;

  auto key = GetLocalKey();
  data_.comm = new Communicator(server_settings->host_addr, port, key.first,
                                key.second, ssl, app_name_);

  data_.transpiler =
      new Transpiler(isolate_, GetTranspilerSrc(), h_config->handler_headers,
                     h_config->handler_footers);
  data_.utils = new Utils(isolate_, context);
  data_.timer = new Timer(isolate_, context);
  execute_start_time_ = Time::now();

  cb_source_bucket_.assign(config->source_bucket);

  Bucket *bucket_handle = nullptr;
  execute_flag_ = false;
  shutdown_terminator_ = false;
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

          bucket_handle = new Bucket(
              this, bucket_name.c_str(), settings_->kv_host_port.c_str(),
              bucket_alias.c_str(), cb_source_bucket_ == bucket_name);

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
               << " curl_timeout: " << curl_timeout
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

  auto data = UnwrapData(isolate_);
  delete data->comm;
  delete data->transpiler;
  delete data->utils;
  delete data->timer;

  curl_global_cleanup();
  context_.Reset();
  on_update_.Reset();
  on_delete_.Reset();
  delete conn_pool_;
  delete n1ql_handle_;
  delete settings_;
  delete histogram_;
  delete js_exception_;
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

  auto uniline_info = transpiler->UniLineN1QL(script_to_execute);
  LOG(logInfo) << "code after Unilining N1QL: " << RM(uniline_info.handler_code)
               << std::endl;
  if (uniline_info.code != kOK) {
    LOG(logError) << "failed to uniline N1QL: " << RM(uniline_info.code)
                  << std::endl;
    return uniline_info.code;
  }

  auto jsify_info = Jsify(script_to_execute);
  LOG(logInfo) << "jsified code: " << RM(jsify_info.handler_code) << std::endl;
  if (jsify_info.code != kOK) {
    LOG(logError) << "failed to jsify: " << RM(jsify_info.handler_code)
                  << std::endl;
    return jsify_info.code;
  }

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
        if (!(*bucket_handle)->Initialize(this)) {
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
  std::string val;

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
          auto is_valid = bucketop_filters_validity_[vb_no].Get();
          auto filter_seq_no = bucketop_filters_[vb_no].Get();
          if (is_valid && seq_no <= filter_seq_no) {
            if (seq_no == filter_seq_no) {
              bucketop_filters_validity_[vb_no].Set(false);
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
          auto is_valid = bucketop_filters_validity_[vb_no].Get();
          auto filter_seq_no = bucketop_filters_[vb_no].Get();
          if (is_valid && seq_no <= filter_seq_no) {
            if (seq_no == filter_seq_no) {
              bucketop_filters_validity_[vb_no].Set(false);
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
        vb_no = msg.header->partition;
        if (timer_filters_[vb_no].Get() == false) {
          payload = flatbuf::payload::GetPayload(
              (const void *)msg.payload->payload.c_str());
          TimerEvent event(payload);
          timer_msg_counter++;
          this->SendTimer(event);
        }
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
  vb_seq_[vb_no].Set(vb_no);
  vb_seq_validity_[vb_no].Set(true);
  processed_bucketops_[vb_no].Set(seq_no);
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
  vb_seq_[vb_no].Set(vb_no);
  vb_seq_validity_[vb_no].Set(true);
  processed_bucketops_[vb_no].Set(seq_no);
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

void V8Worker::SendTimer(const TimerEvent &event) {
  LOG(logTrace) << "Got timer event, context:" << RU(event.context)
                << " callback:" << RU(event.callback) << std::endl;

  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  v8::Local<v8::Value> timer_ctx_val;
  v8::Local<v8::Value> arg[1];

  if (event.context == "undefined") {
    arg[0] = v8::Undefined(isolate_);
  } else {
    if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, event.context)),
                  &timer_ctx_val)) {
      return;
    }
    arg[0] = timer_ctx_val;
  }

  auto utils = UnwrapData(isolate_)->utils;
  auto callback_func_val = utils->GetPropertyFromGlobal(event.callback);
  auto callback_func = callback_func_val.As<v8::Function>();

  if (debugger_started_) {
    if (!agent_->IsStarted()) {
      agent_->Start(isolate_, platform_, src_path_.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    DebugExecute(event.callback.c_str(), arg, 1);
    return;
  }

  execute_flag_ = true;
  execute_start_time_ = Time::now();
  RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                        IsTerminatingRetriable, IsExecutionTerminating,
                        isolate_);

  v8::TryCatch try_catch(isolate_);
  callback_func->Call(callback_func_val, 1, arg);
  execute_flag_ = false;
  if (try_catch.HasCaught()) {
    LOG(logDebug) << "SendTimer exception"
                  << ExceptionString(isolate_, &try_catch) << std::endl;
    return;
  }

  auto info = metadata_bucket_->Delete(event.alarm_key, event.alarm_cas);
  if (!info.success) {
    ++timer_alarm_delete_failure;
  }

  info = metadata_bucket_->Delete(event.context_key, event.context_cas);
  if (!info.success) {
    ++timer_context_delete_failure;
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

  agent_ = new inspector::Agent(settings_->host_addr,
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
  if (uniline_info.code != kOK) {
    throw "Unline N1QL failed when trying to identify version";
  }

  auto jsify_info = Jsify(handler);
  if (jsify_info.code != kOK) {
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
    if (vb_seq_validity_[vb].Get()) {
      std::string seq_no =
          std::to_string(vb) + "::" + std::to_string(vb_seq_[vb].Get());
      auto curr_messages =
          BuildResponse(seq_no, mBucket_Ops_Response, checkpointResponse);
      for (auto &msg : curr_messages) {
        messages.push_back(msg);
      }
      // Reset the validity of checkpointed vb
      ResetCheckpoint(vb);
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

  if (seq_val->IsNumber() && vb_val->IsNumber()) {
    v8::Local<v8::Integer> vb_val_int;
    if (!TO_LOCAL(vb_val->ToInteger(context), &vb_val_int)) {
      return kToLocalFailed;
    }

    v8::Local<v8::Integer> seq_val_int;
    if (!TO_LOCAL(seq_val->ToInteger(context), &seq_val_int)) {
      return kToLocalFailed;
    }

    vb_no = vb_val_int->Value();
    seq_no = seq_val_int->Value();
  }
  return kSuccess;
}

int64_t V8Worker::GetBucketopsSeqno(int vb_no) {
  return processed_bucketops_[vb_no].Get();
}

void V8Worker::UpdateBucketopsSeqno(int vb_no, int64_t seq_no) {
  processed_bucketops_[vb_no].Set(seq_no);
}

void V8Worker::ResetCheckpoint(int vb_no) {
  vb_seq_validity_[vb_no].Set(false);
}

void V8Worker::SetBucketopFilter(int vb_no, int64_t seq_no) {
  bucketop_filters_[vb_no].Set(seq_no);
  bucketop_filters_validity_[vb_no].Set(true);
}

void V8Worker::SetTimerFilter(int vb_no) { timer_filters_[vb_no].Set(true); }

void V8Worker::ClearTimerFilter(int vb_no) { timer_filters_[vb_no].Set(false); }

void V8Worker::SetThreadExitFlag() {
  thread_exit_cond_.store(true);
  timer_queue_->Close();
  worker_queue_->Close();
}
