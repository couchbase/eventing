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
#include "utils.h"

#include "../../gen/js/builtin.h"

bool debugger_started = false;
bool enable_recursive_mutation = false;

std::atomic<int64_t> bucket_op_exception_count = {0};
std::atomic<int64_t> n1ql_op_exception_count = {0};
std::atomic<int64_t> timeout_count = {0};
std::atomic<int16_t> checkpoint_failure_count = {0};

std::atomic<int64_t> on_update_success = {0};
std::atomic<int64_t> on_update_failure = {0};
std::atomic<int64_t> on_delete_success = {0};
std::atomic<int64_t> on_delete_failure = {0};

std::atomic<int64_t> doc_timer_create_failure = {0};

std::atomic<int64_t> messages_processed_counter = {0};

std::atomic<int64_t> cron_timer_msg_counter = {0};
std::atomic<int64_t> dcp_delete_msg_counter = {0};
std::atomic<int64_t> dcp_mutation_msg_counter = {0};
std::atomic<int64_t> doc_timer_msg_counter = {0};

std::atomic<int64_t> enqueued_cron_timer_msg_counter = {0};
std::atomic<int64_t> enqueued_dcp_delete_msg_counter = {0};
std::atomic<int64_t> enqueued_dcp_mutation_msg_counter = {0};
std::atomic<int64_t> enqueued_doc_timer_msg_counter = {0};

enum RETURN_CODE {
  kSuccess = 0,
  kFailedToCompileJs,
  kNoHandlersDefined,
  kFailedInitBucketHandle,
  kOnUpdateCallFail,
  kOnDeleteCallFail,
  kToLocalFailed
};

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

void get_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  // lcb_get calls against metadata bucket is only triggered for timer lookups
  auto rg = reinterpret_cast<const lcb_RESPGET *>(rb);
  const void *data = lcb_get_cookie(instance);

  std::string ts;
  std::string timestamp_marker("");
  lcb_CMDSTORE acmd = {0};
  Result res;

  switch (rb->rc) {
  case LCB_KEY_ENOENT:
    ts.assign(reinterpret_cast<const char *>(data));

    LCB_CMD_SET_KEY(&acmd, ts.c_str(), ts.length());
    LCB_CMD_SET_VALUE(&acmd, timestamp_marker.c_str(),
                      timestamp_marker.length());
    acmd.operation = LCB_ADD;

    lcb_store3(instance, &res, &acmd);
    lcb_wait(instance);
    break;
  case LCB_SUCCESS:
    LOG(logTrace) << "NValue "
                  << RU(std::to_string(static_cast<int>(rg->nvalue)))
                  << "Value " << RU(reinterpret_cast<const char *>(rg->value));
    break;
  default:
    LOG(logTrace) << "LCB_CALLBACK_GET: Operation failed, "
                  << lcb_strerror(nullptr, rb->rc) << " rc:" << rb->rc
                  << std::endl;
    break;
  }
}

void set_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  auto rs = reinterpret_cast<const lcb_RESPSTORE *>(rb);
  auto result = reinterpret_cast<Result *>(rb->cookie);
  result->rc = rs->rc;
}

void sdmutate_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  auto res = reinterpret_cast<Result *>(rb->cookie);
  res->rc = rb->rc;
}

void sdlookup_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  auto *res = reinterpret_cast<Result *>(rb->cookie);
  res->cas = rb->cas;
  res->rc = rb->rc;

  if (rb->rc == LCB_SUCCESS) {
    auto rg = reinterpret_cast<const lcb_RESPGET *>(rb);
    res->value.assign(reinterpret_cast<const char *>(rg->value), rg->nvalue);

    auto resp = reinterpret_cast<const lcb_RESPSUBDOC *>(rb);
    lcb_SDENTRY ent;
    size_t iter = 0;
    int index = 0;
    while (lcb_sdresult_next(resp, &ent, &iter)) {
      res->value.assign(reinterpret_cast<const char *>(ent.value),
                        static_cast<int>(ent.nvalue));

      if (index == 0) {
        std::string exptime(reinterpret_cast<const char *>(ent.value));
        exptime.substr(0, static_cast<int>(ent.nvalue));

        unsigned long long int ttl;
        char *pEnd;
        ttl = strtoull(exptime.c_str(), &pEnd, 10);
        res->exptime = (uint32_t)ttl;
      }

      if (index == 1) {
        res->value.assign(reinterpret_cast<const char *>(ent.value),
                          static_cast<int>(ent.nvalue));
      }
      index++;
    }
  }
}

void startDebuggerFlag(bool started) {
  LOG(logInfo) << "debugger_started flag: " << debugger_started << std::endl;
  debugger_started = started;

  // Disable logging when inspector is running
  if (started) {
    SystemLog::setLogLevel(logSilent);
  }
}

V8Worker::V8Worker(v8::Platform *platform, handler_config_t *h_config,
                   server_settings_t *server_settings,
                   const std::string &handler_name,
                   const std::string &handler_uuid)
    : app_name_(h_config->app_name), settings_(server_settings),
      platform_(platform), handler_name_(handler_name),
      handler_uuid_(handler_uuid) {
  enable_recursive_mutation = h_config->enable_recursive_mutation;
  curl_timeout = h_config->curl_timeout;
  histogram_ = new Histogram(HIST_FROM, HIST_TILL, HIST_WIDTH);

  for (int i = 0; i < NUM_VBUCKETS; i++) {
    vb_seq_[i] = atomic_ptr_t(new std::atomic<int64_t>(0));
  }

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
  global->Set(v8::String::NewFromUtf8(isolate_, "docTimer"),
              v8::FunctionTemplate::New(isolate_, CreateDocTimer));
  global->Set(v8::String::NewFromUtf8(isolate_, "cronTimer"),
              v8::FunctionTemplate::New(isolate_, CreateCronTimer));
  global->Set(v8::String::NewFromUtf8(isolate_, "iter"),
              v8::FunctionTemplate::New(isolate_, IterFunction));
  global->Set(v8::String::NewFromUtf8(isolate_, "stopIter"),
              v8::FunctionTemplate::New(isolate_, StopIterFunction));
  global->Set(v8::String::NewFromUtf8(isolate_, "execQuery"),
              v8::FunctionTemplate::New(isolate_, ExecQueryFunction));
  global->Set(v8::String::NewFromUtf8(isolate_, "getReturnValue"),
              v8::FunctionTemplate::New(isolate_, GetReturnValueFunction));

  if (try_catch.HasCaught()) {
    LOG(logError) << "Exception logged:"
                  << ExceptionString(isolate_, &try_catch) << std::endl;
  }

  auto context = v8::Context::New(isolate_, nullptr, global);
  context_.Reset(isolate_, context);
  js_exception_ = new JsException(isolate_);
  data_.js_exception = js_exception_;
  data_.cron_timers_per_doc = h_config->cron_timers_per_doc;

  auto ssl = false;
  auto port = server_settings->eventing_port;

  // Temporarily disabling ssl as requests to eventing-producer
  // are having problem with ssl.
  /*
  auto ssl = true;
  auto port = server_settings->eventing_sslport;
  if (port.length() < 1) {
    LOG(logError) << "SSL not available, using plain HTTP" << std::endl;
    port = server_settings->eventing_port;
    ssl = false;
  }*/

  auto key = GetLocalKey();
  data_.comm = new Communicator(server_settings->host_addr, port, key.first,
                                key.second, ssl);

  data_.transpiler =
      new Transpiler(isolate_, GetTranspilerSrc(), h_config->handler_headers,
                     h_config->handler_footers);
  data_.fuzz_offset = h_config->fuzz_offset;

  execute_start_time_ = Time::now();

  deployment_config *config = ParseDeployment(h_config->dep_cfg.c_str());

  cb_source_bucket_.assign(config->source_bucket);

  Bucket *bucket_handle = nullptr;
  execute_flag_ = false;
  shutdown_terminator_ = false;
  max_task_duration_ = SECS_TO_NS * h_config->execution_timeout;

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
               << " curr_host: " << RS(settings_->host_addr)
               << " cron_timers_per_doc: " << h_config->cron_timers_per_doc
               << " curr_eventing_port: " << RS(settings_->eventing_port)
               << " curr_eventing_sslport: " << RS(settings_->eventing_sslport)
               << " kv_host_port: " << RS(settings_->kv_host_port)
               << " lcb_cap: " << h_config->lcb_inst_capacity
               << " execution_timeout: " << h_config->execution_timeout
               << " fuzz offset: " << h_config->fuzz_offset
               << " enable_recursive_mutation: " << enable_recursive_mutation
               << " curl_timeout: " << curl_timeout
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

  this->doc_timer_queue_ = new Queue<doc_timer_msg_t>();
  this->worker_queue_ = new Queue<worker_msg_t>();

  std::thread r_thr(&V8Worker::RouteMessage, this);
  processing_thr_ = std::move(r_thr);
}

V8Worker::~V8Worker() {
  if (checkpointing_thr_.joinable()) {
    checkpointing_thr_.join();
  }

  if (processing_thr_.joinable()) {
    processing_thr_.join();
  }

  auto data = UnwrapData(isolate_);
  delete data->comm;
  delete data->transpiler;

  curl_global_cleanup();
  context_.Reset();
  on_update_.Reset();
  on_delete_.Reset();
  delete conn_pool_;
  delete n1ql_handle_;
  delete settings_;
  delete histogram_;
  delete js_exception_;
}

// TODO : Use v8::MaybeLocal types once MB-29560 is resolved
// Re-compile and execute handler code for debugger
bool V8Worker::DebugExecute(const char *func_name, v8::Local<v8::Value> *args,
                            int args_len) {
  v8::HandleScope handle_scope(isolate_);
  v8::TryCatch try_catch(isolate_);

  // Need to construct origin for source-map to apply.
  auto origin_v8_str = v8Str(isolate_, src_path_);
  v8::ScriptOrigin origin(origin_v8_str);
  auto context = context_.Get(isolate_);
  auto source = v8Str(isolate_, script_to_execute_);

  // Replace the usual log function with console.log
  auto global = context->Global();
  global->Set(v8Str(isolate_, "log"),
              v8::FunctionTemplate::New(isolate_, ConsoleLog)->GetFunction());

  v8::Local<v8::Script> script;
  if (!v8::Script::Compile(context, source, &origin).ToLocal(&script)) {
    return false;
  } else {
    v8::Local<v8::Value> result;
    if (!script->Run(context).ToLocal(&result)) {
      assert(try_catch.HasCaught());
      return false;
    } else {
      assert(!try_catch.HasCaught());
      auto func_v8_str = v8::String::NewFromUtf8(isolate_, func_name);
      auto func_ref = context->Global()->Get(func_v8_str);
      auto func = v8::Local<v8::Function>::Cast(func_ref);
      RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                            IsTerminatingRetriable, IsExecutionTerminating,
                            isolate_);

      func->Call(v8::Null(isolate_), args_len, args);
      if (try_catch.HasCaught()) {
        agent_->FatalException(try_catch.Exception(), try_catch.Message());
      }

      return true;
    }
  }
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
  LOG(logTrace) << "code after Unilining N1QL: "
                << RM(uniline_info.handler_code) << std::endl;
  if (uniline_info.code != kOK) {
    LOG(logError) << "failed to uniline N1QL: " << RM(uniline_info.code)
                  << std::endl;
    return uniline_info.code;
  }

  handler_code_ = uniline_info.handler_code;

  auto jsify_info = Jsify(script_to_execute);
  LOG(logTrace) << "jsified code: " << RM(jsify_info.handler_code) << std::endl;
  if (jsify_info.code != kOK) {
    LOG(logError) << "failed to jsify: " << RM(jsify_info.handler_code)
                  << std::endl;
    return jsify_info.code;
  }

  n1ql_handle_ = new N1QL(conn_pool_, isolate_);
  UnwrapData(isolate_)->n1ql_handle = n1ql_handle_;

  script_to_execute =
      transpiler->Transpile(jsify_info.handler_code, app_name_ + ".js",
                            app_name_ + ".map.json", settings_->host_addr,
                            settings_->eventing_port) +
      '\n';
  script_to_execute += std::string((const char *)js_builtin) + '\n';
  source_map_ =
      transpiler->GetSourceMap(jsify_info.handler_code, app_name_ + ".js");
  LOG(logTrace) << "source map:" << RM(source_map_) << std::endl;

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

  auto auth = lcbauth_new();
  lcbauth_set_callbacks(auth, isolate_, GetUsername, GetPassword);
  lcbauth_set_mode(auth, LCBAUTH_MODE_DYNAMIC);

  lcb_U32 lcb_timeout = 2500000; // 2.5s

  if (transpiler->IsTimerCalled(script_to_execute)) {
    LOG(logDebug) << "Timer is called" << std::endl;

    lcb_create_st crst;
    memset(&crst, 0, sizeof crst);

    crst.version = 3;
    crst.v.v3.connstr = connstr_.c_str();
    crst.v.v3.type = LCB_TYPE_BUCKET;
    crst.v.v3.passwd = settings_->rbac_pass.c_str();

    lcb_create(&cb_instance_, &crst);
    lcb_set_auth(cb_instance_, auth);
    lcb_error_t rc = lcb_connect(cb_instance_);
    LOG(logDebug) << "LCB_CONNECT to " << RS(cb_instance_) << " returns " << rc
                  << std::endl;
    lcb_wait(cb_instance_);

    lcb_install_callback3(cb_instance_, LCB_CALLBACK_GET, get_callback);
    lcb_install_callback3(cb_instance_, LCB_CALLBACK_STORE, set_callback);
    lcb_install_callback3(cb_instance_, LCB_CALLBACK_SDMUTATE,
                          sdmutate_callback);
    lcb_install_callback3(cb_instance_, LCB_CALLBACK_SDLOOKUP,
                          sdlookup_callback);
    lcb_cntl(cb_instance_, LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT, &lcb_timeout);
    UnwrapData(isolate_)->cb_instance = cb_instance_;

    memset(&crst, 0, sizeof crst);

    crst.version = 3;
    crst.v.v3.connstr = meta_connstr_.c_str();
    crst.v.v3.type = LCB_TYPE_BUCKET;
    crst.v.v3.passwd = settings_->rbac_pass.c_str();

    lcb_create(&meta_cb_instance_, &crst);
    lcb_set_auth(meta_cb_instance_, auth);
    lcb_connect(meta_cb_instance_);
    lcb_wait(meta_cb_instance_);

    lcb_install_callback3(meta_cb_instance_, LCB_CALLBACK_GET, get_callback);
    lcb_install_callback3(meta_cb_instance_, LCB_CALLBACK_STORE, set_callback);
    lcb_install_callback3(meta_cb_instance_, LCB_CALLBACK_SDMUTATE,
                          sdmutate_callback);
    lcb_install_callback3(meta_cb_instance_, LCB_CALLBACK_SDLOOKUP,
                          sdlookup_callback);
    lcb_cntl(meta_cb_instance_, LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT,
             &lcb_timeout);

    UnwrapData(isolate_)->meta_cb_instance = meta_cb_instance_;
  }

  lcb_create_st crst;

  memset(&crst, 0, sizeof crst);

  crst.version = 3;
  crst.v.v3.connstr = meta_connstr_.c_str();
  crst.v.v3.type = LCB_TYPE_BUCKET;
  crst.v.v3.passwd = settings_->rbac_pass.c_str();

  lcb_create(&checkpoint_cb_instance_, &crst);
  lcb_set_auth(checkpoint_cb_instance_, auth);
  lcb_connect(checkpoint_cb_instance_);
  lcb_wait(checkpoint_cb_instance_);

  lcb_install_callback3(checkpoint_cb_instance_, LCB_CALLBACK_GET,
                        get_callback);
  lcb_install_callback3(checkpoint_cb_instance_, LCB_CALLBACK_STORE,
                        set_callback);
  lcb_install_callback3(checkpoint_cb_instance_, LCB_CALLBACK_SDMUTATE,
                        sdmutate_callback);
  lcb_install_callback3(checkpoint_cb_instance_, LCB_CALLBACK_SDLOOKUP,
                        sdlookup_callback);
  lcb_cntl(checkpoint_cb_instance_, LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT,
           &lcb_timeout);

  // Spawning terminator thread to monitor the wall clock time for execution
  // of javascript code isn't going beyond max_task_duration. Passing
  // reference to current object instead of having terminator thread make a
  // copy of the object. Spawned thread will execute the terminator loop logic
  // in function call operator() for V8Worker class
  terminator_thr_ = new std::thread(std::ref(*this));

  std::thread c_thr(&V8Worker::Checkpoint, this);
  checkpointing_thr_ = std::move(c_thr);

  return kSuccess;
}

void V8Worker::Checkpoint() {
  const auto checkpoint_interval =
      std::chrono::milliseconds(settings_->checkpoint_interval);

  while (true) {
    std::string doc_timer_path("last_processed_doc_id_timer_event");

    doc_timer_mtx_.lock();
    std::map<int, std::string> curr_dtimer_checkpoint(
        doc_timer_checkpoint_.begin(), doc_timer_checkpoint_.end());
    doc_timer_mtx_.unlock();

    for (auto &vbTimer : curr_dtimer_checkpoint) {
      std::stringstream vb_key;
      vb_key << app_name_ << "::vb::" << vbTimer.first;

      lcb_CMDSUBDOC cmd = {0};
      LCB_CMD_SET_KEY(&cmd, vb_key.str().c_str(), vb_key.str().length());

      lcb_SDSPEC dtimer_spec = {0};
      dtimer_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
      dtimer_spec.options = LCB_SDSPEC_F_MKINTERMEDIATES;

      // Prepend and append '"' to make value a valid json
      vbTimer.second.insert(0, 1, '"');
      vbTimer.second.append("\"");

      LCB_SDSPEC_SET_PATH(&dtimer_spec, doc_timer_path.c_str(),
                          doc_timer_path.length());
      LCB_SDSPEC_SET_VALUE(&dtimer_spec, vbTimer.second.c_str(),
                           vbTimer.second.length());

      cmd.specs = &dtimer_spec;
      cmd.nspecs = 1;

      Result cres;
      lcb_subdoc3(checkpoint_cb_instance_, &cres, &cmd);
      lcb_wait(checkpoint_cb_instance_);

      if (cres.rc == LCB_SUCCESS) {
        doc_timer_mtx_.lock();
        if (vbTimer.second.compare(1, vbTimer.second.length() - 2,
                                   doc_timer_checkpoint_[vbTimer.first]) == 0) {
          doc_timer_checkpoint_.erase(vbTimer.first);
        }
        doc_timer_mtx_.unlock();
      }

      auto sleep_duration = LCB_OP_RETRY_INTERVAL;

      while (cres.rc != LCB_SUCCESS) {
        checkpoint_failure_count++;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
        sleep_duration *= 1.5;

        if (sleep_duration > 5000) {
          sleep_duration = 5000;
        };

        lcb_subdoc3(checkpoint_cb_instance_, &cres, &cmd);
        lcb_wait(checkpoint_cb_instance_);
      }
    }

    std::string cron_timer_path("last_processed_cron_timer_event");

    cron_timer_mtx_.lock();
    std::map<int, std::string> curr_ctimer_checkpoint(
        cron_timer_checkpoint_.begin(), cron_timer_checkpoint_.end());
    cron_timer_mtx_.unlock();

    for (auto &vbTimer : curr_ctimer_checkpoint) {
      std::stringstream vb_key;
      vb_key << app_name_ << "::vb::" << vbTimer.first;

      lcb_CMDSUBDOC cmd = {0};
      LCB_CMD_SET_KEY(&cmd, vb_key.str().c_str(), vb_key.str().length());

      lcb_SDSPEC ctimer_spec = {0};
      ctimer_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
      ctimer_spec.options = LCB_SDSPEC_F_MKINTERMEDIATES;

      // Prepend and append '"' to make value a valid json
      vbTimer.second.insert(0, 1, '"');
      vbTimer.second.append("\"");

      LCB_SDSPEC_SET_PATH(&ctimer_spec, cron_timer_path.c_str(),
                          cron_timer_path.length());
      LCB_SDSPEC_SET_VALUE(&ctimer_spec, vbTimer.second.c_str(),
                           vbTimer.second.length());

      cmd.specs = &ctimer_spec;
      cmd.nspecs = 1;

      Result cres;
      lcb_subdoc3(checkpoint_cb_instance_, &cres, &cmd);
      lcb_wait(checkpoint_cb_instance_);

      if (cres.rc == LCB_SUCCESS) {
        cron_timer_mtx_.lock();
        if (vbTimer.second.compare(1, vbTimer.second.length() - 2,
                                   cron_timer_checkpoint_[vbTimer.first]) ==
            0) {
          cron_timer_checkpoint_.erase(vbTimer.first);
        }
        cron_timer_mtx_.unlock();
      }

      auto sleep_duration = LCB_OP_RETRY_INTERVAL;

      while (cres.rc != LCB_SUCCESS) {
        checkpoint_failure_count++;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
        sleep_duration *= 1.5;

        if (sleep_duration > 5000) {
          sleep_duration = 5000;
        };

        lcb_subdoc3(checkpoint_cb_instance_, &cres, &cmd);
        lcb_wait(checkpoint_cb_instance_);
      }
    }

    std::this_thread::sleep_for(checkpoint_interval);
  }
}

void V8Worker::RouteMessage() {
  const flatbuf::payload::Payload *payload;
  std::string key, val, timer_ts, doc_id, callback_fn, cron_cb_fns, metadata;

  while (true) {
    auto msg = worker_queue_->Pop();
    payload = flatbuf::payload::GetPayload(
        (const void *)msg.payload->payload.c_str());

    LOG(logTrace) << " event: " << static_cast<int16_t>(msg.header->event)
                  << " opcode: " << static_cast<int16_t>(msg.header->opcode)
                  << " metadata: " << RU(msg.header->metadata)
                  << " partition: " << msg.header->partition << std::endl;

    switch (getEvent(msg.header->event)) {
    case eDCP:
      switch (getDCPOpcode(msg.header->opcode)) {
      case oDelete:
        dcp_delete_msg_counter++;
        this->SendDelete(msg.header->metadata);
        break;
      case oMutation:
        payload = flatbuf::payload::GetPayload(
            (const void *)msg.payload->payload.c_str());
        val.assign(payload->value()->str());
        metadata.assign(msg.header->metadata);
        dcp_mutation_msg_counter++;
        this->SendUpdate(val, metadata, "json");
        break;
      default:
        break;
      }
      break;
    case eTimer:
      switch (getTimerOpcode(msg.header->opcode)) {
      case oDocTimer:
        payload = flatbuf::payload::GetPayload(
            (const void *)msg.payload->payload.c_str());
        callback_fn.assign(payload->callback_fn()->str());
        doc_id.assign(payload->doc_id()->str());
        timer_ts.assign(payload->timer_ts()->str());
        doc_timer_msg_counter++;
        this->SendDocTimer(callback_fn, doc_id, timer_ts,
                           payload->timer_partition());
        break;

      case oCronTimer:
        payload = flatbuf::payload::GetPayload(
            (const void *)msg.payload->payload.c_str());
        cron_cb_fns.assign(payload->doc_ids_callback_fns()->str());
        timer_ts.assign(payload->timer_ts()->str());
        this->SendCronTimer(cron_cb_fns, timer_ts, payload->timer_partition());
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

  if (lcb_exceptions_.find(err_code) == lcb_exceptions_.end()) {
    lcb_exceptions_[err_code] = 0;
  }

  lcb_exceptions_[err_code]++;
}

void V8Worker::ListLcbExceptions(std::map<int, int64_t> &agg_lcb_exceptions) {
  std::lock_guard<std::mutex> lock(lcb_exception_mtx_);

  for (auto const &entry : lcb_exceptions_) {
    if (agg_lcb_exceptions.find(entry.first) == agg_lcb_exceptions.end()) {
      agg_lcb_exceptions[entry.first] = 0;
    }

    agg_lcb_exceptions[entry.first] += entry.second;
  }
}

void V8Worker::UpdateHistogram(Time::time_point start_time) {
  Time::time_point t = Time::now();
  nsecs ns = std::chrono::duration_cast<nsecs>(t - start_time);
  histogram_->Add(ns.count() / 1000);
}

int V8Worker::UpdateVbSeqNumbers(const v8::Local<v8::Value> &metadata) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  // Look for vbucket and corresponding seq no in metadata
  v8::Local<v8::Object> metadata_obj;
  if (!TO_LOCAL(metadata->ToObject(context), &metadata_obj)) {
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

    currently_processed_vb_ = vb_val_int->Value();
    currently_processed_seqno_ = seq_val_int->Value();
    vb_seq_[currently_processed_vb_]->store(currently_processed_seqno_,
                                            std::memory_order_seq_cst);
  }

  return kSuccess;
}

int V8Worker::SendUpdate(std::string value, std::string meta,
                         std::string doc_type) {
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

  auto update_status = UpdateVbSeqNumbers(args[1]);
  if (update_status != kSuccess) {
    return update_status;
  }

  if (on_update_.IsEmpty()) {
    UpdateHistogram(start_time);
    return kOnUpdateCallFail;
  }

  if (try_catch.HasCaught()) {
    APPLOG << "OnUpdate Exception: " << ExceptionString(isolate_, &try_catch)
           << std::endl;
  }

  if (debugger_started) {
    if (!agent_->IsStarted()) {
      agent_->Start(isolate_, platform_, src_path_.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    if (DebugExecute("OnUpdate", args, 2)) {
      return kSuccess;
    }
    return kOnUpdateCallFail;
  } else {
    auto on_doc_update = on_update_.Get(isolate_);
    execute_flag_ = true;
    execute_start_time_ = Time::now();
    RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                          IsTerminatingRetriable, IsExecutionTerminating,
                          isolate_);

    on_doc_update->Call(context->Global(), 2, args);
    execute_flag_ = false;
    if (try_catch.HasCaught()) {
      APPLOG << "OnUpdate Exception: " << ExceptionString(isolate_, &try_catch)
             << std::endl;
      UpdateHistogram(start_time);
      on_update_failure++;
      return kOnUpdateCallFail;
    }

    on_update_success++;
    UpdateHistogram(start_time);
    return kSuccess;
  }
}

int V8Worker::SendDelete(std::string meta) {
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

  auto update_status = UpdateVbSeqNumbers(args[0]);
  if (update_status != kSuccess) {
    return update_status;
  }

  if (on_delete_.IsEmpty()) {
    UpdateHistogram(start_time);
    return kOnDeleteCallFail;
  }

  assert(!try_catch.HasCaught());

  if (debugger_started) {
    if (!agent_->IsStarted()) {
      agent_->Start(isolate_, platform_, src_path_.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    if (DebugExecute("OnDelete", args, 1)) {
      return kSuccess;
    }
    return kOnDeleteCallFail;
  } else {
    auto on_doc_delete = on_delete_.Get(isolate_);

    execute_flag_ = true;
    execute_start_time_ = Time::now();
    RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                          IsTerminatingRetriable, IsExecutionTerminating,
                          isolate_);

    on_doc_delete->Call(context->Global(), 1, args);
    execute_flag_ = false;
    if (try_catch.HasCaught()) {
      APPLOG << "OnDelete Exception: " << ExceptionString(isolate_, &try_catch)
             << std::endl;
      UpdateHistogram(start_time);
      on_delete_failure++;
      return kOnDeleteCallFail;
    }

    UpdateHistogram(start_time);
    on_delete_success++;
    return kSuccess;
  }
}

void V8Worker::SendCronTimer(std::string cron_cb_fns, std::string timer_ts,
                             int32_t partition) {
  /*
   {"cron_timers":[
                   {"callback_func":"timerCallback1", "payload": "doc_id1"},
                   {"callback_func":"timerCallback2", "payload": "doc_id2"},
                   ...
                  ]
    ,"version":"vulcan"}
 */
  LOG(logTrace) << "cron timers: " << cron_cb_fns << std::endl;

  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  v8::Local<v8::Value> data;
  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, cron_cb_fns)),
                &data)) {
    return;
  }

  v8::Local<v8::Object> data_obj;
  if (!TO_LOCAL(data->ToObject(context), &data_obj)) {
    return;
  }

  v8::Local<v8::Value> cron_timer_entries;
  if (!TO_LOCAL(data_obj->Get(context, v8Str(isolate_, "cron_timers")),
                &cron_timer_entries)) {
    return;
  }

  if (cron_timer_entries->IsArray()) {
    v8::Local<v8::Object> entries;
    if (!TO_LOCAL(cron_timer_entries->ToObject(context), &entries)) {
      return;
    }

    auto entries_arr = entries.As<v8::Array>();
    auto callback_fn = v8Str(isolate_, "callback_func");
    auto payload = v8Str(isolate_, "payload");

    for (uint32_t i = 0; i < entries_arr->Length(); i++) {
      v8::Local<v8::Value> entry_val;
      if (!TO_LOCAL(entries_arr->Get(context, i), &entry_val)) {
        return;
      }

      v8::Local<v8::Object> entry_obj;
      if (!TO_LOCAL(entry_val->ToObject(context), &entry_obj)) {
        return;
      }

      v8::Local<v8::Value> cb_fn;
      if (!TO_LOCAL(entry_obj->Get(context, callback_fn), &cb_fn)) {
        return;
      }

      v8::Local<v8::Value> opaque;
      if (!TO_LOCAL(entry_obj->Get(context, payload), &opaque)) {
        return;
      }

      if (cb_fn->IsString() && opaque->IsString()) {
        v8::String::Utf8Value fn(cb_fn);

        v8::Local<v8::Value> fn_value;
        if (!TO_LOCAL(context->Global()->Get(context, cb_fn), &fn_value)) {
          return;
        }

        auto fn_handle = fn_value.As<v8::Function>();
        v8::Local<v8::Value> arg[1];
        arg[0] = opaque;

        if (debugger_started) {
          if (!agent_->IsStarted()) {
            agent_->Start(isolate_, platform_, src_path_.c_str());
          }

          agent_->PauseOnNextJavascriptStatement("Break on start");
          if (DebugExecute(*fn, arg, 1)) {
            std::lock_guard<std::mutex> lck(cron_timer_mtx_);
            cron_timer_checkpoint_[partition] = timer_ts;
            return;
          }
        } else {
          execute_flag_ = true;
          execute_start_time_ = Time::now();
          cron_timer_msg_counter++;
          RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                                IsTerminatingRetriable, IsExecutionTerminating,
                                isolate_);

          fn_handle->Call(context->Global(), 1, arg);
          std::lock_guard<std::mutex> lck(cron_timer_mtx_);
          cron_timer_checkpoint_[partition] = timer_ts;
          execute_flag_ = false;
        }
      }
    }
  }
}

void V8Worker::SendDocTimer(std::string callback_fn, std::string doc_id,
                            std::string timer_ts, int32_t partition) {
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  LOG(logTrace) << "Got timer event, doc_id:" << RU(doc_id)
                << " callback_fn:" << callback_fn << std::endl;

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  auto global = context->Global();
  v8::Local<v8::Value> cb_fn_val;
  if (!TO_LOCAL(global->Get(context, v8Str(isolate_, callback_fn)),
                &cb_fn_val)) {
    return;
  }

  auto cb_fn = cb_fn_val.As<v8::Function>();
  v8::Local<v8::Value> arg[1];
  arg[0] = v8Str(isolate_, doc_id);

  if (debugger_started) {
    if (!agent_->IsStarted()) {
      agent_->Start(isolate_, platform_, src_path_.c_str());
    }

    agent_->PauseOnNextJavascriptStatement("Break on start");
    if (DebugExecute(callback_fn.c_str(), arg, 1)) {
      std::lock_guard<std::mutex> lck(doc_timer_mtx_);
      doc_timer_checkpoint_[partition] = timer_ts;
      return;
    }
  } else {
    execute_flag_ = true;
    execute_start_time_ = Time::now();
    RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                          IsTerminatingRetriable, IsExecutionTerminating,
                          isolate_);

    cb_fn->Call(global, 1, arg);
    execute_flag_ = false;
    std::lock_guard<std::mutex> lck(doc_timer_mtx_);
    doc_timer_checkpoint_[partition] = timer_ts;
  }
}

void V8Worker::StartDebugger() {
  if (debugger_started) {
    LOG(logError) << "Debugger already started" << std::endl;
    return;
  }

  LOG(logInfo) << "Starting Debugger" << std::endl;
  startDebuggerFlag(true);
  agent_ = new inspector::Agent(settings_->host_addr, settings_->eventing_dir +
                                                          "/" + app_name_ +
                                                          "_frontend.url");
}

void V8Worker::StopDebugger() {
  if (debugger_started) {
    LOG(logInfo) << "Stopping Debugger" << std::endl;
    startDebuggerFlag(false);
    agent_->Stop();
    delete agent_;
  } else {
    LOG(logError) << "Debugger wasn't started" << std::endl;
  }
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
      info_obj->Set(v8Str(isolate_, "using_doc_timer"),
                    v8Str(isolate_, ident.using_doc_timer));
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
  v8::Context::Scope context_scope(context);

  auto jsify_info = Jsify(handler);
  if (jsify_info.code != kOK) {
    throw "Jsify failed when trying to identify version";
  }

  auto transpiler = UnwrapData(isolate_)->transpiler;
  auto script_to_execute =
      transpiler->Transpile(jsify_info.handler_code, app_name_ + ".js",
                            app_name_ + ".map.json", settings_->host_addr,
                            settings_->eventing_port) +
      '\n';

  script_to_execute += std::string((const char *)js_builtin) + '\n';

  auto ver = transpiler->GetCodeVersion(script_to_execute);
  return ver;
}

void V8Worker::GetDocTimerMessages(std::vector<uv_buf_t> &messages,
                                   std::vector<int> &length_prefix_sum,
                                   size_t window_size) {
  int64_t doc_timer_count =
      std::min(doc_timer_queue_->Count(), static_cast<int64_t>(window_size));
  int bytes_to_write =
      (length_prefix_sum.size() == 0) ? 0 : length_prefix_sum.back();

  for (int64_t idx = 0; idx < doc_timer_count; ++idx) {
    auto doc_timer_msg = doc_timer_queue_->Pop();
    auto curr_messages = BuildResponse(doc_timer_msg.timer_entry,
                                       mDoc_Timer_Response, timerResponse);
    for (auto &msg : curr_messages) {
      bytes_to_write += msg.len;
      messages.push_back(msg);
      length_prefix_sum.push_back(bytes_to_write);
    }
  }
}

void V8Worker::GetBucketOpsMessages(std::vector<uv_buf_t> &messages,
                                    std::vector<int> &length_prefix_sum) {
  int bytes_to_write =
      (length_prefix_sum.size() == 0) ? 0 : length_prefix_sum.back();
  for (int vb = 0; vb < NUM_VBUCKETS; ++vb) {
    auto seq = vb_seq_[vb].get()->load(std::memory_order_seq_cst);
    if (seq > 0) {
      std::string seq_no = std::to_string(vb) + "::" + std::to_string(seq);
      auto curr_messages =
          BuildResponse(seq_no, mBucket_Ops_Response, checkpointResponse);
      for (auto &msg : curr_messages) {
        bytes_to_write += msg.len;
        messages.push_back(msg);
        length_prefix_sum.push_back(bytes_to_write);
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
