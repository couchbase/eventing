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

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <mutex>
#include <regex>
#include <sstream>
#include <thread>
#include <typeinfo>
#include <vector>

#include "../include/bucket.h"
#include "../include/n1ql.h"
#include "../include/parse_deployment.h"

#include "../../gen/js/builtin.h"
#include "../../gen/js/escodegen.h"
#include "../../gen/js/esprima.h"
#include "../../gen/js/estraverse.h"
#include "../../gen/js/source-map.h"
#include "../../gen/js/transpiler.h"

#define BUFSIZE 100

bool debugger_started = false;
bool enable_recursive_mutation = false;

std::atomic<std::int64_t> bucket_op_exception_count = {0};
std::atomic<std::int64_t> n1ql_op_exception_count = {0};
std::atomic<std::int64_t> timeout_count = {0};
std::atomic<std::int16_t> checkpoint_failure_count = {0};

std::atomic<std::int64_t> on_update_success = {0};
std::atomic<std::int64_t> on_update_failure = {0};
std::atomic<std::int64_t> on_delete_success = {0};
std::atomic<std::int64_t> on_delete_failure = {0};

std::atomic<std::int64_t> non_doc_timer_create_failure = {0};
std::atomic<std::int64_t> doc_timer_create_failure = {0};

std::atomic<std::int64_t> messages_processed_counter = {0};

enum RETURN_CODE {
  kSuccess = 0,
  kFailedToCompileJs,
  kNoHandlersDefined,
  kFailedInitBucketHandle,
  kFailedInitN1QLHandle,
  kOnUpdateCallFail,
  kOnDeleteCallFail
};

template <typename... Args>
std::string string_sprintf(const char *format, Args... args) {
  int length = std::snprintf(nullptr, 0, format, args...);
  assert(length >= 0);

  char *buf = new char[length + 1];
  std::snprintf(buf, length + 1, format, args...);

  std::string str(buf);
  delete[] buf;
  return str;
}

void get_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  // lcb_get calls against metadata bucket is only triggered for timer lookups
  const lcb_RESPGET *rg = reinterpret_cast<const lcb_RESPGET *>(rb);
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
    LOG(logTrace) << string_sprintf("Value %.*s", static_cast<int>(rg->nvalue),
                                    reinterpret_cast<const char *>(rg->value));
    break;
  default:
    LOG(logTrace) << "LCB_CALLBACK_GET: Operation failed, "
                  << lcb_strerror(NULL, rb->rc) << " rc:" << rb->rc
                  << std::endl;
    break;
  }
}

void set_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  const lcb_RESPSTORE *rs = reinterpret_cast<const lcb_RESPSTORE *>(rb);
  Result *result = reinterpret_cast<Result *>(rb->cookie);
  result->rc = rs->rc;
}

void sdmutate_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  const lcb_RESPSUBDOC *resp = reinterpret_cast<const lcb_RESPSUBDOC *>(rb);
  lcb_SDENTRY ent;
  size_t iter = 0;

  Result *res = reinterpret_cast<Result *>(rb->cookie);
  res->rc = rb->rc;

  if (lcb_sdresult_next(resp, &ent, &iter)) {
    LOG(logTrace) << string_sprintf("Status: 0x%x. Value: %.*s\n", ent.status,
                                    static_cast<int>(ent.nvalue),
                                    reinterpret_cast<const char *>(ent.value));
  }
}

void sdlookup_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  Result *res = reinterpret_cast<Result *>(rb->cookie);
  res->cas = rb->cas;
  res->rc = rb->rc;

  if (rb->rc == LCB_SUCCESS) {
    const lcb_RESPGET *rg = reinterpret_cast<const lcb_RESPGET *>(rb);
    res->value.assign(reinterpret_cast<const char *>(rg->value), rg->nvalue);

    const lcb_RESPSUBDOC *resp = reinterpret_cast<const lcb_RESPSUBDOC *>(rb);
    lcb_SDENTRY ent;
    size_t iter = 0;
    int index = 0;
    while (lcb_sdresult_next(resp, &ent, &iter)) {
      LOG(logTrace) << string_sprintf(
          "Status: 0x%x. Value: %.*s\n", ent.status,
          static_cast<int>(ent.nvalue),
          reinterpret_cast<const char *>(ent.value));

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
    setLogLevel(logSilent);
  }
}
void enableRecursiveMutation(bool state) { enable_recursive_mutation = state; }

V8Worker::V8Worker(v8::Platform *platform, handler_config_t *h_config,
                   server_settings_t *server_settings)
    : settings(server_settings), platform_(platform) {
  enableRecursiveMutation(h_config->enable_recursive_mutation);

  histogram = new Histogram(HIST_FROM, HIST_TILL, HIST_WIDTH);

  for (int i = 0; i < NUM_VBUCKETS; i++) {
    vb_seq[i] = atomic_ptr_t(new std::atomic<int64_t>(0));
  }

  v8::Isolate::CreateParams create_params;
  create_params.array_buffer_allocator =
      v8::ArrayBuffer::Allocator::NewDefaultAllocator();

  isolate_ = v8::Isolate::New(create_params);
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  isolate_->SetData(DATA_SLOT, &data);
  isolate_->SetCaptureStackTraceForUncaughtExceptions(true);
  data.v8worker = this;

  v8::Local<v8::ObjectTemplate> global = v8::ObjectTemplate::New(GetIsolate());

  v8::TryCatch try_catch;

  global->Set(v8::String::NewFromUtf8(GetIsolate(), "curl"),
              v8::FunctionTemplate::New(GetIsolate(), Curl));
  global->Set(v8::String::NewFromUtf8(GetIsolate(), "log"),
              v8::FunctionTemplate::New(GetIsolate(), Log));
  global->Set(v8::String::NewFromUtf8(GetIsolate(), "docTimer"),
              v8::FunctionTemplate::New(GetIsolate(), CreateDocTimer));
  global->Set(v8::String::NewFromUtf8(GetIsolate(), "cronTimer"),
              v8::FunctionTemplate::New(GetIsolate(), CreateCronTimer));
  global->Set(v8::String::NewFromUtf8(GetIsolate(), "iter"),
              v8::FunctionTemplate::New(GetIsolate(), IterFunction));
  global->Set(v8::String::NewFromUtf8(GetIsolate(), "stopIter"),
              v8::FunctionTemplate::New(GetIsolate(), StopIterFunction));
  global->Set(v8::String::NewFromUtf8(GetIsolate(), "execQuery"),
              v8::FunctionTemplate::New(GetIsolate(), ExecQueryFunction));
  global->Set(v8::String::NewFromUtf8(GetIsolate(), "getReturnValue"),
              v8::FunctionTemplate::New(GetIsolate(), GetReturnValueFunction));

  if (try_catch.HasCaught()) {
    last_exception = ExceptionString(GetIsolate(), &try_catch);
    LOG(logError) << "Last exception: " << last_exception << std::endl;
  }

  v8::Local<v8::Context> context = v8::Context::New(GetIsolate(), NULL, global);
  context_.Reset(GetIsolate(), context);
  js_exception = new JsException(isolate_);
  data.js_exception = js_exception;

  app_name_ = h_config->app_name;
  execute_start_time = Time::now();

  deployment_config *config = ParseDeployment(h_config->dep_cfg.c_str());

  cb_source_bucket.assign(config->source_bucket);

  std::map<std::string,
           std::map<std::string, std::vector<std::string>>>::iterator it =
      config->component_configs.begin();

  Bucket *bucket_handle = nullptr;
  execute_flag = false;
  shutdown_terminator = false;
  max_task_duration = SECS_TO_NS * h_config->execution_timeout;

  if (!h_config->skip_lcb_bootstrap) {
    for (; it != config->component_configs.end(); it++) {
      if (it->first == "buckets") {
        std::map<std::string, std::vector<std::string>>::iterator bucket =
            config->component_configs["buckets"].begin();
        for (; bucket != config->component_configs["buckets"].end(); bucket++) {
          std::string bucket_alias = bucket->first;
          std::string bucket_name =
              config->component_configs["buckets"][bucket_alias][0];

          bucket_handle = new Bucket(
              this, bucket_name.c_str(), settings->kv_host_port.c_str(),
              bucket_alias.c_str(), settings->rbac_user, settings->rbac_pass);

          bucket_handles.push_back(bucket_handle);
        }
      }
    }
  }

  LOG(logInfo) << "Initialised V8Worker handle, app_name: "
               << h_config->app_name << " curr_host: " << settings->host_addr
               << " curr_eventing_port: " << settings->eventing_port
               << " kv_host_port: " << settings->kv_host_port
               << " lcb_cap: " << h_config->lcb_inst_capacity
               << " execution_timeout: " << h_config->execution_timeout
               << " enable_recursive_mutation: " << enable_recursive_mutation
               << " curl_timeout: " << curl_timeout << std::endl;

  connstr = "couchbase://" + settings->kv_host_port + "/" +
            cb_source_bucket.c_str() + "?username=" + settings->rbac_user +
            "&select_bucket=true";

  meta_connstr = "couchbase://" + settings->kv_host_port + "/" +
                 config->metadata_bucket.c_str() +
                 "?username=" + settings->rbac_user + "&select_bucket=true";

  if (!h_config->skip_lcb_bootstrap) {
    conn_pool = new ConnectionPool(h_config->lcb_inst_capacity,
                                   settings->kv_host_port, cb_source_bucket,
                                   settings->rbac_user, settings->rbac_pass);
  }
  src_path = settings->eventing_dir + "/" + app_name_ + ".t.js";

  delete config;

  this->worker_queue = new Queue<worker_msg_t>();

  std::thread r_thr(&V8Worker::RouteMessage, this);
  processing_thr = std::move(r_thr);
}

V8Worker::~V8Worker() {
  if (checkpointing_thr.joinable()) {
    checkpointing_thr.join();
  }

  if (processing_thr.joinable()) {
    processing_thr.join();
  }

  context_.Reset();
  on_update_.Reset();
  on_delete_.Reset();
  delete conn_pool;
  delete n1ql_handle;
  delete settings;
  delete histogram;
  delete js_exception;
}

// Re-compile and execute handler code for debugger
bool V8Worker::DebugExecute(const char *func_name, v8::Local<v8::Value> *args,
                            int args_len) {
  v8::HandleScope handle_scope(isolate_);
  v8::TryCatch try_catch(isolate_);

  // Need to construct origin for source-map to apply.
  auto origin_v8_str = v8::String::NewFromUtf8(isolate_, src_path.c_str());
  v8::ScriptOrigin origin(origin_v8_str);
  auto context = context_.Get(isolate_);
  auto source = v8::String::NewFromUtf8(isolate_, script_to_execute_.c_str());

  // Replace the usual log function with console.log
  auto global = context->Global();
  global->Set(v8::String::NewFromUtf8(isolate_, "log"),
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
      func->Call(v8::Null(isolate_), args_len, args);
      if (try_catch.HasCaught()) {
        agent->FatalException(try_catch.Exception(), try_catch.Message());
      }

      return true;
    }
  }
}

std::string GetTranspilerSrc() {
  std::string transpiler_js_src =
      std::string((const char *)js_esprima) + '\n' +
      std::string((const char *)js_escodegen) + '\n' +
      std::string((const char *)js_estraverse) + '\n' +
      std::string((const char *)js_transpiler) + '\n' +
      std::string((const char *)js_source_map);
  return transpiler_js_src;
}

int V8Worker::V8WorkerLoad(std::string script_to_execute) {
  LOG(logInfo) << "Eventing dir: " << settings->eventing_dir << std::endl;
  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  v8::TryCatch try_catch;
  std::string plain_js;
  int code = UniLineN1QL(script_to_execute.c_str(), &plain_js, nullptr);
  LOG(logTrace) << "code after Unilining N1QL: " << plain_js << std::endl;
  if (code != kOK) {
    LOG(logError) << "failed to uniline N1QL: " << code << std::endl;
    return code;
  }

  handler_code_ = plain_js;

  code = Jsify(script_to_execute.c_str(), &plain_js, nullptr);
  LOG(logTrace) << "jsified code: " << plain_js << std::endl;
  if (code != kOK) {
    LOG(logError) << "failed to jsify: " << code << std::endl;
    return code;
  }

  n1ql_handle = new N1QL(conn_pool, isolate_);
  UnwrapData(isolate_)->n1ql_handle = n1ql_handle;

  Transpiler transpiler(GetIsolate(), GetTranspilerSrc());
  script_to_execute =
      transpiler.Transpile(plain_js, app_name_ + ".js", app_name_ + ".map.json",
                           settings->host_addr, settings->eventing_port) +
      '\n';
  script_to_execute += std::string((const char *)js_builtin) + '\n';
  source_map_ = transpiler.GetSourceMap(plain_js, app_name_ + ".js");
  LOG(logTrace) << "source map:" << source_map_ << std::endl;

  v8::Local<v8::String> source =
      v8::String::NewFromUtf8(GetIsolate(), script_to_execute.c_str());

  script_to_execute_ = script_to_execute;
  LOG(logTrace) << "script to execute: " << script_to_execute << std::endl;

  if (!ExecuteScript(source))
    return kFailedToCompileJs;

  v8::Local<v8::String> on_update = v8Str(GetIsolate(), "OnUpdate");
  v8::Local<v8::String> on_delete = v8Str(GetIsolate(), "OnDelete");

  auto on_update_def = context->Global()->Get(on_update);
  auto on_delete_def = context->Global()->Get(on_delete);

  if (!on_update_def->IsFunction() && !on_delete_def->IsFunction()) {
    return kNoHandlersDefined;
  }

  if (on_update_def->IsFunction()) {
    v8::Local<v8::Function> on_update_fun =
        v8::Local<v8::Function>::Cast(on_update_def);
    on_update_.Reset(GetIsolate(), on_update_fun);
  }

  if (on_delete_def->IsFunction()) {
    v8::Local<v8::Function> on_delete_fun =
        v8::Local<v8::Function>::Cast(on_delete_def);
    on_delete_.Reset(GetIsolate(), on_delete_fun);
  }

  if (bucket_handles.size() > 0) {
    std::list<Bucket *>::iterator bucket_handle = bucket_handles.begin();

    for (; bucket_handle != bucket_handles.end(); bucket_handle++) {
      if (*bucket_handle) {
        if (!(*bucket_handle)->Initialize(this)) {
          LOG(logError) << "Error initializing bucket handle" << std::endl;
          return kFailedInitBucketHandle;
        }
      }
    }
  }

  curl_global_init(CURL_GLOBAL_ALL);
  CURL *curl = curl_easy_init();
  if (curl) {
    UnwrapData(isolate_)->curl_handle = curl;
  }

  lcb_U32 lcb_timeout = 2500000; // 2.5s

  if (transpiler.IsTimerCalled(script_to_execute)) {
    LOG(logDebug) << "Timer is called" << std::endl;

    lcb_create_st crst;
    memset(&crst, 0, sizeof crst);

    crst.version = 3;
    crst.v.v3.connstr = connstr.c_str();
    crst.v.v3.type = LCB_TYPE_BUCKET;
    crst.v.v3.passwd = settings->rbac_pass.c_str();

    lcb_create(&cb_instance, &crst);
    lcb_connect(cb_instance);
    lcb_wait(cb_instance);

    lcb_install_callback3(cb_instance, LCB_CALLBACK_GET, get_callback);
    lcb_install_callback3(cb_instance, LCB_CALLBACK_STORE, set_callback);
    lcb_install_callback3(cb_instance, LCB_CALLBACK_SDMUTATE,
                          sdmutate_callback);
    lcb_install_callback3(cb_instance, LCB_CALLBACK_SDLOOKUP,
                          sdlookup_callback);
    lcb_cntl(cb_instance, LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT, &lcb_timeout);
    UnwrapData(isolate_)->cb_instance = cb_instance;

    memset(&crst, 0, sizeof crst);

    crst.version = 3;
    crst.v.v3.connstr = meta_connstr.c_str();
    crst.v.v3.type = LCB_TYPE_BUCKET;
    crst.v.v3.passwd = settings->rbac_pass.c_str();

    lcb_create(&meta_cb_instance, &crst);
    lcb_connect(meta_cb_instance);
    lcb_wait(meta_cb_instance);

    lcb_install_callback3(meta_cb_instance, LCB_CALLBACK_GET, get_callback);
    lcb_install_callback3(meta_cb_instance, LCB_CALLBACK_STORE, set_callback);
    lcb_install_callback3(meta_cb_instance, LCB_CALLBACK_SDMUTATE,
                          sdmutate_callback);
    lcb_install_callback3(meta_cb_instance, LCB_CALLBACK_SDLOOKUP,
                          sdlookup_callback);
    lcb_cntl(meta_cb_instance, LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT, &lcb_timeout);

    UnwrapData(isolate_)->meta_cb_instance = meta_cb_instance;
  }

  lcb_create_st crst;

  memset(&crst, 0, sizeof crst);

  crst.version = 3;
  crst.v.v3.connstr = meta_connstr.c_str();
  crst.v.v3.type = LCB_TYPE_BUCKET;
  crst.v.v3.passwd = settings->rbac_pass.c_str();

  lcb_create(&checkpoint_cb_instance, &crst);
  lcb_connect(checkpoint_cb_instance);
  lcb_wait(checkpoint_cb_instance);

  lcb_install_callback3(checkpoint_cb_instance, LCB_CALLBACK_GET, get_callback);
  lcb_install_callback3(checkpoint_cb_instance, LCB_CALLBACK_STORE,
                        set_callback);
  lcb_install_callback3(checkpoint_cb_instance, LCB_CALLBACK_SDMUTATE,
                        sdmutate_callback);
  lcb_install_callback3(checkpoint_cb_instance, LCB_CALLBACK_SDLOOKUP,
                        sdlookup_callback);
  lcb_cntl(checkpoint_cb_instance, LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT,
           &lcb_timeout);

  // Spawning terminator thread to monitor the wall clock time for execution of
  // javascript code isn't going beyond max_task_duration. Passing reference to
  // current object instead of having terminator thread make a copy of the
  // object.
  // Spawned thread will execute the terminator loop logic in function call
  // operator() for V8Worker class
  terminator_thr = new std::thread(std::ref(*this));

  std::thread c_thr(&V8Worker::Checkpoint, this);
  checkpointing_thr = std::move(c_thr);

  return kSuccess;
}

void V8Worker::Checkpoint() {
  const auto checkpoint_interval =
      std::chrono::milliseconds(settings->checkpoint_interval);
  std::string seq_no_path("last_processed_seq_no");

  while (true) {
    for (int i = 0; i < NUM_VBUCKETS; i++) {
      auto seq = vb_seq[i].get()->load(std::memory_order_seq_cst);
      if (seq > 0) {
        std::stringstream vb_key;
        vb_key << appName << "_vb_" << i;

        lcb_CMDSUBDOC cmd = {0};
        LCB_CMD_SET_KEY(&cmd, vb_key.str().c_str(), vb_key.str().length());

        lcb_SDSPEC seq_spec = {0};
        seq_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
        seq_spec.options = LCB_SDSPEC_F_MKINTERMEDIATES;

        LCB_SDSPEC_SET_PATH(&seq_spec, seq_no_path.c_str(),
                            seq_no_path.length());
        auto seq_str = std::to_string(seq);
        LCB_SDSPEC_SET_VALUE(&seq_spec, seq_str.c_str(), seq_str.length());

        cmd.specs = &seq_spec;
        cmd.nspecs = 1;

        Result cres;
        lcb_subdoc3(checkpoint_cb_instance, &cres, &cmd);
        lcb_wait(checkpoint_cb_instance);

        auto sleep_duration = LCB_OP_RETRY_INTERVAL;
        while (cres.rc != LCB_SUCCESS) {
          checkpoint_failure_count++;
          std::this_thread::sleep_for(
              std::chrono::milliseconds(sleep_duration));
          sleep_duration *= 1.5;
          lcb_subdoc3(checkpoint_cb_instance, &cres, &cmd);
          lcb_wait(checkpoint_cb_instance);
        }

        // Reset the seq no of checkpointed vb to 0
        if (cres.rc == LCB_SUCCESS) {
          vb_seq[i].get()->compare_exchange_strong(seq, 0);
        }
      }
    }
    std::this_thread::sleep_for(checkpoint_interval);
  }
}

int64_t V8Worker::QueueSize() { return worker_queue->count(); }

void V8Worker::RouteMessage() {
  const flatbuf::payload::Payload *payload;
  std::string key, val, doc_id, callback_fn, cron_cb_fns, metadata;

  while (true) {
    worker_msg_t msg;
    msg = worker_queue->pop();
    payload = flatbuf::payload::GetPayload(
        (const void *)msg.payload->payload.c_str());

    LOG(logTrace) << " event: " << static_cast<int16_t>(msg.header->event)
                  << " opcode: " << static_cast<int16_t>(msg.header->opcode)
                  << " metadata: " << msg.header->metadata
                  << " partition: " << msg.header->partition << std::endl;

    switch (getEvent(msg.header->event)) {
    case eDCP:
      switch (getDCPOpcode(msg.header->opcode)) {
      case oDelete:
        this->SendDelete(msg.header->metadata);
        break;
      case oMutation:
        payload = flatbuf::payload::GetPayload(
            (const void *)msg.payload->payload.c_str());
        val.assign(payload->value()->str());
        metadata.assign(msg.header->metadata);
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
        doc_id.assign(payload->doc_id()->str());
        callback_fn.assign(payload->callback_fn()->str());
        this->SendDocTimer(doc_id, callback_fn);
        break;

      case oCronTimer:
        payload = flatbuf::payload::GetPayload(
            (const void *)msg.payload->payload.c_str());
        cron_cb_fns.assign(payload->doc_ids_callback_fns()->str());
        this->SendCronTimer(cron_cb_fns);
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

bool V8Worker::ExecuteScript(v8::Local<v8::String> script) {
  v8::HandleScope handle_scope(GetIsolate());
  v8::TryCatch try_catch(GetIsolate());

  auto context = context_.Get(isolate_);
  auto script_name =
      v8::String::NewFromUtf8(isolate_, (app_name_ + ".js").c_str());
  v8::ScriptOrigin origin(script_name);

  v8::Local<v8::Script> compiled_script;
  if (!v8::Script::Compile(context, script, &origin)
           .ToLocal(&compiled_script)) {
    assert(try_catch.HasCaught());
    last_exception = ExceptionString(GetIsolate(), &try_catch);
    LOG(logError) << "Exception logged:" << last_exception << std::endl;
    // The script failed to compile; bail out.
    return false;
  }

  v8::Local<v8::Value> result;
  if (!compiled_script->Run(context).ToLocal(&result)) {
    assert(try_catch.HasCaught());
    last_exception = ExceptionString(GetIsolate(), &try_catch);
    LOG(logError) << "Exception logged:" << last_exception << std::endl;
    // Running the script failed; bail out.
    return false;
  }

  return true;
}

void V8Worker::AddLcbException(int err_code) {
  std::lock_guard<std::mutex> lock(lcb_exception_mtx);

  if (lcb_exceptions.find(err_code) == lcb_exceptions.end()) {
    lcb_exceptions[err_code] = 0;
  }

  lcb_exceptions[err_code]++;
}

void V8Worker::ListLcbExceptions(std::map<int, int64_t> &agg_lcb_exceptions) {
  std::lock_guard<std::mutex> lock(lcb_exception_mtx);

  for (auto const &entry : lcb_exceptions) {
    if (agg_lcb_exceptions.find(entry.first) == agg_lcb_exceptions.end()) {
      agg_lcb_exceptions[entry.first] = 0;
    }

    agg_lcb_exceptions[entry.first] += entry.second;
  }
}

void V8Worker::UpdateHistogram(Time::time_point start_time) {
  Time::time_point t = Time::now();
  nsecs ns = std::chrono::duration_cast<nsecs>(t - start_time);
  histogram->Add(ns.count() / 1000);
}

int V8Worker::SendUpdate(std::string value, std::string meta,
                         std::string doc_type) {
  Time::time_point start_time = Time::now();

  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  if (on_update_.IsEmpty()) {
    UpdateHistogram(start_time);
    on_update_failure++;
    return kOnUpdateCallFail;
  }

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  LOG(logTrace) << "value: " << value << " meta: " << meta
                << " doc_type: " << doc_type << std::endl;
  v8::TryCatch try_catch(GetIsolate());

  v8::Handle<v8::Value> args[2];
  if (doc_type.compare("json") == 0) {
    args[0] =
        v8::JSON::Parse(v8::String::NewFromUtf8(GetIsolate(), value.c_str()));
  } else {
    args[0] = v8::String::NewFromUtf8(GetIsolate(), value.c_str());
  }

  args[1] =
      v8::JSON::Parse(v8::String::NewFromUtf8(GetIsolate(), meta.c_str()));

  // Look for vbucket and corresponding seq no in metadata
  auto meta_fields = args[1]->ToObject(context).ToLocalChecked();
  auto seq_str = v8Str(GetIsolate(), "seq");
  auto vb_str = v8Str(GetIsolate(), "vb");

  auto seq_val = meta_fields->Get(seq_str);
  auto vb_val = meta_fields->Get(vb_str);

  if (seq_val->IsNumber() && vb_val->IsNumber()) {
    vb_seq[vb_val->ToInteger()->Value()].get()->store(
        seq_val->ToInteger()->Value(), std::memory_order_seq_cst);
  }

  if (try_catch.HasCaught()) {
    last_exception = ExceptionString(GetIsolate(), &try_catch);
    LOG(logError) << "Last exception: " << last_exception << std::endl;
  }

  if (debugger_started) {
    if (!agent->IsStarted()) {
      agent->Start(isolate_, platform_, src_path.c_str());
    }

    agent->PauseOnNextJavascriptStatement("Break on start");
    if (DebugExecute("OnUpdate", args, 2)) {
      return kSuccess;
    }
    return kOnUpdateCallFail;
  } else {
    auto on_doc_update = on_update_.Get(isolate_);

    execute_flag = true;
    execute_start_time = Time::now();
    on_doc_update->Call(context->Global(), 2, args);
    execute_flag = false;

    if (try_catch.HasCaught()) {
      LOG(logDebug) << "Exception message: "
                    << ExceptionString(GetIsolate(), &try_catch) << std::endl;
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

  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  if (on_delete_.IsEmpty()) {
    UpdateHistogram(start_time);
    on_delete_failure++;
    return kOnDeleteCallFail;
  }

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  LOG(logTrace) << " meta: " << meta << std::endl;
  v8::TryCatch try_catch(GetIsolate());

  v8::Local<v8::Value> args[1];
  args[0] =
      v8::JSON::Parse(v8::String::NewFromUtf8(GetIsolate(), meta.c_str()));

  // Look for vbucket and corresponding seq no in metadata
  auto meta_fields = args[0]->ToObject(context).ToLocalChecked();
  auto seq_str = v8Str(GetIsolate(), "seq");
  auto vb_str = v8Str(GetIsolate(), "vb");

  auto seq_val = meta_fields->Get(seq_str);
  auto vb_val = meta_fields->Get(vb_str);

  if (seq_val->IsNumber() && vb_val->IsNumber()) {
    vb_seq[vb_val->ToInteger()->Value()].get()->store(
        seq_val->ToInteger()->Value(), std::memory_order_seq_cst);
  }

  assert(!try_catch.HasCaught());

  if (debugger_started) {
    if (!agent->IsStarted()) {
      agent->Start(isolate_, platform_, src_path.c_str());
    }

    agent->PauseOnNextJavascriptStatement("Break on start");
    if (DebugExecute("OnDelete", args, 1)) {
      return kSuccess;
    }
    return kOnDeleteCallFail;
  } else {
    auto on_doc_delete = on_delete_.Get(isolate_);

    execute_flag = true;
    execute_start_time = Time::now();
    on_doc_delete->Call(context->Global(), 1, args);
    execute_flag = false;

    if (try_catch.HasCaught()) {
      LOG(logError) << "Exception message"
                    << ExceptionString(GetIsolate(), &try_catch) << std::endl;
      UpdateHistogram(start_time);
      on_delete_failure++;
      return kOnDeleteCallFail;
    }

    UpdateHistogram(start_time);
    on_delete_success++;
    return kSuccess;
  }
}

void V8Worker::SendCronTimer(std::string cron_cb_fns) {
  /*
   {"cron_timers":[
                   {"callback_func":"timerCallback1", "payload": "doc_id1"},
                   {"callback_func":"timerCallback2", "payload": "doc_id2"},
                   ...
                  ]
    ,"version":"vulcan"}
 */
  LOG(logTrace) << "cron timers: " << cron_cb_fns << std::endl;

  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  auto data = v8::JSON::Parse(v8Str(GetIsolate(), cron_cb_fns.c_str()));

  auto params = data->ToObject(context).ToLocalChecked();
  auto cron_timers = v8Str(GetIsolate(), "cron_timers");

  auto cron_timer_entries = params->Get(cron_timers);

  if (cron_timer_entries->IsArray()) {

    auto entries = cron_timer_entries->ToObject(context).ToLocalChecked();
    auto entries_arr = entries.As<v8::Array>();

    auto callback_fn = v8Str(GetIsolate(), "callback_func");
    auto payload = v8Str(GetIsolate(), "payload");

    for (int i = 0; i < static_cast<int>(entries_arr->Length()); i++) {
      auto entry = entries_arr->Get(i).As<v8::Object>();

      auto cb_fn = entry->Get(callback_fn);
      auto opaque = entry->Get(payload);

      if (cb_fn->IsString() && opaque->IsString()) {
        v8::String::Utf8Value fn(cb_fn);

        auto fn_value = context->Global()->Get(v8Str(GetIsolate(), *fn));
        auto fn_handle = v8::Handle<v8::Function>::Cast(fn_value);

        v8::Handle<v8::Value> arg[1];
        arg[0] = opaque;

        if (debugger_started) {
          if (!agent->IsStarted()) {
            agent->Start(isolate_, platform_, src_path.c_str());
          }

          agent->PauseOnNextJavascriptStatement("Break on start");
          if (DebugExecute(*fn, arg, 1)) {
            return;
          }
        } else {
          execute_flag = true;
          execute_start_time = Time::now();
          fn_handle->Call(context->Global(), 1, arg);
          execute_flag = false;
        }
      }
    }
  }
}

void V8Worker::SendDocTimer(std::string doc_id, std::string callback_fn) {
  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  LOG(logTrace) << "Got timer event, doc_id:" << doc_id
                << " callback_fn:" << callback_fn << std::endl;

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  v8::Handle<v8::Value> val = context->Global()->Get(
      v8::String::NewFromUtf8(GetIsolate(), callback_fn.c_str(),
                              v8::NewStringType::kNormal)
          .ToLocalChecked());
  v8::Handle<v8::Function> cb_fn = v8::Handle<v8::Function>::Cast(val);

  v8::Handle<v8::Value> arg[1];
  arg[0] = v8::String::NewFromUtf8(GetIsolate(), doc_id.c_str());

  if (debugger_started) {
    if (!agent->IsStarted()) {
      agent->Start(isolate_, platform_, src_path.c_str());
    }

    agent->PauseOnNextJavascriptStatement("Break on start");
    if (DebugExecute(callback_fn.c_str(), arg, 1)) {
      return;
    }
  } else {
    execute_flag = true;
    execute_start_time = Time::now();
    cb_fn->Call(context->Global(), 1, arg);
    execute_flag = false;
  }
}

void V8Worker::StartDebugger() {
  if (debugger_started) {
    LOG(logError) << "Debugger already started" << std::endl;
    return;
  }

  LOG(logInfo) << "Starting Debugger" << std::endl;
  startDebuggerFlag(true);
  agent = new inspector::Agent(settings->host_addr, settings->eventing_dir +
                                                        "/" + app_name_ +
                                                        "_frontend.url");
}

void V8Worker::StopDebugger() {
  if (debugger_started) {
    LOG(logInfo) << "Stopping Debugger" << std::endl;
    startDebuggerFlag(false);
    agent->Stop();
    delete agent;
  } else {
    LOG(logError) << "Debugger wasn't started" << std::endl;
  }
}

void V8Worker::Enqueue(header_t *h, message_t *p) {
  const flatbuf::payload::Payload *payload;
  std::string key, val;
  payload = flatbuf::payload::GetPayload((const void *)p->payload.c_str());

  worker_msg_t msg;
  msg.header = h;
  msg.payload = p;
  LOG(logTrace) << "Inserting event: " << static_cast<int16_t>(h->event)
                << " opcode: " << static_cast<int16_t>(h->opcode)
                << " partition: " << h->partition
                << " metadata: " << h->metadata << std::endl;
  worker_queue->push(msg);
}

std::string V8Worker::CompileHandler(std::string handler) {
  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  try {
    Transpiler transpiler(GetIsolate(), GetTranspilerSrc());
    auto info = transpiler.Compile(handler);
    Transpiler::LogCompilationInfo(info);

    v8::HandleScope handle_scoe(isolate_);

    auto info_obj = v8::Object::New(isolate_);
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

    return JSONStringify(isolate_, info_obj);
  } catch (const char *e) {
    LOG(logError) << e << std::endl;
  }

  return "";
}

const char *V8Worker::V8WorkerLastException() { return last_exception.c_str(); }

const char *V8Worker::V8WorkerVersion() { return v8::V8::GetVersion(); }

void V8Worker::V8WorkerTerminateExecution() {
  v8::V8::TerminateExecution(GetIsolate());
}
