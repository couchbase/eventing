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

bool enable_recursive_mutation = false;

N1QL *n1ql_handle;
JsException V8Worker::exception;

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
  return std::move(str);
}

void get_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  // lcb_get calls against metadata bucket is only triggered for timer lookups
  const lcb_RESPGET *rg = reinterpret_cast<const lcb_RESPGET *>(rb);
  const void *data = lcb_get_cookie(instance);

  std::string ts;
  std::string timestamp_marker("");
  lcb_CMDSTORE acmd = {0};

  switch (rb->rc) {
  case LCB_KEY_ENOENT:
    ts.assign(reinterpret_cast<const char *>(data));

    LCB_CMD_SET_KEY(&acmd, ts.c_str(), ts.length());
    LCB_CMD_SET_VALUE(&acmd, timestamp_marker.c_str(),
                      timestamp_marker.length());
    acmd.operation = LCB_ADD;

    lcb_store3(instance, NULL, &acmd);
    lcb_wait(instance);
    break;
  case LCB_SUCCESS:
    LOG(logTrace) << string_sprintf("Value %.*s", static_cast<int>(rg->nvalue),
                                    reinterpret_cast<const char *>(rg->value));
    break;
  default:
    LOG(logTrace) << "LCB_CALLBACK_GET: Operation failed, "
                  << lcb_strerror(NULL, rb->rc) << " rc:" << rb->rc << '\n';
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

void enableRecursiveMutation(bool state) { enable_recursive_mutation = state; }

V8Worker::V8Worker(v8::Platform *platform, handler_config_t *h_config,
                   server_settings_t *server_settings)
    : settings(server_settings), platform_(platform) {
  enableRecursiveMutation(h_config->enable_recursive_mutation);

  v8::Isolate::CreateParams create_params;
  create_params.array_buffer_allocator =
      v8::ArrayBuffer::Allocator::NewDefaultAllocator();

  isolate_ = v8::Isolate::New(create_params);
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  isolate_->SetCaptureStackTraceForUncaughtExceptions(true);
  isolate_->SetData(0, this);
  v8::Local<v8::ObjectTemplate> global = v8::ObjectTemplate::New(GetIsolate());

  v8::TryCatch try_catch;

  global->Set(v8::String::NewFromUtf8(GetIsolate(), "log"),
              v8::FunctionTemplate::New(GetIsolate(), Log));
  global->Set(v8::String::NewFromUtf8(GetIsolate(), "docTimer"),
              v8::FunctionTemplate::New(GetIsolate(), CreateDocTimer));
  global->Set(v8::String::NewFromUtf8(GetIsolate(), "nonDocTimer"),
              v8::FunctionTemplate::New(GetIsolate(), CreateNonDocTimer));
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
    LOG(logError) << "Last expection: " << last_exception << '\n';
  }

  v8::Local<v8::Context> context = v8::Context::New(GetIsolate(), NULL, global);
  context_.Reset(GetIsolate(), context);
  exception = JsException(isolate_);

  app_name_ = h_config->app_name;
  execute_start_time = Time::now();

  deployment_config *config = ParseDeployment(h_config->dep_cfg.c_str());

  cb_source_bucket.assign(config->source_bucket);

  std::map<std::string,
           std::map<std::string, std::vector<std::string>>>::iterator it =
      config->component_configs.begin();

  Bucket *bucket_handle = nullptr;
  debugger_started = false;
  execute_flag = false;
  shutdown_terminator = false;
  max_task_duration = SECS_TO_NS * h_config->execution_timeout;

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

  LOG(logInfo) << "Initialised V8Worker handle, app_name: "
               << h_config->app_name << " curr_host: " << settings->host_addr
               << " curr_eventing_port: " << settings->eventing_port
               << " kv_host_port: " << settings->kv_host_port
               << " rbac_user: " << settings->rbac_user
               << " rbac_pass: " << settings->rbac_pass
               << " lcb_cap: " << h_config->lcb_inst_capacity
               << " execution_timeout: " << h_config->execution_timeout
               << " enable_recursive_mutation: " << enable_recursive_mutation
               << '\n';

  connstr = "couchbase://" + settings->kv_host_port + "/" +
            cb_source_bucket.c_str() + "?username=" + settings->rbac_user +
            "&select_bucket=true";

  meta_connstr = "couchbase://" + settings->kv_host_port + "/" +
                 config->metadata_bucket.c_str() +
                 "?username=" + settings->rbac_user + "&select_bucket=true";

  conn_pool = new ConnectionPool(h_config->lcb_inst_capacity,
                                 settings->kv_host_port, cb_source_bucket,
                                 settings->rbac_user, settings->rbac_pass);
  src_path = settings->eventing_dir + "/" + app_name_ + ".t.js";

  delete config;

  this->worker_queue = new Queue<worker_msg_t>();

  std::thread th(&V8Worker::RouteMessage, this);
  processing_thr = std::move(th);
}

V8Worker::~V8Worker() {
  if (processing_thr.joinable()) {
    processing_thr.join();
  }
  context_.Reset();
  on_update_.Reset();
  on_delete_.Reset();
  delete conn_pool;
  delete n1ql_handle;
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

int V8Worker::V8WorkerLoad(std::string script_to_execute) {
  LOG(logInfo) << "Eventing dir: " << settings->eventing_dir << '\n';
  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  v8::TryCatch try_catch;
  std::string plain_js;
  int code = UniLineN1QL(script_to_execute.c_str(), &plain_js);
  LOG(logTrace) << "code after Unilining N1QL: " << plain_js << '\n';
  if (code != kOK) {
    LOG(logError) << "failed to uniline N1QL: " << code << '\n';
    return code;
  }

  handler_code_ = plain_js;

  code = Jsify(script_to_execute.c_str(), &plain_js);
  LOG(logTrace) << "jsified code: " << plain_js << '\n';
  if (code != kOK) {
    LOG(logError) << "failed to jsify: " << code << '\n';
    return code;
  }

  plain_js += std::string((const char *)js_builtin) + '\n';

  std::string transpiler_js_src =
      std::string((const char *)js_esprima) + '\n' +
      std::string((const char *)js_escodegen) + '\n' +
      std::string((const char *)js_estraverse) + '\n' +
      std::string((const char *)js_transpiler) + '\n' +
      std::string((const char *)js_source_map);

  n1ql_handle = new N1QL(conn_pool);

  Transpiler transpiler(transpiler_js_src);
  script_to_execute =
      transpiler.Transpile(plain_js, app_name_ + ".js", app_name_ + ".map.json",
                           settings->host_addr, settings->eventing_port) +
      '\n';
  source_map_ = transpiler.GetSourceMap(plain_js, app_name_ + ".js");
  LOG(logTrace) << "source map:" << source_map_ << '\n';

  v8::Local<v8::String> source =
      v8::String::NewFromUtf8(GetIsolate(), script_to_execute.c_str());

  script_to_execute_ = script_to_execute;
  LOG(logTrace) << "script to execute: " << script_to_execute << '\n';

  if (!ExecuteScript(source))
    return kFailedToCompileJs;

  v8::Local<v8::String> on_update = v8Str(GetIsolate(), "OnUpdate");
  v8::Local<v8::String> on_delete = v8Str(GetIsolate(), "OnDelete");

  v8::Local<v8::Value> on_update_val;
  v8::Local<v8::Value> on_delete_val;

  if (!context->Global()->Get(context, on_update).ToLocal(&on_update_val) ||
      !context->Global()->Get(context, on_delete).ToLocal(&on_delete_val)) {
    return kNoHandlersDefined;
  }

  v8::Local<v8::Function> on_update_fun =
      v8::Local<v8::Function>::Cast(on_update_val);
  on_update_.Reset(GetIsolate(), on_update_fun);

  v8::Local<v8::Function> on_delete_fun =
      v8::Local<v8::Function>::Cast(on_delete_val);
  on_delete_.Reset(GetIsolate(), on_delete_fun);

  if (bucket_handles.size() > 0) {
    std::list<Bucket *>::iterator bucket_handle = bucket_handles.begin();

    for (; bucket_handle != bucket_handles.end(); bucket_handle++) {
      if (*bucket_handle) {
        std::map<std::string, std::string> data_bucket;
        if (!(*bucket_handle)->Initialize(this, &data_bucket)) {
          LOG(logError) << "Error initializing bucket handle" << '\n';
          return kFailedInitBucketHandle;
        }
      }
    }
  }

  if (transpiler.IsTimerCalled(script_to_execute)) {
    LOG(logDebug) << "Timer is called" << '\n';

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

    this->GetIsolate()->SetData(1, (void *)(&cb_instance));

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
    this->GetIsolate()->SetData(2, (void *)(&meta_cb_instance));
  }

  // Spawning terminator thread to monitor the wall clock time for execution of
  // javascript code isn't going beyond max_task_duration. Passing reference to
  // current object instead of having terminator thread make a copy of the
  // object.
  // Spawned thread will execute the terminator loop logic in function call
  // operator() for V8Worker class
  terminator_thr = new std::thread(std::ref(*this));

  return kSuccess;
}

void V8Worker::RouteMessage() {
  const flatbuf::payload::Payload *payload;
  std::string key, val, doc_id, callback_fn, doc_ids_cb_fns, metadata;

  while (true) {
    worker_msg_t msg;
    msg = worker_queue->pop();
    payload = flatbuf::payload::GetPayload(
        (const void *)msg.payload->payload.c_str());

    LOG(logTrace) << " event: " << static_cast<int16_t>(msg.header->event)
                  << " opcode: " << static_cast<int16_t>(msg.header->opcode)
                  << " metadata: " << msg.header->metadata
                  << " partition: " << msg.header->partition << '\n';

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

      case oNonDocTimer:
        payload = flatbuf::payload::GetPayload(
            (const void *)msg.payload->payload.c_str());
        doc_ids_cb_fns.assign(payload->doc_ids_callback_fns()->str());
        this->SendNonDocTimer(doc_ids_cb_fns);
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
    LOG(logError) << "Exception logged:" << last_exception << '\n';
    // The script failed to compile; bail out.
    return false;
  }

  v8::Local<v8::Value> result;
  if (!compiled_script->Run(context).ToLocal(&result)) {
    assert(try_catch.HasCaught());
    last_exception = ExceptionString(GetIsolate(), &try_catch);
    LOG(logError) << "Exception logged:" << last_exception << '\n';
    // Running the script failed; bail out.
    return false;
  }

  return true;
}

int V8Worker::SendUpdate(std::string value, std::string meta,
                         std::string doc_type) {
  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  LOG(logTrace) << "value: " << value << " meta: " << meta
                << " doc_type: " << doc_type << '\n';
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

  if (try_catch.HasCaught()) {
    last_exception = ExceptionString(GetIsolate(), &try_catch);
    LOG(logError) << "Last exception: " << last_exception << '\n';
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
                    << ExceptionString(GetIsolate(), &try_catch) << '\n';
      return kOnUpdateCallFail;
    }

    return kSuccess;
  }
}

int V8Worker::SendDelete(std::string meta) {
  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  LOG(logTrace) << " meta: " << meta << '\n';
  v8::TryCatch try_catch(GetIsolate());

  v8::Local<v8::Value> args[1];
  args[0] =
      v8::JSON::Parse(v8::String::NewFromUtf8(GetIsolate(), meta.c_str()));

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
                    << ExceptionString(GetIsolate(), &try_catch) << '\n';
      return kOnDeleteCallFail;
    }

    return kSuccess;
  }
}

void V8Worker::SendNonDocTimer(std::string doc_ids_cb_fns) {
  std::vector<std::string> entries = split(doc_ids_cb_fns, ';');
  char tstamp[BUFSIZE], fn[BUFSIZE];

  if (entries.size() > 0) {
    v8::Locker locker(GetIsolate());
    v8::Isolate::Scope isolate_scope(GetIsolate());
    v8::HandleScope handle_scope(GetIsolate());

    auto context = context_.Get(isolate_);
    v8::Context::Scope context_scope(context);

    for (auto entry : entries) {
      if (entry.length() > 0) {
        sscanf(entry.c_str(),
               "{\"callback_func\": \"%[^\"]\", \"start_ts\": \"%[^\"]\"}", fn,
               tstamp);
        LOG(logTrace) << "Non doc timer event for callback_fn: " << fn
                      << " tstamp: " << tstamp << '\n';

        v8::Handle<v8::Value> val =
            context->Global()->Get(v8Str(GetIsolate(), fn));
        v8::Handle<v8::Function> cb_func = v8::Handle<v8::Function>::Cast(val);

        v8::Handle<v8::Value> arg[1];

        if (debugger_started) {
          if (!agent->IsStarted()) {
            agent->Start(isolate_, platform_, src_path.c_str());
          }

          agent->PauseOnNextJavascriptStatement("Break on start");
          if (DebugExecute(fn, arg, 0)) {
            return;
          }
        } else {
          execute_flag = true;
          execute_start_time = Time::now();
          cb_func->Call(context->Global(), 0, arg);
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
                << " callback_fn:" << callback_fn << '\n';

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
    LOG(logError) << "Debugger already started" << '\n';
    return;
  }

  LOG(logInfo) << "Starting Debugger" << '\n';
  debugger_started = true;
  agent = new inspector::Agent(settings->host_addr, settings->eventing_dir +
                                                        "/" + app_name_ +
                                                        "_frontend.url");
}

void V8Worker::StopDebugger() {
  if (debugger_started) {
    LOG(logInfo) << "Stopping Debugger" << '\n';
    debugger_started = false;
    agent->Stop();
    delete agent;
  } else {
    LOG(logError) << "Debugger wasn't started" << '\n';
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
                << " metadata: " << h->metadata << '\n';
  worker_queue->push(msg);
}

const char *V8Worker::V8WorkerLastException() { return last_exception.c_str(); }

const char *V8Worker::V8WorkerVersion() { return v8::V8::GetVersion(); }

void V8Worker::V8WorkerTerminateExecution() {
  v8::V8::TerminateExecution(GetIsolate());
}
