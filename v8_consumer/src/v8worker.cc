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
#include <mutex>
#include <regex>
#include <sstream>
#include <thread>
#include <typeinfo>
#include <unistd.h>
#include <vector>

#include "../include/bucket.h"
#include "../include/n1ql.h"
#include "../include/parse_deployment.h"

#define BUFSIZE 100
#define MAXPATHLEN 256
#define TRANSPILER_JS_PATH "transpiler.js"
#define ESPRIMA_PATH "esprima.js"
#define ESCODEGEN_PATH "escodegen.js"
#define ESTRAVERSE_PATH "estraverse.js"
#define BUILTIN_JS_PATH "builtin.js"

bool enable_recursive_mutation = false;

N1QL *n1ql_handle;

enum RETURN_CODE {
  SUCCESS = 0,
  FAILED_TO_COMPILE_JS,
  NO_HANDLERS_DEFINED,
  FAILED_INIT_BUCKET_HANDLE,
  FAILED_INIT_N1QL_HANDLE,
  ON_UPDATE_CALL_FAIL,
  ON_DELETE_CALL_FAIL
};

// Copies a C string to a 16-bit string.  Does not check for buffer overflow.
// Does not use the V8 engine to convert strings, so it can be used
// in any thread.  Returns the length of the string.
int AsciiToUtf16(const char *input_buffer, uint16_t *output_buffer) {
  int i;
  for (i = 0; input_buffer[i] != '\0'; ++i) {
    // ASCII does not use chars > 127, but be careful anyway.
    output_buffer[i] = static_cast<unsigned char>(input_buffer[i]);
  }
  output_buffer[i] = 0;
  return i;
}

// Reads a file from the given path and returns the content.
std::string ReadFile(std::string file_path) {
  std::ifstream file(file_path);
  std::string source((std::istreambuf_iterator<char>(file)),
                     std::istreambuf_iterator<char>());
  return source;
}

v8::Local<v8::String> createUtf8String(v8::Isolate *isolate, const char *str) {
  return v8::String::NewFromUtf8(isolate, str, v8::NewStringType::kNormal)
      .ToLocalChecked();
}

std::string ObjectToString(v8::Local<v8::Value> value) {
  v8::String::Utf8Value utf8_value(value);
  return std::string(*utf8_value);
}

std::string ToString(v8::Isolate *isolate, v8::Handle<v8::Value> object) {
  v8::HandleScope handle_scope(isolate);

  v8::Local<v8::Context> context = isolate->GetCurrentContext();
  v8::Local<v8::Object> global = context->Global();

  v8::Local<v8::Object> JSON =
      global->Get(v8::String::NewFromUtf8(isolate, "JSON"))->ToObject();
  v8::Local<v8::Function> JSON_stringify = v8::Local<v8::Function>::Cast(
      JSON->Get(v8::String::NewFromUtf8(isolate, "stringify")));

  v8::Local<v8::Value> result;
  v8::Local<v8::Value> args[1];
  args[0] = {object};
  result = JSON_stringify->Call(context->Global(), 1, args);
  return ObjectToString(result);
}

lcb_t *UnwrapLcbInstance(v8::Local<v8::Object> obj) {
  v8::Local<v8::External> field =
      v8::Local<v8::External>::Cast(obj->GetInternalField(1));
  void *ptr = field->Value();
  return static_cast<lcb_t *>(ptr);
}

lcb_t *UnwrapV8WorkerLcbInstance(v8::Local<v8::Object> obj) {
  v8::Local<v8::External> field =
      v8::Local<v8::External>::Cast(obj->GetInternalField(2));
  void *ptr = field->Value();
  return static_cast<lcb_t *>(ptr);
}

V8Worker *UnwrapV8WorkerInstance(v8::Local<v8::Object> obj) {
  v8::Local<v8::External> field =
      v8::Local<v8::External>::Cast(obj->GetInternalField(1));
  void *ptr = field->Value();
  return static_cast<V8Worker *>(ptr);
}

std::map<std::string, std::string> *UnwrapMap(v8::Local<v8::Object> obj) {
  v8::Local<v8::External> field =
      v8::Local<v8::External>::Cast(obj->GetInternalField(0));
  void *ptr = field->Value();
  return static_cast<std::map<std::string, std::string> *>(ptr);
}

// Extracts a C string from a V8 Utf8Value.
const char *ToCString(const v8::String::Utf8Value &value) {
  return *value ? *value : "<std::string conversion failed>";
}

bool ToCBool(const v8::Local<v8::Boolean> &value) {
  if (value.IsEmpty()) {
    LOG(logError) << "Failed to convert to bool" << '\n';
  }

  return value->Value();
}

const char *ToJson(v8::Isolate *isolate, v8::Handle<v8::Value> object) {
  v8::HandleScope handle_scope(isolate);

  v8::Local<v8::Context> context = isolate->GetCurrentContext();
  v8::Local<v8::Object> global = context->Global();

  v8::Local<v8::Object> JSON =
      global->Get(v8::String::NewFromUtf8(isolate, "JSON"))->ToObject();
  v8::Local<v8::Function> JSON_stringify = v8::Local<v8::Function>::Cast(
      JSON->Get(v8::String::NewFromUtf8(isolate, "stringify")));

  v8::Local<v8::Value> result;
  v8::Local<v8::Value> args[1];
  args[0] = {object};
  result = JSON_stringify->Call(context->Global(), 1, args);
  v8::String::Utf8Value str(result->ToString());

  return ToCString(str);
}

void Print(const v8::FunctionCallbackInfo<v8::Value> &args) {
  std::string log_msg;
  for (int i = 0; i < args.Length(); i++) {
    log_msg += ToJson(args.GetIsolate(), args[i]);
    log_msg += ' ';
  }
  LOG(logDebug) << log_msg << '\n';
}

std::string ConvertToISO8601(std::string timestamp) {
  char buf[sizeof "2016-08-09T10:11:12"];
  std::string buf_s;
  time_t now;

  int timerValue = atoi(timestamp.c_str());

  // Expiry timers more than 30 days will mention epoch
  // otherwise it will mention seconds from when key
  // was set
  if (timerValue > 25920000) {
    now = timerValue;
    strftime(buf, sizeof buf, "%FT%T", gmtime(&now));
    buf_s.assign(buf);
  } else {
    time(&now);
    now += timerValue;
    strftime(buf, sizeof buf, "%FT%T", gmtime(&now));
    buf_s.assign(buf);
  }
  return buf_s;
}

bool isFuncReference(const v8::FunctionCallbackInfo<v8::Value> &args, int i) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);

  if (args[i]->IsFunction()) {
    v8::Local<v8::Function> func_ref = args[i].As<v8::Function>();
    v8::String::Utf8Value func_name(func_ref->GetName());

    if (func_name.length()) {
      v8::Local<v8::Context> context = isolate->GetCurrentContext();
      v8::Local<v8::Function> timer_func_ref =
          context->Global()->Get(func_ref->GetName()).As<v8::Function>();

      if (timer_func_ref->IsUndefined()) {
        LOG(logError) << *func_name << " is not defined in global scope"
                      << '\n';
        return false;
      }
    } else {
      LOG(logError) << "Invalid arg: Anonymous function is not allowed" << '\n';
      return false;
    }
  } else {
    LOG(logError) << "Invalid arg: Function reference expected" << '\n';
    return false;
  }

  return true;
}

void CreateNonDocTimer(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);

  std::string cb_func;
  if (isFuncReference(args, 0)) {
    v8::Local<v8::Function> func_ref = args[0].As<v8::Function>();
    v8::String::Utf8Value func_name(func_ref->GetName());
    cb_func.assign(std::string(*func_name));
  } else {
    return;
  }

  v8::String::Utf8Value ts(args[1]);

  std::string start_ts, timer_entry, value;
  start_ts.assign(std::string(*ts));

  if (atoi(start_ts.c_str()) <= 0) {
    LOG(logError)
        << "Skipping non-doc_id timer callback setup, invalid start timestamp"
        << '\n';
    return;
  }

  timer_entry.assign(appName);
  timer_entry.append("::");
  timer_entry.append(ConvertToISO8601(start_ts));
  timer_entry.append("Z");
  LOG(logTrace) << "Request to register non-doc_id timer, callback_func:"
                << cb_func << "start_ts : " << timer_entry << '\n';

  // Store blob in KV store, blob structure:
  // {
  //    "callback_func": CallbackFunc,
  //    "start_ts": timestamp
  // }

  // prepending delimiter ";"
  value.assign(";{\"callback_func\": \"");
  value.append(cb_func);
  value.append("\", \"start_ts\": \"");
  value.append(timer_entry);
  value.append("\"}");

  lcb_t *meta_cb_instance =
      reinterpret_cast<lcb_t *>(args.GetIsolate()->GetData(2));

  // Append doc_id to key that keeps tracks of doc_ids for which
  // callbacks need to be triggered at any given point in time
  lcb_CMDGET gcmd = {0};
  LCB_CMD_SET_KEY(&gcmd, timer_entry.c_str(), timer_entry.length());
  lcb_get3(*meta_cb_instance, NULL, &gcmd);
  lcb_set_cookie(*meta_cb_instance, timer_entry.c_str());
  lcb_wait(*meta_cb_instance);
  lcb_set_cookie(*meta_cb_instance, NULL);

  lcb_CMDSTORE cmd = {0};
  cmd.operation = LCB_APPEND;

  LOG(logTrace) << "Non doc_id timer entry to append: " << value << '\n';
  LCB_CMD_SET_KEY(&cmd, timer_entry.c_str(), timer_entry.length());
  LCB_CMD_SET_VALUE(&cmd, value.c_str(), value.length());
  lcb_sched_enter(*meta_cb_instance);
  lcb_store3(*meta_cb_instance, NULL, &cmd);
  lcb_sched_leave(*meta_cb_instance);
  lcb_wait(*meta_cb_instance);
}

void CreateDocTimer(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);

  std::string cb_func;
  if (isFuncReference(args, 0)) {
    v8::Local<v8::Function> func_ref = args[0].As<v8::Function>();
    v8::String::Utf8Value func_name(func_ref->GetName());
    cb_func.assign(std::string(*func_name));
  } else {
    return;
  }

  v8::String::Utf8Value doc(args[1]);
  v8::String::Utf8Value ts(args[2]);

  std::string doc_id, start_ts, timer_entry;
  doc_id.assign(std::string(*doc));
  start_ts.assign(std::string(*ts));

  // If the doc not supposed to expire, skip
  // setting up timer callback for it
  if (atoi(start_ts.c_str()) == 0) {
    LOG(logError) << "Skipping timer callback setup for doc_id:" << doc_id
                  << ", won't expire" << '\n';
    return;
  }

  timer_entry.assign(appName);
  timer_entry += "::";
  timer_entry += ConvertToISO8601(start_ts);

  /* Perform XATTR operations, XATTR structure with timers:
   *
   * {
   * "eventing": {
   *              "timers": ["appname::2017-04-30ZT12:00:00::callback_func",
   * ...],
   *              "cas": ${Mutation.CAS},
   *   }
   * }
   * */
  std::string xattr_cas_path("eventing.cas");
  std::string xattr_timer_path("eventing.timers");
  std::string mutation_cas_macro("\"${Mutation.CAS}\"");
  std::string doc_exptime("$document.exptime");
  timer_entry += "Z::";
  timer_entry += cb_func;
  timer_entry += "\"";
  timer_entry.insert(0, 1, '"');
  LOG(logTrace) << "Request to register doc timer, callback_func:" << cb_func
                << " doc_id:" << doc_id << " start_ts:" << timer_entry << '\n';

  while (true) {
    lcb_t *cb_instance =
        reinterpret_cast<lcb_t *>(args.GetIsolate()->GetData(1));

    lcb_CMDSUBDOC gcmd = {0};
    LCB_CMD_SET_KEY(&gcmd, doc_id.c_str(), doc_id.size());

    // Fetch document expiration using virtual extended attributes
    Result res;
    lcb_SDSPEC gspec = {0};
    gcmd.specs = &gspec;
    gcmd.nspecs = 1;

    gspec.sdcmd = LCB_SDCMD_GET;
    gspec.options = LCB_SDSPEC_F_XATTRPATH;
    LCB_SDSPEC_SET_PATH(&gspec, doc_exptime.c_str(), doc_exptime.size());
    lcb_subdoc3(*cb_instance, &res, &gcmd);
    lcb_wait(*cb_instance);

    LOG(logTrace) << "CreateDocTimer cas: " << res.cas
                  << " exptime: " << res.exptime << '\n';

    lcb_CMDSUBDOC mcmd = {0};
    lcb_SDSPEC spec = {0};
    LCB_CMD_SET_KEY(&mcmd, doc_id.c_str(), doc_id.size());

    std::vector<lcb_SDSPEC> specs;

    mcmd.cas = res.cas;
    // TODO - waiting on fix for https://issues.couchbase.com/browse/CCBC-799 -
    // updating expiration on subdoc mutation
    // mcmd.exptime = res.exptime;

    spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
    spec.options =
        LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTR_MACROVALUES;

    LCB_SDSPEC_SET_PATH(&spec, xattr_cas_path.c_str(), xattr_cas_path.size());
    LCB_SDSPEC_SET_VALUE(&spec, mutation_cas_macro.c_str(),
                         mutation_cas_macro.size());
    specs.push_back(spec);

    spec.sdcmd = LCB_SDCMD_ARRAY_ADD_LAST;
    spec.options = LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTRPATH;
    LCB_SDSPEC_SET_PATH(&spec, xattr_timer_path.c_str(),
                        xattr_timer_path.size());
    LCB_SDSPEC_SET_VALUE(&spec, timer_entry.c_str(), timer_entry.size());
    specs.push_back(spec);

    mcmd.specs = specs.data();
    mcmd.nspecs = specs.size();

    lcb_error_t rc = lcb_subdoc3(*cb_instance, &res, &mcmd);
    if (rc != LCB_SUCCESS) {
      LOG(logError) << "Failed to update timer related xattr fields for doc_id:"
                    << doc_id << " return code:" << rc
                    << " msg:" << lcb_strerror(NULL, rc) << '\n';
      return;
    }
    lcb_wait(*cb_instance);

    if (res.rc == LCB_SUCCESS) {
      LOG(logTrace) << "Stored doc_id timer_entry: " << timer_entry
                    << " for doc_id: " << doc_id << '\n';
      break;
    } else if (res.rc == LCB_KEY_EEXISTS) {
      LOG(logTrace) << "CAS Mismatch for " << doc_id << ". Retrying" << '\n';
      std::this_thread::sleep_for(
          std::chrono::milliseconds(LCB_OP_RETRY_INTERVAL));
      continue;
    } else {
      LOG(logTrace) << "Couldn't store xattr update as part of doc_id based "
                       "timer for doc_id:"
                    << doc_id << " return code: " << res.rc
                    << " msg: " << lcb_strerror(NULL, res.rc) << '\n';
      break;
    }
  }
}

// Exception details will be appended to the first argument.
std::string ExceptionString(v8::Isolate *isolate, v8::TryCatch *try_catch) {
  std::string out;
  size_t scratchSize = 20;
  char scratch[scratchSize]; // just some scratch space for sprintf

  v8::HandleScope handle_scope(isolate);
  v8::String::Utf8Value exception(try_catch->Exception());
  const char *exception_string = ToCString(exception);

  v8::Handle<v8::Message> message = try_catch->Message();

  if (message.IsEmpty()) {
    // V8 didn't provide any extra information about this error; just
    // print the exception.
    out.append(exception_string);
    out.append("\n");
  } else {
    // Print (filename):(line number)
    v8::String::Utf8Value filename(message->GetScriptOrigin().ResourceName());
    const char *filename_string = ToCString(filename);
    int linenum = message->GetLineNumber();

    snprintf(scratch, scratchSize, "%i", linenum);
    out.append(filename_string);
    out.append(":");
    out.append(scratch);
    out.append("\n");

    // Print line of source code.
    v8::String::Utf8Value sourceline(message->GetSourceLine());
    const char *sourceline_string = ToCString(sourceline);

    out.append(sourceline_string);
    out.append("\n");

    // Print wavy underline (GetUnderline is deprecated).
    int start = message->GetStartColumn();
    for (int i = 0; i < start; i++) {
      out.append(" ");
    }
    int end = message->GetEndColumn();
    for (int i = start; i < end; i++) {
      out.append("^");
    }
    out.append("\n");
    v8::String::Utf8Value stack_trace(try_catch->StackTrace());
    if (stack_trace.length() > 0) {
      const char *stack_trace_string = ToCString(stack_trace);
      out.append(stack_trace_string);
      out.append("\n");
    } else {
      out.append(exception_string);
      out.append("\n");
    }
  }
  return out;
}

std::vector<std::string> &split(const std::string &s, char delim,
                                std::vector<std::string> &elems) {
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
  return elems;
}

std::vector<std::string> split(const std::string &s, char delim) {
  std::vector<std::string> elems;
  split(s, delim, elems);
  return elems;
}

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

static void multi_op_callback(lcb_t cb_instance, int cbtype,
                              const lcb_RESPBASE *rb) {
  LOG(logTrace) << "Got callback for " << lcb_strcbtype(cbtype) << '\n';

  if (cbtype == LCB_CALLBACK_GET) {
    // lcb_get calls against metadata bucket is only triggered for timer lookups
    const lcb_RESPGET *rg = reinterpret_cast<const lcb_RESPGET *>(rb);
    const void *data = lcb_get_cookie(cb_instance);

    std::string ts;
    std::string timestamp_marker("");
    lcb_CMDSTORE acmd = {0};

    switch (rb->rc) {
    case LCB_KEY_ENOENT:
      ts.assign((const char *)data);

      LCB_CMD_SET_KEY(&acmd, ts.c_str(), ts.length());
      LCB_CMD_SET_VALUE(&acmd, timestamp_marker.c_str(),
                        timestamp_marker.length());
      acmd.operation = LCB_ADD;

      lcb_store3(cb_instance, NULL, &acmd);
      lcb_wait(cb_instance);
      break;
    case LCB_SUCCESS:
      LOG(logTrace) << string_sprintf("Value %.*s", (int)rg->nvalue, rg->value)
                    << '\n';
      break;
    default:
      LOG(logTrace) << "LCB_CALLBACK_GET: Operation failed, "
                    << lcb_strerror(NULL, rb->rc) << " rc:" << rb->rc << '\n';
      break;
    }
  }

  if (cbtype == LCB_CALLBACK_SDMUTATE) {
    const lcb_RESPSUBDOC *resp = reinterpret_cast<const lcb_RESPSUBDOC *>(rb);
    lcb_SDENTRY ent;
    size_t iter = 0;

    Result *res = reinterpret_cast<Result *>(rb->cookie);
    res->rc = rb->rc;

    if (lcb_sdresult_next(resp, &ent, &iter)) {
      LOG(logTrace) << string_sprintf("Status: 0x%x. Value: %.*s\n", ent.status,
                                      (int)ent.nvalue, ent.value)
                    << '\n';
    }
  }

  if (cbtype == LCB_CALLBACK_SDLOOKUP) {
    Result *res = reinterpret_cast<Result *>(rb->cookie);
    res->cas = rb->cas;
    res->rc = rb->rc;

    if (rb->rc == LCB_SUCCESS) {
      const lcb_RESPGET *rg = reinterpret_cast<const lcb_RESPGET *>(rb);
      res->value.assign(reinterpret_cast<const char *>(rg->value), rg->nvalue);

      const lcb_RESPSUBDOC *resp = reinterpret_cast<const lcb_RESPSUBDOC *>(rb);
      lcb_SDENTRY ent;
      size_t iter = 0;
      if (lcb_sdresult_next(resp, &ent, &iter)) {
        LOG(logTrace) << string_sprintf("Status: 0x%x. Value: %.*s\n",
                                        ent.status, (int)ent.nvalue, ent.value)
                      << '\n';

        std::string exptime(reinterpret_cast<const char *>(ent.value));
        exptime.substr(0, (int)ent.nvalue);

        unsigned long long int ttl;
        char *pEnd;
        ttl = strtoull(exptime.c_str(), &pEnd, 10);
        res->exptime = (uint32_t)ttl;
      } else {
        LOG(logTrace) << "LCB_CALLBACK_SDLOOKUP: No result!" << '\n';
      }
    }
  }
}

void enableRecursiveMutation(bool state) { enable_recursive_mutation = state; }

V8Worker::V8Worker(std::string app_name, std::string dep_cfg,
                   std::string host_addr, std::string kv_host_port,
                   std::string rbac_user, std::string rbac_pass,
                   int lcb_inst_capacity, int execution_timeout,
                   bool enable_recursive_mutation)
    : rbac_pass(rbac_pass), curr_host_addr(host_addr) {
  enableRecursiveMutation(enable_recursive_mutation);
  v8::V8::InitializeICU();
  platform = v8::platform::CreateDefaultPlatform();
  v8::V8::InitializePlatform(platform);
  v8::V8::Initialize();

  v8::Isolate::CreateParams create_params;
  create_params.array_buffer_allocator =
      v8::ArrayBuffer::Allocator::NewDefaultAllocator();
  ;

  isolate_ = v8::Isolate::New(create_params);
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  isolate_->SetCaptureStackTraceForUncaughtExceptions(true);
  isolate_->SetData(0, this);
  v8::Local<v8::ObjectTemplate> global = v8::ObjectTemplate::New(GetIsolate());

  v8::TryCatch try_catch;

  global->Set(v8::String::NewFromUtf8(GetIsolate(), "log"),
              v8::FunctionTemplate::New(GetIsolate(), Print));
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

  app_name_ = app_name;
  cb_kv_endpoint = kv_host_port;
  execute_start_time = Time::now();

  deployment_config *config = ParseDeployment(dep_cfg.c_str());

  cb_source_bucket.assign(config->source_bucket);

  std::map<std::string,
           std::map<std::string, std::vector<std::string>>>::iterator it =
      config->component_configs.begin();

  Bucket *bucket_handle = nullptr;
  debugger_started = false;
  execute_flag = false;
  shutdown_terminator = false;
  max_task_duration = SECS_TO_NS * execution_timeout;

  for (; it != config->component_configs.end(); it++) {
    if (it->first == "buckets") {
      std::map<std::string, std::vector<std::string>>::iterator bucket =
          config->component_configs["buckets"].begin();
      for (; bucket != config->component_configs["buckets"].end(); bucket++) {
        std::string bucket_alias = bucket->first;
        std::string bucket_name =
            config->component_configs["buckets"][bucket_alias][0];

        bucket_handle =
            new Bucket(this, bucket_name.c_str(), cb_kv_endpoint.c_str(),
                       bucket_alias.c_str(), rbac_user, rbac_pass);

        bucket_handles.push_back(bucket_handle);
      }
    }
  }

  LOG(logInfo) << "Initialised V8Worker handle, app_name: " << app_name
               << " curr_host_addr: " << curr_host_addr
               << " kv_host_port: " << kv_host_port
               << " rbac_user: " << rbac_user << " rbac_pass: " << rbac_pass
               << " lcb_cap: " << lcb_inst_capacity
               << " execution_timeout: " << execution_timeout
               << " enable_recursive_mutation: " << enable_recursive_mutation
               << '\n';

  connstr = "couchbase://" + cb_kv_endpoint + "/" + cb_source_bucket.c_str() +
            "?username=" + rbac_user + "&select_bucket=true";

  meta_connstr = "couchbase://" + cb_kv_endpoint + "/" +
                 config->metadata_bucket.c_str() + "?username=" + rbac_user +
                 "&select_bucket=true";

  conn_pool = new ConnectionPool(lcb_inst_capacity, cb_kv_endpoint,
                                 cb_source_bucket, rbac_user, rbac_pass);
  delete config;
}

V8Worker::~V8Worker() {
  context_.Reset();
  on_update_.Reset();
  on_delete_.Reset();
  delete conn_pool;
  delete n1ql_handle;
}

std::string GetWorkingPath() {
  char temp[MAXPATHLEN];
  return (getcwd(temp, MAXPATHLEN) ? std::string(temp) : std::string(""));
}

int V8Worker::V8WorkerLoad(std::string script_to_execute) {
  LOG(logInfo) << "getcwd: " << GetWorkingPath() << '\n';
  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  v8::Local<v8::Context> context =
      v8::Local<v8::Context>::New(GetIsolate(), context_);
  v8::Context::Scope context_scope(context);

  v8::TryCatch try_catch;
  if (debugger_started) {
    agent->Start(GetIsolate(), platform, nullptr);
    agent->PauseOnNextJavascriptStatement("Break on start");
  }
  std::string plain_js;
  int code = Jsify(script_to_execute.c_str(), &plain_js);
  LOG(logTrace) << "jsified code: " << plain_js << '\n';
  if (code != OK) {
    LOG(logError) << "failed to jsify: " << code << '\n';
    return code;
  }

  std::string transpiler_js_src = ReadFile(ESPRIMA_PATH) + '\n';
  transpiler_js_src += ReadFile(ESCODEGEN_PATH) + '\n';
  transpiler_js_src += ReadFile(ESTRAVERSE_PATH) + '\n';
  transpiler_js_src += ReadFile(TRANSPILER_JS_PATH) + '\n';

  n1ql_handle = new N1QL(conn_pool);

  Transpiler transpiler(transpiler_js_src);
  script_to_execute = transpiler.Transpile(plain_js) + '\n';
  script_to_execute += ReadFile(BUILTIN_JS_PATH) + '\n';

  v8::Local<v8::String> source =
      v8::String::NewFromUtf8(GetIsolate(), script_to_execute.c_str());

  script_to_execute_ = script_to_execute;
  LOG(logTrace) << "script to execute: " << script_to_execute << '\n';

  if (!ExecuteScript(source))
    return FAILED_TO_COMPILE_JS;

  v8::Local<v8::String> on_update =
      v8::String::NewFromUtf8(GetIsolate(), "OnUpdate",
                              v8::NewStringType::kNormal)
          .ToLocalChecked();

  v8::Local<v8::String> on_delete =
      v8::String::NewFromUtf8(GetIsolate(), "OnDelete",
                              v8::NewStringType::kNormal)
          .ToLocalChecked();

  v8::Local<v8::Value> on_update_val;
  v8::Local<v8::Value> on_delete_val;

  if (!context->Global()->Get(context, on_update).ToLocal(&on_update_val) ||
      !context->Global()->Get(context, on_delete).ToLocal(&on_delete_val)) {
    return NO_HANDLERS_DEFINED;
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
          return FAILED_INIT_BUCKET_HANDLE;
        }
      }
    }
  }

  if (transpiler.IsTimerCalled(script_to_execute)) {
    LOG(logDebug) << "Transpiler is called" << '\n';

    lcb_create_st crst;
    memset(&crst, 0, sizeof crst);

    crst.version = 3;
    crst.v.v3.connstr = connstr.c_str();
    crst.v.v3.type = LCB_TYPE_BUCKET;
    crst.v.v3.passwd = rbac_pass.c_str();

    lcb_create(&cb_instance, &crst);
    lcb_connect(cb_instance);
    lcb_wait(cb_instance);

    lcb_install_callback3(cb_instance, LCB_CALLBACK_DEFAULT, multi_op_callback);
    this->GetIsolate()->SetData(1, (void *)(&cb_instance));

    crst.version = 3;
    crst.v.v3.connstr = meta_connstr.c_str();
    crst.v.v3.type = LCB_TYPE_BUCKET;
    crst.v.v3.passwd = rbac_pass.c_str();

    lcb_create(&meta_cb_instance, &crst);
    lcb_connect(meta_cb_instance);
    lcb_wait(meta_cb_instance);

    lcb_install_callback3(meta_cb_instance, LCB_CALLBACK_DEFAULT,
                          multi_op_callback);
    this->GetIsolate()->SetData(2, (void *)(&meta_cb_instance));
  }

  // Spawning terminator thread to monitor the wall clock time for execution of
  // javascript code isn't going beyond max_task_duration. Passing reference to
  // current object instead of having terminator thread make a copy of the
  // object.
  // Spawned thread will execute the temininator loop logic in function call
  // operator()
  // for V8Worker class
  terminator_thr = new std::thread(std::ref(*this));

  return SUCCESS;
}

bool V8Worker::ExecuteScript(v8::Local<v8::String> script) {
  v8::HandleScope handle_scope(GetIsolate());

  v8::TryCatch try_catch(GetIsolate());

  v8::Local<v8::Context> context(GetIsolate()->GetCurrentContext());

  v8::Local<v8::Script> compiled_script;
  if (!v8::Script::Compile(context, script).ToLocal(&compiled_script)) {
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

  v8::Local<v8::Context> context =
      v8::Local<v8::Context>::New(GetIsolate(), context_);
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

  v8::Local<v8::Function> on_doc_update =
      v8::Local<v8::Function>::New(GetIsolate(), on_update_);

  execute_flag = true;
  execute_start_time = Time::now();
  on_doc_update->Call(context->Global(), 2, args);
  execute_flag = false;

  if (try_catch.HasCaught()) {
    LOG(logDebug) << "Exception message: "
                  << ExceptionString(GetIsolate(), &try_catch) << '\n';
    return ON_UPDATE_CALL_FAIL;
  }

  return SUCCESS;
}

int V8Worker::SendDelete(std::string meta) {
  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  v8::Local<v8::Context> context =
      v8::Local<v8::Context>::New(GetIsolate(), context_);
  v8::Context::Scope context_scope(context);

  LOG(logTrace) << " meta: " << meta << '\n';
  v8::TryCatch try_catch(GetIsolate());

  v8::Local<v8::Value> args[1];
  args[0] =
      v8::JSON::Parse(v8::String::NewFromUtf8(GetIsolate(), meta.c_str()));

  assert(!try_catch.HasCaught());

  v8::Local<v8::Function> on_doc_delete =
      v8::Local<v8::Function>::New(GetIsolate(), on_delete_);

  execute_flag = true;
  execute_start_time = Time::now();
  on_doc_delete->Call(context->Global(), 1, args);
  execute_flag = false;

  if (try_catch.HasCaught()) {
    LOG(logError) << "Exception message"
                  << ExceptionString(GetIsolate(), &try_catch) << '\n';
    return ON_DELETE_CALL_FAIL;
  }

  return SUCCESS;
}

void V8Worker::SendNonDocTimer(std::string doc_ids_cb_fns) {
  std::vector<std::string> entries = split(doc_ids_cb_fns, ';');
  char tstamp[BUFSIZE], fn[BUFSIZE];

  if (entries.size() > 0) {
    v8::Locker locker(GetIsolate());
    v8::Isolate::Scope isolate_scope(GetIsolate());
    v8::HandleScope handle_scope(GetIsolate());

    v8::Local<v8::Context> context =
        v8::Local<v8::Context>::New(GetIsolate(), context_);
    v8::Context::Scope context_scope(context);

    for (auto entry : entries) {
      if (entry.length() > 0) {
        sscanf(entry.c_str(),
               "{\"callback_func\": \"%[^\"]\", \"start_ts\": \"%[^\"]\"}", fn,
               tstamp);
        LOG(logTrace) << "Non doc timer event for callback_fn: " << fn
                      << " tstamp: " << tstamp << '\n';

        v8::Handle<v8::Value> val =
            context->Global()->Get(createUtf8String(GetIsolate(), fn));
        v8::Handle<v8::Function> cb_func = v8::Handle<v8::Function>::Cast(val);

        v8::Handle<v8::Value> arg[0];

        execute_flag = true;
        execute_start_time = Time::now();
        cb_func->Call(context->Global(), 0, arg);
        execute_flag = false;
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

  v8::Local<v8::Context> context =
      v8::Local<v8::Context>::New(GetIsolate(), context_);
  v8::Context::Scope context_scope(context);

  v8::Handle<v8::Value> val = context->Global()->Get(
      v8::String::NewFromUtf8(GetIsolate(), callback_fn.c_str(),
                              v8::NewStringType::kNormal)
          .ToLocalChecked());
  v8::Handle<v8::Function> cb_fn = v8::Handle<v8::Function>::Cast(val);

  v8::Handle<v8::Value> arg[1];
  arg[0] = v8::String::NewFromUtf8(GetIsolate(), doc_id.c_str());

  execute_flag = true;
  execute_start_time = Time::now();
  cb_fn->Call(context->Global(), 1, arg);
  execute_flag = false;
}

void V8Worker::StartDebugger() {
  if (debugger_started) {
    LOG(logError) << "Debugger already started" << '\n';
    return;
  }
  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());
  v8::Local<v8::Context> context =
      v8::Local<v8::Context>::New(GetIsolate(), context_);
  v8::Context::Scope context_scope(context);
  LOG(logInfo) << "Starting Debugger" << '\n';
  debugger_started = true;
  agent = new inspector::Agent(curr_host_addr, GetWorkingPath() + "/" +
                                                   app_name_ + "_frontend.url");
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

std::string V8Worker::GetSourceMap() { return source_map_; }

const char *V8Worker::V8WorkerLastException() { return last_exception.c_str(); }

const char *V8Worker::V8WorkerVersion() { return v8::V8::GetVersion(); }

void V8Worker::V8WorkerTerminateExecution() {
  v8::V8::TerminateExecution(GetIsolate());
}
