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
#include <unistd.h>
#include <vector>

#include "../include/bucket.h"
#include "../include/n1ql.h"
#include "../include/parse_deployment.h"

#include "../gen/builtin.h"
#include "../gen/escodegen.h"
#include "../gen/esprima.h"
#include "../gen/estraverse.h"
#include "../gen/source-map.h"
#include "../gen/transpiler.h"

#define BUFSIZE 100
#define MAXPATHLEN 256

bool enable_recursive_mutation = false;

N1QL *n1ql_handle;
JsException V8Worker::exception;

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

v8::Local<v8::String> createUtf8String(v8::Isolate *isolate, const char *str) {
  return v8::String::NewFromUtf8(isolate, str, v8::NewStringType::kNormal)
      .ToLocalChecked();
}

std::string ObjectToString(v8::Local<v8::Value> value) {
  v8::String::Utf8Value utf8_value(value);
  return std::string(*utf8_value);
}

std::string GetWorkingPath() {
  char temp[MAXPATHLEN];
  return (getcwd(temp, MAXPATHLEN) ? std::string(temp) : std::string(""));
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

void Log(const v8::FunctionCallbackInfo<v8::Value> &args) {
  std::string log_msg;
  for (int i = 0; i < args.Length(); i++) {
    log_msg += ToJson(args.GetIsolate(), args[i]);
    log_msg += ' ';
  }

  LOG(logDebug) << log_msg << '\n';
}

// console.log for debugger - also logs to eventing.log
void ConsoleLog(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  Log(args);
  auto console_v8_str = v8::String::NewFromUtf8(isolate, "console");
  auto log_v8_str = v8::String::NewFromUtf8(isolate, "log");
  auto console = context->Global()
                     ->Get(console_v8_str)
                     ->ToObject(context)
                     .ToLocalChecked();
  auto log_fn = v8::Local<v8::Function>::Cast(console->Get(log_v8_str));
  v8::Local<v8::Value> log_args[args.Length()];
  for (auto i = 0; i < args.Length(); ++i) {
    log_args[i] = args[i];
  }

  // Calling console.log with the args passed to log() function.
  log_fn->Call(log_fn, args.Length(), log_args);
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

  Result result;
  lcb_CMDSTORE cmd = {0};
  cmd.operation = LCB_APPEND;

  LOG(logTrace) << "Non doc_id timer entry to append: " << value << '\n';
  LCB_CMD_SET_KEY(&cmd, timer_entry.c_str(), timer_entry.length());
  LCB_CMD_SET_VALUE(&cmd, value.c_str(), value.length());
  lcb_sched_enter(*meta_cb_instance);
  lcb_store3(*meta_cb_instance, &result, &cmd);
  lcb_sched_leave(*meta_cb_instance);
  lcb_wait(*meta_cb_instance);

  if (result.rc != LCB_SUCCESS) {
    V8Worker::exception.Throw(*meta_cb_instance, result.rc);
    return;
  }
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
  std::string xattr_digest_path("eventing.digest");
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
    std::vector<lcb_SDSPEC> gspecs;
    lcb_SDSPEC exp_spec, fdoc_spec = {0};

    exp_spec.sdcmd = LCB_SDCMD_GET;
    exp_spec.options = LCB_SDSPEC_F_XATTRPATH;
    LCB_SDSPEC_SET_PATH(&exp_spec, doc_exptime.c_str(), doc_exptime.size());
    gspecs.push_back(exp_spec);

    fdoc_spec.sdcmd = LCB_SDCMD_GET_FULLDOC;
    gspecs.push_back(fdoc_spec);

    gcmd.specs = gspecs.data();
    gcmd.nspecs = gspecs.size();
    lcb_subdoc3(*cb_instance, &res, &gcmd);
    lcb_wait(*cb_instance);

    if (res.rc != LCB_SUCCESS) {
      LOG(logError)
          << "Failed to while performing lookup for fulldoc and exptime"
          << lcb_strerror(NULL, res.rc) << '\n';
      return;
    }

    uint32_t d = crc32c(0, res.value.c_str(), res.value.length());
    std::string digest = std::to_string(d);

    LOG(logTrace) << "CreateDocTimer cas: " << res.cas
                  << " exptime: " << res.exptime << " digest: " << digest
                  << '\n';

    lcb_CMDSUBDOC mcmd = {0};
    lcb_SDSPEC digest_spec, xattr_spec, tspec = {0};
    LCB_CMD_SET_KEY(&mcmd, doc_id.c_str(), doc_id.size());

    std::vector<lcb_SDSPEC> specs;

    mcmd.cas = res.cas;
    mcmd.exptime = res.exptime;

    digest_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
    digest_spec.options = LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTRPATH;

    LCB_SDSPEC_SET_PATH(&digest_spec, xattr_digest_path.c_str(),
                        xattr_digest_path.size());
    LCB_SDSPEC_SET_VALUE(&digest_spec, digest.c_str(), digest.size());
    specs.push_back(digest_spec);

    xattr_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
    xattr_spec.options =
        LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTR_MACROVALUES;

    LCB_SDSPEC_SET_PATH(&xattr_spec, xattr_cas_path.c_str(),
                        xattr_cas_path.size());
    LCB_SDSPEC_SET_VALUE(&xattr_spec, mutation_cas_macro.c_str(),
                         mutation_cas_macro.size());
    specs.push_back(xattr_spec);

    tspec.sdcmd = LCB_SDCMD_ARRAY_ADD_LAST;
    tspec.options = LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTRPATH;
    LCB_SDSPEC_SET_PATH(&tspec, xattr_timer_path.c_str(),
                        xattr_timer_path.size());
    LCB_SDSPEC_SET_VALUE(&tspec, timer_entry.c_str(), timer_entry.size());
    specs.push_back(tspec);

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
    if (res.rc != LCB_SUCCESS) {
      V8Worker::exception.Throw(*cb_instance, res.rc);
      return;
    }

    if (res.rc == LCB_SUCCESS) {
      LOG(logTrace) << "Stored doc_id timer_entry: " << timer_entry
                    << " for doc_id: " << doc_id << '\n';
      return;
    } else if (res.rc == LCB_KEY_EEXISTS) {
      LOG(logTrace) << "CAS Mismatch for " << doc_id << ". Retrying" << '\n';
      std::this_thread::sleep_for(
          std::chrono::milliseconds(LCB_OP_RETRY_INTERVAL));
      break;
    } else {
      LOG(logTrace) << "Couldn't store xattr update as part of doc_id based "
                       "timer for doc_id:"
                    << doc_id << " return code: " << res.rc
                    << " msg: " << lcb_strerror(NULL, res.rc) << '\n';
      return;
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

  if (cbtype == LCB_CALLBACK_STORE) {
    const lcb_RESPSTORE *rs = reinterpret_cast<const lcb_RESPSTORE *>(rb);
    Result *result = reinterpret_cast<Result *>(rb->cookie);
    result->rc = rs->rc;
  }

  if (cbtype == LCB_CALLBACK_GET) {
    // lcb_get calls against metadata bucket is only triggered for timer lookups
    const lcb_RESPGET *rg = reinterpret_cast<const lcb_RESPGET *>(rb);
    const void *data = lcb_get_cookie(cb_instance);

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

      lcb_store3(cb_instance, NULL, &acmd);
      lcb_wait(cb_instance);
      break;
    case LCB_SUCCESS:
      LOG(logTrace) << string_sprintf(
          "Value %.*s", static_cast<int>(rg->nvalue),
          reinterpret_cast<const char *>(rg->value));
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
      LOG(logTrace) << string_sprintf(
          "Status: 0x%x. Value: %.*s\n", ent.status,
          static_cast<int>(ent.nvalue),
          reinterpret_cast<const char *>(ent.value));
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
}

void enableRecursiveMutation(bool state) { enable_recursive_mutation = state; }

V8Worker::V8Worker(v8::Platform *platform, handler_config_t *h_config,
                   server_settings_t *settings)
    : platform_(platform), rbac_pass(settings->rbac_pass),
      curr_host(settings->host_addr),
      curr_eventing_port(settings->eventing_port) {
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
  cb_kv_endpoint = settings->kv_host_port;
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

        bucket_handle =
            new Bucket(this, bucket_name.c_str(), cb_kv_endpoint.c_str(),
                       bucket_alias.c_str(), settings->rbac_user, rbac_pass);

        bucket_handles.push_back(bucket_handle);
      }
    }
  }

  LOG(logInfo) << "Initialised V8Worker handle, app_name: "
               << h_config->app_name << " curr_host: " << curr_host
               << " curr_eventing_port: " << curr_eventing_port
               << " kv_host_port: " << cb_kv_endpoint
               << " rbac_user: " << settings->rbac_user
               << " rbac_pass: " << rbac_pass
               << " lcb_cap: " << h_config->lcb_inst_capacity
               << " execution_timeout: " << h_config->execution_timeout
               << " enable_recursive_mutation: " << enable_recursive_mutation
               << '\n';

  connstr = "couchbase://" + cb_kv_endpoint + "/" + cb_source_bucket.c_str() +
            "?username=" + settings->rbac_user + "&select_bucket=true";

  meta_connstr = "couchbase://" + cb_kv_endpoint + "/" +
                 config->metadata_bucket.c_str() +
                 "?username=" + settings->rbac_user + "&select_bucket=true";

  conn_pool =
      new ConnectionPool(h_config->lcb_inst_capacity, cb_kv_endpoint,
                         cb_source_bucket, settings->rbac_user, rbac_pass);
  src_path = GetWorkingPath() + "/" + app_name_ + ".t.js";
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

#ifdef ZLIB_FOUND
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
#endif

int V8Worker::V8WorkerLoad(std::string script_to_execute) {
  LOG(logInfo) << "getcwd: " << GetWorkingPath() << '\n';
  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  v8::TryCatch try_catch;
#ifdef FLEX_FOUND
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
#else
  std::string plain_js = script_to_execute;
  LOG(logError) << "jsify built without flex, n1ql will not work\n";
#endif

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
                           curr_host, curr_eventing_port) +
      '\n';
  source_map_ = transpiler.GetSourceMap(plain_js, app_name_ + ".js");
  LOG(logTrace) << "source map:" << source_map_ << '\n';

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
    LOG(logDebug) << "Timer is called" << '\n';

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
  // Spawned thread will execute the terminator loop logic in function call
  // operator() for V8Worker class
  terminator_thr = new std::thread(std::ref(*this));

  return SUCCESS;
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
#ifdef ZLIB_FOUND
    if (!agent->IsStarted()) {
      agent->Start(isolate_, platform_, src_path.c_str());
    }

    agent->PauseOnNextJavascriptStatement("Break on start");
    if (DebugExecute("OnUpdate", args, 2)) {
      return SUCCESS;
    }
#endif
    return ON_UPDATE_CALL_FAIL;
  } else {
    auto on_doc_update = on_update_.Get(isolate_);

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
#ifdef ZLIB_FOUND
    if (!agent->IsStarted()) {
      agent->Start(isolate_, platform_, src_path.c_str());
    }

    agent->PauseOnNextJavascriptStatement("Break on start");
    if (DebugExecute("OnDelete", args, 1)) {
      return SUCCESS;
    }
#endif
    return ON_DELETE_CALL_FAIL;
  } else {
    auto on_doc_delete = on_delete_.Get(isolate_);

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
            context->Global()->Get(createUtf8String(GetIsolate(), fn));
        v8::Handle<v8::Function> cb_func = v8::Handle<v8::Function>::Cast(val);

        v8::Handle<v8::Value> arg[0];

        if (debugger_started) {
#ifdef ZLIB_FOUND
          if (!agent->IsStarted()) {
            agent->Start(isolate_, platform_, src_path.c_str());
          }

          agent->PauseOnNextJavascriptStatement("Break on start");
          if (DebugExecute(fn, arg, 0)) {
            return;
          }
#endif
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
#ifdef ZLIB_FOUND
    if (!agent->IsStarted()) {
      agent->Start(isolate_, platform_, src_path.c_str());
    }

    agent->PauseOnNextJavascriptStatement("Break on start");
    if (DebugExecute(callback_fn.c_str(), arg, 1)) {
      return;
    }
#endif
  } else {
    execute_flag = true;
    execute_start_time = Time::now();
    cb_fn->Call(context->Global(), 1, arg);
    execute_flag = false;
  }
}

void V8Worker::StartDebugger() {
#ifdef ZLIB_FOUND
  if (debugger_started) {
    LOG(logError) << "Debugger already started" << '\n';
    return;
  }

  LOG(logInfo) << "Starting Debugger" << '\n';
  debugger_started = true;
  agent = new inspector::Agent(curr_host, GetWorkingPath() + "/" + app_name_ +
                                              "_frontend.url");
#endif
}

void V8Worker::StopDebugger() {
#ifdef ZLIB_FOUND
  if (debugger_started) {
    LOG(logInfo) << "Stopping Debugger" << '\n';
    debugger_started = false;
    agent->Stop();
    delete agent;
  } else {
    LOG(logError) << "Debugger wasn't started" << '\n';
  }
#endif
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
