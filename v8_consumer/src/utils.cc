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

#include <regex>

#include "utils.h"
#include "js_exception.h"

#include "../../gen/js/escodegen.h"
#include "../../gen/js/esprima.h"
#include "../../gen/js/estraverse.h"
#include "../../gen/js/source-map.h"
#include "../../gen/js/transpiler.h"

static bool ipv6 = false;

#if defined(WIN32) || defined(_WIN32)
int Wvasprintf(char **strp, const char *fmt, va_list ap) {
  // _vscprintf tells you how big the buffer needs to be
  int len = _vscprintf(fmt, ap);
  if (len == -1) {
    return -1;
  }
  size_t size = (size_t)len + 1;
  char *str = static_cast<char *>(malloc(size));
  if (!str) {
    return -1;
  }

  // vsprintf_s is the "secure" version of vsprintf
  int r = vsprintf_s(str, len + 1, fmt, ap);
  if (r == -1) {
    free(str);
    return -1;
  }
  *strp = str;
  return r;
}

int WinSprintf(char **strp, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  int r = Wvasprintf(strp, fmt, ap);
  va_end(ap);
  return r;
}
#endif

v8::Local<v8::String> v8Str(v8::Isolate *isolate, const char *str) {
  v8::EscapableHandleScope handle_scope(isolate);

  auto v8_str =
      v8::String::NewFromUtf8(isolate, str, v8::NewStringType::kNormal)
          .ToLocalChecked();
  return handle_scope.Escape(v8_str);
}

v8::Local<v8::String> v8Str(v8::Isolate *isolate, const std::string &str) {
  v8::EscapableHandleScope handle_scope(isolate);

  auto v8_str =
      v8::String::NewFromUtf8(isolate, str.c_str(), v8::NewStringType::kNormal)
          .ToLocalChecked();
  return handle_scope.Escape(v8_str);
}

v8::Local<v8::Name> v8Name(v8::Isolate *isolate, uint32_t key) {
  v8::EscapableHandleScope handle_scope(isolate);

  auto key_v8_str = v8Str(isolate, std::to_string(key));
  v8::Local<v8::Name> key_name(key_v8_str);
  return handle_scope.Escape(key_name);
}

std::string JSONStringify(v8::Isolate *isolate, v8::Handle<v8::Value> object) {
  v8::HandleScope handle_scope(isolate);

  v8::Local<v8::Context> context = isolate->GetCurrentContext();
  v8::Local<v8::Object> global = context->Global();

  auto JSON = global->Get(v8Str(isolate, "JSON"))->ToObject();
  auto JSON_stringify =
      v8::Local<v8::Function>::Cast(JSON->Get(v8Str(isolate, "stringify")));

  v8::Local<v8::Value> result;
  v8::Local<v8::Value> args[1];
  args[0] = {object};
  result = JSON_stringify->Call(context->Global(), 1, args);

  v8::String::Utf8Value utf8_result(result);
  std::string stringified_obj(*utf8_result);
  return stringified_obj;
}

lcb_t *UnwrapLcbInstance(v8::Local<v8::Object> obj) {
  v8::Local<v8::External> field =
      v8::Local<v8::External>::Cast(obj->GetInternalField(0));
  void *ptr = field->Value();
  return static_cast<lcb_t *>(ptr);
}

lcb_t *UnwrapV8WorkerLcbInstance(v8::Local<v8::Object> object) {
  v8::Local<v8::External> field =
      v8::Local<v8::External>::Cast(object->GetInternalField(2));
  void *ptr = field->Value();
  return static_cast<lcb_t *>(ptr);
}

// Extracts a C string from a V8 Utf8Value.
const char *ToCString(const v8::String::Utf8Value &value) {
  return *value ? *value : "<std::string conversion failed>";
}

bool ToCBool(const v8::Local<v8::Boolean> &value) {
  if (value.IsEmpty()) {
    LOG(logError) << "Failed to convert to bool" << std::endl;
  }

  return value->Value();
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
  auto js_exception = UnwrapData(isolate)->js_exception;

  if (args[i]->IsFunction()) {
    v8::Local<v8::Function> func_ref = args[i].As<v8::Function>();
    v8::String::Utf8Value func_name(func_ref->GetName());

    if (func_name.length()) {
      v8::Local<v8::Context> context = isolate->GetCurrentContext();
      v8::Local<v8::Function> timer_func_ref =
          context->Global()->Get(func_ref->GetName()).As<v8::Function>();

      if (timer_func_ref->IsUndefined()) {
        auto exception_msg =
            std::string(*func_name) + " is not defined in global scope";
        js_exception->Throw(exception_msg);
        LOG(logError) << exception_msg << std::endl;
        return false;
      }
    } else {
      auto exception_msg = "Invalid arg: Anonymous function is not allowed";
      js_exception->Throw(exception_msg);
      LOG(logError) << exception_msg << std::endl;
      return false;
    }
  } else {
    auto exception_msg = "Invalid arg: Function reference expected";
    js_exception->Throw(exception_msg);
    LOG(logError) << exception_msg << std::endl;
    return false;
  }

  return true;
}

// Exception details will be appended to the first argument.
std::string ExceptionString(v8::Isolate *isolate, v8::TryCatch *try_catch) {
  std::string out;
  char scratch[EXCEPTION_STR_SIZE]; // just some scratch space for sprintf

  v8::HandleScope handle_scope(isolate);
  v8::String::Utf8Value exception(try_catch->Exception());
  const char *exception_string = ToCString(exception);

  v8::Handle<v8::Message> message = try_catch->Message();

  if (message.IsEmpty()) {
    // V8 didn't provide any extra information about this error;
    // just print the exception.
    out.append(exception_string);
    out.append("\n");
  } else {
    // Print (filename):(line number)
    v8::String::Utf8Value filename(message->GetScriptOrigin().ResourceName());
    const char *filename_string = ToCString(filename);
    int linenum = message->GetLineNumber();

    snprintf(scratch, EXCEPTION_STR_SIZE, "%i", linenum);
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

std::string GetTranspilerSrc() {
  std::string transpiler_js_src =
      std::string((const char *)js_esprima) + '\n' +
      std::string((const char *)js_escodegen) + '\n' +
      std::string((const char *)js_estraverse) + '\n' +
      std::string((const char *)js_transpiler) + '\n' +
      std::string((const char *)js_source_map);
  return transpiler_js_src;
}

void SetIPv6(bool is6) {
  ipv6 = is6;
}

std::string Localhost(bool isUrl) {
  return ipv6 ?  (isUrl ? "[::1]" : "::1") : "127.0.0.1";
}

bool IsIPv6() {
  return ipv6;
}

std::string JoinHostPort(const std::string &host, const std::string &port) {
  static std::regex ipv6re("^[0-9a-f:]*:[0-9a-f:]+$");
  return std::regex_match(host, ipv6re) ? "["+host+"]:"+port : host+":"+port;
}

std::pair<std::string, std::string> GetLocalKey() {
  const char *usr = std::getenv("CBEVT_CALLBACK_USR");
  const char *key = std::getenv("CBEVT_CALLBACK_KEY");
  if (!usr || !key) {
    LOG(logError) << "Failed to read CBEVT_CALLBACK_USR/KEY env var" << std::endl;
    usr = "unknown-client";
    key = "unknown-client";
  }
  return std::make_pair<std::string,std::string>(usr, key);
}
