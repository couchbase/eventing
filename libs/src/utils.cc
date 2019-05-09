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

#include "crc64.h"
#include "isolate_data.h"
#include "js_exception.h"
#include "utils.h"

#include "../../gen/js/escodegen.h"
#include "../../gen/js/esprima.h"
#include "../../gen/js/estraverse.h"
#include "../../gen/js/source-map.h"
#include "../../gen/js/transpiler.h"

static bool ipv6 = false;
std::mutex time_now_mutex;
std::mutex convert_to_iso8601_mutex;

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

  auto v8maybe_str =
      v8::String::NewFromUtf8(isolate, str, v8::NewStringType::kNormal);
  v8::Local<v8::String> v8local_str;
  if (TO_LOCAL(v8maybe_str, &v8local_str)) {
    return handle_scope.Escape(v8local_str);
  }

  // TODO : Need to throw an exception and propagate it to the handler
  return handle_scope.Escape(v8::String::Empty(isolate));
}

v8::Local<v8::String> v8Str(v8::Isolate *isolate, const std::string &str) {
  v8::EscapableHandleScope handle_scope(isolate);

  auto v8maybe_str =
      v8::String::NewFromUtf8(isolate, str.c_str(), v8::NewStringType::kNormal);
  v8::Local<v8::String> v8local_str;
  if (TO_LOCAL(v8maybe_str, &v8local_str)) {
    return handle_scope.Escape(v8local_str);
  }

  return handle_scope.Escape(v8::String::Empty(isolate));
}

v8::Local<v8::Name> v8Name(v8::Isolate *isolate, uint32_t key) {
  v8::EscapableHandleScope handle_scope(isolate);

  auto key_v8_str = v8Str(isolate, std::to_string(key));
  v8::Local<v8::Name> key_name(key_v8_str);
  return handle_scope.Escape(key_name);
}

v8::Local<v8::Array> v8Array(v8::Isolate *isolate,
                             const std::vector<std::string> &from) {
  v8::EscapableHandleScope handle_scope(isolate);

  auto context = isolate->GetCurrentContext();
  auto array = v8::Array::New(isolate, static_cast<int>(from.size()));
  for (uint32_t i = 0; i < from.size(); ++i) {
    auto success = false;
    if (!TO(array->Set(context, i, v8Str(isolate, from[i])), &success)) {
      return handle_scope.Escape(array);
    }
  }

  return handle_scope.Escape(array);
}

std::string JSONStringify(v8::Isolate *isolate,
                          const v8::Local<v8::Value> &object) {
  if (IS_EMPTY(object)) {
    return "";
  }

  v8::HandleScope handle_scope(isolate);

  auto context = isolate->GetCurrentContext();
  auto global = context->Global();

  auto key = v8Str(isolate, "JSON");
  v8::Local<v8::Value> v8val_json;
  if (!TO_LOCAL(global->Get(context, key), &v8val_json)) {
    return "";
  }

  v8::Local<v8::Object> v8obj_json;
  if (!TO_LOCAL(v8val_json->ToObject(context), &v8obj_json)) {
    return "";
  }

  key = v8Str(isolate, "stringify");
  v8::Local<v8::Value> v8val_stringify;
  if (!TO_LOCAL(v8obj_json->Get(context, key), &v8val_stringify)) {
    return "";
  }

  auto v8fun_stringify = v8val_stringify.As<v8::Function>();
  v8::Local<v8::Value> args[1] = {object};

  v8::Local<v8::Value> v8obj_result;
  if (!TO_LOCAL(v8fun_stringify->Call(context, global, 1, args),
                &v8obj_result)) {
    return "";
  }

  v8::String::Utf8Value utf8_result(v8obj_result);
  return *utf8_result;
}

// Extracts a C string from a V8 Utf8Value.
const char *ToCString(const v8::String::Utf8Value &value) {
  return *value ? *value : "<std::string conversion failed>";
}

std::string ConvertToISO8601(std::string timestamp) {
  std::lock_guard<std::mutex> lock(convert_to_iso8601_mutex);

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

// Exception details will be appended to the first argument.
std::string ExceptionString(v8::Isolate *isolate, v8::TryCatch *try_catch) {
  std::string out;
  char scratch[EXCEPTION_STR_SIZE]; // just some scratch space for sprintf

  v8::HandleScope handle_scope(isolate);
  const char *exception_string =
      JSONStringify(isolate, try_catch->Exception()).c_str();
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

void SetIPv6(bool is6) { ipv6 = is6; }

std::string Localhost(bool isUrl) {
  return ipv6 ? (isUrl ? "[::1]" : "::1") : "127.0.0.1";
}

bool IsIPv6() { return ipv6; }

std::string JoinHostPort(const std::string &host, const std::string &port) {
  static std::regex ipv6re("^[0-9a-fA-F:]*:[0-9a-fA-F:]+$");
  return std::regex_match(host, ipv6re) ? "[" + host + "]:" + port
                                        : host + ":" + port;
}

std::pair<std::string, std::string> GetLocalKey() {
  const char *usr = std::getenv("CBEVT_CALLBACK_USR");
  const char *key = std::getenv("CBEVT_CALLBACK_KEY");
  if (!usr || !key) {
    LOG(logError) << "Failed to read CBEVT_CALLBACK_USR/KEY env var"
                  << std::endl;
    usr = "unknown-client";
    key = "unknown-client";
  }
  return std::make_pair<std::string, std::string>(usr, key);
}

std::string GetTimestampNow() {
  // std::ctime is not thread safe -
  // http://en.cppreference.com/w/cpp/chrono/c/ctime
  std::lock_guard<std::mutex> lock(time_now_mutex);

  auto now = std::chrono::system_clock::now();
  auto now_time = std::chrono::system_clock::to_time_t(now);
  std::string now_str = std::ctime(&now_time);
  now_str.erase(std::remove(now_str.begin(), now_str.end(), '\n'),
                now_str.end());
  return ConvertToISO8601(now_str) + "Z";
}

ParseInfo UnflattenParseInfo(std::unordered_map<std::string, std::string> &kv) {
  ParseInfo info;
  info.is_valid = std::stoi(kv["is_valid"]) != 0;
  info.is_select_query = std::stoi(kv["is_select_query"]) != 0;
  info.is_dml_query = std::stoi(kv["is_dml_query"]) != 0;
  info.keyspace_name = kv["keyspace_name"];
  info.info = kv["info"];
  return info;
}

bool IsRetriable(lcb_error_t error) {
  return static_cast<bool>(LCB_EIFTMP(error));
}

bool IsTerminatingRetriable(bool retry) { return retry; }

bool IsExecutionTerminating(v8::Isolate *isolate) {
  return isolate->IsExecutionTerminating();
}

Utils::Utils(v8::Isolate *isolate, const v8::Local<v8::Context> &context)
    : isolate_(isolate), curl_handle_(curl_easy_init()) {
  v8::HandleScope handle_scope(isolate_);

  context_.Reset(isolate_, context);
  global_.Reset(isolate_, context->Global());
}

Utils::~Utils() {
  curl_easy_cleanup(curl_handle_);
  context_.Reset();
  global_.Reset();
}

v8::Local<v8::Value>
Utils::GetPropertyFromGlobal(const std::string &method_name) {
  v8::EscapableHandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  auto global = global_.Get(isolate_);

  auto method_name_v8str = v8Str(isolate_, method_name.c_str());
  v8::Local<v8::Value> method;
  if (!TO_LOCAL(global->Get(context, method_name_v8str), &method)) {
    return handle_scope.Escape(method);
  }

  return handle_scope.Escape(method);
}

v8::Local<v8::Value>
Utils::GetPropertyFromObject(const v8::Local<v8::Value> &obj_v8val,
                             const std::string &method_name) {
  v8::EscapableHandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  v8::Local<v8::Value> method;

  v8::Local<v8::Object> obj_local;
  if (!TO_LOCAL(obj_v8val->ToObject(context), &obj_local)) {
    return handle_scope.Escape(method);
  }

  auto method_name_v8str = v8Str(isolate_, method_name.c_str());
  if (!TO_LOCAL(obj_local->Get(context, method_name_v8str), &method)) {
    return handle_scope.Escape(method);
  }

  return handle_scope.Escape(method);
}

std::string Utils::GetFunctionName(const v8::Local<v8::Value> &func_val) {
  v8::HandleScope handle_scope(isolate_);

  auto func = func_val.As<v8::Function>();
  return ToCPPString(func->GetName());
}

std::string Utils::ToCPPString(const v8::Local<v8::Value> &str_val) {
  v8::HandleScope handle_scope(isolate_);

  v8::String::Utf8Value utf8(str_val);
  std::string str = *utf8;
  return str;
}

bool Utils::IsFuncGlobal(const v8::Local<v8::Value> &func) {
  v8::HandleScope handle_scope(isolate_);

  auto js_exception = UnwrapData(isolate_)->js_exception;
  if (!func->IsFunction()) {
    auto message = "Invalid arg: Function reference expected";
    js_exception->ThrowEventingError(message);
    return false;
  }

  auto func_ref = func.As<v8::Function>();
  auto func_name = ToCPPString(func_ref->GetName());

  if (func_name.empty()) {
    auto message = "Invalid arg: Anonymous function is not allowed";
    js_exception->ThrowEventingError(message);
    return false;
  }

  auto global_func_val = GetPropertyFromGlobal(func_name);
  auto global_func = global_func_val.As<v8::Function>();
  if (global_func->IsUndefined()) {
    auto message = func_name + " is not accessible from global scope";
    js_exception->ThrowEventingError(message);
    return false;
  }

  return true;
}

std::string Utils::TrimBack(const std::string &s, const char *ws) const {
  std::string ts(s);
  ts.erase(ts.find_last_not_of(ws) + 1);
  return ts;
}

std::string Utils::TrimFront(const std::string &s, const char *ws) const {
  std::string ts(s);
  ts.erase(0, ts.find_first_not_of(ws));
  return ts;
}

std::string Utils::Trim(const std::string &s, const char *ws) const {
  return TrimFront(TrimBack(s, ws), ws);
}

v8::Local<v8::ArrayBuffer> Utils::ToArrayBuffer(void *buffer,
                                                std::size_t size) {
  v8::EscapableHandleScope handle_scope(isolate_);

  auto arr_buf = v8::ArrayBuffer::New(isolate_, size);
  memcpy(arr_buf->GetContents().Data(), buffer, size);
  return handle_scope.Escape(arr_buf);
}

UrlEncode Utils::UrlEncodeAsString(const std::string &data) {
  if (curl_handle_ == nullptr) {
    return {true, "Curl handle is not initialized"};
  }

  // libcurl uses strlen if last param is 0
  // It's preferable to use 0 because the return type of strlen is size_t, which
  // has a bigger range than int
  auto encoded_ptr = curl_easy_escape(curl_handle_, data.c_str(), 0);
  if (encoded_ptr == nullptr) {
    return {true, "Unable to url encode " + data};
  }

  std::string encoded(encoded_ptr);
  curl_free(encoded_ptr);
  return {encoded};
}

UrlEncode Utils::UrlEncodeAsKeyValue(const v8::Local<v8::Value> &obj_val) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  v8::Local<v8::Object> obj;
  if (!TO_LOCAL(obj_val->ToObject(context), &obj)) {
    return {true, "Unable to read JSON"};
  }

  v8::Local<v8::Array> keys_arr;
  if (!TO_LOCAL(obj->GetOwnPropertyNames(context), &keys_arr)) {
    return {true, "Unable to read keys"};
  }

  std::string encoded;
  for (uint32_t i = 0, len = keys_arr->Length(); i < len; ++i) {
    v8::Local<v8::Value> key_v8val;
    if (!TO_LOCAL(keys_arr->Get(context, i), &key_v8val)) {
      return {true, "Unable to read keys"};
    }
    v8::String::Utf8Value key_utf8(key_v8val);

    v8::Local<v8::Value> value_v8val;
    if (!TO_LOCAL(obj->Get(context, key_v8val), &value_v8val)) {
      return {true, "Unable to read value of key " + std::string(*key_utf8)};
    }
    std::string value;
    if (value_v8val->IsString()) {
      v8::String::Utf8Value value_utf8(value_v8val);
      value = *value_utf8;
    } else {
      value = JSONStringify(isolate_, value_v8val);
    }

    auto info = UrlEncodeAsString(*key_utf8);
    if (info.is_fatal) {
      return info;
    }
    if (!encoded.empty()) {
      encoded += "&";
    }
    encoded += info.encoded;
    encoded += "=";

    info = UrlEncodeAsString(value);
    if (info.is_fatal) {
      return info;
    }
    encoded += info.encoded;
  }
  return {encoded};
}

UrlDecode Utils::UrlDecodeString(const std::string &data) {
  auto n_decode = 0;
  auto decoded_ptr =
      curl_easy_unescape(curl_handle_, data.c_str(), 0, &n_decode);
  if (decoded_ptr == nullptr) {
    return {true, "Unable to url decode " + data};
  }
  std::string decoded(decoded_ptr);
  curl_free(decoded_ptr);
  return {decoded};
}

UrlDecode Utils::UrlDecodeAsKeyValue(const std::string &data,
                                     v8::Local<v8::Object> &obj_out) {
  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  std::istringstream tokenizer(data);
  std::string item;
  while (std::getline(tokenizer, item, '&')) {
    auto i = item.find('=');
    if (i == std::string::npos) {
      return {true, "Encoded string is not delimited by ="};
    }

    auto key_info = UrlDecodeString(item.substr(0, i));
    if (key_info.is_fatal) {
      return key_info;
    }

    auto value = item.substr(i + 1);
    std::replace(value.begin(), value.end(), '+', ' ');
    auto value_info = UrlDecodeString(value);
    if (value_info.is_fatal) {
      return value_info;
    }

    auto result = false;
    if (!TO(obj_out->Set(context, v8Str(isolate_, key_info.decoded),
                         v8Str(isolate_, value_info.decoded)),
            &result) ||
        !result) {
      return {true, "Unable to set key value"};
    }
  }
  return {};
}

UrlEncode Utils::UrlEncodeAny(const v8::Local<v8::Value> &val) {
  auto utils = UnwrapData(isolate_)->utils;

  if (val->IsObject()) {
    return utils->UrlEncodeAsKeyValue(val);
  }

  v8::String::Utf8Value val_utf8(val);
  return utils->UrlEncodeAsString(*val_utf8);
}

UrlDecode
Utils::UrlDecodeAsKeyValue(const std::string &data,
                           std::unordered_map<std::string, std::string> &kv) {
  std::istringstream tokenizer(data);
  std::string item;
  while (std::getline(tokenizer, item, '&')) {
    auto i = item.find('=');
    if (i == std::string::npos) {
      return {true, "Encoded string is not delimited by ="};
    }

    auto key_info = UrlDecodeString(item.substr(0, i));
    if (key_info.is_fatal) {
      return key_info;
    }

    auto value = item.substr(i + 1);
    std::replace(value.begin(), value.end(), '+', ' ');
    auto value_info = UrlDecodeString(value);
    if (value_info.is_fatal) {
      return value_info;
    }

    kv[key_info.decoded] = value_info.decoded;
  }
  return UrlDecode();
}

void UrlEncodeFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);

  auto js_exception = UnwrapData(isolate)->js_exception;
  auto utils = UnwrapData(isolate)->utils;

  if (args.Length() == 0) {
    js_exception->ThrowEventingError("Need at least one parameter");
    return;
  }

  UrlEncode info;
  if (args[0]->IsObject()) {
    info = utils->UrlEncodeAsKeyValue(args[0]);
    if (info.is_fatal) {
      js_exception->ThrowEventingError(info.msg);
      return;
    }
  } else {
    v8::String::Utf8Value arg_utf8(args[0]);
    info = utils->UrlEncodeAsString(*arg_utf8);
    if (info.is_fatal) {
      js_exception->ThrowEventingError(info.msg);
      return;
    }
  }

  args.GetReturnValue().Set(v8Str(isolate, info.encoded));
}

void UrlDecodeFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);

  auto js_exception = UnwrapData(isolate)->js_exception;
  auto utils = UnwrapData(isolate)->utils;

  if (args.Length() == 0) {
    js_exception->ThrowEventingError("Need at least one parameter");
    return;
  }
  if (!args[0]->IsString()) {
    js_exception->ThrowEventingError("Expected an argument of type string");
    return;
  }

  v8::String::Utf8Value arg_utf8(args[0]);
  if (strchr(*arg_utf8, '=') == nullptr) {
    auto info = utils->UrlDecodeString(*arg_utf8);
    if (info.is_fatal) {
      js_exception->ThrowEventingError(info.msg);
      return;
    }
    args.GetReturnValue().Set(v8Str(isolate, info.decoded));
    return;
  }

  auto decoded_obj = v8::Object::New(isolate);
  auto info = utils->UrlDecodeAsKeyValue(*arg_utf8, decoded_obj);
  if (info.is_fatal) {
    js_exception->ThrowEventingError(info.msg);
    return;
  }
  args.GetReturnValue().Set(decoded_obj);
}

void Crc64Function(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);

  auto js_exception = UnwrapData(isolate)->js_exception;

  if (args.Length() != 1) {
    js_exception->ThrowEventingError("Need one parameter");
    return;
  }
  const uint8_t *data = nullptr;
  uint64_t crc = 0;
  uint64_t len = 0;
  if (args[0]->IsArrayBuffer()) {
    auto array_buf = args[0].As<v8::ArrayBuffer>();
    auto contents = array_buf->GetContents();
    data = static_cast<const uint8_t *>(contents.Data());
    len = contents.ByteLength();
    crc = crc64_iso.Checksum(data, len);
  } else {
    std::string data_str = JSONStringify(isolate, args[0]);
    data = reinterpret_cast<const uint8_t *>(data_str.c_str());
    len = data_str.size();
    crc = crc64_iso.Checksum(data, len);
  }

  char crc_str[32] = {0};
  std::sprintf(crc_str, "%016llx", crc);
  args.GetReturnValue().Set(v8Str(isolate, crc_str));
}

std::string GetConnectionStr(const std::string &end_point,
                             const std::string &bucket_name) {
  auto connstr =
      "couchbase://" + end_point + "/" + bucket_name + "?select_bucket=true";
  if (IsIPv6()) {
    connstr += "&ipv6=allow";
  }
  return connstr;
}
