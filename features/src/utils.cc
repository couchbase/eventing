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

#include <cinttypes>
#include <mutex>
#include <regex>
#include <sstream>

#include "crc64.h"
#include "error.h"
#include "isolate_data.h"
#include "js_exception.h"
#include "lcb_utils.h"
#include "utils.h"
#include <cinttypes>
#include <nlohmann/json.hpp>
#include <platform/base64.h>

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
  for (uint32_t i = 0; i < array->Length(); ++i) {
    auto success = false;
    if (!TO(array->Set(context, i, v8Str(isolate, from[i])), &success)) {
      return handle_scope.Escape(array);
    }
  }

  return handle_scope.Escape(array);
}

v8::Local<v8::Array> v8Array(v8::Isolate *isolate,
                             const std::vector<double> &from) {
  v8::EscapableHandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();
  auto array = v8::Array::New(isolate, static_cast<int>(from.size()));
  for (uint32_t i = 0; i < array->Length(); ++i) {
    auto success = false;
    if (!TO(array->Set(context, i, v8::Number::New(isolate, from[i])),
            &success)) {
      return handle_scope.Escape(array);
    }
  }

  return handle_scope.Escape(array);
}

std::string JSONStringify(v8::Isolate *isolate,
                          const v8::Local<v8::Value> &object,
                          bool pretty /* = false */) {
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
  v8::Local<v8::Value> args[3] = {object, v8::Null(isolate),
                                  v8::Integer::New(isolate, 2)};

  v8::Local<v8::Value> v8obj_result;
  if (!TO_LOCAL(v8fun_stringify->Call(context, global, pretty ? 3 : 1, args),
                &v8obj_result)) {
    return "";
  }

  v8::String::Utf8Value utf8_result(isolate, v8obj_result);
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

std::string ExceptionString(v8::Isolate *isolate,
                            v8::Local<v8::Context> &context,
                            v8::TryCatch *try_catch, bool script_timeout) {

  std::ostringstream os;

  V8ExceptionInfo v8exception_info =
      GetV8ExceptionInfo(isolate, context, try_catch, script_timeout);

  // The actual exception
  if (!v8exception_info.exception.empty()) {

    os << v8exception_info.exception;
    os << " " << std::endl;
  }

  // and its location
  if (!v8exception_info.file.empty()) {

    os << "Line: " << v8exception_info.line << " " << std::endl;

    if (!v8exception_info.srcLine.empty()) {
      os << "Code: " << RU(v8exception_info.srcLine) << " " << std::endl;
    }

    // and stack trace
    if (!v8exception_info.stack.empty()) {
      os << "Stack: " << std::endl
         << v8exception_info.stack << " " << std::endl;
    }
  }

  return os.str();
}

V8ExceptionInfo GetV8ExceptionInfo(v8::Isolate *isolate,
                                   v8::Local<v8::Context> &context,
                                   v8::TryCatch *try_catch,
                                   bool script_timeout) {

  V8ExceptionInfo v8exception_info;
  if (script_timeout) {
    // If function is terminated all the info like line stack trace is lost and
    // will be populated with wrong info Populate the exception and return
    v8exception_info.exception = "function execution timed out";
    return v8exception_info;
  }
  auto offset = UnwrapData(isolate)->insight_line_offset;
  v8::HandleScope handle_scope(isolate);

  // Extract exception object
  auto exception = try_catch->Exception();
  if (!exception.IsEmpty()) {
    // If the exception is of Error type, then call toString() on it

    v8::Local<v8::Object> obj;
    v8::Local<v8::Value> fn;
    v8::Local<v8::Value> val;
    if (exception->IsNativeError() &&
        TO_LOCAL(exception->ToObject(context), &obj) &&
        TO_LOCAL(obj->Get(context, v8Str(isolate, "toString")), &fn) &&
        TO_LOCAL(fn.As<v8::Function>()->Call(context, obj, 0, nullptr), &val)) {
      v8exception_info.exception = JSONStringify(isolate, val, true);
    } else {
      v8exception_info.exception =
          JSONStringify(isolate, try_catch->Exception(), true);
    }
  }

  // Extract exception location details
  v8::Handle<v8::Message> message = try_catch->Message();
  if (!message.IsEmpty()) {
    // Extract location
    v8::String::Utf8Value file(isolate, message->GetScriptResourceName());

    v8exception_info.file = ToCString(file);
    auto line = message->GetLineNumber(context).FromMaybe(0) - offset;
    v8exception_info.line = line > 0 ? line : 0;

    // Extract source code
    auto maybe_srcline = message->GetSourceLine(context);
    if (!maybe_srcline.IsEmpty()) {
      v8::Local<v8::String> local_srcline;
      TO_LOCAL(maybe_srcline, &local_srcline);
      v8::String::Utf8Value sourceline_utf8(isolate, local_srcline);
      std::string srcline = ToCString(sourceline_utf8);
      srcline = std::regex_replace(srcline, std::regex("^\\s+"), "");
      srcline = std::regex_replace(srcline, std::regex("\\s+$"), "");

      v8exception_info.srcLine = srcline;
    }

    // Extract stack trace
    auto maybe_stack = try_catch->StackTrace(context);
    if (!maybe_stack.IsEmpty()) {
      v8::Local<v8::Value> local_stack;
      TO_LOCAL(maybe_stack, &local_stack);
      v8::String::Utf8Value stack_utf8(isolate, local_stack);

      v8exception_info.stack = ToCString(stack_utf8);
    }
  }

  return v8exception_info;
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

bool IsTerminatingRetriable(bool retry) { return retry; }

bool IsExecutionTerminating(v8::Isolate *isolate) {
  return isolate->IsExecutionTerminating();
}

Utils::Utils(v8::Isolate *isolate, const v8::Local<v8::Context> &context,
             bool isIpv6, std::string certFile, std::string clientCertFile,
             std::string clientKeyFile)
    : isIpv6_(isIpv6), certFile_(certFile), client_cert_file_(clientCertFile),
      client_key_file_(clientKeyFile), isolate_(isolate),
      curl_handle_(curl_easy_init()) {
  v8::HandleScope handle_scope(isolate_);

  context_.Reset(isolate_, context);
  global_.Reset(isolate_, context->Global());
}

Utils::~Utils() {
  curl_easy_cleanup(curl_handle_);
  context_.Reset();
  global_.Reset();
}

const char *Utils::QUESTION_MARK = "?";
const char *Utils::EQ = "=";
const char *Utils::AMP = "&";
const char *Utils::SLASH = "/";
const std::string Utils::HEX_QUESTION_MARK = "%3F";
const std::string Utils::HEX_EQ = "%3D";
const std::string Utils::HEX_AMP = "%26";
const std::string Utils::HEX_SLASH = "%2F";

std::unordered_map<std::string, std::string> Utils::enc_to_dec_map = {
    {Utils::HEX_EQ, Utils::EQ},
    {Utils::HEX_AMP, Utils::AMP},
    {Utils::HEX_SLASH, Utils::SLASH},
    {Utils::HEX_QUESTION_MARK, Utils::QUESTION_MARK}};

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

  v8::String::Utf8Value utf8(isolate_, str_val);
  std::string str(*utf8, utf8.length());
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

Info Utils::ValidateDataType(const v8::Local<v8::Value> &arg) {
  if (arg->IsUndefined()) {
    return {true, R"("undefined" is not a valid type)"};
  }
  if (arg->IsFunction()) {
    return {true, R"("function" is not a valid type)"};
  }
  if (arg->IsSymbol()) {
    return {true, R"("symbol" is not a valid type)"};
  }
  return {false};
}

// KV engine only allows size of XATTR key less than 16 characters currently
bool Utils::ValidateXattrKeyLength(const std::string &key) {
  std::string_view root_key{key};
  auto dot_pos = key.find('.');
  if (dot_pos != std::string::npos) {
    root_key = root_key.substr(0, dot_pos);
  }
  return root_key.size() < 16;
}

ConnStrInfo Utils::GetConnectionString(const std::string &bucket) const {
  auto comm = UnwrapData(isolate_)->comm;
  auto nodes_info = comm->GetKVNodes();

  ConnStrInfo conn_info;
  conn_info.is_valid = false;
  if (!nodes_info.is_valid) {
    conn_info.msg = nodes_info.msg;
    return conn_info;
  }
  conn_info.is_valid = true;
  conn_info.conn_str = GetConnectionStr(isIpv6_, nodes_info, bucket, certFile_,
                                        client_cert_file_, client_key_file_);
  if (nodes_info.use_client_cert) {
    conn_info.client_key_passphrase = nodes_info.client_key_passphrase;
  }
  return conn_info;
}

v8::Local<v8::ArrayBuffer> Utils::ToArrayBuffer(const void *buffer,
                                                std::size_t size) {
  v8::EscapableHandleScope handle_scope(isolate_);

  auto arr_buf = v8::ArrayBuffer::New(isolate_, size);
  memcpy(arr_buf->GetBackingStore()->Data(), buffer, size);
  return handle_scope.Escape(arr_buf);
}

UrlEncode Utils::UrlEncodeAsString(const std::string &data) {
  if (curl_handle_ == nullptr) {
    return {true, "Curl handle is not initialized"};
  }

  // libcurl uses strlen if last param is 0
  // It's preferable to use 0 because the return type of strlen is
  // size_t, which has a bigger range than int
  auto encoded_ptr = curl_easy_escape(curl_handle_, data.c_str(), 0);
  if (encoded_ptr == nullptr) {
    return {true, "Unable to url encode " + data};
  }

  std::string encoded(encoded_ptr);
  curl_free(encoded_ptr);
  return {encoded};
}

UrlEncode Utils::EncodeAndNormalizePath(const std::string &path) {
  auto encoded_path = UrlEncodeAsString(path);
  if (encoded_path.is_fatal)
    return encoded_path;

  std::string encoded_url = encoded_path.encoded;
  for (auto itr = Utils::enc_to_dec_map.begin();
       itr != Utils::enc_to_dec_map.end(); itr++) {
    size_t found = encoded_url.find(itr->first);
    while (found != std::string::npos) {
      encoded_url.replace(found, itr->first.length(), itr->second);
      if (itr->first != Utils::HEX_SLASH)
        break;
      else
        found = encoded_url.find(itr->first, found + (itr->second.length()));
    }
  }
  return {encoded_url};
}

UrlEncode Utils::UrlEncodeAsKeyValue(const v8::Local<v8::Value> &obj_val,
                                     bool encode_kv_pairs) {
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

  std::string params_str;
  for (uint32_t i = 0, len = keys_arr->Length(); i < len; ++i) {
    v8::Local<v8::Value> key_v8val;
    if (!TO_LOCAL(keys_arr->Get(context, i), &key_v8val)) {
      return {true, "Unable to read keys"};
    }
    v8::String::Utf8Value key_utf8(isolate_, key_v8val);

    v8::Local<v8::Value> value_v8val;
    if (!TO_LOCAL(obj->Get(context, key_v8val), &value_v8val)) {
      return {true, "Unable to read value of key " + std::string(*key_utf8)};
    }
    std::string value;
    if (value_v8val->IsString()) {
      v8::String::Utf8Value value_utf8(isolate_, value_v8val);
      value = *value_utf8;
    } else {
      value = JSONStringify(isolate_, value_v8val);
    }

    if (!params_str.empty())
      params_str += "&";

    if (encode_kv_pairs) {
      auto info = UrlEncodeAsString(*key_utf8);
      if (info.is_fatal) {
        return info;
      }
      params_str += info.encoded;
    } else {
      params_str += *key_utf8;
    }
    params_str += "=";

    if (encode_kv_pairs) {
      auto info = UrlEncodeAsString(value);
      if (info.is_fatal) {
        return info;
      }
      params_str += info.encoded;
    } else {
      params_str += value;
    }
  }
  return {params_str};
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

std::pair<std::string, std::string>
Utils::ExtractPathAndQueryParamsFromURL(const std::string &encoded_url) {
  size_t qm_pos = encoded_url.find(Utils::QUESTION_MARK);
  if (qm_pos != std::string::npos) {
    if (qm_pos == encoded_url.size() - 1) {
      return std::make_pair(encoded_url.substr(0, qm_pos), "");
    }
    return std::make_pair(encoded_url.substr(0, qm_pos),
                          encoded_url.substr(qm_pos + 1, encoded_url.size()));
  }
  return std::make_pair(encoded_url, "");
}

UrlEncode
Utils::EncodeAndNormalizeQueryParams(const std::string &query_params) {
  if (query_params.size() == 0 || query_params == "") {
    return {false};
  }

  std::ostringstream result;
  size_t start_idx = 0;
  bool delim_found;

  while (start_idx <= query_params.size() - 1) {
    auto delim_pos = query_params.find(Utils::AMP, start_idx);
    if (delim_pos != std::string::npos) {
      delim_found = true;
    } else {
      delim_found = false;
      delim_pos = query_params.size();
    }
    auto single_query_param =
        query_params.substr(start_idx, (delim_pos - start_idx));
    auto encoded_single_query_param = UrlEncodeAsString(single_query_param);
    if (encoded_single_query_param.is_fatal)
      return encoded_single_query_param;

    auto normalized_single_query_param =
        NormalizeSingleQueryParam(encoded_single_query_param.encoded);
    result << normalized_single_query_param.c_str();
    if (delim_found)
      result << Utils::AMP;
    start_idx = delim_pos + 1;
  }

  return {result.str()};
}

std::string Utils::NormalizeSingleQueryParam(std::string &query_param) {
  size_t found = query_param.find(Utils::HEX_EQ);
  if (found != std::string::npos) {
    query_param.replace(found, Utils::HEX_EQ.size(), Utils::EQ);
  }
  return query_param;
}

UrlEncode Utils::UrlEncodePath(const std::string &raw_path,
                               const std::string &curl_lang_compat,
                               const std::string &app_lang_compat) {
  if (!curl_lang_compat.empty()) {
    if (curl_lang_compat == "7.1.0")
      return UrlEncodePath7_1_0(raw_path);
    return {raw_path};
  }
  return {raw_path};
}

UrlEncode Utils::UrlEncodeObjectParams(const v8::Local<v8::Value> &params_val,
                                       const std::string &curl_lang_compat,
                                       const std::string &app_lang_compat) {
  if (!curl_lang_compat.empty()) {
    if (curl_lang_compat == "7.2.0")
      return UrlEncodeAsKeyValue(params_val, false);
    else
      return UrlEncodeAsKeyValue(params_val, true);
  } else {
    if (app_lang_compat == "7.2.0")
      return UrlEncodeAsKeyValue(params_val, false);
    else
      return UrlEncodeAsKeyValue(params_val, true);
  }
}

UrlEncode Utils::UrlEncodeStringParams(const std::string &qry_params,
                                       const std::string &curl_lang_compat,
                                       const std::string &app_lang_compat) {
  if (!curl_lang_compat.empty()) {
    if (curl_lang_compat == "6.6.2") {
      return UrlEncodeAsString(qry_params);
    }
    if (curl_lang_compat == "7.1.0") {
      return EncodeAndNormalizeQueryParams(qry_params);
    }
    return {qry_params};
  } else {
    if (app_lang_compat == "6.0.0" || app_lang_compat == "6.5.0" ||
        app_lang_compat == "6.6.2") {
      return UrlEncodeAsString(qry_params);
    }
    return {qry_params};
  }
}

UrlEncode Utils::UrlEncodeParams(const v8::Local<v8::Value> &params_val,
                                 const std::string &curl_lang_compat,
                                 const std::string &app_lang_compat) {
  if (params_val->IsObject()) {
    auto encoded_query_params =
        UrlEncodeObjectParams(params_val, curl_lang_compat, app_lang_compat);
    if (encoded_query_params.is_fatal) {
      return {encoded_query_params.is_fatal, encoded_query_params.msg};
    }
    return {encoded_query_params.encoded};
  }

  v8::String::Utf8Value query_param_utf8(isolate_, params_val);
  auto raw_query_params_str = *query_param_utf8;
  auto encoded_query_params = UrlEncodeStringParams(
      raw_query_params_str, curl_lang_compat, app_lang_compat);
  if (encoded_query_params.is_fatal) {
    return {encoded_query_params.is_fatal, encoded_query_params.msg};
  }
  return {encoded_query_params.encoded};
}

UrlEncode Utils::UrlEncodePath7_1_0(const std::string &raw_path) {
  auto query_and_param_pair = ExtractPathAndQueryParamsFromURL(raw_path);
  auto encoded_path = EncodeAndNormalizePath(query_and_param_pair.first);
  if (encoded_path.is_fatal) {
    return {encoded_path.is_fatal, encoded_path.msg};
  }

  auto encoded_query_params =
      EncodeAndNormalizeQueryParams(query_and_param_pair.second);
  if (encoded_query_params.is_fatal) {
    return {encoded_query_params.is_fatal, encoded_query_params.msg};
  }

  std::ostringstream oss;
  if (encoded_query_params.encoded != "") {
    oss << encoded_path.encoded << QUESTION_MARK
        << encoded_query_params.encoded;
  } else {
    oss << encoded_path.encoded;
  }
  auto final_url{oss.str()};
  return {final_url};
}

void Crc64Function(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

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
    auto store = array_buf->GetBackingStore();
    data = static_cast<const uint8_t *>(store->Data());
    len = store->ByteLength();
    crc = crc64_iso.Checksum(data, len);
  } else {
    std::string data_str = JSONStringify(isolate, args[0]);
    data = reinterpret_cast<const uint8_t *>(data_str.c_str());
    len = data_str.size();
    crc = crc64_iso.Checksum(data, len);
  }

  char crc_str[32] = {0};
  std::snprintf(crc_str, sizeof(crc_str), "%016lx", (unsigned long)crc);
  args.GetReturnValue().Set(v8Str(isolate, crc_str));
}

void Crc64GoIsoFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  v8::HandleScope handle_scope(isolate);

  auto js_exception = UnwrapData(isolate)->js_exception;

  auto utils = UnwrapData(isolate)->utils;

  if (args.Length() != 1) {
    js_exception->ThrowEventingError("Need exactly one argument");
    return;
  }
  const uint8_t *data = nullptr;
  uint64_t crc = 0;
  uint64_t len = 0;
  if (args[0]->IsArrayBuffer()) {
    auto array_buf = args[0].As<v8::ArrayBuffer>();
    data = static_cast<const uint8_t *>(array_buf->Data());
    len = array_buf->ByteLength();
    crc = crc64_iso.Checksum(data, len);
  } else {
    std::string data_str;
    if (args[0]->IsString())
      data_str = utils->ToCPPString(args[0]);
    else
      data_str = JSONStringify(isolate, args[0]);
    data = reinterpret_cast<const uint8_t *>(data_str.c_str());
    len = data_str.size();
    crc = crc64_iso.Checksum(data, len);
  }

  char crc_str[32];
  std::snprintf(crc_str, sizeof(crc_str), "%016" PRIx64, crc);
  args.GetReturnValue().Set(v8Str(isolate, crc_str));
}

void Base64EncodeFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  v8::HandleScope handle_scope(isolate);

  auto js_exception = UnwrapData(isolate)->js_exception;

  if (args.Length() != 1) {
    js_exception->ThrowEventingError("Need exactly one argument");
    return;
  }

  std::string base64_string;
  if (args[0]->IsArrayBuffer()) {
    auto array_buf = args[0].As<v8::ArrayBuffer>();
    auto store = array_buf->GetBackingStore();
    base64_string.assign(static_cast<const char *>(store->Data()),
                         store->ByteLength());
  }
  if (args[0]->IsString()) {
    auto utils = UnwrapData(isolate)->utils;
    base64_string = utils->ToCPPString(args[0]);
  } else {
    base64_string = JSONStringify(isolate, args[0]);
  }

  std::string base64;
  try {
    base64 = cb::base64::encode(base64_string);
  } catch (const std::invalid_argument &i) {
    js_exception->ThrowEventingError("Invalid input");
    return;
  }

  args.GetReturnValue().Set(v8Str(isolate, base64));
}

void Base64DecodeFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  v8::HandleScope handle_scope(isolate);

  auto js_exception = UnwrapData(isolate)->js_exception;

  if (args.Length() != 1) {
    js_exception->ThrowEventingError("Need exactly one argument");
    return;
  }

  v8::Local<v8::Value> v8_base64_string = args[0];
  if (!v8_base64_string->IsString()) {
    js_exception->ThrowEventingError("Argument must be of type String");
    return;
  }
  auto utils = UnwrapData(isolate)->utils;
  auto base64_string = utils->ToCPPString(v8_base64_string);

  std::string base64_decoded;
  try {
    base64_decoded = cb::base64::decode(base64_string);
  } catch (const std::invalid_argument &i) {
    js_exception->ThrowEventingError("Invalid input");
    return;
  }
  args.GetReturnValue().Set(v8Str(isolate, base64_decoded));
}

bool isLittleEndian() {
  constexpr uint16_t num = 1;
  return (*(uint8_t *)&num == 1);
}

template <typename T,
          typename std::enable_if_t<
              std::is_same_v<T, float> || std::is_same_v<T, double>, int> = 0>
void push_bytes(T value, std::string &bytes) {
  auto *p = reinterpret_cast<uint8_t *>(&value);
  if (isLittleEndian()) {
    for (std::size_t i = 0; i < sizeof(T); ++i) {
      bytes.push_back(p[i]);
    }
  } else {
    std::size_t size = sizeof(T);
    for (std::size_t i = 0; i < size; i++) {
      bytes.push_back(p[size - i - 1]);
    }
  }
}

template <typename T,
          typename std::enable_if_t<
              std::is_same_v<T, float> || std::is_same_v<T, double>, int> = 0>
double get_value(const uint8_t *bytes, size_t index) {
  T d;
  if (isLittleEndian()) {
    memcpy(&d, bytes + index, sizeof(T));
  } else {
    constexpr auto size = sizeof(T);
    uint8_t bytesCopy[size];
    for (; index < size; ++index) {
      bytesCopy[size - index - 1] = bytes[index];
    }
    std::memcpy(&d, bytesCopy, sizeof(T));
  }

  return static_cast<double>(d);
}

template <typename T,
          typename std::enable_if_t<
              std::is_same_v<T, float> || std::is_same_v<T, double>, int> = 0>
std::tuple<Error, v8::Local<v8::Array>> get_v8_array(v8::Isolate *isolate,
                                                     std::string base64Bytes) {
  if (base64Bytes.length() % sizeof(T) != 0) {
    return {std::make_unique<std::string>("Incorrect base64 string provided"),
            v8::Local<v8::Array>()};
  }

  v8::EscapableHandleScope handle_scope(isolate);

  std::vector<double> array;
  array.reserve(base64Bytes.length() / sizeof(T));
  auto bytes = reinterpret_cast<const uint8_t *>(base64Bytes.data());
  for (size_t i = 0; i < base64Bytes.length(); i += sizeof(T)) {
    double d = get_value<T>(bytes, i);
    array.push_back(d);
  }
  return {nullptr, handle_scope.Escape(v8Array(isolate, array))};
}

void base64FloatArrayEncode(const v8::FunctionCallbackInfo<v8::Value> &args,
                            arrayEncoding encoding) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }
  auto context = isolate->GetCurrentContext();
  v8::HandleScope handle_scope(isolate);

  auto js_exception = UnwrapData(isolate)->js_exception;

  if (args.Length() != 1) {
    js_exception->ThrowEventingError("Need exactly one argument");
    return;
  }

  if (!args[0]->IsArray()) {
    js_exception->ThrowEventingError("Argument must be of type Array");
    return;
  }

  auto array = args[0].As<v8::Array>();
  auto length = array->Length();

  std::string bytes;
  switch (encoding) {
  case arrayEncoding::Float64Encoding:
    bytes.reserve(length * sizeof(double));
    break;
  case arrayEncoding::Float32Encoding:
    bytes.reserve(length * sizeof(float));
    break;
  }

  for (uint32_t i = 0; i < length; i++) {
    v8::Local<v8::Value> number_val;
    if (!TO_LOCAL(array->Get(context, i), &number_val)) {
      js_exception->ThrowEventingError("Error reading array element");
      return;
    }

    if (!number_val->IsNumber()) {
      js_exception->ThrowEventingError(
          "Array elements should be of type Number");
      return;
    }

    v8::Local<v8::Number> number_v8val;
    if (!TO_LOCAL(number_val->ToNumber(context), &number_v8val)) {
      js_exception->ThrowEventingError(
          "Unable to convert element to type Number");
      return;
    }
    switch (encoding) {
    case arrayEncoding::Float64Encoding: {
      double value = number_v8val->Value();
      push_bytes<double>(value, bytes);
    } break;
    case arrayEncoding::Float32Encoding: {
      float value = static_cast<float>(number_v8val->Value());
      push_bytes<float>(value, bytes);
    } break;
    }
  }

  std::string base64;
  try {
    base64 = cb::base64::encode(bytes);
  } catch (const std::invalid_argument &i) {
    js_exception->ThrowEventingError("Invalid input");
    return;
  }
  args.GetReturnValue().Set(v8Str(isolate, base64));
}

void Base64Float64EncodeFunction(
    const v8::FunctionCallbackInfo<v8::Value> &args) {
  base64FloatArrayEncode(args, arrayEncoding::Float64Encoding);
}

void Base64Float32EncodeFunction(
    const v8::FunctionCallbackInfo<v8::Value> &args) {
  base64FloatArrayEncode(args, arrayEncoding::Float32Encoding);
}

void base64FloatArrayDecode(const v8::FunctionCallbackInfo<v8::Value> &args,
                            arrayEncoding encoding) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }
  v8::HandleScope handle_scope(isolate);

  auto js_exception = UnwrapData(isolate)->js_exception;

  if (args.Length() != 1) {
    js_exception->ThrowEventingError("Need exactly one argument");
    return;
  }

  v8::Local<v8::Value> v8_base64_string = args[0];
  if (!v8_base64_string->IsString()) {
    js_exception->ThrowEventingError("Argument must be of type String");
    return;
  }

  auto utils = UnwrapData(isolate)->utils;
  std::string base64Bytes;
  try {
    base64Bytes = cb::base64::decode(utils->ToCPPString(v8_base64_string));
  } catch (const std::invalid_argument &i) {
    js_exception->ThrowEventingError("Invalid input");
    return;
  }
  std::tuple<Error, v8::Local<v8::Array>> returnValue;
  switch (encoding) {
  case arrayEncoding::Float64Encoding:
    returnValue = get_v8_array<double>(isolate, base64Bytes);
    break;

  case arrayEncoding::Float32Encoding:
    returnValue = get_v8_array<float>(isolate, base64Bytes);
    break;
  }

  auto error = std::move(std::get<0>(returnValue));
  if (error != nullptr) {
    js_exception->ThrowEventingError(*error);
    return;
  }
  args.GetReturnValue().Set(std::get<1>(returnValue));
}

void Base64Float64DecodeFunction(
    const v8::FunctionCallbackInfo<v8::Value> &args) {
  base64FloatArrayDecode(args, arrayEncoding::Float64Encoding);
}

void Base64Float32DecodeFunction(
    const v8::FunctionCallbackInfo<v8::Value> &args) {
  base64FloatArrayDecode(args, arrayEncoding::Float32Encoding);
}

std::string GetConnectionStr(bool isIpv6, const KVNodesInfo &nodes_info,
                             const std::string &bucket_name,
                             const std::string certFile,
                             const std::string client_cert_file,
                             const std::string client_key_file) {
  std::string nodes_list;
  for (std::vector<std::string>::const_iterator nodes_iter =
           nodes_info.kv_nodes.begin();
       nodes_iter != nodes_info.kv_nodes.end(); ++nodes_iter) {
    nodes_list += *nodes_iter;
    if (nodes_iter != nodes_info.kv_nodes.end() - 1) {
      nodes_list += ',';
    }
  }

  std::stringstream conn_str;
  if (nodes_info.encrypt_data) {
    conn_str << "couchbases://" << nodes_list << '/' << bucket_name
             << "?select_bucket=true&detailed_errcodes=1&truststorepath="
             << certFile;
    if (nodes_info.use_client_cert) {
      conn_str << "&certpath=" << client_cert_file
               << "&keypath=" << client_key_file
               << "&use_credentials_with_client_certificate=true";
    }
  } else
    conn_str << "couchbase://" << nodes_list << '/' << bucket_name
             << "?select_bucket=true&detailed_errcodes=1";
  if(isIpv6) {
    conn_str << "&ipv6=allow";
  }
  return conn_str.str();
}

bool CheckURLAccess(const std::string &path) {
  size_t last = 0;
  size_t next = 0;
  char delimiter = '/';
  int access = 0;
  while ((next = path.find(delimiter, last)) != std::string::npos) {
    if (path.compare(last, next - last, ".") == 0 || next - last == 0) {
      last = next + 1;
      continue;
    }
    path.compare(last, next - last, "..") == 0 ? access-- : access++;
    if (access < 0)
      return false;
    last = next + 1;
  }
  if (path.compare(last, next - last, ".") == 0 || next - last == 0)
    return access >= 0;
  path.compare(last, next - last, "..") == 0 ? access-- : access++;
  return access >= 0;
}

std::string BuildUrl(const std::string &host, const std::string &path) {
  if (!CheckURLAccess(path)) {
    LOG(logWarning) << path << " accesses beyond the binding. Redirecting to "
                    << host << std::endl;
    return host;
  }
  std::size_t host_length = host.length();
  std::size_t path_length = path.length();

  if (host_length > 1 && path_length > 1) {
    if ((host[host_length - 1] == '/' && host[host_length - 2] == '/') ||
        (path[0] == '/' && path[1] == '/')) {
      return host + path;
    } else if (host[host_length - 1] == '/' && path[0] == '/') {
      return host + path.substr(1, path_length - 1);
    } else if (host[host_length - 1] != '/' && path[0] != '/') {
      return host + '/' + path;
    }
  }
  return host + path;
}

CompilationInfo BuildCompileInfo(v8::Isolate *isolate,
                                 v8::Local<v8::Context> &context,
                                 v8::TryCatch *try_catch) {

  v8::HandleScope handle_scope(isolate);
  CompilationInfo info;

  std::string exception_string(JSONStringify(isolate, try_catch->Exception()));
  v8::Handle<v8::Message> message = try_catch->Message();

  info.compile_success = false;
  info.language = "JavaScript";

  if (message.IsEmpty()) {
    info.description = exception_string;
  } else {
    v8::String::Utf8Value filename(isolate, message->GetScriptResourceName());
    const char *filename_string = ToCString(filename);

    info.area = filename_string;
    info.line_no = message->GetLineNumber(context).FromMaybe(0);

    std::string out;
    const char *sourceline_string = "(unknown)";
    int start = message->GetStartColumn();
    int end = message->GetEndColumn();
    auto maybe_srcline = message->GetSourceLine(context);
    v8::Local<v8::String> local_srcline;

    if (TO_LOCAL(maybe_srcline, &local_srcline)) {
      v8::String::Utf8Value sourceline_utf8(isolate, local_srcline);
      sourceline_string = ToCString(sourceline_utf8);
      out.append(sourceline_string);
      out.append("\n");

      for (int i = 0; i < start; i++) {
        out.append(" ");
      }
      for (int i = start; i < end; i++) {
        out.append("^");
      }
      out.append("\n");
    }

    auto maybe_stack = try_catch->StackTrace(context);
    v8::Local<v8::Value> local_stack;
    if (!maybe_stack.IsEmpty()) {
      TO_LOCAL(maybe_stack, &local_stack);
      v8::String::Utf8Value stack_utf8(isolate, local_stack);
      auto stack_string = ToCString(stack_utf8);
      out.append(stack_string);
      out.append("\n");
    } else {
      out.append(exception_string);
      out.append("\n");
    }

    info.description = out;
    info.index = message->GetEndPosition();
    info.col_no = end;
  }
  return info;
}

std::string CompileInfoToString(CompilationInfo info) {
  nlohmann::json compileInfo;

  compileInfo["compile_success"] = info.compile_success;
  compileInfo["language"] = info.language;
  compileInfo["index"] = info.index;
  compileInfo["description"] = info.description;
  compileInfo["line_number"] = info.line_no;
  compileInfo["column_number"] = info.col_no;
  compileInfo["area"] = info.area;

  return compileInfo.dump();
}

int64_t GetUnixTime() {
  auto t = std::time(nullptr);
  auto secs = static_cast<std::chrono::seconds>(t).count();
  return static_cast<int64_t>(secs);
}

// Replaces param:count occurences of param:search with param:replace inplace
void ReplaceSubstringsInPlace(std::string &subject, const std::string &search,
                              const std::string &replace, int count = -1) {
  size_t pos = 0;
  while (((pos = subject.find(search, pos)) != std::string::npos) &&
         count != 0) {
    count--;
    subject.replace(pos, search.length(), replace);
  }
}

std::string to_hex(uint64_t val) {
  char buf[32];
  snprintf(buf, sizeof(buf), "0x%016" PRIx64, val);
  return std::string{buf};
}

void splitString(const std::string &str, std::vector<std::string> &result,
                 char delimiter) {
  std::stringstream ss(str);
  std::string token;

  while (std::getline(ss, token, delimiter)) {
    result.push_back(token);
  }
}
