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

#ifndef UTILS_H
#define UTILS_H

#include <algorithm>
#include <curl/curl.h>
#include <libcouchbase/api3.h>
#include <libcouchbase/couchbase.h>
#include <libplatform/libplatform.h>
#include <limits>
#include <sstream>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <v8.h>
#include <vector>

#include "comm.h"
#include "info.h"
#include "log.h"

#define EXCEPTION_STR_SIZE 20

#define TO(maybe, local)                                                       \
  (To((maybe), (local), __FILE__, __FUNCTION__, __LINE__))
#define TO_LOCAL(maybe, local)                                                 \
  (ToLocal((maybe), (local), __FILE__, __FUNCTION__, __LINE__))
#define IS_EMPTY(v8obj) (IsEmpty((v8obj), __FILE__, __FUNCTION__, __LINE__))

template <typename T>
T *UnwrapInternalField(v8::Local<v8::Object> obj, int field_no) {
  auto field = v8::Local<v8::External>::Cast(obj->GetInternalField(field_no));
  return static_cast<T *>(field->Value());
}

template <typename T>
bool ToLocal(const v8::MaybeLocal<T> &from, v8::Local<T> *to,
             const char *file = "", const char *caller = "", int line = -1) {
  if (from.ToLocal(to)) {
    return true;
  }

  LOG(logError) << "file : " << file << " line : " << line
                << " caller : " << caller << " : Returning empty value";
  return false;
}

template <typename T>
bool To(const v8::Maybe<T> &from, T *to, const char *file = "",
        const char *caller = "", int line = -1) {
  if (from.To(to)) {
    return true;
  }

  LOG(logError) << "file : " << file << " line : " << line
                << " caller : " << caller << " : Returning empty value";
  return false;
}

template <typename T>
bool IsEmpty(const T &obj, const char *file = "", const char *caller = "",
             int line = -1) {
  if (obj.IsEmpty()) {
    LOG(logError) << "file : " << file << " line : " << line
                  << " caller : " << caller << " : v8 object is empty";
    return true;
  }

  return false;
}

struct UrlEncode : public Info {
  UrlEncode() = default;
  UrlEncode(bool is_fatal, std::string msg) : Info(is_fatal, std::move(msg)) {}
  UrlEncode(std::string encoded) : encoded(std::move(encoded)) {}

  std::string encoded;
};

struct UrlDecode : public Info {
  UrlDecode() = default;
  UrlDecode(bool is_fatal, std::string msg) : Info(is_fatal, std::move(msg)) {}
  UrlDecode(std::string decoded) : decoded(std::move(decoded)) {}

  std::string decoded;
};

class Utils {
public:
  Utils(v8::Isolate *isolate, const v8::Local<v8::Context> &context);

  virtual ~Utils();

  v8::Local<v8::Value> GetPropertyFromGlobal(const std::string &method_name);
  v8::Local<v8::Value>
  GetPropertyFromObject(const v8::Local<v8::Value> &obj_v8val,
                        const std::string &method_name);
  std::string GetFunctionName(const v8::Local<v8::Value> &func_val);
  std::string ToCPPString(const v8::Local<v8::Value> &str_val);
  UrlEncode UrlEncodeAny(const v8::Local<v8::Value> &val);
  UrlEncode UrlEncodeAsString(const std::string &data);
  UrlEncode UrlEncodeAsKeyValue(const v8::Local<v8::Value> &obj);
  UrlDecode UrlDecodeString(const std::string &data);
  UrlDecode UrlDecodeAsKeyValue(const std::string &data,
                                v8::Local<v8::Object> &obj_out);
  UrlDecode
  UrlDecodeAsKeyValue(const std::string &data,
                      std::unordered_map<std::string, std::string> &kv);
  v8::Local<v8::ArrayBuffer> ToArrayBuffer(void *buffer, std::size_t size);
  bool IsFuncGlobal(const v8::Local<v8::Value> &func);
  std::string TrimBack(const std::string &s,
                       const char *ws = " \t\n\r\f\v") const;
  std::string TrimFront(const std::string &s,
                        const char *ws = " \t\n\r\f\v") const;
  std::string Trim(const std::string &s, const char *ws = " \t\n\r\f\v") const;

private:
  v8::Isolate *isolate_;
  CURL *curl_handle_; // Used only to perform url encode/decode
  v8::Persistent<v8::Context> context_;
  v8::Persistent<v8::Object> global_;
};

int WinSprintf(char **strp, const char *fmt, ...);

v8::Local<v8::String> v8Str(v8::Isolate *isolate, const char *str);
v8::Local<v8::String> v8Str(v8::Isolate *isolate, const std::string &str);
v8::Local<v8::Name> v8Name(v8::Isolate *isolate, uint32_t key);
v8::Local<v8::Array> v8Array(v8::Isolate *isolate,
                             const std::vector<std::string> &from);

std::string JSONStringify(v8::Isolate *isolate,
                          const v8::Local<v8::Value> &object);

const char *ToCString(const v8::String::Utf8Value &value);

std::string ConvertToISO8601(std::string timestamp);
std::string GetTranspilerSrc();
std::string ExceptionString(v8::Isolate *isolate, v8::TryCatch *try_catch);

std::vector<std::string> split(const std::string &s, char delimiter);

std::string Localhost(bool isUrl);
void SetIPv6(bool is6);
bool IsIPv6();
std::string JoinHostPort(const std::string &host, const std::string &port);
std::pair<std::string, std::string> GetLocalKey();
std::string GetTimestampNow();
ParseInfo UnflattenParseInfo(std::unordered_map<std::string, std::string> &kv);

std::string EventingVer();
bool IsRetriable(lcb_error_t error);
bool IsTerminatingRetriable(bool retry);
bool IsExecutionTerminating(v8::Isolate *isolate);
std::string base64Encode(const std::string &data);

void UrlEncodeFunction(const v8::FunctionCallbackInfo<v8::Value> &args);
void UrlDecodeFunction(const v8::FunctionCallbackInfo<v8::Value> &args);

void Crc64Function(const v8::FunctionCallbackInfo<v8::Value> &args);

std::string GetConnectionStr(const std::string &end_point,
                             const std::string &bucket_name);
#endif
