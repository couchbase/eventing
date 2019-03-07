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
#include "log.h"
#include "timer.h"

#define DATA_SLOT 0
#define EXCEPTION_STR_SIZE 20

#define TO(maybe, local)                                                       \
  (To((maybe), (local), __FILE__, __FUNCTION__, __LINE__))
#define TO_LOCAL(maybe, local)                                                 \
  (ToLocal((maybe), (local), __FILE__, __FUNCTION__, __LINE__))
#define IS_EMPTY(v8obj) (IsEmpty((v8obj), __FILE__, __FUNCTION__, __LINE__))

class N1QL;
struct N1QLCodex;
class V8Worker;
class JsException;
class Transpiler;
class Utils;
class Timer;
class CustomError;

// Struct for storing isolate data
struct Data {
  N1QLCodex *n1ql_codex;
  CURL *curl_handle;
  N1QL *n1ql_handle;
  V8Worker *v8worker;
  JsException *js_exception;
  Communicator *comm;
  Transpiler *transpiler;
  Utils *utils;
  Timer *timer;
  CustomError *custom_error;
};

// Code version of handler
struct CodeVersion {
  std::string version;
  std::string level;
  std::string using_timer;
};

inline Data *UnwrapData(v8::Isolate *isolate) {
  return reinterpret_cast<Data *>(isolate->GetData(DATA_SLOT));
}

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
  bool IsFuncGlobal(const v8::Local<v8::Value> &func);

private:
  v8::Isolate *isolate_;
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

#endif
