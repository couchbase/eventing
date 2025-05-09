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
#include <libcouchbase/couchbase.h>
#include <libplatform/libplatform.h>
#include <limits>
#include <sstream>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <v8.h>
#include <vector>

#include "comm.h"
#include "info.h"
#include "isolate_data.h"
#include "log.h"

#define TO(maybe, local)                                                       \
  (To((maybe), (local), __FILE__, __FUNCTION__, __LINE__))
#define TO_LOCAL(maybe, local)                                                 \
  (ToLocal((maybe), (local), __FILE__, __FUNCTION__, __LINE__))
#define IS_EMPTY(v8obj) (IsEmpty((v8obj), __FILE__, __FUNCTION__, __LINE__))
#define CHECK_SUCCESS(maybe)                                                   \
  (CheckSuccess(maybe, __FILE__, __FUNCTION__, __LINE__))

// This should match the feature list in common.go
enum Features { Feature_Curl = 1 << 0 };

inline bool run_feature(const IsolateData *isolate_data, Features feature) {
  return (isolate_data->feature_matrix & feature) == 1;
}

struct CompilationInfo {
  CompilationInfo() : compile_success(false), index(0), line_no(0), col_no(0) {}

  std::string language;
  bool compile_success;
  int32_t index;
  int32_t line_no;
  int32_t col_no;
  std::string description;
  std::string area;
};

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
void CheckSuccess(const v8::Maybe<T> &from, const char *file = "",
                  const char *caller = "", int line = -1) {
  if (!from.FromJust()) {
    LOG(logError) << "file : " << file << "line : " << line
                  << "caller : " << caller << " : Error with SET/GET";
  }
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
  UrlEncode(bool is_fatal) : Info(is_fatal) {}

  std::string encoded;
};

struct UrlDecode : public Info {
  UrlDecode() = default;
  UrlDecode(bool is_fatal, std::string msg) : Info(is_fatal, std::move(msg)) {}
  UrlDecode(std::string decoded) : decoded(std::move(decoded)) {}

  std::string decoded;
};

struct ConnStrInfo {
  bool is_valid{true};
  std::string msg;
  std::string conn_str;
  std::string client_key_passphrase;
};

struct V8ExceptionInfo {

  std::string exception{""};
  uint32_t line{0};
  std::string file{""};
  std::string srcLine{""};
  std::string stack{""};
};

class Utils {
public:
  Utils(v8::Isolate *isolate, const v8::Local<v8::Context> &context,
        bool isIpv6, std::string certFile, std::string clientCertFile,
        std::string clientKeyFile);

  virtual ~Utils();

  v8::Local<v8::Value> GetPropertyFromGlobal(const std::string &method_name);
  v8::Local<v8::Value>
  GetPropertyFromObject(const v8::Local<v8::Value> &obj_v8val,
                        const std::string &method_name);
  std::string GetFunctionName(const v8::Local<v8::Value> &func_val);
  std::string ToCPPString(const v8::Local<v8::Value> &str_val);
  UrlEncode UrlEncodeAsString(const std::string &data);
  UrlEncode UrlEncodeAsKeyValue(const v8::Local<v8::Value> &obj_val,
                                bool encode_kv_pairs);
  UrlDecode UrlDecodeString(const std::string &data);
  UrlDecode UrlDecodeAsKeyValue(const std::string &data,
                                v8::Local<v8::Object> &obj_out);
  UrlDecode
  UrlDecodeAsKeyValue(const std::string &data,
                      std::unordered_map<std::string, std::string> &kv);
  v8::Local<v8::ArrayBuffer> ToArrayBuffer(const void *buffer,
                                           std::size_t size);
  bool IsFuncGlobal(const v8::Local<v8::Value> &func);
  std::string TrimBack(const std::string &s,
                       const char *ws = " \t\n\r\f\v") const;
  std::string TrimFront(const std::string &s,
                        const char *ws = " \t\n\r\f\v") const;
  std::string Trim(const std::string &s, const char *ws = " \t\n\r\f\v") const;
  static Info ValidateDataType(const v8::Local<v8::Value> &arg);
  bool ValidateXattrKeyLength(const std::string &key);
  ConnStrInfo GetConnectionString(const std::string &bucket) const;
  std::string certFile_;
  std::string client_cert_file_;
  std::string client_key_file_;
  UrlEncode UrlEncodePath(const std::string &path,
                          const std::string &curl_lang_compat,
                          const std::string &app_lang_comp);
  UrlEncode UrlEncodeParams(const v8::Local<v8::Value> &val,
                            const std::string &curl_lang_compat,
                            const std::string &app_lang_comp);

private:
  UrlEncode UrlEncodeObjectParams(const v8::Local<v8::Value> &obj,
                                  const std::string &curl_lang_compat,
                                  const std::string &app_lang_comp);
  UrlEncode UrlEncodeStringParams(const std::string &qry_params,
                                  const std::string &curl_lang_compat,
                                  const std::string &app_lang_comp);
  UrlEncode UrlEncodePath7_1_0(const std::string &raw_path);
  UrlEncode EncodeAndNormalizePath(const std::string &path);
  UrlEncode EncodeAndNormalizeQueryParams(const std::string &query_params);
  std::string NormalizeSingleQueryParam(std::string &query_param);
  std::pair<std::string, std::string>
  ExtractPathAndQueryParamsFromURL(const std::string &encoded_url);

  v8::Isolate *isolate_;
  bool isIpv6_;
  CURL *curl_handle_; // Used only to perform url encode/decode
  v8::Persistent<v8::Context> context_;
  v8::Persistent<v8::Object> global_;
  static std::unordered_map<std::string, std::string> enc_to_dec_map;
  static const char *QUESTION_MARK;
  static const char *EQ;
  static const char *AMP;
  static const char *SLASH;
  static const std::string HEX_QUESTION_MARK;
  static const std::string HEX_EQ;
  static const std::string HEX_AMP;
  static const std::string HEX_SLASH;
};

template <typename T> class AtomicWrapper {
public:
  AtomicWrapper() : data(T()) {}

  explicit AtomicWrapper(T const &val) : data(val) {}

  explicit AtomicWrapper(std::atomic<T> const &val) : data(val.load()) {}

  AtomicWrapper(AtomicWrapper const &other) : data(other.data.load()) {}

  AtomicWrapper &operator=(AtomicWrapper const &other) {
    data.store(other.data.load());
    return *this;
  }

  inline void Set(T val) { data.store(val); }

  inline T Get() { return data.load(); }

private:
  std::atomic<T> data;
};

using AtomicInt64 = AtomicWrapper<int64_t>;
using AtomicUint64 = AtomicWrapper<uint64_t>;
using AtomicBool = AtomicWrapper<bool>;

int WinSprintf(char **strp, const char *fmt, ...);

v8::Local<v8::String> v8Str(v8::Isolate *isolate, const char *str);
v8::Local<v8::String> v8Str(v8::Isolate *isolate, const std::string &str);
v8::Local<v8::Name> v8Name(v8::Isolate *isolate, uint32_t key);
v8::Local<v8::Array> v8Array(v8::Isolate *isolate,
                             const std::vector<std::string> &from);
v8::Local<v8::Array> v8Array(v8::Isolate *isolate,
                             const std::vector<double> &from);

std::string JSONStringify(v8::Isolate *isolate,
                          const v8::Local<v8::Value> &object,
                          bool pretty = false);

const char *ToCString(const v8::String::Utf8Value &value);

std::string ConvertToISO8601(std::string timestamp);
std::string GetTranspilerSrc();
std::string ExceptionString(v8::Isolate *isolate,
                            v8::Local<v8::Context> &context,
                            v8::TryCatch *try_catch,
                            bool script_timeout = false);
V8ExceptionInfo GetV8ExceptionInfo(v8::Isolate *isolate,
                                   v8::Local<v8::Context> &context,
                                   v8::TryCatch *try_catch,
                                   bool script_timeout = false);

CompilationInfo BuildCompileInfo(v8::Isolate *isolate,
                                 v8::Local<v8::Context> &context,
                                 v8::TryCatch *try_catch);
std::string CompileInfoToString(CompilationInfo info);

std::vector<std::string> split(const std::string &s, char delimiter);

std::string JoinHostPort(const std::string &host, const std::string &port);
std::pair<std::string, std::string> GetLocalKey();
std::string GetTimestampNow();

std::string EventingVer();
bool IsTerminatingRetriable(bool retry);
bool IsExecutionTerminating(v8::Isolate *isolate);

void UrlEncodeFunction(const v8::FunctionCallbackInfo<v8::Value> &args);
void UrlDecodeFunction(const v8::FunctionCallbackInfo<v8::Value> &args);

void Crc64Function(const v8::FunctionCallbackInfo<v8::Value> &args);
void Crc64GoIsoFunction(const v8::FunctionCallbackInfo<v8::Value> &args);

enum class arrayEncoding { Float64Encoding, Float32Encoding };

void Base64EncodeFunction(const v8::FunctionCallbackInfo<v8::Value> &args);
void Base64DecodeFunction(const v8::FunctionCallbackInfo<v8::Value> &args);
void Base64Float64EncodeFunction(
    const v8::FunctionCallbackInfo<v8::Value> &args);
void Base64Float64DecodeFunction(
    const v8::FunctionCallbackInfo<v8::Value> &args);
void Base64Float32EncodeFunction(
    const v8::FunctionCallbackInfo<v8::Value> &args);
void Base64Float32DecodeFunction(
    const v8::FunctionCallbackInfo<v8::Value> &args);

std::string GetConnectionStr(bool isIpv6, const KVNodesInfo &node_info,
                             const std::string &bucket_name,
                             const std::string certFile,
                             const std::string client_cert_file,
                             const std::string client_key_file);

std::string BuildUrl(const std::string &host, const std::string &path);

int64_t GetUnixTime();

// If this function is used for LCB_XXX_timeout which takes in a uint32_t
// the return value loses precision and is capped at 4294 seconds
inline long ConvertSecondsToMicroSeconds(long time) { return time * 1e6; }
inline long ConvertMicroSecondsToSeconds(long time) { return time / 1e6; }

void ReplaceSubstringsInPlace(std::string &subject, const std::string &search,
                              const std::string &replace, int count);

static inline uint64_t byte_swap_64(uint64_t x) {
  return (((x & UINT64_C(0xFF)) << 56) | ((x & UINT64_C(0xFF00)) << 40) |
          ((x & UINT64_C(0xFF0000)) << 24) | ((x & UINT64_C(0xFF000000)) << 8) |
          ((x & UINT64_C(0xFF00000000)) >> 8) |
          ((x & UINT64_C(0xFF0000000000)) >> 24) |
          ((x & UINT64_C(0xFF000000000000)) >> 40) |
          ((x & UINT64_C(0xFF00000000000000)) >> 56));
}

std::string to_hex(uint64_t);
void splitString(const std::string &, std::vector<std::string> &, char);
#endif
