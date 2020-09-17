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

#ifndef CURL_H
#define CURL_H

#include <algorithm>
#include <atomic>
#include <cctype>
#include <curl/curl.h>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <v8.h>
#include <vector>

#include "info.h"

struct CurlParameters;
struct CurlRequest;
struct CurlResponse;
struct CurlBinding;
struct CurlBindingInfo;
struct HTTPPostResponse;

class CurlStats {
public:
  CurlStats();
  void UpdateCounters(const std::string &request_type);
  inline void UpdateNon200Counter() { non_200_resp_counter_++; }

  inline std::int64_t GetCurlGetStat() const {
    return curl_get_counter_.load();
  }

  inline std::int64_t GetCurlPostStat() const {
    return curl_post_counter_.load();
  }

  inline std::int64_t GetCurlDeleteStat() const {
    return curl_delete_counter_.load();
  }

  inline std::int64_t GetCurlHeadStat() const {
    return curl_head_counter_.load();
  }

  inline std::int64_t GetCurlPutStat() const {
    return curl_put_counter_.load();
  }

  inline std::int64_t GetCurlFailureStat() const {
    return non_200_resp_counter_.load();
  }

private:
  std::atomic<std::int64_t> curl_get_counter_;
  std::atomic<std::int64_t> curl_post_counter_;
  std::atomic<std::int64_t> curl_delete_counter_;
  std::atomic<std::int64_t> curl_head_counter_;
  std::atomic<std::int64_t> curl_put_counter_;
  std::atomic<std::int64_t> non_200_resp_counter_;
};

class CurlClient {
public:
  static std::stringstream curl_debug_log_;
  explicit CurlClient(bool enable_cookies);
  ~CurlClient();

  // Utility method used by Communicator
  HTTPPostResponse HTTPPost(const std::vector<std::string> &header_list,
                            const std::string &url, const std::string &body,
                            const std::string &usr, const std::string &key);
  CurlResponse Execute(const std::vector<CurlParameters> &params_list);
  void ResetOptions() { curl_easy_reset(curl_handle_); }

  Info AddTransferInfo(v8::Isolate *isolate,
                       const v8::Local<v8::Context> &context,
                       v8::Local<v8::Object> &response_obj, CurlStats &stats);

  static std::size_t BodyWriteCallback(void *contents_recv, std::size_t size,
                                       std::size_t nmemb, void *cookie);
  static std::size_t BodyReadCallback(void *upload_buffer, std::size_t size,
                                      std::size_t nmemb, void *cookie);
  static std::size_t HeaderCallback(void *buffer, std::size_t size,
                                    std::size_t nitems, void *cookie);
  static int DebuggerCallback(CURL *curl_handle, curl_infotype type, char *data,
                              std::size_t size, void *user_ptr);
  static void DumpLog(std::string type, unsigned char *ptr, std::size_t size);

private:
  CURL *curl_handle_;
  std::mutex curl_handle_lck_;
  std::string user_agent_;
};

class Curl {
public:
  using ParamsList = std::vector<CurlParameters>;
  using Buffer = std::unique_ptr<std::vector<uint8_t>>;
  using ReadBuffer = std::pair<const Buffer *, std::size_t>;
  struct Headers {
    std::unordered_map<std::string, std::string> data;
    std::string content_type;

    void AddHeader(std::string key, std::string value);
  };

  Curl(v8::Isolate *isolate, const v8::Local<v8::Context> &context,
       bool enable_cookies);
  ~Curl();

  Info CurlImpl(const v8::FunctionCallbackInfo<v8::Value> &args);
  static Info ValidateParams(const v8::FunctionCallbackInfo<v8::Value> &args);
  inline static const CurlStats &GetStats() { return stats_; };

private:
  std::string ConstructUrl(const CurlRequest &request) const;
  CurlResponse DoRequest(const CurlBinding &binding,
                         const CurlRequest &request);
  void SetRequestMethod(const CurlRequest &request,
                        ParamsList &params_list) const;
  void SetAuthentication(const CurlBinding &binding,
                         ParamsList &params_list) const;
  Info SetRequestBody(const CurlRequest &request, ParamsList &params_list,
                      Curl::ReadBuffer &buffer) const;

  CurlClient curl_client_;
  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;
  std::string user_agent_;
  static CurlStats stats_;
};

struct CurlCodex {
  using Headers = Curl::Headers;
  using ContentTypes = std::unordered_set<std::string>;
  using Methods = std::unordered_set<std::string>;

  bool IsSupportedJson(const std::string &content_type) const {
    return supported_json_.find(content_type) != supported_json_.end();
  }
  bool IsSupportedBinary(const std::string &content_type) const {
    return supported_binary_.find(content_type) != supported_binary_.end();
  }
  bool IsSupportedForm(const std::string &content_type) const {
    return supported_form_.find(content_type) != supported_form_.end();
  }
  bool IsSupportedText(const std::string &content_type) const {
    return content_type.compare(0, supported_text_.length(), supported_text_) ==
           0;
  }
  bool IsSupportedMethod(const std::string &method) const {
    return supported_methods_.find(method) != supported_methods_.end();
  }

  const std::string default_json_{"application/json"};
  const std::string default_text_{"text/plain"};
  const std::string default_binary_{"application/octet-stream"};
  const std::string default_form_{"application/x-www-form-urlencoded"};

private:
  const Methods supported_methods_{"GET", "POST", "HEAD", "DELETE", "PUT"};
  const ContentTypes supported_json_{"application/json"};
  const ContentTypes supported_binary_{"application/octet-stream"};
  const ContentTypes supported_form_{"application/x-www-form-urlencoded"};
  const std::string supported_text_{"text/"};
};

struct CurlInfo : public Info {
  CurlInfo(bool is_fatal) : Info(is_fatal), curl(nullptr) {}
  CurlInfo(bool is_fatal, std::string msg)
      : Info(is_fatal, std::move(msg)), curl(nullptr) {}
  CurlInfo(bool is_fatal, Curl *curl) : Info(is_fatal), curl(curl) {}

  Curl *curl;
};

// Forward declaring the flatbuf struct to avoid #including the generated
// flatbuf headers
namespace flatbuf {
namespace cfg {
struct Curl;
}
} // namespace flatbuf

struct CurlBinding {
  CurlBinding() = default;
  explicit CurlBinding(const flatbuf::cfg::Curl *curl) noexcept;
  static CurlBindingInfo FromObject(v8::Isolate *isolate,
                                    const v8::Local<v8::Context> &context,
                                    const v8::Local<v8::Object> &obj);

  void InstallBinding(v8::Isolate *isolate,
                      const v8::Local<v8::Context> &context) const;
  static bool IsGenuine(v8::Isolate *isolate, const v8::Local<v8::Object> &obj);
  static int GetInternalFieldsCount() { return InternalFields::Count; }
  static CurlInfo GetCurlInstance(v8::Isolate *isolate,
                                  const v8::Local<v8::Context> &context,
                                  const v8::Local<v8::Value> &val);

  std::string auth_type;
  std::string hostname;
  std::string value;
  std::string username;
  std::string password;
  std::string bearer_key;
  bool allow_cookies{false};
  bool validate_ssl_certificate{false};
  Curl *curl_instance{nullptr};

private:
  enum InternalFields {
    kAuthType,
    kHostname,
    kUsername,
    kPassword,
    kBindingId,
    kBearerKey,
    kAllowCookies,
    kValidateSSLCertificate,
    kCurlInstance,
    Count // This is not a field, its value represents the count of this enum
    // Ensure that "Count" is always the last value in this enum
  };
};

struct CurlBindingInfo : public Info {
  CurlBindingInfo(bool is_fatal) : Info(is_fatal) {}
  CurlBindingInfo(bool is_fatal, std::string msg)
      : Info(is_fatal, std::move(msg)) {}
  CurlBindingInfo(bool is_fatal, CurlBinding &binding) : Info(is_fatal) {
    std::swap(this->binding, binding);
  }

  CurlBinding binding;
};

struct CurlRequest : public Info {
  CurlRequest() = default;
  CurlRequest(bool is_fatal) : Info(is_fatal) {}
  CurlRequest(bool is_fatal, std::string msg)
      : Info(is_fatal, std::move(msg)) {}

  bool redirect{true};
  std::string method;
  std::string host;
  std::string path;
  std::string params_urlencoded;
  Curl::Headers headers;
  Curl::Buffer body;
};

struct CurlResponse {
  CurlResponse(CURLcode code) : code(code) {}
  CurlResponse(CURLcode code, std::string msg)
      : code(code), msg(std::move(msg)) {}
  CurlResponse(CURLcode code, Curl::Buffer body, Curl::Headers &headers)
      : code(code), body(std::move(body)) {
    (this->headers).data.swap(headers.data);
    (this->headers).content_type.swap(headers.content_type);
  }

  CURLcode code;
  std::string msg;
  Curl::Buffer body;
  Curl::Headers headers;
};

struct HTTPPostResponse {
  HTTPPostResponse(CURLcode code) : code(code) {}
  HTTPPostResponse(CURLcode code, std::string msg)
      : code(code), msg(std::move(msg)) {}
  HTTPPostResponse(CURLcode code, std::string body, Curl::Headers &headers)
      : code(code), body(std::move(body)) {
    (this->headers).data.swap(headers.data);
    (this->headers).content_type.swap(headers.content_type);
  }

  CURLcode code;
  std::string msg;
  std::string body;
  Curl::Headers headers;
};

struct CurlData {
  using CurlCallback = std::size_t(void *buffer, std::size_t size,
                                   std::size_t nmemb, void *cookie);

  enum Type {
    kLong,
    kConstCharPtr,
    kVoidPtr,
    kCurlCallbackPtr,
    kStructCurlSListPtr
  } type;

  union Data {
    Data() {}
    Data(long long_val) : long_val(long_val) {}
    Data(const char *const_char_ptr) : const_char_ptr(const_char_ptr) {}
    Data(void *void_ptr) : void_ptr(void_ptr) {}
    Data(CurlCallback *curl_callback_ptr)
        : curl_callback_ptr(curl_callback_ptr) {}
    Data(curl_slist *struct_curl_slist)
        : struct_curl_slist_ptr(struct_curl_slist) {}

    long long_val;
    const char *const_char_ptr;
    void *void_ptr;
    CurlCallback *curl_callback_ptr;
    curl_slist *struct_curl_slist_ptr;
  } data;

  CurlData() {}
  CurlData(Type type, Data data) : type(type), data(data) {}
};

struct CurlParameters {
  CurlParameters(CURLoption option, CurlData data, std::string err_msg)
      : option(option), data(data), err_msg(std::move(err_msg)) {}

  CURLoption option;
  CurlData data;
  std::string err_msg;
};

class CurlHeaders {
public:
  explicit CurlHeaders(const std::vector<std::string> &headers);
  explicit CurlHeaders(const Curl::Headers &headers);
  ~CurlHeaders();

  curl_slist *GetHeaders() const { return headers_; }

private:
  curl_slist *headers_;
};

enum class BodyEncoding {
  Absent, // Not to be treated as an encoding
  kJSON,
  kText,
  kForm,
  kBinary
};

class BodyExtractor {
public:
  BodyExtractor(v8::Isolate *isolate, const BodyEncoding &encoding,
                const v8::Local<v8::Value> &body_val);
  ~BodyExtractor();

  Info FromArrayBuffer(Curl::Buffer &body_out);
  Info FromObject(Curl::Buffer &body_out);
  Info FromText(Curl::Buffer &body_out);

private:
  BodyEncoding encoding_;
  v8::Isolate *isolate_;
  v8::Persistent<v8::Value> body_val_;
};

class CurlRequestBuilder {
  using Headers = Curl::Headers;
  using ContentTypes = std::unordered_set<std::string>;
  using Methods = std::unordered_set<std::string>;

public:
  CurlRequestBuilder(v8::Isolate *isolate,
                     const v8::Local<v8::Context> &context);
  ~CurlRequestBuilder();

  CurlRequest NewRequest(const CurlBinding &binding,
                         const v8::FunctionCallbackInfo<v8::Value> &args);

private:
  Info ExtractEncoding(const v8::Local<v8::Value> &encoding_val,
                       BodyEncoding &encoding_out);
  Info ExtractMethod(const v8::Local<v8::Value> &method_val,
                     std::string &value_out);
  Info ExtractBody(const v8::Local<v8::Value> &body_val,
                   const BodyEncoding &encoding, Curl::Buffer &body_out);
  Info ExtractPath(const v8::Local<v8::Value> &path_val,
                   const CurlBinding &binding, std::string &value_out);
  Info ExtractParams(const v8::Local<v8::Value> &params_val,
                     std::string &value_out);
  Info ExtractHeaders(const v8::Local<v8::Value> &headers_val,
                      Headers &value_out);
  Info ExtractRedirect(const v8::Local<v8::Value> &redirect_val,
                       bool &value_out);
  Info FillContentType(const v8::Local<v8::Value> &body_val,
                       CurlRequest &request, const BodyEncoding &encoding);

  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;
};

// TODO : If and when we add green threads, we may need to have one CurlFactory
// per green thread
class CurlFactory {
public:
  CurlFactory(v8::Isolate *isolate, const v8::Local<v8::Context> &context);
  ~CurlFactory();

  v8::Local<v8::Object> NewCurlObj();

private:
  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;
  v8::Persistent<v8::ObjectTemplate> curl_template_;
};

class CurlResponseBuilder {
public:
  CurlResponseBuilder(v8::Isolate *isolate,
                      const v8::Local<v8::Context> &context);
  ~CurlResponseBuilder();

  Info NewResponse(CurlClient &curl_client, const CurlResponse &response,
                   v8::Local<v8::Object> &resp_obj_out, CurlStats &stats);

private:
  std::string ExtractContentType(const std::string &header);
  Info SetHeaders(const CurlResponse &response,
                  v8::Local<v8::Object> &response_obj);
  Info SetBody(const CurlResponse &response,
               v8::Local<v8::Object> &response_obj);
  Info SetBodyAsBinary(const CurlResponse &response,
                       v8::Local<v8::Object> &response_obj);
  Info SetBodyAsJSON(const CurlResponse &response,
                     v8::Local<v8::Object> &response_obj);
  Info SetBodyAsForm(const CurlResponse &response,
                     v8::Local<v8::Object> &response_obj);
  Info SetBodyAsText(const CurlResponse &response,
                     v8::Local<v8::Object> &response_obj);
  Info SetBodyAsNull(v8::Local<v8::Object> &response_obj);

  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;
};

void CurlFunction(const v8::FunctionCallbackInfo<v8::Value> &args);
void UpdateCurlLatencyHistogram(
    v8::Isolate *isolate,
    const std::chrono::high_resolution_clock::time_point &start);

#endif
