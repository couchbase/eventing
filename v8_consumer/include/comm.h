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

#ifndef COMM_H
#define COMM_H

#include <curl/curl.h>
#include <string>
#include <unordered_map>
#include <v8.h>
#include <vector>

struct CURLResponse {
  bool is_error;
  std::string response;
  std::unordered_map<std::string, std::string> headers;
};

struct CredsInfo {
  bool is_error;
  std::string error;
  std::string username;
  std::string password;
};

// Info about parsing N1QL query
struct ParseInfo {
  bool is_valid;
  std::string info;
};

struct NamedParamsInfo {
  ParseInfo p_info;
  std::vector<std::string> named_params;
};

class CURLClient {
public:
  CURLClient();
  ~CURLClient();

  CURLResponse HTTPPost(const std::vector<std::string> &headers,
                        const std::string &url, const std::string &body);

private:
  static size_t BodyCallback(void *buffer, size_t size, size_t nmemb,
                             void *cookie);
  static size_t HeaderCallback(char *buffer, size_t size, size_t nitems,
                               void *cookie);

  CURL *curl_handle;
  CURLcode code;
  struct curl_slist *headers;
};

// Channel to communicate to eventing-producer through CURL
class Communicator {
public:
  Communicator(const std::string &host_ip, const std::string &host_port,
               v8::Isolate *isolate);

  ParseInfo ParseQuery(const std::string &query);
  CredsInfo GetCreds(const std::string &endpoint);
  NamedParamsInfo GetNamedParams(const std::string &query);

private:
  ParseInfo ExtractParseInfo(v8::Local<v8::Object> &parse_info_v8val);

  std::string parse_query_url;
  std::string get_creds_url;
  std::string get_named_params_url;
  v8::Isolate *isolate;
};

#endif
