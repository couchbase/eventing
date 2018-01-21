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

#ifndef COUCHBASE_COMM_H
#define COUCHBASE_COMM_H

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

class CURLClient {
private:
  CURL *curl_handle;
  CURLcode code;
  struct curl_slist *headers;

  static size_t BodyCallback(void *buffer, size_t size, size_t nmemb,
                             void *cookie);
  static size_t HeaderCallback(char *buffer, size_t size, size_t nitems,
                               void *cookie);

public:
  CURLClient();
  ~CURLClient();

  CURLResponse HTTPPost(const std::vector<std::string> &headers,
                        const std::string &url, const std::string &body);
};

class Communicator {
private:
  std::string get_creds_url;
  v8::Isolate *isolate;

public:
  Communicator(const std::string &host_port, v8::Isolate *isolate);

  CredsInfo GetCreds(const std::string &endpoint);
};

#endif
