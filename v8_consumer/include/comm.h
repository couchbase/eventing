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

#include <string>
#include <unordered_map>
#include <v8.h>
#include <vector>

#include "curl.h"

struct CredsInfo {
  bool is_valid;
  std::string msg;
  std::string username;
  std::string password;
  time_t time_fetched;
};

// Info about parsing N1QL query
struct ParseInfo {
  bool is_valid;
  bool is_select_query;
  bool is_dml_query;
  std::string keyspace_name;
  std::string info;
};

struct NamedParamsInfo {
  ParseInfo p_info;
  std::vector<std::string> named_params;
};

// Channel to communicate to eventing-producer through CURL
class Communicator {
public:
  Communicator(const std::string &host_ip, const std::string &host_port,
               const std::string &usr, const std::string &key, bool ssl,
               const std::string &app_name, v8::Isolate *isolate);

  CredsInfo GetCreds(const std::string &endpoint);
  CredsInfo GetCredsCached(const std::string &endpoint);
  NamedParamsInfo GetNamedParams(const std::string &query);
  ParseInfo ParseQuery(const std::string &query);
  void WriteDebuggerURL(const std::string &url);
  void Refresh();

private:
  CredsInfo ExtractCredentials(const std::string &encoded_str);
  NamedParamsInfo ExtractNamedParams(const std::string &encoded_str);
  ParseInfo ExtractParseInfo(const std::string &encoded_str);

  v8::Isolate *isolate_;
  std::unordered_map<std::string, CredsInfo> creds_cache_;
  CurlClient curl_;
  std::string app_name_;
  std::string get_creds_url_;
  std::string get_named_params_url_;
  std::string lo_key_;
  std::string lo_usr_;
  std::string parse_query_url_;
  std::string write_debugger_url_;
};

#endif
