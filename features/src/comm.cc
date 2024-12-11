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

#include <nlohmann/json.hpp>
#include <sstream>

#include "comm.h"
#include "isolate_data.h"
#include "utils.h"

Communicator::Communicator(const std::string &host_ip,
                           const std::string &host_port, const std::string &usr,
                           const std::string &key, bool ssl,
                           const std::string &app_name,
                           const std::string &bucket, const std::string &scope,
                           v8::Isolate *isolate)
    : isolate_(isolate), curl_(false), app_name_(app_name), bucket_(bucket),
      scope_(scope) {
  std::string base_url = (ssl ? "https://" : "http://") +
                         JoinHostPort(Localhost(false), host_port);
  parse_query_url_ = base_url + "/parseQuery";
  get_creds_url_ = base_url + "/getCreds";
  get_named_params_url_ = base_url + "/getNamedParams";
  write_debugger_url_ = base_url + "/writeDebuggerURL";
  get_kv_nodes_url_ = base_url + "/getKVNodesAddresses";
  lo_usr_ = usr;
  lo_key_ = key;
}

CredsInfo Communicator::ExtractCredentials(const std::string &encoded_str) {
  auto utils = UnwrapData(isolate_)->utils;
  std::unordered_map<std::string, std::string> kv;
  CredsInfo info;
  info.is_valid = false;

  auto decode_info = utils->UrlDecodeAsKeyValue(encoded_str, kv);
  if (decode_info.is_fatal) {
    info.msg = decode_info.msg;
    return info;
  }

  info.is_valid = true;
  info.username = kv["username"];
  info.password = kv["password"];
  return info;
}

CredsInfo Communicator::GetCreds(const std::string &endpoint) {
  auto response = curl_.HTTPPost({"Content-Type: text/plain"}, get_creds_url_,
                                 endpoint, lo_usr_, lo_key_);

  CredsInfo info;
  info.is_valid = false;
  info.msg = response.msg;

  if (response.code != CURLE_OK) {
    LOG(logError) << "Unable to get creds: Something went wrong with cURL lib: "
                  << response.msg << std::endl;
    return info;
  }

  if (response.headers.data.find("Status") == response.headers.data.end()) {
    LOG(logError) << "Unable to get creds: status code is missing in header: "
                  << response.msg << std::endl;
    return info;
  }

  if (std::stoi(response.headers.data["Status"]) != 0) {
    LOG(logError) << "Unable to get creds: non 200 status in header: "
                  << response.msg << std::endl;
    return info;
  }

  return ExtractCredentials(response.body);
}

KVNodesInfo Communicator::GetKVNodes() {
  // TODO : Use GET here instead of POST
  auto response = curl_.HTTPPost({}, get_kv_nodes_url_, "", lo_usr_, lo_key_);

  std::stringstream error;
  KVNodesInfo info;
  info.is_valid = false;

  if (response.code != CURLE_OK) {
    error << "Unable to get KV nodes addresses, error : "
          << curl_easy_strerror(response.code);
    info.msg = error.str();
    return info;
  }

  auto resp_json = nlohmann::json::parse(response.body, nullptr, false);
  if (resp_json.is_discarded()) {
    error << "Failed to parse response as JSON : " << RS(response.body);
    info.msg = error.str();
    return info;
  }

  if (resp_json["is_error"].get<bool>()) {
    info.msg = resp_json["error"].get<std::string>();
    return info;
  }

  auto nodes = resp_json["kv_nodes"].get<std::vector<std::string>>();
  info.kv_nodes.reserve(nodes.size());
  for (const auto &node : nodes) {
    info.kv_nodes.emplace_back(node);
  }
  info.encrypt_data = resp_json["encrypt_data"].get<bool>();
  info.is_client_auth_mandatory = resp_json["is_client_auth_mandatory"].get<bool>();
  info.is_valid = true;
  return info;
}

void Communicator::Refresh() { creds_cache_.clear(); }

void Communicator::WriteDebuggerURL(const std::string &url) {
  auto utils = UnwrapData(isolate_)->utils;
  auto bucket_encoded = utils->UrlEncodeAsString(bucket_);
  auto scope_encoded = utils->UrlEncodeAsString(scope_);
  auto app_name_encoded = utils->UrlEncodeAsString(app_name_);
  auto response =
      curl_.HTTPPost({"Content-Type: text/plain"},
                     write_debugger_url_ + "?name=" + app_name_encoded.encoded +
                         "&bucket=" + bucket_encoded.encoded +
                         "&scope=" + scope_encoded.encoded,
                     url, lo_usr_, lo_key_);
  int status = std::stoi(response.headers.data["Status"]);
  if (status != 0) {
    LOG(logError) << "Unable to write debugger URL: non-zero status in header"
                  << status << std::endl;
  }
}

CredsInfo Communicator::GetCredsCached(const std::string &endpoint,
                                       bool useCachedEntry) {
  auto find = creds_cache_.find(endpoint);
  if (useCachedEntry && find != creds_cache_.end()) {
    return find->second;
  }

  auto credentials = GetCreds(endpoint);
  if (useCachedEntry && credentials.is_valid) {
    creds_cache_[endpoint] = credentials;
  }
  return credentials;
}

void Communicator::removeCachedEntry(const std::string &endpoint) {
  creds_cache_.erase(endpoint);
}
