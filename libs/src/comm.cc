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

#include "comm.h"
#include "isolate_data.h"
#include "utils.h"

Communicator::Communicator(const std::string &host_ip,
                           const std::string &host_port, const std::string &usr,
                           const std::string &key, bool ssl,
                           const std::string &app_name, v8::Isolate *isolate)
    : isolate_(isolate), curl_(false), app_name_(app_name) {
  std::string base_url = (ssl ? "https://" : "http://") +
                         JoinHostPort(Localhost(false), host_port);
  parse_query_url_ = base_url + "/parseQuery";
  get_creds_url_ = base_url + "/getCreds";
  get_named_params_url_ = base_url + "/getNamedParams";
  write_debugger_url_ = base_url + "/writeDebuggerURL";
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

NamedParamsInfo
Communicator::ExtractNamedParams(const std::string &encoded_str) {
  auto utils = UnwrapData(isolate_)->utils;
  std::unordered_map<std::string, std::string> kv;
  NamedParamsInfo info;

  auto decode_info = utils->UrlDecodeAsKeyValue(encoded_str, kv);
  if (decode_info.is_fatal) {
    info.p_info.is_valid = false;
    info.p_info.info = decode_info.msg;
    return info;
  }

  info.p_info = UnflattenParseInfo(kv);
  for (auto i = 0, len = std::stoi(kv["named_params_size"]); i < len; ++i) {
    info.named_params.emplace_back(kv[std::to_string(i)]);
  }

  return info;
}

ParseInfo Communicator::ExtractParseInfo(const std::string &encoded_str) {
  auto utils = UnwrapData(isolate_)->utils;
  std::unordered_map<std::string, std::string> kv;

  auto kv_info = utils->UrlDecodeAsKeyValue(encoded_str, kv);
  if (kv_info.is_fatal) {
    ParseInfo info;
    info.is_valid = false;
    info.info = kv_info.msg;
    return info;
  }

  return UnflattenParseInfo(kv);
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

  if (response.headers.find("Status") == response.headers.end()) {
    LOG(logError) << "Unable to get creds: status code is missing in header: "
                  << response.msg << std::endl;
    return info;
  }

  if (std::stoi(response.headers["Status"]) != 0) {
    LOG(logError) << "Unable to get creds: non-zero status in header: "
                  << response.msg << std::endl;
    return info;
  }

  return ExtractCredentials(response.body);
}

CredsInfo Communicator::GetCredsCached(const std::string &endpoint) {
  auto now = time(NULL);
  auto find = creds_cache_.find(endpoint);
  if ((find != creds_cache_.end()) && (find->second.time_fetched >= now - 2)) {
    return find->second;
  }

  LOG(logInfo) << "Getting credentials afresh for " << RS(endpoint)
               << std::endl;

  auto credentials = GetCreds(endpoint);
  credentials.time_fetched = now;
  creds_cache_[endpoint] = credentials;
  return credentials;
}

NamedParamsInfo Communicator::GetNamedParams(const std::string &query) {
  auto response =
      curl_.HTTPPost({"Content-Type: text/plain"}, get_named_params_url_, query,
                     lo_usr_, lo_key_);

  NamedParamsInfo info;
  info.p_info.is_valid = false;
  info.p_info.info = "Something went wrong while extracting named parameters";

  if (response.code != CURLE_OK) {
    LOG(logError)
        << "Unable to get named params: Something went wrong with cURL lib: "
        << RM(response.msg) << std::endl;
    return info;
  }

  if (response.headers.find("Status") == response.headers.end()) {
    LOG(logError)
        << "Unable to get named params: status code is missing in header: "
        << RM(response.msg) << std::endl;
    return info;
  }

  if (std::stoi(response.headers["Status"]) != 0) {
    LOG(logError) << "Unable to get named params: non-zero status in header: "
                  << RM(response.msg) << std::endl;
    return info;
  }

  return ExtractNamedParams(response.body);
}

ParseInfo Communicator::ParseQuery(const std::string &query) {
  auto response = curl_.HTTPPost({"Content-Type: text/plain"}, parse_query_url_,
                                 query, lo_usr_, lo_key_);

  ParseInfo info;
  info.is_valid = false;
  info.info = "Something went wrong while parsing the N1QL query";

  if (response.code != CURLE_OK) {
    LOG(logError)
        << "Unable to parse N1QL query: Something went wrong with cURL lib: "
        << RU(response.msg) << std::endl;
    return info;
  }

  if (response.headers.find("Status") == response.headers.end()) {
    LOG(logError)
        << "Unable to parse N1QL query: status code is missing in header:"
        << RU(response.msg) << std::endl;
    return info;
  }

  int status = std::stoi(response.headers["Status"]);
  if (status != 0) {
    LOG(logError) << "Unable to parse N1QL query: non-zero status in header"
                  << status << std::endl;
    return info;
  }

  return ExtractParseInfo(response.body);
}

void Communicator::Refresh() { creds_cache_.clear(); }

void Communicator::WriteDebuggerURL(const std::string &url) {
  auto response = curl_.HTTPPost({"Content-Type: text/plain"},
                                 write_debugger_url_ + "/" + app_name_, url,
                                 lo_usr_, lo_key_);
  int status = std::stoi(response.headers["Status"]);
  if (status != 0) {
    LOG(logError) << "Unable to write debugger URL: non-zero status in header"
                  << status << std::endl;
  }
}
