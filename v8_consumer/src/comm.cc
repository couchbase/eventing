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
#include "utils.h"

CURLHeaders::CURLHeaders(const std::vector<std::string> &headers) {
  headers_ = nullptr;
  for (const auto &header : headers) {
    headers_ = curl_slist_append(headers_, header.c_str());
  }
}

CURLHeaders::~CURLHeaders() { curl_slist_free_all(headers_); }

CURLClient::CURLClient() { curl_handle_ = curl_easy_init(); }

CURLClient::~CURLClient() { curl_easy_cleanup(curl_handle_); }

// Callback gets invoked for every chunk of body data that arrives
size_t CURLClient::BodyCallback(void *buffer, size_t size, size_t nmemb,
                                void *cookie) {
  auto realsize = size * nmemb;
  auto data = static_cast<std::string *>(cookie);
  auto content = static_cast<char *>(buffer);
  data->assign(&content[0], &content[0] + realsize);
  return realsize;
}

std::string CURLClient::Decode(const std::string &encoded_str) {
  int n_decode;
  auto decoded_str_ptr =
      curl_easy_unescape(curl_handle_, encoded_str.c_str(),
                         static_cast<int>(encoded_str.length()), &n_decode);
  std::string decoded_str(decoded_str_ptr, decoded_str_ptr + n_decode);
  curl_free(decoded_str_ptr);
  return decoded_str;
}

// Callback gets invoked for every header that arrives
size_t CURLClient::HeaderCallback(char *buffer, size_t size, size_t nitems,
                                  void *cookie) {
  auto realsize = size * nitems;
  auto headers =
      static_cast<std::unordered_map<std::string, std::string> *>(cookie);
  auto header = std::string(static_cast<char *>(buffer));

  // Split the header into key:value
  auto find = header.find(':');
  if (find != std::string::npos) {
    (*headers)[header.substr(0, find)] =
        header.substr(find + 1); // Adding 1 to discount the ':'
  }

  return realsize;
}

ExtractKVInfo CURLClient::ExtractKV(const std::string &encoded_str) {
  ExtractKVInfo info;
  info.is_valid = false;

  std::istringstream tokenizer(encoded_str);
  std::string item;
  while (std::getline(tokenizer, item, '&')) {
    auto i = item.find('=');
    if (i == std::string::npos) {
      info.msg = "Encoded string is not delimited by =";
      return info;
    }

    auto key = Decode(item.substr(0, i));
    auto value = item.substr(i + 1);
    std::replace(value.begin(), value.end(), '+', ' ');
    info.kv[key] = Decode(value);
  }

  info.is_valid = true;
  return info;
}

CURLResponse CURLClient::HTTPPost(const std::vector<std::string> &header_list,
                                  const std::string &url,
                                  const std::string &body,
                                  const std::string &usr,
                                  const std::string &key) {
  std::lock_guard<std::mutex> lock(curl_handle_lck_);
  CURLResponse response;

  code_ = curl_easy_setopt(curl_handle_, CURLOPT_URL, url.c_str());
  if (code_ != CURLE_OK) {
    response.is_error = true;
    response.response =
        "Unable to set URL: " + std::string(curl_easy_strerror(code_));
    return response;
  }

  auto curl_headers = CURLHeaders(header_list);
  code_ = curl_easy_setopt(curl_handle_, CURLOPT_HTTPHEADER,
                           curl_headers.GetHeaders());
  if (code_ != CURLE_OK) {
    response.is_error = true;
    response.response = "Unable to do set HTTP header(s): " +
                        std::string(curl_easy_strerror(code_));
    return response;
  }

  code_ = curl_easy_setopt(curl_handle_, CURLOPT_POSTFIELDS, body.c_str());
  if (code_ != CURLE_OK) {
    response.is_error = true;
    response.response =
        "Unable to set POST body: " + std::string(curl_easy_strerror(code_));
    return response;
  }

  code_ = curl_easy_setopt(curl_handle_, CURLOPT_WRITEFUNCTION,
                           CURLClient::BodyCallback);
  if (code_ != CURLE_OK) {
    response.is_error = true;
    response.response = "Unable to set body callback function: " +
                        std::string(curl_easy_strerror(code_));
    return response;
  }

  code_ = curl_easy_setopt(curl_handle_, CURLOPT_HEADERFUNCTION,
                           CURLClient::HeaderCallback);
  if (code_ != CURLE_OK) {
    response.is_error = true;
    response.response = "Unable to set header callback function: " +
                        std::string(curl_easy_strerror(code_));
    return response;
  }

  code_ = curl_easy_setopt(curl_handle_, CURLOPT_HEADERDATA,
                           (void *)&response.headers);
  if (code_ != CURLE_OK) {
    response.is_error = true;
    response.response = "Unable to set cookie for headers: " +
                        std::string(curl_easy_strerror(code_));
    return response;
  }

  code_ = curl_easy_setopt(curl_handle_, CURLOPT_WRITEDATA,
                           (void *)&response.response);
  if (code_ != CURLE_OK) {
    response.is_error = true;
    response.response = "Unable to set cookie for body: " +
                        std::string(curl_easy_strerror(code_));
    return response;
  }

  code_ =
      curl_easy_setopt(curl_handle_, CURLOPT_USERAGENT, "libcurl-agent/1.0");
  if (code_ != CURLE_OK) {
    response.is_error = true;
    response.response =
        "Unable to set user agent: " + std::string(curl_easy_strerror(code_));
    return response;
  }

  code_ = curl_easy_setopt(curl_handle_, CURLOPT_TIMEOUT, 30L);
  if (code_ != CURLE_OK) {
    response.is_error = true;
    response.response = "Unable to set timeout";
    return response;
  }

  code_ = curl_easy_setopt(curl_handle_, CURLOPT_SSL_VERIFYPEER, 0L);
  if (code_ != CURLE_OK) {
    response.is_error = true;
    response.response = "Unable to turn off SSL peer verification";
    return response;
  }

  code_ = curl_easy_setopt(curl_handle_, CURLOPT_USERNAME, usr.c_str());
  if (code_ != CURLE_OK) {
    response.is_error = true;
    response.response = "Unable to set username";
    return response;
  }

  code_ = curl_easy_setopt(curl_handle_, CURLOPT_PASSWORD, key.c_str());
  if (code_ != CURLE_OK) {
    response.is_error = true;
    response.response = "Unable to set password";
    return response;
  }

  response.is_error = false;
  code_ = curl_easy_perform(curl_handle_);
  if (code_ != CURLE_OK) {
    response.is_error = true;
    response.response =
        "Unable to do HTTP POST: " + std::string(curl_easy_strerror(code_));
    return response;
  }

  return response;
}

Communicator::Communicator(const std::string &host_ip,
                           const std::string &host_port, const std::string &usr,
                           const std::string &key, bool ssl) {
  std::string base_url = (ssl ? "https://" : "http://") +
                         JoinHostPort(Localhost(false), host_port);
  parse_query_url_ = base_url + "/parseQuery";
  get_creds_url_ = base_url + "/getCreds";
  get_named_params_url_ = base_url + "/getNamedParams";
  lo_usr_ = usr;
  lo_key_ = key;
}

CredsInfo Communicator::ExtractCredentials(const std::string &encoded_str) {
  CredsInfo info;
  info.is_valid = false;

  auto kv_info = curl_.ExtractKV(encoded_str);
  if (!kv_info.is_valid) {
    info.msg = kv_info.msg;
    return info;
  }

  info.is_valid = true;
  info.username = kv_info.kv["username"];
  info.password = kv_info.kv["password"];
  return info;
}

NamedParamsInfo
Communicator::ExtractNamedParams(const std::string &encoded_str) {
  NamedParamsInfo info;

  auto kv_info = curl_.ExtractKV(encoded_str);
  if (!kv_info.is_valid) {
    info.p_info.is_valid = false;
    info.p_info.info = kv_info.msg;
    return info;
  }

  info.p_info = UnflattenParseInfo(kv_info.kv);
  for (int i = 0; i < std::stoi(kv_info.kv["named_params_size"]); ++i) {
    info.named_params.emplace_back(kv_info.kv[std::to_string(i)]);
  }

  return info;
}

ParseInfo Communicator::ExtractParseInfo(const std::string &encoded_str) {
  auto kv_info = curl_.ExtractKV(encoded_str);
  if (!kv_info.is_valid) {
    ParseInfo info;
    info.is_valid = false;
    info.info = kv_info.msg;
    return info;
  }

  return UnflattenParseInfo(kv_info.kv);
}

CredsInfo Communicator::GetCreds(const std::string &endpoint) {
  auto response = curl_.HTTPPost({"Content-Type: text/plain"}, get_creds_url_,
                                 endpoint, lo_usr_, lo_key_);

  CredsInfo info;
  info.is_valid = false;

  if (response.is_error) {
    LOG(logError) << "Unable to get creds: Something went wrong with cURL lib: "
                  << response.response << std::endl;
    info.msg = response.response;
    return info;
  }

  if (response.headers.find("Status") == response.headers.end()) {
    LOG(logError) << "Unable to get creds: status code is missing in header: "
                  << response.response << std::endl;
    return info;
  }

  if (std::stoi(response.headers["Status"]) != 0) {
    LOG(logError) << "Unable to get creds: non-zero status in header: "
                  << response.response << std::endl;
    return info;
  }

  return ExtractCredentials(response.response);
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

  if (response.is_error) {
    LOG(logError)
        << "Unable to get named params: Something went wrong with cURL lib: "
        << RM(response.response) << std::endl;
    return info;
  }

  if (response.headers.find("Status") == response.headers.end()) {
    LOG(logError)
        << "Unable to get named params: status code is missing in header: "
        << RM(response.response) << std::endl;
    return info;
  }

  if (std::stoi(response.headers["Status"]) != 0) {
    LOG(logError) << "Unable to get named params: non-zero status in header: "
                  << RM(response.response) << std::endl;
    return info;
  }

  return ExtractNamedParams(response.response);
}

ParseInfo Communicator::ParseQuery(const std::string &query) {
  auto response = curl_.HTTPPost({"Content-Type: text/plain"}, parse_query_url_,
                                 query, lo_usr_, lo_key_);

  ParseInfo info;
  info.is_valid = false;
  info.info = "Something went wrong while parsing the N1QL query";

  if (response.is_error) {
    LOG(logError)
        << "Unable to parse N1QL query: Something went wrong with cURL lib: "
        << RU(response.response) << std::endl;
    return info;
  }

  if (response.headers.find("Status") == response.headers.end()) {
    LOG(logError)
        << "Unable to parse N1QL query: status code is missing in header:"
        << RU(response.response) << std::endl;
    return info;
  }

  int status = std::stoi(response.headers["Status"]);
  if (status != 0) {
    LOG(logError) << "Unable to parse N1QL query: non-zero status in header"
                  << status << std::endl;
    return info;
  }

  return ExtractParseInfo(response.response);
}

void Communicator::Refresh() { creds_cache_.clear(); }