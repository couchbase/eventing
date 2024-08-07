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

#include "parse_deployment.h"

deployment_config *ParseDeployment(const char *app_code) {
  deployment_config *config = new deployment_config();

  auto app_cfg = flatbuf::cfg::GetConfig((const void *)app_code);

  auto dep_cfg = app_cfg->depCfg();
  config->metadata_bucket = dep_cfg->metadataBucket()->str();
  config->metadata_scope =
      checkAndReturnDefaultForScopeOrCollection(dep_cfg->metadataScope()->str(), "_default");
  config->metadata_collection =
      checkAndReturnDefaultForScopeOrCollection(dep_cfg->metadataCollection()->str(), "_default");
  config->source_bucket = dep_cfg->sourceBucket()->str();
  config->source_scope =
      checkAndReturnDefaultForScopeOrCollection(dep_cfg->sourceScope()->str(), "_default");
  config->source_collection =
      checkAndReturnDefaultForScopeOrCollection(dep_cfg->sourceCollection()->str(), "_default");

  auto buckets = dep_cfg->buckets();

  std::unordered_map<std::string, std::vector<std::string>> buckets_info;
  std::vector<std::string> bucket_alias;
  for (flatbuffers::uoffset_t i = 0; i < buckets->size(); i++) {
    std::vector<std::string> bucket_info;
    bucket_info.push_back(buckets->Get(i)->bucketName()->str());
    bucket_info.push_back(
        checkAndReturnDefaultForScopeOrCollection(buckets->Get(i)->scopeName()->str(), "_default"));
    bucket_info.push_back(checkAndReturnDefaultForScopeOrCollection(
        buckets->Get(i)->collectionName()->str(), "_default"));
    bucket_info.push_back(buckets->Get(i)->alias()->str());
    bucket_alias.push_back(buckets->Get(i)->alias()->str());

    buckets_info[buckets->Get(i)->alias()->str()] = bucket_info;
  }

  const auto buckets_access = app_cfg->access();
  if (buckets_access != nullptr) {
    auto alias = bucket_alias.begin();
    for (flatbuffers::uoffset_t i = 0; i < buckets_access->size(); i++) {
      if (alias != bucket_alias.end()) {
        buckets_info[*alias].push_back(buckets_access->Get(i)->str());
        alias++;
      } else {
        break;
      }
    }
  } else {
    for (auto alias = bucket_alias.begin(); alias != bucket_alias.end();
         alias++) {
      buckets_info[*alias].push_back("rw");
    }
  }
  config->component_configs["buckets"] = buckets_info;

  const auto curl_cfg = app_cfg->curl();
  if (curl_cfg != nullptr) {
    config->curl_bindings.reserve(static_cast<std::size_t>(curl_cfg->size()));
    for (flatbuffers::uoffset_t i = 0; i < curl_cfg->size(); ++i) {
      config->curl_bindings.emplace_back(curl_cfg->Get(i));
    }
  }

  auto constant_bindings = app_cfg->constants();
  if (constant_bindings != nullptr) {
    for (flatbuffers::uoffset_t i = 0; i < constant_bindings->size(); i++) {
      config->constant_bindings.push_back(
          {constant_bindings->Get(i)->value()->str(),
           constant_bindings->Get(i)->literal()->str()});
    }
  }
  return config;
}

std::vector<std::string> ToStringArray(
  const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> *from) {
  std::vector<std::string> to(from->size());
  for (flatbuffers::uoffset_t i = 0; i < from->size(); ++i) {
    to[i] = from->Get(i)->str();
  }
  return to;
}

std::string checkAndReturnDefaultForScopeOrCollection(const std::string &key,
                                  const std::string &defaultValue) {
  if (key == "") {
    return defaultValue;
  }
  return key;
}
