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

#ifndef PARSE_DEPLOYMENT
#define PARSE_DEPLOYMENT

#include <cassert>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "curl.h"

#include "../../gen/flatbuf/cfg_schema_generated.h"
#include "../../gen/flatbuf/payload_generated.h"

typedef struct deployment_config_s {
  std::string metadata_bucket;
  std::string source_bucket;
  std::map<std::string, std::map<std::string, std::vector<std::string>>>
      component_configs;
  std::vector<CurlBinding> curl_bindings;
} deployment_config;

deployment_config *ParseDeployment(const char *app_name);
std::vector<std::string> ToStringArray(
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> *from);

#endif