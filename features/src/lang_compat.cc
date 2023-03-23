// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

#include "lang_compat.h"

LanguageCompatibility::LanguageCompatibility(const std::string &version_str) {
  if (version_str == "6.0.0") {
    version = Version::k6_0_0;
    return;
  }
  if (version_str == "6.5.0") {
    version = Version::k6_5_0;
    return;
  }
  if (version_str == "6.6.2") {
    version = Version::k6_6_2;
    return;
  }
  if (version_str == "7.2.0") {
    version = Version::k7_2_0;
  }
}