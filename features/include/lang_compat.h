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

#ifndef LANG_COMPAT_H
#define LANG_COMPAT_H

#include <string>

struct LanguageCompatibility {
  explicit LanguageCompatibility(const std::string &version_str);

  enum Version { k6_0_0, k6_5_0, k6_6_2};
  Version version{k6_6_2};
  std::string version_str {"6.6.2"};
  std::string get_version_as_string() const {
    return this->version_str;
  }
};

#endif
