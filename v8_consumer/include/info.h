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

#ifndef INFO_H
#define INFO_H

#include <string>

struct Info {
  Info() : is_fatal(false) {}
  Info(bool is_fatal) : is_fatal(is_fatal) {}
  Info(bool is_fatal, std::string msg)
      : is_fatal(is_fatal), msg(std::move(msg)) {}

  bool is_fatal;
  std::string msg;
};

#endif // COUCHBASE_INFO_H
