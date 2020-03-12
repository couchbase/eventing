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

#ifndef ERROR_H
#define ERROR_H

#include <memory>
#include <string>

using Error = std::unique_ptr<std::string>;

template <typename T> Error MakeError(T &&msg) {
  return std::make_unique<T>(std::forward<T>(msg));
}

#endif
