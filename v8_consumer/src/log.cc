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

#include "log.h"
#include <cstdlib>

std::mutex SystemLog::lock_;
std::mutex ApplicationLog::lock_;

bool SystemLog::redact_ = SystemLog::getRedactOverride();
LogLevel SystemLog::level_ = logInfo;

void SystemLog::setLogLevel(LogLevel level) { level_ = level; }

bool SystemLog::getRedactOverride() {
  const char *evar = std::getenv("CB_EVENTING_NOREDACT");
  if (!evar) {
    return true;
  }
  return (std::string(evar) != "true");
}
