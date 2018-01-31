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

std::ostringstream app_log_os;

std::string appName = "";
LogLevel desiredLogLevel = LogLevel(0);
std::string workerID = "";

void setAppName(std::string app) { appName = app; }

void setLogLevel(LogLevel level) { desiredLogLevel = level; }

void setWorkerID(std::string wID) { workerID = wID; }
