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

#ifndef LOG_H
#define LOG_H

#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>

enum LogLevel { logInfo, logError, logWarning, logDebug, logTrace };

extern LogLevel desiredLogLevel;

inline std::string NowTime();
static std::string LevelToString(LogLevel level);

extern std::ostringstream os;

extern void setLogLevel(LogLevel level);

static std::ostringstream &Logger(LogLevel level = logInfo) {
  auto t = std::time(nullptr);
  auto tm = *std::localtime(&t);
  os << std::put_time(&tm, "%Y-%m-%dT%H-%M-%S%z");
  os << " " << LevelToString(level) << " ";
  os << "VWCP"
     << " ";
  return os;
}

static std::string FlushLog() {
  std::string str = os.str();
  os.str(std::string());
  return str;
}

static std::string LevelToString(LogLevel level) {
  static const char *const buffer[] = {"[Info]", "[Error]", "[Warning]",
                                       "[Debug]", "[Trace]"};
  return buffer[level];
}

static LogLevel LevelFromString(const std::string &level) {
  if (level == "INFO")
    return logInfo;
  if (level == "ERROR")
    return logError;
  if (level == "WARNING")
    return logWarning;
  if (level == "DEBUG")
    return logDebug;
  if (level == "TRACE")
    return logTrace;

  return logInfo;
}

#define LOG(level)                                                             \
  if (level > desiredLogLevel)                                                 \
    ;                                                                          \
  else                                                                         \
    Logger(level)

#endif
