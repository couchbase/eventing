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

#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

enum LogLevel { logSilent, logError, logInfo, logWarning, logDebug, logTrace };

inline LogLevel LevelFromString(const std::string &level) {
  if (level == "SILENT")
    return logSilent;
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

class SystemLog : public std::ostringstream {
public:
  static LogLevel level_;
  static void setLogLevel(LogLevel level);
  static bool getRedactOverride();
  static bool isDisabled(LogLevel check) { return check > level_; }
  static std::string redact(const std::string msg) {
    return redact_ ? "<ud>" + msg + "</ud>" : msg;
  }
  ~SystemLog() {
    std::lock_guard<std::mutex> guard(lock_);
    std::cerr << str() << std::flush;
  }

private:
  static std::mutex lock_;
  static bool redact_;
};

class ApplicationLog : public std::ostringstream {
public:
  ~ApplicationLog() {
    std::lock_guard<std::mutex> guard(lock_);
    std::cout << str() << std::flush;
  }

private:
  static std::mutex lock_;
};

#define APPLOG ApplicationLog()

#define LOG(level)                                                             \
  if (SystemLog::isDisabled(level))                                            \
    ;                                                                          \
  else                                                                         \
    SystemLog()

#define RM(msg) msg
#define RS(msg) msg
#define RU(msg) SystemLog::redact(msg)

#endif // LOG_H
