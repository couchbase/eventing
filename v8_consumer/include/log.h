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

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>

enum LogLevel { logSilent, logInfo, logError, logWarning, logDebug, logTrace };

extern std::string appName;
extern LogLevel desiredLogLevel;
extern std::string workerID;

inline std::string NowTime();
static std::string LevelToString(LogLevel level);

extern std::ostringstream os;

extern void setAppName(std::string appName);
extern void setLogLevel(LogLevel level);
extern void setWorkerID(std::string ID);

static std::ostringstream &Logger(LogLevel level = logInfo) {
  using namespace std::chrono;

  auto now = system_clock::now();
  auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

  auto t = std::time(nullptr);
  auto tm = *std::localtime(&t);

  os << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S");
  os << '.' << std::setfill('0') << std::setw(3) << ms.count();

  // %z format specifier will dump timezone offset as hhmm format
  // Performing string splits to get hh:mm format - in order to make
  // it consistent with golang format specifier
  std::ostringstream time_fmt_os;
  time_fmt_os << std::put_time(&tm, "%z");
  std::string ts = time_fmt_os.str();
  time_fmt_os.str(std::string());

  os << ts.substr(0, ts.length() - 2);
  os << ":";
  os << ts.substr(ts.length() - 2);

  os << " " << LevelToString(level) << " ";
  os << "VWCP [" << appName << ":" << workerID << "] ";
  return os;
}

static std::string FlushLog() {
  std::string str = os.str();
  os.str(std::string());
  return str;
}

static std::string LevelToString(LogLevel level) {
  static const char *const buffer[] = {"[Silent]",  "[Info]",  "[Error]",
                                       "[Warning]", "[Debug]", "[Trace]"};
  return buffer[level];
}

static LogLevel LevelFromString(const std::string &level) {
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

#define LOG(level)                                                             \
  if (level > desiredLogLevel)                                                 \
    ;                                                                          \
  else                                                                         \
    Logger(level)

#endif
