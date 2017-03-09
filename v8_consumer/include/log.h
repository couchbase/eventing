#ifndef __LOG_H__
#define __LOG_H__

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
  os << std::put_time(&tm, "%d-%m-%YT%H-%M-%S%z");
  os << " " << LevelToString(level) << " ";
  os << "VWCP" << " ";
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

#define LOG(level) \
  if (level > desiredLogLevel) ; \
else Logger(level)

#endif // __LOG_H__
