#ifndef _EVENTING_EXCEPTIONINSIGHT
#define _EVENTING_EXCEPTIONINSIGHT

#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <v8.h>

#include "utils.h"
#include "crc64.h"

struct ExceptionInfoEntry {
  ExceptionInfoEntry();

  uint32_t count_;
  ExceptionInfo exception_info_;
};

class ExceptionInsight {
public:
  explicit ExceptionInsight(v8::Isolate *isolate);

  void Setup(const std::string &function_name);

  void AccumulateException(v8::TryCatch &);

  void AccumulateAndClear(ExceptionInsight &from);
  void LogExceptionSummary(ExceptionInsight &summary);

  static ExceptionInsight &Get(v8::Isolate *isolate);

private:
  ExceptionInsight(const ExceptionInsight &) = delete;
  ExceptionInsight &operator=(const ExceptionInsight &) = delete;

  void InitStartTime();

  std::mutex lock_;
  std::string function_name_;
  char start_time_[26];

  std::map<uint64_t, ExceptionInfoEntry> entries_;

  v8::Isolate *isolate_;
};

#endif
