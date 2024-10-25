#ifndef _EVENTING_EXCEPTIONINSIGHT
#define _EVENTING_EXCEPTIONINSIGHT

#include <chrono>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <v8.h>

#include "crc64.h"
#include "utils.h"

struct ExceptionInfoEntry {
  ExceptionInfoEntry();

  uint32_t count_;
  V8ExceptionInfo v8exception_info_;
};

class ExceptionInsight {
public:
  ExceptionInsight();

  void Setup(const std::string &instance_id);

  void AccumulateException(v8::Isolate *isolate, v8::TryCatch &,
                           bool timeout = false);

  void AccumulateAndClear(ExceptionInsight &from);
  void LogExceptionSummary(ExceptionInsight &summary);

private:
  ExceptionInsight(const ExceptionInsight &) = delete;
  ExceptionInsight &operator=(const ExceptionInsight &) = delete;

  void PopulateExceptionInfo(nlohmann::json &exceptionInfo,
                             V8ExceptionInfo v8exception_info);

  void InitStartTime();

  std::string location_;
  std::mutex lock_;
  char start_time_[sizeof "2021-02-10T15:46:00"];

  std::map<uint64_t, ExceptionInfoEntry> entries_;
};

#endif
