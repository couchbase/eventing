#include <chrono>
#include <iomanip>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <v8.h>
#include <nlohmann/json.hpp>

#include "exceptioninsight.h"
#include "isolate_data.h"

#include "log.h"
#include "utils.h"

void ExceptionInsight::AccumulateException(v8::TryCatch &try_catch) {

  auto context = isolate_->GetCurrentContext();
  auto v8exception_info = GetV8ExceptionInfo(isolate_, context, &try_catch);

  // compute a hash for the exception-info to identify duplicates:
  // The field 'stack' contains the exception as well as the stack-track and
  // seems to be the right candidate to check for distinct exceptions.
  uint64_t crc = crc64_iso.Checksum(reinterpret_cast<const uint8_t *>(v8exception_info.stack.c_str()),
    v8exception_info.stack.length());

  {
    std::lock_guard<std::mutex> lock(lock_);
    auto &entry = this->entries_[crc];

    if (entry.count_ == 0) { // capture first occurrance of the exception
      entry.v8exception_info_ = v8exception_info;
      entry.count_ = 1;
    } else {
      entry.count_++;
    }
  }
}

void ExceptionInsight::AccumulateAndClear(ExceptionInsight &from) {

  std::unique_lock<std::mutex> lock_me(lock_, std::defer_lock);
  std::unique_lock<std::mutex> lock_from(from.lock_, std::defer_lock);
  std::lock(lock_me, lock_from);

  // Merge all exceptions from 'from' into the current ExceptionInsight instance,
  // either adding new ones in, or incrementing counts of known ones.
  for (auto const& iter : from.entries_)
  {
    auto &entry = this->entries_[iter.first];

    if (entry.count_ == 0) { // got a new one not in this instance yet.
      entry.v8exception_info_ = iter.second.v8exception_info_;
      entry.count_ = iter.second.count_;
    } else {
      entry.count_ += iter.second.count_;
    }
  }

  // re-init the 'from' instance to capture exceptions for the next period of time.
  from.entries_.clear();
  from.InitStartTime();
}

void ExceptionInsight::LogExceptionSummary(ExceptionInsight &exception_insight) {

    std::lock_guard<std::mutex> guard(exception_insight.lock_);

    for (auto const& iter : exception_insight.entries_)
    {
      nlohmann::json exceptionInfo;

      exceptionInfo["since"] = exception_insight.start_time_;
      exceptionInfo["count"] = iter.second.count_;
      exceptionInfo["exception"] = iter.second.v8exception_info_.exception;
      exceptionInfo["file"] = iter.second.v8exception_info_.file;
      exceptionInfo["line"] = iter.second.v8exception_info_.line;
      exceptionInfo["srcLine"] = iter.second.v8exception_info_.srcLine;
      exceptionInfo["stack"] = iter.second.v8exception_info_.stack;

      APPLOG << exceptionInfo.dump() << std::endl;
    }
}

ExceptionInsight::ExceptionInsight(v8::Isolate *isolate) : isolate_(isolate),
  entries_(std::map<uint64_t, ExceptionInfoEntry>()) {

    InitStartTime();
  };

ExceptionInsight &ExceptionInsight::Get(v8::Isolate *isolate) {
  return *(UnwrapData(isolate)->exception_insight);
}

void ExceptionInsight::Setup(const std::string &function_name) {
  std::lock_guard<std::mutex> lock(lock_);
  function_name_ = function_name;
}

void ExceptionInsight::InitStartTime() {
  time_t t;
  struct tm tmbuf;

  time(&t);

#ifdef WIN32
  localtime_s(&tmbuf, &t);
#else
  localtime_r(&t, &tmbuf);
#endif

  strftime(start_time_, sizeof(start_time_), "%FT%T",  &tmbuf);
}

ExceptionInfoEntry::ExceptionInfoEntry(): count_(0) {};