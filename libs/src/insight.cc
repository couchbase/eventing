#include <chrono>
#include <iomanip>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <v8.h>

#include "insight.h"
#include "isolate_data.h"
#include "log.h"
#include "utils.h"

void CodeInsight::AccumulateTime(uint64_t nanotime) {
  auto stack = v8::StackTrace::CurrentStackTrace(isolate_, 1,
                                                 v8::StackTrace::kLineNumber);
  if (stack->GetFrameCount() < 1)
    return;
  auto line = stack->GetFrame(isolate_, 0)->GetLineNumber();
  std::lock_guard<std::mutex> lock(lock_);
  auto &entry = insight_[line];
  entry.count_++;
  entry.time_ -= entry.time_ / window_size;
  entry.time_ += nanotime / window_size;
}

void CodeInsight::AccumulateLog(const std::string &log) {
  auto stack = v8::StackTrace::CurrentStackTrace(isolate_, 1,
                                                 v8::StackTrace::kLineNumber);
  if (stack->GetFrameCount() < 1)
    return;
  auto line = stack->GetFrame(isolate_, 0)->GetLineNumber();
  std::lock_guard<std::mutex> lock(lock_);
  auto &entry = insight_[line];
  entry.last_log_ = log;
}

void CodeInsight::AccumulateException(v8::TryCatch &try_catch) {
  auto context = isolate_->GetCurrentContext();
  auto emsg = ExceptionString(isolate_, context, &try_catch);
  v8::Local<v8::Message> msg = try_catch.Message();
  if (msg.IsEmpty())
    return;
  auto line = msg->GetLineNumber(context).FromMaybe(0);
  std::lock_guard<std::mutex> lock(lock_);
  auto &entry = insight_[line];
  entry.err_count_++;
  entry.last_err_ = emsg;
  Log(entry, emsg);
}

void CodeInsight::Accumulate(CodeInsight &other) {
  std::unique_lock<std::mutex> lock_me(lock_, std::defer_lock);
  std::unique_lock<std::mutex> lock_other(other.lock_, std::defer_lock);
  std::lock(lock_me, lock_other);
  for (auto i = other.insight_.begin(); i != other.insight_.end(); ++i) {
    auto &src = i->second;
    auto &dst = this->insight_[i->first];
    dst.count_ += src.count_;
    dst.time_ += src.time_;
    dst.err_count_ += src.err_count_;
    if (src.last_err_.length() > 0) {
      dst.last_err_ = src.last_err_;
    }
    if (src.last_log_.length() > 0) {
      dst.last_log_ = src.last_log_;
    }
  }
  if (other.script_.length() > 0) {
    script_ = other.script_;
  }
  if (other.srcmap_.length() > 0) {
    srcmap_ = other.srcmap_;
  }
  if (other.insertions_.size() > 0) {
    insertions_ = other.insertions_;
  }
}

void CodeInsight::Log(LineEntry &line, const std::string &msg) {
  auto action = line.limiter_.Tick();
  if (action == RateLimiter::drop_it)
    return;

  std::string str(msg);
  str.erase(remove_if(str.begin(), str.end(),
                      [](char c) { return c < 32 || c > 126; }),
            str.end());
  if (action == RateLimiter::no_more)
    str += "\n(above exceeded log rate limit, squelching)";
  LOG(logInfo) << str << std::endl;
}

CodeInsight::CodeInsight(v8::Isolate *isolate) : isolate_(isolate) {}

CodeInsight &CodeInsight::Get(v8::Isolate *isolate) {
  return *(UnwrapData(isolate)->code_insight);
}

void CodeInsight::Setup(const std::string &script, const std::string &srcmap,
                        const std::list<InsertedCharsInfo> &insertions) {
  std::lock_guard<std::mutex> lock(lock_);
  script_ = script;
  srcmap_ = srcmap;
  insertions_ = insertions;
}

static std::string escape(const std::string &str) {
  std::ostringstream os;
  for (auto ch = str.cbegin(); ch != str.cend(); ++ch) {
    if (*ch == '"' || *ch == '\\' || *ch < 32 || *ch > 126) {
      os << R"(\u)";
      os << std::hex << std::setw(4) << std::setfill('0');
      os << unsigned(*ch);
      continue;
    }
    os << *ch;
  }
  return os.str();
}

std::string CodeInsight::ToJSON() {
  std::lock_guard<std::mutex> lock(lock_);
  std::ostringstream os;
  os << "{" << std::endl;
  os << R"( "script": ")" << escape(script_) << R"(",)" << std::endl;
  os << R"( "srcmap": ")" << escape(srcmap_) << R"(",)" << std::endl;
  os << R"( "lines": {)" << std::endl;
  for (auto i = insight_.begin(); i != insight_.end(); ++i) {
    if (i != insight_.begin())
      os << "," << std::endl;
    os << R"( ")" << RectifyLine(i->first) << R"(": {)" << std::endl;
    os << R"(  "call_count": )" << i->second.count_ << "," << std::endl;
    os << R"(  "call_time": )" << i->second.time_ << "," << std::endl;
    os << R"(  "error_count": )" << i->second.err_count_ << "," << std::endl;
    os << R"(  "error_msg": ")" << escape(i->second.last_err_) << R"(",)"
       << std::endl;
    os << R"(  "last_log": ")" << escape(i->second.last_log_) << '"'
       << std::endl;
    os << " }";
  }
  os << std::endl << " }" << std::endl;
  os << std::endl << "}" << std::endl;
  return os.str();
}

// Use transpiler's logic to account for N1QL generated expansions
int CodeInsight::RectifyLine(int line) const {
  for (const auto &pos : insertions_) {
    if (pos.line_no < line) {
      line -= pos.type_len;
    }
  }
  return line;
}

RateLimiter::Action RateLimiter::Tick() {
  auto now = clock::now();
  auto diff = now - start_time_;
  if (std::chrono::duration_cast<ms>(diff).count() > min_time * 1000) {
    start_time_ = now;
    msg_count_ = 0;
  }
  msg_count_++;
  if (msg_count_ < max_count)
    return go_on;
  if (msg_count_ == max_count)
    return no_more;
  return drop_it;
}

RateLimiter::RateLimiter() : msg_count_(0), start_time_(clock::now()) {}

LineEntry::LineEntry() : count_(0), time_(0), err_count_(0) {}
