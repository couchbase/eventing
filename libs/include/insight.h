#ifndef _EVENTING_INSIGHT
#define _EVENTING_INSIGHT

#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <v8.h>

#include "transpiler.h"

class RateLimiter {
public:
  RateLimiter();
  enum Action { go_on, no_more, drop_it };
  Action Tick();

private:
  using clock = std::chrono::high_resolution_clock;
  using point = std::chrono::time_point<clock>;
  using ms = std::chrono::milliseconds;
  static constexpr int max_count = 100; // 100 messages
  static constexpr int min_time = 100;  // .. in 100 seconds
  uint64_t msg_count_;
  point start_time_;
};

struct LineEntry {
  LineEntry();
  uint64_t count_;
  double time_;
  uint64_t err_count_;
  std::string last_err_;
  std::string last_log_;
  RateLimiter limiter_;
};

typedef std::map<int, LineEntry> Insight;

class CodeInsight {
public:
  explicit CodeInsight(v8::Isolate *isolate);

  void Setup(const std::string &script, const std::string &srcmap,
             const std::list<InsertedCharsInfo> &insertions);

  void AccumulateTime(uint64_t nanotime);
  void AccumulateException(v8::TryCatch &);
  void AccumulateLog(const std::string &msg);

  std::string ToJSON();
  void Accumulate(CodeInsight &other);

  static CodeInsight &Get(v8::Isolate *isolate);

private:
  CodeInsight(const CodeInsight &) = delete;
  CodeInsight &operator=(const CodeInsight &) = delete;

  int RectifyLine(int line) const;
  void Log(LineEntry &line, const std::string &msg);

  std::mutex lock_;
  Insight insight_;
  v8::Isolate *isolate_;
  std::string script_;
  std::string srcmap_;
  std::list<InsertedCharsInfo> insertions_;

  // sliding window
  static constexpr double window_size = 100;
};

#endif
