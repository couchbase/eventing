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

#ifndef TIMER_H
#define TIMER_H

#include <random>
#include <string>
#include <v8.h>

extern thread_local std::mt19937_64 rng;

struct EpochInfo {
  EpochInfo(bool is_valid) : is_valid(is_valid), epoch(0) {}
  EpochInfo(bool is_valid, int64_t epoch) : is_valid(is_valid), epoch(epoch) {}

  bool is_valid;
  int64_t epoch;
};

struct TimerInfo {
  TimerInfo() : epoch(0), vb(0), seq_num(0) {}

  std::string ToJSON(v8::Isolate *isolate,
                     const v8::Local<v8::Context> &context);

  int64_t epoch;
  int64_t vb;
  int64_t seq_num;
  std::string callback;
  std::string reference;
  std::string context;
};

class Timer {
public:
  Timer(v8::Isolate *isolate, const v8::Local<v8::Context> &context);
  virtual ~Timer();

  bool CreateTimerImpl(const v8::FunctionCallbackInfo<v8::Value> &args);

private:
  EpochInfo Epoch(const v8::Local<v8::Value> &date_val);
  bool ValidateArgs(const v8::FunctionCallbackInfo<v8::Value> &args);

  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;
};

void CreateTimer(const v8::FunctionCallbackInfo<v8::Value> &args);

#endif
