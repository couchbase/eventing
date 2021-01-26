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

#include "timer_defs.h"

struct TIMER_MSG {
  TIMER_MSG(bool success, std::string message)
      : success(success), message(message){};
  bool success;
  std::string message;
};

extern thread_local std::mt19937_64 rng;

class Timer {
public:
  Timer(v8::Isolate *isolate, const v8::Local<v8::Context> &context,
        int32_t timer_reduction_ratio);
  virtual ~Timer();

  TIMER_MSG CreateTimerImpl(const v8::FunctionCallbackInfo<v8::Value> &args);
  bool CancelTimerImpl(const v8::FunctionCallbackInfo<v8::Value> &args);

  uint16_t timer_mask_bits_{0};

private:
  timer::EpochInfo Epoch(const v8::Local<v8::Value> &date_val);
  bool ValidateArgs(const v8::FunctionCallbackInfo<v8::Value> &args);
  bool ValidateCancelTimerArgs(const v8::FunctionCallbackInfo<v8::Value> &args);
  void FillTimerPartition(timer::TimerInfo &tinfo, const int32_t &num_vbuckets);

  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;
};

void CreateTimer(const v8::FunctionCallbackInfo<v8::Value> &args);
void CancelTimer(const v8::FunctionCallbackInfo<v8::Value> &args);

#endif
