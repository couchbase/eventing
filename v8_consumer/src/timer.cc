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

#include <mutex>

#include "isolate_data.h"
#include "js_exception.h"
#include "timer.h"
#include "utils.h"
#include "v8worker.h"
#include "crc32.h"

std::atomic<int64_t> timer_context_size_exceeded_counter = {0};
thread_local std::mt19937_64
    rng(std::random_device{}() +
        std::hash<std::thread::id>()(std::this_thread::get_id()));

Timer::Timer(v8::Isolate *isolate, const v8::Local<v8::Context> &context)
    : isolate_(isolate) {
  context_.Reset(isolate_, context);
}

Timer::~Timer() { context_.Reset(); }

timer::EpochInfo Timer::Epoch(const v8::Local<v8::Value> &date_val) {
  auto utils = UnwrapData(isolate_)->utils;
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  auto get_time_val = utils->GetPropertyFromObject(date_val, "getTime");
  auto get_time_func = get_time_val.As<v8::Function>();

  v8::Local<v8::Value> seconds_v8val;
  if (!TO_LOCAL(get_time_func->Call(context, date_val, 0, nullptr),
                &seconds_v8val)) {
    return {false};
  }

  v8::Local<v8::Number> number_v8val;
  if (!TO_LOCAL(seconds_v8val->ToNumber(context), &number_v8val)) {
    return {false};
  }

  // v8 returns epoch time in milliseconds
  // Hence, dividing it by 1000 to convert to seconds
  auto epoch = number_v8val->IntegerValue(context).FromMaybe(0) / 1000;
  return {true, epoch};
}

bool Timer::CreateTimerImpl(const v8::FunctionCallbackInfo<v8::Value> &args) {
  if (!ValidateArgs(args)) {
    return false;
  }

  v8::HandleScope handle_scope(isolate_);

  auto js_exception = UnwrapData(isolate_)->js_exception;
  auto epoch_info = Epoch(args[1]);
  if (!epoch_info.is_valid) {
    js_exception->ThrowEventingError(
        "Unable to compute epoch for the given Date instance");
    return false;
  }

  auto utils = UnwrapData(isolate_)->utils;
  auto v8worker = UnwrapData(isolate_)->v8worker;
  timer::TimerInfo timer_info;
  timer_info.epoch = epoch_info.epoch;
  timer_info.seq_num = v8worker->currently_processed_seqno_;
  timer_info.callback = utils->GetFunctionName(args[0]);
  timer_info.context = JSONStringify(isolate_, args[3]);

  // if reference is null or undefined, generate one
  if (args[2]->IsString()) {
    timer_info.reference = utils->ToCPPString(args[2]);
  } else {
    timer_info.reference =
        std::to_string(rng()) + std::to_string(timer_info.seq_num);
  }

  auto ref = timer_info.callback + ":" + timer_info.reference;
  uint32_t hash = crc32_8(ref.c_str(), ref.size(), 0 /*crc_in*/);
  timer_info.vb = hash % NUM_VBUCKETS;
  LOG(logTrace) << "ref: " << ref << "hash: " << hash  << "Timer Partition is: " << timer_info.vb << " " << std::endl;

  if (timer_info.context.size() > static_cast<unsigned>(timer_context_size)) {
    js_exception->ThrowEventingError(
        "The context payload size is more than the configured size:" +
        std::to_string(timer_context_size) + " bytes");
    timer_context_size_exceeded_counter++;
    return false;
  }
  auto err = v8worker->SetTimer(timer_info);
  if (err != LCB_SUCCESS) {
    js_exception->ThrowKVError(v8worker->GetTimerLcbHandle(), err);
    return false;
  }
  args.GetReturnValue().Set(v8Str(isolate_, timer_info.reference));
  return true;
}

bool Timer::ValidateArgs(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto js_exception = UnwrapData(isolate_)->js_exception;
  if (args.kArgsLength < 3) {
    js_exception->ThrowEventingError(
        "Need 3 arguments - callback function, time, reference");
    return false;
  }

  auto utils = UnwrapData(isolate_)->utils;
  if (!utils->IsFuncGlobal(args[0])) {
    js_exception->ThrowEventingError(
        "First argument to createTimer must be a valid global function");
    return false;
  }

  if (!args[1]->IsDate()) {
    js_exception->ThrowEventingError(
        "Second argument to createTimer must be a Date object instance");
    return false;
  }

  if (!args[2]->IsString() && !args[2]->IsNull() && !args[2]->IsUndefined()) {
    js_exception->ThrowEventingError("Third argument to createTimer must be a "
                                     "string (or null to generate an ID)");
    return false;
  }

  return true;
}

void CreateTimer(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  auto timer = UnwrapData(isolate)->timer;
  ++timer_create_counter;
  timer->CreateTimerImpl(args);
}
