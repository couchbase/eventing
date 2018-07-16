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

#include "timer.h"
#include "js_exception.h"
#include "utils.h"
#include "v8worker.h"

Timer::Timer(v8::Isolate *isolate, const v8::Local<v8::Context> &context)
    : isolate_(isolate) {
  context_.Reset(isolate_, context);
}

Timer::~Timer() { context_.Reset(); }

EpochInfo Timer::Epoch(const v8::Local<v8::Value> &date_val) {
  auto utils = UnwrapData(isolate_)->utils;
  v8::HandleScope handle_scope(isolate_);

  auto context = context_.Get(isolate_);
  auto get_time_val = utils->GetMethodFromObject(date_val, "getTime");
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
  auto epoch = number_v8val->IntegerValue() / 1000;
  return {true, epoch};
}

bool Timer::CreateTimerImpl(const v8::FunctionCallbackInfo<v8::Value> &args) {
  if (!ValidateArgs(args)) {
    return false;
  }

  v8::HandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);

  auto js_exception = UnwrapData(isolate_)->js_exception;
  auto epoch_info = Epoch(args[1]);
  if (!epoch_info.is_valid) {
    js_exception->Throw("Unable to compute epoch for the given Date instance");
    return false;
  }

  auto utils = UnwrapData(isolate_)->utils;
  auto v8worker = UnwrapData(isolate_)->v8worker;

  TimerInfo timer_info;
  timer_info.epoch = epoch_info.epoch;
  timer_info.vb = v8worker->currently_processed_vb_;
  timer_info.seq_num = v8worker->currently_processed_seqno_;
  timer_info.callback = utils->GetFunctionName(args[0]);
  timer_info.reference = utils->ToCPPString(args[2]);
  timer_info.context = JSONStringify(isolate_, args[3]);

  doc_timer_msg_t msg;
  msg.timer_entry = timer_info.ToJSON(isolate_, context);
  v8worker->timer_queue_->Push(msg);

  return true;
}

bool Timer::ValidateArgs(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto js_exception = UnwrapData(isolate_)->js_exception;
  if (args.kArgsLength < 3) {
    js_exception->Throw(
        "Need 3 arguments - callback function, time, reference");
    return false;
  }

  auto utils = UnwrapData(isolate_)->utils;
  if (!utils->IsFuncGlobal(args[0])) {
    return false;
  }

  if (!args[1]->IsDate()) {
    js_exception->Throw("First argument must be a JavaScript Date instance");
    return false;
  }

  if (!args[2]->IsString()) {
    js_exception->Throw("Third argument must be a JavaScript string");
    return false;
  }

  return true;
}

void CreateTimer(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  auto timer = UnwrapData(isolate)->timer;
  timer->CreateTimerImpl(args);
}

std::string TimerInfo::ToJSON(v8::Isolate *isolate,
                              const v8::Local<v8::Context> &context) {
  bool success = false;
  v8::HandleScope handle_scope(isolate);

  std::string json;
  auto entry = v8::Object::New(isolate);

  {
    auto key = v8Str(isolate, "epoch");
    auto value = v8::Number::New(isolate, epoch);
    if (!TO(entry->Set(context, key, value), &success) && !success) {
      return json;
    }
  }

  {
    auto key = v8Str(isolate, "callback");
    auto value = v8Str(isolate, callback);
    if (!TO(entry->Set(context, key, value), &success) && !success) {
      return json;
    }
  }

  {
    auto key = v8Str(isolate, "reference");
    auto value = v8Str(isolate, reference);
    if (!TO(entry->Set(context, key, value), &success) && !success) {
      return json;
    }
  }

  {
    auto key = v8Str(isolate, "vb");
    auto value = v8::Number::New(isolate, vb);
    if (!TO(entry->Set(context, key, value), &success) && !success) {
      return json;
    }
  }

  {
    auto key = v8Str(isolate, "seq_num");
    auto value = v8::Number::New(isolate, seq_num);
    if (!TO(entry->Set(context, key, value), &success) && !success) {
      return json;
    }
  }

  {
    auto key = v8Str(isolate, "context");
    auto value = v8Str(isolate, this->context);
    if (!TO(entry->Set(context, key, value), &success) && !success) {
      return json;
    }
  }

  json = JSONStringify(isolate, entry);
  return json;
}
