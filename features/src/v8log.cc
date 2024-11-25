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

#include "insight.h"
#include "isolate_data.h"
#include "utils.h"
#include "v8worker2.h"

const auto ConsoleLogMaxArity = 20;

void Log(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  v8::Locker locker(isolate);
  auto location = UnwrapData(isolate)->instance_id;
  auto v8worker = UnwrapData(isolate)->v8worker2;
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();
  std::string log_msg;

  for (auto i = 0; i < args.Length(); i++) {
    if (args[i]->IsNativeError()) {
      v8::Local<v8::Object> object;
      if (!TO_LOCAL(args[i]->ToObject(context), &object)) {
        return;
      }

      v8::Local<v8::Value> to_string_val;
      if (!TO_LOCAL(object->Get(context, v8Str(isolate, "toString")),
                    &to_string_val)) {
        return;
      }

      auto to_string_func = to_string_val.As<v8::Function>();
      v8::Local<v8::Value> stringified_val;
      if (!TO_LOCAL(to_string_func->Call(context, object, 0, nullptr),
                    &stringified_val)) {
        return;
      }

      log_msg += JSONStringify(isolate, stringified_val);
    } else {
      log_msg += JSONStringify(isolate, args[i]);
    }

    log_msg += " ";
  }

  std::ostringstream ss;
  auto locationSize = location.length();
  auto msgSize = log_msg.length();

  ss << static_cast<uint8_t>(locationSize >> 8)
     << static_cast<uint8_t>(locationSize);
  ss << static_cast<uint8_t>(msgSize >> 24)
     << static_cast<uint8_t>(msgSize >> 16)
     << static_cast<uint8_t>(msgSize >> 8) << static_cast<uint8_t>(msgSize);

  APPLOG << ss.str() << location << log_msg << std::flush;
  v8worker->AccumulateLog(log_msg);
}

// console.log for debugger - also logs to eventing.log
void ConsoleLog(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::Locker locker(isolate);
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  Log(args);
  auto console_v8_str =
      v8::String::NewFromUtf8(isolate, "console").ToLocalChecked();
  auto log_v8_str = v8::String::NewFromUtf8(isolate, "log").ToLocalChecked();
  auto console = context->Global()
                     ->Get(context, console_v8_str)
                     .ToLocalChecked()
                     ->ToObject(context)
                     .ToLocalChecked();
  auto log_fn = v8::Local<v8::Function>::Cast(
      console->Get(context, log_v8_str).ToLocalChecked());

  v8::Handle<v8::Value> result;
  v8::Local<v8::Value> log_args[ConsoleLogMaxArity];
  auto i = 0;
  for (; i < args.Length() && i < ConsoleLogMaxArity; ++i) {
    log_args[i] = args[i];
  }

  // Calling console.log with the args passed to log() function.
  if (i < ConsoleLogMaxArity) {
    if (!TO_LOCAL(log_fn->Call(context, log_fn, args.Length(), log_args),
                  &result))
      return;
  } else {
    if (!TO_LOCAL(log_fn->Call(context, log_fn, ConsoleLogMaxArity, log_args),
                  &result))
      return;
  }
}
