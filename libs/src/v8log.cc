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

#include "utils.h"

const auto ConsoleLogMaxArity = 20;

void Log(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::Locker locker(isolate);
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

  APPLOG << log_msg << std::endl;
}

// console.log for debugger - also logs to eventing.log
void ConsoleLog(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::Locker locker(isolate);
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  Log(args);
  auto console_v8_str = v8::String::NewFromUtf8(isolate, "console");
  auto log_v8_str = v8::String::NewFromUtf8(isolate, "log");
  auto console = context->Global()
                     ->Get(console_v8_str)
                     ->ToObject(context)
                     .ToLocalChecked();
  auto log_fn = v8::Local<v8::Function>::Cast(console->Get(log_v8_str));

  v8::Local<v8::Value> log_args[ConsoleLogMaxArity];
  auto i = 0;
  for (; i < args.Length() && i < ConsoleLogMaxArity; ++i) {
    log_args[i] = args[i];
  }

  // Calling console.log with the args passed to log() function.
  if (i < ConsoleLogMaxArity) {
    log_fn->Call(log_fn, args.Length(), log_args);
  } else {
    log_fn->Call(log_fn, ConsoleLogMaxArity, log_args);
  }
}
