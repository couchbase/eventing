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

#include "../include/log.h"
#include "../include/n1ql.h"
#include "include/v8.h"

// Accepts transpiler.js source and user code as parameters and performs
// transpilation.
std::string Transpile(std::string js_src, std::string user_code, int mode) {
  std::string utf8Result;
  v8::Isolate *isolate = v8::Isolate::GetCurrent();
  {
    v8::Isolate::Scope isolate_scope(isolate);
    v8::HandleScope handle_scope(isolate);
    v8::Local<v8::ObjectTemplate> global = v8::ObjectTemplate::New(isolate);

    v8::Local<v8::Context> context = v8::Context::New(isolate, NULL, global);
    v8::Context::Scope context_scope(context);

    v8::Local<v8::String> source =
        v8::String::NewFromUtf8(isolate, js_src.c_str());

    v8::Local<v8::Script> script =
        v8::Script::Compile(context, source).ToLocalChecked();

    script->Run(context).ToLocalChecked();
    v8::Local<v8::String> function_name;
    switch (mode) {
    case EXEC_JS_FORMAT:
      // Format the given JavaScript code.
      function_name = v8::String::NewFromUtf8(isolate, "jsFormat");
      break;
    case EXEC_TRANSPILER:
      // Perform transpilation.
      function_name = v8::String::NewFromUtf8(isolate, "transpile");
    default:
      break;
    }

    v8::Local<v8::Value> function_def = context->Global()->Get(function_name);
    v8::Local<v8::Function> function_ref =
        v8::Local<v8::Function>::Cast(function_def);

    // Input source for transpilation.
    v8::Local<v8::Value> args[1];
    args[0] = v8::String::NewFromUtf8(isolate, user_code.c_str());

    v8::Local<v8::Value> function_result =
        function_ref->Call(function_ref, 1, args);

    v8::String::Utf8Value utf8(function_result);
    utf8Result = *utf8;
  }

  return utf8Result;
}
