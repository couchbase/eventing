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
#include "v8.h"

Transpiler::Transpiler(std::string transpiler_src) {
  isolate = v8::Isolate::GetCurrent();
  v8::EscapableHandleScope handle_scope(isolate);
  v8::Local<v8::ObjectTemplate> global = v8::ObjectTemplate::New(isolate);

  context = v8::Context::New(isolate, NULL, global);
  v8::Context::Scope context_scope(context);
  auto source = v8::String::NewFromUtf8(isolate, transpiler_src.c_str());
  auto script = v8::Script::Compile(context, source).ToLocalChecked();
  script->Run(context).ToLocalChecked();

  this->context = handle_scope.Escape(context);
}

v8::Local<v8::Value> Transpiler::ExecTranspiler(std::string code,
                                                std::string function) {
  v8::EscapableHandleScope handle_scope(isolate);
  v8::Context::Scope context_scope(context);
  auto function_name = v8::String::NewFromUtf8(isolate, function.c_str());
  auto function_def = context->Global()->Get(function_name);
  auto function_ref = v8::Local<v8::Function>::Cast(function_def);

  v8::Local<v8::Value> args[1];
  args[0] = v8::String::NewFromUtf8(isolate, code.c_str());
  auto result = function_ref->Call(function_ref, 1, args);

  return handle_scope.Escape(result);
}

std::string Transpiler::Transpile(std::string handler_code) {
  auto result = ExecTranspiler(handler_code, "transpile");
  v8::String::Utf8Value utf8result(result);

  return ToCString(utf8result);
}

std::string Transpiler::JsFormat(std::string handler_code) {
  auto result = ExecTranspiler(handler_code, "jsFormat");
  v8::String::Utf8Value utf8result(result);

  return ToCString(utf8result);
}

bool Transpiler::IsTimerCalled(std::string handler_code) {
  auto result = ExecTranspiler(handler_code, "isTimerCalled");
  auto bool_result = v8::Local<v8::Boolean>::Cast(result);

  return ToCBool(bool_result);
}
