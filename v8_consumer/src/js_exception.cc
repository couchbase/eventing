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

#include "../include/js_exception.h"

JsException::JsException(v8::Isolate *isolate) : isolate(isolate) {
  code_str = "code";
  name_str = "name";
  desc_str = "desc";

  code.Reset(isolate, v8::String::NewFromUtf8(isolate, code_str));
  desc.Reset(isolate, v8::String::NewFromUtf8(isolate, desc_str));
  name.Reset(isolate, v8::String::NewFromUtf8(isolate, name_str));
}

JsException::JsException(JsException &&exc_obj) {
  CopyMembers(std::move(exc_obj));
}

JsException &JsException::operator=(JsException &&exc_obj) {
  // This check prevents copying when the object is assigned to itself.
  if (this != &exc_obj) {
    CopyMembers(std::move(exc_obj));
  }

  return *this;
}

// Extracts the error name: LCB_HTTP_ERROR (0x0D) -> LCB_HTTP_ERROR
std::string JsException::ExtractErrorName(std::string error) {
  auto space_pos = error.find(" ");
  if (space_pos == std::string::npos) {
    return error;
  } else {
    return error.substr(0, space_pos);
  }
}

// Copies members from one object to another when an assignment is made.
void JsException::CopyMembers(JsException &&exc_obj) {
  std::swap(isolate, exc_obj.isolate);
  std::swap(code_str, exc_obj.code_str);
  std::swap(desc_str, exc_obj.desc_str);
  std::swap(name_str, exc_obj.name_str);

  code.Reset(isolate, v8::String::NewFromUtf8(isolate, code_str));
  desc.Reset(isolate, v8::String::NewFromUtf8(isolate, desc_str));
  name.Reset(isolate, v8::String::NewFromUtf8(isolate, name_str));
}

// Composes exception message for curl related errors
void JsException::Throw(CURLcode res) {
  v8::HandleScope handle_scope(isolate);

  auto code_name = code.Get(isolate);
  auto desc_name = desc.Get(isolate);

  auto code_value = v8::Number::New(isolate, static_cast<int>(res));
  auto desc_value = v8::String::NewFromUtf8(isolate, curl_easy_strerror(res));

  auto exception = v8::Object::New(isolate);
  exception->Set(code_name, code_value);
  exception->Set(desc_name, desc_value);

  isolate->ThrowException(exception);
}

// Extracts the error message, composes an exception object and throws.
void JsException::Throw(lcb_t instance, lcb_error_t error) {
  v8::HandleScope handle_scope(isolate);

  auto code_name = code.Get(isolate);
  auto desc_name = desc.Get(isolate);
  auto name_name = name.Get(isolate);

  auto code_value = v8::Number::New(isolate, lcb_get_errtype(error));
  auto name_value = v8::String::NewFromUtf8(
      isolate, ExtractErrorName(lcb_strerror_short(error)).c_str());
  v8::Local<v8::String> desc_value;
  desc_value = v8::String::NewFromUtf8(isolate, lcb_strerror(instance, error));

  auto exception = v8::Object::New(isolate);
  exception->Set(code_name, code_value);
  exception->Set(desc_name, desc_value);
  exception->Set(name_name, name_value);

  isolate->ThrowException(exception);
}

// Extracts the error messages and aggregates, composes an exception object and
// throws.
void JsException::Throw(lcb_t instance, lcb_error_t error,
                        std::vector<std::string> error_msgs) {
  v8::HandleScope handle_scope(isolate);

  auto code_name = code.Get(isolate);
  auto desc_name = desc.Get(isolate);
  auto name_name = name.Get(isolate);

  auto code_value = v8::Number::New(isolate, lcb_get_errtype(error));
  auto name_value = v8::String::NewFromUtf8(
      isolate, ExtractErrorName(lcb_strerror_short(error)).c_str());

  auto desc_arr =
      v8::Array::New(isolate, static_cast<int>(error_msgs.size() + 1));
  for (std::string::size_type i = 0; i < error_msgs.size(); ++i) {
    auto desc = v8::String::NewFromUtf8(isolate, error_msgs[i].c_str());
    desc_arr->Set(static_cast<uint32_t>(i), desc);
  }

  v8::Local<v8::String> desc_value;
  desc_value = v8::String::NewFromUtf8(isolate, lcb_strerror(instance, error));
  desc_arr->Set(static_cast<uint32_t>(error_msgs.size()), desc_value);

  auto exception = v8::Object::New(isolate);
  exception->Set(code_name, code_value);
  exception->Set(desc_name, desc_arr);
  exception->Set(name_name, name_value);

  isolate->ThrowException(exception);
}

JsException::~JsException() {
  code.Reset();
  desc.Reset();
}
