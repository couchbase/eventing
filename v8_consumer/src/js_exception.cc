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

#include "js_exception.h"
#include "utils.h"

JsException::JsException(v8::Isolate *isolate) : isolate_(isolate) {
  code_str_ = "code";
  name_str_ = "name";
  desc_str_ = "desc";

  code_.Reset(isolate, v8Str(isolate, code_str_));
  desc_.Reset(isolate, v8Str(isolate, desc_str_));
  name_.Reset(isolate, v8Str(isolate, name_str_));
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
std::string JsException::ExtractErrorName(const std::string &error) {
  auto space_pos = error.find(" ");
  if (space_pos == std::string::npos) {
    return error;
  } else {
    return error.substr(0, space_pos);
  }
}

// Copies members from one object to another when an assignment is made.
void JsException::CopyMembers(JsException &&exc_obj) {
  std::swap(isolate_, exc_obj.isolate_);
  std::swap(code_str_, exc_obj.code_str_);
  std::swap(desc_str_, exc_obj.desc_str_);
  std::swap(name_str_, exc_obj.name_str_);

  code_.Reset(isolate_, v8Str(isolate_, code_str_));
  desc_.Reset(isolate_, v8Str(isolate_, desc_str_));
  name_.Reset(isolate_, v8Str(isolate_, name_str_));
}

// Composes exception message for curl related errors
void JsException::Throw(CURLcode res) {
  v8::HandleScope handle_scope(isolate_);

  auto code_name = code_.Get(isolate_);
  auto desc_name = desc_.Get(isolate_);

  auto code_value = v8::Number::New(isolate_, static_cast<int>(res));
  auto desc_value = v8Str(isolate_, curl_easy_strerror(res));

  auto exception = v8::Object::New(isolate_);
  exception->Set(code_name, code_value);
  exception->Set(desc_name, desc_value);

  isolate_->ThrowException(exception);
}

// Extracts the error message, composes an exception object and throws.
void JsException::Throw(lcb_t instance, lcb_error_t error) {
  v8::HandleScope handle_scope(isolate_);

  auto code_name = code_.Get(isolate_);
  auto desc_name = desc_.Get(isolate_);
  auto name_name = name_.Get(isolate_);

  auto code_value = v8::Number::New(isolate_, lcb_get_errtype(error));
  auto name_value =
      v8Str(isolate_, ExtractErrorName(lcb_strerror_short(error)).c_str());
  auto desc_value = v8Str(isolate_, lcb_strerror(instance, error));

  auto exception = v8::Object::New(isolate_);
  exception->Set(code_name, code_value);
  exception->Set(desc_name, desc_value);
  exception->Set(name_name, name_value);

  isolate_->ThrowException(exception);
}

// Extracts the error messages and aggregates, composes an exception object and
// throws.
void JsException::Throw(lcb_t instance, lcb_error_t error,
                        std::vector<std::string> error_msgs) {
  v8::HandleScope handle_scope(isolate_);

  auto code_name = code_.Get(isolate_);
  auto desc_name = desc_.Get(isolate_);
  auto name_name = name_.Get(isolate_);

  auto code_value = v8::Number::New(isolate_, lcb_get_errtype(error));
  auto name_value =
      v8Str(isolate_, ExtractErrorName(lcb_strerror_short(error)).c_str());

  auto desc_arr =
      v8::Array::New(isolate_, static_cast<int>(error_msgs.size() + 1));
  for (std::string::size_type i = 0; i < error_msgs.size(); ++i) {
    auto desc = v8Str(isolate_, error_msgs[i].c_str());
    desc_arr->Set(static_cast<uint32_t>(i), desc);
  }

  auto desc_value = v8Str(isolate_, lcb_strerror(instance, error));
  desc_arr->Set(static_cast<uint32_t>(error_msgs.size()), desc_value);

  auto exception = v8::Object::New(isolate_);
  exception->Set(code_name, code_value);
  exception->Set(desc_name, desc_arr);
  exception->Set(name_name, name_value);

  isolate_->ThrowException(exception);
}

// Throws an exception with just the message (typically used for those errors
// where code and name of exception isn't available)
void JsException::Throw(const std::string &message) {
  v8::HandleScope handle_scope(isolate_);

  auto desc_name = desc_.Get(isolate_);
  auto exception = v8::Object::New(isolate_);
  exception->Set(desc_name, v8Str(isolate_, message));

  isolate_->ThrowException(exception);
}

JsException::~JsException() {
  code_.Reset();
  desc_.Reset();
}
