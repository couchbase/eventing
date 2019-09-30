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
#include "isolate_data.h"
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

// Extracts the error message, composes an exception object and throws.
void JsException::ThrowKVError(lcb_t instance, lcb_error_t error) {
  v8::HandleScope handle_scope(isolate_);

  auto code_name = code_.Get(isolate_);
  auto desc_name = desc_.Get(isolate_);
  auto name_name = name_.Get(isolate_);

  auto code_value = v8::Number::New(isolate_, lcb_get_errtype(error));
  auto name_value =
      v8Str(isolate_, ExtractErrorName(lcb_strerror_short(error)).c_str());
  auto desc_value = v8Str(isolate_, lcb_strerror(instance, error));

  auto message = v8::Object::New(isolate_);
  message->Set(code_name, code_value);
  message->Set(desc_name, desc_value);
  message->Set(name_name, name_value);

  auto custom_error = UnwrapData(isolate_)->custom_error;
  v8::Local<v8::Object> error_obj;
  auto info = custom_error->NewKVError(message, error_obj);
  if (info.is_fatal) {
    LOG(logError) << "Unable to construct KVError : " << info.msg << std::endl;
    isolate_->ThrowException(message);
    return;
  }
  isolate_->ThrowException(error_obj);
}

void JsException::ThrowKVError(const std::string &err_msg) {
  v8::HandleScope handle_scope(isolate_);
  auto custom_error = UnwrapData(isolate_)->custom_error;

  v8::Local<v8::Object> error_obj;
  auto info = custom_error->NewKVError(v8Str(isolate_, err_msg), error_obj);
  if (info.is_fatal) {
    LOG(logError) << "Unable to construct KVError : " << info.msg << std::endl;
    isolate_->ThrowException(v8Str(isolate_, err_msg));
    return;
  }
  isolate_->ThrowException(error_obj);
}

void JsException::ThrowN1QLError(const std::string &err_msg) {
  v8::HandleScope handle_scope(isolate_);
  auto custom_error = UnwrapData(isolate_)->custom_error;

  v8::Local<v8::Object> error_obj;
  auto info = custom_error->NewN1QLError(v8Str(isolate_, err_msg), error_obj);
  if (info.is_fatal) {
    LOG(logError) << "Unable to construct N1QLError : " << info.msg
                  << std::endl;
    isolate_->ThrowException(v8Str(isolate_, err_msg));
    return;
  }
  isolate_->ThrowException(error_obj);
}

void JsException::ThrowEventingError(const std::string &err_msg) {
  v8::HandleScope handle_scope(isolate_);
  auto custom_error = UnwrapData(isolate_)->custom_error;

  v8::Local<v8::Object> error_obj;
  auto info =
      custom_error->NewEventingError(v8Str(isolate_, err_msg), error_obj);
  if (info.is_fatal) {
    LOG(logError) << "Unable to construct EventingError : " << info.msg
                  << std::endl;
    isolate_->ThrowException(v8Str(isolate_, err_msg));
    return;
  }
  isolate_->ThrowException(error_obj);
}

void JsException::ThrowCurlError(const std::string &err_msg) {
  v8::HandleScope handle_scope(isolate_);
  auto custom_error = UnwrapData(isolate_)->custom_error;

  v8::Local<v8::Object> error_obj;
  auto info = custom_error->NewCurlError(v8Str(isolate_, err_msg), error_obj);
  if (info.is_fatal) {
    LOG(logError) << "Unable to construct CurlError : " << info.msg
                  << std::endl;
    isolate_->ThrowException(v8Str(isolate_, err_msg));
    return;
  }
  isolate_->ThrowException(error_obj);
}

JsException::~JsException() {
  code_.Reset();
  desc_.Reset();
}

CustomError::CustomError(v8::Isolate *isolate,
                         const v8::Local<v8::Context> &context)
    : isolate_(isolate) {
  context_.Reset(isolate_, context);
}

CustomError::~CustomError() { context_.Reset(); }

Info CustomError::NewCustomError(const v8::Local<v8::Value> &message_val,
                                 v8::Local<v8::Object> &custom_error_out,
                                 const std::string &type_name) {
  v8::EscapableHandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  auto global = context->Global();

  v8::Local<v8::Value> custom_error_val;
  if (!TO_LOCAL(global->Get(context, v8Str(isolate_, type_name)),
                &custom_error_val)) {
    return {true, "Unable to get " + type_name + " from global"};
  }

  auto custom_error_func = custom_error_val.As<v8::Function>();
  v8::Local<v8::Value> args[1];
  args[0] = message_val;

  v8::Local<v8::Object> custom_error_obj;
  if (!TO_LOCAL(custom_error_func->NewInstance(context, 1, args),
                &custom_error_obj)) {
    return {true, "Unable to instantiate " + type_name};
  }
  custom_error_out = handle_scope.Escape(custom_error_obj);
  return {false};
}

void CustomErrorCtor(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  auto result = false;
  auto this_obj = args.This();

  if (args.Length() > 0) {
    auto message_val = args[0];
    if (!TO(this_obj->Set(context, v8Str(isolate, "message"), message_val),
            &result) ||
        !result) {
      return;
    }
  }

  auto utils = UnwrapData(isolate)->utils;
  auto error_val = utils->GetPropertyFromGlobal("Error");
  auto error_func = error_val.As<v8::Function>();

  v8::Local<v8::Object> error_obj;
  if (!TO_LOCAL(error_func->NewInstance(context), &error_obj)) {
    return;
  }

  v8::Local<v8::Value> stack_val;
  if (!TO_LOCAL(error_obj->Get(context, v8Str(isolate, "stack")), &stack_val)) {
    return;
  }

  if (!TO(this_obj->Set(context, v8Str(isolate, "stack"), stack_val),
          &result) ||
      !result) {
    return;
  }
}

Info DeriveFromError(v8::Isolate *isolate,
                     const v8::Local<v8::Context> &context,
                     const std::string &type_name) {
  v8::HandleScope handle_scope(isolate);
  auto utils = UnwrapData(isolate)->utils;
  auto global = context->Global();

  auto error_val = utils->GetPropertyFromGlobal("Error");
  v8::Local<v8::Value> custom_error_val;
  if (!TO_LOCAL(global->Get(context, v8Str(isolate, type_name)),
                &custom_error_val)) {
    return {true, "Unable to get " + type_name + "from global"};
  }

  auto error_func = error_val.As<v8::Function>();
  v8::Local<v8::Value> error_prototype_val;
  if (!TO_LOCAL(error_func->Get(context, v8Str(isolate, "prototype")),
                &error_prototype_val)) {
    return {true, "Unable to get prototype of Error"};
  }

  v8::Local<v8::Object> object_obj;
  if (!TO_LOCAL(utils->GetPropertyFromGlobal("Object")->ToObject(context),
                &object_obj)) {
    return {true, "Unable to read Object"};
  }

  auto create_func =
      utils->GetPropertyFromObject(object_obj, "create").As<v8::Function>();
  v8::Local<v8::Value> error_prototype_clone_val;
  if (!TO_LOCAL(create_func->Call(context, global, 1, &error_prototype_val),
                &error_prototype_clone_val)) {
    return {true, "Unable to call clone Error prototype"};
  }

  auto result = false;
  auto custom_error_func = custom_error_val.As<v8::Function>();
  if (!TO(custom_error_func->Set(context, v8Str(isolate, "prototype"),
                                 error_prototype_clone_val),
          &result) ||
      !result) {
    return {true, "Unable to set prototype on " + type_name + " class"};
  }
  return {false};
}
