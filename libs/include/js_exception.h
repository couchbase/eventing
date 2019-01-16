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

#ifndef JS_EXCEPTION_H
#define JS_EXCEPTION_H

#include <curl/curl.h>
#include <libcouchbase/couchbase.h>
#include <string>
#include <v8.h>
#include <vector>

#include "info.h"

class CustomError {
public:
  CustomError(v8::Isolate *isolate, const v8::Local<v8::Context> &context);
  ~CustomError();

  Info NewN1QLError(const v8::Local<v8::Value> &message_val,
                    v8::Local<v8::Object> &n1ql_error_out) {
    return NewCustomError(message_val, n1ql_error_out, "N1QLError");
  }
  Info NewKVError(const v8::Local<v8::Value> &message_val,
                  v8::Local<v8::Object> &kv_error_out) {
    return NewCustomError(message_val, kv_error_out, "KVError");
  }
  Info NewEventingError(const v8::Local<v8::Value> &message_val,
                        v8::Local<v8::Object> &eventing_error_out) {
    return NewCustomError(message_val, eventing_error_out, "EventingError");
  }
  Info NewCurlError(const v8::Local<v8::Value> &message_val,
                    v8::Local<v8::Object> &curl_error_out) {
    return NewCustomError(message_val, curl_error_out, "CurlError");
  }

private:
  Info NewCustomError(const v8::Local<v8::Value> &message_val,
                      v8::Local<v8::Object> &custom_error_out,
                      const std::string &type_name = "CustomError");

  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;
};

class JsException {
public:
  JsException() {}
  JsException(v8::Isolate *isolate);
  JsException(JsException &&exc_obj);

  // Need to overload '=' as we have members of v8::Persistent type.
  JsException &operator=(JsException &&exc_obj);

  void ThrowKVError(const std::string &err_msg);
  void ThrowKVError(lcb_t instance, lcb_error_t error);
  void ThrowN1QLError(const std::string &err_msg);
  void ThrowEventingError(const std::string &err_msg);
  void ThrowCurlError(const std::string &err_msg);

  ~JsException();

private:
  std::string ExtractErrorName(const std::string &error);
  void CopyMembers(JsException &&exc_obj);

  // Fields of the exception.
  const char *code_str_;
  const char *desc_str_;
  const char *name_str_;

  v8::Isolate *isolate_;
  v8::Persistent<v8::String> code_;
  v8::Persistent<v8::String> name_;
  v8::Persistent<v8::String> desc_;
};

Info DeriveFromError(v8::Isolate *isolate,
                     const v8::Local<v8::Context> &context,
                     const std::string &type_name);
void CustomErrorCtor(const v8::FunctionCallbackInfo<v8::Value> &args);

#endif
