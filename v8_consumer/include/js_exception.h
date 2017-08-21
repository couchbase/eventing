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

#include <libcouchbase/couchbase.h>
#include <string>
#include <v8.h>
#include <vector>

class JsException {
private:
  // Fields of the exception.
  const char *code_str;
  const char *desc_str;
  const char *name_str;

  v8::Isolate *isolate;
  v8::Persistent<v8::String> code;
  v8::Persistent<v8::String> name;
  v8::Persistent<v8::String> desc;

  std::string ExtractErrorName(std::string error);
  void CopyMembers(JsException &&exc_obj);

public:
  JsException() {}
  JsException(v8::Isolate *isolate);
  JsException(JsException &&exc_obj);

  // Need to overload '=' as we have members of v8::Persistent type.
  JsException &operator=(JsException &&exc_obj);

  void Throw(lcb_t instance, lcb_error_t error);
  void Throw(lcb_t instance, lcb_error_t error,
             std::vector<std::string> error_msgs);

  ~JsException();
};

#endif
