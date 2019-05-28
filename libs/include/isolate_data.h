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

#ifndef ISOLATE_DATA_H
#define ISOLATE_DATA_H

#include <libcouchbase/sysdefs.h>
#include <v8.h>

struct N1QLCodex;
class Curl;
class N1QL;
class V8Worker;
class JsException;
class Transpiler;
class Utils;
class Timer;
class CustomError;
class CurlFactory;
class CurlRequestBuilder;
class CurlResponseBuilder;
class Communicator;
struct CurlCodex;

// Struct for storing isolate data
struct IsolateData {
  IsolateData()
      : n1ql_codex(nullptr), n1ql_handle(nullptr), v8worker(nullptr),
        js_exception(nullptr), comm(nullptr), transpiler(nullptr),
        utils(nullptr), timer(nullptr), custom_error(nullptr),
        curl_codex(nullptr), curl_factory(nullptr), req_builder(nullptr),
        resp_builder(nullptr) {}

  static const uint32_t index{0};
  lcb_U32 n1ql_timeout{0};

  N1QLCodex *n1ql_codex;
  N1QL *n1ql_handle;
  V8Worker *v8worker;
  JsException *js_exception;
  Communicator *comm;
  Transpiler *transpiler;
  Utils *utils;
  Timer *timer;
  CustomError *custom_error;
  CurlCodex *curl_codex;
  CurlFactory *curl_factory;
  CurlRequestBuilder *req_builder;
  CurlResponseBuilder *resp_builder;
};

inline IsolateData *UnwrapData(v8::Isolate *isolate) {
  return reinterpret_cast<IsolateData *>(isolate->GetData(IsolateData::index));
}

#endif
