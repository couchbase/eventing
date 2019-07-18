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
class CodeInsight;
struct CurlCodex;

namespace Query {
class Manager;
class Iterable;
class IterableImpl;
class IterableResult;
class Helper;
} // namespace Query

// Struct for storing isolate data
struct IsolateData {
  static const uint32_t index{0};
  lcb_U32 n1ql_timeout{0};
  long curl_timeout{0};

  Query::Manager *query_mgr{nullptr};
  Query::Iterable *query_iterable{nullptr};
  Query::IterableImpl *query_iterable_impl{nullptr};
  Query::IterableResult *query_iterable_result{nullptr};
  Query::Helper *query_helper{nullptr};
  N1QL *n1ql_handle{nullptr};
  V8Worker *v8worker{nullptr};
  JsException *js_exception{nullptr};
  Communicator *comm{nullptr};
  Transpiler *transpiler{nullptr};
  Utils *utils{nullptr};
  Timer *timer{nullptr};
  CustomError *custom_error{nullptr};
  CurlCodex *curl_codex{nullptr};
  CurlFactory *curl_factory{nullptr};
  CurlRequestBuilder *req_builder{nullptr};
  CurlResponseBuilder *resp_builder{nullptr};
  CodeInsight *code_insight{nullptr};
};

inline IsolateData *UnwrapData(v8::Isolate *isolate) {
  return reinterpret_cast<IsolateData *>(isolate->GetData(IsolateData::index));
}

#endif
