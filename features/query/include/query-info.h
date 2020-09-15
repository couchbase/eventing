// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

#ifndef QUERY_INFO_H
#define QUERY_INFO_H

#include <libcouchbase/couchbase.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <v8.h>
#include <vector>

#include "info.h"

namespace Query {
struct Options {
  class Extractor {
  public:
    Extractor(v8::Isolate *isolate, const v8::Local<v8::Context> &context);

    ~Extractor();

    Extractor() = delete;
    Extractor(const Extractor &) = delete;
    Extractor(Extractor &&) = delete;
    Extractor &operator=(const Extractor &) = delete;
    Extractor &operator=(Extractor &&) = delete;

    ::Info Extract(const v8::FunctionCallbackInfo<v8::Value> &args,
                   Options &opt_out) const;

  private:
    ::Info ExtractConsistency(
        const v8::Local<v8::Object> &options_obj,
        std::unique_ptr<lcb_QUERY_CONSISTENCY> &consistency_out) const;
    ::Info
    ExtractClientCtxId(const v8::Local<v8::Object> &options_obj,
                       std::unique_ptr<std::string> &client_ctx_id_out) const;
    ::Info ExtractIsPrepared(const v8::Local<v8::Object> &options_obj,
                             std::unique_ptr<bool> &is_prepared_out) const;

    v8::Isolate *isolate_;
    v8::Persistent<v8::Context> context_;
    const std::unordered_set<std::string> consistencies_{"none", "request"};
    v8::Persistent<v8::String> client_ctx_id_property_;
    v8::Persistent<v8::String> consistency_property_;
    v8::Persistent<v8::String> is_prepared_property_;
  };

  bool GetOrDefaultIsPrepared(v8::Isolate *isolate) const;
  lcb_QUERY_CONSISTENCY GetOrDefaultConsistency(v8::Isolate *isolate) const;

  std::unique_ptr<lcb_QUERY_CONSISTENCY> consistency;
  std::unique_ptr<std::string> client_context_id;
  std::unique_ptr<bool> is_prepared;
};

struct Info : public ::Info {
  Info() = default;
  Info(const bool is_fatal) : ::Info(is_fatal) {}
  Info(const bool is_fatal, const std::string &msg) : ::Info(is_fatal, msg) {}

  std::string query;
  std::unordered_map<std::string, std::string> named_params;
  std::vector<std::string> pos_params;
  Options options;
};

} // namespace Query

#endif
