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

#ifndef QUERY_HELPER_H
#define QUERY_HELPER_H

#include <string>
#include <unordered_map>
#include <utility>
#include <v8.h>
#include <vector>

#include "info.h"
#include "isolate_data.h"
#include "query-info.h"
#include "query-row.h"

namespace Query {
class N1qlController;
class AnalyticsController;

class Helper {
  friend N1qlController;
  friend AnalyticsController;

public:
  Helper(v8::Isolate *isolate, const v8::Local<v8::Context> &context);
  ~Helper();

  Helper(const Helper &) = delete;
  Helper(Helper &&) = delete;
  Helper &operator=(const Helper &) = delete;
  Helper &operator=(Helper &&) = delete;

  ::Info AccountLCBError(const std::string &err_str);
  void AccountLCBError(int err_code);
  bool CheckRetriable(int max_retry_count, uint32_t max_retry_secs,
                      int retry_count, uint32_t start_time);
  std::string RowErrorString(const Query::Row &row);
  std::string ErrorFormat(const std::string &message, lcb_INSTANCE *connection,
                          lcb_STATUS error);
  static lcb_QUERY_CONSISTENCY
  GetN1qlConsistency(const std::string &consistency);

  inline std::string GetQueryStatement(const v8::Local<v8::Value> &arg) {
    v8::HandleScope handle_scope(isolate_);
    Query::Info query_info;
    v8::String::Utf8Value query_utf8(isolate_, arg);
    return *query_utf8;
  }

  static lcb_ANALYTICS_CONSISTENCY
  GetAnalyticsConsistency(const std::string &consistency);

protected:
  struct ErrorCodesInfo : public ::Info {
    ErrorCodesInfo(const ::Info &info) : ::Info(info.is_fatal, info.msg) {}
    ErrorCodesInfo(bool is_fatal, const std::string &msg)
        : ::Info(is_fatal, msg) {}
    ErrorCodesInfo(std::vector<int64_t> &errors) : ::Info(false) {
      std::swap(this->err_codes, errors);
    }

    std::vector<int64_t> err_codes;
  };

  struct NamedParamsInfo : public ::Info {
    NamedParamsInfo(const bool is_fatal, const std::string &msg)
        : ::Info(is_fatal, msg) {}
    NamedParamsInfo(std::unordered_map<std::string, std::string> &named_params)
        : ::Info(false) {
      std::swap(this->named_params, named_params);
    }

    std::unordered_map<std::string, std::string> named_params;
  };

  struct PosParamsInfo : public ::Info {
    PosParamsInfo(const bool is_fatal, const std::string &msg)
        : ::Info(is_fatal, msg) {}
    PosParamsInfo(std::vector<std::string> &pos_params) : ::Info(false) {
      std::swap(this->pos_params, pos_params);
    }

    std::vector<std::string> pos_params;
  };

  NamedParamsInfo GetN1qlNamedParams(const v8::Local<v8::Value> &arg) const;
  PosParamsInfo GetN1qlPosParams(const v8::Local<v8::Value> &arg) const;
  NamedParamsInfo
  GetAnalyticsNamedParams(const v8::Local<v8::Value> &arg) const;
  PosParamsInfo GetAnalyticsPosParams(const v8::Local<v8::Value> &arg) const;

  Options::Extractor opt_extractor_;

private:
  ErrorCodesInfo GetErrorCodes(const std::string &error);
  ErrorCodesInfo GetErrorCodes(const v8::Local<v8::Value> &errors_val);

  v8::Isolate *isolate_;
  v8::Global<v8::Context> context_;
};
} // namespace Query

void AddLcbException(const IsolateData *isolate_data, int code);

#endif
