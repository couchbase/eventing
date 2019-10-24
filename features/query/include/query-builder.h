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

#ifndef QUERY_BUILDER_H
#define QUERY_BUILDER_H

#include <libcouchbase/couchbase.h>
#include <libcouchbase/n1ql.h>
#include <string>
#include <v8.h>

#include "info.h"
#include "isolate_data.h"
#include "query-helper.h"

namespace Query {
class Builder {
public:
  Builder(v8::Isolate *isolate, Query::Info query_info, lcb_t connection)
      : isolate_(isolate), params_(lcb_n1p_new()),
        query_info_(std::move(query_info)), connection_(connection),
        timeout_(UnwrapData(isolate)->n1ql_timeout) {}
  ~Builder() { lcb_n1p_free(params_); }

  Builder(const Builder &) = delete;
  Builder(Builder &&) = delete;
  Builder &operator=(const Builder &) = delete;
  Builder &operator=(Builder &&) = delete;

  ::Info Build(void (*row_callback)(lcb_t, int, const lcb_RESPN1QL *),
               void *cookie);
  lcb_CMDN1QL *GetCmd() { return &cmd_; }
  lcb_N1QLHANDLE GetHandle() const { return handle_; }

private:
  ::Info ErrorFormat(const std::string &message, lcb_t connection,
                     lcb_error_t error) const;

  v8::Isolate *isolate_{nullptr};
  lcb_CMDN1QL cmd_{0};
  lcb_N1QLPARAMS *params_{nullptr};
  lcb_N1QLHANDLE handle_{nullptr};
  Query::Info query_info_;
  lcb_t connection_{nullptr};
  lcb_U32 timeout_{0};
};
} // namespace Query

#endif // QUERY_BUILDER_H
