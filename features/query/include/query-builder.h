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
#include <string>
#include <v8.h>

#include "info.h"
#include "isolate_data.h"
#include "query-helper.h"

namespace Query {
const lcb_U32 n1ql_grace_period = 2000000;
class Builder {
public:
  Builder(v8::Isolate *isolate, Query::Info query_info,
          lcb_INSTANCE *connection)
      : isolate_(isolate), query_info_(std::move(query_info)),
        connection_(connection), timeout_(UnwrapData(isolate)->n1ql_timeout) {}
  ~Builder() { lcb_cmdquery_destroy(cmd_); }

  Builder(const Builder &) = delete;
  Builder(Builder &&) = delete;
  Builder &operator=(const Builder &) = delete;
  Builder &operator=(Builder &&) = delete;

  ::Info Build(void (*row_callback)(lcb_INSTANCE *, int, const lcb_RESPQUERY *),
               void *cookie);
  lcb_CMDQUERY *GetCmd() { return cmd_; }
  lcb_QUERY_HANDLE *GetHandle() const { return handle_; }

private:
  ::Info ErrorFormat(const std::string &message, lcb_INSTANCE *connection,
                     lcb_STATUS error) const;

  v8::Isolate *isolate_{nullptr};
  lcb_CMDQUERY *cmd_{0};
  lcb_QUERY_HANDLE *handle_{nullptr};
  Query::Info query_info_;
  lcb_INSTANCE *connection_{nullptr};
  lcb_U32 timeout_{0};
};
} // namespace Query

#endif // QUERY_BUILDER_H
