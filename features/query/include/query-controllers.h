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

#ifndef QUERY_CONTROLLERS_H
#define QUERY_CONTROLLERS_H

#include <libcouchbase/couchbase.h>
#include <string>
#include <v8.h>

#include "log.h"
#include "query-info.h"

namespace Query {

const lcb_U32 n1ql_grace_period = 2'000'000;

class QueryController {
public:
  explicit QueryController(v8::Isolate *isolate, Query::Info query_info)
      : isolate_(isolate), query_info_(std::move(query_info)) {}
  virtual ~QueryController() {}

  QueryController() = delete;
  QueryController(QueryController &&) = delete;
  QueryController(const QueryController &) = delete;
  QueryController &operator=(QueryController &&) = delete;
  QueryController &operator=(const QueryController &) = delete;

  void AddDetails(lcb_INSTANCE *connection,
                  const std::string &on_behalf_of) {
    connection_ = connection;
    on_behalf_of_.assign(on_behalf_of);
  }

  virtual ::Info build(void *cookie) = 0;
  virtual lcb_STATUS run() = 0;
  virtual void cancel() = 0;
  virtual void ThrowQueryError(const std::string &err_msg) = 0;

protected:
  v8::Isolate *isolate_;
  Query::Info query_info_;
  lcb_INSTANCE *connection_{nullptr};
  std::string on_behalf_of_;
};

class N1qlController : public QueryController {
public:
  explicit N1qlController(v8::Isolate *isolate, Query::Info query_info_)
      : QueryController(isolate, std::move(query_info_)) {}
  ~N1qlController() {}

  static ::Info ValidateQuery(const v8::FunctionCallbackInfo<v8::Value> &args);
  static Query::Info
  GetQueryInfo(Query::Helper *helper,
               const v8::FunctionCallbackInfo<v8::Value> &args);

  ::Info build(void *cookie);
  lcb_STATUS run();
  void cancel();
  void ThrowQueryError(const std::string &err_msg);

private:
  ::Info ErrorFormat(const std::string &message, lcb_INSTANCE *connection,
                     lcb_STATUS error) const;
  ::Info do_build(void *cookie);
  static void RowCallback(lcb_INSTANCE *connection, int,
                          const lcb_RESPQUERY *resp);
  static bool IsStatusSuccess(const std::string &row);

  lcb_CMDQUERY *GetCmd() { return cmd_; }
  lcb_QUERY_HANDLE *GetHandle() const { return handle_; }

  lcb_CMDQUERY *cmd_{0};
  lcb_QUERY_HANDLE *handle_{nullptr};
};
void N1qlFunction(const v8::FunctionCallbackInfo<v8::Value> &args);

} // namespace Query

#endif // QUERY_CONTROLLERS_H
