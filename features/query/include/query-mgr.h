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

#ifndef QUERY_MGR_H
#define QUERY_MGR_H

#include <libcouchbase/couchbase.h>
#include <platform/base64.h>
#include <string>
#include <unordered_map>
#include <v8.h>

#include "conn-pool.h"
#include "query-helper.h"
#include "query-iterable.h"
#include "utils.h"

namespace Query {
class Manager {
public:
  explicit Manager(v8::Isolate *isolate, const std::string &src_bucket,
                   const std::size_t pool_size, std::string user,
                   std::string domain)
      : isolate_(isolate), conn_pool_(pool_size, src_bucket, isolate) {
    if (user.size() != 0 || domain.size() != 0) {
      on_behalf_of_ = cb::base64::encode(user + ":" + domain);
    } else {
      on_behalf_of_ = "";
    }
  }
  ~Manager() { ClearQueries(); }

  Manager() = delete;
  Manager(Manager &&) = delete;
  Manager(const Manager &) = delete;
  Manager &operator=(const Manager &) = delete;
  Manager &operator=(Manager &&) = delete;

  Iterable::Info NewIterable(std::unique_ptr<QueryController> query_controller);
  void ClearQueries();
  void RestoreConnection(lcb_INSTANCE *connection) {
    conn_pool_.RestoreConnection(connection);
  }
  void RefreshTopConnection() { conn_pool_.RefreshTopConnection(); }

private:
  v8::Isolate *isolate_;
  Connection::Pool conn_pool_;
  std::unordered_map<lcb_INSTANCE *, std::shared_ptr<Iterator>> iterators_;
  std::string on_behalf_of_;
};

} // namespace Query

#endif
