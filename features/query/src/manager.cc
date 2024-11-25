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

#include <memory>
#include <mutex>
#include <utility>

#include "conn-pool.h"
#include "info.h"
#include "isolate_data.h"
#include "js_exception.h"
#include "lcb_utils.h"
#include "query-helper.h"
#include "query-iterable.h"
#include "query-mgr.h"
#include "utils.h"
#include "v8worker2.h"
#include <nlohmann/json.hpp>

void Query::Manager::ClearQueries() {
  for (auto &iterator : iterators_) {
    iterator.second->Stop();
  }
  iterators_.clear();
}

Query::Iterable::Info
Query::Manager::NewIterable(std::unique_ptr<QueryController> query_controller) {
  auto conn_info = conn_pool_.GetConnection();
  if (conn_info.is_fatal) {
    return {true, conn_info.msg};
  }

  query_controller->AddDetails(conn_info.connection, on_behalf_of_);
  auto iterator =
      std::make_shared<Query::Iterator>(std::move(query_controller), isolate_);
  auto iterator_ptr = iterator.get();
  auto iterable = UnwrapData(isolate_)->query_iterable;
  auto info = iterable->NewObject(iterator_ptr);
  if (info.is_fatal) {
    conn_pool_.RestoreConnection(conn_info.connection);
    return {true, info.msg};
  }
  iterators_[conn_info.connection] = iterator;
  return {iterator, info.object};
}
