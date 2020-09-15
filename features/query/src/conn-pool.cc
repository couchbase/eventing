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

#include <mutex>
#include <sstream>
#include <string.h>

#include "comm.h"
#include "conn-pool.h"
#include "isolate_data.h"
#include "lcb_utils.h"
#include "query-helper.h"
#include "utils.h"

Connection::Info Connection::Pool::CreateConnection() const {
  auto utils = UnwrapData(isolate_)->utils;
  auto conn_str_info = utils->GetConnectionString(src_bucket_);
  if (!conn_str_info.is_valid) {
    return {true, conn_str_info.msg};
  }

  std::stringstream error;
  lcb_CREATEOPTS *options;
  lcb_createopts_create(&options, LCB_TYPE_BUCKET);
  lcb_createopts_connstr(options, conn_str_info.conn_str.c_str(),
                         strlen(conn_str_info.conn_str.c_str()));

  lcb_INSTANCE *connection;
  auto result = lcb_create(&connection, options);
  if (result != LCB_SUCCESS) {
    return FormatErrorAndDestroyConn("Unable to initialize Couchbase handle",
                                     connection, result);
  }

  lcb_createopts_destroy(options);
  // TODO: loging for lcb
  /*
    result = lcb_cntl(connection, LCB_CNTL_SET, LCB_CNTL_LOGGER, &evt_logger);
    if (result != LCB_SUCCESS) {
      return FormatErrorAndDestroyConn("Unable to set libcouchbase logger
    hooks", connection, result);
    }
  */

  auto auth = lcbauth_new();
  result = lcbauth_set_callbacks(auth, isolate_, GetUsername, GetPassword);
  if (result != LCB_SUCCESS) {
    return FormatErrorAndDestroyConn("Unable to set auth callbacks", connection,
                                     result);
  }

  result = lcbauth_set_mode(auth, LCBAUTH_MODE_DYNAMIC);
  if (result != LCB_SUCCESS) {
    return FormatErrorAndDestroyConn("Unable to set auth mode to dynamic",
                                     connection, result);
  }

  lcb_set_auth(connection, auth);

  result = lcb_connect(connection);
  if (result != LCB_SUCCESS) {
    return FormatErrorAndDestroyConn("Unable to schedule connection",
                                     connection, result);
  }

  result = lcb_wait(connection, LCB_WAIT_DEFAULT);
  if (result != LCB_SUCCESS) {
    return FormatErrorAndDestroyConn("Unable to connect", connection, result);
  }

  result = lcb_get_bootstrap_status(connection);
  if (result != LCB_SUCCESS) {
    return FormatErrorAndDestroyConn("Unable to bootstrap connection",
                                     connection, result);
  }
  return {connection};
}

Connection::Info
Connection::Pool::FormatErrorAndDestroyConn(const std::string &message,
                                            lcb_INSTANCE *connection,
                                            const lcb_STATUS error) const {
  auto helper = UnwrapData(isolate_)->query_helper;
  auto info =
      Connection::Info{true, helper->ErrorFormat(message, connection, error)};
  lcb_destroy(connection);
  return info;
}

Connection::Info Connection::Pool::GetConnection() {
  std::lock_guard<std::mutex> lock(pool_sync_);

  if (pool_.empty()) {
    if (current_size_ < capacity_) {
      if (auto info = CreateConnection(); info.is_fatal) {
        return info;
      } else {
        pool_.push_front(info.connection);
        ++current_size_;
      }
    } else {
      return {true, "Connection pool maximum capacity reached"};
    }
  }

  auto connection = pool_.front();
  pool_.pop_front();
  return {connection};
}

void Connection::Pool::DestroyAllConnectionsInPoolLocked() {
  while (!pool_.empty()) {
    auto handle = pool_.front();
    pool_.pop_front();
    lcb_destroy(handle);
    --current_size_;
  }
}

Connection::Pool::~Pool() { DestroyAllConnectionsInPoolLocked(); }

void Connection::Pool::RestoreConnection(lcb_INSTANCE *connection) {
  std::lock_guard<std::mutex> lock(pool_sync_);
  pool_.push_front(connection);
}

void Connection::Pool::RefreshTopConnection() {
  std::lock_guard<std::mutex> lock(pool_sync_);
  if (!pool_.empty()) {
    auto handle = pool_.front();
    pool_.pop_front();
    lcb_destroy(handle);
    --current_size_;

    if (auto info = CreateConnection(); info.is_fatal) {
      DestroyAllConnectionsInPoolLocked();
    } else {
      pool_.push_front(info.connection);
      ++current_size_;
    }
  }
}
