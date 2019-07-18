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

#include <sstream>

#include "comm.h"
#include "conn-pool.h"
#include "isolate_data.h"
#include "log.h"
#include "utils.h"

const char *GetUsernameCached(void *cookie, const char *host, const char *port,
                              const char *bucket) {
  auto isolate = static_cast<v8::Isolate *>(cookie);
  auto comm = UnwrapData(isolate)->comm;
  auto endpoint = JoinHostPort(host, port);
  auto info = comm->GetCredsCached(endpoint);
  if (!info.is_valid) {
    LOG(logError) << "Failed to get username for " << RS(host) << ":" << port
                  << " err: " << info.msg << std::endl;
  }

  static const char *username = "";
  if (info.username != username) {
    username = strdup(info.username.c_str());
  }

  return username;
}

const char *GetPasswordCached(void *cookie, const char *host, const char *port,
                              const char *bucket) {
  auto isolate = static_cast<v8::Isolate *>(cookie);
  auto comm = UnwrapData(isolate)->comm;
  auto endpoint = JoinHostPort(host, port);
  auto info = comm->GetCredsCached(endpoint);
  if (!info.is_valid) {
    LOG(logError) << "Failed to get password for " << RS(host) << ":" << port
                  << " err: " << info.msg << std::endl;
  }

  static const char *password = "";
  if (info.password != password) {
    password = strdup(info.password.c_str());
  }

  return password;
}

Info Connection::Pool::Initialize() {
  for (std::size_t i = 0; i < capacity_; ++i) {
    auto info = CreateConnection();
    if (info.is_fatal) {
      return {true, info.msg};
    }
    pool_.push(info.connection);
  }
  return {false};
}

Connection::Info Connection::Pool::CreateConnection() {
  std::stringstream error;
  lcb_create_st options = {nullptr};
  options.version = 3;
  options.v.v3.connstr = conn_str_.c_str();
  options.v.v3.type = LCB_TYPE_BUCKET;

  lcb_t connection = nullptr;
  auto result = lcb_create(&connection, &options);
  if (result != LCB_SUCCESS) {
    error << "Unable to initialize Couchbase handle : "
          << lcb_strerror(connection, result) << std::endl;
    return {true, error.str()};
  }

  auto auth = lcbauth_new();
  result = lcbauth_set_callbacks(auth, isolate_, GetUsernameCached,
                                 GetPasswordCached);
  if (result != LCB_SUCCESS) {
    error << "Unable to set auth callbacks" << lcb_strerror(connection, result)
          << std::endl;
    return {true, error.str()};
  }

  result = lcbauth_set_mode(auth, LCBAUTH_MODE_DYNAMIC);
  if (result != LCB_SUCCESS) {
    error << "Unable to set auth mode to dynamic"
          << lcb_strerror(connection, result) << std::endl;
    return {true, error.str()};
  }

  lcb_set_auth(connection, auth);

  result = lcb_connect(connection);
  if (result != LCB_SUCCESS) {
    error << "Unable to schedule connection : "
          << lcb_strerror(connection, result) << std::endl;
    return {true, error.str()};
  }

  result = lcb_wait(connection);
  if (result != LCB_SUCCESS) {
    error << "Unable to connect : " << lcb_strerror(connection, result)
          << std::endl;
    return {true, error.str()};
  }

  result = lcb_get_bootstrap_status(connection);
  if (result != LCB_SUCCESS) {
    error << "Bootstrap status : " << lcb_strerror(connection, result)
          << std::endl;
    return {true, error.str()};
  }
  return {connection};
}

Connection::Info Connection::Pool::GetConnection() {
  if (pool_.empty()) {
    return {true, "Connection pool is empty"};
  }
  auto connection = pool_.front();
  pool_.pop();
  return {connection};
}

Connection::Pool::~Pool() {
  while (!pool_.empty()) {
    lcb_destroy(pool_.front());
    pool_.pop();
  }
}
