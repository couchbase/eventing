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

#ifndef QUERY_ROW_H
#define QUERY_ROW_H

#include <libcouchbase/couchbase.h>
#include <string>

namespace Query {
struct Row {
  Row(bool is_done, bool is_error, bool is_auth_error, bool is_client_error,
      bool is_query_error, lcb_error_t err_code, const std::string &data)
      : is_done(is_done), is_error(is_error),
        is_client_auth_error(is_auth_error), is_client_error(is_client_error),
        is_query_error(is_query_error), err_code(err_code), data(data) {}

  bool is_done{false};
  bool is_error{false};
  bool is_client_auth_error{false};
  bool is_client_error{false};
  bool is_query_error{false};
  lcb_error_t err_code{LCB_SUCCESS};
  const std::string &data;
};
} // namespace Query

#endif // QUERY_ROW_H
