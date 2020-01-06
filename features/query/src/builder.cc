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

#include "isolate_data.h"
#include "query-builder.h"

::Info Query::Builder::Build(void (*row_callback)(lcb_t, int,
                                                  const lcb_RESPN1QL *),
                             void *cookie) {
  auto result = lcb_n1p_setstmtz(params_, query_info_.query.c_str());
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set query", connection_, result);
  }

  for (const auto &[key, value] : query_info_.named_params) {
    result = lcb_n1p_namedparamz(params_, key.c_str(), value.c_str());
    if (result != LCB_SUCCESS) {
      return ErrorFormat("Unable to set named parameter", connection_, result);
    }
  }

  for (const auto &param : query_info_.pos_params) {
    result = lcb_n1p_posparam(params_, param.c_str(), param.size());
    if (result != LCB_SUCCESS) {
      return ErrorFormat("Unable to set positional parameter", connection_,
                         result);
    }
  }

  result = lcb_n1p_setconsistency(params_, query_info_.options.consistency);
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set consistency", connection_, result);
  }

  if (query_info_.options.client_context_id != nullptr) {
    result = lcb_n1p_setoptz(params_, "client_context_id",
                             query_info_.options.client_context_id->c_str());
    if (result != LCB_SUCCESS) {
      return ErrorFormat("Unable to set clientContextId", connection_, result);
    }
  }

  result = lcb_n1p_mkcmd(params_, &cmd_);
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to make query cmd", connection_, result);
  }

  cmd_.handle = &handle_;
  cmd_.callback = row_callback;
  if (query_info_.options.is_prepared != nullptr &&
      *query_info_.options.is_prepared) {
    cmd_.cmdflags |= LCB_CMDN1QL_F_PREPCACHE;
  }
  lcb_set_cookie(connection_, cookie);

  result =
      lcb_cntl(connection_, LCB_CNTL_SET, LCB_CNTL_N1QL_TIMEOUT, &timeout_);
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set timeout for query", connection_, result);
  }
  lcb_U32 n1ql_grace_period = Query::n1ql_grace_period;
  result =
      lcb_cntl(connection_, LCB_CNTL_SET, LCB_CNTL_N1QL_GRACE_PERIOD, &n1ql_grace_period);
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set n1ql grace period for query", connection_, result);
  }
  return {false};
}

::Info Query::Builder::ErrorFormat(const std::string &message, lcb_t connection,
                                   const lcb_error_t error) const {
  auto helper = UnwrapData(isolate_)->query_helper;
  return {true, helper->ErrorFormat(message, connection, error)};
}
