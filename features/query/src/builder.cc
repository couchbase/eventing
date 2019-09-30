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

#include "isolate_data.h"
#include "query-builder.h"

::Info Query::Builder::Build(void (*row_callback)(lcb_t, int,
                                                  const lcb_RESPN1QL *),
                             void *cookie) {
  std::stringstream error;

  auto result = lcb_n1p_setstmtz(params_, query_info_.query.c_str());
  if (result != LCB_SUCCESS) {
    error << "Unable to set query : " << lcb_strerror(connection_, result)
          << std::endl;
    return {true, error.str()};
  }

  for (const auto &[key, value] : query_info_.named_params) {
    result = lcb_n1p_namedparamz(params_, key.c_str(), value.c_str());
    if (result != LCB_SUCCESS) {
      error << "Unable to set named parameter : " << key << " : "
            << lcb_strerror(connection_, result) << std::endl;
      return {true, error.str()};
    }
  }

  for (const auto &param : query_info_.pos_params) {
    result = lcb_n1p_posparam(params_, param.c_str(), param.size());
    if (result != LCB_SUCCESS) {
      error << "Unable to set parameter " << param << " : "
            << lcb_strerror(connection_, result) << std::endl;
      return {true, error.str()};
    }
  }

  result = lcb_n1p_mkcmd(params_, &cmd_);
  if (result != LCB_SUCCESS) {
    error << "Unable to make query cmd : " << lcb_strerror(connection_, result)
          << std::endl;
    return {true, error.str()};
  }

  cmd_.handle = &handle_;
  cmd_.callback = row_callback;
  cmd_.cmdflags |= LCB_CMDN1QL_F_PREPCACHE;
  lcb_set_cookie(connection_, cookie);

  result =
      lcb_cntl(connection_, LCB_CNTL_SET, LCB_CNTL_N1QL_TIMEOUT, &timeout_);
  if (result != LCB_SUCCESS) {
    error << "Unable to set timeout for query : "
          << lcb_strerror(connection_, result) << std::endl;
    return {true, error.str()};
  }
  return {false};
}
