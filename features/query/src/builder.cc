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
#include "log.h"
#include "query-builder.h"
#include <algorithm>
#include <sstream>

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

  result = lcb_n1p_setconsistency(
      params_, query_info_.options.GetOrDefaultConsistency(isolate_));
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set consistency", connection_, result);
  }

  if (query_info_.options.client_context_id == nullptr) {
    auto stack = v8::StackTrace::CurrentStackTrace(isolate_, 1,
                                                   v8::StackTrace::kLineNumber);
    if (stack->GetFrameCount() > 0) {
      auto frame = stack->GetFrame(isolate_, 0);
      std::string file = "?";
      if (!frame->GetScriptName().IsEmpty()) {
        file = *v8::String::Utf8Value(isolate_, frame->GetScriptName());
      }
      std::string func = "?";
      if (!frame->GetFunctionName().IsEmpty()) {
        func = *v8::String::Utf8Value(isolate_, frame->GetFunctionName());
      }
      int line = frame->GetLineNumber();
      std::ostringstream id;
      id << '"' << line << "@" << file << "(" << func << ')' << '"';
      query_info_.options.client_context_id =
          std::make_unique<std::string>(id.str());
    }
  }

  if (query_info_.options.client_context_id != nullptr) {
    // n1ql limits to 64 chars, and no \ or " chars
    std::string id = query_info_.options.client_context_id->c_str();
    id.erase(std::remove(id.begin(), id.end(), '\\'), id.end());
    id.erase(std::remove(id.begin(), id.end(), '\"'), id.end());
    if (id.length() > 62) {
      id.erase(62);
    }
    id = "\"" + id + "\"";
    result = lcb_n1p_setoptz(params_, "client_context_id", id.c_str());
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
  if (query_info_.options.GetOrDefaultIsPrepared(isolate_)) {
    cmd_.cmdflags |= LCB_CMDN1QL_F_PREPCACHE;
  }
  lcb_set_cookie(connection_, cookie);

  result =
      lcb_cntl(connection_, LCB_CNTL_SET, LCB_CNTL_N1QL_TIMEOUT, &timeout_);
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set timeout for query", connection_, result);
  }
  lcb_U32 n1ql_grace_period = Query::n1ql_grace_period;
  result = lcb_cntl(connection_, LCB_CNTL_SET, LCB_CNTL_N1QL_GRACE_PERIOD,
                    &n1ql_grace_period);
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set n1ql grace period for query", connection_,
                       result);
  }
  return {false};
}

::Info Query::Builder::ErrorFormat(const std::string &message, lcb_t connection,
                                   const lcb_error_t error) const {
  auto helper = UnwrapData(isolate_)->query_helper;
  return {true, helper->ErrorFormat(message, connection, error)};
}
