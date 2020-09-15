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
#include <libcouchbase/couchbase.h>
#include <sstream>

::Info Query::Builder::Build(void (*row_callback)(lcb_INSTANCE *, int,
                                                  const lcb_RESPQUERY *),
                             void *cookie) {
  lcb_cmdquery_create(&cmd_);
  auto result = lcb_cmdquery_statement(cmd_, query_info_.query.c_str(),
                                       query_info_.query.size());
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set query", connection_, result);
  }

  for (const auto &[key, value] : query_info_.named_params) {
    size_t first = key.find_first_not_of(' ');
    std::string key2 = key.substr(first, key.size() - first);
    first = key2.find_first_not_of('$');
    std::string key3 = key2.substr(first, key2.size() - first);
    result = lcb_cmdquery_named_param(cmd_, key3.c_str(), key3.size(),
                                      value.c_str(), value.size());

    if (result != LCB_SUCCESS) {
      return ErrorFormat("Unable to set named parameter", connection_, result);
    }
  }

  for (const auto &param : query_info_.pos_params) {
    result = lcb_cmdquery_positional_param(cmd_, param.c_str(), param.size());
    if (result != LCB_SUCCESS) {
      return ErrorFormat("Unable to set positional parameter", connection_,
                         result);
    }
  }

  result = lcb_cmdquery_consistency(
      cmd_, query_info_.options.GetOrDefaultConsistency(isolate_));
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
      id << line << "@" << file << "(" << func << ')';
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

    result = lcb_cmdquery_client_context_id(cmd_, id.c_str(), id.size());
    if (result != LCB_SUCCESS) {
      return ErrorFormat("Unable to set clientContextId", connection_, result);
    }
  }

  lcb_cmdquery_handle(cmd_, &handle_);
  lcb_cmdquery_callback(cmd_, row_callback);
  lcb_cmdquery_adhoc(cmd_,
                     !query_info_.options.GetOrDefaultIsPrepared(isolate_));

  lcb_set_cookie(connection_, cookie);

  result = lcb_cmdquery_timeout(cmd_, timeout_);
  if (result != LCB_SUCCESS) {
    return ErrorFormat("Unable to set timeout for query", connection_, result);
  }

  // TODO: Currently lcb is not suppporting n1ql grace time period. Add it once
  // its available
  /*
    lcb_U32 n1ql_grace_period = Query::n1ql_grace_period;
    result = lcb_cntl(connection_, LCB_CNTL_SET, LCB_CNTL_N1QL_GRACE_PERIOD,
                      &n1ql_grace_period);
    if (result != LCB_SUCCESS) {
      return ErrorFormat("Unable to set n1ql grace period for query",
    connection_, result);
    }
  */

  return {false};
}

::Info Query::Builder::ErrorFormat(const std::string &message,
                                   lcb_INSTANCE *connection,
                                   const lcb_STATUS error) const {
  auto helper = UnwrapData(isolate_)->query_helper;
  return {true, helper->ErrorFormat(message, connection, error)};
}
