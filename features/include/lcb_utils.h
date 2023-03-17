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

#ifndef COUCHBASE_LCB_UTILS_H
#define COUCHBASE_LCB_UTILS_H

#include "utils.h"
#include <libcouchbase/couchbase.h>
#include <string>
#include <thread>

const uint8_t BINARY_DOC = 0;
const uint8_t JSON_DOC = 1;
const uint8_t XATTR_DOC = 4;
const uint8_t UNKNOWN_TYPE = 8;

const uint16_t UNKNOWN_SCOPE = 0x8C;
const uint16_t UNKNOWN_COLLECTION = 0x88;

struct Result {
  std::string key;
  uint64_t cas{0};
  lcb_STATUS rc{LCB_SUCCESS};
  uint8_t datatype{UNKNOWN_TYPE};
  std::string value;
  uint32_t exptime{0};
  int64_t subdoc_counter{0};
  uint64_t counter{0};
  uint16_t kv_err_code{0};
};

constexpr int def_lcb_retry_count = 6;
constexpr int def_lcb_retry_timeout = 0;

void GetUsernameAndPassword(lcbauth_CREDENTIALS *credentials);

// lcb related callbacks
void GetCallback(lcb_INSTANCE *instance, int, const lcb_RESPBASE *rb);

void SetCallback(lcb_INSTANCE *instance, int cbtype, const lcb_RESPBASE *rb);

void SubDocumentLookupCallback(lcb_INSTANCE *instance, int cbtype,
                               const lcb_RESPBASE *rb);

void SubDocumentCallback(lcb_INSTANCE *instance, int cbtype,
                         const lcb_RESPBASE *rb);

void DeleteCallback(lcb_INSTANCE *instance, int cbtype, const lcb_RESPBASE *rb);

void TouchCallback(lcb_INSTANCE *instance, int cbtype, const lcb_RESPBASE *rb);

void counter_callback(lcb_INSTANCE *instance, int cbtype,
                      const lcb_RESPBASE *rb);

void unlock_callback(lcb_INSTANCE *instance, int cbtype,
                     const lcb_RESPBASE *rb);

std::pair<lcb_STATUS, Result> LcbGet(lcb_INSTANCE *instance, lcb_CMDGET &cmd);

std::pair<lcb_STATUS, Result> LcbSet(lcb_INSTANCE *instance, lcb_CMDSTORE &cmd);

std::pair<lcb_STATUS, Result> LcbDelete(lcb_INSTANCE *instance,
                                        lcb_CMDREMOVE &cmd);

std::pair<lcb_STATUS, Result> LcbTouch(lcb_INSTANCE *instance,
                                       lcb_CMDTOUCH &cmd);

std::pair<lcb_STATUS, Result> LcbSubdocSet(lcb_INSTANCE *instance,
                                           lcb_CMDSUBDOC &cmd);

std::pair<lcb_STATUS, Result> LcbSubdocDelete(lcb_INSTANCE *instance,
                                              lcb_CMDSUBDOC &cmd);

std::pair<lcb_STATUS, Result> LcbGetCounter(lcb_INSTANCE *instance,
                                            lcb_CMDCOUNTER &cmd);

std::pair<lcb_STATUS, Result> LcbUnlock(lcb_INSTANCE *instance,
                                        lcb_CMDUNLOCK &cmd);

bool IsErrCodeRetriable(lcb_STATUS error, uint16_t err_code);
bool IsRetriable(lcb_STATUS error);

template <typename CmdType, typename Callable>
std::pair<lcb_STATUS, Result>
RetryLcbCommand(lcb_INSTANCE *instance, CmdType &cmd, int max_retry_count,
                uint32_t max_retry_secs, Callable &&callable) {
  int retry_count = 1;
  std::pair<lcb_STATUS, Result> result;
  auto start = GetUnixTime();
  while (true) {
    result = callable(instance, cmd);
    if ((result.first == LCB_SUCCESS && result.second.rc == LCB_SUCCESS) ||
        (!IsRetriable(result.first) &&
         !IsErrCodeRetriable(result.second.rc, result.second.kv_err_code)) ||
        (max_retry_count && retry_count >= max_retry_count)) {
      break;
    }

    if (max_retry_secs > 0) {
      auto now = GetUnixTime();
      if (now - start >= max_retry_secs)
        break;
    }
    ++retry_count;
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  LOG(logTrace) << "RetryLcbCommand retry_count: " << retry_count << std::endl;
  return result;
}

struct Logger {
  Logger() : base(NULL) {}
  lcb_LOGGER *base;
};

void evt_log_handler(const lcb_LOGGER *procs, uint64_t iid, const char *subsys,
                     lcb_LOG_SEVERITY severity, const char *srcfile,
                     int srcline, const char *fmt, va_list ap);

extern Logger evt_logger;

#endif // COUCHBASE_LCB_UTILS_H
