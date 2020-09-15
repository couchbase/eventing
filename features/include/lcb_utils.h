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

struct Result {
  std::string key;
  uint64_t cas{0};
  lcb_STATUS rc{LCB_SUCCESS};
  uint8_t datatype{0};
  std::string value;
  uint32_t exptime{0};
  int64_t subdoc_counter{0};
  uint64_t counter{0};
  const void *binary{0};
  size_t byteLength;
};

constexpr int def_lcb_retry_count = 6;
constexpr int def_lcb_retry_timeout = 0;

const char *GetUsername(void *cookie, const char *host, const char *port,
                        const char *bucket);

const char *GetPassword(void *cookie, const char *host, const char *port,
                        const char *bucket);

// lcb related callbacks
void GetCallback(lcb_INSTANCE *instance, int, const lcb_RESPBASE *rb);

void SetCallback(lcb_INSTANCE *instance, int cbtype, const lcb_RESPBASE *rb);

void SubDocumentLookupCallback(lcb_INSTANCE *instance, int cbtype,
                               const lcb_RESPBASE *rb);

void SubDocumentCallback(lcb_INSTANCE *instance, int cbtype,
                         const lcb_RESPBASE *rb);

void DeleteCallback(lcb_INSTANCE *instance, int cbtype, const lcb_RESPBASE *rb);

void counter_callback(lcb_INSTANCE *instance, int cbtype,
                      const lcb_RESPBASE *rb);

void unlock_callback(lcb_INSTANCE *instance, int cbtype,
                     const lcb_RESPBASE *rb);

std::pair<lcb_STATUS, Result> LcbGet(lcb_INSTANCE *instance, lcb_CMDGET &cmd);

std::pair<lcb_STATUS, Result> LcbSet(lcb_INSTANCE *instance, lcb_CMDSTORE &cmd);

std::pair<lcb_STATUS, Result> LcbDelete(lcb_INSTANCE *instance,
                                        lcb_CMDREMOVE &cmd);

std::pair<lcb_STATUS, Result> LcbSubdocSet(lcb_INSTANCE *instance,
                                           lcb_CMDSUBDOC &cmd);

std::pair<lcb_STATUS, Result> LcbSubdocDelete(lcb_INSTANCE *instance,
                                              lcb_CMDSUBDOC &cmd);

std::pair<lcb_STATUS, Result> LcbGetCounter(lcb_INSTANCE *instance,
                                            lcb_CMDCOUNTER &cmd);

std::pair<lcb_STATUS, Result> LcbUnlock(lcb_INSTANCE *instance,
                                        lcb_CMDUNLOCK &cmd);

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
        (!IsRetriable(result.first) && !IsRetriable(result.second.rc)) ||
        (max_retry_count && retry_count >= max_retry_count))
      break;

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

extern lcb_LOGGER *evt_logger;

#endif // COUCHBASE_LCB_UTILS_H
