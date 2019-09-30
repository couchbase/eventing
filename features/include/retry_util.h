// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

#ifndef RETRY_UTIL_H
#define RETRY_UTIL_H

#include <algorithm>
#include <cassert>
#include <chrono>
#include <functional>
#include <thread>
#include <type_traits>

#include "log.h"

constexpr int64_t max_backoff_milliseconds = 30000; // 30 seconds

template <typename Predicate, typename Callable, typename... Args,
          // figure out what the callable returns
          typename R = typename std::decay<
              typename std::result_of<Callable &(Args...)>::type>::type,
          // require that Predicate is actually a Predicate
          typename std::enable_if<
              std::is_convertible<typename std::result_of<Predicate &(R)>::type,
                                  bool>::value,
              int>::type = 0>
R RetryWithFixedBackoff(int max_retry_count, int64_t initial_delay_milliseconds,
                        Predicate &&isRetriable, Callable &&callable,
                        Args &&... args) {
  int retry_count = 0;
  while (true) {
    auto status = callable(std::forward<Args>(args)...);
    if (!isRetriable(status)) {
      return status;
    }

    if (retry_count >= max_retry_count) {
      // Return status and abort retry
      return status;
    }

    LOG(logTrace) << "Callable execution failed and will be retried in "
                  << initial_delay_milliseconds << " milliseconds (attempt "
                  << (retry_count + 1) << " out of " << max_retry_count
                  << "), caused by error: " << status << std::endl;
    std::this_thread::sleep_for(
        std::chrono::milliseconds(initial_delay_milliseconds));
    retry_count++;
  }
}

template <typename Predicate, typename Callable, typename... Args,
          // figure out what the callable returns
          typename R = typename std::decay<
              typename std::result_of<Callable &(Args...)>::type>::type,
          // require that Predicate is actually a Predicate
          typename std::enable_if<
              std::is_convertible<typename std::result_of<Predicate &(R)>::type,
                                  bool>::value,
              int>::type = 0>
R RetryWithExponentialBackoff(int max_retry_count,
                              int64_t initial_delay_milliseconds,
                              Predicate &&isRetriable, Callable &&callable,
                              Args &&... args) {
  int retry_count = 0;
  while (true) {
    auto status = callable(std::forward<Args>(args)...);
    if (!isRetriable(status)) {
      return status;
    }

    if (retry_count >= max_retry_count) {
      // Return status and abort retry
      return status;
    }
    int64_t delay_milliseconds = 0;
    if (initial_delay_milliseconds > 0) {
      delay_milliseconds = std::min(initial_delay_milliseconds << retry_count,
                                    max_backoff_milliseconds);
    }
    LOG(logTrace) << "Callable execution failed and will be retried in "
                  << delay_milliseconds << " milliseconds (attempt "
                  << (retry_count + 1) << " out of " << max_retry_count
                  << "), caused by error: " << status << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_milliseconds));
    retry_count++;
  }
}

#endif
