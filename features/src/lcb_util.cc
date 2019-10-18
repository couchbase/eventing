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
#include "lcb_utils.h"
#include "utils.h"
#include <v8.h>

#define EVT_LOG_MSG_SIZE 1024

const char *GetUsername(void *cookie, const char *host, const char *port,
                        const char *bucket) {
  LOG(logDebug) << "Getting username for host " << RS(host) << " port " << port
                << std::endl;

  auto endpoint = JoinHostPort(host, port);
  auto isolate = static_cast<v8::Isolate *>(cookie);
  auto comm = UnwrapData(isolate)->comm;
  auto info = comm->GetCreds(endpoint);
  if (!info.is_valid) {
    LOG(logError) << "Failed to get username for " << RS(host) << ":" << port
                  << " err: " << info.msg << std::endl;
  }

  static thread_local std::string username;
  username = info.username;
  return username.c_str();
}

const char *GetPassword(void *cookie, const char *host, const char *port,
                        const char *bucket) {
  LOG(logDebug) << "Getting password for host " << RS(host) << " port " << port
                << std::endl;

  auto isolate = static_cast<v8::Isolate *>(cookie);
  auto comm = UnwrapData(isolate)->comm;
  auto endpoint = JoinHostPort(host, port);
  auto info = comm->GetCreds(endpoint);
  if (!info.is_valid) {
    LOG(logError) << "Failed to get password for " << RS(host) << ":" << port
                  << " err: " << info.msg << std::endl;
  }

  static thread_local std::string password;
  password = info.password;
  return password.c_str();
}

// lcb related callbacks
void get_callback(lcb_t instance, int, const lcb_RESPBASE *rb) {
  auto resp = reinterpret_cast<const lcb_RESPGET *>(rb);
  auto result = reinterpret_cast<Result *>(rb->cookie);

  LOG(logTrace) << "Bucket: LCB_GET callback, res: "
                << lcb_strerror(nullptr, rb->rc) << rb->rc << " cas " << rb->cas
                << std::endl;

  if (rb->rc == LCB_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_GET breaking out" << std::endl;
    lcb_breakout(instance);
  }

  result->rc = resp->rc;
  result->cas = resp->cas;

  if (resp->rc == LCB_SUCCESS) {
    result->value.assign(reinterpret_cast<const char *>(resp->value),
                         static_cast<int>(resp->nvalue));
    LOG(logTrace) << "Bucket: Value: " << RU(result->value)
                  << " flags: " << resp->itmflags << std::endl;
  }
}

void set_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  auto resp = reinterpret_cast<const lcb_RESPSTORE *>(rb);
  auto result = reinterpret_cast<Result *>(rb->cookie);

  if (rb->rc == LCB_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_STORE breaking out" << std::endl;
    lcb_breakout(instance);
  }

  result->rc = resp->rc;
  result->cas = resp->cas;

  LOG(logTrace) << "Bucket: LCB_STORE callback "
                << lcb_strerror(instance, result->rc) << " cas " << resp->cas
                << std::endl;
}

void sdmutate_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  auto result = reinterpret_cast<Result *>(rb->cookie);
  result->rc = rb->rc;

  if (rb->rc == LCB_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_SDMUTATE breaking out" << std::endl;
    lcb_breakout(instance);
  }

  LOG(logTrace) << "Bucket: LCB_SDMUTATE callback "
                << lcb_strerror(nullptr, result->rc) << std::endl;
}

void del_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  auto result = reinterpret_cast<Result *>(rb->cookie);
  result->rc = rb->rc;

  if (rb->rc == LCB_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_DEL breaking out" << std::endl;
    lcb_breakout(instance);
  }

  LOG(logTrace) << "Bucket: LCB_DEL callback "
                << lcb_strerror(nullptr, result->rc) << std::endl;
}

void counter_callback(lcb_t instance, int cbtype, const lcb_RESPBASE *rb) {
  auto result = reinterpret_cast<Result *>(rb->cookie);
  const lcb_RESPCOUNTER *resp = reinterpret_cast<const lcb_RESPCOUNTER *>(rb);
  if (rb->rc == LCB_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_COUNTER breaking out" << std::endl;
    lcb_breakout(instance);
  }
  result->rc = resp->rc;
  result->counter = resp->value;
  LOG(logTrace) << "Bucket: LCB_COUNTER callback "
                << lcb_strerror(nullptr, result->rc) << std::endl;
}

void evt_log_formatter(char *buf, int buf_size, const char *subsystem,
                       int srcline, unsigned int instance_id, const char *fmt,
                       va_list ap) {
  char msg[EVT_LOG_MSG_SIZE] = {};

  vsnprintf(msg, EVT_LOG_MSG_SIZE, fmt, ap);
  msg[EVT_LOG_MSG_SIZE - 1] = '\0';
  for (int i = 0; i < EVT_LOG_MSG_SIZE; i++) {
    if (msg[i] == '\n') {
      msg[i] = ' ';
    }
  }
  snprintf(buf, buf_size, "[lcb,%s L:%d I:%u] %s", subsystem, srcline,
           instance_id, msg);
}

/**
 * Conversion needed as libcouchbase using ascending order for level, while
 * eventing is using reversed order.
 */
LogLevel evt_log_map_level(int severity) {
  switch (severity) {
    // TODO : We can anyway not log at these levels as eventing-producer only
    // logs at INFO. So, we will log only WARN, ERROR and FATAL messages
  case LCB_LOG_TRACE:
  case LCB_LOG_DEBUG:
    return logTrace;

  case LCB_LOG_INFO:
    return logInfo;

  case LCB_LOG_WARN:
  case LCB_LOG_ERROR:
  case LCB_LOG_FATAL:
  default:
    return logError;
  }
}

bool evt_should_log(int severity, const char *subsys) {
  if (strcmp(subsys, "negotiation") == 0) {
    return false;
  }

  if (evt_log_map_level(severity) <= SystemLog::level_) {
    return true;
  }

  return false;
}

void evt_log_handler(struct lcb_logprocs_st *procs, unsigned int iid,
                     const char *subsys, int severity, const char *srcfile,
                     int srcline, const char *fmt, va_list ap) {
  if (evt_should_log(severity, subsys)) {
    char buf[EVT_LOG_MSG_SIZE] = {};
    evt_log_formatter(buf, EVT_LOG_MSG_SIZE, subsys, srcline, iid, fmt, ap);
    LOG(evt_log_map_level(severity)) << buf << std::endl;
  }
}

struct lcb_logprocs_st evt_logger = {
    0, /* version */
    {
        {evt_log_handler} /* v1 */
    }                     /* v */
};
