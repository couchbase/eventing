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

#include <nlohmann/json.hpp>
#include <thread>

#include "isolate_data.h"
#include "lcb_utils.h"
#include "utils.h"

#define EVT_LOG_MSG_SIZE 1024

void GetUsernameAndPassword(lcbauth_CREDENTIALS *credentials) {
  void *cookie;
  lcbauth_credentials_cookie(credentials, &cookie);
  auto isolate = static_cast<v8::Isolate *>(cookie);
  auto comm = UnwrapData(isolate)->comm;

  const char *port = nullptr;
  size_t port_len = 0;
  lcbauth_credentials_port(credentials, &port, &port_len);

  const char *host = nullptr;
  size_t host_len = 0;
  lcbauth_credentials_hostname(credentials, &host, &host_len);
  auto endpoint = JoinHostPort(host, port);

  auto reason = lcbauth_credentials_reason(credentials);
  if (reason == LCBAUTH_REASON_AUTHENTICATION_FAILURE) {
    comm->removeCachedEntry(endpoint);
  }

  auto useCacheEntry =
      (lcbauth_credentials_service(credentials) == LCBAUTH_SERVICE_QUERY);
  auto cached_credentials = comm->GetCredsCached(endpoint, useCacheEntry);
  if (!cached_credentials.is_valid) {
    lcbauth_credentials_result(credentials, LCBAUTH_RESULT_NOT_AVAILABLE);
    return;
  }

  static thread_local std::string username, password;
  username = cached_credentials.username;
  password = cached_credentials.password;
  lcbauth_credentials_username(credentials, username.c_str(), username.size());
  lcbauth_credentials_password(credentials, password.c_str(), password.size());
  lcbauth_credentials_result(credentials, LCBAUTH_RESULT_OK);
}

// lcb related callbacks
void GetCallback(lcb_INSTANCE *instance, int, const lcb_RESPBASE *rb) {
  auto resp = reinterpret_cast<const lcb_RESPGET *>(rb);

  Result *result;
  lcb_respget_cookie(resp, reinterpret_cast<void **>(&result));
  result->rc = lcb_respget_status(resp);

  LOG(logTrace) << "Bucket: LCB_GET callback, res: "
                << lcb_strerror_short(result->rc) << std::endl;

  if (result->rc == LCB_ERR_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_GET breaking out" << std::endl;
    lcb_breakout(instance);
  }

  if (result->rc != LCB_SUCCESS) {
    const lcb_KEY_VALUE_ERROR_CONTEXT *ctx = nullptr;
    lcb_respget_error_context(resp, &ctx);
    lcb_errctx_kv_status_code(ctx, &result->kv_err_code);
    return;
  }

  lcb_respget_cas(resp, &result->cas);
  lcb_respget_datatype(resp, &result->datatype);

  if (result->rc == LCB_SUCCESS) {
    const char *value;
    size_t nValue;
    lcb_respget_value(resp, &value, &nValue);
    result->value.assign(value, nValue);
    LOG(logTrace) << "Bucket: Value: " << RU(result->value) << std::endl;
  }
}

void SetCallback(lcb_INSTANCE *instance, int cbtype, const lcb_RESPBASE *rb) {
  auto resp = reinterpret_cast<const lcb_RESPSTORE *>(rb);
  Result *result;
  lcb_respstore_cookie(resp, reinterpret_cast<void **>(&result));
  result->rc = lcb_respstore_status(resp);

  if (result->rc == LCB_ERR_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_STORE breaking out" << std::endl;
    lcb_breakout(instance);
  }

  if (result->rc != LCB_SUCCESS) {
    const lcb_KEY_VALUE_ERROR_CONTEXT *ctx = nullptr;
    lcb_respstore_error_context(resp, &ctx);
    lcb_errctx_kv_status_code(ctx, &result->kv_err_code);
    return;
  }

  lcb_respstore_cas(resp, &result->cas);

  LOG(logTrace) << "Bucket: LCB_STORE callback "
                << lcb_strerror_short(result->rc) << std::endl;
}

void SubDocumentCallback(lcb_INSTANCE *instance, int cbtype,
                         const lcb_RESPBASE *rb) {
  const lcb_RESPSUBDOC *resp = (const lcb_RESPSUBDOC *)rb;
  Result *result;
  lcb_respsubdoc_cookie(resp, reinterpret_cast<void **>(&result));
  result->rc = lcb_respsubdoc_status(resp);

  if (result->rc == LCB_ERR_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_SDMUTATE breaking out" << std::endl;
    lcb_breakout(instance);
  }

  if (result->rc != LCB_SUCCESS) {
    const lcb_KEY_VALUE_ERROR_CONTEXT *ctx = nullptr;
    lcb_respsubdoc_error_context(resp, &ctx);
    lcb_errctx_kv_status_code(ctx, &result->kv_err_code);
    return;
  }

  auto total = lcb_respsubdoc_result_size(resp);
  for (size_t index = 0; index < total; index++) {
    result->rc = lcb_respsubdoc_result_status(resp, index);
    if (result->rc != LCB_SUCCESS) {
      LOG(logTrace) << "Bucket: LCB_SDMUTATE callback "
                    << lcb_strerror_short(result->rc) << std::endl;
      return;
    }
    const char *value;
    size_t nvalue;
    std::string temp;

    lcb_respsubdoc_result_value(resp, index, &value, &nvalue);
    if (nvalue != 0) {
      temp.assign(value, nvalue);
      result->subdoc_counter = std::stoll(temp, nullptr, 10);
    }
  }
  lcb_respsubdoc_cas(resp, &result->cas);

  LOG(logTrace) << "Bucket: LCB_SDMUTATE callback "
                << lcb_strerror_short(result->rc) << std::endl;
}

void SubDocumentLookupCallback(lcb_INSTANCE *instance, int cbtype,
                               const lcb_RESPBASE *rb) {
  const lcb_RESPSUBDOC *resp = (const lcb_RESPSUBDOC *)rb;
  Result *result;
  lcb_respsubdoc_cookie(resp, reinterpret_cast<void **>(&result));
  result->rc = lcb_respsubdoc_status(resp);

  if (result->rc == LCB_ERR_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_SDLOOKUP breaking out" << std::endl;
    lcb_breakout(instance);
  }

  if (result->rc != LCB_SUCCESS) {
    const lcb_KEY_VALUE_ERROR_CONTEXT *ctx = nullptr;
    lcb_respsubdoc_error_context(resp, &ctx);
    lcb_errctx_kv_status_code(ctx, &result->kv_err_code);
    return;
  }

  auto total = lcb_respsubdoc_result_size(resp);
  for (size_t index = 0; index < total; index++) {
    result->rc = lcb_respsubdoc_result_status(resp, 0);
    if (result->rc != LCB_SUCCESS) {
      return;
    }

    const char *cValue;
    size_t nValue;
    lcb_respsubdoc_result_value(resp, index, &cValue, &nValue);

    if (index == 0) {
      std::string value;
      value.assign(cValue, nValue);
      result->exptime = std::stoul(value, nullptr, 10);
    } else if (index == 1) {
      // 0x00: raw, 0x01 json, 0x05: jsonXattr, 0x04: rawXattr
      // Default it to binary doc
      result->datatype = BINARY_DOC;
      auto json = nlohmann::json::parse(cValue);
      auto values = json.get<std::vector<std::string>>();
      for (const auto &type : values) {
        if (type == "json") {
          result->datatype = result->datatype | JSON_DOC;
        }
        if (type == "xattr") {
          result->datatype = result->datatype | XATTR_DOC;
        }
      }
    } else {
      result->value.assign(cValue, nValue);
    }
  }

  lcb_respsubdoc_cas(resp, &result->cas);
  LOG(logTrace) << "Bucket: LCB_SDLOOKUP callback "
                << lcb_strerror_short(result->rc) << std::endl;
}

void DeleteCallback(lcb_INSTANCE *instance, int cbtype,
                    const lcb_RESPBASE *rb) {
  auto resp = reinterpret_cast<const lcb_RESPREMOVE *>(rb);
  Result *result;
  lcb_respremove_cookie(resp, reinterpret_cast<void **>(&result));
  result->rc = lcb_respremove_status(resp);

  if (result->rc == LCB_ERR_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_DEL breaking out" << std::endl;
    lcb_breakout(instance);
  }

  if (result->rc != LCB_SUCCESS) {
    const lcb_KEY_VALUE_ERROR_CONTEXT *ctx = nullptr;
    lcb_respremove_error_context(resp, &ctx);
    lcb_errctx_kv_status_code(ctx, &result->kv_err_code);
    return;
  }

  lcb_respremove_cas(resp, &result->cas);

  LOG(logTrace) << "Bucket: LCB_DEL callback " << lcb_strerror_short(result->rc)
                << std::endl;
}

void TouchCallback(lcb_INSTANCE *instance, int cbtype, const lcb_RESPBASE *rb) {
  auto resp = reinterpret_cast<const lcb_RESPTOUCH *>(rb);
  Result *result;
  lcb_resptouch_cookie(resp, reinterpret_cast<void **>(&result));
  result->rc = lcb_resptouch_status(resp);

  if (result->rc == LCB_ERR_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_TOUCH breaking out" << std::endl;
    lcb_breakout(instance);
  }

  if (result->rc != LCB_SUCCESS) {
    const lcb_KEY_VALUE_ERROR_CONTEXT *ctx = nullptr;
    lcb_resptouch_error_context(resp, &ctx);
    lcb_errctx_kv_status_code(ctx, &result->kv_err_code);
    return;
  }

  lcb_resptouch_cas(resp, &result->cas);
  LOG(logInfo) << "Bucket: LCB_TOUCH callback "
               << lcb_strerror_short(result->rc) << std::endl;
}

void counter_callback(lcb_INSTANCE *instance, int cbtype,
                      const lcb_RESPBASE *rb) {
  const lcb_RESPCOUNTER *resp = reinterpret_cast<const lcb_RESPCOUNTER *>(rb);
  Result *result;
  lcb_respcounter_cookie(resp, reinterpret_cast<void **>(&result));
  result->rc = lcb_respcounter_status(resp);

  if (result->rc == LCB_ERR_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_COUNTER breaking out" << std::endl;
    lcb_breakout(instance);
  }

  if (result->rc != LCB_SUCCESS) {
    const lcb_KEY_VALUE_ERROR_CONTEXT *ctx = nullptr;
    lcb_respcounter_error_context(resp, &ctx);
    lcb_errctx_kv_status_code(ctx, &result->kv_err_code);
    return;
  }

  lcb_respcounter_value(resp, &result->counter);
  LOG(logTrace) << "Bucket: LCB_COUNTER callback "
                << lcb_strerror_short(result->rc) << std::endl;
}

void unlock_callback(lcb_INSTANCE *instance, int cbtype,
                     const lcb_RESPBASE *rb) {
  const lcb_RESPUNLOCK *resp = reinterpret_cast<const lcb_RESPUNLOCK *>(rb);
  Result *result;
  lcb_respunlock_cookie(resp, reinterpret_cast<void **>(&result));
  result->rc = lcb_respunlock_status(resp);

  if (result->rc == LCB_ERR_PROTOCOL_ERROR) {
    LOG(logError) << "Bucket: LCB_UNLOCK breaking out" << std::endl;
    lcb_breakout(instance);
  }
}

std::pair<lcb_STATUS, Result> LcbGet(lcb_INSTANCE *instance, lcb_CMDGET &cmd) {
  Result result;
  auto err = lcb_get(instance, &result, &cmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_GET: "
                  << lcb_strerror_short(err) << std::endl;
    return {err, result};
  }

  err = lcb_wait(instance, LCB_WAIT_DEFAULT);

  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_GET: "
                  << lcb_strerror_short(err) << std::endl;
  }
  return {err, result};
}

std::pair<lcb_STATUS, Result> LcbSet(lcb_INSTANCE *instance,
                                     lcb_CMDSTORE &cmd) {
  Result result;
  auto err = lcb_store(instance, &result, &cmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_SET: "
                  << lcb_strerror_short(err) << std::endl;
    return {err, result};
  }

  err = lcb_wait(instance, LCB_WAIT_DEFAULT);

  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_SET: "
                  << lcb_strerror_short(err) << std::endl;
  }
  return {err, result};
}

std::pair<lcb_STATUS, Result> LcbTouch(lcb_INSTANCE *instance,
                                       lcb_CMDTOUCH &cmd) {
  Result result;
  auto err = lcb_touch(instance, &result, &cmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_TOUCH: "
                  << lcb_strerror_short(err) << std::endl;
    return {err, result};
  }

  err = lcb_wait(instance, LCB_WAIT_DEFAULT);

  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_TOUCH: "
                  << lcb_strerror_short(err) << std::endl;
  }
  return {err, result};
}

std::pair<lcb_STATUS, Result> LcbDelete(lcb_INSTANCE *instance,
                                        lcb_CMDREMOVE &cmd) {
  Result result;
  auto err = lcb_remove(instance, &result, &cmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_REMOVE: "
                  << lcb_strerror_short(err) << std::endl;
    return {err, result};
  }

  err = lcb_wait(instance, LCB_WAIT_DEFAULT);

  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_REMOVE: "
                  << lcb_strerror_short(err) << std::endl;
  }
  return {err, result};
}

std::pair<lcb_STATUS, Result> LcbSubdocSet(lcb_INSTANCE *instance,
                                           lcb_CMDSUBDOC &cmd) {
  Result result;
  auto err = lcb_subdoc(instance, &result, &cmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_SUBDOC_SET: "
                  << lcb_strerror_short(err) << std::endl;
    return {err, result};
  }

  err = lcb_wait(instance, LCB_WAIT_DEFAULT);

  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_SUBDOC_SET: "
                  << lcb_strerror_short(err) << std::endl;
  }
  return {err, result};
}

std::pair<lcb_STATUS, Result> LcbSubdocDelete(lcb_INSTANCE *instance,
                                              lcb_CMDSUBDOC &cmd) {
  Result result;
  auto err = lcb_subdoc(instance, &result, &cmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_SUBDOC_REMOVE: "
                  << lcb_strerror_short(err) << std::endl;
    return {err, result};
  }

  err = lcb_wait(instance, LCB_WAIT_DEFAULT);

  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_SUBDOC_REMOVE: "
                  << lcb_strerror_short(err) << std::endl;
  }
  return {err, result};
}

std::pair<lcb_STATUS, Result> LcbGetCounter(lcb_INSTANCE *instance,
                                            lcb_CMDCOUNTER &cmd) {
  Result result;
  auto err = lcb_counter(instance, &result, &cmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_COUNTER: "
                  << lcb_strerror_short(err) << std::endl;
    return {err, result};
  }

  err = lcb_wait(instance, LCB_WAIT_DEFAULT);

  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_COUNTER: "
                  << lcb_strerror_short(err) << std::endl;
  }
  return {err, result};
}

std::pair<lcb_STATUS, Result> LcbUnlock(lcb_INSTANCE *instance,
                                        lcb_CMDUNLOCK &cmd) {
  Result result;
  auto err = lcb_unlock(instance, &result, &cmd);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to set params for LCB_UNLOCK: "
                  << lcb_strerror_short(err) << std::endl;
    return {err, result};
  }

  err = lcb_wait(instance, LCB_WAIT_DEFAULT);
  if (err != LCB_SUCCESS) {
    LOG(logTrace) << "Bucket: Unable to schedule LCB_UNLOCK: "
                  << lcb_strerror_short(err) << std::endl;
  }
  return {err, result};
}

bool IsErrCodeRetriable(lcb_STATUS error, uint16_t err_code) {
  if (err_code == UNKNOWN_SCOPE || err_code == UNKNOWN_COLLECTION) {
    return false;
  }

  return LCB_ERROR_IS_TRANSIENT(error);
}

bool IsRetriable(lcb_STATUS error) { return LCB_ERROR_IS_TRANSIENT(error); }

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

void evt_log_handler(const lcb_LOGGER *procs, uint64_t iid, const char *subsys,
                     lcb_LOG_SEVERITY severity, const char *srcfile,
                     int srcline, const char *fmt, va_list ap) {
  if (evt_should_log(severity, subsys)) {
    char buf[EVT_LOG_MSG_SIZE] = {};
    evt_log_formatter(buf, EVT_LOG_MSG_SIZE, subsys, srcline, iid, fmt, ap);
    LOG(evt_log_map_level(severity)) << buf << std::endl;
  }
}

struct Logger evt_logger;
