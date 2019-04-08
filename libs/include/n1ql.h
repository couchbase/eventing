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

#ifndef N1QL_H
#define N1QL_H

#include <libcouchbase/couchbase.h>
#include <libcouchbase/n1ql.h>
#include <list>
#include <queue>
#include <stack>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <v8.h>
#include <vector>

#include "comm.h"
#include "isolate_data.h"
#include "log.h"
#include "transpiler.h"

struct N1QLCodex {
  bool IsRetriable(int64_t code) const {
    return retriable_errors.find(code) != retriable_errors.end();
  }

  enum {
    auth_failure = 13014,
    attempts_failure = 12009,
  };

private:
  const std::unordered_set<int64_t> retriable_errors{
      auth_failure,
      attempts_failure,
  };
};

// Data type for managing iterators.
struct IterQueryHandler {
  std::string metadata;
  v8::Local<v8::Function> callback;
};

struct BlockingQueryHandler {
  std::string metadata;
  std::vector<std::string> rows;
};

struct QueryHandler {
  std::string hash;
  std::string query;
  lcb_t instance = nullptr;
  IterQueryHandler *iter_handler = nullptr;
  BlockingQueryHandler *block_handler = nullptr;
  std::unordered_map<std::string, std::string> *named_params = nullptr;
};

// Data type for cookie to be used during row callback execution.
struct HandlerCookie {
  bool must_retry{false};
  std::string error;
  v8::Isolate *isolate{nullptr};
  lcb_N1QLHANDLE handle{nullptr};
};

// Pool of lcb instances and routines for pool management.
class ConnectionPool {
public:
  ConnectionPool(v8::Isolate *isolate, int capacity, std::string cb_kv_endpoint,
                 std::string cb_source_bucket);

  void Restore(lcb_t instance) { instances_.push(instance); }
  lcb_t GetResource();
  static void Error(lcb_t instance, const char *msg, lcb_error_t err);
  ~ConnectionPool();

private:
  void AddResource();

  const int capacity_;
  int inst_count_;
  std::string conn_str_;
  std::queue<lcb_t> instances_;
  v8::Isolate *isolate_;
};

// Data structure for maintaining the operations.
// Each stack element is hashed, providing access through both the ADTs - Stack
// and HashMap.
class HashedStack {
public:
  HashedStack() = default;
  void Push(QueryHandler &q_handler);
  void Pop();
  QueryHandler Top() { return qstack_.top(); }
  QueryHandler *Get(const std::string &index_hash) { return qmap_[index_hash]; }
  // TODO : Deduce return type
  int Size() { return static_cast<int>(qstack_.size()); }

private:
  std::stack<QueryHandler> qstack_;
  std::unordered_map<std::string, QueryHandler *> qmap_;
};

struct ErrorCodesInfo : Info {
  ErrorCodesInfo(const Info &info) : Info(info.is_fatal, info.msg) {}
  ErrorCodesInfo(bool is_fatal, const std::string &msg) : Info(is_fatal, msg) {}
  ErrorCodesInfo(std::vector<int64_t> &errors) : Info(false) {
    std::swap(this->errors, errors);
  }

  std::vector<int64_t> errors;
};

class N1QLErrorExtractor {
public:
  explicit N1QLErrorExtractor(v8::Isolate *isolate);
  virtual ~N1QLErrorExtractor();

  ErrorCodesInfo GetErrorCodes(const char *err_str);

private:
  ErrorCodesInfo GetErrorCodes(const v8::Local<v8::Value> &errors_val);

  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;
};

class N1QL {
public:
  N1QL(ConnectionPool *inst_pool, v8::Isolate *isolate)
      : isolate_(isolate), inst_pool_(inst_pool) {}
  HashedStack qhandler_stack;
  // Schedules operations for execution.
  template <typename> void ExecQuery(QueryHandler &q_handler);

private:
  template <typename>
  static bool ExecQueryImpl(v8::Isolate *isolate, lcb_t &instance,
                            lcb_N1QLPARAMS *n1ql_params, std::string &err_out);

  // Callback for each row.
  template <typename>
  static void RowCallback(lcb_t instance, int callback_type,
                          const lcb_RESPN1QL *resp);
  static void HandleRowCallbackFailure(const lcb_RESPN1QL *resp,
                                       v8::Isolate *isolate,
                                       HandlerCookie *cookie);
  static bool IsStatusSuccess(const char *row);

  v8::Isolate *isolate_;
  ConnectionPool *inst_pool_;
};

void IterFunction(const v8::FunctionCallbackInfo<v8::Value> &args);
void StopIterFunction(const v8::FunctionCallbackInfo<v8::Value> &args);
void ExecQueryFunction(const v8::FunctionCallbackInfo<v8::Value> &args);
void GetReturnValueFunction(const v8::FunctionCallbackInfo<v8::Value> &args);
void SetReturnValue(const v8::FunctionCallbackInfo<v8::Value> &args,
                    v8::Local<v8::Object> return_value);

template <typename HandlerType, typename ResultType>
void AddQueryMetadata(HandlerType handler, v8::Isolate *isolate,
                      ResultType &result);

std::string AppendStackIndex(int obj_hash, v8::Isolate *isolate);
bool HasKey(const v8::FunctionCallbackInfo<v8::Value> &args, std::string key);
std::string SetUniqueHash(const v8::FunctionCallbackInfo<v8::Value> &args);
std::string GetUniqueHash(const v8::FunctionCallbackInfo<v8::Value> &args);
std::string GetBaseHash(const v8::FunctionCallbackInfo<v8::Value> &args,
                        bool &exists);
void PushScopeStack(const v8::FunctionCallbackInfo<v8::Value> &args,
                    std::string key_hash_str, std::string value_hash_str);
void PopScopeStack(const v8::FunctionCallbackInfo<v8::Value> &args);
template <typename T> v8::Local<T> ToLocal(const v8::MaybeLocal<T> &handle);
std::unordered_map<std::string, std::string>
ExtractNamedParams(const v8::FunctionCallbackInfo<v8::Value> &args);

// TODO : Currently, this method needs to be implemented by the file that is
// importing this method
//  This method will be deprecated soon
void AddLcbException(const IsolateData *isolate_data, const int code);

#endif
