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

#include <list>
#include <map>
#include <queue>
#include <stack>
#include <string>
#include <v8.h>
#include <vector>

#include "libcouchbase/couchbase.h"
#include "libcouchbase/n1ql.h"

#include "log.h"
#include "v8worker.h"

enum op_code {
  kOK,
  kKeywordAlter,
  kKeywordBuild,
  kKeywordCreate,
  kKeywordDelete,
  kKeywordDrop,
  kKeywordExecute,
  kKeywordExplain,
  kKeywordFrom,
  kKeywordGrant,
  kKeywordInfer,
  kKeywordInsert,
  kKeywordMerge,
  kKeywordPrepare,
  kKeywordRename,
  kKeywordRevoke,
  kKeywordSelect,
  kKeywordUpdate,
  kKeywordUpsert
};

enum lex_op_code { kJsify, kUniLineN1QL, kCommentN1QL };

// Insertion types for CommentN1QL
enum class insert_type { kN1QLBegin, kN1QLEnd };

// Keeps track of the type of literal inserted during CommentN1QL
struct InsertedCharsInfo {
  InsertedCharsInfo(insert_type type)
      : type(type), type_len(0), line_no(0), index(0) {}

  insert_type type;
  int type_len;
  int32_t line_no;
  int32_t index;
};

// Represents position of each char in the source code
struct Pos {
  Pos() : line_no(0), col_no(0), index(0) {}

  int32_t line_no;
  int32_t col_no;
  int32_t index;
};

// Represents compilation status
struct CompilationInfo {
  CompilationInfo() : compile_success(false), index(0), line_no(0), col_no(0) {}

  std::string language;
  bool compile_success;
  int32_t index;
  int32_t line_no;
  int32_t col_no;
  std::string description;
};

// Data type for managing iterators.
struct IterQueryHandler {
  std::string metadata;
  v8::Local<v8::Function> callback;
  v8::Local<v8::Value> return_value;
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
  std::list<std::string> *pos_params = nullptr;
};

// Data type for cookie to be used during row callback execution.
struct HandlerCookie {
  v8::Isolate *isolate = nullptr;
  lcb_N1QLHANDLE handle = nullptr;
};

// Pool of lcb instances and routines for pool management.
class ConnectionPool {
private:
  const int capacity;
  int inst_count;
  std::string conn_str;
  std::string rbac_pass;
  std::queue<lcb_t> instances;
  void AddResource();

public:
  ConnectionPool(int capacity, std::string cb_kv_endpoint,
                 std::string cb_source_bucket, std::string rbac_user,
                 std::string rbac_pass);
  void Restore(lcb_t instance) { instances.push(instance); }
  lcb_t GetResource();
  static void Error(lcb_t instance, const char *msg, lcb_error_t err);
  ~ConnectionPool();
};

// Data structure for maintaining the operations.
// Each stack element is hashed, providing access through both the ADTs - Stack
// and HashMap.
class HashedStack {
  std::stack<QueryHandler> qstack;
  std::map<std::string, QueryHandler *> qmap;

public:
  HashedStack() {}
  void Push(QueryHandler &q_handler);
  void Pop();
  QueryHandler Top() { return qstack.top(); }
  QueryHandler *Get(std::string index_hash) { return qmap[index_hash]; }
  // TODO : Need to deduce return type
  int Size() { return static_cast<int>(qstack.size()); }
  ~HashedStack() {}
};

class N1QL {
private:
  v8::Isolate *isolate;
  ConnectionPool *inst_pool;
  // Callback for each row.
  template <typename>
  static void RowCallback(lcb_t instance, int callback_type,
                          const lcb_RESPN1QL *resp);

public:
  N1QL(ConnectionPool *inst_pool, v8::Isolate *isolate)
      : isolate(isolate), inst_pool(inst_pool) {}
  HashedStack qhandler_stack;
  std::vector<std::string> ExtractErrorMsg(const char *metadata);
  // Schedules operations for execution.
  template <typename> void ExecQuery(QueryHandler &q_handler);
  ~N1QL() {}
};

class Transpiler {
  v8::Isolate *isolate;
  v8::Local<v8::Context> context;

public:
  Transpiler(v8::Isolate *isolate, const std::string &transpiler_src);
  v8::Local<v8::Value> ExecTranspiler(const std::string &function,
                                      v8::Local<v8::Value> args[],
                                      const int &args_len);
  CompilationInfo Compile(const std::string &plain_js);
  std::string Transpile(const std::string &handler_code,
                        const std::string &src_filename,
                        const std::string &src_map_name,
                        const std::string &host_addr,
                        const std::string &eventing_port);
  std::string JsFormat(const std::string &handler_code);
  std::string GetSourceMap(const std::string &handler_code,
                           const std::string &src_filename);
  bool IsTimerCalled(const std::string &handler_code);
  static void LogCompilationInfo(const CompilationInfo &info);
  ~Transpiler() {}

private:
  void RectifyCompilationInfo(CompilationInfo &info,
                              const std::list<InsertedCharsInfo> &n1ql_pos);
  CompilationInfo
  ComposeErrorInfo(int code, const Pos &last_pos,
                   const std::list<InsertedCharsInfo> &ins_list);
  CompilationInfo
  ComposeCompilationInfo(v8::Local<v8::Value> &compiler_result,
                         const std::list<InsertedCharsInfo> &ins_list);
  std::string ComposeDescription(int code);
};

int Jsify(const char *input, std::string *output, Pos *last_pos_out);
int UniLineN1QL(const char *input, std::string *output, Pos *last_pos_out);
int CommentN1QL(const char *input, std::string *output,
                std::list<InsertedCharsInfo> *pos_out, Pos *last_pos_out);

void HandleStrStart(int state);
void HandleStrStop(int state);
bool IsEsc();
void UpdatePos(insert_type type);
void UpdatePos(Pos *pos);

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
std::list<std::string>
ExtractPosParams(const v8::FunctionCallbackInfo<v8::Value> &args);

#endif
