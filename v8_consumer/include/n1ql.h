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
#include <v8.h>
#include <vector>

#include "comm.h"
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
  kKeywordUpsert,
  kN1QLParserError
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

struct JsifyInfo {
  int code;
  std::string handler_code;
  Pos last_pos;
};

struct UniLineN1QLInfo {
  int code;
  std::string handler_code;
  Pos last_pos;
};

struct CommentN1QLInfo {
  int code;
  std::string handler_code;
  std::list<InsertedCharsInfo> insertions;
  Pos last_pos;
  ParseInfo parse_info;
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
  std::unordered_map<std::string, std::string> *named_params = nullptr;
};

// Data type for cookie to be used during row callback execution.
struct HandlerCookie {
  v8::Isolate *isolate = nullptr;
  lcb_N1QLHANDLE handle = nullptr;
};

// Pool of lcb instances and routines for pool management.
class ConnectionPool {
public:
  ConnectionPool(v8::Isolate *isolate, int capacity, std::string cb_kv_endpoint,
                 std::string cb_source_bucket);

  void Restore(lcb_t instance) { instances.push(instance); }
  lcb_t GetResource();
  static void Error(lcb_t instance, const char *msg, lcb_error_t err);
  ~ConnectionPool();

private:
  void AddResource();

  const int capacity;
  int inst_count;
  std::string conn_str;
  std::queue<lcb_t> instances;
  v8::Isolate *isolate;
};

// Data structure for maintaining the operations.
// Each stack element is hashed, providing access through both the ADTs - Stack
// and HashMap.
class HashedStack {
public:
  HashedStack() {}
  void Push(QueryHandler &q_handler);
  void Pop();
  QueryHandler Top() { return qstack.top(); }
  QueryHandler *Get(std::string index_hash) { return qmap[index_hash]; }
  // TODO : Deduce return type
  int Size() { return static_cast<int>(qstack.size()); }

private:
  std::stack<QueryHandler> qstack;
  std::unordered_map<std::string, QueryHandler *> qmap;
};

class N1QL {
public:
  N1QL(ConnectionPool *inst_pool, v8::Isolate *isolate)
      : isolate(isolate), inst_pool(inst_pool) {}
  HashedStack qhandler_stack;
  std::vector<std::string> ExtractErrorMsg(const char *metadata);
  // Schedules operations for execution.
  template <typename> void ExecQuery(QueryHandler &q_handler);

private:
  // Callback for each row.
  template <typename>
  static void RowCallback(lcb_t instance, int callback_type,
                          const lcb_RESPN1QL *resp);

  v8::Isolate *isolate;
  ConnectionPool *inst_pool;
};

class Transpiler {
public:
  Transpiler(v8::Isolate *isolate, const std::string &transpiler_src)
      : isolate(isolate), transpiler_src(transpiler_src) {}

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
  std::string TranspileQuery(const std::string &query,
                             const std::vector<std::string> &named_params);
  bool IsTimerCalled(const std::string &handler_code);
  bool IsJsExpression(const std::string &str);
  static void LogCompilationInfo(const CompilationInfo &info);

private:
  void RectifyCompilationInfo(CompilationInfo &info,
                              const std::list<InsertedCharsInfo> &n1ql_pos);
  CompilationInfo ComposeErrorInfo(const CommentN1QLInfo &cmt_info);
  CompilationInfo
  ComposeCompilationInfo(v8::Local<v8::Value> &compiler_result,
                         const std::list<InsertedCharsInfo> &ins_list);
  std::string ComposeDescription(int code);

  v8::Isolate *isolate;
  std::string transpiler_src;
};

// Function prototypes of jsify.lex
JsifyInfo Jsify(const std::string &input);
UniLineN1QLInfo UniLineN1QL(const std::string &info);
CommentN1QLInfo CommentN1QL(const std::string &input);

void HandleStrStart(int state);
void HandleStrStop(int state);
bool IsEsc(const std::string &str);
void UpdatePos(insert_type type);
void UpdatePos(Pos *pos);
std::string TranspileQuery(const std::string &query);
void ReplaceRecentChar(std::string &str, char m, char n);
ParseInfo ParseQuery(const std::string &query);

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

#endif
