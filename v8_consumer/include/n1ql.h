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
  std::string version;
  std::string level;
  std::string area;
};

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
  static void HandleRowCallbackFailure(lcb_t instance, const lcb_RESPN1QL *resp,
                                       v8::Isolate *isolate,
                                       HandlerCookie *cookie);
  static bool IsStatusSuccess(const char *row);
  v8::Isolate *isolate_;
  ConnectionPool *inst_pool_;
};

struct TranspiledInfo {
  TranspiledInfo(v8::Isolate *isolate, const v8::Local<v8::Context> &context,
                 const v8::Local<v8::Value> &transpiler_result);
  ~TranspiledInfo();
  bool ReplaceSource(const std::string &handler_code);

  std::string transpiled_code;
  std::string source_map;

private:
  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;
};

class Transpiler {
public:
  Transpiler(v8::Isolate *isolate, const std::string &transpiler_src,
             const std::vector<std::string> &handler_headers,
             const std::vector<std::string> &handler_footers);
  ~Transpiler();

  v8::Local<v8::Value> ExecTranspiler(const std::string &function,
                                      v8::Local<v8::Value> args[],
                                      const int &args_len);
  CompilationInfo Compile(const std::string &plain_js);
  std::string Transpile(const std::string &jsified_code,
                        const std::string &src_filename,
                        const std::string &handler_code);
  std::string JsFormat(const std::string &handler_code);
  std::string GetSourceMap(const std::string &handler_code,
                           const std::string &src_filename);
  std::string TranspileQuery(const std::string &query,
                             const NamedParamsInfo &info);
  UniLineN1QLInfo UniLineN1QL(const std::string &handler_code);
  CodeVersion GetCodeVersion(const std::string &handler_code);
  bool IsTimerCalled(const std::string &handler_code);
  bool IsJsExpression(const std::string &str);
  static void LogCompilationInfo(const CompilationInfo &info);

private:
  std::string AppendSourceMap(const TranspiledInfo &info);
  static void Log(const v8::FunctionCallbackInfo<v8::Value> &args);
  void RectifyCompilationInfo(CompilationInfo &info,
                              const std::list<InsertedCharsInfo> &n1ql_pos);
  CompilationInfo ComposeErrorInfo(const CommentN1QLInfo &cmt_info);
  CompilationInfo
  ComposeCompilationInfo(v8::Local<v8::Value> &compiler_result,
                         const std::list<InsertedCharsInfo> &ins_list);
  std::string ComposeDescription(int code);

  v8::Persistent<v8::Context> context_;
  v8::Isolate *isolate_;
  std::string transpiler_src_;
  std::vector<std::string> handler_headers_;
  std::vector<std::string> handler_footers_;
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
int32_t CountNewLines(const std::string &str, const int32_t from = 0);
int32_t CountStr(const std::string &needle, const std::string &haystack,
                 const int32_t from = 0);

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
