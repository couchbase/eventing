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

#ifndef TRANSPILER_H
#define TRANSPILER_H

#include <list>
#include <string>
#include <v8.h>
#include <vector>

#include "comm.h"

enum Jsify {
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

// Insertion types for CommentN1QL
enum class InsertType { kN1QLBegin, kN1QLEnd };

// Code version of handler
struct CodeVersion {
  std::string version;
  std::string level;
  std::string using_timer;
};

// Keeps track of the type of literal inserted during CommentN1QL
struct InsertedCharsInfo {
  InsertedCharsInfo(InsertType type)
      : type(type), type_len(0), line_no(0), index(0) {}

  InsertType type;
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
             const std::vector<std::string> &handler_footers,
             const std::string &source_bucket = "");
  ~Transpiler();

  v8::Local<v8::Value> ExecTranspiler(const std::string &function,
                                      v8::Local<v8::Value> args[],
                                      const int &args_len);
  CompilationInfo Compile(const std::string &plain_js);
  std::string Transpile(const std::string &jsified_code,
                        const std::string &src_filename,
                        const std::string &handler_code);
  std::string TranspileQuery(const std::string &query,
                             const NamedParamsInfo &info);
  UniLineN1QLInfo UniLineN1QL(const std::string &handler_code);
  CodeVersion GetCodeVersion(const std::string &handler_code);
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
  const std::string source_bucket_;
  std::vector<std::string> handler_headers_;
  std::vector<std::string> handler_footers_;
};

// Functions usable from jsify.lex
JsifyInfo Jsify(const std::string &input, bool validate_source_bucket,
                const std::string &source_bucket);
UniLineN1QLInfo UniLineN1QL(const std::string &info,
                            bool validate_source_bucket,
                            const std::string &source_bucket);
CommentN1QLInfo CommentN1QL(const std::string &input,
                            bool validate_source_bucket,
                            const std::string &source_bucket);

#endif
