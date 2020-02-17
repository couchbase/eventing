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
  std::string AddHeadersAndFooters(const std::string &handler_code);
  UniLineN1QLInfo UniLineN1QL(const std::string &handler_code);
  CodeVersion GetCodeVersion(const std::string &handler_code);
  static void LogCompilationInfo(const CompilationInfo &info);

private:
  static void Log(const v8::FunctionCallbackInfo<v8::Value> &args);
  CompilationInfo
  ComposeCompilationInfo(v8::Local<v8::Value> &compiler_result);
  std::string ComposeDescription(int code);

  v8::Persistent<v8::Context> context_;
  v8::Isolate *isolate_;
  std::string transpiler_src_;
  const std::string source_bucket_;
  std::vector<std::string> handler_headers_;
  std::vector<std::string> handler_footers_;
};

#endif
