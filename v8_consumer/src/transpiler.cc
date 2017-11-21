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

#include "../include/log.h"
#include "../include/n1ql.h"
#include "v8.h"

Transpiler::Transpiler(v8::Isolate *isolate_, const std::string &transpiler_src)
    : isolate(isolate_) {
  v8::EscapableHandleScope handle_scope(isolate_);
  v8::Local<v8::ObjectTemplate> global = v8::ObjectTemplate::New(isolate_);

  context = v8::Context::New(isolate_, NULL, global);
  v8::Context::Scope context_scope(context);
  auto source = v8Str(isolate_, transpiler_src.c_str());
  auto script = v8::Script::Compile(context, source).ToLocalChecked();
  script->Run(context).ToLocalChecked();

  this->context = handle_scope.Escape(context);
}

v8::Local<v8::Value> Transpiler::ExecTranspiler(const std::string &function,
                                                v8::Local<v8::Value> args[],
                                                const int &args_len) {
  v8::EscapableHandleScope handle_scope(isolate);
  v8::Context::Scope context_scope(context);
  auto function_name = v8Str(isolate, function.c_str());
  auto function_def = context->Global()->Get(function_name);
  auto function_ref = v8::Local<v8::Function>::Cast(function_def);
  auto result = function_ref->Call(function_ref, args_len, args);

  return handle_scope.Escape(result);
}

CompilationInfo Transpiler::Compile(const std::string &n1ql_js_src) {
  std::string js_src;
  Pos last_pos;
  std::list<InsertedCharsInfo> insertions;
  // Comment-out N1QL queries and obtain the list of insertions that was made
  auto code = CommentN1QL(n1ql_js_src.c_str(), &js_src, &insertions, &last_pos);
  if (code != kOK) {
    return ComposeErrorInfo(code, last_pos, insertions);
  }

  // CommentN1QL went through fine, move ahead to check JavaScript errors
  v8::HandleScope handle_scope(isolate);
  v8::Local<v8::Value> args[1];
  args[0] = v8Str(isolate, js_src.c_str());
  auto result = ExecTranspiler("compile", args, 1);
  return ComposeCompilationInfo(result, insertions);
}

// Rectify line and column offset by discounting the insertions
void Transpiler::RectifyCompilationInfo(
    CompilationInfo &info, const std::list<InsertedCharsInfo> &insertions) {
  for (const auto &pos : insertions) {
    // Discount the index from info only if it's after the insertion
    if (pos.index < info.index) {
      info.index -= pos.type_len;
      // Discount the column number only if it's in the same line as the
      // insertion and is after the insertion (checked by the enclosing if)
      if (pos.line_no == info.line_no) {
        info.col_no -= pos.type_len;
      }
    }
  }
}

std::string Transpiler::Transpile(const std::string &handler_code,
                                  const std::string &src_filename,
                                  const std::string &src_map_name,
                                  const std::string &host_addr,
                                  const std::string &eventing_port) {
  v8::HandleScope handle_scope(isolate);
  v8::Local<v8::Value> args[2];
  args[0] = v8Str(isolate, handler_code.c_str());
  args[1] = v8Str(isolate, src_filename.c_str());
  auto result = ExecTranspiler("transpile", args, 2);
  v8::String::Utf8Value utf8result(result);

  std::string src_transpiled = *utf8result;
  src_transpiled += "\n//# sourceMappingURL=http://" + host_addr + ":" +
                    eventing_port + "/debugging/" + src_map_name;

  return src_transpiled;
}

std::string Transpiler::JsFormat(const std::string &handler_code) {
  v8::HandleScope handle_scope(isolate);
  v8::Local<v8::Value> args[1];
  args[0] = v8Str(isolate, handler_code.c_str());
  auto result = ExecTranspiler("jsFormat", args, 1);
  v8::String::Utf8Value utf8result(result);

  return *utf8result;
}

std::string Transpiler::GetSourceMap(const std::string &handler_code,
                                     const std::string &src_filename) {
  v8::HandleScope handle_scope(isolate);
  v8::Local<v8::Value> args[2];
  args[0] = v8Str(isolate, handler_code.c_str());
  args[1] = v8Str(isolate, src_filename.c_str());
  auto result = ExecTranspiler("getSourceMap", args, 2);
  v8::String::Utf8Value utf8result(result);

  return *utf8result;
}

bool Transpiler::IsTimerCalled(const std::string &handler_code) {
  v8::HandleScope handle_scope(isolate);
  v8::Local<v8::Value> args[1];
  args[0] = v8Str(isolate, handler_code.c_str());
  auto result = ExecTranspiler("isTimerCalled", args, 1);
  auto bool_result = v8::Local<v8::Boolean>::Cast(result);

  return ToCBool(bool_result);
}

void Transpiler::LogCompilationInfo(const CompilationInfo &info) {
  if (info.compile_success) {
    LOG(logInfo) << "Compilation successful."
                 << " Language: " << info.language << '\n';
  } else {
    LOG(logInfo) << "Syntax error. Language: " << info.language
                 << " Index: " << info.index << " Line number: " << info.line_no
                 << " Column number: " << info.col_no
                 << " Description: " << info.description << '\n';
  }
}

// Composes error info based on the code and recent position returned by
// CommentN1QL
CompilationInfo
Transpiler::ComposeErrorInfo(int code, const Pos &last_pos,
                             const std::list<InsertedCharsInfo> &ins_list) {
  CompilationInfo info;
  info.compile_success = false;
  info.language = "JavaScript";
  info.line_no = last_pos.line_no;
  info.col_no = last_pos.col_no;
  info.index = last_pos.index;
  info.description = ComposeDescription(code);

  // Rectify position info
  RectifyCompilationInfo(info, ins_list);
  return info;
}

// Composes compilation info returned by transpiler.js
CompilationInfo Transpiler::ComposeCompilationInfo(
    v8::Local<v8::Value> &compiler_result,
    const std::list<InsertedCharsInfo> &insertions) {
  if (compiler_result.IsEmpty()) {
    throw "Result of ExecTranspiler is empty";
  }

  auto res_obj = compiler_result->ToObject();
  // Extract info returned from JavaScript compilation
  auto language = res_obj->Get(v8Str(isolate, "language"))->ToString();
  auto compilation_status =
      res_obj->Get(v8Str(isolate, "compileSuccess"))->ToBoolean();
  auto index = res_obj->Get(v8Str(isolate, "index"))->ToInteger();
  auto line_no = res_obj->Get(v8Str(isolate, "lineNumber"))->ToInteger();
  auto col_no = res_obj->Get(v8Str(isolate, "columnNumber"))->ToInteger();
  auto description = res_obj->Get(v8Str(isolate, "description"))->ToString();

  CompilationInfo info;
  info.compile_success = compilation_status->Value();
  v8::String::Utf8Value lang_str(language);
  info.language = *lang_str;
  if (!info.compile_success) {
    // Compilation failed, attach more info
    v8::String::Utf8Value desc_str(description);
    info.description = *desc_str;
    info.index = static_cast<int32_t>(index->Value());
    info.line_no = static_cast<int32_t>(line_no->Value());
    info.col_no = static_cast<int32_t>(col_no->Value());

    // Rectify position info
    RectifyCompilationInfo(info, insertions);
  }

  return info;
}

std::string Transpiler::ComposeDescription(int code) {
  std::string keyword;
  switch (code) {
  case kKeywordAlter:
    keyword = "alter";
    break;

  case kKeywordBuild:
    keyword = "build";
    break;

  case kKeywordCreate:
    keyword = "create";
    break;

  case kKeywordDelete:
    keyword = "delete";
    break;

  case kKeywordDrop:
    keyword = "drop";
    break;

  case kKeywordExecute:
    keyword = "execute";
    break;

  case kKeywordExplain:
    keyword = "explain";
    break;

  case kKeywordGrant:
    keyword = "grant";
    break;

  case kKeywordInfer:
    keyword = "infer";
    break;

  case kKeywordInsert:
    keyword = "insert";
    break;

  case kKeywordMerge:
    keyword = "merge";
    break;

  case kKeywordPrepare:
    keyword = "prepare";
    break;

  case kKeywordRename:
    keyword = "rename";
    break;

  case kKeywordRevoke:
    keyword = "revoke";
    break;

  case kKeywordSelect:
    keyword = "select";
    break;

  case kKeywordUpdate:
    keyword = "update";
    break;

  case kKeywordUpsert:
    keyword = "upsert";
    break;

  default:
    std::string msg = "No keyword exists for code " + std::to_string(code);
    throw msg;
  }

  std::string description = keyword + " is a reserved name in N1QLJs";
  return description;
}
