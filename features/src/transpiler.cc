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

#include "transpiler.h"
#include "log.h"
#include "retry_util.h"
#include "utils.h"

Transpiler::Transpiler(v8::Isolate *isolate, const std::string &transpiler_src,
                       const std::vector<std::string> &handler_headers,
                       const std::vector<std::string> &handler_footers,
                       const std::string &source_bucket)
    : isolate_(isolate), transpiler_src_(transpiler_src),
      source_bucket_(source_bucket), handler_headers_(handler_headers),
      handler_footers_(handler_footers) {
  auto global = v8::ObjectTemplate::New(isolate);
  global->Set(v8Str(isolate, "log"),
              v8::FunctionTemplate::New(isolate, Transpiler::Log));
  auto context = v8::Context::New(isolate, nullptr, global);
  context_.Reset(isolate, context);
}

Transpiler::~Transpiler() { context_.Reset(); }

void Transpiler::Log(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::Locker locker(isolate);
  v8::HandleScope handle_scope(isolate);

  std::string log_msg;
  for (auto i = 0; i < args.Length(); i++) {
    log_msg += JSONStringify(isolate, args[i]);
    log_msg += " ";
  }

  std::cerr << log_msg << std::endl;
}

v8::Local<v8::Value> Transpiler::ExecTranspiler(const std::string &function,
                                                v8::Local<v8::Value> args[],
                                                const int &args_len) {
  v8::EscapableHandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  v8::Context::Scope context_scope(context);

  v8::Local<v8::Value> result;
  auto source = v8Str(isolate_, transpiler_src_);
  auto global = context->Global();

  v8::Local<v8::Script> script;
  if (!TO_LOCAL(v8::Script::Compile(context, source), &script)) {
    return handle_scope.Escape(result);
  }

  if (!TO_LOCAL(script->Run(context), &result)) {
    return handle_scope.Escape(result);
  }

  auto function_name = v8Str(isolate_, function);
  v8::Local<v8::Value> function_def;
  if (!TO_LOCAL(global->Get(context, function_name), &function_def)) {
    return handle_scope.Escape(result);
  }

  RetryWithFixedBackoff(std::numeric_limits<int>::max(), 10,
                        IsTerminatingRetriable, IsExecutionTerminating,
                        isolate_);

  auto function_ref = function_def.As<v8::Function>();
  TO_LOCAL(function_ref->Call(context, function_ref, args_len, args), &result);
  return handle_scope.Escape(result);
}

CompilationInfo Transpiler::Compile(const std::string &n1ql_js_src) {
  v8::HandleScope handle_scope(isolate_);
  v8::Local<v8::Value> args[3];
  args[0] = v8Str(isolate_, n1ql_js_src);
  args[1] = v8Array(isolate_, handler_headers_);
  args[2] = v8Array(isolate_, handler_footers_);
  auto result = ExecTranspiler("compile", args, 3);

  return ComposeCompilationInfo(result);
}

std::string Transpiler::AddHeadersAndFooters(const std::string &handler_code) {
  v8::HandleScope handle_scope(isolate_);
  v8::Local<v8::Value> args[3];
  args[0] = v8Str(isolate_, handler_code);
  args[1] = v8Array(isolate_, handler_headers_);
  args[2] = v8Array(isolate_, handler_footers_);
  auto result = ExecTranspiler("AddHeadersAndFooters", args, 3);

  v8::String::Utf8Value utf8result(isolate_, result);
  return *utf8result;
}

CodeVersion Transpiler::GetCodeVersion(const std::string &handler_code) {
  v8::HandleScope handle_scope(isolate_);
  auto context = isolate_->GetCurrentContext();

  v8::Local<v8::Value> args[1];
  args[0] = v8Str(isolate_, handler_code);
  auto res = ExecTranspiler("getCodeVersion", args, 1);
  auto ans = res.As<v8::Array>();

  v8::Local<v8::Value> version_val;
  if (!TO_LOCAL(ans->Get(context, 0), &version_val)) {
    return CodeVersion{};
  }

  v8::Local<v8::Value> level_val;
  if (!TO_LOCAL(ans->Get(context, 1), &level_val)) {
    return CodeVersion{};
  }

  v8::Local<v8::Value> using_timer_val;
  if (!TO_LOCAL(ans->Get(context, 2), &using_timer_val)) {
    return CodeVersion{};
  }

  v8::String::Utf8Value version(isolate_, version_val);
  v8::String::Utf8Value level(isolate_, level_val);
  v8::String::Utf8Value using_timer(isolate_, using_timer_val);

  return CodeVersion{*version, *level, *using_timer};
}

void Transpiler::LogCompilationInfo(const CompilationInfo &info) {
  if (info.compile_success) {
    LOG(logInfo) << "Compilation successful."
                 << " Language: " << info.language << std::endl;
  } else {
    LOG(logInfo) << "Syntax error. Language: " << info.language
                 << " Index: " << info.index << " Line number: " << info.line_no
                 << " Column number: " << info.col_no
                 << " Description: " << info.description << std::endl;
  }
}

// Composes compilation info returned by transpiler.js
CompilationInfo
Transpiler::ComposeCompilationInfo(v8::Local<v8::Value> &compiler_result) {
  if (IS_EMPTY(compiler_result)) {
    throw "Result of ExecTranspiler is empty";
  }

  CompilationInfo info;
  info.compile_success = false;
  info.description = "Internal error";

  v8::HandleScope handle_scope(isolate_);
  auto context = isolate_->GetCurrentContext();

  v8::Local<v8::Object> res_obj;
  if (!TO_LOCAL(compiler_result->ToObject(context), &res_obj)) {
    return info;
  }

  // Extract info returned from JavaScript compilation
  v8::Local<v8::Value> language;
  if (!TO_LOCAL(res_obj->Get(context, v8Str(isolate_, "language")),
                &language)) {
    return info;
  }

  v8::Local<v8::Value> area;
  if (!TO_LOCAL(res_obj->Get(context, v8Str(isolate_, "area")), &area)) {
    return info;
  }

  v8::Local<v8::Value> compilation_status_val;
  if (!TO_LOCAL(res_obj->Get(context, v8Str(isolate_, "compileSuccess")),
                &compilation_status_val)) {
    return info;
  }

  v8::Local<v8::Boolean> compilation_status;
  if (!TO_LOCAL(compilation_status_val->ToBoolean(context),
                &compilation_status)) {
    return info;
  }

  v8::Local<v8::Value> index_val;
  if (!TO_LOCAL(res_obj->Get(context, v8Str(isolate_, "index")), &index_val)) {
    return info;
  }

  v8::Local<v8::Integer> index;
  if (!TO_LOCAL(index_val->ToInteger(context), &index)) {
    return info;
  }

  v8::Local<v8::Value> line_no_val;
  if (!TO_LOCAL(res_obj->Get(context, v8Str(isolate_, "lineNumber")),
                &line_no_val)) {
    return info;
  }

  v8::Local<v8::Integer> line_no;
  if (!TO_LOCAL(line_no_val->ToInteger(context), &line_no)) {
    return info;
  }

  v8::Local<v8::Value> col_no_val;
  if (!TO_LOCAL(res_obj->Get(context, v8Str(isolate_, "columnNumber")),
                &col_no_val)) {
    return info;
  }

  v8::Local<v8::Integer> col_no;
  if (!TO_LOCAL(col_no_val->ToInteger(context), &col_no)) {
    return info;
  }

  v8::Local<v8::Value> description;
  if (!TO_LOCAL(res_obj->Get(context, v8Str(isolate_, "description")),
                &description)) {
    return info;
  }

  v8::String::Utf8Value lang_str(isolate_, language);
  info.language = *lang_str;
  info.compile_success = compilation_status->Value();

  if (info.compile_success) {
    info.description = "Compilation success";
    return info;
  }

  // Compilation failed, attach more info
  v8::String::Utf8Value desc_str(isolate_, description);
  info.description = *desc_str;

  v8::String::Utf8Value area_str(isolate_, area);
  info.area = *area_str;

  info.index = static_cast<int32_t>(index->Value());
  info.line_no = static_cast<int32_t>(line_no->Value());
  info.col_no = static_cast<int32_t>(col_no->Value());

  return info;
}

std::string Transpiler::ComposeDescription(int code) {
  std::string keyword;
  switch (code) {
  case Jsify::kKeywordAlter:
    keyword = "alter";
    break;

  case Jsify::kKeywordBuild:
    keyword = "build";
    break;

  case Jsify::kKeywordCreate:
    keyword = "create";
    break;

  case Jsify::kKeywordDelete:
    keyword = "delete";
    break;

  case Jsify::kKeywordDrop:
    keyword = "drop";
    break;

  case Jsify::kKeywordExecute:
    keyword = "execute";
    break;

  case Jsify::kKeywordExplain:
    keyword = "explain";
    break;

  case Jsify::kKeywordFrom:
    keyword = "from";
    break;

  case Jsify::kKeywordGrant:
    keyword = "grant";
    break;

  case Jsify::kKeywordInfer:
    keyword = "infer";
    break;

  case Jsify::kKeywordInsert:
    keyword = "insert";
    break;

  case Jsify::kKeywordMerge:
    keyword = "merge";
    break;

  case Jsify::kKeywordPrepare:
    keyword = "prepare";
    break;

  case Jsify::kKeywordRename:
    keyword = "rename";
    break;

  case Jsify::kKeywordRevoke:
    keyword = "revoke";
    break;

  case Jsify::kKeywordSelect:
    keyword = "select";
    break;

  case Jsify::kKeywordUpdate:
    keyword = "update";
    break;

  case Jsify::kKeywordUpsert:
    keyword = "upsert";
    break;

  default:
    std::string msg = "No keyword exists for code " + std::to_string(code);
  }

  std::string description = keyword + " is a reserved name in N1QLJs";
  return description;
}
