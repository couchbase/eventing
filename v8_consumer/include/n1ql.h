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

#include <include/v8.h>
#include <vector>

#include "libcouchbase/couchbase.h"
#include "libcouchbase/n1ql.h"

enum op_code {
  OK,
  KWD_ALTER,
  KWD_BUILD,
  KWD_CREATE,
  KWD_DELETE,
  KWD_DROP,
  KWD_EXECUTE,
  KWD_EXPLAIN,
  KWD_GRANT,
  KWD_INFER,
  KWD_INSERT,
  KWD_MERGE,
  KWD_PREPARE,
  KWD_RENAME,
  KWD_REVOKE,
  KWD_SELECT,
  KWD_UPDATE,
  KWD_UPSERT
};

enum builder_mode { EXEC_JS_FORMAT, EXEC_TRANSPILER };

int Jsify(const char *, std::string *);
std::string Transpile(std::string, std::string, int);

class N1QL {
private:
  bool init_success = true;
  lcb_t instance;
  static void RowCallback(lcb_t, int, const lcb_RESPN1QL *);
  static void Error(lcb_t, const char *, lcb_error_t);

public:
  N1QL(std::string);
  bool GetInitStatus() { return init_success; }
  std::vector<std::string> ExecQuery(std::string);
  void ExecQuery(std::string, v8::Local<v8::Function>);
  virtual ~N1QL();
};

void IterFunction(const v8::FunctionCallbackInfo<v8::Value> &);
void StopIterFunction(const v8::FunctionCallbackInfo<v8::Value> &);
void ExecQueryFunction(const v8::FunctionCallbackInfo<v8::Value> &);
void N1qlQueryConstructor(const v8::FunctionCallbackInfo<v8::Value> &);

#endif
