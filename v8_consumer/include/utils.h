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

#ifndef UTILS_H
#define UTILS_H

#include <unistd.h>
#include <vector>

#include <libplatform/libplatform.h>
#include <v8.h>

#include <libcouchbase/api3.h>
#include <libcouchbase/couchbase.h>

#include "log.h"

#define EXCEPTION_STR_SIZE 20
#define MAXPATHLEN 256

int WinSprintf(char **strp, const char *fmt, ...);

v8::Local<v8::String> v8Str(v8::Isolate *isolate, const char *str);
std::string ObjectToString(v8::Local<v8::Value> value);

std::string ToString(v8::Isolate *isolate, v8::Handle<v8::Value> object);

lcb_t *UnwrapLcbInstance(v8::Local<v8::Object> object);
lcb_t *UnwrapV8WorkerLcbInstance(v8::Local<v8::Object> object);

const char *ToCString(const v8::String::Utf8Value &value);
bool ToCBool(const v8::Local<v8::Boolean> &value);

std::string ConvertToISO8601(std::string timestamp);

bool isFuncReference(const v8::FunctionCallbackInfo<v8::Value> &args, int i);
std::string ExceptionString(v8::Isolate *isolate, v8::TryCatch *try_catch);

std::vector<std::string> split(const std::string &s, char delimiter);

#endif
