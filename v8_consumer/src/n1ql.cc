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

#include <include/libplatform/libplatform.h>
#include <include/v8.h>
#include <iostream>
#include <libcouchbase/api3.h>
#include <libcouchbase/couchbase.h>
#include <libcouchbase/n1ql.h>
#include <string>
#include <vector>

#include "../include/log.h"
#include "../include/n1ql.h"

// A flag to signal the stop of iteration.
extern bool stop_signal;
// Reference to the query engine instantiated by v8worker.
extern N1QL *n1ql_handle;

std::vector<std::string> rows;
v8::Local<v8::Function> callback;
bool is_callback_set = false;
bool stop_signal = false;

N1QL::N1QL(std::string cb_kv_endpoint, std::string cb_source_bucket,
           std::string rbac_user, std::string rbac_pass) {

  std::string conn_str =
      "couchbase://" + cb_kv_endpoint + "/" + cb_source_bucket + "?username=" +
      rbac_user +
      "&console_log_level=5&detailed_errcodes=true&select_bucket=true";
  LOG(logInfo) << "N1QL: connstr " << conn_str << '\n';

  lcb_create_st options;
  lcb_error_t err;
  memset(&options, 0, sizeof(options));
  options.version = 3;
  options.v.v3.connstr = conn_str.c_str();
  options.v.v3.type = LCB_TYPE_BUCKET;
  options.v.v3.passwd = rbac_pass.c_str();

  err = lcb_create(&instance, &options);
  if (err != LCB_SUCCESS) {
    init_success = false;
    Error(instance, "N1QL: unable to create lcb handle", err);
  }

  err = lcb_connect(instance);
  if (err != LCB_SUCCESS) {
    init_success = false;
    Error(instance, "N1QL: unable to connect to server", err);
  }

  lcb_wait(instance);

  err = lcb_get_bootstrap_status(instance);
  if (err != LCB_SUCCESS) {
    init_success = false;
    Error(instance, "N1QL: unable to get bootstrap status", err);
  }
}

N1QL::~N1QL() { lcb_destroy(instance); }

void N1QL::Error(lcb_t instance, const char *msg, lcb_error_t err) {
  LOG(logError) << "error: " << err << " " << lcb_strerror(instance, err)
                << '\n';
}

void N1QL::ExecQuery(std::string query, v8::Local<v8::Function> function) {
  is_callback_set = true;
  callback = function;
  ExecQuery(query);
}

// TODO: Pull out 'rows.clear' and 'return rows;' and turn that to a separate
// function.
std::vector<std::string> N1QL::ExecQuery(std::string query) {
  rows.clear();

  lcb_error_t err;
  lcb_CMDN1QL cmd = {0};
  lcb_N1QLPARAMS *n1ql_params = lcb_n1p_new();
  err = lcb_n1p_setstmtz(n1ql_params, query.c_str());
  if (err != LCB_SUCCESS)
    Error(instance, "unable to build query string", err);

  lcb_n1p_mkcmd(n1ql_params, &cmd);

  cmd.callback = RowCallback;
  err = lcb_n1ql_query(instance, NULL, &cmd);
  if (err != LCB_SUCCESS)
    Error(instance, "unable to query", err);

  lcb_n1p_free(n1ql_params);
  lcb_wait(instance);

  return rows;
}

// Callback to execute for each row.
void N1QL::RowCallback(lcb_t instance, int callback_type,
                       const lcb_RESPN1QL *resp) {
  // If stop_signal is set, then just breakout.
  if (stop_signal) {
    lcb_breakout(instance);
    return;
  }

  if (!(resp->rflags & LCB_RESP_F_FINAL)) {
    char *temp;
    asprintf(&temp, "%.*s\n", (int)resp->nrow, resp->row);

    // Execute the function callback passed in JavaScript, if iter() is
    // called.
    if (is_callback_set) {
      v8::Isolate *isolate = v8::Isolate::GetCurrent();
      v8::Local<v8::Object> json =
          isolate->GetCurrentContext()
              ->Global()
              ->Get(v8::String::NewFromUtf8(isolate, "JSON"))
              ->ToObject();
      v8::Local<v8::Function> parse =
          json->Get(v8::String::NewFromUtf8(isolate, "parse"))
              .As<v8::Function>();

      v8::Local<v8::Value> args[1];
      args[0] = v8::String::NewFromUtf8(isolate, temp);
      args[0] = parse->Call(json, 1, &args[0]);

      callback->Call(callback, 1, args);
    } else {
      // If it is not a callback - execQuery() is called, append the
      // result to the rows vector.
      rows.push_back(std::string(temp));
    }

    free(temp);
  } else {
    LOG(logDebug) << "query metadata:" << resp->row << '\n';
  }
}

// Iterator function for iterating over the results.
void IterFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = v8::Isolate::GetCurrent();
  v8::HandleScope handleScope(isolate);

  v8::Local<v8::Name> query_name = v8::String::NewFromUtf8(isolate, "query");
  v8::Local<v8::Value> query_value = args.This()->Get(query_name);
  v8::String::Utf8Value query_string(query_value);

  v8::Local<v8::Function> func = v8::Local<v8::Function>::Cast(args[0]);

  // Pass the function to callback and execute the query.
  n1ql_handle->ExecQuery(*query_string, func);
}

// Signals stop to the iterator.
void StopIterFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  stop_signal = true;
}

// Executes the query, blocking call.
void ExecQueryFunction(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = v8::Isolate::GetCurrent();
  v8::HandleScope handleScope(isolate);

  v8::Local<v8::Name> query_name = v8::String::NewFromUtf8(isolate, "query");
  v8::Local<v8::Value> query_value = args.This()->Get(query_name);
  v8::String::Utf8Value query_string(query_value);

  std::vector<std::string> rows = n1ql_handle->ExecQuery(*query_string);

  v8::Local<v8::Array> result_array =
      v8::Array::New(isolate, static_cast<int>(rows.size()));

  // Populate the result array with the rows of the result.
  for (int i = 0; i < rows.size(); ++i) {
    v8::Local<v8::Value> json_row =
        v8::JSON::Parse(v8::String::NewFromUtf8(isolate, rows[i].c_str()));
    result_array->Set(static_cast<uint32_t>(i), json_row);
  }

  args.GetReturnValue().Set(result_array);
}
