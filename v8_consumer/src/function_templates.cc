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

#include "../include/function_templates.h"

void Log(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);

  auto context = isolate->GetCurrentContext();
  auto JSON = context->Global()->Get(v8Str(isolate, "JSON"))->ToObject();
  auto JSON_stringify =
      v8::Local<v8::Function>::Cast(JSON->Get(v8Str(isolate, "stringify")));

  std::string log_msg;
  v8::Local<v8::Value> json_args[1];
  for (auto i = 0; i < args.Length(); i++) {
    json_args[0] = args[i];
    auto result = JSON_stringify->Call(context->Global(), 1, json_args);
    v8::String::Utf8Value utf8_value(result);

    log_msg += *utf8_value;
    log_msg += " ";
  }

  LOG(logDebug) << log_msg << '\n';
}

// console.log for debugger - also logs to eventing.log
void ConsoleLog(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();

  Log(args);
  auto console_v8_str = v8::String::NewFromUtf8(isolate, "console");
  auto log_v8_str = v8::String::NewFromUtf8(isolate, "log");
  auto console = context->Global()
                     ->Get(console_v8_str)
                     ->ToObject(context)
                     .ToLocalChecked();
  auto log_fn = v8::Local<v8::Function>::Cast(console->Get(log_v8_str));

  v8::Local<v8::Value> log_args[CONSOLE_LOG_MAX_ARITY];
  auto i = 0;
  for (; i < args.Length() && i < CONSOLE_LOG_MAX_ARITY; ++i) {
    log_args[i] = args[i];
  }

  // Calling console.log with the args passed to log() function.
  if (i < CONSOLE_LOG_MAX_ARITY) {
    log_fn->Call(log_fn, args.Length(), log_args);
  } else {
    log_fn->Call(log_fn, CONSOLE_LOG_MAX_ARITY, log_args);
  }
}

void CreateCronTimer(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);

  if (args.Length() != 3) {
    LOG(logError) << "Cron timer needs 3 args: <callback_func> <payload> "
                     "<timeWhenToKickOff>"
                  << '\n';
    return;
  }

  std::string cb_func;
  if (isFuncReference(args, 0)) {
    v8::Local<v8::Function> func_ref = args[0].As<v8::Function>();
    v8::String::Utf8Value func_name(func_ref->GetName());
    cb_func.assign(std::string(*func_name));
  } else {
    return;
  }

  std::string opaque(JSONStringify(args.GetIsolate(), args[1]));

  v8::String::Utf8Value ts(args[2]);

  std::string start_ts, timer_entry, value;
  start_ts.assign(std::string(*ts));

  if (atoi(start_ts.c_str()) <= 0) {
    LOG(logError)
        << "Skipping cron timer callback setup, invalid start timestamp"
        << '\n';
    return;
  }

  timer_entry.assign(appName);
  timer_entry.append("::");
  timer_entry.append(ConvertToISO8601(start_ts));
  timer_entry.append("Z");
  LOG(logTrace) << "Request to register cron timer, callback_func:" << cb_func
                << " start_ts : " << timer_entry << '\n';

  // Store blob in KV store, blob structure:
  // {
  //    "callback_func": CallbackFunc,
  //    "payload": opaque
  // }

  value.assign("{\"callback_func\": \"");
  value.append(cb_func);
  value.append("\", \"payload\": ");
  value.append(opaque);
  value.append("}");

  LOG(logTrace) << "cron timer value:" << value << '\n';

  auto meta_cb_instance = UnwrapData(isolate)->meta_cb_instance;
  Result res;
  lcb_CMDSUBDOC mcmd = {0};
  LCB_CMD_SET_KEY(&mcmd, timer_entry.c_str(), timer_entry.length());

  std::vector<lcb_SDSPEC> specs;

  lcb_SDSPEC cspec = {0};
  cspec.sdcmd = LCB_SDCMD_ARRAY_ADD_LAST;
  cspec.options = LCB_SDSPEC_F_MKINTERMEDIATES;
  LCB_SDSPEC_SET_PATH(&cspec, "cron_timers", 11);
  LCB_SDSPEC_SET_VALUE(&cspec, value.c_str(), value.length());
  specs.push_back(cspec);

  lcb_SDSPEC dspec = {0};
  dspec.sdcmd = LCB_SDCMD_DICT_UPSERT;
  LCB_SDSPEC_SET_PATH(&dspec, "version", 7);
  LCB_SDSPEC_SET_VALUE(&dspec, "\"vulcan\"", 8);
  specs.push_back(dspec);

  mcmd.specs = specs.data();
  mcmd.nspecs = specs.size();
  mcmd.cmdflags = LCB_CMDSUBDOC_F_UPSERT_DOC;

  lcb_subdoc3(meta_cb_instance, &res, &mcmd);
  lcb_wait(meta_cb_instance);

  auto sleep_duration = LCB_OP_RETRY_INTERVAL;
  while (res.rc != LCB_SUCCESS) {
    non_doc_timer_create_failure++;
    LOG(logInfo) << "Cron timer create failure for doc:" << timer_entry
                 << " payload: " << opaque
                 << " lcb rc:" << lcb_strerror(NULL, res.rc)
                 << " entry id: " << non_doc_timer_create_failure
                 << " sleep_duration: " << sleep_duration * 10000 << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
    sleep_duration *= 1.5;
    lcb_subdoc3(meta_cb_instance, &res, &mcmd);
    lcb_wait(meta_cb_instance);
  }
}

void CreateDocTimer(const v8::FunctionCallbackInfo<v8::Value> &args) {
  v8::Isolate *isolate = args.GetIsolate();
  v8::HandleScope handle_scope(isolate);

  std::string cb_func;
  if (isFuncReference(args, 0)) {
    v8::Local<v8::Function> func_ref = args[0].As<v8::Function>();
    v8::String::Utf8Value func_name(func_ref->GetName());
    cb_func.assign(std::string(*func_name));
  } else {
    return;
  }

  v8::String::Utf8Value doc(args[1]);
  v8::String::Utf8Value ts(args[2]);

  std::string doc_id, start_ts, timer_entry;
  doc_id.assign(std::string(*doc));
  start_ts.assign(std::string(*ts));

  // If the doc not supposed to expire, skip
  // setting up timer callback for it
  if (atoi(start_ts.c_str()) == 0) {
    LOG(logError) << "Skipping timer callback setup for doc_id:" << doc_id
                  << ", won't expire" << '\n';
    return;
  }

  timer_entry.assign(appName);
  timer_entry += "::";
  timer_entry += ConvertToISO8601(start_ts);

  /* Perform XATTR operations, XATTR structure with timers:
   *
   * {
   * "eventing": {
   *              "timers": ["appname::2017-04-30ZT12:00:00::callback_func",
   * ...],
   *              "cas": ${Mutation.CAS},
   *   }
   * }
   * */
  std::string xattr_cas_path("eventing.cas");
  std::string xattr_digest_path("eventing.digest");
  std::string xattr_timer_path("eventing.timers");
  std::string mutation_cas_macro("\"${Mutation.CAS}\"");
  std::string doc_exptime("$document.exptime");
  timer_entry += "Z::";
  timer_entry += cb_func;
  timer_entry += "\"";
  timer_entry.insert(0, 1, '"');
  LOG(logTrace) << "Request to register doc timer, callback_func:" << cb_func
                << " doc_id:" << doc_id << " start_ts:" << timer_entry << '\n';

  while (true) {
    auto cb_instance = UnwrapData(isolate)->cb_instance;
    lcb_CMDSUBDOC gcmd = {0};
    LCB_CMD_SET_KEY(&gcmd, doc_id.c_str(), doc_id.size());

    // Fetch document expiration using virtual extended attributes
    Result res;
    std::vector<lcb_SDSPEC> gspecs;
    lcb_SDSPEC exp_spec, fdoc_spec = {0};

    exp_spec.sdcmd = LCB_SDCMD_GET;
    exp_spec.options = LCB_SDSPEC_F_XATTRPATH;
    LCB_SDSPEC_SET_PATH(&exp_spec, doc_exptime.c_str(), doc_exptime.size());
    gspecs.push_back(exp_spec);

    fdoc_spec.sdcmd = LCB_SDCMD_GET_FULLDOC;
    gspecs.push_back(fdoc_spec);

    gcmd.specs = gspecs.data();
    gcmd.nspecs = gspecs.size();
    lcb_subdoc3(cb_instance, &res, &gcmd);
    lcb_wait(cb_instance);

    auto sleep_duration = LCB_OP_RETRY_INTERVAL;
    while (res.rc != LCB_SUCCESS) {
      doc_timer_create_failure++;
      LOG(logError)
          << "Failed to while performing lookup for fulldoc and exptime"
          << " doc key:" << doc_id << " rc: " << lcb_strerror(NULL, res.rc)
          << '\n';
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
      sleep_duration *= 1.5;
      lcb_subdoc3(cb_instance, &res, &gcmd);
      lcb_wait(cb_instance);
    }

    uint32_t d = crc32c(0, res.value.c_str(), res.value.length());
    std::string digest = std::to_string(d);

    LOG(logTrace) << "CreateDocTimer cas: " << res.cas
                  << " exptime: " << res.exptime << " digest: " << digest
                  << '\n';

    lcb_CMDSUBDOC mcmd = {0};
    lcb_SDSPEC digest_spec, xattr_spec, tspec = {0};
    LCB_CMD_SET_KEY(&mcmd, doc_id.c_str(), doc_id.size());

    std::vector<lcb_SDSPEC> specs;

    mcmd.cas = res.cas;
    mcmd.exptime = res.exptime;

    digest_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
    digest_spec.options = LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTRPATH;

    LCB_SDSPEC_SET_PATH(&digest_spec, xattr_digest_path.c_str(),
                        xattr_digest_path.size());
    LCB_SDSPEC_SET_VALUE(&digest_spec, digest.c_str(), digest.size());
    specs.push_back(digest_spec);

    xattr_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
    xattr_spec.options =
        LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTR_MACROVALUES;

    LCB_SDSPEC_SET_PATH(&xattr_spec, xattr_cas_path.c_str(),
                        xattr_cas_path.size());
    LCB_SDSPEC_SET_VALUE(&xattr_spec, mutation_cas_macro.c_str(),
                         mutation_cas_macro.size());
    specs.push_back(xattr_spec);

    tspec.sdcmd = LCB_SDCMD_ARRAY_ADD_LAST;
    tspec.options = LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTRPATH;
    LCB_SDSPEC_SET_PATH(&tspec, xattr_timer_path.c_str(),
                        xattr_timer_path.size());
    LCB_SDSPEC_SET_VALUE(&tspec, timer_entry.c_str(), timer_entry.size());
    specs.push_back(tspec);

    mcmd.specs = specs.data();
    mcmd.nspecs = specs.size();

    lcb_subdoc3(cb_instance, &res, &mcmd);
    lcb_wait(cb_instance);
    if (res.rc != LCB_SUCCESS) {
      doc_timer_create_failure++;
      LOG(logError) << "Failed to update timer related xattr fields for doc_id:"
                    << doc_id << " return code:" << res.rc
                    << " msg:" << lcb_strerror(NULL, res.rc) << '\n';
      return;
    }

    sleep_duration = LCB_OP_RETRY_INTERVAL;
    while (res.rc != LCB_SUCCESS && res.rc != LCB_KEY_EEXISTS) {
      doc_timer_create_failure++;
      LOG(logError) << "Failed to update timer related xattr fields for doc_id:"
                    << doc_id << " return code:" << res.rc
                    << " msg:" << lcb_strerror(NULL, res.rc) << '\n';
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
      sleep_duration *= 1.5;
      lcb_subdoc3(cb_instance, &res, &mcmd);
      lcb_wait(cb_instance);
    }

    if (res.rc == LCB_SUCCESS) {
        return;
    }

    if (res.rc == LCB_KEY_EEXISTS) {
      LOG(logTrace) << "CAS Mismatch for " << doc_id << ". Retrying" << '\n';
      std::this_thread::sleep_for(
          std::chrono::milliseconds(LCB_OP_RETRY_INTERVAL));
    }
  }
}
