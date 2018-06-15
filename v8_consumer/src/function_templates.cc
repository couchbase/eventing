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

#include "function_templates.h"

long curl_timeout = 500L; // Default curl timeout of 500ms

void Log(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::Locker locker(isolate);
  v8::HandleScope handle_scope(isolate);
  auto context = isolate->GetCurrentContext();
  std::string log_msg;

  for (auto i = 0; i < args.Length(); i++) {
    if (args[i]->IsNativeError()) {
      v8::Local<v8::Object> object;
      if (!TO_LOCAL(args[i]->ToObject(context), &object)) {
        return;
      }

      v8::Local<v8::Value> to_string_val;
      if (!TO_LOCAL(object->Get(context, v8Str(isolate, "toString")),
                    &to_string_val)) {
        return;
      }

      auto to_string_func = to_string_val.As<v8::Function>();
      v8::Local<v8::Value> stringified_val;
      if (!TO_LOCAL(to_string_func->Call(context, object, 0, nullptr),
                    &stringified_val)) {
        return;
      }

      log_msg += JSONStringify(isolate, stringified_val);
    } else {
      log_msg += JSONStringify(isolate, args[i]);
    }

    log_msg += " ";
  }

  APPLOG << log_msg << std::endl;
}

// console.log for debugger - also logs to eventing.log
void ConsoleLog(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::Locker locker(isolate);
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
  auto isolate = args.GetIsolate();
  v8::Locker locker(isolate);
  v8::HandleScope handle_scope(isolate);

  auto context = isolate->GetCurrentContext();
  if (args.Length() < 3) {
    LOG(logError)
        << "Cron timer: Need 3 args: <callback_func> <timeWhenToKickOff>"
           "<payload>"
        << std::endl;
    return;
  }

  std::string cb_func;
  if (isFuncReference(args, 0)) {
    auto func_ref = args[0].As<v8::Function>();
    v8::String::Utf8Value func_name(func_ref->GetName());
    cb_func.assign(std::string(*func_name));
  } else {
    return;
  }

  std::string opaque = JSONStringify(isolate, args[2]);
  v8::Local<v8::Integer> ts_int;
  if (!TO_LOCAL(args[1]->ToInteger(context), &ts_int)) {
    return;
  }

  auto actual_ts = ts_int->Value();
  if (actual_ts <= 0) {
    LOG(logError) << "Cron timer: Skipping cron timer callback setup, invalid "
                     "start timestamp"
                  << std::endl;
    return;
  }

  std::string start_ts, timer_entry, value;

  // Add a fuzz of (0, fuzz_offset) to the actual timestamp
  auto fuzz_offset = UnwrapData(isolate)->fuzz_offset;
  fuzz_offset = fuzz_offset <= 0 ? 1 : fuzz_offset;
  auto fuzz_ts = actual_ts + rand() % fuzz_offset;

  start_ts.assign(std::to_string(fuzz_ts));
  LOG(logTrace) << "Cron timer: Actual timestamp: " << actual_ts
                << " Fuzz timestamp: " << fuzz_ts << std::endl;

  auto v8worker = UnwrapData(isolate)->v8worker;
  timer_entry.assign(v8worker->GetAppName());
  timer_entry.append("::");
  timer_entry.append(ConvertToISO8601(start_ts));
  timer_entry.append("Z");
  LOG(logTrace) << "Cron timer: Request to register cron timer, callback_func:"
                << cb_func << " start_ts : " << timer_entry << std::endl;

  // Store blob in KV store, blob structure:
  // {
  //    "callback_func": "CallbackFunc",
  //    "payload": opaque,
  //    "version": "5.5.0"
  // }

  value.assign(R"({"callback_func": ")");
  value.append(cb_func);
  value.append(R"(", "payload": )");
  value.append(opaque);
  value.append(R"(, "version": )");
  value.append(REventingVer());
  value.append("}");

  auto meta_cb_instance = UnwrapData(isolate)->meta_cb_instance;
  auto cron_timers_per_doc = UnwrapData(isolate)->cron_timers_per_doc;
  Result cres;

  std::string counter("counter");
  std::string timer_entry_doc_id;

  int doc_id_counter = 0;

  while (true) {
    timer_entry_doc_id.assign(timer_entry);
    timer_entry_doc_id.append(std::to_string(doc_id_counter));

    lcb_CMDSUBDOC td_cmd = {0};
    LCB_CMD_SET_KEY(&td_cmd, timer_entry_doc_id.c_str(),
                    timer_entry_doc_id.length());

    lcb_SDSPEC ctd_spec = {0};
    ctd_spec.sdcmd = LCB_SDCMD_GET;

    td_cmd.specs = &ctd_spec;
    td_cmd.nspecs = 1;

    LCB_SDSPEC_SET_PATH(&ctd_spec, counter.c_str(), counter.length());
    auto err = lcb_subdoc3(meta_cb_instance, &cres, &td_cmd);
    if (err != LCB_SUCCESS) {
      LOG(logError) << "CronTimer: Unable to set subdoc op to get counter"
                    << std::endl;
    }

    err = lcb_wait(meta_cb_instance);
    if (err != LCB_SUCCESS) {
      LOG(logError) << "CronTimer: Unable to schedule subdoc op to get counter"
                    << std::endl;
    }

    if (cres.rc == LCB_SUCCESS) {
      int entry_count = std::stoi(cres.value);
      if (entry_count >= cron_timers_per_doc) {
        doc_id_counter++;
        continue;
      } else {
        break;
      }
    } else if ((cres.rc == LCB_KEY_ENOENT) ||
               (cres.rc == LCB_SUBDOC_PATH_ENOENT)) {
      break;
    }
  }

  timer_entry.assign(timer_entry_doc_id);

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

  lcb_SDSPEC ctr_spec = {0};
  ctr_spec.sdcmd = LCB_SDCMD_COUNTER;
  LCB_SDSPEC_SET_PATH(&ctr_spec, counter.c_str(), counter.length());
  LCB_SDSPEC_SET_VALUE(&ctr_spec, "1", 1);
  specs.push_back(ctr_spec);

  lcb_SDSPEC dspec = {0};
  dspec.sdcmd = LCB_SDCMD_DICT_UPSERT;
  auto version_path = "version";
  auto version_value = REventingVer();
  LCB_SDSPEC_SET_PATH(&dspec, version_path, strlen(version_path));
  LCB_SDSPEC_SET_VALUE(&dspec, version_value.c_str(), version_value.length());
  specs.push_back(dspec);

  mcmd.specs = specs.data();
  mcmd.nspecs = specs.size();
  mcmd.cmdflags = LCB_CMDSUBDOC_F_UPSERT_DOC;

  auto err = lcb_subdoc3(meta_cb_instance, &res, &mcmd);
  if (err != LCB_SUCCESS) {
    LOG(logError) << "CronTimer: Unable to set subdoc op to store timer"
                  << std::endl;
  }

  err = lcb_wait(meta_cb_instance);
  if (err != LCB_SUCCESS) {
    LOG(logError) << "CronTimer: Unable to schedule subdoc op to store timer"
                  << std::endl;
  }

  auto sleep_duration = LCB_OP_RETRY_INTERVAL;
  while (res.rc != LCB_SUCCESS) {
    LOG(logTrace) << "Cron timer: (Retry) Create failure for doc:"
                  << timer_entry << " payload: " << RU(opaque)
                  << " lcb rc:" << lcb_strerror(meta_cb_instance, res.rc)
                  << " sleep_duration: " << sleep_duration * 1000 << std::endl;

    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
    sleep_duration *= 1.5;

    if (sleep_duration > 5000) {
      sleep_duration = 5000;
    }

    err = lcb_subdoc3(meta_cb_instance, &res, &mcmd);
    if (err != LCB_SUCCESS) {
      LOG(logError)
          << "CronTimer: (Retry) Unable to set subdoc op to store timer"
          << std::endl;
    }

    err = lcb_wait(meta_cb_instance);
    if (err != LCB_SUCCESS) {
      LOG(logError)
          << "CronTimer: (Retry) Unable to schedule subdoc op to store timer"
          << std::endl;
    }
  }
}

void CreateDocTimer(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::Locker locker(isolate);
  v8::HandleScope handle_scope(isolate);

  if (args.Length() < 3) {
    LOG(logError) << "Doc Timer : <callback_func> <timeWhenToKickOff> <DocID>"
                     "<payload>"
                  << std::endl;
    return;
  }

  std::string cb_func;
  if (isFuncReference(args, 0)) {
    auto func_ref = args[0].As<v8::Function>();
    v8::String::Utf8Value func_name(func_ref->GetName());
    cb_func.assign(std::string(*func_name));
  } else {
    return;
  }

  v8::String::Utf8Value doc(args[2]);
  v8::String::Utf8Value ts(args[1]);

  std::string doc_id, start_ts, timer_entry;
  doc_id.assign(std::string(*doc));
  start_ts.assign(std::string(*ts));

  // If the doc not supposed to expire, skip
  // setting up timer callback for it
  if (atoi(start_ts.c_str()) == 0) {
    LOG(logError) << "DocTimer: Skipping timer callback setup for doc_id:"
                  << RU(doc_id) << ", won't expire" << std::endl;
    ++doc_timer_create_failure;
    auto js_exception = UnwrapData(isolate)->js_exception;
    js_exception->Throw("Timer won't expire");
    return;
  }

  auto v8worker = UnwrapData(isolate)->v8worker;
  timer_entry.assign(v8worker->GetAppName());
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
  std::string handler_uuid = v8worker->GetHandlerUUID();
  std::string xattr_cas_path = handler_uuid + ".cas";
  std::string xattr_digest_path = handler_uuid + ".digest";
  std::string xattr_timer_path = handler_uuid + ".timers";
  std::string xattr_eventing_ver_path = handler_uuid + ".version";

  std::string mutation_cas_macro(R"("${Mutation.CAS}")");
  std::string doc_exptime("$document.exptime");
  timer_entry += "Z::";
  timer_entry += cb_func;
  timer_entry += "\"";
  timer_entry.insert(0, 1, '"');
  LOG(logTrace) << "DocTimer: Request to register doc timer, callback_func:"
                << cb_func << " doc_id:" << RU(doc_id)
                << " start_ts:" << timer_entry << std::endl;

  while (true) {
    auto cb_instance = UnwrapData(isolate)->cb_instance;
    lcb_CMDSUBDOC gcmd = {0};
    LCB_CMD_SET_KEY(&gcmd, doc_id.c_str(), doc_id.size());

    auto v8worker = UnwrapData(isolate)->v8worker;

    // Frame the payload that needs to be sent over To Eventing-producer
    // to store doc timer entry inside plasma
    doc_timer_msg_t msg;
    msg.timer_entry.assign(ConvertToISO8601(start_ts));
    msg.timer_entry += "Z::";
    msg.timer_entry += cb_func;
    msg.timer_entry += "::";
    msg.timer_entry += doc_id;
    msg.timer_entry += "::";
    msg.timer_entry += std::to_string(v8worker->currently_processed_vb_);
    msg.timer_entry += "::";
    msg.timer_entry += std::to_string(v8worker->currently_processed_seqno_);

    v8worker->doc_timer_queue_->Push(msg);

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
    auto err = lcb_subdoc3(cb_instance, &res, &gcmd);
    if (err != LCB_SUCCESS) {
      LOG(logError) << "DocTimer: Unable to set subdoc op to get timer doc"
                    << std::endl;
    }

    err = lcb_wait(cb_instance);
    if (err != LCB_SUCCESS) {
      LOG(logError) << "DocTimer: Unable to schedule subdoc op to get timer doc"
                    << std::endl;
    }

    auto sleep_duration = LCB_OP_RETRY_INTERVAL;
    if (res.rc == LCB_KEY_ENOENT) {
      HandleDocTimerFailure(isolate, cb_instance, res.rc);
      return;
    }

    while (res.rc != LCB_SUCCESS) {
      LOG(logError) << "DocTimer: (Retry) Failed while performing lookup for "
                       "fulldoc and exptime"
                    << " doc key:" << RU(doc_id)
                    << " rc: " << lcb_strerror(cb_instance, res.rc)
                    << std::endl;

      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
      sleep_duration *= 1.5;

      if (sleep_duration > 5000) {
        sleep_duration = 5000;
      }

      err = lcb_subdoc3(cb_instance, &res, &gcmd);
      if (err != LCB_SUCCESS) {
        LOG(logError)
            << "DocTimer: (Retry) Unable to set subdoc op to get timer doc"
            << std::endl;
      }

      err = lcb_wait(cb_instance);
      if (err != LCB_SUCCESS) {
        LOG(logError)
            << "DocTimer: (Retry) Unable to schedule subdoc op to get timer doc"
            << std::endl;
      }
    }

    uint32_t d = crc32c(0, res.value.c_str(), res.value.length());
    std::string digest = std::to_string(d);

    LOG(logTrace) << "DocTimer: CreateDocTimer cas: " << res.cas
                  << " exptime: " << res.exptime << " digest: " << digest
                  << std::endl;

    lcb_CMDSUBDOC mcmd = {0};
    lcb_SDSPEC digest_spec, xattr_spec, tspec, eventing_ver_spec = {0};
    LCB_CMD_SET_KEY(&mcmd, doc_id.c_str(), doc_id.size());

    std::vector<lcb_SDSPEC> specs;

    mcmd.cas = res.cas;
    mcmd.exptime = res.exptime;

    auto eventing_ver_value = REventingVer();
    eventing_ver_spec.sdcmd = LCB_SDCMD_DICT_UPSERT;
    eventing_ver_spec.options =
        LCB_SDSPEC_F_MKINTERMEDIATES | LCB_SDSPEC_F_XATTRPATH;
    LCB_SDSPEC_SET_PATH(&eventing_ver_spec, xattr_eventing_ver_path.c_str(),
                        xattr_eventing_ver_path.size());
    LCB_SDSPEC_SET_VALUE(&eventing_ver_spec, eventing_ver_value.c_str(),
                         eventing_ver_value.size());
    specs.push_back(eventing_ver_spec);

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

    err = lcb_subdoc3(cb_instance, &res, &mcmd);
    if (err != LCB_SUCCESS) {
      LOG(logError) << "DocTimer: Unable to set subdoc op to create timer"
                    << std::endl;
    }

    err = lcb_wait(cb_instance);
    if (err != LCB_SUCCESS) {
      LOG(logError) << "DocTimer: Unable to schedule subdoc op to create timer"
                    << std::endl;
    }

    if (res.rc == LCB_KEY_ENOENT) {
      HandleDocTimerFailure(isolate, cb_instance, res.rc);
      return;
    }

    if (res.rc != LCB_SUCCESS) {
      HandleDocTimerFailure(isolate, cb_instance, res.rc);
      LOG(logError)
          << "DocTimer: Failed to update timer related xattr fields for doc_id:"
          << RU(doc_id) << " return code:" << res.rc
          << " msg:" << lcb_strerror(cb_instance, res.rc) << std::endl;
      return;
    }

    sleep_duration = LCB_OP_RETRY_INTERVAL;
    while (res.rc != LCB_SUCCESS && res.rc != LCB_KEY_EEXISTS) {
      LOG(logError) << "DocTimer: (Retry) Failed to update timer related xattr "
                       "fields for doc_id:"
                    << RU(doc_id) << " return code:" << res.rc
                    << " msg:" << lcb_strerror(cb_instance, res.rc)
                    << std::endl;

      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
      sleep_duration *= 1.5;

      if (sleep_duration > 5000) {
        sleep_duration = 5000;
      }

      err = lcb_subdoc3(cb_instance, &res, &mcmd);
      if (err != LCB_SUCCESS) {
        LOG(logError)
            << "DocTimer: (Retry) Unable to set subdoc op to create timer"
            << std::endl;
      }

      err = lcb_wait(cb_instance);
      if (err != LCB_SUCCESS) {
        LOG(logError)
            << "DocTimer: (Retry) Unable to schedule subdoc op to create timer"
            << std::endl;
      }
    }

    if (res.rc == LCB_SUCCESS) {
      return;
    }

    if (res.rc == LCB_KEY_EEXISTS) {
      LOG(logTrace) << "DocTimer: CAS Mismatch for " << RU(doc_id)
                    << ". Retrying" << std::endl;

      std::this_thread::sleep_for(
          std::chrono::milliseconds(LCB_OP_RETRY_INTERVAL));
    }
  }
}

size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb,
                           void *userp) {
  size_t realsize = size * nmemb;
  struct CurlResult *mem = static_cast<struct CurlResult *>(userp);

  mem->memory =
      static_cast<char *>(realloc(mem->memory, mem->size + realsize + 1));
  if (mem->memory == nullptr) {
    LOG(logError) << "not enough memory (realloc returned NULL)" << std::endl;
    return 0;
  }

  memcpy(&(mem->memory[mem->size]), contents, realsize);
  mem->size += realsize;
  mem->memory[mem->size] = 0;

  return realsize;
}

void Curl(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  v8::Locker locker(isolate);
  v8::HandleScope handle_scope(isolate);

  std::string auth, data, http_method, mime_type, url, url_suffix;
  struct curl_slist *headers = nullptr;
  v8::String::Utf8Value u(args[0]);

  url.assign(*u);

  auto context = isolate->GetCurrentContext();
  v8::Local<v8::Object> options;
  if (!TO_LOCAL(args[1]->ToObject(context), &options)) {
    return;
  }

  v8::Local<v8::Array> option_names;
  if (!TO_LOCAL(option_names->GetOwnPropertyNames(context), &option_names)) {
    return;
  }

  for (uint32_t i = 0; i < option_names->Length(); i++) {
    v8::Local<v8::Value> key;
    if (!TO_LOCAL(option_names->Get(context, i), &key)) {
      return;
    }

    v8::Local<v8::Value> value;
    if (!TO_LOCAL(options->Get(context, key), &value)) {
      return;
    }

    if (key->IsString()) {
      v8::String::Utf8Value utf8_key(key);

      if ((strcmp(*utf8_key, "method") == 0) && value->IsString()) {
        v8::String::Utf8Value method(value);
        http_method.assign(*method);

      } else if ((strcmp(*utf8_key, "auth") == 0) && value->IsString()) {
        v8::String::Utf8Value creds(value);
        auto creds_vec = split(*creds, ':');
        if (creds_vec.size() == 2) {
          auth.assign(*creds);
        } else {
          LOG(logError) << "Credentials vector size: " << creds_vec.size()
                        << std::endl;
        }

      } else if (strcmp(*utf8_key, "data") == 0) {
        if (value->IsString()) {
          v8::String::Utf8Value payload(value);
          data.assign(*payload);

          headers = curl_slist_append(
              headers, "Content-Type: application/x-www-form-urlencoded");

        } else if (value->IsObject()) {
          data.assign(JSONStringify(args.GetIsolate(), value));
          headers =
              curl_slist_append(headers, "Content-Type: application/json");
        }
      } else if (strcmp(*utf8_key, "parameters") == 0) {
        v8::Local<v8::Object> parameters;
        if (!TO_LOCAL(value->ToObject(context), &parameters)) {
          return;
        }

        auto parameter_fields = parameters.As<v8::Array>();
        for (uint32_t j = 0; j < parameter_fields->Length(); j++) {
          v8::Local<v8::Value> param;
          if (!TO_LOCAL(parameter_fields->Get(context, j), &param)) {
            return;
          }

          if (param->IsString()) {
            v8::String::Utf8Value param_str(param);
            if (j > 0) {
              url_suffix += "&" + std::string(*param_str);
            } else {
              url_suffix += "?" + std::string(*param_str);
            }
          }
        }
      } else if (strcmp(*utf8_key, "headers") == 0) {
        if (value->IsString()) {
          v8::String::Utf8Value m(value);
          mime_type.assign(*m);
        }
      }
    }
  }

  url += url_suffix;

  LOG(logTrace) << "method: " << http_method << " data: " << data
                << " url: " << url << std::endl;

  if (http_method.empty()) {
    http_method.assign("GET");
  }

  CURLcode res;
  CURL *curl = UnwrapData(isolate)->curl_handle;

  if ((strcmp(http_method.c_str(), "GET") == 0 ||
       strcmp(http_method.c_str(), "POST") == 0) &&
      curl) {
    // Initialize common bootstrap code
    struct CurlResult chunk;
    chunk.memory = static_cast<char *>(malloc(1));
    chunk.size = 0;

    curl_easy_reset(curl);

    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);

    if (!auth.empty()) {
      curl_easy_setopt(curl, CURLOPT_HTTPAUTH, (long)CURLAUTH_ANY);
      curl_easy_setopt(curl, CURLOPT_USERPWD, auth.c_str());
    }

    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 120L); // Idle time of 120s
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 60L); // probe interval of 60s

    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, curl_timeout);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "couchbase-eventing/1.0");

    if (strcmp(http_method.c_str(), "GET") == 0) {
      res = curl_easy_perform(curl);

    } else {
      if (mime_type.empty()) {
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
      } else {
        curl_slist_free_all(headers);
        headers = curl_slist_append(headers, mime_type.c_str());
      }

      curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
      curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)data.size());

      res = curl_easy_perform(curl);
      curl_slist_free_all(headers);
    }

    LOG(logTrace) << "Response code from curl call: " << static_cast<int>(res)
                  << std::endl;
    if (res != CURLE_OK) {
      auto js_exception = UnwrapData(isolate)->js_exception;
      js_exception->Throw(res);
      return;
    }

    v8::Local<v8::Value> response;
    if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate, chunk.memory)),
                  &response)) {
      return;
    }

    args.GetReturnValue().Set(response);
  }
}

void HandleDocTimerFailure(v8::Isolate *isolate, lcb_t instance,
                           lcb_error_t error) {
  auto isolate_data = UnwrapData(isolate);
  auto w = isolate_data->v8worker;
  w->AddLcbException(error);
  ++doc_timer_create_failure;

  auto js_exception = isolate_data->js_exception;
  js_exception->Throw(instance, error);
}
