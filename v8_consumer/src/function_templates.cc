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

long curl_timeout = 10000L; // Default curl timeout in ms

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
  if (!TO_LOCAL(options->GetOwnPropertyNames(context), &option_names)) {
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
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "couchbase-eventing/5.5");

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