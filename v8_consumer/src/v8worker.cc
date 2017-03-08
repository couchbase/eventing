#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <unistd.h>
#include <iostream>
#include <mutex>
#include <regex>
#include <sstream>
#include <thread>
#include <typeinfo>

#include "../include/parse_deployment.h"
#include "../include/bucket.h"
#include "../include/n1ql.h"

#define MAXPATHLEN 256

enum RETURN_CODE
{
    SUCCESS = 0,
    FAILED_TO_COMPILE_JS,
    NO_HANDLERS_DEFINED,
    FAILED_INIT_BUCKET_HANDLE,
    FAILED_INIT_N1QL_HANDLE,
    ON_UPDATE_CALL_FAIL,
    ON_DELETE_CALL_FAIL
};

// Copies a C string to a 16-bit string.  Does not check for buffer overflow.
// Does not use the V8 engine to convert strings, so it can be used
// in any thread.  Returns the length of the string.
int AsciiToUtf16(const char *input_buffer, uint16_t *output_buffer) {
  int i;
  for (i = 0; input_buffer[i] != '\0'; ++i) {
    // ASCII does not use chars > 127, but be careful anyway.
    output_buffer[i] = static_cast<unsigned char>(input_buffer[i]);
  }
  output_buffer[i] = 0;
  return i;
}

v8::Local<v8::String> createUtf8String(v8::Isolate *isolate, const char *str) {
  return v8::String::NewFromUtf8(isolate, str,
          v8::NewStringType::kNormal).ToLocalChecked();
}

std::string ObjectToString(v8::Local<v8::Value> value) {
  v8::String::Utf8Value utf8_value(value);
  return std::string(*utf8_value);
}

std::string ToString(v8::Isolate *isolate, v8::Handle<v8::Value> object) {
  v8::HandleScope handle_scope(isolate);

  v8::Local<v8::Context> context = isolate->GetCurrentContext();
  v8::Local<v8::Object> global = context->Global();

  v8::Local<v8::Object> JSON =
      global->Get(v8::String::NewFromUtf8(isolate, "JSON"))->ToObject();
  v8::Local<v8::Function> JSON_stringify = v8::Local<v8::Function>::Cast(
      JSON->Get(v8::String::NewFromUtf8(isolate, "stringify")));

  v8::Local<v8::Value> result;
  v8::Local<v8::Value> args[1];
  args[0] = { object };
  result = JSON_stringify->Call(context->Global(), 1, args);
  return ObjectToString(result);
}

lcb_t *UnwrapLcbInstance(v8::Local<v8::Object> obj) {
  v8::Local<v8::External> field =
      v8::Local<v8::External>::Cast(obj->GetInternalField(1));
  void* ptr = field->Value();
  return static_cast<lcb_t*>(ptr);
}

lcb_t *UnwrapV8WorkerLcbInstance(v8::Local<v8::Object> obj) {
  v8::Local<v8::External> field =
      v8::Local<v8::External>::Cast(obj->GetInternalField(2));
  void *ptr = field->Value();
  return static_cast<lcb_t *>(ptr);
}

V8Worker *UnwrapV8WorkerInstance(v8::Local<v8::Object> obj) {
  v8::Local<v8::External> field =
      v8::Local<v8::External>::Cast(obj->GetInternalField(1));
  void *ptr = field->Value();
  return static_cast<V8Worker *>(ptr);
}

std::map<std::string, std::string> *UnwrapMap(v8::Local<v8::Object> obj) {
  v8::Local<v8::External> field =
      v8::Local<v8::External>::Cast(obj->GetInternalField(0));
  void *ptr = field->Value();
  return static_cast<std::map<std::string, std::string> *>(ptr);
}

// Extracts a C string from a V8 Utf8Value.
const char *ToCString(const v8::String::Utf8Value &value) {
  return *value ? *value : "<std::string conversion failed>";
}

const char *ToJson(v8::Isolate *isolate, v8::Handle<v8::Value> object) {
  v8::HandleScope handle_scope(isolate);

  v8::Local<v8::Context> context = isolate->GetCurrentContext();
  v8::Local<v8::Object> global = context->Global();

  v8::Local<v8::Object> JSON =
      global->Get(v8::String::NewFromUtf8(isolate, "JSON"))->ToObject();
  v8::Local<v8::Function> JSON_stringify = v8::Local<v8::Function>::Cast(
      JSON->Get(v8::String::NewFromUtf8(isolate, "stringify")));

  v8::Local<v8::Value> result;
  v8::Local<v8::Value> args[1];
  args[0] = {object};
  result = JSON_stringify->Call(context->Global(), 1, args);
  v8::String::Utf8Value str(result->ToString());
  return ToCString(str);
}

void Print(const v8::FunctionCallbackInfo<v8::Value> &args) {
  bool first = true;
  for (int i = 0; i < args.Length(); i++) {
    v8::HandleScope handle_scope(args.GetIsolate());
    if (first) {
      first = false;
    } else {
      printf(" ");
    }
    v8::String::Utf8Value str(args[i]);
    const char* cstr = ToJson(args.GetIsolate(), args[i]);
    printf("%s", cstr);
  }
  printf("\n");
  fflush(stdout);
}

std::string ConvertToISO8601(std::string timestamp) {
  char buf[sizeof "2016-08-09T10:11:12"];
  std::string buf_s;
  time_t now;

  int timerValue = atoi(timestamp.c_str());

  // Expiry timers more than 30 days will mention epoch
  // otherwise it will mention seconds from when key
  // was set
  if (timerValue > 25920000) {
    now = timerValue;
    strftime(buf, sizeof buf, "%FT%T", gmtime(&now));
    buf_s.assign(buf);
  } else {
    time(&now);
    now += timerValue;
    strftime(buf, sizeof buf, "%FT%T", gmtime(&now));
    buf_s.assign(buf);
  }
  return buf_s;
}

// Exception details will be appended to the first argument.
std::string ExceptionString(v8::Isolate *isolate, v8::TryCatch *try_catch) {
  std::string out;
  size_t scratchSize = 20;
  char scratch[scratchSize]; // just some scratch space for sprintf

  v8::HandleScope handle_scope(isolate);
  v8::String::Utf8Value exception(try_catch->Exception());
  const char* exception_string = ToCString(exception);

  v8::Handle<v8::Message> message = try_catch->Message();

  if (message.IsEmpty()) {
    // V8 didn't provide any extra information about this error; just
    // print the exception.
    out.append(exception_string);
    out.append("\n");
  } else {
    // Print (filename):(line number)
    v8::String::Utf8Value filename(message->GetScriptOrigin().ResourceName());
    const char *filename_string = ToCString(filename);
    int linenum = message->GetLineNumber();

    snprintf(scratch, scratchSize, "%i", linenum);
    out.append(filename_string);
    out.append(":");
    out.append(scratch);
    out.append("\n");

    // Print line of source code.
    v8::String::Utf8Value sourceline(message->GetSourceLine());
    const char *sourceline_string = ToCString(sourceline);

    out.append(sourceline_string);
    out.append("\n");

    // Print wavy underline (GetUnderline is deprecated).
    int start = message->GetStartColumn();
    for (int i = 0; i < start; i++) {
      out.append(" ");
    }
    int end = message->GetEndColumn();
    for (int i = start; i < end; i++) {
      out.append("^");
    }
    out.append("\n");
    v8::String::Utf8Value stack_trace(try_catch->StackTrace());
    if (stack_trace.length() > 0) {
      const char *stack_trace_string = ToCString(stack_trace);
      out.append(stack_trace_string);
      out.append("\n");
    } else {
      out.append(exception_string);
      out.append("\n");
    }
  }
  return out;
}

std::vector<std::string> &split(const std::string &s, char delim,
                                std::vector<std::string> &elems) {
  std::stringstream ss(s);
  std::string item;
  while (getline(ss, item, delim)) {
    elems.push_back(item);
  }
  return elems;
}

std::vector<std::string> split(const std::string &s, char delim) {
  std::vector<std::string> elems;
  split(s, delim, elems);
  return elems;
}

static void op_get_callback(lcb_t instance, int cbtype,
                            const lcb_RESPBASE *rb) {
  const lcb_RESPGET *resp = reinterpret_cast<const lcb_RESPGET *>(rb);
  Result *result = reinterpret_cast<Result *>(rb->cookie);

  result->status = resp->rc;
  result->cas = resp->cas;
  result->itmflags = resp->itmflags;
  result->value.clear();

  if (resp->rc == LCB_SUCCESS) {
    result->value.assign(reinterpret_cast<const char *>(resp->value),
                         resp->nvalue);
  } else {
    std::cout << "lcb get failed with error "
              << lcb_strerror(instance, resp->rc) << std::endl;
  }
}

static void op_set_callback(lcb_t instance, int cbtype,
                            const lcb_RESPBASE *rb) {
  std::cout << "lcb set response code: " << lcb_strerror(instance, rb->rc)
            << std::endl;
}

static ArrayBufferAllocator array_buffer_allocator;

V8Worker::V8Worker(std::string app_init_meta) {
  v8::V8::InitializeICU();
  v8::Platform *platform = v8::platform::CreateDefaultPlatform();
  v8::V8::InitializePlatform(platform);
  v8::V8::Initialize();

  v8::Isolate::CreateParams create_params;
  create_params.array_buffer_allocator = &allocator;

  isolate_ = v8::Isolate::New(create_params);
  v8::Locker locker(isolate_);
  v8::Isolate::Scope isolate_scope(isolate_);
  v8::HandleScope handle_scope(isolate_);

  isolate_->SetCaptureStackTraceForUncaughtExceptions(true);
  isolate_->SetData(0, this);
  v8::Local<v8::ObjectTemplate> global = v8::ObjectTemplate::New(GetIsolate());

  v8::TryCatch try_catch;

  global->Set(v8::String::NewFromUtf8(GetIsolate(), "log"),
              v8::FunctionTemplate::New(GetIsolate(), Print));

  if(try_catch.HasCaught()) {
    last_exception = ExceptionString(GetIsolate(), &try_catch);
    printf("ERROR Print exception: %s\n", last_exception.c_str());
  }

  v8::Local<v8::Context> context = v8::Context::New(GetIsolate(), NULL, global);
  context_.Reset(GetIsolate(), context);

  auto cfg = flatbuf::v8init::GetInit((const void *)app_init_meta.c_str());
  app_name_ = cfg->appname()->str();
  cb_kv_endpoint = cfg->kvhostport()->str();

  deployment_config *config = ParseDeployment(cfg->depcfg()->str().c_str());

  cb_source_bucket.assign(config->source_bucket);

  std::map<std::string, std::map<std::string, std::vector<std::string>>>::iterator it = config->component_configs.begin();

  for (; it != config->component_configs.end(); it++) {

    if (it->first == "buckets") {
      std::map<std::string, std::vector<std::string>>::iterator bucket =
          config->component_configs["buckets"].begin();
      for (; bucket != config->component_configs["buckets"].end(); bucket++) {
        std::string bucket_alias = bucket->first;
        std::string bucket_name =
            config->component_configs["buckets"][bucket_alias][0];

        bucket_handle =
            new Bucket(this, bucket_name.c_str(), cb_kv_endpoint.c_str(), bucket_alias.c_str());
      }
    }

    n1ql_handle =
        new N1QL(this, cb_source_bucket.c_str(), cb_kv_endpoint.c_str(), "_n1ql");
  }

  std::string connstr = "couchbase://" + cb_kv_endpoint + "/" + config->metadata_bucket.c_str();

  delete config;
  // lcb related setup
  lcb_create_st crst;
  memset(&crst, 0, sizeof crst);

  crst.version = 3;
  crst.v.v3.connstr = connstr.c_str();

  lcb_create(&cb_instance, &crst);
  lcb_connect(cb_instance);
  lcb_wait(cb_instance);

  lcb_install_callback3(cb_instance, LCB_CALLBACK_GET, op_get_callback);
  lcb_install_callback3(cb_instance, LCB_CALLBACK_STORE, op_set_callback);
}

V8Worker::~V8Worker() {
  context_.Reset();
  on_update_.Reset();
  on_delete_.Reset();
}

std::string GetWorkingPath() {
  char temp[MAXPATHLEN];
  return (getcwd(temp, MAXPATHLEN) ? std::string(temp) : std::string(""));
}

void LoadBuiltins(std::string *out) {
  std::ifstream ifs("builtin.js");
  std::string content((std::istreambuf_iterator<char>(ifs)),
                      (std::istreambuf_iterator<char>()));
  out->assign(content);
  return;
}

int V8Worker::V8WorkerLoad(std::string source_s) {
  std::cout << "getcwd: " << GetWorkingPath() << std::endl;
  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  v8::Local<v8::Context> context =
      v8::Local<v8::Context>::New(GetIsolate(), context_);
  v8::Context::Scope context_scope(context);

  v8::TryCatch try_catch;

  std::string builtin_functions, content, script_to_execute;

  LoadBuiltins(&builtin_functions);

  content.assign(source_s);

  std::regex n1ql_ttl("(n1ql\\(\")(.*)(\"\\))");
  std::smatch n1ql_m;

  while (std::regex_search(content, n1ql_m, n1ql_ttl)) {
    script_to_execute += n1ql_m.prefix();
    std::regex re_prefix("n1ql\\(\"");
    std::regex re_suffix("\"\\)");
    script_to_execute +=
        std::regex_replace(n1ql_m[1].str(), re_prefix, "n1ql`");
    script_to_execute += n1ql_m[2].str();
    script_to_execute += std::regex_replace(n1ql_m[3].str(), re_suffix, "`");
    content = n1ql_m.suffix();
  }

  script_to_execute += content;
  script_to_execute.append(builtin_functions);

  v8::Local<v8::String> source =
      v8::String::NewFromUtf8(GetIsolate(), script_to_execute.c_str());

  script_to_execute_ = script_to_execute;
  // std::cout << "script to execute: " << script_to_execute << std::endl;

  if (!ExecuteScript(source))
    return FAILED_TO_COMPILE_JS;

  v8::Local<v8::String> on_update =
      v8::String::NewFromUtf8(GetIsolate(), "OnUpdate",
                              v8::NewStringType::kNormal)
          .ToLocalChecked();

  v8::Local<v8::String> on_delete =
      v8::String::NewFromUtf8(GetIsolate(), "OnDelete",
                              v8::NewStringType::kNormal)
          .ToLocalChecked();

  v8::Local<v8::Value> on_update_val;
  v8::Local<v8::Value> on_delete_val;

  if (!context->Global()->Get(context, on_update).ToLocal(&on_update_val) ||
      !context->Global()->Get(context, on_delete).ToLocal(&on_delete_val)) {
    return NO_HANDLERS_DEFINED;
  }

  v8::Local<v8::Function> on_update_fun =
      v8::Local<v8::Function>::Cast(on_update_val);
  on_update_.Reset(GetIsolate(), on_update_fun);

  v8::Local<v8::Function> on_delete_fun =
      v8::Local<v8::Function>::Cast(on_delete_val);
  on_delete_.Reset(GetIsolate(), on_delete_fun);

  if (bucket_handle) {
    if (!bucket_handle->Initialize(this, &bucket)) {
      std::cerr << "Error initializing bucket handle" << std::endl;
      return FAILED_INIT_BUCKET_HANDLE;
    }
  }

  if (!n1ql_handle->Initialize(this, &n1ql)) {
    std::cerr << "Error initializing n1ql handle" << std::endl;
  }

  return SUCCESS;
}

bool V8Worker::ExecuteScript(v8::Local<v8::String> script) {
  v8::HandleScope handle_scope(GetIsolate());

  v8::TryCatch try_catch(GetIsolate());

  v8::Local<v8::Context> context(GetIsolate()->GetCurrentContext());

  v8::Local<v8::Script> compiled_script;
  if (!v8::Script::Compile(context, script).ToLocal(&compiled_script)) {
    assert(try_catch.HasCaught());
    last_exception = ExceptionString(GetIsolate(), &try_catch);
    std::cout << "Exception logged:" << last_exception << std::endl;
    // The script failed to compile; bail out.
    return false;
  }

  v8::Local<v8::Value> result;
  if (!compiled_script->Run(context).ToLocal(&result)) {
    assert(try_catch.HasCaught());
    last_exception = ExceptionString(GetIsolate(), &try_catch);
    std::cout << "Exception logged:" << last_exception << std::endl;
    // Running the script failed; bail out.
    return false;
  }
  return true;
}

int V8Worker::SendUpdate(std::string value, std::string meta,
                         std::string doc_type) {
  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  v8::Local<v8::Context> context =
      v8::Local<v8::Context>::New(GetIsolate(), context_);
  v8::Context::Scope context_scope(context);

  // std::cout << "value: " << value << " meta: " << meta
  //           << " doc_type: " << doc_type << std::endl;
  v8::TryCatch try_catch(GetIsolate());

  v8::Handle<v8::Value> args[2];
  if (doc_type.compare("json") == 0) {
    args[0] =
        v8::JSON::Parse(v8::String::NewFromUtf8(GetIsolate(), value.c_str()));
  }
  else {
    args[0] = v8::String::NewFromUtf8(GetIsolate(), value.c_str());
  }

  args[1] =
      v8::JSON::Parse(v8::String::NewFromUtf8(GetIsolate(), meta.c_str()));

  if(try_catch.HasCaught()) {
    last_exception = ExceptionString(GetIsolate(), &try_catch);
    fprintf(stderr, "Logged: %s\n", last_exception.c_str());
    fflush(stderr);
  }

  v8::Local<v8::Function> on_doc_update =
      v8::Local<v8::Function>::New(GetIsolate(), on_update_);
  on_doc_update->Call(context->Global(), 2, args);

  if (try_catch.HasCaught()) {
    std::cout << "Exception message: "
              << ExceptionString(GetIsolate(), &try_catch) << std::endl;
    return ON_UPDATE_CALL_FAIL;
  }

  return SUCCESS;
}

int V8Worker::SendDelete(std::string meta) {
  v8::Locker locker(GetIsolate());
  v8::Isolate::Scope isolate_scope(GetIsolate());
  v8::HandleScope handle_scope(GetIsolate());

  v8::Local<v8::Context> context =
      v8::Local<v8::Context>::New(GetIsolate(), context_);
  v8::Context::Scope context_scope(context);

  // std::cout << " meta: " << meta << std::endl;
  v8::TryCatch try_catch(GetIsolate());

  v8::Local<v8::Value> args[1];
  args[0] =
      v8::JSON::Parse(v8::String::NewFromUtf8(GetIsolate(), meta.c_str()));

  assert(!try_catch.HasCaught());

  v8::Local<v8::Function> on_doc_delete =
      v8::Local<v8::Function>::New(GetIsolate(), on_delete_);
  on_doc_delete->Call(context->Global(), 1, args);

  if (try_catch.HasCaught()) {
    std::cout << "Exception message"
              << ExceptionString(GetIsolate(), &try_catch) << std::endl;
    return ON_DELETE_CALL_FAIL;
  }

  return SUCCESS;
}

const char *V8Worker::V8WorkerLastException() { return last_exception.c_str(); }

const char *V8Worker::V8WorkerVersion() { return v8::V8::GetVersion(); }

void V8Worker::V8WorkerTerminateExecution() {
  v8::V8::TerminateExecution(GetIsolate());
}


