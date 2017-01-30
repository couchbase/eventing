#ifndef V8WORKER_H
#define V8WORKER_H

#include <map>
#include <string>

#include <include/libplatform/libplatform.h>
#include <include/v8-debug.h>
#include <include/v8.h>

class V8Worker;

v8::Local<v8::String> createUtf8String(v8::Isolate *isolate, const char *str);
std::string ObjectToString(v8::Local<v8::Value> value);
std::string ToString(v8::Isolate *isolate, v8::Handle<v8::Value> object);
V8Worker *UnwrapV8WorkerInstance(v8::Local<v8::Object> obj);
std::map<std::string, std::string> *UnwrapMap(v8::Local<v8::Object> obj);

class ArrayBufferAllocator : public v8::ArrayBuffer::Allocator {
public:
  virtual void *Allocate(size_t length) {
    void *data = AllocateUninitialized(length);
    return data == NULL ? data : memset(data, 0, length);
  }
  virtual void *AllocateUninitialized(size_t length) { return malloc(length); }
  virtual void Free(void *data, size_t) { free(data); }
};

class V8Worker {
public:
  V8Worker(std::string app_name);
  ~V8Worker();

  int V8WorkerLoad(std::string source_s);
  const char *V8WorkerLastException();
  const char *V8WorkerVersion();

  int SendUpdate(std::string value, std::string meta, std::string doc_type);

  void V8WorkerDispose();
  void V8WorkerTerminateExecution();

  v8::Isolate *GetIsolate() { return isolate_; }
  v8::Persistent<v8::Context> context_;

  v8::Persistent<v8::Function> on_update_;

  v8::Global<v8::ObjectTemplate> worker_template;

  std::string script_to_execute_;
  std::string app_name_;

private:
  bool ExecuteScript(v8::Local<v8::String> script);

  ArrayBufferAllocator allocator;
  v8::Isolate *isolate_;

  std::string last_exception;
};

#endif
