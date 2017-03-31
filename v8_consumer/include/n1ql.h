#ifndef N1QL_H
#define N1QL_H

#include <map>
#include <string>
#include <vector>

#include <include/libplatform/libplatform.h>
#include <include/v8.h>

#include "v8worker.h"

struct Rows {
  std::vector<std::string> rows;
  std::string metadata;
  lcb_error_t rc;
  short htcode;
  Rows() : rc(LCB_ERROR), htcode(0) {}
};

class N1QL {
public:
  N1QL(V8Worker *w, const char *bname, const char *ep, const char *alias);
  ~N1QL();

  virtual bool Initialize(V8Worker *w,
                          std::map<std::string, std::string> *n1ql);

  v8::Isolate *GetIsolate() { return isolate_; }
  std::string GetBucketName() { return bucket_name; }
  std::string GetEndPoint() { return endpoint; }

  v8::Global<v8::ObjectTemplate> n1ql_map_template_;

  lcb_t n1ql_lcb_obj;

private:
  bool InstallMaps(std::map<std::string, std::string> *n1ql);

  v8::Local<v8::ObjectTemplate> MakeN1QLMapTemplate(v8::Isolate *isolate);

  static void N1QLEnumGetCall(v8::Local<v8::Name> name,
                              const v8::PropertyCallbackInfo<v8::Value> &info);

  v8::Local<v8::Object> WrapN1QLMap(std::map<std::string, std::string> *bucket);

  v8::Isolate *isolate_;
  v8::Persistent<v8::Context> context_;

  std::string bucket_name;
  std::string endpoint;
  std::string n1ql_alias;
};

#endif
