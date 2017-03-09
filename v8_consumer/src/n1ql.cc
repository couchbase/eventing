#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <include/v8.h>
#include <include/libplatform/libplatform.h>

#include <libcouchbase/api3.h>
#include <libcouchbase/couchbase.h>
#include <libcouchbase/n1ql.h>

#include "../include/n1ql.h"

static void query_callback(lcb_t, int, const lcb_RESPN1QL *resp) {
  Rows *rows = reinterpret_cast<Rows *>(resp->cookie);

  if (resp->rflags & LCB_RESP_F_FINAL) {
    rows->rc = resp->rc;
    rows->metadata.assign(resp->row, resp->nrow);
  } else {
    rows->rows.push_back(std::string(resp->row, resp->nrow));
  }
}

N1QL::N1QL(V8Worker *w, const char *bname, const char *ep, const char *alias) {

  isolate_ = w->GetIsolate();
  context_.Reset(isolate_, w->context_);

  bucket_name.assign(bname);
  endpoint.assign(ep);
  n1ql_alias.assign(alias);

  std::string connstr = "couchbase://" + GetEndPoint() + "/" + GetBucketName();

  // LCB setup
  lcb_create_st crst;
  memset(&crst, 0, sizeof crst);

  crst.version = 3;
  crst.v.v3.connstr = connstr.c_str();

  lcb_create(&n1ql_lcb_obj, &crst);
  lcb_connect(n1ql_lcb_obj);
  lcb_wait(n1ql_lcb_obj);
}

N1QL::~N1QL() {
  lcb_destroy(n1ql_lcb_obj);
  context_.Reset();
}

bool N1QL::Initialize(V8Worker *w, std::map<std::string, std::string> *n1ql) {

  v8::HandleScope handle_scope(GetIsolate());

  v8::Local<v8::Context> context = v8::Local<v8::Context>::New(GetIsolate(), w->context_);
  context_.Reset(GetIsolate(), context);

  v8::Context::Scope context_scope(context);

  if (!InstallMaps(n1ql))
      return false;

  return true;
}

v8::Local<v8::ObjectTemplate> N1QL::MakeN1QLMapTemplate(v8::Isolate *isolate) {

  v8::EscapableHandleScope handle_scope(isolate);

  v8::Local<v8::ObjectTemplate> result = v8::ObjectTemplate::New(isolate);
  result->SetInternalFieldCount(2);
  result->SetHandler(v8::NamedPropertyHandlerConfiguration(N1QLEnumGetCall));

  return handle_scope.Escape(result);
}

v8::Local<v8::Object>
N1QL::WrapN1QLMap(std::map<std::string, std::string> *obj) {

  v8::EscapableHandleScope handle_scope(GetIsolate());

  if (n1ql_map_template_.IsEmpty()) {
    v8::Local<v8::ObjectTemplate> raw_template =
        MakeN1QLMapTemplate(GetIsolate());
    n1ql_map_template_.Reset(GetIsolate(), raw_template);
  }

  v8::Local<v8::ObjectTemplate> templ =
      v8::Local<v8::ObjectTemplate>::New(GetIsolate(), n1ql_map_template_);

  v8::Local<v8::Object> result =
      templ->NewInstance(GetIsolate()->GetCurrentContext()).ToLocalChecked();

  v8::Local<v8::External> map_ptr = v8::External::New(GetIsolate(), obj);
  v8::Local<v8::External> n1ql_lcb_obj_ptr =
      v8::External::New(GetIsolate(), &n1ql_lcb_obj);

  result->SetInternalField(0, map_ptr);
  result->SetInternalField(1, n1ql_lcb_obj_ptr);

  return handle_scope.Escape(result);
}

bool N1QL::InstallMaps(std::map<std::string, std::string> *n1ql) {

  v8::HandleScope handle_scope(GetIsolate());

  v8::Local<v8::Object> n1ql_obj = WrapN1QLMap(n1ql);

  v8::Local<v8::Context> context = v8::Local<v8::Context>::New(GetIsolate(), context_);

  LOG(logInfo) << "Registering handler for n1ql_alias: " << n1ql_alias.c_str()
               << '\n';

  // Set the options object as a property on the global object.
  context->Global()
      ->Set(context, v8::String::NewFromUtf8(GetIsolate(), n1ql_alias.c_str(),
                                             v8::NewStringType::kNormal)
                         .ToLocalChecked(),
            n1ql_obj)
      .FromJust();

  return true;
}

void N1QL::N1QLEnumGetCall(v8::Local<v8::Name> name,
                           const v8::PropertyCallbackInfo<v8::Value> &info) {

  if (name->IsSymbol())
    return;

  std::string query = ObjectToString(v8::Local<v8::String>::Cast(name));

  lcb_t* n1ql_lcb_obj_ptr = UnwrapLcbInstance(info.Holder());

  lcb_error_t rc;
  lcb_N1QLPARAMS *params;
  lcb_CMDN1QL qcmd= { 0 };
  Rows rows;

  LOG(logInfo) << "n1ql query fired: " << query << '\n';
  params = lcb_n1p_new();
  rc = lcb_n1p_setstmtz(params, query.c_str());
  qcmd.callback = query_callback;
  rc = lcb_n1p_mkcmd(params, &qcmd);
  rc = lcb_n1ql_query(*n1ql_lcb_obj_ptr, &rows, &qcmd);
  lcb_wait(*n1ql_lcb_obj_ptr);

  auto begin = rows.rows.begin();
  auto end = rows.rows.end();

  v8::Handle<v8::Array> result =
      v8::Array::New(info.GetIsolate(), distance(begin, end));

  if (rows.rc == LCB_SUCCESS) {
    LOG(logInfo) << "Query successful!, rows retrieved: "
                 << distance(begin, end) << '\n';
    int index = 0;
    for (auto &row : rows.rows) {
      result->Set(
          v8::Integer::New(info.GetIsolate(), index),
          v8::JSON::Parse(createUtf8String(info.GetIsolate(), row.c_str())));
      index++;
    }
  } else {
    LOG(logError) << "Query failed!";
    LOG(logError) << "(" << int(rows.rc) << "). ";
    LOG(logError) << lcb_strerror(NULL, rows.rc) << '\n';
  }

  info.GetReturnValue().Set(result);
}
