// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

#include <v8.h>

#include "comm.h"
#include "isolate_data.h"
#include "js_exception.h"
#include "query-iterable.h"
#include "query-row.h"
#include "utils.h"

std::atomic<int64_t> n1ql_op_exception_count = {0};

Query::IterableBase::IterableBase(v8::Isolate *isolate,
                                  const v8::Local<v8::Context> &context)
    : isolate_(isolate) {
  context_.Reset(isolate_, context);
}

Query::IterableBase::~IterableBase() {
  template_.Reset();
  context_.Reset();
}

Query::Iterable::Iterable(v8::Isolate *isolate,
                          const v8::Local<v8::Context> &context)
    : IterableBase(isolate, context) {
  v8::HandleScope handle_scope(isolate_);

  auto iterable_template = v8::ObjectTemplate::New(isolate_);
  iterable_template->SetInternalFieldCount(InternalField::Count);
  iterable_template->Set(v8::Symbol::GetIterator(isolate_),
                         v8::FunctionTemplate::New(isolate_, Impl));
  iterable_template->Set(v8Str(isolate_, "close"),
                         v8::FunctionTemplate::New(isolate_, Close));
  template_.Reset(isolate_, iterable_template);
}

Query::IterableBase::Info Query::Iterable::NewObject(Iterator *iterator) const {
  v8::EscapableHandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  auto iterable_template = template_.Get(isolate_);

  v8::Local<v8::Object> iterable_obj;
  if (!TO_LOCAL(iterable_template->NewInstance(context), &iterable_obj)) {
    return {true, "Unable to instantiate iterable object"};
  }

  iterable_obj->SetInternalField(InternalField::kIterator,
                                 v8::External::New(isolate_, iterator));
  return {handle_scope.Escape(iterable_obj)};
}

void Query::Iterable::Impl(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  v8::HandleScope handle_scope(isolate);
  auto iterable_impl = UnwrapData(isolate)->query_iterable_impl;
  auto js_exception = UnwrapData(isolate)->js_exception;

  auto iter_val = args.This()->GetInternalField(InternalField::kIterator);
  auto iterator =
      reinterpret_cast<Iterator *>(iter_val.As<v8::External>()->Value());

  if (auto impl_info = iterable_impl->NewObject(iterator); impl_info.is_fatal) {
    ++n1ql_op_exception_count;
    js_exception->ThrowN1QLError(impl_info.msg);
    return;
  } else {
    args.GetReturnValue().Set(impl_info.object);
  }
}

void Query::Iterable::Close(const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  auto iter_val = args.This()->GetInternalField(InternalField::kIterator);
  auto iterator =
      reinterpret_cast<Iterator *>(iter_val.As<v8::External>()->Value());
  iterator->Stop();
}

Query::IterableImpl::IterableImpl(v8::Isolate *isolate,
                                  const v8::Local<v8::Context> &context)
    : IterableBase(isolate, context) {
  v8::HandleScope handle_scope(isolate_);

  auto impl_template = v8::ObjectTemplate::New(isolate_);
  impl_template->SetInternalFieldCount(InternalField::Count);
  impl_template->Set(isolate_, "next",
                     v8::FunctionTemplate::New(isolate_, Next));
  template_.Reset(isolate_, impl_template);
}

Query::IterableBase::Info
Query::IterableImpl::NewObject(Iterator *iterator) const {
  v8::EscapableHandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  auto result_template = template_.Get(isolate_);

  v8::Local<v8::Object> result_obj;
  if (!TO_LOCAL(result_template->NewInstance(context), &result_obj)) {
    return {true, "Unable instantiate iterable result object"};
  }

  result_obj->SetInternalField(InternalField::kIterator,
                               v8::External::New(isolate_, iterator));
  return {handle_scope.Escape(result_obj)};
}

void Query::IterableImpl::Next(
    const v8::FunctionCallbackInfo<v8::Value> &args) {
  auto isolate = args.GetIsolate();
  std::lock_guard<std::mutex> guard(UnwrapData(isolate)->termination_lock_);
  if (!UnwrapData(isolate)->is_executing_) {
    return;
  }

  v8::HandleScope handle_scope(isolate);
  auto js_exception = UnwrapData(isolate)->js_exception;
  auto helper = UnwrapData(isolate)->query_helper;
  auto iterable_result = UnwrapData(isolate)->query_iterable_result;

  auto iter_val = args.This()->GetInternalField(InternalField::kIterator);
  auto iterator =
      reinterpret_cast<Query::Iterator *>(iter_val.As<v8::External>()->Value());

  auto next = iterator->Next();
  if (next.is_done || next.is_error) {
    // Error reported by lcb_wait (coming from LCB client)
    if (auto it_result = iterator->Wait(); it_result.is_fatal) {
      ++n1ql_op_exception_count;
      js_exception->ThrowN1QLError(it_result.msg);
      return;
    }
  }

  if (next.is_error) {
    helper->HandleRowError(next);
    return;
  }

  if (auto result_info = iterable_result->NewObject(next);
      result_info.is_fatal) {
    ++n1ql_op_exception_count;
    js_exception->ThrowN1QLError(result_info.msg);
    return;
  } else {
    args.GetReturnValue().Set(result_info.result);
  }
}

Query::IterableResult::IterableResult(v8::Isolate *isolate,
                                      const v8::Local<v8::Context> &context)
    : isolate_(isolate) {
  v8::HandleScope handle_scope(isolate_);

  context_.Reset(isolate_, context);

  auto result_template = v8::ObjectTemplate::New(isolate_);
  result_template->Set(isolate_, "value", v8::Null(isolate_));
  result_template->Set(isolate_, "done", v8::Boolean::New(isolate_, true));
  template_.Reset(isolate_, result_template);
}

Query::IterableResult::Info
Query::IterableResult::NewObject(const Query::Row &row) {
  v8::EscapableHandleScope handle_scope(isolate_);
  auto context = context_.Get(isolate_);
  auto result_template = template_.Get(isolate_);

  v8::Local<v8::Object> result_obj;
  if (!TO_LOCAL(result_template->NewInstance(context), &result_obj)) {
    return {true, "Unable to instantiate iterable result object"};
  }

  auto result = false;
  if (!TO(result_obj->Set(context, v8Str(isolate_, "done"),
                          v8::Boolean::New(isolate_, row.is_done)),
          &result) ||
      !result) {
    return {true, "Unable to set the value of done on iterable result object"};
  }
  if (row.is_done) {
    return {handle_scope.Escape(result_obj)};
  }

  v8::Local<v8::Value> value_val;
  // TODO : If JSON parse fails, try to provide (row, col) information
  if (!TO_LOCAL(v8::JSON::Parse(isolate_, v8Str(isolate_, row.data)),
                &value_val)) {
    return {true, "Unable to parse query row as JSON"};
  }
  result = false;
  if (!TO(result_obj->Set(context, v8Str(isolate_, "value"), value_val),
          &result)) {
    return {true, "Unable to set value on iterable result object"};
  }
  return {handle_scope.Escape(result_obj)};
}

Query::IterableResult::~IterableResult() {
  context_.Reset();
  template_.Reset();
}
