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

#include <sstream>
#include <v8.h>

#include "comm.h"
#include "isolate_data.h"
#include "query-iterable.h"
#include "query-row.h"
#include "utils.h"
#include "v8worker2.h"

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
  const auto v8worker = UnwrapData(isolate)->v8worker2;

  auto iter_val = args.This()->GetInternalField(InternalField::kIterator);
  auto iterator =
      reinterpret_cast<Iterator *>(iter_val.As<v8::External>()->Value());

  if (auto impl_info = iterable_impl->NewObject(iterator); impl_info.is_fatal) {
    js_exception->ThrowN1qlError(impl_info.msg);
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
  auto helper = UnwrapData(isolate)->query_helper;
  auto iterable_result = UnwrapData(isolate)->query_iterable_result;
  auto js_exception = UnwrapData(isolate)->js_exception;
  const auto v8worker = UnwrapData(isolate)->v8worker2;

  auto iter_val = args.This()->GetInternalField(InternalField::kIterator);
  auto iterator =
      reinterpret_cast<Query::Iterator *>(iter_val.As<v8::External>()->Value());

  auto next = iterator->Next();
  if (next.is_done || next.is_error) {
    // Error reported by lcb_wait (coming from LCB client)
    if (auto it_result = iterator->Wait(); it_result.is_fatal) {
      js_exception->ThrowN1qlError(it_result.msg);
      return;
    }
  }

  if (next.is_error) {
    auto err_msg = helper->RowErrorString(next);
    iterator->ThrowQueryError(err_msg);
    return;
  }

  if (auto result_info = iterable_result->NewObject(next);
      result_info.is_fatal) {
    js_exception->ThrowN1qlError(result_info.msg);
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

  std::stringstream err_msg;
  v8::Local<v8::Value> value_val;
  // TODO : If JSON parse fails, try to provide (row, col) information
  if (!TO_LOCAL(v8::JSON::Parse(context, v8Str(isolate_, row.data)),
                &value_val)) {
    err_msg << "Unable to parse query row as JSON : " << RU(row.data);
    return {true, err_msg.str()};
  }
  result = false;
  if (!TO(result_obj->Set(context, v8Str(isolate_, "value"), value_val),
          &result)) {
    err_msg << "Unable to set value on iterable result object : "
            << RU(row.data);
    return {true, err_msg.str()};
  }
  return {handle_scope.Escape(result_obj)};
}

Query::IterableResult::~IterableResult() {
  context_.Reset();
  template_.Reset();
}

Query::WrapStop::WrapStop(v8::Isolate *isolate,
                          std::shared_ptr<Query::Iterator> iter,
                          v8::Local<v8::Value> val)
    : value_(isolate, val), iterator_(iter), isolate_(isolate) {
  // SetWeak method sets the Persistent handle up for garbage collection
  // when the object goes out of scope. The callback is invoked when the handle
  // is GCed
  value_.SetWeak(this, Query::WrapStop::Callback,
                 v8::WeakCallbackType::kParameter);
}

void Query::WrapStop::Callback(
    const v8::WeakCallbackInfo<Query::WrapStop> &data) {
  // Query WrapStop pointer is sent to the callback
  auto wrapper = data.GetParameter();

  // It is mandatory for the Callback to delete the data
  v8::Locker locker(wrapper->isolate_);
  if (!wrapper->value_.IsEmpty()) {
    wrapper->value_.Reset();
  }

  // Queue a second pass callback on the same WrapStop
  // The second pass callbacks are served after all the first callbacks
  data.SetSecondPassCallback(Query::WrapStop::SecondCallback);
}

void Query::WrapStop::SecondCallback(
    const v8::WeakCallbackInfo<Query::WrapStop> &data) {
  // Query WrapStop pointer is sent to the second pass callback
  auto wrapper = data.GetParameter();

  // Close the iterator when the second pass callback is invoked
  // Stop the Query only if it hasn't been stopped yet:
  // Stop() is a NO-OP if iterator is already stoppped.
  wrapper->iterator_->Stop();
  delete wrapper;
}
