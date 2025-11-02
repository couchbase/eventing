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

#ifndef QUERY_ITERABLE_H
#define QUERY_ITERABLE_H

#include "query-iterator.h"

#include <v8.h>

#include "info.h"

namespace Query {
class IterableBase {
public:
  struct Info : public ::Info {
    Info(bool is_fatal, std::string msg) : ::Info(is_fatal, std::move(msg)) {}
    Info(const v8::Local<v8::Value> &object) : ::Info(false), object(object) {}

    v8::Local<v8::Value> object;
  };

  IterableBase(v8::Isolate *isolate, const v8::Local<v8::Context> &context);
  virtual ~IterableBase();

  IterableBase() = delete;
  IterableBase(IterableBase &&) = delete;
  IterableBase(const IterableBase &) = delete;
  IterableBase &operator=(IterableBase &&) = delete;
  IterableBase &operator=(const IterableBase &) = delete;

  virtual IterableBase::Info NewObject(Iterator *iterator) const = 0;

protected:
  enum InternalField {
    kIterator,
    Count // Not a field
  };

  v8::Isolate *isolate_;
  v8::Global<v8::Context> context_{};
  v8::Global<v8::ObjectTemplate> template_{};
};

class Iterable : public IterableBase {
public:
  struct Info : public ::Info {
    Info(bool is_fatal, std::string msg) : ::Info(is_fatal, std::move(msg)) {}
    Info(std::shared_ptr<Iterator> iterator,
         const v8::Local<v8::Value> &iterable)
        : ::Info(false), iterator(iterator), iterable(iterable) {}

    std::shared_ptr<Iterator> iterator{nullptr};
    v8::Local<v8::Value> iterable;
  };

  Iterable(v8::Isolate *isolate, const v8::Local<v8::Context> &context);
  virtual ~Iterable() {}

  IterableBase::Info NewObject(Iterator *iterator) const override;

private:
  static void Impl(const v8::FunctionCallbackInfo<v8::Value> &args);
  static void Close(const v8::FunctionCallbackInfo<v8::Value> &args);
};

class IterableImpl : public IterableBase {
public:
  IterableImpl(v8::Isolate *isolate, const v8::Local<v8::Context> &context);
  virtual ~IterableImpl() {}

  IterableBase::Info NewObject(Iterator *iterator) const override;

private:
  static void Next(const v8::FunctionCallbackInfo<v8::Value> &args);
};

class IterableResult {
public:
  struct Info : public ::Info {
    Info(bool is_fatal, std::string msg) : ::Info(is_fatal, std::move(msg)) {}
    Info(const v8::Local<v8::Value> &result) : ::Info(false), result(result) {}

    v8::Local<v8::Value> result;
  };

  IterableResult(v8::Isolate *isolate, const v8::Local<v8::Context> &context);
  ~IterableResult();

  IterableResult() = delete;
  IterableResult(IterableResult &&) = delete;
  IterableResult(const IterableResult &) = delete;
  IterableResult &operator=(IterableResult &&) = delete;
  IterableResult &operator=(const IterableResult &) = delete;

  IterableResult::Info NewObject(const Query::Row &row);

private:
  v8::Isolate *isolate_;
  v8::Global<v8::Context> context_{};
  v8::Global<v8::ObjectTemplate> template_{};
};

struct WrapStop {
  explicit WrapStop(v8::Isolate *, std::shared_ptr<Query::Iterator>,
                    v8::Local<v8::Value>);
  v8::Global<v8::Value> value_;
  std::shared_ptr<Query::Iterator> iterator_;
  v8::Isolate *isolate_;
  static void Callback(const v8::WeakCallbackInfo<WrapStop> &);
  static void SecondCallback(const v8::WeakCallbackInfo<Query::WrapStop> &);
};
} // namespace Query

#endif
