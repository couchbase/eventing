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

#include <iostream>

#include "breakpad.h"

#if defined(BREAKPAD_FOUND) && defined(__linux__)
#include "client/linux/handler/exception_handler.h"
static bool dumpCallback(const google_breakpad::MinidumpDescriptor &descriptor,
                         void *context, bool succeeded) {
  std::cerr << std::endl
            << "== Minidump location: " << descriptor.path()
            << " Status: " << succeeded << " ==" << std::endl;
  return succeeded;
}
void *setupBreakpad(const std::string &diagdir) {
  if (diagdir.length() < 1)
    return nullptr;
  google_breakpad::MinidumpDescriptor descriptor(diagdir.c_str());
  void *exceptionHandler = new google_breakpad::ExceptionHandler(
      descriptor, NULL, dumpCallback, NULL, true, -1);
  return exceptionHandler;
}

#elif defined(BREAKPAD_FOUND) && defined(_WIN32)
#include "client/windows/handler/exception_handler.h"
static bool dumpCallback(const wchar_t *dump_path, const wchar_t *minidump_id,
                         void *context, EXCEPTION_POINTERS *exinfo,
                         MDRawAssertionInfo *assertion, bool succeeded) {
  std::wcerr << std::endl
             << "== Minidump location: " << dump_path << " ID: " << minidump_id
             << " Status: " << succeeded << " ==" << std::endl;
  return succeeded;
}
void *setupBreakpad(const std::string &diagdir) {
  if (diagdir.length() < 1)
    return nullptr;
  std::wstring path(diagdir.begin(), diagdir.end());
  MINIDUMP_TYPE dumptype = static_cast<MINIDUMP_TYPE>(
      MiniDumpWithFullMemory | MiniDumpWithProcessThreadData |
      MiniDumpWithHandleData);
  void *exceptionHandler = new google_breakpad::ExceptionHandler(
      path, nullptr, dumpCallback, nullptr,
      google_breakpad::ExceptionHandler::HANDLER_ALL, dumptype,
      (wchar_t *)nullptr, nullptr);
  return exceptionHandler;
}

#else
void *setupBreakpad(const std::string &diagdir) { return nullptr; }
#endif
