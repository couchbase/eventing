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

#include "commands.h"
#include <cassert>

event_type getEvent(uint8_t event) {
  assert(static_cast<uint8_t>(event_type::startMarker) < event &&
         event < static_cast<uint8_t>(event_type::endMarker));
  return static_cast<event_type>(event);
}

v8_worker_opcode getV8WorkerOpcode(uint8_t opcode) {
  assert(static_cast<uint8_t>(v8_worker_opcode::startMarker) < opcode &&
         opcode < static_cast<uint8_t>(v8_worker_opcode::endMarker));
  return static_cast<v8_worker_opcode>(opcode);
}

dcp_opcode getDCPOpcode(uint8_t opcode) {
  assert(static_cast<uint8_t>(dcp_opcode::startMarker) < opcode &&
         opcode < static_cast<uint8_t>(dcp_opcode::endMarker));
  return static_cast<dcp_opcode>(opcode);
}

app_worker_setting_opcode getAppWorkerSettingOpcode(uint8_t opcode) {
  assert(static_cast<uint8_t>(app_worker_setting_opcode::startMarker) <
             opcode &&
         opcode < static_cast<uint8_t>(app_worker_setting_opcode::endMarker));
  return static_cast<app_worker_setting_opcode>(opcode);
}

filter_opcode getFilterOpcode(uint8_t opcode) {
  assert(static_cast<uint8_t>(filter_opcode::startMarker) < opcode &&
         opcode < static_cast<uint8_t>(filter_opcode::endMarker));
  return static_cast<filter_opcode>(opcode);
}

debugger_opcode getDebuggerOpcode(uint8_t opcode) {
  assert(static_cast<uint8_t>(debugger_opcode::startMarker) < opcode &&
         opcode < static_cast<uint8_t>(debugger_opcode::endMarker));
  return static_cast<debugger_opcode>(opcode);
}

config_opcode getConfigOpcode(uint8_t opcode) {
  assert(static_cast<uint8_t>(config_opcode::startMarker) < opcode &&
         opcode < static_cast<uint8_t>(config_opcode::endMarker));
  return static_cast<config_opcode>(opcode);
}

internal_opcode getInternalOpcode(uint8_t opcode) {
  assert(static_cast<uint8_t>(internal_opcode::startMarker) < opcode &&
         opcode < static_cast<uint8_t>(internal_opcode::endMarker));
  return static_cast<internal_opcode>(opcode);
}
