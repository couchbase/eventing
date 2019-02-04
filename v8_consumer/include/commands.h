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

#ifndef COMMANDS_H
#define COMMANDS_H

#include <iostream>

// Opcodes for incoming messages from Go to C++
enum event_type {
  eDCP,
  eV8_Worker,
  eApp_Worker_Setting,
  eTimer,
  eDebugger,
  eFilter,
  Event_Unknown
};

enum v8_worker_opcode {
  oDispose,
  oInit,
  oLoad,
  oTerminate,
  oGetSourceMap,
  oGetHandlerCode,
  oGetLatencyStats,
  oGetFailureStats,
  oGetExecutionStats,
  oGetCompileInfo,
  oGetLcbExceptions,
  oGetCurlLatencyStats,
  oVersion,
  V8_Worker_Opcode_Unknown
};

enum dcp_opcode { oDelete, oMutation, DCP_Opcode_Unknown };

enum filter_opcode { oVbFilter, oProcessedSeqNo, Filter_Opcode_Unknown };

enum app_worker_setting_opcode {
  oLogLevel,
  oWorkerThreadCount,
  oWorkerThreadMap,
  oTimerContextSize,
  App_Worker_Setting_Opcode_Unknown
};

enum timer_opcode { oTimer, oCronTimer, Timer_Opcode_Unknown };

enum debugger_opcode { oDebuggerStart, oDebuggerStop, Debugger_Opcode_Unknown };

event_type getEvent(int8_t event);
v8_worker_opcode getV8WorkerOpcode(int8_t opcode);
dcp_opcode getDCPOpcode(int8_t opcode);
app_worker_setting_opcode getAppWorkerSettingOpcode(int8_t opcode);
filter_opcode getFilterOpcode(int8_t opcode);
timer_opcode getTimerOpcode(int8_t opcode);
debugger_opcode getDebuggerOpcode(int8_t opcode);

// Opcodes for outgoing messages from C++ to Go
enum msg_type {
  mType,
  mV8_Worker_Config,
  mTimer_Response,
  mBucket_Ops_Response,
  mFilterAck,
  Msg_Unknown
};

enum v8_worker_config_opcode {
  oConfigOpcode,
  oSourceMap,
  oHandlerCode,
  oAppLogMessage,
  oSysLogMessage,
  oLatencyStats,
  oFailureStats,
  oExecutionStats,
  oCompileInfo,
  oQueueSize,
  oLcbExceptions,
  oCurlLatencyStats,
  V8_Worker_Config_Opcode_Unknown
};

enum doc_timer_response_opcode { timerResponse };

enum bucket_ops_response_opcode { checkpointResponse };

#endif
