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

event_type getEvent(int8_t event) {
  if (event == 1)
    return eDCP;
  if (event == 2)
    return eV8_Worker;
  if (event == 3)
    return eApp_Worker_Setting;
  if (event == 4)
    return eTimer;
  if (event == 5)
    return eDebugger;
  if (event == 6)
    return eFilter;
  return Event_Unknown;
}

v8_worker_opcode getV8WorkerOpcode(int8_t opcode) {
  if (opcode == 1)
    return oDispose;
  if (opcode == 2)
    return oInit;
  if (opcode == 3)
    return oLoad;
  if (opcode == 4)
    return oTerminate;
  if (opcode == 5)
    return oGetSourceMap;
  if (opcode == 6)
    return oGetHandlerCode;
  if (opcode == 7)
    return oGetLatencyStats;
  if (opcode == 8)
    return oGetFailureStats;
  if (opcode == 9)
    return oGetExecutionStats;
  if (opcode == 10)
    return oGetCompileInfo;
  if (opcode == 11)
    return oGetLcbExceptions;
  if (opcode == 12)
    return oGetCurlLatencyStats;
  return V8_Worker_Opcode_Unknown;
}

dcp_opcode getDCPOpcode(int8_t opcode) {
  if (opcode == 1)
    return oDelete;
  if (opcode == 2)
    return oMutation;
  return DCP_Opcode_Unknown;
}

app_worker_setting_opcode getAppWorkerSettingOpcode(int8_t opcode) {
  if (opcode == 1)
    return oLogLevel;
  if (opcode == 2)
    return oWorkerThreadCount;
  if (opcode == 3)
    return oWorkerThreadMap;
  if (opcode == 4)
    return oTimerContextSize;
  return App_Worker_Setting_Opcode_Unknown;
}

timer_opcode getTimerOpcode(int8_t opcode) {
  if (opcode == 1)
    return oTimer;
  return Timer_Opcode_Unknown;
}

filter_opcode getFilterOpcode(int8_t opcode) {
  if (opcode == 1)
    return oVbFilter;
  if (opcode == 2)
    return oProcessedSeqNo;
  return Filter_Opcode_Unknown;
}

debugger_opcode getDebuggerOpcode(int8_t opcode) {
  if (opcode == 1)
    return oDebuggerStart;
  if (opcode == 2)
    return oDebuggerStop;
  return Debugger_Opcode_Unknown;
}
