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
  if (event == 7)
    return eInternal;
  if (event == 8)
    return ePauseConsumer;
  if (event == 9)
    return eConfigChange;
  return Event_Unknown;
}

v8_worker_opcode getV8WorkerOpcode(int8_t opcode) {
  if (opcode == 1)
    return oDispose;
  if (opcode == 2)
    return oInit;
  if (opcode == 3)
    return oTracker;
  if (opcode == 4)
    return oLoad;
  if (opcode == 5)
    return oTerminate;
  if (opcode == 6)
    return oUnused1;
  if (opcode == 7)
    return oUnused2;
  if (opcode == 8)
    return oGetLatencyStats;
  if (opcode == 9)
    return oGetFailureStats;
  if (opcode == 10)
    return oGetExecutionStats;
  if (opcode == 11)
    return oGetCompileInfo;
  if (opcode == 12)
    return oGetLcbExceptions;
  if (opcode == 13)
    return oGetCurlLatencyStats;
  if (opcode == 14)
    return oInsight;
  return V8_Worker_Opcode_Unknown;
}

dcp_opcode getDCPOpcode(int8_t opcode) {
  if (opcode == 1)
    return oDelete;
  if (opcode == 2)
    return oMutation;
  if (opcode == 3)
    return oNoOp;
  if (opcode == 4)
    return oDeleteCid;
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
  if (opcode == 5)
    return oVbMap;
  if (opcode == 6)
    return oWorkerMemQuota;
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

config_opcode getConfigOpcode(int8_t opcode) {
  if (opcode == 1)
    return oUpdateDisableFeatureList;
  if (opcode == 2)
    return oUpdateEncryptionLevel;
  return Config_Opcode_Unknown;
}
