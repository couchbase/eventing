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
#include <cstdint>

// This enum class is only used within the C++ layer. It does not have any Go
// counterpart.
enum class internal_opcode {
  startMarker,
  oScanTimer,
  oUpdateV8HeapSize,
  oRunGc,
  endMarker
};

// Opcodes for incoming messages from Go to C++

// This aligns with the `eventType` in the file `consumer/protocol.go`
enum class event_type {
  startMarker,
  eDCP,
  eV8_Worker,
  eApp_Worker_Setting,
  eDebugger,
  eFilter,
  eInternal,
  ePauseConsumer,
  eConfigChange,
  endMarker
};

// This aligns with the `v8WorkerOpcode` in the file `consumer/protocol.go`
enum class v8_worker_opcode {
  startMarker,
  oInit,
  oTracker,
  oLoad,
  oGetLatencyStats,
  oGetFailureStats,
  oGetExecutionStats,
  oGetCompileInfo,
  oGetLcbExceptions,
  oGetCurlLatencyStats,
  oInsight,
  endMarker
};

// This aligns with the `dcpOpcode` in the file `consumer/protocol.go`
enum class dcp_opcode {
  startMarker,
  oDelete,
  oMutation,
  oNoOp,
  oDeleteCid,
  endMarker,
};

// This aligns with the `filterOpcode` in the file `consumer/protocol.go`
enum class filter_opcode { startMarker, oVbFilter, oProcessedSeqNo, endMarker };

// This aligns with the `appWorkerSettingsOpcode` in the file
// `consumer/protocol.go`
enum class app_worker_setting_opcode {
  startMarker,
  oLogLevel,
  oWorkerThreadCount,
  oWorkerThreadMap,
  oTimerContextSize,
  oVbMap,
  oWorkerMemQuota,
  endMarker
};

// This aligns with the `debuggerOpcode` in the file `consumer/protocol.go`
enum class debugger_opcode {
  startMarker,
  oDebuggerStart,
  oDebuggerStop,
  endMarker
};

// This aligns with the `configOpcode` in the file `consumer/protocol.go`
enum class config_opcode {
  startMarker,
  oUpdateDisableFeatureList,
  oUpdateEncryptionLevel,
  endMarker
};

event_type getEvent(uint8_t event);
v8_worker_opcode getV8WorkerOpcode(uint8_t opcode);
dcp_opcode getDCPOpcode(uint8_t opcode);
app_worker_setting_opcode getAppWorkerSettingOpcode(uint8_t opcode);
filter_opcode getFilterOpcode(uint8_t opcode);
debugger_opcode getDebuggerOpcode(uint8_t opcode);
config_opcode getConfigOpcode(uint8_t opcode);
internal_opcode getInternalOpcode(uint8_t opcode);

// Opcodes for outgoing messages from C++ to Go

// This aligns with the `respMsgType` in the file `consumer/protocol.go`
enum class resp_msg_type {
  mV8_Worker_Config = 1,
  mBucket_Ops_Response,
  mFilterAck,
  mPauseAck,
  Msg_Unknown
};

// This aligns with the `respV8WorkerConfigOpcode` in the file
// `consumer/protocol.go`
enum class v8_worker_config_opcode {
  oLatencyStats = 1,
  oFailureStats,
  oExecutionStats,
  oCompileInfo,
  oQueueSize,
  oLcbExceptions,
  oCurlLatencyStats,
  oCodeInsights,
  V8_Worker_Config_Opcode_Unknown
};

// `bucket_ops_response_opcode` has only one enum variant, which is
// `checkpointResponse`.
// Hence, on the Go side, when we receive `bucketOpsResponse` in the
// file `consumer/protocol.go`, we assume the opcode must be
// `checkpointResponse` and proceed to parse it.
// However, in the future, if this enum class has multiple enum variants, then
// only we would need a switch case on the Go side to check each possible
// opcode.
enum class bucket_ops_response_opcode { checkpointResponse };

#endif
