#include "../include/commands.h"

event_type getEvent(int8_t event) {
  if (event == 1)
    return eDCP;
  if (event == 2)
    return eHTTP;
  if (event == 3)
    return eV8_Debug;
  if (event == 4)
    return eV8_Worker;
  if (event == 5)
    return eApp_Worker_Setting;
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
  return V8_Worker_Opcode_Unknown;
}

dcp_opcode getDCPOpcode(int8_t opcode) {
  if (opcode == 1)
    return oDelete;
  if (opcode == 2)
    return oMutation;
  return DCP_Opcode_Unknown;
}

http_opcode getHTTPOpcode(int8_t opcode) {
  if (opcode == 1)
    return oGet;
  if (opcode == 2)
    return oPost;
  return HTTP_Opcode_Unknown;
}

v8_debug_opcode getV8DebugOpcode(int8_t opcode) {
  if (opcode == 1)
    return oBacktrace;
  if (opcode == 2)
    return oClear_Breakpoint;
  if (opcode == 3)
    return oContinue;
  if (opcode == 4)
    return oEvaluate;
  if (opcode == 5)
    return oFrame;
  if (opcode == 6)
    return oList_Breakpoints;
  if (opcode == 7)
    return oLookup;
  if (opcode == 8)
    return oSet_Breakpoint;
  if (opcode == 9)
    return oSource;
  if (opcode == 10)
    return oStart_Debugger;
  if (opcode == 11)
    return oStop_Debugger;
  return V8_Debug_Opcode_Unknown;
}

app_worker_setting_opcode getAppWorkerSettingOpcode(int8_t opcode) {
  if (opcode == 1)
    return oLogLevel;
  return App_Worker_Setting_Opcode_Unknown;
}
