#ifndef COMMANDS_H
#define COMMANDS_H

#include <iostream>

enum event_type {
    eDCP,
    eHTTP,
    eV8_Debug,
    eV8_Worker,
    Event_Unknown
};

enum v8_worker_opcode {
    oDispose,
    oInit,
    oLoad,
    oTerminate,
    oVersion,
    V8_Worker_Opcode_Unknown
};

enum dcp_opcode {
    oDelete,
    oMutation,
    DCP_Opcode_Unknown
};

enum http_opcode {
    oGet,
    oPost,
    HTTP_Opcode_Unknown
};

enum v8_debug_opcode {
    oBacktrace,
    oClear_Breakpoint,
    oContinue,
    oEvaluate,
    oFrame,
    oList_Breakpoints,
    oLookup,
    oSet_Breakpoint,
    oSource,
    oStart_Debugger,
    oStop_Debugger,
    V8_Debug_Opcode_Unknown
};

event_type getEvent(int8_t event);
v8_worker_opcode getV8WorkerOpcode(int8_t opcode);
dcp_opcode getDCPOpcode(int8_t opcode);
http_opcode getHTTPOpcode(int8_t opcode);
v8_debug_opcode getV8DebugOpcode(int8_t opcode);

#endif
