#include "../include/commands.h"

command_type getCommandType(std::string const &command) {
  if (command == "v8_worker")
    return cV8_Worker;
  if (command == "dcp")
    return cDCP;
  if (command == "http")
    return cHTTP;
  if (command == "v8_debug")
    return cV8_Debug;
  return cUnknown;
}

cv8_worker_cmd getV8WorkerCommandType(std::string const &command) {
  if (command == "dispose")
    return mDispose;
  if (command == "init")
    return mInit;
  if (command == "load")
    return mLoad;
  if (command == "terminate")
    return mTerminate;
  return mV8_Worker_Cmd_Unknown;
}

cdcp_cmd getDCPCommandType(std::string const &command) {
  if (command == "delete")
    return mDelete;
  if (command == "mutation")
    return mMutation;
  return mDCP_Cmd_Unknown;
}

chttp_cmd getHTTPCommandType(std::string const &command) {
    if (command == "get") return mGet;
    if (command == "post") return mPost;
    return mHTTP_Cmd_Unknown;
}

cv8_debug_cmd getV8DebugCommandType(std::string const &command) {
  if (command == "backtrace")
    return mBacktrace;
  if (command == "clear_breakpoint")
    return mClear_Breakpoint;
  if (command == "continue")
    return mContinue;
  if (command == "evalute")
    return mEvaluate;
  if (command == "frame")
    return mFrame;
  if (command == "list_breakpoints")
    return mList_Breakpoints;
  if (command == "lookup")
    return mLookup;
  if (command == "set_breakpoint")
    return mSet_Breakpoint;
  if (command == "source")
    return mSource;
  if (command == "start_debugger")
    return mStart_Debugger;
  if (command == "stop_debugger")
    return mStop_Debugger;
  return mV8_Debug_Cmd_Unknown;
}
