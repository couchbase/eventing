#pragma once

#include <iostream>

enum command_type {
    cV8_Worker,
    cDCP,
    cHTTP,
    cV8_Debug,
    cUnknown
};

enum cv8_worker_cmd {
    mDispose,
    mInit,
    mLoad,
    mNew,
    mTerminate,
    mVersion,
    mV8_Worker_Cmd_Unknown
};

enum cdcp_cmd {
    mDelete,
    mMutation,
    mDCP_Cmd_Unknown
};

enum chttp_cmd {
    mGet,
    mPost,
    mHTTP_Cmd_Unknown
};

enum cv8_debug_cmd {
    mBacktrace,
    mClear_Breakpoint,
    mContinue,
    mEvaluate,
    mFrame,
    mList_Breakpoints,
    mLookup,
    mSet_Breakpoint,
    mSource,
    mStart_Debugger,
    mStop_Debugger,
    mV8_Debug_Cmd_Unknown
};

command_type getCommandType(std::string const& command);
cv8_worker_cmd getV8WorkerCommandType(std::string const &command);
cdcp_cmd getDCPCommandType(std::string const &command);
chttp_cmd getHTTPCommandType(std::string const &command);
cv8_debug_cmd getV8DebugCommandType(std::string const &command);
