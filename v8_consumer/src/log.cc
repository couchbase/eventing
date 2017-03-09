#include "../include/log.h"

#include <sstream>

std::ostringstream os;

LogLevel desiredLogLevel = LogLevel(0);

void setLogLevel(LogLevel level) { desiredLogLevel = level; }
