package logging

import (
	"fmt"

	"github.com/couchbase/gocb/v2"
)

type GocbLogger struct{}

const (
	gocbFormat = "[gocb] %s"
)

func (r *GocbLogger) Log(level gocb.LogLevel, offset int, format string, v ...interface{}) error {
	if level > gocbLogLevel {
		return nil
	}
	printf(true, mask(level), gocbFormat, fmt.Sprintf(format, v...))
	return nil
}

func mask(level gocb.LogLevel) LogLevel {
	switch level {
	case gocb.LogSched:
		fallthrough
	case gocb.LogTrace:
		return Trace
	case gocb.LogMaxVerbosity:
		return Verbose
	case gocb.LogDebug:
		return Debug
	case gocb.LogInfo:
		return Info
	case gocb.LogWarn:
		return Warn
	case gocb.LogError:
		return Error
	default:
		return Debug
	}
}
