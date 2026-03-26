package logging

import (
	"bytes"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"time"

	l "log"

	"github.com/couchbase/gocb/v2"
)

type LogLevel int

// public interface
const (
	Silent LogLevel = iota
	Fatal
	Error
	Warn
	Info
	Verbose
	Timing
	Debug
	Trace
)

type LogLevelInfo struct {
	GocbLevel     string `json:"gocb_log_level,omitempty"`
	EventingLevel string `json:"eventing_log_level,omitempty"`
}

func (l LogLevelInfo) String() string {
	return fmt.Sprintf("Gocb Log Level: %s, Eventing Log Level: %s", l.GocbLevel, l.EventingLevel)
}

type logType uint8

const (
	gocbLog logType = iota
	eventingLog
)

var (
	LogTimeFormat = "2006-01-02T15:04:05.000-07:00"
)

type Logger interface {
	// Warnings, logged by default.
	Warnf(format string, v ...interface{})
	// Errors, logged by default.
	Errorf(format string, v ...interface{})
	// Fatal errors. Will not terminate execution.
	Fatalf(format string, v ...interface{})
	// Informational messages.
	Infof(format string, v ...interface{})
	// Verbose messages like request logging
	Verbosef(format string, v ...interface{})
	// Get stack trace
	StackTrace() string
	// Debugging messages
	Debugf(format string, v ...interface{})
	// Program execution - most verbose
	Tracef(format string, v ...interface{})
}

// implementation
func (t LogLevel) String() string {
	switch t {
	case Silent:
		return "Silent"
	case Fatal:
		return "Fatal"
	case Error:
		return "Error"
	case Warn:
		return "Warn"
	case Info:
		return "Info"
	case Verbose:
		return "Verbose"
	case Timing:
		return "Timing"
	case Debug:
		return "Debug"
	case Trace:
		return "Trace"
	default:
		return "Info"
	}
}

func Level(s string) LogLevel {
	if s == "" {
		return -1
	}
	switch strings.ToUpper(s) {
	case "SILENT":
		return Silent
	case "FATAL":
		return Fatal
	case "ERROR":
		return Error
	case "WARN":
		return Warn
	case "INFO":
		return Info
	case "VERBOSE":
		return Verbose
	case "TIMING":
		return Timing
	case "DEBUG":
		return Debug
	case "TRACE":
		return Trace
	default:
		return Info
	}
}

var gocbLogLevel gocb.LogLevel
var baselevel LogLevel
var target *l.Logger
var noredact bool

func init() {
	target = l.New(os.Stdout, "", 0)
	baselevel = Info
	noredact = os.Getenv("CB_EVENTING_NOREDACT") == "true"

	gocbLogLevel = gocb.LogInfo
	gocb.SetLogger(&GocbLogger{})
}

func printf(force bool, at LogLevel, format string, v ...interface{}) {
	if IsEnabled(at) || force {
		format := RedactFormat(format)
		ts := time.Now().Format(LogTimeFormat)
		msg := fmt.Sprintf(ts+" ["+at.String()+"] "+format, v...)
		if !strings.Contains(msg, "[gocb] Threshold Log:") { // sigh
			target.Print(msg)
		}
	}
}

func RedactFormat(format string) string {
	if noredact {
		format = strings.Replace(format, "%ru", "%v", -1)
	} else {
		format = strings.Replace(format, "%ru", "<ud>%v</ud>", -1)
	}
	format = strings.Replace(format, "%rm", "%v", -1) // not implemented
	format = strings.Replace(format, "%rs", "%v", -1) // not implemented
	return format
}

func Warnf(format string, v ...interface{}) {
	printf(false, Warn, format, v...)
}

func Errorf(format string, v ...interface{}) {
	printf(false, Error, format, v...)
}

func Fatalf(format string, v ...interface{}) {
	printf(true, Fatal, format, v...)
}

func Infof(format string, v ...interface{}) {
	printf(false, Info, format, v...)
}

func Verbosef(format string, v ...interface{}) {
	printf(false, Verbose, format, v...)
}

func Debugf(format string, v ...interface{}) {
	printf(false, Debug, format, v...)
}

func Tracef(format string, v ...interface{}) {
	printf(false, Trace, format, v...)
}

func IsEnabled(at LogLevel) bool {
	return baselevel >= at
}

func SetLogLevel(loglevelInfo LogLevelInfo) {
	setLogLevel(gocbLog, loglevelInfo.GocbLevel)
	setLogLevel(eventingLog, loglevelInfo.EventingLevel)
}

func setLogLevel(log logType, logLevel string) {
	to := Level(logLevel)
	if to < Silent || to > Trace {
		return
	}
	switch log {
	case gocbLog:
		switch to {
		case Timing:
			fallthrough
		case Info:
			gocbLogLevel = gocb.LogInfo
		case Trace:
			gocbLogLevel = gocb.LogTrace
		case Fatal:
			fallthrough
		case Silent:
			fallthrough
		case Error:
			gocbLogLevel = gocb.LogError
		case Warn:
			gocbLogLevel = gocb.LogWarn
		case Verbose:
			gocbLogLevel = gocb.LogMaxVerbosity
		case Debug:
			gocbLogLevel = gocb.LogDebug
		}
	case eventingLog:
		baselevel = to
	}
}

func StackTrace() string {
	var buf bytes.Buffer
	lines := strings.Split(string(debug.Stack()), "\n")
	for _, call := range lines[2:] {
		buf.WriteString(fmt.Sprintf("%s\n", call))
	}
	return buf.String()
}

// log redaction related
func TagUD(arg interface{}) interface{} {
	var udtag_begin = "<ud>"
	var udtag_end = "</ud>"
	return fmt.Sprintf("%s(%v)%s", udtag_begin, arg, udtag_end)
}
