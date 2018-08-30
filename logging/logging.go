package logging

import "os"
import "fmt"
import "strings"
import "time"
import "bytes"
import "runtime/debug"
import l "log"

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

var baselevel LogLevel
var target *l.Logger
var noredact bool

func init() {
	target = l.New(os.Stdout, "", 0)
	baselevel = Info
	noredact = os.Getenv("CB_EVENTING_NOREDACT") == "true"
}

func printf(at LogLevel, format string, v ...interface{}) {
	if baselevel >= at {
		format := RedactFormat(format)
		ts := time.Now().Format("2006-01-02T15:04:05.000-07:00")
		target.Printf(ts+" ["+at.String()+"] "+format, v...)
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
	printf(Warn, format, v...)
}

func Errorf(format string, v ...interface{}) {
	printf(Error, format, v...)
}

func Fatalf(format string, v ...interface{}) {
	printf(Fatal, format, v...)
}

func Infof(format string, v ...interface{}) {
	printf(Info, format, v...)
}

func Verbosef(format string, v ...interface{}) {
	printf(Verbose, format, v...)
}

func Debugf(format string, v ...interface{}) {
	printf(Debug, format, v...)
}

func Tracef(format string, v ...interface{}) {
	printf(Trace, format, v...)
}

func IsEnabled(at LogLevel) bool {
	return baselevel >= at
}

func SetLogLevel(to LogLevel) {
	baselevel = to
}

func StackTrace() string {
	var buf bytes.Buffer
	lines := strings.Split(string(debug.Stack()), "\n")
	for _, call := range lines[2:] {
		buf.WriteString(fmt.Sprintf("%s\n", call))
	}
	return buf.String()
}
