# Interface

js-evaluator client honours the following interface.

```golang
package defs

// Various configuration parameters accepted during Setup
type Config int

const (
	WorkersPerNode   Config = iota // Worker processes on each node (int)
	ThreadsPerWorker               // Threads in each worker (int)
	NsServerURL                    // ns_server HTTP endpoint (string)
)

// Various options available at the time of function Evaluation
type Option int

const (
	Timeout     Option = iota // time in nanoseconds for function to run (int64)
	SideEffects               // allow side effects or not (bool)
	Depth                     // recursion depth (int)
	Credentials               // currently unused (interface, to be defined here)
)

// The full list of errors returned by this interface
var (
	TimeOutErr       = errors.New("n1ql function has timed out")
	AuthFailedErr    = errors.New("authentication failed")
	InvalidOptionErr = errors.New("invalid options provided to Evaluate")
	InternalError    = errors.New("unexpected error in js evaluator")
)

type Error struct {
	Err     error        // stable comparable error key
	Details fmt.Stringer // runtime diagnostic details
}

// The function metadata
type DefnType int

const (
	Invariant DefnType = iota
	NoSideEffects
	SideEffects
)

type DefnState int

const (
	Draft      DefnState = iota // Can be edited, cannot be run
	Published                   // Can be started cannot be edited
	Deprecated                  // Cannot be started, cannot be edited
)

// The function definition
type Defn interface {
	State() DefnState
	Type() DefnType
}

// The function runtime
type Evaluator interface {
	// Evaluate a specified function against a set of parameters.
	// params and result must use Go's json.Unmarshal types, which are
	// bool, float64, string, []interface{} and map[string]interface{}.
	// The members of array and map must be one of above types themselves.
	Evaluate(library, function string,
		options map[Option]interface{},
		params []interface{}) (result interface{}, err Error)
}

// n1ql-client and gsi-client implement this
type Engine interface {
	// Register the Admin UI.
	RegisterUI(mux *http.ServeMux) Error

	// Initialize. This can be called only if there are no Evaluators running.
	Configure(cfg map[Config]interface{}) Error

	// Starts the evaluator engine after configuration.
	Start() Error

	// Fetch the current evaluator. Returns nil if not started.
	Fetch() Evaluator

	// Terminates the current evaluator engine.
	Terminate() Error

	// Fetches the function metadata
	Describe(library, function string) (Defn, Error)
}
```
