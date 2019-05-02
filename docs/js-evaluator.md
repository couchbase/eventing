# Interface

js-evaluator client honours the following interface.

```golang
package jseval

type Config struct {
        WorkersPerNode   int
        ThreadsPerWorker int
        NsServerURL      string
}

type Option int

const (
        Timeout Option = iota  // time in nanoseconds for function to run (int64)
        SideEffects            // allow side effects or not (bool)
        Depth                  // recursion depth (int)
        Credentials            // currently unused (struct)
)

type Error struct {
        Err     error                 // stable comparable error key
        Details fmt.Stringer          // runtime diagnostic details
}

const (
        TimeOutErr       = errors.New("n1ql function has timed out")
        AuthFailedErr    = errors.New("authentication failed")
        InvalidOptionErr = errors.New("invalid options provided to Evaluate")
)

type Evaluator interface {
        // Evaluate a specified function against a set of parameters
        Evaluate(library, function string,
                options map[Option] interface{},
                params []value.Value) (result value.Value, exception Error)

        // Indicate that the client will no longer be used
        Release() Error
}

// Setup the configuration. This can be called only once per process
func Setup(cfg Config) {
}

// Fetch a function evaluator
func Get(cfg Config) (Evaluator, Error) {
}
```
