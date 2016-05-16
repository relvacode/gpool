# gpool
--
    import "github.com/relvacode/gpool"

gPool is a utility for pooling workers

## Usage

```go
const (
	// Job completed
	HookDone = 1 << iota
	// Job started
	HookStart
	// Job added
	HookAdd
)
```

```go
var ErrClosedPool = errors.New("send on closed pool")
```

```go
var ErrKilledPool = errors.New("send on killed pool")
```

#### type HookFn

```go
type HookFn func(Job)
```

HookFn is a function to be called when a Job state triggers a set Pool Hook

#### type Identifier

```go
type Identifier string
```

Identifier implements String() which can be used as an fmt.Stringer in
NewPoolJob

#### func (Identifier) String

```go
func (s Identifier) String() string
```

#### type Job

```go
type Job interface {
	// Run the job
	Run() error
	// Output from the job
	// May return nil
	Output() interface{}
	// A unique identifier
	Identifier() fmt.Stringer
	// Cancels the job during run
	Cancel()
}
```

A Job is an interface that implements methods for execution on a pool

#### func  NewJob

```go
func NewJob(Identifier fmt.Stringer, Fn JobFn) Job
```
NewJob creates a interface using the supplied Identifier and Job function that
satisfies a PoolJob

#### type JobFn

```go
type JobFn func(c chan struct{}) (interface{}, error)
```

JobFn is a function that is executed as a pool Job. c is closed when a Kill()
request is issued.

#### type JobResult

```go
type JobResult struct {
	ID       int
	Job      Job
	Duration time.Duration
	Error    error
}
```

JobResult is the result of an execution in the Pool

#### type Pool

```go
type Pool struct {

	// Hooks are functions that are executed during different stages of a Job
	Hook struct {
		Done  HookFn
		Add   HookFn
		Start HookFn
	}
}
```


#### func  NewPool

```go
func NewPool(Workers int) *Pool
```
NewPool creates a new Pool with the given worker count

#### func (*Pool) Close

```go
func (p *Pool) Close() error
```
Close sends a graceful close request to the pool bus. Workers will finish after
the last submitted job is complete. If the pool is already closed ErrClosedPool.

#### func (*Pool) Kill

```go
func (p *Pool) Kill() error
```
Kill sends a kill request to the pool bus. When sent, any currently running jobs
have Cancel() called. If the pool has already been killed ErrKilledPool is
returned.

#### func (*Pool) Notify

```go
func (p *Pool) Notify(c chan<- struct{})
```
Notify will close the given channel when the pool is cancelled

#### func (*Pool) Send

```go
func (p *Pool) Send(job Job) error
```
Send sends the given PoolJob as a request to the pool bus. If the pool has an
error before call to Send() then that error is returned. If the pool is closed
the error ErrClosedPool is returned. No error is returned if the Send() was
successful.

#### func (*Pool) Wait

```go
func (p *Pool) Wait() ([]JobResult, error)
```
Wait waits for the pool worker group to finish and then returns all jobs
completed during execution. If the pool has an error it is returned here.

#### type PoolError

```go
type PoolError struct {
	ID int
	J  Job
	E  error
}
```

PoolError is an error from a particular Job in the pool

#### func (PoolError) Error

```go
func (e PoolError) Error() string
```
