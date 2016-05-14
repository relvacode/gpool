# gpool
--
    import "github.com/relvacode/gpool"

Pool is a utility for managing a pool of workers

## Usage

```go
const (
	HookDone = 1 << iota
	HookError
	HookStart
	HookAdd
)
```

```go
var ErrCancelled = errors.New("Cancelled")
```

```go
var PoolDone = io.EOF
```

#### type HookFn

```go
type HookFn func(PoolJob)
```

HookFn is a function to be called when a Job state triggers a set Pool Hook

#### type Identifier

```go
type Identifier string
```


#### func (Identifier) String

```go
func (s Identifier) String() string
```

#### type Pool

```go
type Pool struct {
	Cancel chan struct{}

	Hook struct {
		Done  HookFn
		Add   HookFn
		Start HookFn
		Error HookFn
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
func (p *Pool) Close()
```

#### func (*Pool) Send

```go
func (p *Pool) Send(job PoolJob)
```
Send sends a given PoolJob to the worker queue

#### func (*Pool) StartWorkers

```go
func (p *Pool) StartWorkers()
```

#### func (*Pool) Wait

```go
func (p *Pool) Wait() (jobs []PoolJob, e error)
```

#### func (*Pool) Worker

```go
func (p *Pool) Worker()
```

#### type PoolError

```go
type PoolError struct {
	J PoolJob
	E error
}
```

PoolError is an error from a particular Job in the pool

#### func  NewPoolError

```go
func NewPoolError(Job PoolJob, Error error) *PoolError
```

#### func (PoolError) Error

```go
func (e PoolError) Error() string
```

#### type PoolJob

```go
type PoolJob interface {
	// Run the job
	Run() error
	// Output from the job
	Output() interface{}
	// A unique identifier
	Identifier() fmt.Stringer
	// Returns the total duration of the job
	Duration() time.Duration
	// Cancels the job during run
	Cancel()
}
```

A PoolJob is an interface that implements methods for execution on a pool

#### func  NewPoolJob

```go
func NewPoolJob(Identifier fmt.Stringer, Fn PoolJobFn) PoolJob
```
NewPoolJob creates a interface using the supplied Identifier and Job function
that satisfies a PoolJob

#### type PoolJobFn

```go
type PoolJobFn func(c chan struct{}) (interface{}, error)
```

PoolJobFn is a function that is executed as a pool Job
