package gpool

import (
	"fmt"
	"time"
)

// HookStart is a function to be called when a Job starts.
type HookStart func(ID int, j Job)

// HookStop is a function to be called when a Job finishes.
type HookStop func(ID int, res JobResult)

// Identifier implements String() which can be used as an fmt.Stringer in NewPoolJob
type Identifier string

func (s Identifier) String() string {
	return string(s)
}

// JobResult is the result of an execution in the Pool
type JobResult struct {
	ID       int           // Unique Job ID
	Job      Job           // Underlying Job
	Duration time.Duration // Execution duration
	Error    error         // Wrapped PoolError containing the underlying error from Job.Run()
}

// JobFn is a function that is executed as a pool Job.
// c is closed when a Kill() request is issued.
type JobFn func(c chan bool) (interface{}, error)

// NewJob creates a interface using the supplied Identifier and Job function that satisfies a PoolJob
func NewJob(Identifier fmt.Stringer, Fn JobFn) Job {
	return &job{
		i:  Identifier,
		fn: Fn,
		c:  make(chan bool, 1),
	}
}

// A Job is an interface that implements methods for execution on a pool
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

type job struct {
	fn JobFn
	o  interface{}
	i  fmt.Stringer
	c  chan bool
}

func (j *job) Run() error {
	o, e := j.fn(j.c)
	j.o = o
	return e
}

func (j *job) Output() interface{} {
	return j.o
}

func (j *job) Identifier() fmt.Stringer {
	return j.i
}

func (j *job) Cancel() {
	close(j.c)
}
