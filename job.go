package gpool

import (
	"fmt"
	"time"
)

// PoolJobFn is a function that is executed as a pool Job
// c is closed when a Kill() request is issued.
type PoolJobFn func(c chan struct{}) (interface{}, error)

// NewPoolJob creates a interface using the supplied Identifier and Job function that satisfies a PoolJob
func NewPoolJob(Identifier fmt.Stringer, Fn PoolJobFn) PoolJob {
	return &job{
		i:  Identifier,
		fn: Fn,
		c:  make(chan struct{}, 1),
	}
}

// A PoolJob is an interface that implements methods for execution on a pool
type PoolJob interface {
	// Run the job
	Run() error
	// Output from the job
	// May return nil
	Output() interface{}
	// A unique identifier
	Identifier() fmt.Stringer
	// Returns the total duration of the job
	Duration() time.Duration
	// Cancels the job during run
	Cancel()
}

type job struct {
	fn PoolJobFn
	o  interface{}
	t  time.Duration
	i  fmt.Stringer
	c  chan struct{}
}

func (j *job) Run() error {
	s := time.Now()
	o, e := j.fn(j.c)
	j.o = o
	j.t = time.Since(s)
	return e
}

func (j *job) Output() interface{} {
	return j.o
}

func (j *job) Identifier() fmt.Stringer {
	return j.i
}

func (j *job) Duration() time.Duration {
	return j.t
}

func (j *job) Cancel() {
	close(j.c)
}
