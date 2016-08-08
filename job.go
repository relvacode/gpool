package gpool

import (
	"fmt"
)

// Hook is a function to be called when a Job changes state.
type Hook func(*State)

// JobFn is a function that is executed as a pool Job.
type JobFn func(*WorkContext) error

// NewJob wraps a Header and JobFn to implement a Job.
func NewJob(Header fmt.Stringer, Fn JobFn) Job {
	return &job{
		h:  Header,
		fn: Fn,
	}
}

// A Job is an interface that implements methods for execution on a pool
type Job interface {
	// An identity header that implements String()
	Header() fmt.Stringer

	// Run the Job.
	// If propagation is enabled on the Pool then the error returned it is propagated up and the Pool is killed.
	Run(*WorkContext) error

	// Abort is used for when a Job is in the queue and needs to be removed (via call to Pool.Kill() for example).
	// Abort is never called if the Job is already in a starting state, if it is then the Cancel channel of the
	// WorkContext is used instead.
	// Abort is called before the requesting ticket (Pool.Execute, Pool.Submit) is signalled.
	Abort()
}

// Header implements fmt.Stringer and can be used as a Header for NewJob().
type Header string

func (s Header) String() string {
	return string(s)
}

type job struct {
	fn JobFn
	h  fmt.Stringer
}

func (j *job) Header() fmt.Stringer {
	return j.h
}

func (j *job) Run(ctx *WorkContext) error {
	return j.fn(ctx)
}
