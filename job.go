package gpool

import (
	"fmt"
)

// Hook is a function to be called when a Job changes state.
// The contents of a Hook function should NOT perform any requests against the Pool
// as a Hook is called from inside a bus cycle, there is no listener for additional requests in this period which will cause a deadlock.
// Hook functions should be quick as calling a Hook blocks further processing of the pool.
type Hook func(*State)

// JobFn is a function that is executed as a pool Job.
type JobFn func(*WorkContext) error

// NewJob wraps a Header and JobFn to implement a Job.
// AbortFn is optional and is a function to be called if the Job is aborted before it can be started.
func NewJob(Header fmt.Stringer, RunFn JobFn, AbortFn func()) Job {
	return &job{
		h:   Header,
		rFn: RunFn,
		aFn: AbortFn,
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
	rFn JobFn
	aFn func()
	h   fmt.Stringer
}

func (j *job) Header() fmt.Stringer {
	return j.h
}

func (j *job) Abort() {
	if j.aFn != nil {
		j.aFn()
	}
}

func (j *job) Run(ctx *WorkContext) error {
	return j.rFn(ctx)
}
