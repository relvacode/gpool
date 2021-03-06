package gpool

import (
	"context"
	"fmt"
)

// Header wraps a string to provide an fmt.Stringer interface.
type Header string

func (s Header) String() string {
	return string(s)
}

// JobFn is a function that is executed as a pool job.
type JobFn func(context.Context) error

// NewJob wraps a Header and a run and abort function to implement a job.
// AbortFn is optional and is a function to be called if the job is aborted before it can be started.
func NewJob(Header fmt.Stringer, RunFn JobFn, AbortFn func(error)) Job {
	return &job{
		h:   Header,
		rFn: RunFn,
		aFn: AbortFn,
	}
}

// A Job is an interface that implements methods for execution on a pool.
type Job interface {
	// An identity header that implements String().
	Header() fmt.Stringer

	// Run the job with the given context.
	// The context contains the JobIDKey attached to the state of this job.
	Run(context.Context) error

	// Abort is used for when a job is in the queue and needs to be removed (via call to Pool.Kill() for example).
	// Abort is never called if the job is already in an executing state, if it is then the context is cancelled instead.
	// The reason for abortion is given in the non-nil error provided.
	Abort(error)
}

type job struct {
	rFn JobFn
	aFn func(error)
	h   fmt.Stringer
}

func (j *job) Header() fmt.Stringer {
	return j.h
}

func (j *job) Abort(err error) {
	if j.aFn != nil {
		j.aFn(err)
	}
}

func (j *job) Run(ctx context.Context) error {
	return j.rFn(ctx)
}
