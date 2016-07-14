// Package gpool is a utility for executing jobs in a pool of workers
package gpool

import (
	"errors"
)

// ErrClosedPool indicates that a send was attempted on a pool which has already been closed.
var ErrClosedPool = errors.New("send on closed pool")

// ErrKilled indicates that the pool was killed by a call to Kill()
var ErrKilled = errors.New("pool killed by signal")

// ErrCancelled indicates that the job was cancelled via call to Kill()
var ErrCancelled = errors.New("job cancelled by request")

// ErrWorkerCount indicates that a request to modify worker size is invalid.
var ErrWorkerCount = errors.New("invalid worker count request")

// Pool is the main pool struct containing a bus and workers.
// Pool should always be invoked via NewPool().
type Pool struct {
	*pool
}

// NewPool creates a new Pool with the given worker count.
// Workers in the pool are started automatically.
func NewPool(Workers int) *Pool {
	if Workers == 0 {
		panic("need at least one worker")
	}
	p := &Pool{
		newPool(Workers),
	}
	p.pool.start()
	return p
}

//// JobID returns a Job by the specified ID and whether it was found in the pool manager.
//func (p *Pool) JobID(ID string) (JobState, bool) {
//	return p.mgr.ID(ID)
//}
//
//// Jobs returns all Jobs with a given state.
//// State may be empty, in which case all jobs are returned.
//func (p *Pool) Jobs(State string) []JobState {
//	s := p.mgr.Jobs()
//	r := []JobState{}
//	for _, j := range s {
//		if j.State == State || State == "" {
//			r = append(r, j)
//		}
//	}
//	return r
//}
//
//// Workers returns the number of currently active workers (both executing and idle).
//func (p *Pool) Workers() int {
//	return p.mgr.RunningWorkers()
//}
//
//// State takes a snapshot of the current pool state.
//func (p *Pool) State() map[string]interface{} {
//	return p.mgr.State()
//}

// ack attempts to send the ticket to the ticket queue.
// First waits for acknowledgement of ticket, then waits for the return message.
// In future, there may be a timeout around the return message.
func (p *Pool) ack(t ticket) error {
	p.pool.tIN <- t
	return <-t.r
}

func (p *Pool) payload(t ticket) interface{} {
	e := p.ack(t)
	return e.(*returnPayload).data
}

func (p *Pool) Jobs(State string) []JobState {
	return p.payload(newTicket(tReqJobQuery, stateMatcher(State))).([]JobState)
}

// Workers returns the number of running workers in the Pool.
func (p *Pool) Workers() int {
	return p.payload(newTicket(tReqWorkerQuery, nil)).(int)
}

// Kill sends a kill request to the pool bus.
// When sent, any currently running jobs have Cancel() called.
// If the pool has already been killed or closed ErrClosedPool is returned.
func (p *Pool) Kill() error {
	return p.ack(newTicket(tReqKill, nil))
}

// Close sends a graceful close request to the pool bus.
// Workers will finish after the last submitted job is complete.
// Close does not return an error if the pool is already closed.
func (p *Pool) Close() error {
	return p.ack(newTicket(tReqClose, nil))
}

// Destroy sends a bus destroy request to the pool.
// Once all workers have exited, if a Destroy() request is active then the bus will exit.
// This means there will be no listener for pool requests,
// you must ensure that after the pool closes via Close(), Kill() or internal error and a Destroy() request is active
// no additional requests are sent otherwise a deadlock is possible.
func (p *Pool) Destroy() error {
	return p.ack(newTicket(tReqDestroy, nil))
}

// Wait waits for the pool worker group to finish.
// Wait will block until all of the workers in the pool have exited.
// As such it is important that the caller either implements a timeout around Wait,
// or ensures a call to Pool.Close will be made.
// If all workers have already exited Wait() is resolved instantly.
func (p *Pool) Wait() error {
	return p.ack(newTicket(tReqWait, nil))
}

// Send sends the given PoolJob as a request to the pool bus.
// If the pool is closed the error ErrClosedPool is returned.
// No error is returned if the Send() was successful.
// A call to Send is blocked until a worker accepts the Job.
func (p *Pool) Send(job Job) error {
	if job == nil {
		panic("send of nil job")
	}
	return p.ack(newTicket(tReqJob, job))
}

// SendAsync performs the same action as Send but returns an error channel instead of an error.
// Exactly one error will be sent on this channel.
// If the job was successfully started on a worker the returned error will be nil, blocking until then.
func (p *Pool) SendAsync(job Job) chan error {
	if job == nil {
		panic("send of nil job")
	}
	t := newTicket(tReqJob, job)
	p.pool.tIN <- t
	return t.r
}

// Healthy returns true if the pool is healthy and able to receive further jobs.
func (p *Pool) Healthy() bool {
	if p.ack(newTicket(tReqHealthy, nil)) == nil {
		return true
	}
	return false
}

// Error returns the current error present in the pool.
func (p *Pool) Error() error {
	return p.ack(newTicket(tReqGetError, nil))
}

// Resize changes the amount of executing workers in the pool by the requested amount.
// If the requested size is less than 1 then ErrWorkerCount is returned.
func (p *Pool) Resize(Req int) error {
	if Req < 1 {
		return ErrWorkerCount
	}
	return p.ack(newTicket(tReqResize, Req))
}

// Grow grows the amount of workers running in the pool by the requested amount.
// Unlike Shrink(), additional workers are started instantly.
// If the pool is closed ErrClosedPool is return.
func (p *Pool) Grow(Req int) error {
	if Req < 1 {
		return ErrWorkerCount
	}
	return p.ack(newTicket(tReqGrow, Req))
}

// Shrink shrinks the amount of target workers in the pool.
// The number of running workers will not shrink until a worker has completed a task.
// If the requested shrink amount causes the amount of target workers to be less than 1 then ErrWorkerCount is returned.
func (p *Pool) Shrink(Req int) error {
	if Req < 1 {
		return ErrWorkerCount
	}
	return p.ack(newTicket(tReqShrink, Req))
}
