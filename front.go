// Package gpool is a utility for executing jobs in a pool of workers
package gpool

import (
	"errors"
	"time"
)

// ErrClosedPool indicates that a send was attempted on a pool which has already been closed.
var ErrClosedPool = errors.New("send on closed pool")

// ErrKilled indicates that the pool was killed by a call to Kill()
var ErrKilled = errors.New("pool killed by signal")

// ErrWorkerCount indicates that a request to modify worker size is invalid.
var ErrWorkerCount = errors.New("invalid worker count request")

// ErrTimeout indicates that a timeout request had timed out.
var ErrTimeout = errors.New("request timed out")

// Pool is the main pool struct containing a bus and workers.
// Pool should always be invoked via NewPool().
type Pool struct {
	*pool
}

// NewPool returns a new Pool with the supplied settings.
// The number of Workers must be more than 0.
// If Propagate is true then if a Job returns an error during execution then that error is propagated to the Pool,
// during which all remaining Jobs are cancelled and all queued Job have Abort() called on them.
// An optional Scheduler can be provided, if nil then DefaultScheduler is used.
func NewPool(Workers int, Propagate bool, Scheduler Scheduler) *Pool {
	if Workers == 0 {
		panic("need at least one worker")
	}
	p := &Pool{
		newPool(Workers, Propagate, Scheduler),
	}
	p.pool.start()
	return p
}

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

// Kill sends a kill request to the pool bus.
// When sent, any currently running jobs have Cancel() called.
// If the pool has already been killed or closed ErrClosedPool is returned.
// No additional jobs may be sent to the pool after Kill().
func (p *Pool) Kill() error {
	return p.ack(newTicket(tReqKill, nil))
}

// Close sends a graceful close request to the pool bus.
// Workers will finish after the last submitted job is complete.
// Close does not return an error if the pool is already closed.
// No additional jobs may be sent to the pool after Close().
func (p *Pool) Close() error {
	return p.ack(newTicket(tReqClose, nil))
}

// Destroy sends a bus destroy request to the pool.
// Once all workers have exited, if a Destroy() request is active then the bus will exit.
// This means there will be no listener for pool requests,
// you must ensure that after the pool closes via Close(), Kill() or internal error and a Destroy() request is active
// no additional requests are sent otherwise the bus will block forever.
func (p *Pool) Destroy() error {
	return p.ack(newTicket(tReqDestroy, nil))
}

// Wait will block until all of the workers in the pool have exited.
// As such it is important that the caller either uses WaitTimeout()
// or ensures a call to Pool.Close will be made.
// If all workers have already exited Wait() is resolved instantly.
func (p *Pool) Wait() error {
	return p.ack(newTicket(tReqWait, nil))
}

// WaitTimeout waits for the pool workers to exit unless the specified timeout is exceeded in which case ErrTimeout is returned.
func (p *Pool) WaitTimeout(timeout time.Duration) error {
	t := newTicket(tReqWait, nil)
	p.tIN <- t
	select {
	case <-time.After(timeout):
		return ErrTimeout
	case err := <-t.r:
		return err
	}
}

// Queue puts the given Job on the Pool queue and returns nil if the Job was successfully queued.
func (p *Pool) Queue(job Job) error {
	if job == nil {
		panic("send of nil job")
	}
	return p.ack(newTicket(tReqJobQueue, job))
}

// QueueBatch queues one or more jobs at the same time.
func (p *Pool) QueueBatch(jobs []Job) error {
	if len(jobs) > 0 {
		return p.ack(newTicket(tReqBatchJobQueue, jobs))
	}
	return nil
}

// Start begins queueing a Job and waits for it to start executing before returning.
func (p *Pool) Start(job Job) error {
	if job == nil {
		panic("send of nil job")
	}
	return p.ack(newTicket(tReqJobStartCallback, job))
}

func (p *Pool) StartAsync(job Job) chan error {
	if job == nil {
		panic("send of nil job")
	}
	t := newTicket(tReqJobStartCallback, job)
	p.pool.tIN <- t
	return t.r
}

// Execute queues and waits for a given Job to execute in the Pool.
// If the Job was successfully scheduled then the error returned here is the error returned from Job.Run().
func (p *Pool) Execute(job Job) error {
	if job == nil {
		panic("send of nil job")
	}
	return p.ack(newTicket(tReqJobStopCallback, job))
}

// ExecuteASync performs the same function as Execute but returns an asynchronous channel
// for the result of Job execution.
func (p *Pool) ExecuteASync(job Job) chan error {
	if job == nil {
		panic("send of nil job")
	}
	t := newTicket(tReqJobStopCallback, job)
	p.pool.tIN <- t
	return t.r
}

// State returns a snapshot of the current Pool state.
// Including current Job status count, running workers and general pool health.
func (p *Pool) State() *PoolState {
	data := p.ack(newTicket(tReqStat, nil))
	return data.(*returnPayload).data.(*PoolState)
}

// Error returns the current error present in the pool (if any).
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
	if Req == 0 {
		return nil
	}
	if Req < 1 {
		return ErrWorkerCount
	}
	return p.ack(newTicket(tReqGrow, Req))
}

// Shrink shrinks the amount of target workers in the pool.
// The number of running workers will not shrink until a worker has completed a task.
// If the requested shrink amount causes the amount of target workers to be less than 1 then ErrWorkerCount is returned.
func (p *Pool) Shrink(Req int) error {
	if Req == 0 {
		return nil
	}
	if Req < 1 {
		return ErrWorkerCount
	}
	return p.ack(newTicket(tReqShrink, Req))
}
