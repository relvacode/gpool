// Package gpool is a utility for executing jobs in a pool of workers
package gpool

import (
	"context"
	"errors"
)

// ErrClosedPool indicates that a send was attempted on a pool which has already been closed.
var ErrClosedPool = errors.New("send on closed pool")

// ErrKilled indicates that the pool was killed by a call to Kill()
var ErrKilled = errors.New("pool killed by signal")

// ErrWorkerCount indicates that a request to modify worker size is invalid.
var ErrWorkerCount = errors.New("invalid worker count request")

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
	return &Pool{
		newPool(Workers, Propagate, Scheduler),
	}
}

// ack attempts to send the ticket to the ticket queue.
// First waits for acknowledgement of ticket, then waits for the return message.
// In future, there may be a timeout around the return message.
func (p *Pool) ack(t operation) error {
	p.pool.opIN <- t
	return <-t.Receive()
}

func (p *Pool) push(j Job, when Condition, ctx context.Context) chan error {
	t := &opJob{op: newOP(), Request: &Request{Job: j, CallbackCondition: when, Context: ctx}}
	p.opIN <- t
	return t.Receive()
}

// Kill sends a kill request to the pool bus.
// When sent, any currently running jobs have Cancel() called.
// If the pool has already been killed or closed ErrClosedPool is returned.
// No additional jobs may be sent to the pool after Kill().
func (p *Pool) Kill() error {
	return p.ack(&opSetIntent{op: newOP(), intent: intentKill})
}

// Close sends a graceful close request to the pool bus.
// Workers will finish after the last submitted job is complete and the Job queue is empty.
// Close does not return an error if the pool is already closed.
// No additional jobs may be sent to the pool after Close().
func (p *Pool) Close() error {
	return p.ack(&opSetIntent{op: newOP(), intent: intentClose})
}

// Destroy sends a bus destroy request to the pool.
// Once all workers have exited, if a Destroy() request is active then the bus will exit
// meaning there will be no listener for further requests.
// The caller should ensure no additional requests are made to the pool after calling Destroy() to prevent a deadlock.
// Calling destroy automatically marks the pool as closed.
func (p *Pool) Destroy() error {
	return p.ack(&opAcknowledgeCondition{op: newOP(), when: conditionDestroyRelease})
}

// WaitAsync performs the same function as Wait but returns an asynchronous channel for the result of Wait.
func (p *Pool) WaitAsync() chan error {
	t := &opAcknowledgeCondition{op: newOP(), when: conditionWaitRelease}
	p.opIN <- t
	return t.r
}

// Wait will block until all of the workers in the pool have exited.
// As such it is important that the caller ensures the Pool will be closed or killed.
// If the pool state is Done; Wait() is resolved instantly.
func (p *Pool) Wait() error {
	return <-p.WaitAsync()
}

// RequestAsync performs the same function as Request but returns an asynchronous channel for the result of execution.
func (p *Pool) RequestAsync(r *Request) chan error {
	t := &opJob{op: newOP(), Request: r}
	p.opIN <- t
	return t.Receive()
}

// Request submits a custom Job request to the pool.
func (p *Pool) Request(r *Request) error {
	return <-p.RequestAsync(r)
}

// Queue puts the given Job on the Pool queue and returns nil if the Job was successfully queued.
func (p *Pool) Queue(ctx context.Context, job Job) error {
	if job == nil {
		panic("send of nil job")
	}
	return <-p.push(job, ConditionNow, ctx)
}

// QueueBatch queues one or more jobs at the same time.
// If ctx is not nil then that context is used for all Jobs in this batch.
func (p *Pool) QueueBatch(ctx context.Context, jobs []Job) error {
	if len(jobs) > 0 {
		requests := []*Request{}
		for _, j := range jobs {
			requests = append(requests, &Request{Job: j, Context: ctx})
		}
		return p.ack(&opJobBatch{op: newOP(), requests: requests})
	}
	return nil
}

// StartAsync performs the same function as Start but returns an asynchronous channel for the result of starting the Job.
func (p *Pool) StartAsync(ctx context.Context, job Job) chan error {
	if job == nil {
		panic("send of nil job")
	}
	return p.push(job, ConditionJobStart, ctx)
}

// Start begins queueing a Job and waits for it to start executing before returning.
// nil is returned if the Job successfully started to execute.
func (p *Pool) Start(ctx context.Context, job Job) error {
	return <-p.StartAsync(ctx, job)
}

// ExecuteASync performs the same function as Execute but returns an asynchronous channel
// for the result of Job execution.
func (p *Pool) ExecuteASync(ctx context.Context, job Job) chan error {
	if job == nil {
		panic("send of nil job")
	}
	return p.push(job, ConditionJobStop, ctx)
}

// Execute queues and waits for a given Job to execute in the Pool.
// If the Job was successfully scheduled then the error returned here is the error returned from Job.Run().
func (p *Pool) Execute(ctx context.Context, job Job) error {
	return <-p.ExecuteASync(ctx, job)
}

// Resize changes the amount of executing workers in the pool by the requested amount.
// If the requested size is less than 1 then ErrWorkerCount is returned.
func (p *Pool) Resize(Req int) error {
	if Req < 1 {
		return ErrWorkerCount
	}
	return p.ack(&opResize{op: newOP(), target: Req})
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
	return p.ack(&opResize{op: newOP(), alter: Req})
}

// Shrink shrinks the amount of target workers in the pool.
// If the requested shrink amount causes the amount of target workers to be less than 1 then ErrWorkerCount is returned.
// The worker count does not decrease immediately, if a worker is currently active with a Job it will exit once the Job finished.
func (p *Pool) Shrink(Req int) error {
	if Req == 0 {
		return nil
	}
	if Req < 1 {
		return ErrWorkerCount
	}
	return p.ack(&opResize{op: newOP(), alter: -Req})
}

// Status returns a snapshot of the current Pool status.
// Including current Job status count, running workers and general pool health.
// The returned value may be nil if the state could not be retrieved.
func (p *Pool) Status() *PoolStatus {
	payload := p.ack(&opGetState{op: newOP()})
	s, ok := data(payload)
	if ok {
		return s.(*PoolStatus)
	}
	return nil
}

// Error returns the current error present in the pool (if any).
func (p *Pool) Error() error {
	return p.ack(&opGetError{op: newOP()})
}
