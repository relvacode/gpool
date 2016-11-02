// Package gpool is a utility for executing jobs in a pool of workers.
//
// A Pool consists of a central bus where input operations are called synchronously
// and a bridge which executes jobs asynchronously.
// When an input operation such as Close() or Queue() is called the operation is sent to the bus via a channel.
//
// The bus iterates on a for loop called a cycle, first processing any changes made by the last cycle and then blocks while waiting
// for either an input operation or a message from the bridge.
//
// When the bridge signals it is ready to begin executing a job and at least one job exists in the queue,
// a call to the pool's scheduler is made to locate the next job in the queue to be executed.
package gpool

import (
	"context"
	"errors"
	"time"
)

// ErrClosedPool indicates that a send was attempted on a pool which has already been closed.
var ErrClosedPool = errors.New("send on closed pool")

// ErrKilled indicates that the pool was killed by a call to Kill().
var ErrKilled = errors.New("pool killed by signal")

// ErrNotExists indicates that a cancellation request was made to a Job that does not exist in the pool.
var ErrNotExists = errors.New("job does not exist in the pool")

// ErrCancelled indicates that the job was cancelled.
var ErrCancelled = errors.New("cancelled by request")

// ErrPanicRecovered indicates that the worker recovered a panic emitted from a task.
var ErrPanicRecovered = errors.New("recovered from panic")

// DefaultScheduler is the Scheduler used by the Pool if one was not provided.
var DefaultScheduler = FIFOScheduler{}

// AsSoonAsPossible is a 0 duration that can be used when returning "timeout" in a Scheduler Evaluation call.
// This indicates that the next Evaluate call should happen as soon as possible.
const AsSoonAsPossible = time.Duration(0)

// Hook is a function to be called when a job changes state.
// Hooks are always called synchronously.
// There is no listener for additional pool requests in this period which will cause a deadlock if attempted.
// Hook functions should be quick as calling a hook blocks further processing of the pool.
type Hook func(*JobStatus)

// Pool is the main pool struct containing a bus, scheduler and bridge.
// Pool should always be invoked via NewPool().
type Pool struct {
	*bus
}

// NewPool returns a new pool with the supplied settings.
// The number of workers must be more than 0.
// If propagate is true then if a Job returns an error during execution then that error is propagated to the pool,
// during which all remaining jobs are cancelled and all queued jobs have Abort() called on them.
func NewPool(Workers uint, Propagate bool) *Pool {
	if Workers == 0 {
		panic("need at least one worker")
	}
	return NewCustomPool(Propagate, DefaultScheduler, NewStaticBridge(Workers))
}

// NewCustomPool creates a new pool with a custom Scheduler and Bridge.
func NewCustomPool(Propagate bool, Scheduler Scheduler, Bridge Bridge) *Pool {
	if Scheduler == nil {
		panic("a scheduler is required")
	}
	if Bridge == nil {
		panic("a bridge is required")
	}
	return &Pool{
		newBus(Propagate, Scheduler, Bridge),
	}
}

// Scheduler returns the scheduler used by this pool.
func (p *Pool) Scheduler() Scheduler {
	return p.scheduler
}

// Bridge returns the bridge used by this pool.
func (p *Pool) Bridge() Bridge {
	return p.bridge
}

func (p *Pool) newRequest(j Job, when Condition, ctx context.Context) *Request {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Request{
		Job:               j,
		CallbackCondition: when,
		Context:           context.WithValue(ctx, PoolKey, p),
	}
}

// ack sends the operation to the pool op queue then waits for the response.
func (p *Pool) ack(t operation) error {
	p.bus.opIN <- t
	return <-t.Receive()
}

func (p *Pool) push(j Job, when Condition, ctx context.Context) chan error {
	if ctx == nil {
		ctx = context.Background()
	}
	t := &opJob{op: newOP(), Request: p.newRequest(j, when, ctx)}
	p.opIN <- t
	return t.Receive()
}

// Kill sends a kill request to the pool bus.
// When sent, any currently running jobs have their context closed and all jobs in the pool queue have Abort() called.
// If the pool state is not OK, 'ErrClosedPool' is returned.
// No additional jobs may be sent to the pool after Kill().
func (p *Pool) Kill() error {
	return p.ack(&opSetIntent{op: newOP(), intent: intentKill})
}

// Close sends a graceful close request to the pool bus.
// Workers will finish after the last submitted job is complete and the job queue is empty.
// If the pool state is not OK, 'ErrClosedPool' is returned.
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

// WaitAsync performs the same function as Wait() but returns an asynchronous channel.
func (p *Pool) WaitAsync() chan error {
	t := &opAcknowledgeCondition{op: newOP(), when: conditionWaitRelease}
	p.opIN <- t
	return t.r
}

// Wait will block until all of the workers in the pool have exited.
// As such it is important that the caller ensures the pool will be closed or killed.
// If the pool state is Done, Wait() is resolved instantly.
func (p *Pool) Wait() error {
	return <-p.WaitAsync()
}

// Cancel tries to cancel a job in the pool by first checking to see if the job exists in the pool queue.
// If it does then call Abort() and delete that job from the pool.
// If it does not exist then check the currently executing jobs, if the job is currently executing then cancel the context.
// If the job does not exist (or had already been cancelled) then 'ErrorNotExist' is returned.
func (p *Pool) Cancel(ID string) error {
	return p.ack(&opCancel{op: newOP(), ID: ID})
}

// RequestAsync performs the same function as request but returns an asynchronous channel.
func (p *Pool) RequestAsync(r *Request) chan error {
	t := &opJob{op: newOP(), Request: r}
	p.opIN <- t
	return t.Receive()
}

// Request submits a custom job request to the pool.
func (p *Pool) Request(r *Request) error {
	return <-p.RequestAsync(r)
}

// Queue puts the given job on the [ool queue and returns nil if the job was successfully queued.
func (p *Pool) Queue(ctx context.Context, job Job) error {
	return <-p.push(job, ConditionNow, ctx)
}

// QueueBatch queues one or more jobs at the same time using the same context.
// This may also be important for a non-default scheduler
// as it allows the scheduler to see all of these jobs in the queue at the same time.
func (p *Pool) QueueBatch(ctx context.Context, jobs []Job) error {
	if len(jobs) > 0 {
		requests := []*Request{}
		for _, j := range jobs {
			requests = append(requests, p.newRequest(j, ConditionNow, ctx))
		}
		return p.ack(&opJobBatch{op: newOP(), requests: requests})
	}
	return nil
}

// StartAsync performs the same function as Start() but returns an asynchronous channel.
func (p *Pool) StartAsync(ctx context.Context, job Job) chan error {
	return p.push(job, ConditionJobStart, ctx)
}

// Start begins queueing a job and waits for it to start executing before returning.
// nil is returned if the Job successfully started to execute.
func (p *Pool) Start(ctx context.Context, job Job) error {
	return <-p.StartAsync(ctx, job)
}

// ExecuteASync performs the same function as Execute() but returns an asynchronous channel.
func (p *Pool) ExecuteASync(ctx context.Context, job Job) chan error {
	return p.push(job, ConditionJobStop, ctx)
}

// Execute queues and waits for a given job to execute in the pool.
// If the job was successfully scheduled then the error returned here is the error returned from job.Run().
func (p *Pool) Execute(ctx context.Context, job Job) error {
	return <-p.ExecuteASync(ctx, job)
}

// Status returns a snapshot of the current pool status.
// Including current job status count, running workers and general pool health.
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
