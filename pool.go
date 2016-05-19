// Package gpool is a utility for executing jobs in a pool of workers
package gpool

import (
	"errors"
	"sync"
	"time"
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
	// Workers
	wQ chan jobRequest // Queue
	wR chan JobResult  // Return
	wC chan struct{}   // Cancel
	wD chan bool       // Done
	wS chan chan bool  // Shrink

	fJ []JobResult
	iJ int // Job id

	tQ chan ticket // Send ticket requests

	s   *stateManager
	err error

	wg *sync.WaitGroup
	// Hooks are optional functions that are executed during different stages of a Job.
	// They are invoked by the worker and thus are called concurrently.
	// The implementor should consider any race conditions for hook callbacks.
	Hook struct {
		Start HookStart
		Stop  HookStop
	}

	closed bool
	killed bool
	done   bool
}

// NewPool creates a new Pool with the given worker count.
// Workers in the pool are started automatically.
func NewPool(Workers int) *Pool {
	if Workers == 0 {
		panic("need at least one worker")
	}
	p := &Pool{
		tQ: make(chan ticket),
		wQ: make(chan jobRequest),
		wC: make(chan struct{}),
		wR: make(chan JobResult),
		wD: make(chan bool),
		wS: make(chan chan bool),
		wg: &sync.WaitGroup{},
		s:  newStateManager(Workers),
	}
	p.start()
	return p
}

// start starts Pool workers and Pool bus
func (p *Pool) start() {
	for range make([]int, p.s.GetTarget()) {
		p.startWorker()
	}
	go func() {
		p.wg.Wait()
		p.wD <- true
	}()
	go p.bus()
}

// ticket request types
type tReq int

const (
	tReqJob tReq = 1 << iota
	tReqClose
	tReqKill
	tReqWait
	tReqGrow
	tReqShrink
	tReqHealthy
	tReqGetError
)

// A ticket is a request for input in the queue.
// This prevents direct access to queue channels which reduces the risk of bad things happening.
type ticket struct {
	t    tReq        // Ticket type
	data interface{} // Ticket request data
	ack  chan bool   // Acknowledgement channel
	r    chan error  // Return channel
}

// newTicket creates a new ticket with the supplied ReqType and optional data.
func newTicket(r tReq, data interface{}) ticket {
	return ticket{
		t:    r,
		data: data,
		ack:  make(chan bool),
		r:    make(chan error),
	}
}

// ack attempts to send the ticket to the ticket queue.
// First waits for acknowledgement of ticket, then waits for the return message.
// In future, there may be a timeout around the return message.
func (p *Pool) ack(t ticket) error {
	p.tQ <- t
	<-t.ack
	return <-t.r
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

// Wait waits for the pool worker group to finish and then returns all jobs completed during execution.
// If the pool has an error it is returned here.
// Wait will block until the pool is marked as done (via call to Pool.Close) or has an error.
// As such it is important that the caller either implements a timeout around Wait,
// or ensures a call to Pool.Close will be made.
// Wait requests are stacked until they can be resolved in the next bus cycle.
func (p *Pool) Wait() ([]JobResult, error) {
	return p.fJ, p.ack(newTicket(tReqWait, nil))
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

// Grow grows the amount of workers running in the pool by the requested amount.
// If the pool is closed ErrClosedPool is return.
func (p *Pool) Grow(Req int) error {
	if Req == 0 {
		return nil
	}
	return p.ack(newTicket(tReqGrow, Req))
}

// Shrink shrinks the amount of workers in the pool by the requested amount.
// shrink will block until the requested amount of workers have acknowledged the request.
// If the pool is full, acknowledgement happens after the worker finishes its current Job.
// If the requested shrink makes the target worker count less than 1 ErrWorkerCount is returned
func (p *Pool) Shrink(Req int) error {
	if Req == 0 {
		return nil
	}
	return p.ack(newTicket(tReqShrink, Req))
}

// Workers returns the current amount of running workers in the pool.
func (p *Pool) Workers() (c int) {
	c, _ = p.s.WorkerState()
	return
}

// Jobs returns the amount of currently executing jobs in the pool
// and the amount of executed jobs in the pool (including failed).
func (p *Pool) Jobs() (c int, f int) {
	return p.s.JobState()
}

type jobRequest struct {
	Job Job
	ID  int
}

// close closes the pool if not already closed.
// Returns true if the pool was closed.
func (p *Pool) close(kill bool) bool {
	if !p.killed && kill {
		p.killed = true
		if p.err == nil {
			p.err = ErrKilled
		}
		close(p.wC)
	}
	if !p.closed {
		p.closed = true
		close(p.wQ)
		return true
	}
	return false
}

// res receives a JobResult and processes it.
func (p *Pool) res(resp JobResult) {
	if resp.Error != nil {
		p.err = resp.Error
		p.close(true)
		return
	}
	p.fJ = append(p.fJ, resp)
}

// doSend tries to send a jobRequest to the worker pool.
// If the pool is full a deadlock is possible unless we also collect the return message from the workers
func (p *Pool) doSend(r jobRequest) {
	for {
		select {
		case p.wQ <- r:
			// Request successfully sent
			return
		case resp := <-p.wR:
			p.res(resp)
		}
	}
}

func (p *Pool) unhealthy() bool {
	return p.done || p.err != nil || p.closed || p.killed
}

// resolves resolves all remaining tickets by sending them the ctx error.
// any unresolved tickets are returned, currently unused.
func (p *Pool) resolve(ctx error, tickets ...ticket) []ticket {
	if len(tickets) != 0 {
		// Pop every item in tickets and send return signal to ticket
		for {
			if len(tickets) == 0 {
				// No more pending message, continue with next cycle
				break
			}
			var t ticket
			t, tickets = tickets[len(tickets)-1], tickets[:len(tickets)-1]
			t.r <- ctx
		}
	}
	return []ticket{}
}

// bus is the central communication bus for the pool.
// All pool inputs are collected here as tickets and then actioned on depending on the ticket type.
// If the ticket is a tReqWait request they are added to the slice of pending tickets and upon pool closure.
// A central bus is used to prevent concurrent access to any property of the pool.
func (p *Pool) bus() {
	// Pending tReqWaits
	pendWait := []ticket{}
	for {
		select {
		case <-p.wD:
			p.close(false)
			p.done = true
		// Receive worker response
		case resp := <-p.wR:
			p.res(resp)
		// Receive ticket request
		case t := <-p.tQ:
			// Acknowledge ticket receipt
			close(t.ack)
			switch t.t {
			// New Job request
			case tReqJob:
				if p.unhealthy() {
					t.r <- ErrClosedPool
					continue
				}
				p.iJ++
				p.doSend(jobRequest{
					ID:  p.iJ,
					Job: t.data.(Job),
				})
				t.r <- nil
			// Pool close request
			case tReqClose:
				if p.done {
					t.r <- ErrClosedPool
					continue
				}
				p.close(false)
				t.r <- nil
			// Pool kill request
			case tReqKill:
				if p.done {
					t.r <- ErrClosedPool
					continue
				}
				if ok := p.close(true); ok {
					t.r <- nil
				} else {
					t.r <- ErrClosedPool
				}
			// Pool wait request
			case tReqWait:
				pendWait = append(pendWait, t)
			case tReqGrow:
				i := t.data.(int)
				if p.unhealthy() {
					t.r <- ErrClosedPool
				}
				p.s.IncrTarget(i)
				p.resolveWorker()
				t.r <- nil
			case tReqShrink:
				i := t.data.(int)
				if p.unhealthy() {
					t.r <- ErrClosedPool
				}
				if _, target := p.s.WorkerState(); (target - i) > 0 {
					p.s.DecTarget(i)
					p.resolveWorker()
					t.r <- nil
				} else {
					t.r <- ErrWorkerCount
					continue
				}
			// Pool is open request
			case tReqHealthy:
				if p.unhealthy() {
					t.r <- ErrClosedPool
					continue
				}
				t.r <- nil
			case tReqGetError:
				t.r <- p.err
			default:
				panic("unknown ticket type")
			}
		// Default case resolves any pending tickets
		default:
			if p.done {
				pendWait = p.resolve(p.err, pendWait...)
			}
		}

	}
}

// resolveWorker manages the amount of running workers by either starting or stopping workers
// based on the difference in worker state.
func (p *Pool) resolveWorker() {
	c, t := p.s.WorkerState()
	// If more workers than target
	if c > t {
		for range make([]int, c-t) {
			p.stopWorker()
		}
		return
	}
	if c < t {
		for range make([]int, t-c) {
			p.startWorker()
		}
		return
	}
}

// startWorker starts a worker and waits for it to complete starting before return.
// This prevents a race condition on call to Pool.Running() where the worker may not have completed it's state registration.
func (p *Pool) startWorker() {
	ok := make(chan bool)
	p.wg.Add(1)
	go p.worker(ok)
	<-ok
}

// stopWorker generates a shrink request and waits for the worker to acknowledge shrink
func (p *Pool) stopWorker() {
	ok := make(chan bool)
	p.wS <- ok
	<-ok
}

// worker is the Pool worker routine that receives jobRequests from the pool worker queue until the queue is closed.
// It executes the job and returns the result to the worker return channel as a JobResult.
func (p *Pool) worker(started chan bool) {
	defer p.wg.Done()
	p.s.AddWorker()
	defer p.s.RemoveWorker()
	// Acknowledge start
	close(started)
	for {
		select {
		case req, ok := <-p.wQ:
			// Worker queue has been closed
			if !ok {
				return
			}

			p.s.AddJob()

			if p.Hook.Start != nil {
				p.Hook.Start(req.ID, req.Job)
			}

			// Cancel wrapper
			d := make(chan struct{})
			go func() {
				select {
				// Job done
				case <-d:
					return
				// Worker cancel signal received
				case <-p.wC:
					req.Job.Cancel()
				}
			}()

			s := time.Now()

			// Execute Job
			e := req.Job.Run()

			// Close cancel wrapping
			close(d)

			if e != nil {
				e = newPoolError(&req, e)
			}
			j := JobResult{
				Job:      req.Job,
				ID:       req.ID,
				Duration: time.Since(s),
				Error:    e,
			}
			// Send Job result to return queue
			p.wR <- j

			if p.Hook.Stop != nil {
				p.Hook.Stop(req.ID, j)
			}

			p.s.RemoveJob()

		// Cancel
		case <-p.wC:
			return
		// Shrink
		case c := <-p.wS:
			// Acknowledge shrink
			close(c)
			return
		}
	}
}
