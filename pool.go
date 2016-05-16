// gPool is a utility for pooling workers
package gpool

import (
	"errors"
	"sync"
	"time"
)

// ErrClosedPool indicates that a send was attempted on a pool which has already been closed
var ErrClosedPool = errors.New("send on closed pool")

// ErrKilledPool indicates that a send was attempted on a pool was has been killed due to an error
var ErrKilledPool = errors.New("send on killed pool")

type Pool struct {
	// Workers
	wQ chan jobRequest // Queue
	wR chan JobResult  // Return
	wC chan struct{}   // Cancel

	fJ []JobResult
	iJ int // Job id

	tQ chan ticket // Send ticket requests

	w   int
	err error
	wg  *sync.WaitGroup
	// Hooks are functions that are executed during different stages of a Job
	Hook struct {
		Done  HookFn
		Add   HookFn
		Start HookFn
	}

	killed bool
	closed bool
}

// NewPool creates a new Pool with the given worker count.
// Workers in the pool are started automatically.
func NewPool(Workers int) *Pool {
	p := &Pool{
		tQ: make(chan ticket),
		wQ: make(chan jobRequest),
		wC: make(chan struct{}, Workers),
		wR: make(chan JobResult),
		wg: &sync.WaitGroup{},
		w:  Workers,
	}
	p.start()
	return p
}

// start starts Pool workers and Pool bus
func (p *Pool) start() {
	for range make([]int, p.w) {
		p.wg.Add(1)
		go p.worker()
	}
	go func() {
		p.wg.Wait()
		close(p.wR)
	}()
	go p.bus()
}

// call calls a hook if not nil
func call(Fn func(Job), j Job) {
	if Fn != nil {
		Fn(j)
	}
}

// doHook calls a registered hook on the supplied Job
func (p *Pool) doHook(Method int, Job Job) {
	switch Method {
	case HookAdd:
		call(p.Hook.Add, Job)
	case HookDone:
		call(p.Hook.Done, Job)
	case HookStart:
		call(p.Hook.Start, Job)
	default:
		panic("unknown hook method")
	}
}

const (
	tReqJob int = 1 << iota
	tReqClose
	tReqKill
	tReqWait
)

// A ticket is a request for input in the queue.
// This prevents direct access to queue channels which reduces the risk of bad things happening.
type ticket struct {
	t int        // Ticket type
	j Job        // Job request, used with tReqJob
	r chan error // Return channel
}

// Kill sends a kill request to the pool bus.
// When sent, any currently running jobs have Cancel() called.
// If the pool has already been killed ErrKilledPool is returned.
func (p *Pool) Kill() error {
	t := ticket{
		tReqKill, nil, make(chan error),
	}
	p.tQ <- t
	return <-t.r
}

// Close sends a graceful close request to the pool bus.
// Workers will finish after the last submitted job is complete.
// If the pool is already closed ErrClosedPool.
func (p *Pool) Close() error {
	t := ticket{
		tReqClose, nil, make(chan error),
	}
	p.tQ <- t
	return <-t.r
}

// Wait waits for the pool worker group to finish and then returns all jobs completed during execution.
// If the pool has an error it is returned here.
func (p *Pool) Wait() ([]JobResult, error) {
	t := ticket{
		tReqWait, nil, make(chan error),
	}
	p.tQ <- t
	return p.fJ, <-t.r
}

// Send sends the given PoolJob as a request to the pool bus.
// If the pool has an error before call to Send() then that error is returned.
// If the pool is closed the error ErrClosedPool is returned.
// No error is returned if the Send() was successful.
func (p *Pool) Send(job Job) error {
	t := ticket{
		tReqJob, job, make(chan error),
	}
	p.tQ <- t
	return <-t.r
}

type jobRequest struct {
	Job Job
	ID  int
}

// bus is the central communication bus for the pool.
// All pool inputs are collected here as tickets and then actioned on depending on the ticket type.
// If the ticket is a tReqWait request they are added to the slice of pending tickets and upon pool closure
// the ticket return channel is sent any pool errors.
func (p *Pool) bus() {
	// Pending tReqWaits
	pending := []ticket{}
	for {
		select {
		// Receive worker response
		case resp, ok := <-p.wR:
			if !ok {
				continue
			}
			// Kill pool if response contains an error
			if resp.Error != nil {
				p.err = resp.Error
				if !p.killed {
					p.killed = true
					close(p.wC)
				}
				// Else add it to the list of completed jobs
			} else {
				p.fJ = append(p.fJ, resp)
			}
		// Receive ticket request
		case t := <-p.tQ:
			switch t.t {
			// New Job request
			case tReqJob:
				if p.err != nil {
					t.r <- p.err
					continue
				}
				if p.killed {
					t.r <- ErrKilledPool
					continue
				}
				if p.closed {
					t.r <- ErrClosedPool
					continue
				}
				p.iJ++
				p.wQ <- jobRequest{
					Job: t.j,
					ID:  p.iJ,
				}
				p.doHook(HookAdd, t.j)
				t.r <- nil
			// Pool close request
			case tReqClose:
				if !p.closed {
					p.closed = true
					close(p.wQ)
					t.r <- nil
					continue
				}
				t.r <- ErrClosedPool
			// Pool kill request
			case tReqKill:
				if !p.killed {
					p.killed = true
					close(p.wC)
					t.r <- nil
					continue
				}
				t.r <- ErrKilledPool
			// Pool wait request
			case tReqWait:
				pending = append(pending, t)
			}
		// Default case resolves any pending tickets
		default:
			if p.closed || p.killed {
				// Pop every item in pending and send return signal to ticket
				for {
					if len(pending) == 0 {
						break
					}
					var t ticket
					t, pending = pending[len(pending)-1], pending[:len(pending)-1]
					t.r <- p.err
				}
			}

		}

	}
}

// worker is the Pool worker routine that receives jobRequests from the pool worker queue until the queue is closed.
// It executes the job and returns the result to the worker return channel as a JobResult.
func (p *Pool) worker() {
	defer p.wg.Done()
	for {
		select {
		case req, ok := <-p.wQ:
			if !ok {
				return
			}
			p.doHook(HookStart, req.Job)

			d := make(chan struct{})
			go func() {
				select {
				case <-d:
					return
				case <-p.wC:
					req.Job.Cancel()
				}
			}()

			s := time.Now()
			e := req.Job.Run()

			if e != nil {
				e = newPoolError(&req, e)
			}
			p.wR <- JobResult{
				Job:      req.Job,
				ID:       req.ID,
				Duration: time.Since(s),
				Error:    e,
			}
			close(d)
			if e != nil {
				p.doHook(HookDone, req.Job)
			}
		case <-p.wC:
			return
		}
	}
}
