// gPool is a utility for pooling workers
package gpool

import (
	"errors"
	"sync"
	"time"
)

// ErrClosedPool indicates that a send was attempted on a pool which has already been closed.
var ErrClosedPool = errors.New("send on closed pool")

// Pool is the main pool struct containing a bus and workers.
// Pool should always be invoked via NewPool().
type Pool struct {
	// Workers
	wQ chan jobRequest // Queue
	wR chan JobResult  // Return
	wC chan struct{}   // Cancel
	wD chan bool       // Done

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
	p := &Pool{
		tQ: make(chan ticket),
		wQ: make(chan jobRequest),
		wC: make(chan struct{}),
		wR: make(chan JobResult),
		wD: make(chan bool),
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
const (
	tReqJob int = 1 << iota
	tReqClose
	tReqKill
	tReqWait
	tReqGetOpen
	tReqGetError
)

// A ticket is a request for input in the queue.
// This prevents direct access to queue channels which reduces the risk of bad things happening.
type ticket struct {
	t    int         // Ticket type
	data interface{} // Ticket request data
	r    chan error  // Return channel
}

// Kill sends a kill request to the pool bus.
// When sent, any currently running jobs have Cancel() called.
// If the pool has already been killed or closed ErrClosedPool is returned.
func (p *Pool) Kill() error {
	t := ticket{
		tReqKill, nil, make(chan error),
	}
	p.tQ <- t
	return <-t.r
}

// Close sends a graceful close request to the pool bus.
// Workers will finish after the last submitted job is complete.
// Close does not return an error if the pool is already closed.
func (p *Pool) Close() error {
	t := ticket{
		tReqClose, nil, make(chan error),
	}
	p.tQ <- t
	return <-t.r
}

// Wait waits for the pool worker group to finish and then returns all jobs completed during execution.
// If the pool has an error it is returned here.
// Wait will block until the pool is marked as done (via call to Pool.Close) or has an error.
// As such it is important that the caller either implements a timeout around Wait,
// or ensures a call to Pool.Close will be made.
// Wait requests are stacked until they can be resolved in the next bus cycle.
func (p *Pool) Wait() ([]JobResult, error) {
	t := ticket{
		tReqWait, nil, make(chan error),
	}
	p.tQ <- t
	return p.fJ, <-t.r
}

// Send sends the given PoolJob as a request to the pool bus.
// If the pool is closed the error ErrClosedPool is returned.
// No error is returned if the Send() was successful.
// A call to Send is blocked until a worker accepts the Job.
func (p *Pool) Send(job Job) error {
	t := ticket{
		tReqJob, job, make(chan error),
	}
	p.tQ <- t
	return <-t.r
}

// IsOpen returns true if the pool is currently open.
func (p *Pool) IsOpen() bool {
	t := ticket{
		tReqGetOpen, nil, make(chan error),
	}
	p.tQ <- t
	e := <-t.r
	if e == nil {
		return true
	}
	return false
}

// Error returns the current error present in the pool.
func (p *Pool) Error() error {
	t := ticket{
		tReqGetError, nil, make(chan error),
	}
	p.tQ <- t
	return <-t.r
}

// Running returns the current amount of running workers and the amount of requested workers.
func (p *Pool) Running() (c int, t int) {
	return p.s.State()
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
			return
		case resp := <-p.wR:
			p.res(resp)
		}
	}
}

// bus is the central communication bus for the pool.
// All pool inputs are collected here as tickets and then actioned on depending on the ticket type.
// If the ticket is a tReqWait request they are added to the slice of pending tickets and upon pool closure
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
			switch t.t {
			// New Job request
			case tReqJob:
				if p.closed || p.done {
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
			// Pool is open request
			case tReqGetOpen:
				if p.done || p.err != nil || p.closed || p.killed {
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
			if p.done && len(pendWait) != 0 {
				// Pop every item in pending and send return signal to ticket
				for {
					if len(pendWait) == 0 {
						break
					}
					var t ticket
					t, pendWait = pendWait[len(pendWait)-1], pendWait[:len(pendWait)-1]
					t.r <- p.err
				}
			}

		}

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

// worker is the Pool worker routine that receives jobRequests from the pool worker queue until the queue is closed.
// It executes the job and returns the result to the worker return channel as a JobResult.
func (p *Pool) worker(started chan bool) {
	defer p.wg.Done()
	p.s.Add()
	defer p.s.Remove()
	close(started)
	for {
		select {
		case req, ok := <-p.wQ:
			if !ok {
				return
			}

			d := make(chan struct{})
			go func() {
				select {
				case <-d:
					return
				case <-p.wC:
					req.Job.Cancel()
				}
			}()

			if p.Hook.Start != nil {
				p.Hook.Start(req.ID, req.Job)
			}

			s := time.Now()
			e := req.Job.Run()
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
			p.wR <- j

			if p.Hook.Stop != nil {
				p.Hook.Stop(req.ID, j)
			}
		case <-p.wC:
			return
		}
	}
}
