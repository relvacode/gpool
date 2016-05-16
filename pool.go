// gPool is a utility for pooling workers
package gpool

import (
	"errors"
	"sync"
	"time"
)

var ErrClosedPool = errors.New("send on closed pool")
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

// NewPool creates a new Pool with the given worker count
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

func call(Fn func(Job), j Job) {
	if Fn != nil {
		Fn(j)
	}
}

func (p *Pool) doHook(Method int, Job Job) {
	switch Method {
	case HookAdd:
		call(p.Hook.Add, Job)
	case HookDone:
		call(p.Hook.Done, Job)
	case HookStart:
		call(p.Hook.Start, Job)
	default:
		panic("Unknown hook method")
	}
}

// Notify will close the given channel when the pool is cancelled
func (p *Pool) Notify(c chan<- struct{}) {
	if c == nil {
		panic("Cannot close nil channel")
	}
	go func() {
		<-p.wC
		close(c)
	}()
}

const (
	tReqJob int = 1 << iota
	tReqClose
	tReqKill
	tReqWait
)

type ticket struct {
	t int
	j Job
	r chan error // Return channel
}

func (p *Pool) kill() {
	if !p.killed {
		p.killed = true
		close(p.wC)
	}
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
	e := <-t.r
	return p.fJ, e
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
			if resp.Error != nil {
				p.err = resp.Error
				p.kill()
			} else {
				p.fJ = append(p.fJ, resp)
			}
		// Receive ticket request
		case t := <-p.tQ:
			switch t.t {
			case tReqJob:
				if p.err != nil {
					t.r <- p.err
					continue
				}
				if !p.killed && !p.closed {
					p.iJ++
					p.wQ <- jobRequest{
						Job: t.j,
						ID:  p.iJ,
					}
					p.doHook(HookAdd, t.j)
					t.r <- nil
					continue
				}
				t.r <- ErrClosedPool
			case tReqClose:
				if !p.closed {
					p.closed = true
					close(p.wQ)
					t.r <- nil
					continue
				}
				t.r <- ErrClosedPool
			case tReqKill:
				if !p.killed {
					p.killed = true
					close(p.wC)
					t.r <- nil
					continue
				}
				t.r <- ErrKilledPool
			case tReqWait:
				pending = append(pending, t)
				continue
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
