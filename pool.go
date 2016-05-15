// gPool is a utility for pooling workers
package gpool

import (
	"errors"
	"sync"
)

var ErrClosedPool = errors.New("send on closed pool")
var ErrKilledPool = errors.New("send on killed pool")

type Pool struct {
	// Workers
	wQ chan PoolJob  // Queue
	wE chan error    // Error
	wC chan struct{} // Cancel
	wR chan PoolJob  // Return

	fJ []PoolJob

	tQ chan *ticket // Send ticket requests

	w    int
	err  error
	wg   *sync.WaitGroup
	m    *sync.Mutex
	Hook struct {
		Done  HookFn
		Add   HookFn
		Start HookFn
		Error HookFn
	}

	killed bool
	closed bool
}

// NewPool creates a new Pool with the given worker count
func NewPool(Workers int) *Pool {
	p := &Pool{
		tQ: make(chan *ticket),
		wQ: make(chan PoolJob),
		wC: make(chan struct{}, Workers),
		wR: make(chan PoolJob),
		wE: make(chan error, Workers),
		wg: &sync.WaitGroup{},
		m:  &sync.Mutex{},
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
	go p.bus()
}

func call(Fn func(PoolJob), j PoolJob) {
	if Fn != nil {
		Fn(j)
	}
}

func (p *Pool) doHook(Method int, Job PoolJob) {
	switch Method {
	case HookAdd:
		call(p.Hook.Add, Job)
	case HookDone:
		call(p.Hook.Done, Job)
	case HookStart:
		call(p.Hook.Start, Job)
	case HookError:
		call(p.Hook.Error, Job)
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
)

type ticket struct {
	t int
	j PoolJob
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
	t := &ticket{
		tReqKill, nil, make(chan error),
	}
	p.tQ <- t
	return <-t.r
}

// Close sends a graceful close request to the pool bus.
// Workers will finish after the last submitted job is complete.
// If the pool is already closed ErrClosedPool.
func (p *Pool) Close() error {
	t := &ticket{
		tReqClose, nil, make(chan error),
	}
	p.tQ <- t
	return <-t.r
}

// Send sends the given PoolJob as a request to the pool bus.
// If the pool has an error before call to Send() then that error is returned.
// If the pool is closed the error ErrClosedPool is returned.
// No error is returned if the Send() was successful.
func (p *Pool) Send(job PoolJob) error {
	t := &ticket{
		tReqJob, job, make(chan error),
	}
	p.tQ <- t
	return <-t.r
}

func (p *Pool) bus() {
	for {
		select {
		case j := <-p.wR:
			p.fJ = append(p.fJ, j)
		case e := <-p.wE:
			p.err = e
			p.kill()
		case t := <-p.tQ:
			switch t.t {
			case tReqJob:
				if p.err != nil {
					t.r <- p.err
					continue
				}
				if !p.killed && !p.closed {
					p.wQ <- t.j
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
			}

		}

	}
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for {
		select {
		case job, ok := <-p.wQ:
			if !ok {
				return
			}
			p.doHook(HookStart, job)

			d := make(chan struct{})
			go func() {
				select {
				case <-d:
					return
				case <-p.wC:
					job.Cancel()
				}
			}()

			e := job.Run()

			if e != nil {
				p.doHook(HookError, job)
				p.wE <- newPoolError(job, e)
				// Wait for the bus to acknowledge error
				<-p.wC
				return
			}
			p.wR <- job
			close(d)
			p.doHook(HookDone, job)
		case <-p.wC:
			return
		}
	}
}

// Wait waits for the pool worker group to finish and then returns all jobs completed during execution.
// If the pool has an error it is returned here.
func (p *Pool) Wait() ([]PoolJob, error) {
	p.wg.Wait()
	return p.fJ, p.err
}
