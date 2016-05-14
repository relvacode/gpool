// Pool is a utility for managing a pool of workers
package gpool

import (
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
)

type Identifier string

func (s Identifier) String() string {
	return string(s)
}

var PoolDone = io.EOF

type Pool struct {
	c      chan PoolJob
	d      chan struct{}
	Cancel chan struct{}
	e      chan error
	o      chan PoolJob
	w      int
	cw     int // Current workers
	wg     *sync.WaitGroup
	m      *sync.Mutex
	Hook   struct {
		Done  HookFn
		Add   HookFn
		Start HookFn
		Error HookFn
	}

	killed bool
}

// NewPool creates a new Pool with the given worker count
func NewPool(Workers int) *Pool {
	return &Pool{
		c:      make(chan PoolJob),
		d:      make(chan struct{}),
		Cancel: make(chan struct{}, Workers),
		o:      make(chan PoolJob),
		e:      make(chan error, Workers),
		wg:     &sync.WaitGroup{},
		m:      &sync.Mutex{},
		w:      Workers,
	}
}

// Send sends a given PoolJob to the worker queue
func (p *Pool) Send(job PoolJob) {
	p.m.Lock()
	defer p.m.Unlock()
	if p.killed != true {
		p.c <- job
		p.doHook(HookAdd, job)
		return
	}
}

func (p *Pool) panic() {
	p.m.Lock()
	if !p.killed {
		p.killed = true
		p.m.Unlock()
		close(p.Cancel)
		return
	}
	p.m.Unlock()
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

func (p *Pool) Close() {
	close(p.c)
}

func (p *Pool) Worker() {
	go func() {
		defer p.wg.Done()
		for {
			select {
			case job, ok := <-p.c:
				if !ok {
					return
				}
				p.doHook(HookStart, job)

				d := make(chan struct{})
				go func() {
					select {
					case <-d:
						return
					case <-p.Cancel:
						job.Cancel()
					}
				}()

				e := job.Run()
				if e != nil {
					p.doHook(HookError, job)
					p.e <- NewPoolError(job, e)
					return
				}
				select {
				case <-p.Cancel:
					return
				case p.o <- job:
					close(d)
					p.doHook(HookDone, job)
				}

			case <-p.Cancel:
				return
			}
		}
	}()
}

func (p *Pool) StartWorkers() {
	for range make([]int, p.w) {
		p.wg.Add(1)
		p.Worker()
	}
	go func() {
		p.wg.Wait()
		close(p.d)
	}()
}

func (p *Pool) Wait() (jobs []PoolJob, e error) {
	catchInterrupt(p.e)
	for {
		select {
		case j := <-p.o:
			jobs = append(jobs, j)
		case err := <-p.e:
			e = err
			p.panic()
			if e == ErrCancelled {
				log.Println("Shutting down.. signal again to exit")
				ec := make(chan error)
				catchInterrupt(ec)
				select {
				case <-ec:
				case <-p.d:
				}
				return
			}
			<-p.d
		case <-p.d:
			return
		}
	}
}

// catchInterrupt launches a new Go routine that sends an error on the e channel when an interrupt signal is caught
func catchInterrupt(e chan error) {
	sK := make(chan os.Signal, 1)
	signal.Notify(sK, os.Interrupt)
	go func() {
		<-sK
		e <- ErrCancelled
	}()
}
