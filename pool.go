// Pool is a utility for managing a pool of workers
package gpool

import (
	"errors"
	"fmt"
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

type PoolError struct {
	J PoolJob
	E error
}

func (e PoolError) Error() string {
	return fmt.Sprintf("%s: %s", e.J.Identifier().String(), e.E.Error())
}

func NewPoolError(Job PoolJob, Error error) *PoolError {
	return &PoolError{
		J: Job,
		E: Error,
	}
}

var PoolDone = io.EOF

type Pool struct {
	c    chan PoolJob
	d    chan struct{}
	k    chan struct{}
	e    chan error
	o    chan PoolJob
	w    int
	cw   int // Current workers
	wg   *sync.WaitGroup
	m    *sync.Mutex
	Hook struct {
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
		c:  make(chan PoolJob),
		d:  make(chan struct{}),
		k:  make(chan struct{}, Workers),
		o:  make(chan PoolJob),
		e:  make(chan error, Workers),
		wg: &sync.WaitGroup{},
		m:  &sync.Mutex{},
		w:  Workers,
	}
}

// Send sends a given PoolJob to the worker queue
func (p *Pool) Send(job PoolJob) {
	p.c <- job
	p.doHook(HookAdd, job)
}

func (p *Pool) panic() {
	p.m.Lock()
	if !p.killed {
		p.killed = true
		p.m.Unlock()
		go func() {
			for range p.c {

			}
		}()
		close(p.k)
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
					case <-p.k:
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
				case <-p.k:
					return
				case p.o <- job:
					close(d)
					p.doHook(HookDone, job)
				}

			case <-p.k:
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
		//log.Println("Workers complete")
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

var ErrCancelled = errors.New("Cancelled")

// catchInterrupt launches a new Go routine that sends an error on the e channel when an interrupt signal is caught
func catchInterrupt(e chan error) {
	sK := make(chan os.Signal, 1)
	signal.Notify(sK, os.Interrupt)
	go func() {
		<-sK
		e <- ErrCancelled
	}()
}
