// Package gpool is a utility for executing jobs in a pool of workers
package gpool

import (
	"container/list"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// ErrClosedPool indicates that a send was attempted on a pool which has already been closed.
var ErrClosedPool = errors.New("send on closed pool")

// ErrKilled indicates that the pool was killed by a call to Kill()
var ErrKilled = errors.New("pool killed by signal")

// ErrCancelled indicates that the job was cancelled via call to Kill()
var ErrCancelled = errors.New("job cancelled by request")

// ErrWorkerCount indicates that a request to modify worker size is invalid.
var ErrWorkerCount = errors.New("invalid worker count request")

// Pool is the main pool struct containing a bus and workers.
// Pool should always be invoked via NewPool().
type Pool struct {
	// Workers
	wQ chan jobRequest // Queue
	wR chan *jobResult // Return
	wC chan struct{}   // Cancel
	wD chan bool       // Done
	wS chan chan bool  // Shrink

	tQ chan ticket // Send ticket requests

	mgr *mgr
	err error

	wg *sync.WaitGroup
	// Hooks are optional functions that are executed during different stages of a Job.
	// They are invoked by the worker and thus are called concurrently.
	// The implementor should consider any race conditions for hook callbacks.
	Hook struct {
		Queue Hook // Job becomes queued
		Start Hook // Job starts
		Stop  Hook // Job stops
	}

	state  int // Current pool state
	intent int // Pool status intent for next cycle

	pendWait    []ticket
	pendDestroy []ticket
	pendClose   *ticket
	pendCancel  *ticket

	q *list.List
}

// NewPool creates a new Pool with the given worker count.
// Workers in the pool are started automatically.
func NewPool(Workers int) *Pool {
	if Workers == 0 {
		panic("need at least one worker")
	}
	p := &Pool{
		tQ:  make(chan ticket),
		wQ:  make(chan jobRequest),
		wC:  make(chan struct{}),
		wR:  make(chan *jobResult),
		wD:  make(chan bool),
		wS:  make(chan chan bool),
		wg:  &sync.WaitGroup{},
		mgr: newMgr(Workers),
		q:   list.New(),
	}
	p.start()
	return p
}

// start starts Pool workers and Pool bus
func (p *Pool) start() {
	_, t := p.mgr.workers()
	for range make([]int, t) {
		p.startWorker()
	}
	go func() {
		p.wg.Wait()
		p.wD <- true
	}()
	go p.bus()
}

// JobID returns a Job by the specified ID and whether it was found in the pool manager.
func (p *Pool) JobID(ID string) (JobState, bool) {
	return p.mgr.ID(ID)
}

// Jobs returns all Jobs with a given state.
// State may be empty, in which case all jobs are returned.
func (p *Pool) Jobs(State string) []JobState {
	s := p.mgr.Jobs()
	r := []JobState{}
	for _, j := range s {
		if j.State == State || State == "" {
			r = append(r, j)
		}
	}
	return r
}

// Workers returns the number of currently active workers (both executing and idle).
func (p *Pool) Workers() int {
	return p.mgr.RunningWorkers()
}

// State takes a snapshot of the current pool state.
func (p *Pool) State() PoolState {
	return p.mgr.State()
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
	tReqResize
	tReqGetError
	tReqDestroy
)

// A ticket is a request for input in the queue.
// This prevents direct access to queue channels which reduces the risk of bad things happening.
type ticket struct {
	t    tReq        // Ticket type
	data interface{} // Ticket request data
	r    chan error  // Return channel
}

// newTicket creates a new ticket with the supplied ReqType and optional data.
func newTicket(r tReq, data interface{}) ticket {
	return ticket{
		t:    r,
		data: data,
		r:    make(chan error, 1),
	}
}

// ack attempts to send the ticket to the ticket queue.
// First waits for acknowledgement of ticket, then waits for the return message.
// In future, there may be a timeout around the return message.
func (p *Pool) ack(t ticket) error {
	p.tQ <- t
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

// Destroy sends a bus destroy request to the pool.
// Once all workers have exited, if a Destroy() request is active then the bus will exit.
// This means there will be no listener for pool requests,
// you must ensure that after the pool closes via Close(), Kill() or internal error and a Destroy() request is active
// no additional requests are sent otherwise a deadlock is possible.
func (p *Pool) Destroy() error {
	return p.ack(newTicket(tReqDestroy, nil))
}

// Wait waits for the pool worker group to finish.
// Wait will block until all of the workers in the pool have exited.
// As such it is important that the caller either implements a timeout around Wait,
// or ensures a call to Pool.Close will be made.
// If all workers have already exited Wait() is resolved instantly.
func (p *Pool) Wait() error {
	return p.ack(newTicket(tReqWait, nil))
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

// SendAsync performs the same action as Send but returns an error channel instead of an error.
// Exactly one error will be sent on this channel.
// If the job was successfully started on a worker the returned error will be nil, blocking until then.
func (p *Pool) SendAsync(job Job) chan error {
	if job == nil {
		panic("send of nil job")
	}
	t := newTicket(tReqJob, job)
	p.tQ <- t
	return t.r
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

// Resize changes the amount of executing workers in the pool by the requested amount.
func (p *Pool) Resize(Req int) error {
	if Req < 1 {
		return nil
	}
	return p.ack(newTicket(tReqResize, Req))
}

// Grow grows the amount of workers running in the pool by the requested amount.
// If the pool is closed ErrClosedPool is return.
func (p *Pool) Grow(Req int) error {
	if Req < 1 {
		return nil
	}
	return p.ack(newTicket(tReqGrow, Req))
}

// Shrink shrinks the amount of workers in the pool by the requested amount.
// shrink will block until the requested amount of workers have acknowledged the request.
// If the pool is full, acknowledgement happens after the worker finishes its current Job.
// If the requested shrink makes the target worker count less than 1 ErrWorkerCount is returned
func (p *Pool) Shrink(Req int) error {
	if Req < 1 {
		return nil
	}
	return p.ack(newTicket(tReqShrink, Req))
}

// res receives a JobResult and processes it.
func (p *Pool) res(resp *jobResult) bool {
	if resp.Error != nil {
		p.err = resp.Error
		return false
	}
	return true
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

const (
	none int = iota
	closed
	cancelled
	done
)

const (
	_ int = iota
	wantclose
	wantcancel
)

func (p *Pool) do(t ticket) {
	switch t.t {
	// New Job request
	case tReqJob:
		if p.state > 0 {
			t.r <- ErrClosedPool
			return
		}
		u := uuid()
		if u == "" {
			t.r <- fmt.Errorf("unable to generate uuid")
			return
		}
		jr := jobRequest{
			ID:  u,
			Job: t.data.(Job),
			Ack: t.r,
		}
		p.q.PushBack(jr)
		js := p.mgr.trackReq(jr, Queued)
		if p.Hook.Queue != nil {
			p.Hook.Queue(js)
		}
	// Pool close request
	case tReqClose:
		if p.state > 0 {
			t.r <- nil
			return
		}
		if p.intent < wantclose {
			p.intent = wantclose
			p.pendClose = &t
			return
		}
		t.r <- nil
	// Pool kill request
	case tReqKill:
		if p.state > wantclose {
			t.r <- nil
			return
		}
		if p.intent < wantcancel {
			p.intent = wantcancel
			p.pendCancel = &t
			return
		}
	// Pool wait request
	case tReqWait:
		// If done resolve ticket instantly
		if p.state == done {
			p.resolve(p.err, t)
			return
		}
		p.pendWait = append(p.pendWait, t)
	case tReqGrow:
		i := t.data.(int)
		if p.state != none {
			t.r <- ErrClosedPool
			return
		}
		p.mgr.adjWorkerTarget(i)
		p.resolveWorker()
		t.r <- nil
	case tReqShrink:
		i := t.data.(int)
		if p.state != none {
			t.r <- ErrClosedPool
			return
		}
		if _, target := p.mgr.workers(); (target - i) > 0 {
			p.mgr.adjWorkerTarget(-i)
			p.resolveWorker()
			t.r <- nil
		} else {
			t.r <- ErrWorkerCount
			return
		}
	case tReqResize:
		i := t.data.(int)
		if p.state != none {
			t.r <- ErrClosedPool
			return
		}
		p.mgr.setWorkerTarget(i)
		p.resolveWorker()
		t.r <- nil
	// Pool is open request
	case tReqHealthy:
		if p.state != none {
			t.r <- ErrClosedPool
			return
		}
		t.r <- nil
	case tReqGetError:
		t.r <- p.err
	case tReqDestroy:
		p.pendDestroy = append(p.pendDestroy, t)
	default:
		panic("unexpected ticket type")
	}
}

// clearQ iterates through the pending send queue and clears all requests by acknowledging them with the current pool error.
func (p *Pool) clearQ(err error) {
	for e := p.q.Front(); e != nil; e = e.Next() {
		p.mgr.trackReq(e.Value.(jobRequest), Failed)
		e.Value.(jobRequest).Ack <- p.err
		p.q.Remove(e)
	}
}

// bus is the central communication bus for the pool.
// All pool inputs are collected here as tickets and then actioned on depending on the ticket type.
// If the ticket is a tReqWait request they are added to the slice of pending tickets and upon pool closure.
// A central bus is used to prevent concurrent access to any property of the pool.
func (p *Pool) bus() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

cycle:
	for {
		// If pool state is done without any intention and the queue is zero length
		if p.state == done && p.intent == none {
			if p.q.Len() == 0 {
				p.clearQ(p.err)
			}
			// Resolve pending waits
			p.pendWait = p.resolve(p.err, p.pendWait...)

			// If pending destroys then resolve and exit bus.
			if len(p.pendDestroy) > 0 {
				p.pendDestroy = p.resolve(p.err, p.pendDestroy...)
				break cycle
			}
		}

		// Handle pool intent (close or cancel)
		switch p.intent {
		case wantcancel:
			if p.state >= cancelled {
				p.intent = none
				break
			}
			if p.err == nil {
				p.err = ErrKilled
			}
			close(p.wC)
			if p.pendCancel != nil {
				p.pendCancel.r <- nil
				p.pendCancel = nil
			}
			if p.state < closed {
				close(p.wQ)
				p.state = closed
			}
			if p.pendClose != nil {
				p.pendClose.r <- nil
				p.pendClose = nil
			}
			p.clearQ(p.err)

			p.state = cancelled
			p.intent = none
		case wantclose:
			if p.q.Len() > 0 {
				break
			}
			if p.state >= closed {
				p.intent = none
				break
			}
			close(p.wQ)
			p.state = closed
			if p.pendClose != nil {
				p.pendClose.r <- nil
				p.pendClose = nil
			}
			p.intent = none
		}

		cases := []reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(tick.C)}, // Timeout ticket
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.tQ)},   // Ticket input queue
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.wR)},   // Worker return queue
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.wD)},   // Worker done (wg done) queue
		}

		// If a job is ready on the queue then try to send it by adding to reflect select cases.
		elem := p.q.Front()
		if elem != nil && p.state == 0 {
			cases = append(cases,
				reflect.SelectCase{
					Dir: reflect.SelectSend, Chan: reflect.ValueOf(p.wQ),
					Send: reflect.ValueOf(elem.Value),
				})
		}

		c, v, _ := reflect.Select(cases)
		switch c {
		case 0: // Tick timeout
			continue
		case 1: // Ticket request
			p.do(v.Interface().(ticket))
		case 2: // Worker response, error propagation
			resp := v.Interface().(*jobResult)
			// Check if error
			if !p.res(resp) {
				if p.intent == none {
					p.intent = wantcancel
				}
			}
		case 3: // Worker done
			p.state = done
			p.intent = none
		case 4: // Job sent
			p.q.Remove(elem)
			continue
		}
	}
}

// resolveWorker manages the amount of running workers by either starting or stopping workers
// based on the difference in worker state.
func (p *Pool) resolveWorker() {
	c, t := p.mgr.workers()
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
	defer p.mgr.setWorker()()
	// Acknowledge start
	close(started)
	for {
		select {
		case req, ok := <-p.wQ:
			// Worker queue has been closed
			if !ok {
				return
			}

			js := p.mgr.trackReq(req, Executing)

			// Acknowledge ticket
			if req.Ack != nil {
				req.Ack <- nil
			}

			if p.Hook.Start != nil {
				p.Hook.Start(js)
			}

			// Cancel wrapper
			d := make(chan struct{})
			var cancelled bool
			go func() {
				select {
				// Job done
				case <-d:
					return
				// Worker cancel signal received
				case <-p.wC:
					cancelled = true
					req.Job.Cancel()
				}
			}()

			s := time.Now()

			// Execute Job
			o, e := req.Job.Run()

			// Close cancel wrapping
			close(d)

			if e != nil {
				e = newPoolError(&req, e)
			} else if cancelled {
				e = ErrCancelled
			}
			j := &jobResult{
				Job:      req.Job,
				ID:       req.ID,
				Duration: time.Since(s),
				Error:    e,
				Output:   o,
			}
			if e != nil {
				js = p.mgr.trackRes(j, Failed)
			} else {
				js = p.mgr.trackRes(j, Finished)
			}

			// Send Job result to return queue
			p.wR <- j

			if p.Hook.Stop != nil {
				p.Hook.Stop(js)
			}

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
