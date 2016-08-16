package gpool

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

const (
	// The Job is currently waiting in the queue
	Queued string = "Queued"
	// The Job is currently executing
	Executing string = "Executing"
	// The Job failed because the Run() function returned an error
	Failed string = "Failed"
	// The Job finished execution without error.
	Finished string = "Finished"
)

const (
	// Pool has no state, or is running
	OK int = iota
	// Pool is closed, no more Job requests may be made
	// but currently executing and queued Jobs are still managed
	Closed
	// Pool has been killed via error propagation or Kill() call
	Killed
	// Pool is done
	// the queue is empty and all workers have exited.
	Done
)

func newPool(target int, propagate bool, scheduler Scheduler) *pool {
	if scheduler == nil {
		scheduler = DefaultScheduler
	}
	return &pool{
		workers:   make(map[int]*worker),
		wkTgt:     target,
		wg:        &sync.WaitGroup{},
		tIN:       make(chan ticket),
		wIN:       make(chan *workRequest),
		wOUT:      make(chan *WorkState),
		wEXIT:     make(chan int),
		propagate: propagate,
		scheduler: scheduler,
	}
}

type pool struct {
	jQ []*WorkState // Job queue

	workers map[int]*worker
	wkID    int
	wkTgt   int // Target workers
	wkCur   int // Current workers

	jcQueued    int
	jcExecuting int
	jcFailed    int
	jcFinished  int

	wIN   chan *workRequest
	wOUT  chan *WorkState // Return
	wEXIT chan int        // Worker done signal

	tIN chan ticket // Frontend ticket requests

	err error

	// See gpool.Hook for documentation.
	Hook struct {
		Queue Hook // Job becomes queued
		Start Hook // Job starts
		Stop  Hook // Job stops
	}

	state  int // Current pool state
	intent int // Pool status intent for next cycle

	pendWait    []ticket
	pendDestroy []ticket
	pendCancel  *ticket

	wg *sync.WaitGroup

	scheduler Scheduler
	propagate bool // Propagate job errors
}

// start starts Pool workers and Pool bus
func (p *pool) start() {
	p.resolveWorkers()
	go func() {
		p.wg.Wait()
		close(p.wEXIT)
	}()
	go p.bus()
}

func (p *pool) putQueueState(js *WorkState) {
	p.jQ = append(p.jQ, js)
	js.State = Queued
	t := time.Now()
	js.QueuedOn = &t
	p.jcQueued++
	if p.Hook.Queue != nil {
		p.Hook.Queue(js)
	}
	if js.t.t == tReqJobQueueCallback {
		js.t.r <- nil
	}
}

func (p *pool) putStartState(js *WorkState) {
	p.jcQueued--
	p.jcExecuting++
	js.State = Executing
	t := time.Now()
	js.StartedOn = &t

	qd := time.Since(*js.QueuedOn)
	js.QueuedDuration = &qd

	if p.Hook.Start != nil {
		p.Hook.Start(js)
	}
	if js.t.t == tReqJobStartCallback {
		js.t.r <- nil
	}
}

func (p *pool) putStopState(js *WorkState) {
	p.scheduler.Unload(js)
	p.jcExecuting--
	if js.Error != nil {
		p.jcFailed++
		js.State = Failed
	} else {
		js.State = Finished
		p.jcFinished++
	}
	t := time.Now()
	js.StoppedOn = &t

	ed := time.Since(*js.StartedOn)
	js.ExecutionDuration = &ed

	if js.Error != nil && p.propagate {
		p.err = js.Error
		if p.intent == OK {
			p.intent = wantKill
		}
	}
	if p.Hook.Stop != nil {
		p.Hook.Stop(js)
	}
	if js.t.t == tReqJobStopCallback {
		js.t.r <- js.Error
	}
}

func (s *pool) setWorkerTarget(Target int) bool {
	if Target < 1 {
		return false
	}
	s.wkTgt = Target
	s.resolveWorkers()
	return true
}

func (s *pool) adjWorkerTarget(Delta int) bool {
	if (s.wkTgt + Delta) < 1 {
		return false
	}
	s.wkTgt += Delta
	s.resolveWorkers()
	return true
}

func (p *pool) resolveWorkers() {
	// if we have too many workers then kill some off
	if p.wkTgt < p.wkCur {
		delta := p.wkCur - p.wkTgt
		var i int
		for k, ctx := range p.workers {
			i++
			ctx.Close()
			delete(p.workers, k)
			if i == delta {
				break
			}
		}
		// otherwise start some up
	} else {
		for range make([]int, p.wkTgt-p.wkCur) {
			// create a worker
			p.wkID++
			id := p.wkID
			w := newWorker(id, p.wIN, p.wOUT, p.wEXIT)
			p.wg.Add(1)
			go w.Work()
			p.workers[w.i] = w
			p.wkCur++
		}
	}
}

// resolves resolves all remaining tickets by sending them the ctx error.
// any unresolved tickets are returned, currently unused.
func (p *pool) acknowledge(ctx error, tickets ...ticket) []ticket {
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

func (p *pool) stat() *PoolState {
	var err *string
	if p.err != nil {
		s := p.err.Error()
		err = &s
	}
	return &PoolState{
		Error:     err,
		Executing: p.jcExecuting,
		Failed:    p.jcFailed,
		Finished:  p.jcFinished,
		Queued:    p.jcQueued,

		Workers: p.wkCur,
		State:   p.state,
	}
}

const (
	_ int = iota
	wantStop
	wantKill
)

func (p *pool) processTicketRequest(t ticket) {
	switch t.t {
	// New Job request
	case tReqJobStartCallback, tReqJobStopCallback, tReqJobQueueCallback:
		if p.state > 0 {
			t.r <- ErrClosedPool
			return
		}
		u := uuid()
		if u == "" {
			t.r <- fmt.Errorf("unable to generate uuid")
			return
		}

		p.putQueueState(&WorkState{
			ID: u,
			j:  t.data.(Job),
			t:  t,
		})
	// Pool close request
	case tReqClose:
		if p.state == OK && p.intent < wantStop {
			p.intent = wantStop
		}
		t.r <- nil
		return
	// Pool kill request
	case tReqKill:
		if p.state > wantStop {
			t.r <- nil
			return
		}
		if p.intent < wantKill {
			p.intent = wantKill
			p.pendCancel = &t
			return
		}
	// Pool wait request
	case tReqWait:
		// If done resolve ticket instantly
		if p.state == Done {
			p.acknowledge(p.err, t)
			return
		}
		p.pendWait = append(p.pendWait, t)
	case tReqGrow:
		i := t.data.(int)
		if p.state != OK {
			t.r <- ErrClosedPool
			return
		}
		if p.adjWorkerTarget(i) {
			t.r <- nil
		} else {
			t.r <- ErrWorkerCount
		}
	case tReqShrink:
		i := t.data.(int)
		if p.state != OK {
			t.r <- ErrClosedPool
			return
		}
		if p.adjWorkerTarget(-i) {
			t.r <- nil
		} else {
			t.r <- ErrWorkerCount
		}
	case tReqResize:
		i := t.data.(int)
		if p.state != OK {
			t.r <- ErrClosedPool
			return
		}
		if p.setWorkerTarget(i) {
			t.r <- nil
		} else {
			t.r <- ErrWorkerCount
		}
	// Pool is open request
	case tReqHealthy:
		if p.state != OK {
			t.r <- ErrClosedPool
			return
		}
		t.r <- nil
	case tReqGetError:
		t.r <- p.err
	case tReqDestroy:
		if p.intent == OK {
			p.intent = wantKill
		}
		p.pendDestroy = append(p.pendDestroy, t)
	case tReqState:
		t.r <- &returnPayload{data: p.stat()}
	default:
		panic(fmt.Sprintf("unexpected ticket type %d", t.t))
	}
}

// clearQ iterates through the pending send queue and clears all requests by acknowledging them with the given error.
func (p *pool) abortQueue(err error) {
	for _, jr := range p.jQ {
		// Abort job is method is present
		jr.Job().Abort()
		// Clear ticket waiting request
		jr.t.r <- err
	}
	p.jQ = []*WorkState{}
}

// Abort signals all workers and deletes them
func (p *pool) abortWorkers() {
	for i, w := range p.workers {
		w.Close()
		delete(p.workers, i)
	}
}

func (p *pool) schedule() (j *WorkState, next time.Duration) {
	if len(p.jQ) == 0 {
		return
	}
	idx, n, ok := p.scheduler.Evaluate(p.jQ)
	next = n
	if !ok || idx >= len(p.jQ) {
		return
	}
	j = p.jQ[idx]
	// cut index from list
	p.jQ = append(p.jQ[:idx], p.jQ[idx+1:]...)
	return
}

// bus is the central communication bus for the pool.
// All pool inputs are collected here as tickets and then actioned on depending on the ticket type.
// If the ticket is a tReqWait request they are added to the slice of pending tickets and upon pool closure.
// A central bus is used to prevent concurrent access to any property of the pool.
func (p *pool) bus() {
	now := time.Now()
	nextEvaluation := now

cycle:
	for {
		// Processes intent from last cycle
		switch p.intent {
		case wantKill:
			if p.state >= Killed {
				p.intent = OK
				break
			}
			if p.err == nil {
				p.err = ErrKilled
			}
			if p.state < Closed {
				p.state = Closed
			}
			p.abortWorkers()
			if p.pendCancel != nil {
				p.pendCancel.r <- nil
				p.pendCancel = nil
			}
			p.abortQueue(p.err)

			p.state = Killed
			p.intent = OK
		case wantStop:
			if len(p.jQ) > 0 {
				break
			}
			if p.state >= Closed {
				p.intent = OK
				break
			}
			p.state = Closed
			p.intent = OK
		}
		// If pool state is done without any intention and the queue is zero length
		if p.state == Done && p.intent == OK {
			if len(p.jQ) != 0 {
				p.abortQueue(p.err)
			}
			// Resolve pending waits
			p.pendWait = p.acknowledge(p.err, p.pendWait...)

			// If pending destroys then resolve and exit bus.
			if len(p.pendDestroy) > 0 {
				p.pendDestroy = p.acknowledge(p.err, p.pendDestroy...)
				break cycle
			}
		}

		cases := []reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.tIN)},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.wOUT)},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.wEXIT)},
		}

		// Only receive message if we have a Job to send or if pool state is not OK.
		if len(p.jQ) > 0 || p.state > OK {
			now = time.Now()
			if now.After(nextEvaluation) {
				cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.wIN)})
			} else {
				waitFor := nextEvaluation.Sub(now)
				cases = append(cases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(time.NewTimer(waitFor).C),
				})
			}
		}

		selected, inf, ok := reflect.Select(cases)
		switch selected {
		case 0:
			// Ticket request
			t := inf.Interface().(ticket)
			p.processTicketRequest(t)
		case 1:
			// Worker response, error propagation
			s := inf.Interface().(*WorkState)
			p.putStopState(s)
		case 2:
			// Worker done
			i := inf.Interface().(int)
			if !ok {
				// Pool done as no more workers active
				p.state = Done
				p.intent = OK
				p.wEXIT = make(chan int)
				continue
			}
			if wk, ok := p.workers[i]; ok {
				wk.Close()
				delete(p.workers, i)
			}
			p.wg.Done()
			p.wkCur--
		case 3:
			wkr, isRequest := inf.Interface().(*workRequest)
			if !isRequest {
				// Evaluation timer timeout
				continue
			}

			// If not OK state then close the worker
			if p.state > OK {
				close(wkr.Response)
				continue
			}
			j, next := p.schedule()
			nextEvaluation = time.Now().Add(next)
			if j != nil {
				wkr.Response <- j
				p.putStartState(j)
				continue
			} else {
				wkr.Response <- nil
			}

		}
	}
}
