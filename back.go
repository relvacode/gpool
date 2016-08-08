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
	None int = iota
	// Pool is closed, no more Job requests may be made
	// but currently executing and queued Jobs are still managed
	Closed
	// Pool has been killed via error propagation or Kill() call
	Killed
	// Pool is done
	// the queue is empty and all workers have exited.
	Done
)

func newPool(target int, propagate bool, rules ...ScheduleRule) *pool {
	return &pool{
		workers:   make(map[int]*worker),
		wkTgt:     target,
		wg:        &sync.WaitGroup{},
		tIN:       make(chan ticket),
		jIN:       make(chan *State),
		jOUT:      make(chan *State),
		wD:        make(chan int),
		propagate: propagate,
		scheduler: scheduler(rules),
	}
}

type pool struct {
	workers map[int]*worker
	jQ      []*State // Job queue

	wkID int

	wkTgt int // Target workers
	wkCur int // Current workers

	jcQueued    int
	jcExecuting int
	jcFailed    int
	jcFinished  int

	jIN  chan *State
	jOUT chan *State // Return
	wD   chan int    // Worker done signal

	tIN chan ticket // Frontend ticket requests

	err error

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

	scheduler scheduler

	wg *sync.WaitGroup

	propagate bool // Propagate job errors
}

// start starts Pool workers and Pool bus
func (p *pool) start() {
	p.resolveWorkers()
	go func() {
		p.wg.Wait()
		close(p.wD)
	}()
	go p.bus()
}

func (p *pool) putQueueState(js *State) {
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

func (p *pool) putStartState(js *State) {
	p.jcQueued--
	p.jcExecuting++
	js.State = Executing
	t := time.Now()
	js.StartedOn = &t
	if p.Hook.Start != nil {
		p.Hook.Start(js)
	}
	if js.t.t == tReqJobStartCallback {
		js.t.r <- nil
	}
}

func (p *pool) putStopState(js *State) {
	p.scheduler.offload(js.Job())
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
	if js.Error != nil && p.propagate {
		p.err = js.Error
		if p.intent == None {
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
			w := newWorker(id, p.jIN, p.jOUT, p.wD)
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

type PoolState struct {
	Error error

	Executing int
	Failed    int
	Finished  int
	Queued    int

	Workers int
	State   int
}

func (p *pool) stat() *PoolState {
	return &PoolState{
		Error:     p.err,
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

		p.putQueueState(&State{
			ID: u,
			j:  t.data.(Job),
			t:  t,
		})
	// Pool close request
	case tReqClose:
		if p.state > 0 {
			t.r <- nil
			return
		}
		if p.intent < wantStop {
			p.intent = wantStop
			p.pendClose = &t
			return
		}
		t.r <- nil
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
		if p.state != None {
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
		if p.state != None {
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
		if p.state != None {
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
		if p.state != None {
			t.r <- ErrClosedPool
			return
		}
		t.r <- nil
	case tReqGetError:
		t.r <- p.err
	case tReqDestroy:
		if p.intent == None {
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
	p.jQ = []*State{}
}

// Abort signals all workers and deletes them
func (p *pool) abortWorkers() {
	for i, w := range p.workers {
		w.Close()
		delete(p.workers, i)
	}
}

func (p *pool) getNextJob() *State {
	for idx, jr := range p.jQ {
		if err := p.scheduler.evaluate(jr.Job()); err != nil {
			continue
		}
		p.jQ = append(p.jQ[:idx], p.jQ[idx+1:]...)
		return jr
	}
	return nil
}

func (p *pool) processPoolIntent() {
	switch p.intent {
	case wantKill:
		if p.state >= Killed {
			p.intent = None
			return
		}
		if p.err == nil {
			p.err = ErrKilled
		}
		if p.state < Closed {
			close(p.jIN)
			p.state = Closed
		}
		p.abortWorkers()
		if p.pendCancel != nil {
			p.pendCancel.r <- nil
			p.pendCancel = nil
		}

		if p.pendClose != nil {
			p.pendClose.r <- nil
			p.pendClose = nil
		}
		p.abortQueue(p.err)

		p.state = Killed
		p.intent = None
	case wantStop:
		if len(p.jQ) > 0 {
			return
		}
		if p.state >= Closed {
			p.intent = None
			return
		}
		close(p.jIN)
		p.state = Closed
		if p.pendClose != nil {
			p.pendClose.r <- nil
			p.pendClose = nil
		}
		p.intent = None
	}
}

// bus is the central communication bus for the pool.
// All pool inputs are collected here as tickets and then actioned on depending on the ticket type.
// If the ticket is a tReqWait request they are added to the slice of pending tickets and upon pool closure.
// A central bus is used to prevent concurrent access to any property of the pool.
func (p *pool) bus() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	busMsg := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(tick.C)}, // Timeout ticket
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.tIN)},  // Ticket input queue
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.jOUT)}, // Worker return queue
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.wD)},   // Worker done (wg done) queue
	}

cycle:
	for {
		p.processPoolIntent()
		// If pool state is done without any intention and the queue is zero length
		if p.state == Done && p.intent == None {
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

		cases := busMsg

		var jr *State
		// If a job is ready on the queue then try to send it by adding to reflect select cases.
		if p.state == 0 && len(p.jQ) > 0 {
			// Pop the requests from the job queue
			jr = p.getNextJob()
			if jr != nil {
				cases = append(cases,
					reflect.SelectCase{
						Dir: reflect.SelectSend, Chan: reflect.ValueOf(p.jIN),
						Send: reflect.ValueOf(jr),
					})
			}
		}

		putBack := func(j *State) {
			if j != nil {
				p.jQ = append([]*State{j}, p.jQ...)
			}
		}

		c, v, ok := reflect.Select(cases)
		switch c {
		case 0: // Tick timeout
			putBack(jr)
			continue
		case 1: // Ticket request
			putBack(jr)
			p.processTicketRequest(v.Interface().(ticket))
		case 2: // Worker response, error propagation
			putBack(jr)
			p.putStopState(v.Interface().(*State))
		case 3: // Worker done
			putBack(jr)
			if !ok {
				// Pool done as no more workers active
				p.state = Done
				p.intent = None
				p.wD = make(chan int)
				continue
			}
			i := v.Interface().(int)
			if wk, ok := p.workers[i]; ok {
				wk.Close()
				delete(p.workers, i)
			}
			p.wg.Done()
			p.wkCur--
		case 4: // Job sent
			p.putStartState(jr)
			continue
		}
	}
}
