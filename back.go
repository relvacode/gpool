package gpool

import (
	"container/list"
	"fmt"
	"reflect"
	"sync"
	"time"
)

const (
	Queued    string = "Queued"
	Executing string = "Executing"
	Failed    string = "Failed"
	Finished  string = "Finished"
)

func newPool(target int, propagate bool) *pool {
	return &pool{
		jobs:      make(map[string]*JobState),
		workers:   make(map[int]*worker),
		wkTgt:     target,
		wg:        &sync.WaitGroup{},
		tIN:       make(chan ticket),
		jIN:       make(chan *jobRequest),
		jOUT:      make(chan *jobResult),
		wD:        make(chan int),
		jQ:        list.New(),
		propagate: propagate,
	}
}

type pool struct {
	jobs    map[string]*JobState
	workers map[int]*worker
	jQ      *list.List // Job queue

	wkID int

	wkTgt int // Target workers
	wkCur int // Current workers

	jcQueued    int
	jcExecuting int
	jcFailed    int
	jcFinished  int

	jIN  chan *jobRequest
	jOUT chan *jobResult // Return
	wD   chan int        // Worker done signal

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

	wg *sync.WaitGroup

	propagate bool // Propagate job errors
}

//type dict map[string]interface{}
//
//func (s *pool) State() map[string]interface{} {
//	s.mtx.RLock()
//	defer s.mtx.RUnlock()
//	return map[string]interface{}{
//		"Workers": dict{
//			"Running": s.wcCurrent,
//			"Target": s.wcTarget,
//			"Stable": s.wcCurrent == len(s.ctx),
//		},
//		"Jobs": dict{
//			"Queued": s.jcQueued,
//			"Executing": s.jcExecuting,
//			"Finished": s.jcFinished,
//			"Failed": s.jcFailed,
//			"Total": len(s.gs),
//		},
//	}
//}

//func (s *pool) Jobs() []JobState {
//	s.mtx.RLock()
//	defer s.mtx.RUnlock()
//	r := []JobState{}
//	for _, j := range s.gs {
//		r = append(r, *j)
//	}
//	return r
//}
//
//// ID gets a Job by the specified ID.
//func (s *pool) ID(i string) (JobState, bool) {
//	s.mtx.RLock()
//	defer s.mtx.RUnlock()
//	j, ok := s.gs[i]
//	if !ok {
//		return JobState{}, false
//	}
//	return *j, true
//}

// start starts Pool workers and Pool bus
func (p *pool) start() {
	p.resolveWorkers()
	go func() {
		p.wg.Wait()
		close(p.wD)
	}()
	go p.bus()
}

func (p *pool) jobQuery(fn jqMatcher) *returnPayload {
	r := []JobState{}
	for _, j := range p.jobs {
		if fn(j) {
			r = append(r, *j)
		}
	}
	return &returnPayload{data: r}
}

func (p *pool) putQueueJobRequest(jr *jobRequest) {
	p.jQ.PushBack(jr)

	var js *JobState
	var ok bool
	if js, ok = p.jobs[jr.ID]; !ok {
		js = &JobState{
			j:          jr.Job,
			ID:         jr.ID,
			Identifier: jr.Job.Identifier(),
			State:      Queued,
			Duration: ExecutionTiming{
				Overall:   &Timing{},
				Queued:    &Timing{},
				Executing: &Timing{},
			},
		}
		js.Duration.Overall.Start()
		js.Duration.Queued.Start()
		p.jcQueued++
		p.jobs[jr.ID] = js
	}
	if p.Hook.Queue != nil {
		p.Hook.Queue(*js)
	}
}

func (p *pool) unqueueElement(elem *list.Element) {
	p.jQ.Remove(elem)

	jr := elem.Value.(*jobRequest)

	var js *JobState
	var ok bool
	js, ok = p.jobs[jr.ID]
	if !ok {
		panic("job not tracked!")
	}
	js.State = Executing
	js.Duration.Queued.Stop()
	js.Duration.Executing.Start()
	p.jcQueued--
	p.jcExecuting++

	if p.Hook.Start != nil {
		p.Hook.Start(*js)
	}
}

func (p *pool) putJobResult(jr *jobResult) {
	js, ok := p.jobs[jr.ID]
	if !ok {
		panic("job not tracked!")
	}
	js.Duration.Executing.Stop()
	js.Duration.Overall.Stop()
	js.Output = jr.Output

	if jr.Error != nil {
		js.Error = jr.Error
		js.State = Failed
		p.jcFailed++
	} else {
		js.State = Finished
		p.jcFinished++
	}

	if jr.Error != nil && p.propagate {
		p.err = jr.Error
		if p.intent == none {
			p.intent = wantKill
		}
	}
	if p.Hook.Stop != nil {
		p.Hook.Stop(*js)
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
func (p *pool) resolve(ctx error, tickets ...ticket) []ticket {
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
	killed
	done
)

const (
	_ int = iota
	wantStop
	wantKill
)

func (p *pool) do(t ticket) {
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
		p.putQueueJobRequest(&jobRequest{
			ID:  u,
			Job: t.data.(Job),
			Ack: t.r,
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
		if p.adjWorkerTarget(i) {
			t.r <- nil
		} else {
			t.r <- ErrWorkerCount
		}
	case tReqShrink:
		i := t.data.(int)
		if p.state != none {
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
		if p.state != none {
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
		if p.state != none {
			t.r <- ErrClosedPool
			return
		}
		t.r <- nil
	case tReqGetError:
		t.r <- p.err
	case tReqDestroy:
		if p.intent == none {
			p.intent = wantKill
		}
		p.pendDestroy = append(p.pendDestroy, t)
	// Queries
	case tReqJobQuery:
		fn := t.data.(jqMatcher)
		p := p.jobQuery(fn)
		t.r <- p
	case tReqWorkerQuery:
		t.r <- &returnPayload{data: p.wkCur}
	default:
		panic(fmt.Sprintf("unexpected ticket type %d", t.t))
	}
}

// clearQ iterates through the pending send queue and clears all requests by acknowledging them with the current pool error.
func (p *pool) clearQ(err error) {
	for e := p.jQ.Front(); e != nil; e = e.Next() {
		e.Value.(*jobRequest).Ack <- p.err
		p.jQ.Remove(e)
	}
}

// Abort signals all workers and deletes them
func (p *pool) abort() {
	for i, w := range p.workers {
		w.Close()
		delete(p.workers, i)
	}
}

// bus is the central communication bus for the pool.
// All pool inputs are collected here as tickets and then actioned on depending on the ticket type.
// If the ticket is a tReqWait request they are added to the slice of pending tickets and upon pool closure.
// A central bus is used to prevent concurrent access to any property of the pool.
func (p *pool) bus() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

cycle:
	for {
		// If pool state is done without any intention and the queue is zero length
		if p.state == done && p.intent == none {
			if p.jQ.Len() != 0 {
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
		case wantKill:
			if p.state >= killed {
				p.intent = none
				break
			}
			if p.err == nil {
				p.err = ErrKilled
			}
			if p.state < closed {
				close(p.jIN)
				p.state = closed
			}
			p.abort()
			if p.pendCancel != nil {
				p.pendCancel.r <- nil
				p.pendCancel = nil
			}

			if p.pendClose != nil {
				p.pendClose.r <- nil
				p.pendClose = nil
			}
			p.clearQ(p.err)

			p.state = killed
			p.intent = none
		case wantStop:
			if p.jQ.Len() > 0 {
				break
			}
			if p.state >= closed {
				p.intent = none
				break
			}
			close(p.jIN)
			p.state = closed
			if p.pendClose != nil {
				p.pendClose.r <- nil
				p.pendClose = nil
			}
			p.intent = none
		}

		cases := []reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(tick.C)}, // Timeout ticket
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.tIN)},  // Ticket input queue
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.jOUT)}, // Worker return queue
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.wD)},   // Worker done (wg done) queue

		}

		// If a job is ready on the queue then try to send it by adding to reflect select cases.
		elem := p.jQ.Front()
		if elem != nil && p.state == 0 {
			cases = append(cases,
				reflect.SelectCase{
					Dir: reflect.SelectSend, Chan: reflect.ValueOf(p.jIN),
					Send: reflect.ValueOf(elem.Value),
				})
		}

		c, v, ok := reflect.Select(cases)
		switch c {
		case 0: // Tick timeout
			continue
		case 1: // Ticket request
			p.do(v.Interface().(ticket))
		case 2: // Worker response, error propagation
			resp := v.Interface().(*jobResult)
			p.putJobResult(resp)
		case 3: // Worker done
			if !ok {
				p.state = done
				p.intent = none
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
			p.unqueueElement(elem)
			continue
		}
	}
}
