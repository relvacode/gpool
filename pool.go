package gpool

import (
	"reflect"
	"time"
)

const (
	// Queued means the Job is in the Pool queue.
	Queued string = "Queued"
	// Executing means the Job is executing on a worker.
	Executing string = "Executing"
	// Failed means the Job failed because the Run() function returned a non-nil error.
	Failed string = "Failed"
	// Finished means the Job completed because the Run() function returned a nil error.
	Finished string = "Finished"
)

const (
	_ int = iota
	wantStop
	wantKill
)

const (
	// OK means Pool has no state, or is running.
	OK int = iota
	// Closed means Pool is closed, no more Job requests may be made
	// but currently executing and queued Jobs will continue to be executed.
	Closed
	// Killed means the Pool has been killed via error propagation or Kill() call.
	Killed
	// Done means the queue is empty and all workers have exited.
	Done
)

func newPool(target int, propagate bool, scheduler Scheduler) *pool {
	if scheduler == nil {
		scheduler = DefaultScheduler
	}
	p := &pool{
		workers:   make(map[int]*worker),
		tIN:       make(chan ticket),
		wIN:       make(chan *workRequestPacket),
		wOUT:      make(chan *JobState),
		wEXIT:     make(chan int),
		propagate: propagate,
		scheduler: scheduler,
	}
	p.resolveWorkers(target)
	go p.bus()
	return p
}

type pool struct {
	jQ []*JobState // Job queue

	workers map[int]*worker
	wkID    int

	jcQueued    int
	jcExecuting int
	jcFailed    int
	jcFinished  int

	wIN   chan *workRequestPacket
	wOUT  chan *JobState // Return
	wEXIT chan int       // Worker done signal

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

	scheduler Scheduler
	propagate bool // Propagate job errors
}

func (p *pool) putQueueState(js *JobState) {
	p.jQ = append(p.jQ, js)
	js.State = Queued
	t := time.Now()
	js.QueuedOn = &t
	p.jcQueued++
	if p.Hook.Queue != nil {
		p.Hook.Queue(js)
	}
}

func (p *pool) putStartState(js *JobState) {
	p.jcQueued--
	p.jcExecuting++
	js.State = Executing
	t := time.Now()
	js.StartedOn = &t

	qd := t.Sub(*js.QueuedOn)
	js.QueuedDuration = &qd

	if p.Hook.Start != nil {
		p.Hook.Start(js)
	}
	if js.t.t == tReqJobStartCallback {
		js.t.r <- nil
	}
}

func (p *pool) putStopState(js *JobState) {
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

	ed := t.Sub(*js.StartedOn)
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

		Workers: len(p.workers),
		State:   p.state,
	}
}

// clearQ iterates through the pending send queue and clears all requests by acknowledging them with the given error.
func (p *pool) abortQueue(err error) {
	for _, jr := range p.jQ {
		jr.Job().Abort()
	}
	p.jQ = p.jQ[:0]
}

func (p *pool) schedule() (j *JobState, next time.Duration) {
	if len(p.jQ) == 0 {
		return
	}
	idx, n, ok := p.scheduler.Evaluate(p.jQ)
	next = n
	if !ok || idx >= len(p.jQ) {
		return
	}

	// shift if first index
	// deleting a arbitrary index is more than 4 times slower than shifting the slice.
	// for most cases the next scheduled job is the first or last index so make a special case for performance.
	if idx == 0 {
		j, p.jQ = p.jQ[0], p.jQ[1:]
		// shift if last index
	} else if len(p.jQ)-1 == idx {
		j, p.jQ = p.jQ[idx], p.jQ[:idx]
		// else delete (slow)
	} else {
		j = p.jQ[idx]
		p.jQ = p.jQ[:idx+copy(p.jQ[idx:], p.jQ[idx+1:])]
	}
	return
}

func selectRecv(v interface{}) reflect.SelectCase {
	return reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(v)}
}

// bus is the central communication bus for the pool.
// All pool inputs are collected here as tickets and then actioned on depending on the ticket type.
// If the ticket is a tReqWait request they are added to the slice of pending tickets and upon pool closure.
// A central bus is used to prevent concurrent access to any property of the pool.
func (p *pool) bus() {
	nextEvaluation := time.Now()

	workInputReceiver := selectRecv(p.wIN)
	nilInputReceiver := selectRecv(nil)
	cases := []reflect.SelectCase{selectRecv(p.tIN), selectRecv(p.wOUT), selectRecv(p.wEXIT), workInputReceiver}

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
			// Kill all workers
			for idx, w := range p.workers {
				w.Signal <- sigkill
				delete(p.workers, idx)
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
			// Close remaining workers
			for idx, w := range p.workers {
				w.Signal <- sigterm
				delete(p.workers, idx)
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
			p.pendWait = acknowledge(p.err, p.pendWait...)

			// If pending destroys then resolve and exit bus.
			if len(p.pendDestroy) > 0 {
				p.pendDestroy = acknowledge(p.err, p.pendDestroy...)
				break cycle
			}
		}

		// Only receive message if we have a Job to send or if pool state is not OK.
		if len(p.jQ) > 0 && p.state == OK {
			if nextEvaluation.IsZero() {
				cases[3] = workInputReceiver
			} else {
				now := time.Now()
				if now.After(nextEvaluation) {
					cases[3] = workInputReceiver
				} else {
					waitFor := nextEvaluation.Sub(now)
					cases[3] = selectRecv(time.NewTimer(waitFor))
				}

			}
		} else {
			cases[3] = nilInputReceiver
		}

		selected, inf, _ := reflect.Select(cases)
		switch selected {
		case 0:
			// Ticket request
			p.processTicketRequest(inf.Interface().(ticket))
		case 1:
			// Worker response, error propagation
			p.putStopState(inf.Interface().(*JobState))
		case 2:
			// Worker done
			delete(p.workers, inf.Interface().(int))
			// If no more workers then stop listening for worker done messages
			if len(p.workers) == 0 {
				p.state = Done
				cases[2] = selectRecv(nil)
			}
		case 3:
			wkr, isRequest := inf.Interface().(*workRequestPacket)
			if !isRequest {
				// Evaluation timer timeout
				nextEvaluation = time.Time{}
				continue
			}
			j, next := p.schedule()
			if next > 0 {
				nextEvaluation = time.Now().Add(next)
			} else if !nextEvaluation.IsZero() {
				nextEvaluation = time.Time{}
			}

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
