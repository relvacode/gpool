package gpool

import (
	"context"
	"reflect"
	"time"
)

const (
	intentNone int = iota
	intentClose
	intentKill
)

func newCancellationContext(ctx context.Context) *cancellationContext {
	c, x := context.WithCancel(ctx)
	return &cancellationContext{
		ctx:    c,
		cancel: x,
	}
}

type cancellationContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newPool(target int, propagate bool, scheduler Scheduler) *pool {
	if scheduler == nil {
		scheduler = DefaultScheduler
	}
	p := &pool{
		actWorkers: make(map[int]*worker),
		opIN:       make(chan operation),
		wIN:        make(chan *workRequestPacket),
		wOUT:       make(chan *JobStatus),
		wEXIT:      make(chan int),
		tickets:    make(map[Condition][]operation),
		contexts:   make(map[string]*cancellationContext),
		propagate:  propagate,
		scheduler:  scheduler,
	}
	p.resolveWorkers(target)
	go p.bus()
	return p
}

type pool struct {
	jQ []*JobStatus // Job queue

	actWorkers map[int]*worker
	wkCur      int
	wkID       int

	jcExecuting int
	jcFailed    int
	jcFinished  int

	wIN   chan *workRequestPacket
	wOUT  chan *JobStatus // Return
	wEXIT chan int        // Worker done signal

	opIN chan operation // Frontend ticket requests

	err error

	Hook struct {
		Queue Hook // Job becomes queued
		Start Hook // Job starts
		Stop  Hook // Job stops
	}

	state  PoolState // Current pool state
	intent int       // Pool status intent for next cycle

	// Pending tickets awaiting acknowledgement based on condition
	tickets map[Condition][]operation

	// Currently executing cancellation contexts
	contexts map[string]*cancellationContext

	scheduler Scheduler
	propagate bool // Propagate job errors
}

func (p *pool) putStartState(js *JobStatus) {
	p.jcExecuting++
	js.State = Executing
	t := time.Now()
	js.StartedOn = &t

	qd := t.Sub(*js.QueuedOn)
	js.QueuedDuration = &qd

	if p.Hook.Start != nil {
		p.Hook.Start(js)
	}
	if js.t.Condition() == ConditionJobStart {
		js.t.Acknowledge(p.err)
	}
}

func (p *pool) putStopState(js *JobStatus) {
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

	if js.Error != nil && p.propagate && p.err == nil {
		p.err = js.Error
		if p.intent == intentNone {
			p.intent = intentKill
		}
	}
	if p.Hook.Stop != nil {
		p.Hook.Stop(js)
	}
	if js.t.Condition() == ConditionJobStop {
		js.t.Acknowledge(js.Error)
	}
}

func (p *pool) stat() *PoolStatus {
	var err *string
	if p.err != nil {
		s := p.err.Error()
		err = &s
	}
	return &PoolStatus{
		Error: err,
		Jobs: PoolJobsStatus{
			Executing: p.jcExecuting,
			Failed:    p.jcFailed,
			Finished:  p.jcFinished,
			Queued:    len(p.jQ),
		},
		Workers: PoolWorkersStatus{
			Active:      len(p.actWorkers),
			All:         p.wkCur,
			Terminating: p.wkCur - len(p.actWorkers),
		},

		State: p.state,
	}
}

func (p *pool) resolveWorkers(target int) {
	length := len(p.actWorkers)
	if target == length {
		return
		// if we have too many workers then kill some off
	} else if target < length {
		var delta, i int = length - target, 0
		for id, w := range p.actWorkers {
			w.Signal <- sigterm
			delete(p.actWorkers, id)
			i++
			if i == delta {
				return
			}
		}
		// otherwise start some up
	} else {
		for i := 0; i < target-length; i++ {
			// create a worker
			p.wkID++
			p.wkCur++
			id := p.wkID
			w := newWorker(id, p.wIN, p.wOUT, p.wEXIT)
			go w.Work()
			p.actWorkers[id] = w
		}
	}
}

// clearQ iterates through the pending send queue and clears all requests by acknowledging them with the given error.
func (p *pool) abortQueue(err error) {
	for _, jr := range p.jQ {
		jr.Job().Abort()
		// Do not respond if condition is now (on queue).
		if jr.t.Condition() != ConditionNow {
			jr.t.Acknowledge(err)
		}
	}
	p.jQ = p.jQ[:0]
}

func (p *pool) schedule() (j *JobStatus, next time.Duration) {
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
	cases := []reflect.SelectCase{selectRecv(p.opIN), selectRecv(p.wOUT), selectRecv(p.wEXIT), workInputReceiver}

cycle:
	for {
		// Processes intent from last cycle
		switch p.intent {
		case intentKill:
			if p.state >= Killed {
				p.intent = intentNone
				break
			}
			if p.err == nil {
				p.err = ErrKilled
			}
			if p.state < Closed {
				p.state = Closed
			}
			// Kill all workers
			for idx, w := range p.actWorkers {
				w.Signal <- sigkill
				delete(p.actWorkers, idx)
			}
			p.abortQueue(p.err)

			p.state = Killed
			p.intent = intentNone
		case intentClose:
			if len(p.jQ) > 0 {
				break
			}
			if p.state >= Closed {
				p.intent = intentNone
				break
			}
			// Close remaining workers
			for idx, w := range p.actWorkers {
				w.Signal <- sigterm
				delete(p.actWorkers, idx)
			}
			p.state = Closed
			p.intent = intentNone
		}
		// If pool state is done without any intention
		if p.state == Done && p.intent == intentNone {
			if len(p.jQ) != 0 {
				p.abortQueue(p.err)
			}
			// Resolve pending waits
			if tickets, ok := p.tickets[conditionWaitRelease]; ok && len(tickets) > 0 {
				p.tickets[conditionWaitRelease] = acknowledge(p.err, tickets...)
			}

			// If pending destroys then resolve and exit bus.
			if tickets, ok := p.tickets[conditionDestroyRelease]; ok && len(tickets) > 0 {
				p.tickets[conditionDestroyRelease] = acknowledge(p.err, tickets...)
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
					cases[3] = selectRecv(time.NewTimer(waitFor).C)
				}

			}
		} else {
			cases[3] = nilInputReceiver
		}

		selected, inf, _ := reflect.Select(cases)
		switch selected {
		case 0: // Ticket request
			t := inf.Interface().(operation)
			if err := t.Do(p); err != nil || t.Condition() == ConditionNow {
				t.Acknowledge(err)
			}
		case 1: // Job finished
			j := inf.Interface().(*JobStatus)

			// Cancel and delete executing context
			if cCtx, ok := p.contexts[j.ID]; ok {
				cCtx.cancel()
				delete(p.contexts, j.ID)
			}

			p.putStopState(j)
		case 2: // Worker finished
			id := inf.Interface().(int)
			delete(p.actWorkers, id)
			p.wkCur--
			// If no more workers then stop listening for worker done messages
			if p.wkCur == 0 {
				p.state = Done
				cases[2] = selectRecv(nil)
			}
		case 3: // Worker Job request
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

			// Valid job received from scheduler
			if j != nil {
				cCtx := newCancellationContext(j.t.Context)
				j.t.Context = cCtx.ctx
				p.contexts[j.ID] = cCtx

				p.putStartState(j)

				wkr.Response <- j
				continue
			} else {
				wkr.Response <- nil
			}

		}
	}
}
