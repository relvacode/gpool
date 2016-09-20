package gpool

import (
	"context"
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
		Queue Hook // Job becomes queued.
		Start Hook // Job starts.
		Stop  Hook // Job stops or is aborted in queue.
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

	p.scheduler.Load(js)

	if p.Hook.Start != nil {
		p.Hook.Start(js)
	}
	if js.t.Condition() == ConditionJobStart {
		js.t.Acknowledge(p.err)
	}
}

func (p *pool) putStopState(js *JobStatus) {
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

	p.scheduler.Unload(js)

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

func (p *pool) abortState(err error, js *JobStatus) {
	if err == nil {
		err = ErrCancelled
	}

	js.Job().Abort(err)

	js.Error = err
	js.State = Failed

	d := time.Since(*js.QueuedOn)
	js.QueuedDuration = &d

	if p.Hook.Stop != nil {
		p.Hook.Stop(js)
	}

	// Do not respond if condition is now (on queue).
	if js.t.Condition() != ConditionNow {
		js.t.Acknowledge(err)
	}
}

// clearQ iterates through the pending send queue and clears all requests by acknowledging them with the given error.
func (p *pool) abortQueue(err error) {
	for _, js := range p.jQ {
		p.abortState(err, js)
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

// bus is the central communication bus for the pool.
// All pool inputs are collected here as tickets and then actioned on depending on the ticket type.
// If the ticket is a tReqWait request they are added to the slice of pending tickets and upon pool closure.
// A central bus is used to prevent concurrent access to any property of the pool.
func (p *pool) bus() {
	// scheduleReady indicates that there is no active evaluation timeout.
	var scheduleReady = true
	var evaluationTimer <-chan time.Time

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

		var workInputReceiver chan *workRequestPacket

		// Only set workInputReceiver if ready to execute another job
		if len(p.jQ) > 0 && p.state == OK && scheduleReady {
			workInputReceiver = p.wIN
		}

		select {
		case op := <-p.opIN:
			if err := op.Do(p); err != nil || op.Condition() == ConditionNow {
				op.Acknowledge(err)
			}
		case j := <-p.wOUT:
			// Cancel and delete executing context
			if cCtx, ok := p.contexts[j.ID]; ok {
				cCtx.cancel()
				delete(p.contexts, j.ID)
			}
			p.putStopState(j)
		case id := <-p.wEXIT:
			delete(p.actWorkers, id)
			p.wkCur--
			if p.wkCur == 0 {
				p.state = Done
			}
		case req := <-workInputReceiver:
			// request for work from worker
			j, next := p.schedule()
			if next > 0 {
				scheduleReady = false
				evaluationTimer = time.NewTimer(next).C
			}

			// Valid job received from scheduler
			if j != nil {
				cCtx := newCancellationContext(j.t.Context)
				j.t.Context = cCtx.ctx
				p.contexts[j.ID] = cCtx

				p.putStartState(j)

				req.Response <- j
				continue
			} else {
				req.Response <- nil
			}
		case <-evaluationTimer:
			// timeout from last evaluation
			evaluationTimer = nil
			scheduleReady = true
		}
	}
}
