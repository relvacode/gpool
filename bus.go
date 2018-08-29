package gpool

import (
	"context"
	"time"
)

const (
	intentNone  int = iota
	intentClose
	intentKill
)

func newBus(propagate bool, bridge Bridge) *bus {
	p := &bus{
		opIN:          make(chan operation),
		tickets:       make(map[Condition][]operation),
		cancellations: make(map[string]context.CancelFunc),
		jE:            make(map[string]*JobStatus),
		propagate:     propagate,
		bridge:        bridge,
	}
	go p.bus()
	return p
}

type bus struct {
	bridge Bridge

	jQ []*JobStatus          // Job queue
	jE map[string]*JobStatus // Job executing

	jcExecuting int
	jcFailed    int
	jcFinished  int

	opIN chan operation // Frontend ticket requests

	err error

	Hook struct {
		Queue Hook // Job becomes queued.
		Start Hook // Job starts.
		Stop  Hook // Job stops or is aborted in queue.
	}
	propagate bool // Propagate job errors

	state  PoolState // Current bus state
	intent int       // Bus intent for next cycle

	// Pending tickets awaiting acknowledgement based on condition
	tickets map[Condition][]operation

	// Currently executing cancellation contexts
	cancellations map[string]context.CancelFunc
}

func (p *bus) putStartState(js *JobStatus) {
	p.jcExecuting++
	js.State = Executing
	t := time.Now()
	js.StartedOn = &t

	qd := t.Sub(*js.QueuedOn)
	js.QueuedDuration = &qd

	p.jE[js.ID] = js

	if p.Hook.Start != nil {
		p.Hook.Start(js)
	}
	if js.t.Condition() == ConditionJobStart {
		js.t.Acknowledge(p.err)
	}
}

func (p *bus) putStopState(js *JobStatus) {
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

	delete(p.jE, js.ID) // delete job from executing jobs

	if p.Hook.Stop != nil {
		p.Hook.Stop(js)
	}
	if js.t.Condition() == ConditionJobStop {
		js.t.Acknowledge(js.Error)
	}
}

func (p *bus) stat() *PoolStatus {
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
		State: p.state,
	}
}

func (p *bus) abortState(err error, js *JobStatus) {
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

func (p *bus) abortQueue(err error) {
	for _, js := range p.jQ {
		p.abortState(err, js)
	}
	p.jQ = p.jQ[:0]
}

func (p *bus) schedule(t *Transaction) (j *JobStatus) {
	if len(p.jQ) == 0 {
		return
	}
	idx, ok := t.Evaluate(p.jQ)
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

func (p *bus) bus() {
	var bridgeBlocked bool
	var bridgeExit <-chan struct{}

	refresh := time.NewTicker(time.Second * 30)
	defer refresh.Stop()

	bridgeRequest, bridgeReturn := p.bridge.Request(), p.bridge.Return()

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
				bridgeExit = p.bridge.Exit()
				p.state = Closed
			}
			// Cancel all running contexts
			for id, c := range p.cancellations {
				c()
				delete(p.cancellations, id)
			}

			p.abortQueue(p.err)

			p.state = Killed
			p.intent = intentNone
		case intentClose:
			if len(p.jQ) > 0 || p.jcExecuting > 0 {
				break
			}
			if p.state >= Closed {
				p.intent = intentNone
				break
			}
			bridgeExit = p.bridge.Exit()

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

		var bridgeInput <-chan *Transaction

		// Only set workInputReceiver if ready to execute another job
		if len(p.jQ) > 0 && p.state == OK && !bridgeBlocked {
			bridgeInput = bridgeRequest
		}

		select {
		case op := <-p.opIN:
			bridgeBlocked = false
			if err := op.Do(p); err != nil || op.Condition() == ConditionNow {
				op.Acknowledge(err)
			}
		case j := <-bridgeReturn:
			bridgeBlocked = false
			// Cancel and delete executing context
			if cancel, ok := p.cancellations[j.ID]; ok {
				cancel()
				delete(p.cancellations, j.ID)
			}
			p.putStopState(j)
		case <-bridgeExit:
			bridgeExit = nil
			p.state = Done
			// Refresh bridge state every 30 seconds to retry bridge requests
		case <-refresh.C:
			bridgeBlocked = false
		case transaction := <-bridgeInput:
			// request for work from worker
			j := p.schedule(transaction)
			if j != nil {
				// Create cancellation context
				j.t.Context, p.cancellations[j.ID] = context.WithCancel(j.t.Context)
				p.putStartState(j)
			} else {
				bridgeBlocked = true
			}
			transaction.Return <- j
		}
	}
}
