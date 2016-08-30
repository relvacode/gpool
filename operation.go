package gpool

import (
	"context"
	"errors"
	"time"
)

// A Condition is a type used to indicate when the response of a pool operation should be acknowledged.
type Condition int

const (
	// ConditionNow is a zero state condition meaning acknowledge now.
	ConditionNow Condition = iota
	// ConditionJobStart is a condition for when a job begins executing.
	ConditionJobStart
	// ConditionJobStop is a condition for a when job stops executing.
	ConditionJobStop

	// conditionWaitRelease is a condition for when wait tickets are resolved.
	conditionWaitRelease
	// conditionDestroyRelease is a condition for when destroy tickets are resolved.
	conditionDestroyRelease
)

// A Request is a pool request to execute a Job in the pool.
type Request struct {
	// Job is the job to be executed.
	Job Job
	// Context may be optionally supplied to provide values to the Job.
	// A context's Done method is only evaluated during execution of the Job.
	Context context.Context
	// CallbackCondition describes when the ticket requesting this Job is acknowledged.
	// Defaults to ConditionNow (callback on Queue).
	CallbackCondition Condition
}

type payload struct {
	data interface{}
}

func (p payload) Error() string { return "" }

// data gets the payload data from the given error if error is a payload.
func data(err error) (interface{}, bool) {
	if p, ok := err.(*payload); ok {
		return p.data, ok
	}
	return nil, false
}

// A operation is a request for an operation on the pool.
// When a operation is called it has exclusive access to the pool.
type operation interface {
	// Do performs and action on the pool, returning any error in execution.
	Do(*pool) error
	// Condition returns what condition the ticket should be responded to (if err is not nil).
	Condition() Condition
	// Acknowledge responds to the caller of the ticket with the supplied error.
	Acknowledge(error)
	// Receive returns a channel for the result of the operation.
	Receive() chan error
}

func newOP() *op {
	return &op{r: make(chan error, 1)}
}

type op struct {
	r chan error
}

func (o *op) Condition() Condition {
	return ConditionNow
}

func (o *op) Acknowledge(err error) {
	o.r <- err
	close(o.r)
}

func (o *op) Receive() chan error {
	return o.r
}

// opJob submits a Request to the pool
type opJob struct {
	*op
	*Request
}

func (t *opJob) Condition() Condition {
	return t.CallbackCondition
}

func (t *opJob) Do(p *pool) error {
	if p.state > OK {
		return ErrClosedPool
	}
	u := uuid()
	if u == "" {
		return errors.New("unable to generate uuid")
	}
	if t.Context == nil {
		t.Context = context.Background()
	}
	t.Context = context.WithValue(t.Context, JobIDKey, u)
	s := &JobStatus{
		ID:    u,
		t:     t,
		State: Queued,
	}
	now := time.Now()
	s.QueuedOn = &now
	p.jQ = append(p.jQ, s)
	if p.Hook.Queue != nil {
		p.Hook.Queue(s)
	}
	return nil
}

// opJobBatch queues one or more jobs at the same time.
type opJobBatch struct {
	*op
	requests []*Request
}

func (t *opJobBatch) Do(p *pool) error {
	if p.state > OK {
		return ErrClosedPool
	}
	for _, r := range t.requests {
		jt := &opJob{Request: r}
		if err := jt.Do(p); err != nil {
			return err
		}
	}
	return nil
}

// opSetIntent sets the intent of the pool for the next cycle.
type opSetIntent struct {
	*op
	intent int
}

func (t *opSetIntent) Do(p *pool) error {
	if p.state > OK {
		return ErrClosedPool
	}
	if p.state == OK && p.intent < t.intent {
		p.intent = t.intent
	}
	return nil
}

// opResize alters the amount of executing workers in the pool
type opResize struct {
	*op
	target, alter int
}

func (t *opResize) Do(p *pool) error {
	if p.state != OK {
		return ErrClosedPool
	}
	if t.alter != 0 {
		t.target = adjust(p.wkCur, t.alter)
	}
	if t.target == 0 {
		return ErrWorkerCount
	}
	p.resolveWorkers(t.target)
	return nil
}

// opAcknowledgeCondition is used to defer acknowledgement until a pool condition is met such as wait or destroy.
type opAcknowledgeCondition struct {
	*op
	when Condition
}

func (t *opAcknowledgeCondition) Condition() Condition {
	return t.when
}

func (t *opAcknowledgeCondition) Do(p *pool) error {
	if t.when == conditionDestroyRelease {
		p.intent = intentClose
	}
	if t.when == conditionWaitRelease && p.state == Done {
		acknowledge(p.err, t)
		return nil
	}
	if tickets, ok := p.tickets[t.when]; ok {
		p.tickets[t.when] = append(tickets, t)
	} else {
		p.tickets[t.when] = []operation{t}
	}
	return nil
}

// opGetError returns the current error loaded in the pool
type opGetError struct {
	*op
}

func (t *opGetError) Do(p *pool) error {
	return p.err
}

// opGetState returns the result of stat() as a payload.
type opGetState struct {
	*op
}

func (t *opGetState) Do(p *pool) error {
	return &payload{data: p.stat()}
}

// resolves resolves all remaining tickets by sending them the ctx error.
// any unresolved tickets are returned, currently unused.
func acknowledge(ctx error, tickets ...operation) []operation {
	if len(tickets) != 0 {
		// Pop every item in tickets and send return signal to ticket
		for {
			if len(tickets) == 0 {
				// No more pending message, continue with next cycle
				break
			}
			var t operation
			t, tickets = tickets[len(tickets)-1], tickets[:len(tickets)-1]
			t.Acknowledge(ctx)
		}
	}
	return tickets[:0]
}
