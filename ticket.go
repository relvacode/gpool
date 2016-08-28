package gpool

import (
	"fmt"
	"github.com/pkg/errors"
)

type tReq int

const (
	_ tReq = iota
	tReqJobStartCallback
	tReqJobStopCallback
	tReqJobQueue
	tReqBatchJobQueue
	tReqClose
	tReqKill
	tReqWait
	tReqGrow
	tReqShrink
	tReqResize
	tReqGetError
	tReqDestroy
	tReqStat
)

type returnPayload struct {
	data interface{}
}

func (rt *returnPayload) Error() string {
	return ""
}

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

func (p *pool) processTicketRequest(t ticket) {
	switch t.t {
	// New Job request
	case tReqJobStartCallback, tReqJobStopCallback, tReqJobQueue:
		if p.state > OK {
			t.r <- ErrClosedPool
			return
		}
		u := uuid()
		if u == "" {
			t.r <- fmt.Errorf("unable to generate uuid")
			return
		}

		p.putQueueState(&JobState{ID: u, j: t.data.(Job), t: t})
		if t.t == tReqJobQueue {
			t.r <- nil
		}
	case tReqBatchJobQueue:
		if p.state > OK {
			t.r <- ErrClosedPool
			return
		}
		jobs := t.data.([]Job)
		for _, j := range jobs {
			u := uuid()
			if u == "" {
				t.r <- errors.New("unable to generate uuid")
				return
			}
			p.putQueueState(&JobState{ID: u, j: j, t: t})
		}
		t.r <- nil
	// Pool close request
	case tReqClose:
		if p.state >= Closed {
			t.r <- ErrClosedPool
			return
		}
		if p.state == OK && p.intent < wantStop {
			p.intent = wantStop
		}
		t.r <- nil
		return
	// Pool kill request
	case tReqKill:
		if p.state > Closed {
			t.r <- ErrKilled
			return
		}
		if p.intent < wantKill {
			p.intent = wantKill
		}
		t.r <- nil
		return
	// Pool wait request
	case tReqWait:
		// If done resolve ticket instantly
		if p.state == Done {
			acknowledge(p.err, t)
			return
		}
		p.pendWait = append(p.pendWait, t)
	case tReqGrow, tReqShrink, tReqResize:
		if p.state != OK {
			t.r <- ErrClosedPool
			return
		}

		target := t.data.(int)
		if t.t == tReqGrow {
			target = adjust(p.wkCur, target)
		}
		if t.t == tReqShrink {
			target = adjust(p.wkCur, -target)
		}
		if target > 0 {
			p.resolveWorkers(target)
			t.r <- nil
			return
		}
		t.r <- ErrWorkerCount
	case tReqGetError:
		t.r <- p.err
	case tReqDestroy:
		if p.state == OK && p.intent == OK {
			p.intent = wantStop
		}
		p.pendDestroy = append(p.pendDestroy, t)
	case tReqStat:
		t.r <- &returnPayload{data: p.stat()}
	default:
		panic(fmt.Sprintf("unexpected ticket type %d", t.t))
	}
}

// resolves resolves all remaining tickets by sending them the ctx error.
// any unresolved tickets are returned, currently unused.
func acknowledge(ctx error, tickets ...ticket) []ticket {
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
