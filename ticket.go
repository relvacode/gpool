package gpool

// ticket request types
type tReq int

const (
	_               tReq = iota
	tReqJob              // 1
	tReqClose            // 2
	tReqKill             // 3
	tReqWait             // 4
	tReqGrow             // 5
	tReqShrink           // 6
	tReqHealthy          // 7
	tReqResize           // 8
	tReqGetError         // 9
	tReqJobQuery         // 10
	tReqWorkerQuery      // 11
	tReqDestroy          // 12
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
