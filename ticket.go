package gpool

// ticket request types
type tReq int

const (
	_ tReq = iota
	tReqJobStartCallback
	tReqJobStopCallback
	tReqJobQueueCallback
	tReqClose
	tReqKill
	tReqWait
	tReqGrow
	tReqShrink
	tReqHealthy
	tReqResize
	tReqGetError
	tReqDestroy
	tReqState
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
