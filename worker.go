package gpool

// workRequest is used to signal to the pool that a worker is ready to receive another job
type workRequest struct {
	Worker   int
	Response chan *WorkState
}

func newWorker(ID int, in chan *workRequest, out chan *WorkState, done chan int) *worker {
	return &worker{
		i:    ID,
		c:    make(chan bool),
		done: done,
		rIN:  in,
		out:  out,
		req: &workRequest{
			Worker:   ID,
			Response: make(chan *WorkState),
		},
	}
}

type worker struct {
	i    int
	c    chan bool
	rIN  chan *workRequest
	out  chan *WorkState
	done chan int

	req *workRequest
}

func (wk *worker) Close() {
	close(wk.c)
}

func (wk *worker) active() bool {
	select {
	case _, ok := <-wk.c:
		return !ok
	default:
		return false
	}
}

// Work starts listening on the worker input channel and executes jobs until the input channel is closed
// or a cancellation signal is received.
func (wk *worker) Work() {
	// Signal this worker is done on exit
	defer func() {
		wk.done <- wk.i
	}()
cycle:
	for {
		if wk.active() {
			return
		}
		select {
		// Work request successfully sent
		case wk.rIN <- wk.req:
			// Pull the return message
			s, ok := <-wk.req.Response
			// If state is nil then we didn't get a valid state
			if !ok {
				return
			}
			if s == nil {
				continue cycle
			}

			// Create a work context for this job using the Job request ID
			cancel := make(chan bool, 1)
			ctx := &WorkContext{
				WorkID: s.ID,
				Cancel: cancel,
			}

			// Route worker cancellation into the work context
			ctxD := route(wk.c, cancel)

			// Execute Job
			s.Error = s.Job().Run(ctx)

			// Close cancel wrapping
			close(ctxD)

			// Send Job result to return queue
			wk.out <- s
		case <-wk.c:
			return
		}
	}
}
