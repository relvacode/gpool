package gpool

// workRequestPacket is used to signal to the pool that a worker is ready to receive another job
type workRequestPacket struct {
	Worker   int
	Response chan *JobState
}

func newWorker(ID int, in chan *workRequestPacket, out chan *JobState, done chan int) *worker {
	return &worker{
		i:          ID,
		killSignal: make(chan bool),
		dieSignal:  make(chan bool),
		done:       done,
		rIN:        in,
		out:        out,
		req: &workRequestPacket{
			Worker:   ID,
			Response: make(chan *JobState),
		},
	}
}

type worker struct {
	i          int
	killSignal chan bool
	dieSignal  chan bool
	rIN        chan *workRequestPacket
	out        chan *JobState
	done       chan int

	req *workRequestPacket
}

func (wk *worker) Die() {
	close(wk.dieSignal)
}

func (wk *worker) Kill() {
	close(wk.killSignal)
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
		select {
		case <-wk.dieSignal:
			return
		case <-wk.killSignal:
			return
		// Work request successfully sent
		case wk.rIN <- wk.req:
			// Pull the return message
			s := <-wk.req.Response
			// If state is nil then we didn't get a valid state
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
			ctxD := route(wk.killSignal, cancel)

			// Execute Job
			s.Error = s.Job().Run(ctx)

			// Close cancel wrapping
			close(ctxD)

			// Send Job result to return queue
			wk.out <- s
		}
	}
}
