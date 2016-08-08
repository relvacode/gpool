package gpool

func newWorker(ID int, in chan *State, out chan *State, done chan int) *worker {
	return &worker{
		i:    ID,
		c:    make(chan bool),
		done: done,
		in:   in,
		out:  out,
	}
}

type worker struct {
	i    int
	c    chan bool
	in   chan *State
	out  chan *State
	done chan int
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

// worker is the Pool worker routine that receives jobRequests from the pool worker queue until the queue is closed.
// It executes the job and returns the result to the worker return channel as a JobResult.
func (wk *worker) Work() {
	// Signal this worker is done on exit
	defer func() {
		wk.done <- wk.i
	}()
	for {
		if wk.active() {
			return
		}
		select {
		case s, ok := <-wk.in:
			// Worker queue has been closed
			if !ok {
				return
			}

			// Create a work context for this job
			cancel := make(chan bool, 1)
			ctx := &WorkContext{
				WorkID: s.ID,
				Cancel: cancel,
			}
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
