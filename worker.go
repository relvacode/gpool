package gpool

func newWorker(ID int, in chan *jobRequest, out chan *jobResult, done chan int) *worker {
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
	in   chan *jobRequest
	out  chan *jobResult
	done chan int
}

func (ctx *worker) Close() {
	close(ctx.c)
}

func (ctx *worker) active() bool {
	select {
	case _, ok := <-ctx.c:
		return !ok
	default:
		return false
	}
}

// worker is the Pool worker routine that receives jobRequests from the pool worker queue until the queue is closed.
// It executes the job and returns the result to the worker return channel as a JobResult.
func (ctx *worker) Work() {
	// Signal this worker is done on exit
	defer func() {
		ctx.done <- ctx.i
	}()
	for {
		if ctx.active() {
			return
		}
		select {
		case req, ok := <-ctx.in:
			// Worker queue has been closed
			if !ok {
				return
			}
			// Acknowledge ticket
			if req.Ack != nil {
				req.Ack <- nil
			}

			// Cancel wrapper
			d := make(chan struct{})
			var cancelled bool
			go func() {
				select {
				// Job done
				case <-d:
					return
				// Worker cancel signal received
				case <-ctx.c:
					cancelled = true
					req.Job.Cancel()
				}
			}()

			// Execute Job
			o, e := req.Job.Run()

			// Close cancel wrapping
			close(d)

			if e != nil {
				e = newPoolError(req, e)
			} else if cancelled {
				e = ErrCancelled
			}
			j := &jobResult{
				Job:    req.Job,
				ID:     req.ID,
				Error:  e,
				Output: o,
			}

			// Send Job result to return queue
			ctx.out <- j
		case <-ctx.c:
			return
		}
	}
}
