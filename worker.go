package gpool

type signal int

const (
	sigterm = signal(1)
	sigkill = signal(2)
)

// workRequestPacket is used to signal to the pool that a worker is ready to receive another job
type workRequestPacket struct {
	Worker   int
	Response chan *JobState
}

func adjust(length int, adj int) int {
	switch true {
	case adj < 0:
		return length - adj
	case adj > 0:
		return length + adj
	default:
		return length
	}
}

func (p *pool) resolveWorkers(target int) {
	length := len(p.workers)
	if target == len(p.workers) {
		return
		// if we have too many workers then kill some off
	} else if target < length {
		var delta, i int = length - target, 0
		for id, w := range p.workers {
			w.Signal <- sigterm
			delete(p.workers, id)
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
			id := p.wkID
			w := newWorker(id, p.wIN, p.wOUT, p.wEXIT)
			go w.Work()
			p.workers[id] = w
		}
	}
}

func newWorker(ID int, in chan *workRequestPacket, out chan *JobState, done chan int) *worker {
	return &worker{
		ID:     ID,
		Signal: make(chan signal, 2),
		done:   done,
		rIN:    in,
		out:    out,
	}
}

type worker struct {
	ID     int
	Signal chan signal

	rIN  chan *workRequestPacket
	out  chan *JobState
	done chan int
}

// Work starts listening on the worker input channel and executes jobs until the input channel is closed
// or a cancellation signal is received.
func (wk *worker) Work() {
	// Signal this worker is done on exit
	defer func() {
		wk.done <- wk.ID
	}()

	req := &workRequestPacket{
		Worker:   wk.ID,
		Response: make(chan *JobState),
	}

cycle:
	for {
		select {
		case <-wk.Signal:
			break cycle
		// Work request successfully sent
		case wk.rIN <- req:
			// Pull the return message
			s := <-req.Response
			// If state is nil then we didn't get a valid state
			if s == nil {
				continue cycle
			}

			// Create a work context for this job using the Job request ID
			cancel := make(chan bool, 1)
			result := make(chan error, 1)
			ctx := &WorkContext{
				WorkID: s.ID,
				Cancel: cancel,
			}

			// Begin executing the job
			go func() {
				result <- s.Job().Run(ctx)
			}()

			select {
			case sig := <-wk.Signal:
				if sig == sigkill {
					close(cancel)
				}
				s.Error = <-result
				wk.out <- s
				break cycle
			case s.Error = <-result:
				wk.out <- s
			}
		}
	}
}
