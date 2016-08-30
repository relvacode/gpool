package gpool

import "context"

type signal int

const (
	sigterm = signal(1)
	sigkill = signal(2)
)

// workRequestPacket is used to signal to the pool that a worker is ready to receive another job
type workRequestPacket struct {
	Worker   int
	Response chan *JobStatus
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
	length := len(p.actWorkers)
	if target == length {
		return
		// if we have too many workers then kill some off
	} else if target < length {
		var delta, i int = length - target, 0
		for id, w := range p.actWorkers {
			w.Signal <- sigterm
			delete(p.actWorkers, id)
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
			p.wkCur++
			id := p.wkID
			w := newWorker(id, p.wIN, p.wOUT, p.wEXIT)
			go w.Work()
			p.actWorkers[id] = w
		}
	}
}

func newWorker(ID int, in chan *workRequestPacket, out chan *JobStatus, done chan int) *worker {
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
	out  chan *JobStatus
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
		Response: make(chan *JobStatus),
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

			ctx, cancel := context.WithCancel(s.Context())
			result := make(chan error, 1)

			// Begin executing the job
			go func() {
				result <- s.Job().Run(ctx)
			}()

			select {
			case sig := <-wk.Signal:
				if sig == sigkill {
					cancel()
				}
				s.Error = <-result
				wk.out <- s
				cancel()
				break cycle
			case s.Error = <-result:
				wk.out <- s
				cancel()
			}
		}
	}
}
