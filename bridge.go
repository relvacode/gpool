package gpool

// Run is a shortcut for the most common way of executing a job.
// Run the job with the status context and set the error of the status to the result of run.
func Run(j *JobStatus) {
	j.Error = j.Job().Run(j.Context())
}

// A Bridge is an interface which mediates execution of jobs.
type Bridge interface {
	// Request should return a channel that the pool will listen on.
	// The job sent on the return channel may be nil in which case nothing should happen.
	Request() <-chan chan<- *JobStatus
	// Response should return a channel that will send responses from the result of executing a job.
	Return() <-chan *JobStatus
	// Close should trigger exit of all workers, waiting until all running jobs are finished.
	// When everything has exited then it should close the channel returned by the initial call to close.
	Close() <-chan struct{}
}

func NewStaticBridge(N int) *StaticBridge {
	br := &StaticBridge{
		n:    N,
		req:  make(chan chan<- *JobStatus),
		resp: make(chan *JobStatus),
		done: make(chan struct{}),
		exit: make(chan struct{}),
	}
	br.start()
	return br
}

// A StaticBridge is a bridge with a set amount of concurrent workers.
type StaticBridge struct {
	n int

	req  chan chan<- *JobStatus
	resp chan *JobStatus

	done chan struct{}
	exit chan struct{}
}

// Request returns the channel that the pool should listen on.
func (br *StaticBridge) Request() <-chan chan<- *JobStatus {
	return br.req
}

// Return returns the channel that the bridge should send responses on.
func (br *StaticBridge) Return() <-chan *JobStatus {
	return br.resp
}

// Close will stop all workers.
func (br *StaticBridge) Close() <-chan struct{} {
	ack := make(chan struct{})
	go func() {
		close(br.done)
		for i := 0; i < br.n; i++ {
			<-br.exit
		}
		close(ack)
	}()
	return ack
}

func (br *StaticBridge) work() {
	ret := make(chan *JobStatus)
	for {
		select {
		case <-br.done:
			br.exit <- struct{}{}
		case br.req <- ret:
			j := <-ret
			if j == nil {
				continue
			}
			Run(j)
			br.resp <- j
		}
	}
}

func (br *StaticBridge) start() {
	for i := 0; i < br.n; i++ {
		go br.work()
	}
}

//type DynamicBridge struct {
//	mtx *sync.Mutex
//
//	wk []chan struct{}
//
//	req chan chan<- *JobStatus
//	resp chan *JobStatus
//
//	done chan struct{}
//	exit chan struct{}
//}
//
//func (br *DynamicBridge) Add(N int) {
//	br.mtx.Lock()
//	defer br.mtx.Unlock()
//	for i := 0; i < N; i ++ {
//		exit := make(chan struct{})
//		br.wk = append(br.wk, exit)
//		go br.work(exit)
//	}
//}
//
//func (br *DynamicBridge) Remove(N int) {
//	br.mtx.Lock()
//	defer br.mtx.Unlock()
//	if len(br.wk) - N < 1 {
//		return
//	}
//	for i := 0; i < N; i ++ {
//		var exit chan struct{}
//		exit, br.wk = br.wk[0], br.wk[1:]
//		close(exit)
//	}
//}
//
//func (br *DynamicBridge) Close() <-chan struct{} {
//	ret := make(chan struct{})
//
//	return ret
//}
//
//func (br *DynamicBridge) work(exit chan struct{}) {
//	ret := make(chan *JobStatus)
//	for {
//		select {
//		case <-exit:
//			br.exit <- struct {}{}
//		case br.req <- ret:
//			j := <-ret
//			if j == nil {
//				continue
//			}
//			j.Error = j.Job().Run(j.Context())
//			br.resp <- j
//		}
//	}
//}