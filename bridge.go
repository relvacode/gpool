package gpool

import (
	"context"
	"sync"
)

// An Evaluator is a function that is given a slice of the current pool queue.
// The Evaluator select which job in the slice to be executed next within the same transaction.
// The Evaluator should return false if no job should be scheduled at this time.
type Evaluator func([]*JobStatus) (int, bool)

// A Transaction is a request for work to the pool.
// When a worker in a bridge is ready to begin executing a job it will send a message to it's request channel
// with this transaction.
type Transaction struct {
	// Evaluate is called to select the next job in the queue to be executed by this transaction.
	Evaluate Evaluator
	// Return is the channel that the pool will send the selected job to the worker on.
	// The job returned on this channel may be nil if no job could be scheduled at this time.
	Return chan *JobStatus
}

// A Bridge is an interface which controls the execution of jobs on the pool.
type Bridge interface {
	// Request will be called when the pool starts to obtain the channel used to receive request transactions for work.
	Request() <-chan *Transaction
	// Return will be called when the pool starts to obtain the channel for returning the result of
	// job execution back to the pool.
	Return() <-chan *JobStatus
	// Exit will be called when the pool has been requested to be closed.
	// The bridge must complete all running jobs and return their statuses before exiting.
	// The returned channel should be closed once all existing jobs are complete.
	// No additional requests for work are honoured after exit is called.
	Exit() <-chan struct{}
}

// FIFOEvaluator is an evaluator function that always returns the first index of the queue.
func FIFOEvaluator([]*JobStatus) (int, bool) {
	return 0, true
}

// LIFOEvaluator is an evaluator function that always returns the last index of the queue.
func LIFOEvaluator(q []*JobStatus) (int, bool) {
	return len(q) - 1, true
}

// NewSimpleBridge creates a new bridge with a static amount of workers.
// Workers are started when the bridge is created.
func NewSimpleBridge(Workers uint, Evaluator Evaluator) *SimpleBridge {
	ctx, cancel := context.WithCancel(context.Background())
	br := &SimpleBridge{
		Evaluator: Evaluator,
		chRequest: make(chan *Transaction),
		chReturn:  make(chan *JobStatus),
		c:         cancel,
		wg:        &sync.WaitGroup{},
	}
	for i := uint(0); i < Workers; i++ {
		br.wg.Add(1)
		go br.work(ctx)
	}
	return br
}

// SimpleBridge is a static bridge with a preset amount of workers.
type SimpleBridge struct {
	Evaluator Evaluator

	chRequest chan *Transaction
	chReturn  chan *JobStatus

	c  context.CancelFunc
	wg *sync.WaitGroup
}

// Request returns a channel for pool transactions.
func (br *SimpleBridge) Request() <-chan *Transaction {
	return br.chRequest
}

// Return returns a channel for job status returns.
func (br *SimpleBridge) Return() <-chan *JobStatus {
	return br.chReturn
}

// Exit signals the bridge to exit.
func (br *SimpleBridge) Exit() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		br.c()
		br.wg.Wait()
		close(done)
	}()
	return done
}

func (br *SimpleBridge) work(ctx context.Context) {
	defer br.wg.Done()
	tr := &Transaction{
		Evaluate: br.Evaluator,
		Return:   make(chan *JobStatus),
	}
	for {
		select {
		case <-ctx.Done():
			return
		case br.chRequest <- tr:
			j := <-tr.Return
			if j == nil {
				continue
			}
			j.Error = j.Job().Run(j.Context())
			br.chReturn <- j
		}
	}
}
