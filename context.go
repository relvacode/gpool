package gpool

func newWorkContext(ID string) *WorkContext {
	return &WorkContext{
		WorkID: ID,
		Cancel: make(chan bool, 1),
	}
}

// A WorkContext is the context given to a Job's Run() function.
// It contains the unique Job ID for this work and a Cancel channel.
// The Cancel channel is closed if a request to Pool.Kill is made or if propagation is enabled and another Job fails.
type WorkContext struct {
	// WorkID is a uuid of this execution.
	WorkID string
	// Cancel is a buffered channel that is closed if a cancellation is requested by the Pool.
	Cancel chan bool
}

// route routes a given cancellation channel into the WorkContext.
// The returned channel should be closed when finished.
func (ctx *WorkContext) route(c <-chan bool) chan<- bool {
	d := make(chan bool, 1)
	go func() {
		select {
		case <-d:
			return
		case <-c:
			close(ctx.Cancel)
		}
	}()
	return d
}
