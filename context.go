package gpool

// A WorkContext is the context given to a Job's Run() function.
// It contains the unique Job ID for this work and a Cancel channel.
// The Cancel channel is closed if a request to Pool.Kill is made or if propagation is enabled and another Job fails.
type WorkContext struct {
	// WorkID is a uuid of this execution.
	WorkID string
	// Cancel is a buffered channel that is closed if a cancellation is requested by the Pool.
	Cancel <-chan bool
}
