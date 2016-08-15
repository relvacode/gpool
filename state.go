package gpool

import "time"

// PoolState is a snapshot of the state of a Pool.
type PoolState struct {
	// Error is the string representation of the error present in the pool.
	// May be nil if there is no error.
	Error *string

	// Jobs
	Executing int
	Failed    int
	Finished  int
	Queued    int

	// Number of active workers
	Workers int

	// Pool status
	State int
}

// WorkState is a representation of a Job state in the Pool.
type WorkState struct {
	j Job
	t ticket

	ID string

	State string
	Error error

	QueuedOn  *time.Time
	StartedOn *time.Time
	StoppedOn *time.Time
}

// Job returns the actual Job attached to this State.
func (s *WorkState) Job() Job {
	return s.j
}
