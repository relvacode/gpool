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

// JobState is a representation of a Job state in the Pool.
type JobState struct {
	j Job
	t ticket

	ID string

	State string
	Error error

	QueuedOn  *time.Time
	StartedOn *time.Time
	StoppedOn *time.Time

	QueuedDuration    *time.Duration
	ExecutionDuration *time.Duration
}

// Job returns the actual Job attached to this State.
func (s *JobState) Job() Job {
	return s.j
}
