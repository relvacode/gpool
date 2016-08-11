package gpool

import "time"

// PoolState is a snapshot of the state of a Pool.
type PoolState struct {
	Error error

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

// State is a representation of a Job state in the Pool.
type State struct {
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
func (s *State) Job() Job {
	return s.j
}
