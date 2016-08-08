package gpool

import "time"

// State is a representation of a Job state in the Pool.
type State struct {
	j Job
	t ticket

	Header string
	ID     string

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
