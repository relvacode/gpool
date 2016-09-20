package gpool

import (
	"context"
	"time"
)

// ExecutionState is a string representation of the state of a job in the pool.
type ExecutionState string

// OK returns true if the Execution state can be considered 'OK', i.e not 'Failed'.
func (exc ExecutionState) OK() bool {
	return exc != Failed
}

const (
	// Queued means the job is in the pool queue.
	Queued ExecutionState = "Queued"
	// Executing means the job is executing on a worker.
	Executing ExecutionState = "Executing"
	// Failed means the job failed because the Run() function returned a non-nil error.
	Failed ExecutionState = "Failed"
	// Finished means the job completed because the Run() function returned a nil error.
	Finished ExecutionState = "Finished"
)

// PoolState is a integer representing the state of the pool.
type PoolState int

// String returns the string representation of this state.
func (s PoolState) String() string {
	switch s {
	case OK:
		return "OK"
	case Closed:
		return "Closed"
	case Killed:
		return "Killed"
	case Done:
		return "Done"
	}
	return ""
}

const (
	// OK means the pool has no state, or is running.
	OK PoolState = iota
	// Closed means pool is closed, no more job requests may be made
	// but currently executing and queued jobs will continue to be executed.
	Closed
	// Killed means the pool has been killed via error propagation or Kill() call.
	Killed
	// Done means the queue is empty and all workers have exited.
	Done
)

// PoolJobsStatus is a snapshot of the count of jobs grouped by status in the pool.
type PoolJobsStatus struct {
	Executing int
	Failed    int
	Finished  int
	Queued    int
}

// PoolStatus is a snapshot of the state of a pool.
type PoolStatus struct {
	// Error is the string representation of the error present in the pool.
	// May be nil if there is no error.
	// *string is used because it's JSON friendly compared to marshalling arbitrary error interfaces.
	// Use Pool.Error() to get the real error interface shown here.
	Error *string

	// Jobs
	Jobs PoolJobsStatus

	// Pool state
	State PoolState
}

// JobStatus is a representation of a job's state in the pool.
type JobStatus struct {
	t *opJob

	// ID is the unique ID given to this job when it begins queuing.
	ID string

	// State is the current state of the job in the pool.
	State ExecutionState

	// Error is the error returned from execution (if any).
	Error error

	// Time at which the job changed state.
	// nil if not set yet.
	QueuedOn, StartedOn, StoppedOn *time.Time

	// Duration of either queueing or execution.
	// nil if not set yet.
	QueuedDuration, ExecutionDuration *time.Duration
}

// Job returns the actual job attached to this state.
func (s *JobStatus) Job() Job {
	return s.t.Job
}

// Context returns the execution context attached to this job state.
func (s *JobStatus) Context() context.Context {
	return s.t.Context
}
