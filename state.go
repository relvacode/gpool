package gpool

import (
	"context"
	"time"
)

// ExecutionState is a string representation of the state of a Job in the Pool.
type ExecutionState string

// OK returns true if the Execution state can be considered 'OK', i.e not 'Failed'.
func (exc ExecutionState) OK() bool {
	return exc != Failed
}

const (
	// Queued means the Job is in the Pool queue.
	Queued ExecutionState = "Queued"
	// Executing means the Job is executing on a worker.
	Executing ExecutionState = "Executing"
	// Failed means the Job failed because the Run() function returned a non-nil error.
	Failed ExecutionState = "Failed"
	// Finished means the Job completed because the Run() function returned a nil error.
	Finished ExecutionState = "Finished"
)

// PoolState is a integer representing the state of the pool
type PoolState int

// String returns the string representation of this PoolState.
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
	// OK means Pool has no state, or is running.
	OK PoolState = iota
	// Closed means Pool is closed, no more Job requests may be made
	// but currently executing and queued Jobs will continue to be executed.
	Closed
	// Killed means the Pool has been killed via error propagation or Kill() call.
	Killed
	// Done means the queue is empty and all workers have exited.
	Done
)

// PoolJobsStatus is a snapshot of the count of Jobs and their status' in the Pool.
type PoolJobsStatus struct {
	Executing int
	Failed    int
	Finished  int
	Queued    int
}

// PoolWorkersStatus is a snapshot of the count of workers in the Pool.
type PoolWorkersStatus struct {
	// Active is the number of active and valid workers that are executing or ready to execute a Job.
	Active int
	// Terminating is the number of workers waiting to be killed after they completed execution of their currently executing job.
	// It is the difference between All and Active workers.
	Terminating int
	// All is number of all workers including valid and invalid workers.
	All int
}

// PoolStatus is a snapshot of the state of a Pool.
type PoolStatus struct {
	// Error is the string representation of the error present in the pool.
	// May be nil if there is no error.
	Error *string

	// Jobs
	Jobs PoolJobsStatus

	// Workers
	Workers PoolWorkersStatus

	// Pool status
	State PoolState
}

// JobStatus is a representation of a Job state in the Pool.
type JobStatus struct {
	t *opJob

	ID string

	State ExecutionState
	Error error

	QueuedOn  *time.Time
	StartedOn *time.Time
	StoppedOn *time.Time

	QueuedDuration    *time.Duration
	ExecutionDuration *time.Duration
}

// Job returns the actual Job attached to this State.
func (s *JobStatus) Job() Job {
	return s.t.Job
}

// Context returns the execution context attached to this job state (if available).
// May be nil if not set.
func (s *JobStatus) Context() context.Context {
	return s.t.Context
}
