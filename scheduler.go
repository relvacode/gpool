package gpool

import "time"

// Scheduler is an interface that evaluates the next job in the queue to be executed.
type Scheduler interface {
	// Evaluate evaluates a slice of job statuses and returns the index of the job that should be executed.
	// The index is ignored if "ok" is false in which case no job should be scheduled,
	// in this case evaluate is not called until the timeout duration has passed.
	// Evaluate is called for every bus cycle where at least one job is available to be executed.
	Evaluate([]*JobStatus) (idx int, timeout time.Duration, ok bool)

	// Preload is called just before a job begins to queue on the pool and can be used to reject jobs from even entering the queue.
	// The scheduler should return a non-nil error if the job must not begin queueing at this time.
	// The calling operation will receive this error.
	Preload(*JobStatus) error

	// Load loads a job state before it begins executing. This usually happens directly after a successful call to evaluate.
	Load(*JobStatus)

	// Unload unloads a job from the scheduler when the job stops regardless of reason.
	Unload(*JobStatus)
}

// NoOpSchedulerBase is an empty struct that implements preload, load and unload method that do not perform any action.
type NoOpSchedulerBase struct{}

// Preload does nothing.
func (NoOpSchedulerBase) Preload(*JobStatus) error {
	return nil
}

// Load does nothing (noop).
func (NoOpSchedulerBase) Load(*JobStatus) {}

// Unload does nothing (noop).
func (NoOpSchedulerBase) Unload(*JobStatus) {}

// FIFOScheduler is a scheduler that executes jobs on a first in, first out order.
type FIFOScheduler struct {
	NoOpSchedulerBase
}

// Evaluate always returns the first index and a 0 timeout duration.
func (FIFOScheduler) Evaluate(s []*JobStatus) (int, time.Duration, bool) {
	return 0, AsSoonAsPossible, true
}

// LIFOScheduler is a scheduler that executes jobs on a last in, first out order.
type LIFOScheduler struct {
	NoOpSchedulerBase
}

// Evaluate always returns the last index and a 0 timeout duration.
func (LIFOScheduler) Evaluate(jobs []*JobStatus) (int, time.Duration, bool) {
	return len(jobs) - 1, AsSoonAsPossible, true
}
