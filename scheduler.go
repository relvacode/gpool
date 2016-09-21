package gpool

import (
	"sync"
	"time"
)

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
	// Unload may be called if a job is cancelled whilst in queue, for which a call to Load has not occurred yet.
	// The implementer should check the state of the job to determine at which point the job is being unloaded on.
	// A state of Queued means the job was aborted in queue.
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

// NewGatedScheduler returns a new Scheduler by extending an existing one.
// Closed sets the initial gate status.
func NewGatedScheduler(Base Scheduler, Closed bool) *GatedScheduler {
	return &GatedScheduler{
		Scheduler: Base,
		mtx:       &sync.RWMutex{},
		open:      !Closed,
	}
}

// GatedScheduler extends an existing scheduler to allow the ability to open and close execution of further jobs.
// Open and Close may be called concurrently.
// If the pool asks for a job and the scheduler is closed the pool will not ask again for at least 1 second.
type GatedScheduler struct {
	Scheduler

	mtx  *sync.RWMutex
	open bool
}

// IsOpen returns true if this scheduler is able to begin executing jobs (the gate is open).
func (sch *GatedScheduler) IsOpen() bool {
	sch.mtx.RLock()
	defer sch.mtx.RUnlock()
	return sch.open
}

// Close prevents any further jobs from being executed.
func (sch *GatedScheduler) Close() {
	sch.mtx.Lock()
	defer sch.mtx.Unlock()
	sch.open = false
}

// Open allows jobs to begin executing if the gate is closed.
func (sch *GatedScheduler) Open() {
	sch.mtx.Lock()
	defer sch.mtx.Unlock()
	sch.open = true
}

// Evaluate checks whether the gate is open and if so calls the underlying scheduler.
func (sch *GatedScheduler) Evaluate(s []*JobStatus) (int, time.Duration, bool) {
	sch.mtx.RLock()
	if !sch.open {
		sch.mtx.RUnlock()
		return -1, time.Second, false
	}
	sch.mtx.RUnlock()
	return sch.Scheduler.Evaluate(s)
}
