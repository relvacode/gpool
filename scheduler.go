package gpool

import "time"

// DefaultScheduler is the Scheduler used by the Pool if one was not provided.
var DefaultScheduler = FIFOScheduler{}

// AsSoonAsPossible is a 0 duration that can be used when returning "timeout" in a Scheduler Evaluation call.
// This indicates that the next Evaluate call should happen as soon as possible.
const AsSoonAsPossible = time.Duration(0)

// Scheduler is an interface that evaluates the next Job in the queue to be executed.
type Scheduler interface {
	// Evaluate evaluates a slice of Job States and returns the index of the Job that should be executed.
	// The index is ignored if "ok" is false in which case no Job should be scheduled,
	// in this case Evaluate is not called until the timeout duration has passed.
	// Evaluate is called for every bus cycle where at least one Job is available to be executed.
	Evaluate([]*JobStatus) (idx int, timeout time.Duration, ok bool)
	// Unload unloads a Job from the scheduler when the Job stops regardless of reason.
	Unload(*JobStatus)
}

// FIFOScheduler is a scheduler that executes Jobs on a first in, first out order.
type FIFOScheduler struct{}

// Evaluate always returns the first index and a 0 timeout duration.
func (FIFOScheduler) Evaluate(s []*JobStatus) (int, time.Duration, bool) {
	return 0, AsSoonAsPossible, true
}

// Unload does nothing.
func (FIFOScheduler) Unload(*JobStatus) {}

// LIFOScheduler is a scheduler that executes Jobs on a last in, first out order.
type LIFOScheduler struct{}

// Evaluate always returns the last index and a 0 timeout duration.
func (LIFOScheduler) Evaluate(jobs []*JobStatus) (int, time.Duration, bool) {
	return len(jobs) - 1, AsSoonAsPossible, true
}

// Unload does nothing.
func (LIFOScheduler) Unload(*JobStatus) {}

// A ScheduleRule is an interface that implements methods for checking if a Job State should be executed.
type ScheduleRule interface {
	// Check inspects whether the given Job state can be executed at this time.
	// Check should return false if the Job cannot be scheduled.
	// askAgain is the duration to wait before calling Check on this rule again.
	// If a job was accepted by all configured rules then Load is called within the same cycle.
	Check(*JobStatus) (timeout time.Duration, ok bool)
	// Load is called once all configured Rules confirm they are happy to execute a Job.
	// This is used for internal state tracking of the rule.
	// For example you may have a Rule that monitors the total space consumed by a job,
	// in which case load would increase the total amount of consumed space.
	Load(*JobStatus)
	// Unload is called when a job finishes regardless of exit reason.
	Unload(*JobStatus)
}

// RuleScheduler is a Scheduler that schedules the next Job for execution based on a set of ScheduleRules.
// All rules must pass before a Job can be scheduled.
type RuleScheduler []ScheduleRule

// Unload calls Unload on each ScheduleRule.
func (sch RuleScheduler) Unload(j *JobStatus) {
	for _, r := range sch {
		r.Unload(j)
	}
}

// Evaluate calls Check on each ScheduleRule for each Job in jobs.
// If the ScheduleRule returns not ok then the timeout of that ScheduleRule is returned.
// If successful, Load is called on each ScheduleRule and the index of that successful Job is returned.
func (sch RuleScheduler) Evaluate(jobs []*JobStatus) (int, time.Duration, bool) {
	for idx, j := range jobs {
		for _, r := range sch {
			if w, ok := r.Check(j); !ok {
				return -1, w, false
			}
		}
		for _, r := range sch {
			r.Load(j)
		}
		return idx, AsSoonAsPossible, true
	}
	return -1, AsSoonAsPossible, false
}
