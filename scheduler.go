package gpool

import "time"

// Scheduler is an interface that evaluates the next job in the queue to be executed.
type Scheduler interface {
	// Evaluate evaluates a slice of job statuses and returns the index of the job that should be executed.
	// The index is ignored if "ok" is false in which case no job should be scheduled,
	// in this case evaluate is not called until the timeout duration has passed.
	// Evaluate is called for every bus cycle where at least one job is available to be executed.
	Evaluate([]*JobStatus) (idx int, timeout time.Duration, ok bool)
	// Unload unloads a job from the scheduler when the job stops regardless of reason.
	Unload(*JobStatus)
}

// FIFOScheduler is a scheduler that executes jobs on a first in, first out order.
type FIFOScheduler struct{}

// Evaluate always returns the first index and a 0 timeout duration.
func (FIFOScheduler) Evaluate(s []*JobStatus) (int, time.Duration, bool) {
	return 0, AsSoonAsPossible, true
}

// Unload does nothing.
func (FIFOScheduler) Unload(*JobStatus) {}

// LIFOScheduler is a scheduler that executes jobs on a last in, first out order.
type LIFOScheduler struct{}

// Evaluate always returns the last index and a 0 timeout duration.
func (LIFOScheduler) Evaluate(jobs []*JobStatus) (int, time.Duration, bool) {
	return len(jobs) - 1, AsSoonAsPossible, true
}

// Unload does nothing.
func (LIFOScheduler) Unload(*JobStatus) {}

// A ScheduleRule is an interface that implements methods for checking if a job status should be executed.
type ScheduleRule interface {
	// Check inspects whether the given job state can be executed at this time.
	// Check should return false if the job cannot be scheduled.
	// Timeout is the duration to wait before calling check on this rule again.
	// If a job was accepted by all configured rules then Load is called within the same cycle.
	Check(*JobStatus) (timeout time.Duration, ok bool)
	// Load is called once all configured rules confirm they are happy to execute a job.
	// This is used for internal state tracking of the rule.
	// For example you may have a rule that monitors the total space consumed by a job,
	// in which case load would increase the total amount of consumed space.
	Load(*JobStatus)
	// Unload is called when a job finishes regardless of exit reason.
	Unload(*JobStatus)
}

// RuleScheduler is a scheduler that schedules the next job for execution based on a set of schedule rules.
// All rules must pass before a job can be scheduled.
type RuleScheduler []ScheduleRule

// Unload calls unload on each rule.
func (sch RuleScheduler) Unload(j *JobStatus) {
	for _, r := range sch {
		r.Unload(j)
	}
}

// Evaluate calls check on each schedule rule for each job status in jobs.
// If the rule returns not ok then the timeout of that rule is returned.
// If successful, Load is called on each rule and the index of that job is returned.
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
