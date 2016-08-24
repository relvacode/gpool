package gpool

import "time"

// DefaultScheduler is the Scheduler used by the Pool if one was not provided.
var DefaultScheduler = FIFOScheduler{}

// AsSoonAsPossible is a 0 duration that can be used when returning "askAgain" in a Scheduler Evaluation call.
// This indicates that the next Evaluate call should happen as soon as possible.
const AsSoonAsPossible = time.Duration(0)

// Scheduler is an interface that evaluates the next Job in the queue to be executed.
type Scheduler interface {
	// Evaluate evaluates a slice of Job States and returns the index of the Job that should be executed.
	// The index is ignored if "ok" is false in which case no Job should be scheduled,
	// in this case Evaluate is not called until the timeout duration has passed.
	// Evaluate is called for every bus cycle where at least one Job is available to be executed.
	Evaluate([]*JobState) (idx int, timeout time.Duration, ok bool)
	// Unload unloads a Job from the scheduler when the Job stops regardless of reason.
	Unload(*JobState)
}

// FIFOScheduler is a scheduler that executes Jobs on a first in, first out order.
type FIFOScheduler struct{}

// Always return the first Job
func (FIFOScheduler) Evaluate(s []*JobState) (int, time.Duration, bool) {
	return 0, AsSoonAsPossible, true
}

func (FIFOScheduler) Unload(*JobState) {}

// LIFOScheduler is a scheduler that executes Jobs on a last in, first out order.
type LIFOScheduler struct{}

// Always return the last Job
func (LIFOScheduler) Evaluate(jobs []*JobState) (int, time.Duration, bool) {
	return len(jobs) - 1, AsSoonAsPossible, true
}

func (LIFOScheduler) Unload(*JobState) {}

// A ScheduleRule is an interface that implements methods for checking if a Job State should be executed.
type ScheduleRule interface {
	// Check inspects whether the given Job state can be executed at this time.
	// Check should return false if the Job cannot be scheduled.
	// askAgain is the duration to wait before calling Check on this rule again.
	// If a job was accepted by all configured rules then Load is called within the same cycle.
	Check(*JobState) (timeout time.Duration, ok bool)
	// Load is called once all configured Rules confirm they are happy to execute a Job.
	// This is used for internal state tracking of the rule.
	// For example you may have a Rule that monitors the total space consumed by a job,
	// in which case load would increase the total amount of consumed space.
	Load(*JobState)
	// Unload is called when a job finishes regardless of exit reason.
	Unload(*JobState)
}

// RuleScheduler is a Scheduler that schedules the next Job for execution based on a set of ScheduleRules.
// All rules must pass before a Job can be scheduled.
type RuleScheduler []ScheduleRule

func (sch RuleScheduler) Unload(j *JobState) {
	for _, r := range sch {
		r.Unload(j)
	}
}

func (sch RuleScheduler) Evaluate(jobs []*JobState) (int, time.Duration, bool) {
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
