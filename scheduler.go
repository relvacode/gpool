package gpool

import "time"

// DefaultScheduler is the Scheduler used by the Pool if one wasn't provided.
var DefaultScheduler = FIFOScheduler{}

// AsSoonAsPossible is a 0 duration that can be used in the next evaluation timeout to indicate that the next
// call to Evaluate() should happen as soon as possible.
const AsSoonAsPossible = time.Duration(0)

// Scheduler is an interface that evaluates the next Job in the queue to be executed.
type Scheduler interface {
	// Evaluate evaluates a slice of Jobs and returns the index of the Job that should be executed.
	// The index is ignored if ok false in which case no Job should be scheduled.
	// Evaluate is called for every bus cycle where at least one Job is available to be executed.
	// If no Job is available at this time, the pool will not call evaluate until after next duration.
	Evaluate([]*State) (idx int, next time.Duration, ok bool)
	// Unload unloads a Job from the scheduler when the Job stops regardless of reason.
	Unload(*State)
}

// FIFOScheduler is a scheduler that executes Jobs on a first in, first out order.
type FIFOScheduler struct{}

// Always return the first Job
func (FIFOScheduler) Evaluate(s []*State) (int, time.Duration, bool) {
	return 0, AsSoonAsPossible, true
}

func (FIFOScheduler) Unload(*State) {}

// LIFOScheduler is a scheduler that executes Jobs on a last in, first out order.
type LIFOScheduler struct{}

// Always return the last Job
func (LIFOScheduler) Evaluate(jobs []*State) (int, time.Duration, bool) {
	return len(jobs) - 1, AsSoonAsPossible, true
}

func (LIFOScheduler) Unload(*State) {}

// A ScheduleRule is an interface that implements methods for checking if a Job should be executed.
type ScheduleRule interface {
	// Check inspects whether the next Job in the queue can be scheduled.
	// Check should return an error if the job should not be scheduled at this time.
	// During each bus cycle, Check is called for each Job State pending in the Job queue starting at the first (oldest)
	// in the queue.
	// If a job was accepted by all configured rules then Load is called within the same cycle.
	Check(*State) error
	// Load is called once all configured Rules confirm they are happy to execute a Job.
	// This is used for internal state tracking of the rule.
	// For example you may have a Rule that monitors the total space consumed by a job,
	// in which case load would increase the total amount of consumed space.
	Load(*State)
	// Unload is called when a job finishes regardless of exit reason.
	Unload(*State)
}

// RuleScheduler is a Scheduler that schedules the next Job for execution based on a set of ScheduleRules.
// All rules must pass before a Job can be scheduled.
type RuleScheduler []ScheduleRule

func (sch RuleScheduler) Unload(j *State) {
	for _, r := range sch {
		r.Unload(j)
	}
}

func (sch RuleScheduler) Evaluate(jobs []*State) (int, time.Duration, bool) {
	for idx, j := range jobs {
		for _, r := range sch {
			if err := r.Check(j); err != nil {
				return -1, AsSoonAsPossible, false
			}
		}
		for _, r := range sch {
			r.Load(j)
		}
		return idx, AsSoonAsPossible, true
	}
	return -1, AsSoonAsPossible, false
}
