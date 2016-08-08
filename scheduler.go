package gpool

// A ScheduleRule is an interface that implements methods for selecting the next Job in the Pool queue to be executed.
type ScheduleRule interface {
	// Check inspects whether the next Job in the queue can be scheduled.
	// Check should return an error if the job should not be scheduled at this time.
	// During each bus cycle, Check is called for each Job pending in the Job queue starting at the first (oldest)
	// in the queue.
	// If a job was accepted by all configured rules then Load is called within the same cycle.
	Check(Job) error
	// Load is called once all configured Rules confirm they are happy to execute a Job.
	// This is used for internal state tracking of the rule.
	// For example you may have a Rule that monitors the total space consumed by a job,
	// in which case load would increase the total amount of consumed space.
	Load(Job)
	// Unload is called when a job finishes regardless of exit reason.
	Unload(Job)
}

type scheduler []ScheduleRule

// offload processes a finished job from the pool.
func (sch scheduler) offload(j Job) {
	for _, r := range sch {
		r.Unload(j)
	}
}

// Evaluate whether a Job should be scheduled
func (sch scheduler) evaluate(j Job) error {
	// Check interface with each rulseset to ensure we can be scheduled
	for _, r := range sch {
		if err := r.Check(j); err != nil {
			return err
		}
	}
	// If everything goes to plan then load the interface with the ruleset
	for _, r := range sch {
		r.Load(j)
	}
	return nil
}
