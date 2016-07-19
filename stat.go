package gpool

import (
	"fmt"
	"time"
)

type Timing struct {
	Started  time.Time
	Finished time.Time
	Duration time.Duration
}

func (s *Timing) Start() {
	s.Started = time.Now()
}

func (s *Timing) Stop() {
	s.Finished = time.Now()
	s.Duration = s.Finished.Sub(s.Started)
}

type ExecutionTiming struct {
	Queued    *Timing
	Executing *Timing
	Overall   *Timing
}

type JobState struct {
	j          Job
	ID         string
	Identifier fmt.Stringer
	State      string

	Output interface{}

	Duration ExecutionTiming
	Error    error
}

func (js JobState) Job() Job {
	return js.j
}
