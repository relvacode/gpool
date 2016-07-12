package gpool

import (
	"fmt"
	"sync"
	"time"
)

type PoolState struct {
	TotalJobs         int
	RunningWorkers    int
	TargetWorkers     int
	QueuedDuration    time.Duration
	ExecutionDuration time.Duration
	TotalDuration     time.Duration
}

type StateDuration struct {
	Started  time.Time
	Finished time.Time
	Duration time.Duration
}

func (s *StateDuration) Start() {
	s.Started = time.Now()
}

func (s *StateDuration) Stop() {
	s.Finished = time.Now()
	s.Duration = s.Finished.Sub(s.Started)
}

type JobDuration struct {
	Job       *StateDuration
	Queued    *StateDuration
	Executing *StateDuration
}

type JobState struct {
	j          Job
	ID         string
	Identifier fmt.Stringer
	State      string

	Output interface{}

	Duration JobDuration
	Error    error
}

func (js JobState) Job() Job {
	return js.j
}

func newMgr(target int) *mgr {
	return &mgr{
		mtx: &sync.RWMutex{},
		s:   &PoolState{TargetWorkers: target},
		gs:  make(map[string]*JobState),
	}
}

type mgr struct {
	mtx *sync.RWMutex

	s *PoolState

	gs map[string]*JobState

	wT time.Duration
}

const (
	Queued    string = "Queued"
	Executing string = "Executing"
	Failed    string = "Failed"
	Finished  string = "Finished"
)

func (s *mgr) State() PoolState {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return *s.s
}

func (s *mgr) Jobs() []JobState {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	r := []JobState{}
	for _, j := range s.gs {
		r = append(r, *j)
	}
	return r
}

// ID gets a Job by the specified ID.
func (s *mgr) ID(i string) (JobState, bool) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	j, ok := s.gs[i]
	if !ok {
		return JobState{}, false
	}
	return *j, true
}

func (s *mgr) trackReq(jr jobRequest, state string) JobState {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	var js *JobState
	var ok bool
	if js, ok = s.gs[jr.ID]; !ok {
		js = &JobState{
			j:          jr.Job,
			ID:         jr.ID,
			Identifier: jr.Job.Identifier(),
			State:      state,
			Duration: JobDuration{
				Job:       &StateDuration{},
				Queued:    &StateDuration{},
				Executing: &StateDuration{},
			},
		}
		js.Duration.Job.Start()
		s.gs[jr.ID] = js
		s.s.TotalJobs++
	} else {
		js.State = state
	}
	switch state {
	case Queued:
		js.Duration.Queued.Start()
	case Executing:
		js.Duration.Queued.Stop()
		s.s.QueuedDuration += js.Duration.Queued.Duration
		js.Duration.Executing.Start()
	}
	return *js
}

func (s *mgr) trackRes(jr *jobResult, state string) JobState {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if v, ok := s.gs[jr.ID]; !ok {
		panic("job not tracked!")
	} else {
		s.wT += jr.Duration

		v.Duration.Executing.Stop()
		s.s.ExecutionDuration += v.Duration.Executing.Duration
		v.Duration.Job.Stop()
		s.s.TotalDuration += v.Duration.Job.Duration

		v.Error = jr.Error
		v.State = state
		v.Output = jr.Output
		return *v
	}
}

func (s *mgr) setWorker() func() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.s.RunningWorkers++
	return func() {
		s.mtx.Lock()
		defer s.mtx.Unlock()
		s.s.RunningWorkers--
	}
}

func (s *mgr) RunningWorkers() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.s.RunningWorkers
}

func (s *mgr) workers() (int, int) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.s.RunningWorkers, s.s.TargetWorkers
}

func (s *mgr) setWorkerTarget(Target int) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.s.TargetWorkers = Target
}

func (s *mgr) adjWorkerTarget(Delta int) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if Delta < 0 && (s.s.TargetWorkers-Delta) < 1 {
		return
	}
	s.s.TargetWorkers += Delta
}
