package gpool

import (
	"fmt"
	"sync"
	"time"
)

type PoolState struct {
	Jobs              []JobState
	AvailableWorkers  int
	ExecutionDuration time.Duration
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
	ID         int
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
		tW:  target,
		gs:  make(map[int]*JobState),
	}
}

type mgr struct {
	mtx *sync.RWMutex

	tW int // Target workers
	cW int // Current workers

	gs map[int]*JobState

	wT time.Duration
}

const (
	Queued    string = "Queued"
	Executing string = "Executing"
	Failed    string = "Failed"
	Finished  string = "Finished"
)

func (s *mgr) State() *PoolState {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	r := []JobState{}
	for _, j := range s.gs {
		r = append(r, *j)
	}
	return &PoolState{
		Jobs:              r,
		AvailableWorkers:  s.cW,
		ExecutionDuration: s.wT,
	}
}

func (s *mgr) ID(i int) JobState {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	j := s.gs[i]
	return *j
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
	} else {
		js.State = state
	}
	switch state {
	case Queued:
		js.Duration.Queued.Start()
	case Executing:
		js.Duration.Queued.Stop()
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
		v.Duration.Job.Stop()

		v.Error = jr.Error
		v.State = state
		v.Output = jr.Output
		return *v
	}
}

func (s *mgr) setWorker() func() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.cW++
	return func() {
		s.mtx.Lock()
		defer s.mtx.Unlock()
		s.cW--
	}
}

func (s *mgr) RunningWorkers() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.cW
}

func (s *mgr) workers() (int, int) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.cW, s.tW
}

func (s *mgr) setWorkerTarget(Target int) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.tW = Target
}

func (s *mgr) adjWorkerTarget(Delta int) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if Delta < 0 && (s.tW-Delta) < 1 {
		panic("requested target lower than 0")
	}
	s.tW += Delta
}
