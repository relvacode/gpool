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

type JobState struct {
	j          Job
	ID         int
	Identifier fmt.Stringer
	State      string

	Output   interface{}
	Duration time.Duration
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
	if v, ok := s.gs[jr.ID]; !ok {
		j := &JobState{
			j:          jr.Job,
			ID:         jr.ID,
			Identifier: jr.Job.Identifier(),
			State:      state,
		}
		s.gs[jr.ID] = j
		return *j
	} else {
		v.State = state
		return *v
	}
}

func (s *mgr) trackRes(jr *jobResult, state string) JobState {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if v, ok := s.gs[jr.ID]; !ok {
		panic("job not tracked!")
	} else {
		s.wT += jr.Duration
		v.Duration = jr.Duration
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
