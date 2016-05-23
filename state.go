package gpool

import (
	"sync"
	"time"
)

func newStateManager(target int) *stateManager {
	return &stateManager{
		mtx: &sync.RWMutex{},
		tW:  target,
	}
}

type stateManager struct {
	mtx *sync.RWMutex

	tW int // Target workers
	cW int // Current workers

	cJ int // Current jobs
	dJ int // Done jobs
	eJ int // Jobs with error

	wT time.Duration
}

func (s *stateManager) workers() (int, int) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.cW, s.tW
}

func (s *stateManager) Worker() func() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.cW++
	return func() {
		s.mtx.Lock()
		defer s.mtx.Unlock()
		s.cW--
	}
}

func (s *stateManager) Job() func(j JobResult) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.cJ++
	start := time.Now()
	return func(j JobResult) {
		s.mtx.Lock()
		defer s.mtx.Unlock()
		s.cJ--
		s.dJ++
		s.wT += time.Since(start)
		if j.Error != nil {
			s.eJ++
		}
	}
}

func (s *stateManager) SetTarget(Target int) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.tW = Target
}

func (s *stateManager) AdjTarget(Delta int) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if Delta < 0 && (s.tW-Delta) < 1 {
		panic("requested target lower than 0")
	}
	s.tW += Delta
}

// PoolInfo is a snapshot of the pool state.
type PoolInfo struct {
	Workers  int // Number of currently running or waiting workers
	Running  int // Number of Jobs running
	Finished int // Number of Jobs finished
	Failed   int // Number of Jobs that contain an error

	Duration time.Duration // Total Job execution duration
}

func (s *stateManager) Stat() PoolInfo {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return PoolInfo{
		s.cW,
		s.cJ,
		s.dJ,
		s.eJ,
		s.wT,
	}
}
