package gpool

import "sync"

func newStateManager(target int) *stateManager {
	return &stateManager{
		0, target, 0, 0,
		&sync.RWMutex{},
	}
}

type stateManager struct {
	cW  int // Current workers
	tW  int // Target workers
	cJ  int // Current jobs
	dJ  int // Done jobs
	mtx *sync.RWMutex
}

func (s *stateManager) AddWorker() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.cW++
}

func (s *stateManager) RemoveWorker() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.cW == 0 {
		panic("negative scale counter")
	}
	s.cW--
}

func (s *stateManager) AddJob() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.cJ++
}

func (s *stateManager) RemoveJob() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.cJ == 0 {
		panic("negative job counter")
	}
	s.cJ--
	s.dJ++
}

func (s *stateManager) AdjTarget(Delta int) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if Delta < 0 && (s.tW-Delta) < 1 {
		panic("requested target lower than 0")
	}
	s.tW += Delta
}

func (s *stateManager) WorkerState() (int, int) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.cW, s.tW
}

func (s *stateManager) JobState() (int, int) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.cJ, s.dJ
}
