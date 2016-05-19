package gpool

import "sync"

func newStateManager(target int) *stateManager {
	return &stateManager{
		0, target, 0, 0, &sync.Mutex{},
	}
}

type stateManager struct {
	cW   int
	tW   int
	cJ   int
	dJ   int
	lock *sync.Mutex
}

func (s *stateManager) RemoveWorker() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.cW == 0 {
		panic("negative scale counter")
	}
	s.cW--
}

func (s *stateManager) AddWorker() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.cW++
}

func (s *stateManager) AddJob() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.cJ++
}

func (s *stateManager) RemoveJob() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.cJ == 0 {
		panic("negative job counter")
	}
	s.cJ--
	s.dJ++
}

func (s *stateManager) IncrTarget(I int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.tW += I
}

func (s *stateManager) DecTarget(I int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if (s.tW - I) < 1 {
		panic("requested target lower than 0")
	}
	s.tW -= I
}

func (s *stateManager) GetTarget() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.tW
}

func (s *stateManager) WorkerState() (int, int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.cW, s.tW
}

func (s *stateManager) JobState() (int, int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.cJ, s.dJ
}

func (s *stateManager) Stable() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.cW == s.tW
}
