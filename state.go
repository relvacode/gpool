package gpool

import "sync"

func newStateManager(target int) *stateManager {
	return &stateManager{
		0, target, &sync.Mutex{},
	}
}

type stateManager struct {
	current int
	target  int
	lock    *sync.Mutex
}

func (s *stateManager) Remove() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.current == 0 {
		panic("negative scale counter")
	}
	s.current--
}

func (s *stateManager) Add() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.current++
}

func (s *stateManager) SetTarget(t int) int {
	if t < 1 {
		panic("target scale less than 1")
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	s.target = t
	return s.target - s.current
}

func (s *stateManager) GetTarget() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.target
}

// Die returns true if the caller should die to reach target
func (s *stateManager) Die() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.current > s.target {
		return true
	}
	return false
}

func (s *stateManager) State() (int, int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.current, s.target
}

func (s *stateManager) Stable() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.current == s.target
}
