package gpool

import "testing"

func Test_Worker_Defer(t *testing.T) {
	sm := newStateManager(0)

	func() {
		f := sm.Worker()
		defer f()

		c, _ := sm.workers()
		if c != 1 {
			t.Fatal("expected 2 workers, got", c)
		}
	}()

	c, _ := sm.workers()
	if c != 0 {
		t.Fatal("expected 0 worker, got", c)
	}
}
