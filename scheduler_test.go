package gpool

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func block(p *Pool) chan bool {
	r := make(chan bool)
	p.Submit(
		NewJob(
			Header("blocking"),
			func(*WorkContext) error {
				<-r
				return nil
			},
			nil,
		))
	return r
}

type orderedJobHeader struct {
	i int
}

func (oj orderedJobHeader) String() string {
	return strconv.Itoa(oj.i)
}

func newOrderedJob(i int) *orderedJob {
	return &orderedJob{
		i: i,
	}
}

type orderedJob struct {
	i int
}

func (j orderedJob) Header() fmt.Stringer {
	return orderedJobHeader{i: j.i}
}

func (orderedJob) Abort() {}

func (orderedJob) Run(*WorkContext) error {
	time.Sleep(time.Millisecond)
	return nil
}

func testPoolSchedulingOrder(n int, scheduler Scheduler) []int {
	p := NewPool(1, false, scheduler)

	IDs := []int{}
	p.Hook.Stop = func(js *State) {
		if h, ok := js.Job().Header().(orderedJobHeader); ok {
			IDs = append(IDs, h.i)
		}

	}

	defer p.Destroy()
	pause := block(p)

	for i := range make([]int, n) {
		p.Queue(newOrderedJob(i + 1))
	}

	close(pause)
	p.Close()
	p.Wait()
	return IDs
}

func TestFIFOScheduler_Evaluate(t *testing.T) {
	order := testPoolSchedulingOrder(10, FIFOScheduler{})
	if len(order) != 10 {
		t.Fatal("expected 10 jobs, got ", len(order))
	}
	if order[9] != 10 {
		t.Fatal("expected last id to be 10, got ", order[9])
	}
	if order[0] != 1 {
		t.Fatal("expected first id to be 1, got ", order[0])
	}
}

func TestLIFOScheduler_Evaluate(t *testing.T) {
	order := testPoolSchedulingOrder(10, LIFOScheduler{})
	if len(order) != 10 {
		t.Fatal("expected 10 jobs, got ", len(order))
	}
	if order[9] != 1 {
		t.Fatal("expected last id to be 1, got ", order[9])
	}
	if order[0] != 10 {
		t.Fatal("expected first id to be 10, got ", order[0])
	}
}
