package gpool

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"strconv"
	"testing"
	"time"
)

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

func (orderedJob) Abort(error) {}

func (orderedJob) Run(context.Context) error {
	time.Sleep(time.Millisecond)
	return nil
}

func testPoolSchedulingOrder(n int, scheduler Scheduler) []int {
	p := NewCustomPool(false, scheduler, NewStaticBridge(1))

	IDs := []int{}
	p.Hook.Stop = func(js *JobStatus) {
		if h, ok := js.Job().Header().(orderedJobHeader); ok {
			IDs = append(IDs, h.i)
		}

	}

	jobs := []Job{}
	for i := 0; i < n; i++ {
		jobs = append(jobs, newOrderedJob(i+1))
	}

	if err := p.QueueBatch(nil, jobs); err != nil {
		return IDs
	}

	p.Close()
	p.Wait()
	p.Destroy()
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
	// go test -c && ./gpool.test -test.cpuprofile=cpu.prof -test.memprofile=mem.prof -test.bench=BenchmarkPool_Execute && go tool pprof -pdf gpool.test cpu.prof > profile.pdf
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

var errPreload = errors.New("preload denied")

type testPreloadScheduler struct {
	FIFOScheduler
}

func (testPreloadScheduler) Preload(*JobStatus) error {
	return errPreload
}

func TestSchedulerPreload(t *testing.T) {
	p := NewCustomPool(true, testPreloadScheduler{}, NewStaticBridge(1))
	defer p.Destroy()

	err := p.Queue(context.Background(), NewJob(Header("test"), func(context.Context) error {
		return nil
	}, nil))
	if err == nil {
		t.Fatal("expected error but got nil")
	}
	if err != errPreload {
		t.Fatalf("expected %s, got %s", errPreload, err)
	}
}

type testTimeoutScheduler struct {
	NoOpSchedulerBase
}

func (testTimeoutScheduler) Evaluate(jobs []*JobStatus) (int, time.Duration, bool) {
	return 0, time.Second, true
}

func TestSchedulerEvaluateTimeout(t *testing.T) {
	t.Parallel()
	p := NewCustomPool(false, testTimeoutScheduler{}, NewStaticBridge(1))
	defer p.Destroy()

	j := NewJob(Header("test"), func(context.Context) error {
		return nil
	}, nil)

	p.Start(context.Background(), j)
	s := time.Now()
	p.Start(context.Background(), j)
	if time.Since(s) < time.Millisecond {
		t.Fatal("job was started instantly")
	}
}
