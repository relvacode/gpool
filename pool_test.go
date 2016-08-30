package gpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type testingJob struct {
	name    string
	delay   time.Duration
	wait    chan bool
	err     error
	aborted bool
}

func (j *testingJob) Header() fmt.Stringer {
	return Header(j.name)
}

func (j *testingJob) Abort() {
	j.aborted = true
}

func (j *testingJob) Run(ctx context.Context) error {
	time.Sleep(j.delay)
	if j.wait != nil {
		<-j.wait
	}
	return j.err
}

func TestPool_Destroy(t *testing.T) {
	p := NewPool(1, true, nil)
	p.Destroy()

	res := make(chan interface{})
	go func() {
		res <- p.Status()
	}()

	select {
	case <-res:
		t.Fatal("bus isn't dead")
	case <-time.After(time.Microsecond):
		return
	}
}

func TestPool_Context(t *testing.T) {
	p := NewPool(1, false, nil)
	defer p.Destroy()

	var id string
	var value interface{}
	p.Execute(context.WithValue(context.Background(), "key", "value"), NewJob(Header("test"), func(ctx context.Context) error {
		id, _ = JobIDFromContext(ctx)
		value = ctx.Value("key")
		return nil
	}, nil))

	if id == "" {
		t.Fatal("expected ID but got nothing")
	}
	if value == nil {
		t.Fatal("nil value")
	}
	if str, ok := value.(string); !ok {
		t.Fatal("value is not string")
	} else if str != "value" {
		t.Fatal("expected 'value', got ", str)
	}
}

func TestPool_Error(t *testing.T) {
	p := NewPool(1, true, nil)
	defer p.Destroy()

	pErr := errors.New("testing error")

	j := &testingJob{
		name: "TestPool_Error_Propagation",
		err:  pErr,
	}
	if err := p.Start(nil, j); err != nil {
		t.Fatal("unexpected error: ", err)
	}
	p.Close()
	p.Wait()
	err := p.Error()
	if err == nil {
		t.Fatal("expected error got nil")
	}
	if err != pErr {
		t.Fatalf("expected 'testing error', got %v", err)
	}
}

func TestPool_Execute_OK(t *testing.T) {
	p := NewPool(1, true, nil)
	defer p.Destroy()
	j := &testingJob{
		name: "TestPool_Execute_OK",
	}
	if err := p.Execute(nil, j); err != nil {
		t.Fatal(err)
	}
	p.Close()
	p.Wait()
}

func TestPool_Execute_Error(t *testing.T) {
	p := NewPool(1, true, nil)
	defer p.Destroy()

	mkErr := errors.New("execution error")

	j := &testingJob{
		name: "TestPool_Execute_Error",
		err:  mkErr,
	}
	if err := p.Execute(nil, j); err != nil {
		if err != mkErr {
			t.Fatalf("wanted %s, got %s", mkErr, err)
		}
	} else {
		t.Fatal("expected error")
	}
	p.Close()
}

func TestPool_Status(t *testing.T) {
	p := NewPool(1, true, nil)

	ok := make(chan bool)

	j := &testingJob{
		name: "TestPool_State",
		wait: ok,
	}
	if err := p.Start(nil, j); err != nil {
		t.Fatal(err)
	}
	p.Close()
	s := p.Status()
	if s.Jobs.Executing != 1 {
		t.Fatal("wanted 1 executing, got ", s.Jobs.Executing)
	}
	if s.Jobs.Queued != 0 {
		t.Fatal("wanted 0 queued, got ", s.Jobs.Queued)
	}
	if s.Jobs.Finished != 0 {
		t.Fatal("wanted 0 finished, got ", s.Jobs.Finished)
	}
	if s.Jobs.Failed != 0 {
		t.Fatal("wanted 0 failed, got ", s.Jobs.Failed)
	}
	if s.Error != nil {
		t.Fatal("wanted nil error, got ", s.Error)
	}
	close(ok)
}

func TestPool_Wait(t *testing.T) {
	p := NewPool(1, true, nil)
	ok := make(chan bool)
	go func() {
		p.Wait()
		close(ok)
	}()
	p.Kill()
	p.Wait()
	p.Wait()
	<-ok
}

func TestPool_Hook(t *testing.T) {
	p := NewPool(1, true, nil)
	defer p.Destroy()

	var queued, started, stopped bool
	p.Hook.Queue = func(*JobStatus) {
		queued = true
	}
	p.Hook.Start = func(*JobStatus) {
		started = true
	}
	p.Hook.Stop = func(*JobStatus) {
		stopped = true
	}

	j := &testingJob{
		name: "TestPool_Hook",
	}
	if err := p.Execute(nil, j); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if !queued {
		t.Fatal("failed to fire queued hook")
	}
	if !started {
		t.Fatal("failed to fire started hook")
	}
	if !stopped {
		t.Fatal("failed to fire stopped hook")
	}
}

func TestPool_Load(t *testing.T) {
	t.Parallel()
	p := NewPool(5, true, nil)
	defer p.Destroy()
	for idx := range make([]int, 100000) {
		p.ExecuteASync(nil, &testingJob{
			name: fmt.Sprintf("load.%d", idx),
		})
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestPool_Send_Concurrent(t *testing.T) {
	p := NewPool(1, true, nil)
	defer p.Destroy()
	wg := &sync.WaitGroup{}
	for range make([]int, 5000) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Queue(nil, &testingJob{
				name: "Concurrency_Test",
			}); err != nil {
				t.Fatal(err)
			}
		}()
	}
	wg.Wait()
	e := p.Close()
	if e != nil {
		t.Fatal(e)
	}
	e = p.Wait()
	if e != nil {
		t.Fatal(e)
	}
	if p.jcFinished != 5000 {
		t.Fatal("not enough jobs, wanted 5000 got", p.jcFinished)
	}
}

func TestPool_Kill(t *testing.T) {
	p := NewPool(1, true, nil)
	defer p.Destroy()
	cancelled := make(chan bool)
	p.Start(nil, NewJob(Header("Testing"), func(ctx context.Context) error {
		<-ctx.Done()
		close(cancelled)
		return nil
	}, nil))
	p.Kill()
	select {
	case <-cancelled:
		return
	case <-time.After(2 * time.Second):
		t.Fatal("no job response after 2 seconds")
	}
	e := p.Close()
	if e != nil {
		t.Fatal("expected error, got", e)
	}
	e = p.Wait()
	if e != ErrKilled {
		t.Fatal("expected ErrKilled, got", e)
	}
}

func TestPool_Resize(t *testing.T) {
	p := NewPool(1, true, nil)
	defer p.Destroy()
	p.Resize(10)
	p.Resize(1)
	p.Resize(10)
	p.Resize(1)
	p.Resize(10)
	// Wait for pool to stabilise
	time.Sleep(time.Millisecond)
	s := p.Status()
	if s.Workers.Active != 10 {
		t.Fatal("expected 10 workers, got ", s.Workers.Active)
	}
}

func Example() {
	// Create a Pool with 5 workers and propagation enabled.
	p := NewPool(5, true, FIFOScheduler{})

	// Example JobFn.
	// After 10 seconds the job will print Hello, World! and exit
	JobFn := func(ctx context.Context) error {
		<-time.After(10 * time.Second)
		fmt.Println("Hello, World!")
		return nil
	}
	// Create a Job with an Identifier
	Job := NewJob(
		Header("MyPoolJob"), JobFn, nil,
	)
	// Send it to the Pool.
	// Supplying a context to the pool is optional.
	p.Start(nil, Job)

	// Close the pool after all messages are sent
	p.Close()

	// Wait for the pool to finish
	e := p.Wait()
	if e != nil {
		// Do something with errors here
	}
}

func BenchmarkPool_Execute(b *testing.B) {
	p := NewPool(1, false, FIFOScheduler{})
	defer p.Destroy()
	b.ResetTimer()
	j := &testingJob{
		name: "benchmark",
	}
	for i := 0; i < b.N; i++ {
		e := p.Queue(nil, j)
		if e != nil {
			b.Fatal(e)
		}
	}
	p.Close()
	if err := p.Wait(); err != nil {
		b.Fatal(err)
	}
}
