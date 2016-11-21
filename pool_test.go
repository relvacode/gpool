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

func (j *testingJob) Abort(err error) {
	j.aborted = true
}

func (j *testingJob) Run(ctx context.Context) error {
	time.Sleep(j.delay)
	if j.wait != nil {
		select {
		case <-ctx.Done():
			return j.err
		case <-j.wait:
		}
	}
	return j.err
}

func TestPool_Destroy(t *testing.T) {
	p := New(true, NewSimpleBridge(1, FIFOStrategy))
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

func TestPool_ContextValue_JobID(t *testing.T) {
	p := New(false, NewSimpleBridge(1, FIFOStrategy))
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

func TestPool_ContextValue_Pool(t *testing.T) {
	p := New(false, NewSimpleBridge(1, FIFOStrategy))
	defer p.Destroy()

	var ctxPool *Pool
	p.Execute(context.Background(), NewJob(Header("test"), func(ctx context.Context) error {
		ctxPool, _ = PoolFromContext(ctx)
		// Make a request to the pool
		ctxPool.Status()
		return nil
	}, nil))

	if ctxPool == nil {
		t.Fatal("pool not set in context")
	}
	if p != ctxPool {
		t.Fatal("given pool is not the same")
	}
}

func TestPool_ContextCancel(t *testing.T) {
	t.Parallel()
	p := New(false, NewSimpleBridge(1, FIFOStrategy))
	defer p.Destroy()
	ctx, cancel := context.WithCancel(context.Background())

	wait := make(chan bool)
	defer close(wait)

	p.Start(ctx, &testingJob{name: "contextcancel", wait: wait})
	p.Close()
	select {
	case <-p.WaitAsync():
		t.Fatal("pool closed without cancelling context")
	case <-time.After(time.Second):
	}
	cancel()
	select {
	case <-p.WaitAsync():
	case <-time.After(time.Second):
		t.Fatal("cancel did not cancel job")
	}
}

func TestPool_Cancel(t *testing.T) {
	p := New(false, NewSimpleBridge(1, FIFOStrategy))
	defer p.Destroy()

	idCh := make(chan string)

	j := NewJob(Header("cancel"), func(ctx context.Context) error {
		jID, _ := JobIDFromContext(ctx)
		idCh <- jID
		<-ctx.Done()
		return ctx.Err()
	}, nil)

	p.Start(context.Background(), j)
	p.Close()
	jID := <-idCh

	if err := p.Cancel(jID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.WaitAsync():
		return
	case <-time.After(time.Second):
		t.Fatal("job not cancelled")
	}

}

func TestPool_Error(t *testing.T) {
	p := New(true, NewSimpleBridge(1, FIFOStrategy))
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

func TestPool_Hook_Error(t *testing.T) {
	p := New(false, NewSimpleBridge(1, FIFOStrategy))
	defer p.Destroy()

	var err error
	var state ExecutionState

	hook := func(j *JobStatus) {
		err = j.Error
		state = j.State
	}
	p.Hook.Stop = hook

	pErr := errors.New("testing error")

	j := &testingJob{
		name: "TestPool_Error_Propagation",
		err:  pErr,
	}
	if err := p.Execute(nil, j); err != pErr {
		t.Fatal("unexpected error: ", err)
	}
	p.Close()
	p.Wait()
	if err == nil {
		t.Fatal("expected error got nil")
	}
	if err != pErr {
		t.Fatalf("expected 'testing error', got %v", err)
	}
	if state != Failed {
		t.Fatalf("expected 'Failed', got %s", state)
	}
}

func TestPool_Execute_OK(t *testing.T) {
	p := New(true, NewSimpleBridge(1, FIFOStrategy))
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
	p := New(true, NewSimpleBridge(1, FIFOStrategy))
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
	p := New(true, NewSimpleBridge(1, FIFOStrategy))

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
	p := New(true, NewSimpleBridge(1, FIFOStrategy))
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
	p := New(true, NewSimpleBridge(1, FIFOStrategy))
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
	p := New(true, NewSimpleBridge(5, FIFOStrategy))
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
	p := New(true, NewSimpleBridge(1, FIFOStrategy))
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
	p := New(true, NewSimpleBridge(1, FIFOStrategy))
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

func Example() {
	// Create a Pool with 5 workers and propagation enabled.
	p := New(true, NewSimpleBridge(5, FIFOStrategy))

	// Example JobFn.
	// After 10 seconds the job will print 'Hello, World!' and exit unless the context is closed.
	JobFn := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
			fmt.Println("Hello, World!")
		}
		return nil
	}
	// Create a job with a simple string header.
	Job := NewJob(
		Header("MyPoolJob"), JobFn, nil,
	)

	// Send it to the pool.
	p.Start(context.Background(), Job)

	// Close the pool after all jobs are sent.
	p.Close()

	// Wait for the pool to finish.
	err := p.Wait()
	if err != nil {
		// Do something with error here
	}
}

func BenchmarkPool_Execute(b *testing.B) {
	p := New(false, NewSimpleBridge(1, FIFOStrategy))
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
