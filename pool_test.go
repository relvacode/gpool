package gpool

import (
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

func (j *testingJob) Abort() { j.aborted = true }

func (j *testingJob) Run(ctx *WorkContext) error {
	time.Sleep(j.delay)
	if j.wait != nil {
		<-j.wait
	}
	return j.err
}

func TestPool_Kill(t *testing.T) {
	p := NewPool(1, true, nil)
	defer p.Destroy()

	block := make(chan bool)
	j0 := &testingJob{
		name: "blocking",
		wait: block,
	}
	if err := p.Start(j0); err != nil {
		t.Fatal(err)
	}

	j1 := &testingJob{
		name: "waiting job",
	}
	if err := p.Queue(j1); err != nil {
		t.Fatal(err)
	}
	p.Close()

	if err := p.Kill(); err != nil {
		t.Fatal(err)
	}
	close(block)
	p.Wait()

	if !j1.aborted {
		t.Fatal("job not aborted")
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
	if err := p.Start(j); err != nil {
		t.Fatal(p)
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
	if err := p.Execute(j); err != nil {
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
	if err := p.Execute(j); err != nil {
		if err != mkErr {
			t.Fatalf("wanted %s, got %s", mkErr, err)
		}
	} else {
		t.Fatal("expected error")
	}
	p.Close()
}

func TestPool_State(t *testing.T) {
	p := NewPool(1, true, nil)

	ok := make(chan bool)

	j := &testingJob{
		name: "TestPool_State",
		wait: ok,
	}
	if err := p.Start(j); err != nil {
		t.Fatal(err)
	}
	p.Close()
	s := p.State()
	if s.Executing != 1 {
		t.Fatal("wanted 1 executing, got ", s.Executing)
	}
	if s.Queued != 0 {
		t.Fatal("wanted 0 queued, got ", s.Queued)
	}
	if s.Finished != 0 {
		t.Fatal("wanted 0 finished, got ", s.Finished)
	}
	if s.Failed != 0 {
		t.Fatal("wanted 0 failed, got ", s.Failed)
	}
	if s.Error != nil {
		t.Fatal("wanted nil error, got ", s.Error)
	}
	close(ok)
}

func Test_Pool_Wait(t *testing.T) {
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
	p.Hook.Queue = func(*JobState) {
		queued = true
	}
	p.Hook.Start = func(*JobState) {
		started = true
	}
	p.Hook.Stop = func(*JobState) {
		stopped = true
	}

	j := &testingJob{
		name: "TestPool_Hook",
	}
	if err := p.Execute(j); err != nil {
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
	p := NewPool(5, true, nil)
	defer p.Destroy()
	for idx := range make([]int, 100000) {
		p.ExecuteASync(&testingJob{
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

func Test_Pool_Send_Concurrent(t *testing.T) {
	p := NewPool(1, true, nil)
	defer p.Destroy()
	wg := &sync.WaitGroup{}
	for range make([]int, 5000) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Queue(&testingJob{
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

func Test_Pool_Kill(t *testing.T) {
	p := NewPool(1, true, nil)
	defer p.Destroy()
	cancelled := make(chan bool)
	p.Start(NewJob(Header("Testing"), func(ctx *WorkContext) error {
		<-ctx.Cancel
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
	p := NewPool(5, true, FIFOScheduler{})

	// Example JobFn.
	// After 10 seconds the job will print Hello, World! and exit
	JobFn := func(ctx *WorkContext) error {
		<-time.After(10 * time.Second)
		fmt.Println("Hello, World!")
		return nil
	}
	// Create a Job with an Identifier
	Job := NewJob(
		Header("MyPoolJob"), JobFn, nil,
	)
	// Send it to the Pool
	p.Start(Job)

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
		e := p.Queue(j)
		if e != nil {
			b.Fatal(e)
		}
	}
	p.Close()
	if err := p.Wait(); err != nil {
		b.Fatal(err)
	}
}
