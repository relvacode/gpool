package gpool

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type testingJob struct {
	name  string
	delay time.Duration
	wait  chan bool
	err   error
}

func (j testingJob) Header() fmt.Stringer {
	return Header(j.name)
}

func (testingJob) Abort() {}

func (j testingJob) Run(ctx *WorkContext) error {
	time.Sleep(j.delay)
	if j.wait != nil {
		<-j.wait
	}
	return j.err
}

func TestPool_Execute_OK(t *testing.T) {
	p := NewPool(1, true)
	defer p.Destroy()
	j := &testingJob{
		name: "TestPool_Execute_OK",
	}
	if err := p.Execute(j); err != nil {
		t.Fatal(err)
	}
	p.Close()
}

func TestPool_Execute_Error(t *testing.T) {
	p := NewPool(1, true)
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
	p := NewPool(1, true)

	ok := make(chan bool)

	j := &testingJob{
		name: "TestPool_State",
		wait: ok,
	}
	if err := p.Submit(j); err != nil {
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
	p := NewPool(1, true)
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
	p := NewPool(1, true)

	var queued, started, stopped bool
	p.Hook.Queue = func(*State) {
		queued = true
	}
	p.Hook.Start = func(*State) {
		started = true
	}
	p.Hook.Stop = func(*State) {
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

//var errTestError = errors.New("test error")
//
//var failJob = func(ctx *WorkContext) (interface{}, error) {
//	return nil, errTestError
//}
//
//var goodJob = func(ctx *WorkContext) (interface{}, error) {
//	time.Sleep(time.Second / 4)
//	return nil, nil
//}
//

//
//func Test_Propagated_Pool_Jobs(t *testing.T) {
//	p := NewPool(1)
//	defer p.Destroy()
//	p.Submit(NewJob(Identifier("Testing"), failJob))
//	e := p.Close()
//	if e != nil {
//		t.Fatal(e)
//	}
//	e = p.Wait()
//	if e == nil {
//		t.Fatal("Nil error")
//	} else if RealError(e) != errTestError {
//		t.Fatalf("wrong error want %#v, got %#v", errTestError, RealError(e))
//	}
//	t.Log(e)
//	if p.jcFinished > 0 {
//		t.Fatal("Job present in Finished jobs")
//	}
//	if p.jcFailed != 1 {
//		t.Fatal("Job not present in Failed jobs")
//	}
//}
//
//func Test_NonPropagated_Pool_Jobs(t *testing.T) {
//	p := NewNonPropagatingPool(1)
//	defer p.Destroy()
//	p.Submit(NewJob(Identifier("Testing"), failJob))
//	e := p.Close()
//	if e != nil {
//		t.Fatal(e)
//	}
//	e = p.Wait()
//	if e != nil {
//		t.Fatal("unexpected error")
//	}
//	if p.jcFinished > 0 {
//		t.Fatal("Job present in Finished jobs")
//	}
//	if p.jcFinished != 1 {
//		t.Fatal("Job not present in Failed jobs")
//	}
//}
//
//func Test_Pool_Load(t *testing.T) {
//	p := NewPool(runtime.NumCPU())
//	defer p.Destroy()
//	const wrks = 1000000
//	for i := 0; i < wrks; i++ {
//		p.Submit(NewJob(Identifier("Testing"), func(ctx *WorkContext) (interface{}, error) {
//			return nil, nil
//		}))
//	}
//	e := p.Close()
//	if e != nil {
//		t.Fatal(e)
//	}
//	e = p.Wait()
//	if e != nil {
//		t.Fatal(e)
//	}
//	if p.jcFinished != wrks {
//		t.Fatal("not enough jobs, wanted ", wrks, " got", p.jcFinished)
//	}
//}
//
//func Test_PoolError(t *testing.T) {
//	p := NewPool(2)
//	defer p.Destroy()
//	p.Submit(NewJob(Identifier("Testing"), failJob))
//	p.Close()
//	e := p.Wait()
//	if err, ok := e.(PoolError); !ok {
//		t.Fatal("error is not a pool error")
//	} else {
//		if err.E != errTestError {
//			t.Fatal(err.E)
//		}
//	}
//	if l := p.jcFailed; l != 1 {
//		t.Fatal("wanted 1 failed job, got ", l)
//	}
//}
//
//func Test_Pool_Send_Serial(t *testing.T) {
//	p := NewPool(1)
//	defer p.Destroy()
//	for range make([]int, 20) {
//		p.Submit(NewJob(Identifier("Testing"), goodJob))
//	}
//	e := p.Close()
//	if e != nil {
//		t.Fatal(e)
//	}
//	e = p.Wait()
//	if e != nil {
//		t.Fatal(e)
//	}
//	if p.jcFinished != 20 {
//		t.Fatal("not enough jobs, wanted 20 got", p.jcFinished)
//	}
//}
//
func Test_Pool_Send_Concurrent(t *testing.T) {
	p := NewPool(1, true)
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

//
//func Test_Pool_Healthy(t *testing.T) {
//	p := NewPool(1)
//	defer p.Destroy()
//	if ok := p.Healthy(); !ok {
//		t.Fatal("pool unexpectedly closed")
//	}
//	e := p.Close()
//	if e != nil {
//		t.Fatal(e)
//	}
//	if ok := p.Healthy(); ok {
//		t.Fatal("pool not closed")
//	}
//}
//
//func Test_Pool_Error(t *testing.T) {
//	p := NewPool(1)
//	defer p.Destroy()
//	e := p.Submit(NewJob(Identifier("Testing"), failJob))
//	if e != nil {
//		t.Fatal(e)
//	}
//	e = p.Close()
//	if e != nil {
//		t.Fatal(e)
//	}
//	e = p.Wait()
//	if e == nil {
//		t.Fatal("expected error")
//	}
//
//	if e := p.Error(); e == nil {
//		t.Fatal("no pool error")
//	}
//
//}
//
func Test_Pool_Kill(t *testing.T) {
	p := NewPool(1, true)
	defer p.Destroy()
	cancelled := make(chan bool)
	p.Submit(NewJob(Header("Testing"), func(ctx *WorkContext) error {
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
	p := NewPool(5, true)

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
	p.Submit(Job)

	// Close the pool after all messages are sent
	p.Close()

	// Wait for the pool to finish
	e := p.Wait()
	if e != nil {
		// Do something with errors here
	}
}

//func doBenchMarkSubmit(b *testing.B, Workers int, N int) {
//	p := NewPool(Workers)
//	defer p.Wait()
//	j := NewJob(Identifier("Benchmark"), func(c chan bool) (interface{}, error) {
//		return nil, nil
//	})
//	b.ResetTimer()
//	for i := 0; i < N; i++ {
//		e := p.Submit(j)
//		if e != nil {
//			b.Fatal(e)
//		}
//	}
//	p.Close()
//	if err := p.Wait(); err != nil {
//		b.Fatal(err)
//	}
//}
//
//func BenchmarkSubmit_1(b *testing.B) {
//	doBenchMarkSubmit(b, 1, b.N)
//}
//
//func BenchmarkSubmit_10(b *testing.B) {
//	doBenchMarkSubmit(b, 10, b.N)
//}
//
//func BenchmarkSubmit_100(b *testing.B) {
//	doBenchMarkSubmit(b, 100, b.N)
//}
