package gpool

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

var errTestError = errors.New("test error")

var failJob = func(c chan bool) (interface{}, error) {
	return nil, errTestError
}

var goodJob = func(c chan bool) (interface{}, error) {
	time.Sleep(time.Second / 4)
	return nil, nil
}

func Test_Pool_Wait(t *testing.T) {
	p := NewPool(1)
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

func Test_Pool_Jobs(t *testing.T) {
	p := NewPool(1)
	defer p.Destroy()
	p.Send(NewJob(Identifier("Testing"), failJob))
	e := p.Close()
	if e != nil {
		t.Fatal(e)
	}
	e = p.Wait()
	if e == nil {
		t.Fatal("Nil error")
	} else if RealError(e) != errTestError {
		t.Fatalf("wrong error want %#v, got %#v", errTestError, RealError(e))
	}
	t.Log(e)
	if len(p.Jobs(Finished)) > 0 {
		t.Fatal("Job present in Finished jobs")
	}
	if len(p.Jobs(Failed)) != 1 {
		t.Fatal("Job not present in Failed jobs")
	}
}

func Test_Pool_Load(t *testing.T) {
	p := NewPool(runtime.NumCPU())
	defer p.Destroy()
	const wrks = 1000000
	for i := 0; i < wrks; i++ {
		p.Send(NewJob(Identifier("Testing"), func(c chan bool) (interface{}, error) {
			return nil, nil
		}))
	}
	e := p.Close()
	if e != nil {
		t.Fatal(e)
	}
	e = p.Wait()
	if e != nil {
		t.Fatal(e)
	}
	s := p.Jobs(Finished)
	if len(s) != wrks {
		t.Fatal("not enough jobs, wanted ", wrks, " got", len(s))
	}
}

func Test_PoolError(t *testing.T) {
	p := NewPool(2)
	defer p.Destroy()
	for range make([]int, 40) {
		p.Send(NewJob(Identifier("Testing"), failJob))
	}
	p.Close()
	e := p.Wait()
	if err, ok := e.(PoolError); !ok {
		t.Fatal("error is not a pool error")
	} else {
		if err.E != errTestError {
			t.Fatal(err.E)
		}
	}
	if l := len(p.Jobs(Failed)); l != 1 {
		t.Fatal("wanted 1 failed job, got ", l)
	}
}

func Test_Pool_Send_Serial(t *testing.T) {
	p := NewPool(1)
	defer p.Destroy()
	for range make([]int, 20) {
		p.Send(NewJob(Identifier("Testing"), goodJob))
	}
	e := p.Close()
	if e != nil {
		t.Fatal(e)
	}
	e = p.Wait()
	if e != nil {
		t.Fatal(e)
	}
	s := p.Jobs(Finished)
	if len(s) != 20 {
		t.Fatal("not enough jobs, wanted 20 got", len(s))
	}
}

func Test_Pool_Send_Concurrent(t *testing.T) {
	p := NewPool(1)
	defer p.Destroy()
	wg := &sync.WaitGroup{}
	for range make([]int, 20) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Send(NewJob(Identifier("Testing"), goodJob))
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
	s := p.Jobs(Finished)
	if len(s) != 20 {
		t.Fatal("not enough jobs, wanted 20 got", len(s))
	}
}

func Test_Pool_Healthy(t *testing.T) {
	p := NewPool(1)
	defer p.Destroy()
	if ok := p.Healthy(); !ok {
		t.Fatal("pool unexpectedly closed")
	}
	e := p.Close()
	if e != nil {
		t.Fatal(e)
	}
	if ok := p.Healthy(); ok {
		t.Fatal("pool not closed")
	}
}

func Test_Pool_Error(t *testing.T) {
	p := NewPool(1)
	defer p.Destroy()
	e := p.Send(NewJob(Identifier("Testing"), failJob))
	if e != nil {
		t.Fatal(e)
	}
	e = p.Close()
	if e != nil {
		t.Fatal(e)
	}
	e = p.Wait()
	if e == nil {
		t.Fatal("expected error")
	}

	if e := p.Error(); e == nil {
		t.Fatal("no pool error")
	}

}

func Test_Pool_Kill(t *testing.T) {
	p := NewPool(1)
	defer p.Destroy()
	cancelled := make(chan bool)
	p.Send(NewJob(Identifier("Testing"), func(c chan bool) (interface{}, error) {
		<-c
		close(cancelled)
		return nil, nil
	}))
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

//
func Test_Pool_Grow(t *testing.T) {
	p := NewPool(2)
	defer p.Destroy()
	if c := p.Workers(); c != 2 {
		t.Fatal("wanted 2 workers, got", c)
	}
	e := p.Grow(2)
	if e != nil {
		t.Fatal(e)
	}
	if c := p.Workers(); c != 4 {
		t.Fatal("wanted 4 workers, got", c)
	}
	p.Kill()
	p.Wait()
}

//
//func Test_Pool_Shrink_Neg(t *testing.T) {
//	p := NewPool(4)
//	defer p.Destroy()
//	if c, _ := p.mgr.workers(); c != 4 {
//		t.Fatal("wanted 4 workers, got", c)
//	}
//	e := p.Shrink(4)
//	if e != ErrWorkerCount {
//		t.Fatal("wanted ErrWorkerCount, got", e)
//	}
//	p.Kill()
//	p.Wait()
//}
//
//func Test_Pool_Resize(t *testing.T) {
//	p := NewPool(1)
//	defer p.Destroy()
//	e := p.Resize(5)
//	if e != nil {
//		t.Fatal(e)
//	}
//
//	if c, _ := p.mgr.workers(); c != 5 {
//		t.Fatal("worker state incorrect, wanted 5, got", c)
//	}
//	p.Kill()
//	p.Wait()
//}

func Test_Pool_State(t *testing.T) {
	p := NewPool(1)
	defer p.Destroy()
	ok := make(chan bool)

	job := NewJob(Identifier("Testing"), func(c chan bool) (interface{}, error) {
		select {
		case <-ok:
		case <-c:
		}

		return nil, nil
	})
	e := p.Send(job)
	if e != nil {
		t.Fatal(e)
	}

	s := p.Jobs("")

	close(ok)

	if len(s) != 1 {
		t.Fatalf("expected 1 job, got %s", len(s))
	}
	if s[0].State != Executing {
		t.Fatalf("expected Executing, got %s", s[0].State)
	}
	p.Kill()
	p.Wait()
}

//func Test_Pool_NRunning(t *testing.T) {
//	p := NewPool(2)
//	defer p.Destroy()
//	if c, _ := p.mgr.workers(); c != 2 {
//		t.Fatal("wanted 2 workers, got", c)
//	}
//	p.Kill()
//	p.Wait()
//}

func Example() {
	// Create a Pool with 5 workers
	p := NewPool(5)

	// Example PoolJobFn.
	// After 10 seconds the job will return Hello, World!
	JobFn := func(c chan bool) (interface{}, error) {
		<-time.After(10 * time.Second)
		return "Hello, World!", nil
	}
	// Create a Job with an Identifier
	Job := NewJob(
		Identifier("MyPoolJob"), JobFn,
	)
	// Send it to the Pool
	p.Send(Job)

	// Close the pool after all messages are sent
	p.Close()

	// Wait for the pool to finish
	e := p.Wait()
	if e != nil {
		// Do something with errors here
	}

	// Iterate over jobs that have finished and print the output
	for _, j := range p.Jobs(Finished) {
		if s, ok := j.Output.(string); ok {
			fmt.Println(s) // Outputs: Hello, World!
		}
	}
}

func doBenchMarkSubmit(b *testing.B, Workers int, N int) {
	p := NewPool(Workers)
	defer p.Wait()
	j := NewJob(Identifier("Benchmark"), func(c chan bool) (interface{}, error) {
		return nil, nil
	})
	b.ResetTimer()
	for i := 0; i < N; i++ {
		e := p.Send(j)
		if e != nil {
			b.Fatal(e)
		}
	}
	p.Close()
	if err := p.Wait(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkSubmit_1(b *testing.B) {
	doBenchMarkSubmit(b, 1, b.N)
}

func BenchmarkSubmit_10(b *testing.B) {
	doBenchMarkSubmit(b, 10, b.N)
}

func BenchmarkSubmit_100(b *testing.B) {
	doBenchMarkSubmit(b, 100, b.N)
}
