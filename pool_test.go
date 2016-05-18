package gpool

import (
	"fmt"
	"testing"
	"time"
	"sync"
)

var failJob = func(c chan bool) (interface{}, error) {
	return nil, fmt.Errorf("Test error function")
}

var goodJob = func(c chan bool) (interface{}, error) {
	time.Sleep(time.Second / 4)
	return nil, nil
}

func Test_Pool_JobError(t *testing.T) {
	p := NewPool(1)
	p.Send(NewJob(Identifier("Testing"), failJob))
	e := p.Close()
	if e != nil {
		t.Fatal(e)
	}
	j, e := p.Wait()
	if e == nil {
		t.Fatal("Nil error")
	}
	t.Log(e)
	if len(j) > 0 {
		t.Fatal("Job present in completed jobs")
	}
}

func Test_Pool_JobResultID(t *testing.T) {
	p := NewPool(1)
	p.Send(NewJob(Identifier("Testing"), goodJob))
	p.Send(NewJob(Identifier("Testing"), goodJob))
	e := p.Close()
	if e != nil {
		t.Fatal(e)
	}
	j, e := p.Wait()
	if e != nil {
		t.Fatal(e)
	}
	if len(j) != 2 {
		t.Fatal("not enough jobs, wanted 2 got", len(j))
	}
	for i, v := range j {
		if i+1 != v.ID {
			t.Fatal("wrong ID, wanted", i+1, "got", v.ID)
		}
	}
}

func Test_Pool_JobMany_40(t *testing.T) {
	p := NewPool(10)
	for range make([]int, 40) {
		p.Send(NewJob(Identifier("Testing"), goodJob))
	}
	e := p.Close()
	if e != nil {
		t.Fatal(e)
	}
	j, e := p.Wait()
	if e != nil {
		t.Fatal(e)
	}
	if len(j) != 40 {
		t.Fatal("not enough jobs, wanted 40 got", len(j))
	}
}

func Test_Pool_JobMany_1(t *testing.T) {
	p := NewPool(1)
	for range make([]int, 20) {
		p.Send(NewJob(Identifier("Testing"), goodJob))
	}
	e := p.Close()
	if e != nil {
		t.Fatal(e)
	}
	j, e := p.Wait()
	if e != nil {
		t.Fatal(e)
	}
	if len(j) != 20 {
		t.Fatal("not enough jobs, wanted 20 got", len(j))
	}
}

func Test_Pool_JobMany_Concurrent(t *testing.T) {
	p := NewPool(1)
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
	j, e := p.Wait()
	if e != nil {
		t.Fatal(e)
	}
	if len(j) != 20 {
		t.Fatal("not enough jobs, wanted 20 got", len(j))
	}
}

func Test_Pool_Healthy(t *testing.T) {
	p := NewPool(1)
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
	e := p.Send(NewJob(Identifier("Testing"), failJob))
	if e != nil {
		t.Fatal(e)
	}
	e = p.Close()
	if e != nil {
		t.Fatal(e)
	}
	_, e = p.Wait()
	if e == nil {
		t.Fatal("expected error")
	}

	if e := p.Error(); e == nil {
		t.Fatal("no pool error")
	}

}

func Test_Pool_Kill(t *testing.T) {
	p := NewPool(1)
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
	_, e = p.Wait()
	if e != ErrKilled {
		t.Fatal("expected ErrKilled, got", e)
	}
}

func Test_Pool_Grow(t *testing.T) {
	p := NewPool(2)
	if c, _ := p.WorkerState(); c != 2 {
		t.Fatal("wanted 2 workers, got", c)
	}
	e := p.Grow(2)
	if e != nil {
		t.Fatal(e)
	}
	if c, _ := p.WorkerState(); c != 4 {
		t.Fatal("wanted 4 workers, got", c)
	}
	p.Kill()
	p.Wait()
}

func Test_Pool_JobState(t *testing.T) {
	p := NewPool(1)
	ok := make(chan bool)
	job := NewJob(Identifier("Testing"), func(c chan bool) (interface{}, error) {
		<-ok
		return nil, nil
	})
	e := p.Send(job)
	if e != nil {
		t.Fatal(e)
	}
	if c, _ := p.JobState(); c != 1 {
		t.Fatal("wanted 1 running jobs, got", c)
	}
	close(ok)
	p.Kill()
	p.Wait()
}

func Test_Pool_NRunning(t *testing.T) {
	p := NewPool(2)
	if c, _ := p.WorkerState(); c != 2 {
		t.Fatal("wanted 2 workers, got", c)
	}
	p.Kill()
	p.Wait()
}

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
	jobs, e := p.Wait()
	if e != nil {
		// Do something with errors here
	}

	// Iterate over the completed jobs and print the output
	for _, j := range jobs {
		o := j.Job.Output()
		if s, ok := o.(string); ok {
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
	_, e := p.Wait()
	if e != nil {
		b.Fatal(e)
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
