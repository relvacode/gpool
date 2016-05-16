package gpool

import (
	"fmt"
	"testing"
	"time"
)

var failJob = func(c chan struct{}) (interface{}, error) {
	return nil, fmt.Errorf("Test error function")
}

var goodJob = func(c chan struct{}) (interface{}, error) {
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
	p := NewPool(3)
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

func Test_Pool_IsOpen(t *testing.T) {
	p := NewPool(1)
	if ok := p.IsOpen(); !ok {
		t.Fatal("pool unexpectedly closed")
	}
	e := p.Close()
	if e != nil {
		t.Fatal(e)
	}
	if ok := p.IsOpen(); ok {
		t.Fatal("pool not closed")
	}
}

func Test_Pool_NRunning(t *testing.T) {
	p := NewPool(2)
	if c, _ := p.Running(); c != 2 {
		t.Fatal("wanted 2 workers, got", c)
	}
}

func Example() {
	// Create a Pool with 5 workers
	p := NewPool(5)

	// Example PoolJobFn.
	// After 10 seconds the job will return Hello, World!
	JobFn := func(c chan struct{}) (interface{}, error) {
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
	p.Wait()
}

func doBenchMarkSubmit(b *testing.B, Workers int, N int) {
	p := NewPool(Workers)
	j := NewJob(Identifier("Benchmark"), func(c chan struct{}) (interface{}, error) {
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
