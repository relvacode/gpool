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

func Test_JobResultID(t *testing.T) {
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
