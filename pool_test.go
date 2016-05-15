package gpool

import (
	"fmt"
	"testing"
	"time"
)

func Test_Pool_JobError(t *testing.T) {
	p := NewPool(1)
	p.Send(NewPoolJob(Identifier("Testing"),
		func(c chan struct{}) (interface{}, error) {
			return nil, fmt.Errorf("Test error function")
		},
	))
	j, e := p.Wait()
	if e == nil {
		t.Fatal("Nil error")
	}
	t.Log(e)
	if len(j) > 0 {
		t.Fatal("Job present in completed jobs")
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
	Job := NewPoolJob(
		Identifier("MyPoolJob"), JobFn,
	)
	// Send it to the Pool
	p.Send(Job)

	// Close the pool after all messages are sent
	p.Close()

	// Wait for the pool to finished
	p.Wait()
}
