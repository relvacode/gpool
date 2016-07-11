# gPool - Pool Based Job Execution

[![Build Status](https://travis-ci.org/relvacode/gpool.svg?branch=master)](https://travis-ci.org/relvacode/gpool) [![GoDoc](https://godoc.org/github.com/relvacode/gpool?status.svg)](https://godoc.org/github.com/relvacode/gpool)

_gPool is a thread safe queued worker pool implementation_

`import "github.com/relvacode/gpool"`

## Basic Usage

```go
	// Create a Pool with 5 workers.
	// Workers are started on creation
	p := gpool.NewPool(5)

	// Example JobFn.
	// After 10 seconds the job will return 'Hello, World!'
	JobFn := func(c chan bool) (interface{}, error) {
		<-time.After(10 * time.Second)
		return "Hello, World!", nil
	}
	
	// Create a Job with an Identifier
	Job := gpool.NewJob(
		Identifier("MyPoolJob"), JobFn,
	)
	
	// Send it to the Pool
	p.Send(Job)

	// Close the pool after all messages are sent
	p.Close()

	// Wait for the pool to finish. This will block forever if the pool is not closed.
	e := p.Wait()
	// e is any error that occured within the Pool
	
	// Get all jobs that have finished executing
	jobs := p.Jobs(gpool.Finished)
```

## Job
A `Job` is a task to execute on the `Pool` that satisfies `gpool.Job`. `gpool.NewJob()` creates a interface{} that contains an `Identifier` and a execution function `JobFn` which can then be submitted to the pool.

```go
// Create an Identifer, this in an interface{} which has a String() string method. 
// You can use the built in pool.Identifier() for simple strings
i := gpool.Identifier("MyTestJob")

// Create the Job execution function.
// The job can optionally return an output interface{} and any errors as a result of execution.
// The c chan will be closed when the Pool is killed, or another job returns a non-nil error.
fn := func(c chan bool) (interface{}, error) {
  return "Hello, World!", nil
}

// Finally, create a Job that can be submitted to the pool.
Job := gpool.NewJob(i, fn)
```

### Cancel
The execution `JobFn` function supplied in `NewJob()` is given a channel which will be closed when a call to `Pool.Kill()` is made or another `Job` in the `Pool` returns an error. This is called via `Job.Cancel()`.
A call to `Pool.Wait()` will not continue until all currently active Jobs in the Pool have completed, so it's important for especially long-running jobs to be able to receive and action this signal in reasonable time.

```go
fn := func(c chan bool) (interface{}, error) {
  select {
  case <-c:
    // Cancel signal received, exit without doing anything
    return nil, nil
  case <-time.After(10*time.Second):
    // After a 10 second timeout if no cancel signal is received then return "Hello, World!"
    return "Hello, World!", nil
  }
}
```

### Identifier
An Identifier is a `interface{}` that has a `String() string` method. This is used to give jobs a name but can also contain more advanced information via type assertion.

```go
// Create you custom Identifer struct{}
type CopyIdentifier struct {
  Name string
  Source, Dest string
}

// Implement String() string
func (c *CopyIdentifier) String() string {
  return c.Name
}

// Example Job
fn := func(c chan bool) (interface{}, error) {
  return "Hello, World!", nil
}

// Initialise your custom Identifier with some data
i := CopyIdentifier{
  Name:   "Copy file.txt",
  Source: "/src/file.txt",
  Dest:   "/dst/file.txt",
}
Job := gpool.NewJob(i, fn)

p := gpool.NewPool(5)

// Set a Pool Hook
p.Hook.Start = func(j gpool.JobState) {
  // Check if the Idenitifer is a CopyIdentifier. If so then print the source and destination.
  if i, ok :=  j.Identifier.(*CopyIdentifier); ok {
    log.Println("Copy", i.Source, "to", i.Dest) // Outputs: Copy /src/file.txt to /dst/file.txt
  }
}

```

## Bus

The bus is the central communication loop which mediates pool input requests in a thread safe way.
All pool requests are sent to the bus and then resolved instantly with the exception of `Pool.Wait()`, `Pool.Send()` and `Pool.Destroy` which are queued internally until they can be resolved.

## State

The current state of the pool can be inspected via the methods `Pool.Jobs(State string)`, `Pool.Workers()` and `Pool.State()`.
State requests are not fulfilled by the bus, instead they are managed by the internal worker state manager and protected by mutex lock.

`Pool.Jobs(State string)` returns all jobs with the specified state (e.g: `gpool.Executing`).

`Pool.Workers()` returns the current amount of running workers in the pool.

`Pool.State()` returns a snapshot of the pool state in a convenient JSON encodable manner:

	 {
	 	"Jobs": [{
	 		"ID": 1,
	 		"Identifier": "Testing",
	 		"State": "Executing",
	 		"Output": null,
	 		"Duration": 0,
	 		"Error": null
	 	}],
	 	"AvailableWorkers": 1,
	 	"ExecutionDuration": 0
	 }



## Hooks
A `Hook` `func(gpool.JobState)` is a function that is executed when a Pool worker starts or stops executing a `Job`. 
Hooks are entirely optional and should not contain any real computation, primarily they should be used for logging.
They are executed by the worker and thus are called concurrently so it's important you don't introduce any race conditions because of shared access.

```go
p := gpool.NewPool(1)
p.Hook.Start = function(j gpool.JobState) {
  log.Println("Started", j.Identifier)
}
```
