# gPool - Pool Based Job Execution

[![Build Status](https://travis-ci.org/relvacode/gpool.svg?branch=master)](https://travis-ci.org/relvacode/gpool) [![GoDoc](https://godoc.org/github.com/relvacode/gpool?status.svg)](https://godoc.org/github.com/relvacode/gpool)

_gPool is a lightweight utility for managing a pool of workers._

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
	res, e := p.Wait()
	// e is any error that occured within the Pool
	// res is a slice of JobResults of all completed Jobs
```
### Job
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

#### Cancel
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

#### Result
The output from a job is a `JobResult`. This contains a unique ID, the originating Job and the duration it ran.
Only jobs that did not return an error are returned from `Pool.Wait()`.

```go
p := NewPool(5)

//...Send a few jobs and close the pool

res, e := p.Wait()
// Do something with e here

// Aggregate the total duration of all Jobs
totalDuration := time.Duration(0)
for _, r :- range res {
  totalDuration += r.Duration
}

```

### Hooks
A `Hook` is a function that is executed when a Pool worker starts or stops executing a `Job`. 
Hooks are entirely optional and should not contain any real computation, primarily they should be used for logging.
They are executed by the worker and thus are called concurrently so it's important you don't introduce any race conditions because of shared access.

```go
p := gpool.NewPool(1)
p.Hook.Start = function(ID int, j gpool.Job) {
  log.Println("Started", j.Identifier())
}
```

Hook types that are currently available:

```go
// HookStart is a function to be called when a Job starts.
type HookStart func(ID int,j gpool.Job)

// HookStop is a function to be called when a Job finishes.
type HookStop func(ID int,res gpool.JobResult)
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
p.Hook.Start = func(ID int, j gpool.Job) {
  // Check if the Idenitifer is a CopyIdentifier. If so then print the source and destination.
  if i, ok :=  j.Identifier().(*CopyIdentifier); ok {
    log.Println("Copy", i.Source, "to", i.Dest) // Outputs: Copy /src/file.txt to /dst/file.txt
  }
}

```
