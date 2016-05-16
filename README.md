# gPool - Pool Based Job Execution

[![Build Status](https://travis-ci.org/relvacode/gpool.svg?branch=master)](https://travis-ci.org/relvacode/gpool)

_gPool is a lightweight utility for managing a pool of workers._

`import "github.com/relvacode/gpool"`

## Basic Usage
```go
	// Create a Pool with 5 workers.
	// Workers are started on creation
	p := gpool.NewPool(5)

	// Example JobFn.
	// After 10 seconds the job will return Hello, World!
	JobFn := func(c chan struct{}) (interface{}, error) {
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
	jobs, e := p.Wait()
	// e is any error that occured within the Pool
	// jobs is a slice of JobResults of all completed Jobs
```
### Job
A Job is a task to execute on the Pool that contains an identifer and a execution function. It can be any interface that satisfies `gpool.Job` but can also be used with `gpool.NewJob()`.

```go
// Create an Identifer, this in an interface{} which has a String() string method. 
// You can use the built in pool.Identifier() for simple strings
i := gpool.Identifier("MyTestJob")

// Create the Job execution function.
// The job can optionally return an output interface{} and any errors as a result of execution.
// The c chan will be closed when the Pool is killed, or another job returns a non-nil error.
fn := func(c chan struct{}) (interface{}, error) {
  return "Hello, World!", nil
}

// Finally, create a Job that can be submitted to the pool.
Job := gpool.NewJob(i, fn)
```

#### Cancel
Jobs can be cancelled during execution via a channel closer.
The execution function is given a channel which will be closed when a call to `Pool.Kill()` is made or another Job in the pool returns an error.
A call to `Pool.Wait()` will not continue until Jobs in the Pool have completed, so it's important for especially long-running Jobs to be able to receive and action this signal in reasonable time.

```go
fn := func(c chan struct{}) (interface{}, error) {
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

### Hooks
A Hook is a function that is executed when a Job changes state in the Pool. 
Hooks are entirely optional and should not contain any real computation, primarily they should be used for logging.

```go
p := gpool.NewPool(1)
p.Hook.Add = function(j gpool.Job) {
  log.Println("Started", j.Identifier())
}
```

### Identifier
An Identifier is a interface{} that has a `String() string` method. This is used to give Jobs a name but can also contain more advanced information via type assertion.

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
fn := func(c chan struct{}) (interface{}, error) {
  return "Hello, World!", nil
}

// Initialise your custom Identifier with some data
i := &CopyIdentifier{
  Name:   "Copy file.txt",
  Source: "/src/file.txt",
  Dest:   "/dst/file.txt",
}
Job := gpool.NewJob(i, fn)

p := gpool.NewPool(5)

// Add a Pool Hook
p.Hook.Add = func(j gpool.Job) {
  // Check if the Idenitifer is a *CopyIdentifier. If so then print the source and destination.
  if i, ok :=  j.Identifier().(*CopyIdentifier); ok {
    log.Println("Copy", i.Source, "to", i.Dest) // Outputs: Copy /src/file.txt to /dst/file.txt
  }
}

```
