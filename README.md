# gPool - Pool Based Job Execution

[![Build Status](https://travis-ci.org/relvacode/gpool.svg?branch=master)](https://travis-ci.org/relvacode/gpool) [![GoDoc](https://godoc.org/github.com/relvacode/gpool?status.svg)](https://godoc.org/github.com/relvacode/gpool)
[![Go Report Card](https://goreportcard.com/badge/github.com/relvacode/gpool)](https://goreportcard.com/report/github.com/relvacode/gpool)

_gPool is an execution engine for a pool of workers_

`import "github.com/relvacode/gpool"`

# Features

  * Zero import dependencies
  * Optional error propagation
  * Dynamic resizing
  * Queuing, with custom schedulers
  * Lock-free
  * Hooks
  * Cancellable execution

# Usage

Create a new pool with 5 workers, error propagation enabled and using the default `FIFOScheduler` scheduler.

```go
p := NewPool(5, true, nil)
```

Create your `Job` interface

```go
type Job interface {
	// An identity header that implements String()
	Header() fmt.Stringer

	// Run the Job.
	// If propagation is enabled on the Pool then the error returned it is propagated up and the Pool is killed.
	Run(*WorkContext) error

	// Abort is used for when a Job is in the queue and needs to be removed (via call to Pool.Kill() for example).
	// Abort is never called if the Job is already in a starting state, if it is then the Cancel channel of the
	// WorkContext is used instead.
	// Abort is called before the requesting ticket (Pool.Execute, Pool.Submit) is signalled.
	Abort()
}
```

There are a few ways to submit a `Job` to the `Pool`

```go
j := new(MyJob)

// Begin queueing the Job
p.Queue(j)

// Wait for the Job to start executing
p.Start(j)

// Wait for the Job to finish and return the error
p.Execute(j)
```

Finally, when done make sure you call `Pool.Close()` to close the pool.
You can use `Pool.Wait()` to wait for all jobs to finish.

```go
p.Close()
p.Wait()
```
