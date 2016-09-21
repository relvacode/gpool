[![Build Status](https://travis-ci.org/relvacode/gpool.svg?branch=master)](https://travis-ci.org/relvacode/gpool) [![GoDoc](https://godoc.org/github.com/relvacode/gpool?status.svg)](https://godoc.org/github.com/relvacode/gpool)
[![Go Report Card](https://goreportcard.com/badge/github.com/relvacode/gpool)](https://goreportcard.com/report/github.com/relvacode/gpool)

gpool is a toolkit for concurrent execution of jobs.

`import "github.com/relvacode/gpool"`

# Features

  * Lock-free and concurrency safe
  * Context based execution
  * Queuing
  * Custom schedulers and executors
  * Optional error propagation
  * Zero import dependencies
  * Hooks

# Usage

Create a new pool with 5 workers and error propagation enabled.

```go
p := NewPool(5, true)
```

Create a `Job` interface or use `NewJob()` to make a `Job` from a function.

```go
// The header of our job
h := gpool.Header("MyJob")

// The function to be executed
fn := func(ctx context.Context) error {
    select {
        case <-ctx.Done():
          return ctx.Err()
        case <-time.After(10 * time.Second):
          fmt.Println("Hello, World!")
          return nil
    }
}

// Put then together into a job.
j := NewJob(h, fn, nil)
```

Create a context to be used in the execution of the job.
For now we don't need it to do anything so just use `context.Background()`.

```go
ctx := context.Background()
```

There are a few ways to submit a `Job` to a `Pool`.

```go
// Begin queueing the Job and return
p.Queue(ctx, j)
```

```go
// Wait for the Job to start executing and return
p.Start(ctx, j)
```

```go
// Wait for the Job to finish and return the error
p.Execute(ctx, j)
```

Finally, when done make sure you call `Pool.Close()` to close the pool and `Pool.Wait()` to wait for all jobs to finish and workers to exit.

```go
p.Close()
p.Wait()
```

## Scheduler 

A scheduler is an interface which selects the next job in the queue to be executed.

Custom schedulers can be useful to only execute jobs based on arbitrary conditions.

A good example of this is storage allocation, a job may expose the amount of the capacity required 
and the scheduler can pick the most appropriate job to start next based on it's own capacity tracking.

Because the scheduler has full control over when jobs get executed you can also use it to block execution of any or all jobs.

### Pre-made Schedulers

  * `FIFOScheduler` Execute jobs in a first-in, first-out order.
  * `LIFOScheduler` Execute jbos in a last-in, first-out order.
  * `GatedScheduler` Extends an exiting scheduler to open or close further execution of jobs.

## Bridge

A bridge is an interface which mediates execution of jobs.

The most basic bridge is `StaticBridge` which simply has a preset amount of concurrent workers.

Using a custom bridge allows you to have finer control over _how_ jobs get executed.

An example of this might be a Docker based pool, each "worker" supplies the job it is executing with the access details for the Docker node it controls via the job's context.