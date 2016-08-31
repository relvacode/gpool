[![Build Status](https://travis-ci.org/relvacode/gpool.svg?branch=master)](https://travis-ci.org/relvacode/gpool) [![GoDoc](https://godoc.org/github.com/relvacode/gpool?status.svg)](https://godoc.org/github.com/relvacode/gpool)
[![Go Report Card](https://goreportcard.com/badge/github.com/relvacode/gpool)](https://goreportcard.com/report/github.com/relvacode/gpool)

gpool is an execution engine for a pool of concurrent workers

`import "github.com/relvacode/gpool"`

# Features

  * Lock-free and concurrency safe
  * Context based execution
  * Queuing
  * Custom schedulers
  * Optional error propagation
  * Dynamic resizing
  * Zero import dependencies
  * Hooks

# Usage

Create a new pool with 5 workers, error propagation enabled and using the default `FIFOScheduler` scheduler.

```go
p := NewPool(5, true, nil)
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
