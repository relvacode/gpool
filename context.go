package gpool

import "context"

type contextKeyType int

// JobIDKey is the key used to store the Job ID in a run context.
var JobIDKey contextKeyType = 1

// PoolKey is the key used to store the Pool in a run context.
// This can be used to make pool operations from within a Job execution.
var PoolKey contextKeyType = 2

// JobIDFromContext retrieves the state ID from a given context and whether an ID exists within that context.
func JobIDFromContext(ctx context.Context) (string, bool) {
	id := ctx.Value(JobIDKey)
	if id == nil {
		return "", false
	}
	str, ok := id.(string)
	return str, ok
}

// PoolFromContext returns the Pool attached to this context and whether a Pool exists within that context.
// Care should be taken when working on Pool operations from inside the execution of a Job.
// For example, a pool with only 1 worker cannot begin executing another Job from inside the current Job.
func PoolFromContext(ctx context.Context) (*Pool, bool) {
	inf := ctx.Value(PoolKey)
	if inf == nil {
		return nil, false
	}
	p, ok := inf.(*Pool)
	return p, ok
}
