package gpool

import "context"

type jobIDKeyType int

// JobIDKey is the key used to store the Job ID in a run context.
var JobIDKey jobIDKeyType = 1

// ContextJobID retrieves the Job ID from a given context and whether a Job ID exists within that context.
func ContextJobID(ctx context.Context) (string, bool) {
	id := ctx.Value(JobIDKey)
	if id == nil {
		return "", false
	}
	str, ok := id.(string)
	return str, ok
}
