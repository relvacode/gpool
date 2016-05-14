package gpool

import (
	"errors"
	"fmt"
)

var ErrCancelled = errors.New("Cancelled")

// PoolError is an error from a particular Job in the pool
type PoolError struct {
	J PoolJob
	E error
}

func (e PoolError) Error() string {
	return fmt.Sprintf("%s: %s", e.J.Identifier().String(), e.E.Error())
}

func NewPoolError(Job PoolJob, Error error) *PoolError {
	return &PoolError{
		J: Job,
		E: Error,
	}
}
