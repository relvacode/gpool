package gpool

import (
	"fmt"
)

// PoolError is an error from a particular Job in the pool
type PoolError struct {
	ID int
	J  Job
	E  error
}

func (e PoolError) Error() string {
	return fmt.Sprintf("%s@%d: %s", e.J.Identifier().String(), e.ID, e.E.Error())
}

func newPoolError(req *jobRequest, Error error) *PoolError {
	return &PoolError{
		ID: req.ID,
		J:  req.Job,
		E:  Error,
	}
}
