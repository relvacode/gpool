package gpool

import (
	"encoding/json"
	"fmt"
)

// PoolError is an error from a particular Job in the pool
type PoolError struct {
	ID string
	J  Job
	E  error
}

func (e PoolError) Error() string {
	return fmt.Sprintf("(%s) %s: %s", e.ID, e.J.Identifier().String(), e.E.Error())
}

func (e PoolError) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.E.Error())
}

func newPoolError(req *jobRequest, Error error) PoolError {
	return PoolError{
		ID: req.ID,
		J:  req.Job,
		E:  Error,
	}
}

// RealError tries to return the underlying error from a PoolError.
// If the supplied error is not PoolError then the original error is returned.
func RealError(e error) error {
	if v, ok := e.(PoolError); ok {
		return v.E
	}
	return e
}
