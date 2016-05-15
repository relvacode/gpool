package gpool

import (
	"fmt"
	"testing"
)

func Test_Pool_JobError(t *testing.T) {
	p := NewPool(1)
	p.Send(NewPoolJob(Identifier("Testing"),
		func(c chan struct{}) (interface{}, error) {
			return nil, fmt.Errorf("Test error function")
		},
	))
	j, e := p.Wait()
	if e == nil {
		t.Fatal("Nil error")
	}
	t.Log(e)
	if len(j) > 0 {
		t.Fatal("Job present in completed jobs")
	}
}
