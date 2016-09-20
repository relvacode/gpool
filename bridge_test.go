package gpool

import (
	"context"
	"testing"
)

func TestStaticBridge_work(t *testing.T) {
	p := NewCustomPool(false, FIFOScheduler{}, NewStaticBridge(1))
	defer p.Destroy()

	send := make(chan struct{})

	p.Execute(context.Background(), NewJob(
		Header("test"),
		func(context.Context) error {
			close(send)
			return nil
		},
		nil,
	))

	select {
	case <-send:
	default:
		t.Fatal("did not execute job")
	}
}
