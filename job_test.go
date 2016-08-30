package gpool

import "testing"

func TestJob_Abort(t *testing.T) {
	p := NewPool(1, true, nil)
	defer p.Destroy()

	block := make(chan bool)
	j0 := &testingJob{
		name: "blocking",
		wait: block,
	}
	if err := p.Start(nil, j0); err != nil {
		t.Fatal(err)
	}

	j1 := &testingJob{
		name: "waiting job",
	}
	if err := p.Queue(nil, j1); err != nil {
		t.Fatal(err)
	}
	p.Close()

	if err := p.Kill(); err != nil {
		t.Fatal(err)
	}
	close(block)
	p.Wait()

	if !j1.aborted {
		t.Fatal("job not aborted")
	}
}
