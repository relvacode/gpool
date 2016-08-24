package gpool

import "testing"

func testUUIDUniqueness(N int, get func() string, t *testing.T) {
	track := map[string]bool{}
	for i := 0; i < N; i++ {
		uid := get()
		if _, ok := track[uid]; ok {
			t.Fatal("uuid collision: ", uid)
		}
		track[uid] = true
	}
}

func TestUUIDUnique(t *testing.T) {
	N := 10000
	testUUIDUniqueness(N, uuid, t)
}

func TestUUIDConcurrent(t *testing.T) {
	N := 100
	block := make(chan bool)
	recv := make(chan string, N)

	for i := 0; i < N; i++ {
		go func() {
			<-block
			recv <- uuid()
		}()
	}
	close(block)
	testUUIDUniqueness(N, func() string { return <-recv }, t)
}

func Benchmark_uuid(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuid()
	}
}
