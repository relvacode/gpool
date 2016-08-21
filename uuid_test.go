package gpool

import "testing"

func test_uuid_uniqueness(N int, get func() string, t *testing.T) {
	track := map[string]bool{}
	for i := 0; i < N; i++ {
		uid := get()
		if _, ok := track[uid]; ok {
			t.Fatal("uuid collision: ", uid)
		}
	}
}

func Test_uuid_Unique(t *testing.T) {
	N := 10000
	test_uuid_uniqueness(N, uuid, t)
}

func Test_uuid_Concurrent(t *testing.T) {
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
	test_uuid_uniqueness(N, func() string { return <-recv }, t)
}

func Benchmark_uuid(b *testing.B) {
	for i := 0; i < b.N; i++ {
		uuid()
	}
}
