package engine

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"testing"
	"time"
)

func benchOpts(b *testing.B, sync SyncMode) Options {
	return Options{
		Dir:                b.TempDir(),
		MaxFileSize:        256 << 20,
		SyncMode:           sync,
		SyncEvery:          200 * time.Millisecond,
		CompactionInterval: time.Hour,
		Logger:             log.New(io.Discard, "", 0),
	}
}

func openBench(b *testing.B, sync SyncMode) *Store {
	s, err := Open(benchOpts(b, sync))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

// BenchmarkPutRandom is the headline case from the brief: a stream of items
// arriving in random key order.
func BenchmarkPutRandom(b *testing.B) {
	s := openBench(b, SyncInterval)
	value := make([]byte, 128)
	r := rand.New(rand.NewSource(1))
	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("key%09d", r.Intn(1<<24))
		if err := s.Put([]byte(k), value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutRandomSyncAlways(b *testing.B) {
	s := openBench(b, SyncAlways)
	value := make([]byte, 128)
	r := rand.New(rand.NewSource(1))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("key%09d", r.Intn(1<<24))
		if err := s.Put([]byte(k), value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchPut100(b *testing.B) {
	s := openBench(b, SyncInterval)
	value := make([]byte, 128)
	keys := make([][]byte, 100)
	vals := make([][]byte, 100)
	for i := range vals {
		vals[i] = value
	}
	b.SetBytes(int64(100 * len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range keys {
			keys[j] = []byte(fmt.Sprintf("key%09d", i*100+j))
		}
		if err := s.BatchPut(keys, vals); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetRandom(b *testing.B) {
	s := openBench(b, SyncInterval)
	const n = 200000
	value := make([]byte, 128)
	for i := 0; i < n; i++ {
		if err := s.Put([]byte(fmt.Sprintf("key%09d", i)), value); err != nil {
			b.Fatal(err)
		}
	}
	r := rand.New(rand.NewSource(2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Get([]byte(fmt.Sprintf("key%09d", r.Intn(n)))); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetParallel shows what the engine does when readers are the
// bottleneck rather than the network.
func BenchmarkGetParallel(b *testing.B) {
	s := openBench(b, SyncInterval)
	const n = 200000
	value := make([]byte, 128)
	for i := 0; i < n; i++ {
		if err := s.Put([]byte(fmt.Sprintf("key%09d", i)), value); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			if _, err := s.Get([]byte(fmt.Sprintf("key%09d", r.Intn(n)))); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkScan100(b *testing.B) {
	s := openBench(b, SyncInterval)
	const n = 200000
	value := make([]byte, 128)
	for i := 0; i < n; i++ {
		if err := s.Put([]byte(fmt.Sprintf("key%09d", i)), value); err != nil {
			b.Fatal(err)
		}
	}
	r := rand.New(rand.NewSource(3))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lo := r.Intn(n - 100)
		start := []byte(fmt.Sprintf("key%09d", lo))
		end := []byte(fmt.Sprintf("key%09d", lo+99))
		got := 0
		if err := s.Scan(start, end, 0, func(k, v []byte) error { got++; return nil }); err != nil {
			b.Fatal(err)
		}
		if got != 100 {
			b.Fatalf("scan returned %d", got)
		}
	}
}
