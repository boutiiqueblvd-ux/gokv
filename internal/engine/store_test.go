package engine

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testOpts(t *testing.T) Options {
	t.Helper()
	return Options{
		Dir:                t.TempDir(),
		MaxFileSize:        4 << 10, // tiny, so tests exercise rotation
		SyncMode:           SyncNever,
		SyncEvery:          time.Hour,
		CompactionInterval: time.Hour, // tests drive compaction explicitly
		Logger:             log.New(io.Discard, "", 0),
	}
}

func open(t *testing.T, opts Options) *Store {
	t.Helper()
	s, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mustPut(t *testing.T, s *Store, k, v string) {
	t.Helper()
	if err := s.Put([]byte(k), []byte(v)); err != nil {
		t.Fatalf("Put(%q): %v", k, err)
	}
}

func mustGet(t *testing.T, s *Store, k, want string) {
	t.Helper()
	got, err := s.Get([]byte(k))
	if err != nil {
		t.Fatalf("Get(%q): %v", k, err)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q, want %q", k, got, want)
	}
}

func mustMiss(t *testing.T, s *Store, k string) {
	t.Helper()
	if _, err := s.Get([]byte(k)); err != ErrKeyNotFound {
		t.Fatalf("Get(%q) = %v, want ErrKeyNotFound", k, err)
	}
}

func TestPutGetDeleteOverwrite(t *testing.T) {
	s := open(t, testOpts(t))

	mustMiss(t, s, "absent")
	mustPut(t, s, "a", "1")
	mustPut(t, s, "b", "2")
	mustGet(t, s, "a", "1")

	mustPut(t, s, "a", "1-updated")
	mustGet(t, s, "a", "1-updated")

	if err := s.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	mustMiss(t, s, "a")
	mustGet(t, s, "b", "2")

	if err := s.Delete([]byte("a")); err != ErrKeyNotFound {
		t.Fatalf("second Delete = %v, want ErrKeyNotFound", err)
	}
	if err := s.Put(nil, []byte("x")); err != ErrEmptyKey {
		t.Fatalf("empty key accepted: %v", err)
	}
	if v, err := s.Get([]byte("empty-value")); err == nil {
		t.Fatalf("unexpected value %q", v)
	}
	if err := s.Put([]byte("empty-value"), nil); err != nil {
		t.Fatal(err)
	}
	if v, err := s.Get([]byte("empty-value")); err != nil || len(v) != 0 {
		t.Fatalf("empty value round trip: %q %v", v, err)
	}
}

func TestBatchPut(t *testing.T) {
	s := open(t, testOpts(t))

	var keys, vals [][]byte
	for i := 0; i < 200; i++ {
		keys = append(keys, []byte(fmt.Sprintf("k%04d", i)))
		vals = append(vals, []byte(fmt.Sprintf("v%04d", i)))
	}
	if err := s.BatchPut(keys, vals); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}
	for i := range keys {
		mustGet(t, s, string(keys[i]), string(vals[i]))
	}
	if err := s.BatchPut(keys, vals[:3]); err != ErrBatchLen {
		t.Fatalf("mismatched batch = %v, want ErrBatchLen", err)
	}
}

func TestReadKeyRange(t *testing.T) {
	s := open(t, testOpts(t))
	for i := 0; i < 100; i++ {
		mustPut(t, s, fmt.Sprintf("key%03d", i), fmt.Sprintf("val%03d", i))
	}
	// Keys deliberately inserted out of order too.
	mustPut(t, s, "aaa", "first")
	mustPut(t, s, "zzz", "last")

	got, err := s.ScanSlice([]byte("key010"), []byte("key019"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Fatalf("range returned %d results, want 10", len(got))
	}
	for i, kv := range got {
		wantK := fmt.Sprintf("key%03d", 10+i)
		if string(kv.Key) != wantK || string(kv.Value) != "val"+wantK[3:] {
			t.Fatalf("result %d = %q/%q, want %q", i, kv.Key, kv.Value, wantK)
		}
	}

	all, err := s.ScanSlice(nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 102 {
		t.Fatalf("full scan returned %d, want 102", len(all))
	}
	if string(all[0].Key) != "aaa" || string(all[len(all)-1].Key) != "zzz" {
		t.Fatalf("full scan not ordered: %q .. %q", all[0].Key, all[len(all)-1].Key)
	}

	limited, err := s.ScanSlice([]byte("key"), []byte("key999"), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 5 {
		t.Fatalf("limit ignored: got %d", len(limited))
	}

	// Deleted keys must disappear from ranges.
	if err := s.Delete([]byte("key050")); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ScanSlice([]byte("key049"), []byte("key051"), 0)
	if len(got) != 2 {
		t.Fatalf("deleted key still in range: %d results", len(got))
	}

	if got, _ = s.ScanSlice([]byte("zzz"), []byte("aaa"), 0); len(got) != 0 {
		t.Fatalf("inverted range returned %d results", len(got))
	}
}

func TestReopenPreservesState(t *testing.T) {
	opts := testOpts(t)
	s := open(t, opts)
	for i := 0; i < 500; i++ {
		mustPut(t, s, fmt.Sprintf("k%04d", i), fmt.Sprintf("value-%04d", i))
	}
	for i := 0; i < 500; i += 3 {
		if err := s.Delete([]byte(fmt.Sprintf("k%04d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 500; i += 7 {
		mustPut(t, s, fmt.Sprintf("k%04d", i), "rewritten")
	}
	wantLen := s.Len()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := open(t, opts)
	if s2.Len() != wantLen {
		t.Fatalf("after reopen %d keys, want %d", s2.Len(), wantLen)
	}
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("k%04d", i)
		switch {
		case i%7 == 0:
			mustGet(t, s2, k, "rewritten")
		case i%3 == 0:
			mustMiss(t, s2, k)
		default:
			mustGet(t, s2, k, fmt.Sprintf("value-%04d", i))
		}
	}
	// Writes must continue to work, and sequence numbers must not regress.
	before := s2.Seq()
	mustPut(t, s2, "after-restart", "ok")
	if s2.Seq() <= before {
		t.Fatalf("sequence did not advance: %d -> %d", before, s2.Seq())
	}
}

// lastDataFile returns the highest-numbered data file, i.e. the one an
// abruptly killed process would have been writing into.
func lastDataFile(t *testing.T, dir string) string {
	t.Helper()
	ids, err := listDataFiles(dir)
	if err != nil || len(ids) == 0 {
		t.Fatalf("no data files in %s (%v)", dir, err)
	}
	return dataPath(dir, ids[len(ids)-1])
}

func TestRecoveryFromTornTail(t *testing.T) {
	opts := testOpts(t)
	s := open(t, opts)
	for i := 0; i < 50; i++ {
		mustPut(t, s, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
	}
	s.Close()

	// Simulate a crash in the middle of an append: a partial record, then
	// garbage that happens to look like a header.
	p := lastDataFile(t, opts.Dir)
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	junk := make([]byte, 37)
	rand.New(rand.NewSource(1)).Read(junk)
	f.Write(junk)
	f.Close()

	s2 := open(t, opts)
	if s2.Len() != 50 {
		t.Fatalf("recovered %d keys, want 50", s2.Len())
	}
	for i := 0; i < 50; i++ {
		mustGet(t, s2, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
	}
	// The torn tail must have been cut off so appends resume cleanly.
	mustPut(t, s2, "post-crash", "written")
	mustGet(t, s2, "post-crash", "written")
	s2.Close()

	s3 := open(t, opts)
	mustGet(t, s3, "post-crash", "written")
	if s3.Len() != 51 {
		t.Fatalf("after second reopen %d keys, want 51", s3.Len())
	}
}

func TestRecoveryDropsIncompleteBatch(t *testing.T) {
	opts := testOpts(t)
	opts.MaxFileSize = 1 << 30 // keep everything in one file for this test
	s := open(t, opts)
	mustPut(t, s, "before", "kept")

	keys := [][]byte{[]byte("b1"), []byte("b2"), []byte("b3"), []byte("b4")}
	vals := [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}
	if err := s.BatchPut(keys, vals); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Chop off the terminating record of the batch, as a crash mid-batch would.
	p := lastDataFile(t, opts.Dir)
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	lastSize := int64(recordHeaderSize + len("b4") + len("4"))
	if err := os.Truncate(p, st.Size()-lastSize); err != nil {
		t.Fatal(err)
	}

	s2 := open(t, opts)
	mustGet(t, s2, "before", "kept")
	for _, k := range []string{"b1", "b2", "b3", "b4"} {
		mustMiss(t, s2, k)
	}
	if s2.Len() != 1 {
		t.Fatalf("partial batch leaked: %d keys", s2.Len())
	}
}

func TestHintFilesAreUsedAndCorrect(t *testing.T) {
	opts := testOpts(t)
	s := open(t, opts)
	for i := 0; i < 400; i++ {
		mustPut(t, s, fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i))
	}
	s.Close()

	hints, _ := filepath.Glob(filepath.Join(opts.Dir, "*.hint"))
	if len(hints) == 0 {
		t.Fatal("no hint files were produced by rotation")
	}
	s2 := open(t, opts)
	for i := 0; i < 400; i++ {
		mustGet(t, s2, fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i))
	}
	s2.Close()

	// A corrupt hint must not be fatal: recovery falls back to the data file.
	if err := os.WriteFile(hints[0], []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	s3 := open(t, opts)
	for i := 0; i < 400; i++ {
		mustGet(t, s3, fmt.Sprintf("k%04d", i), fmt.Sprintf("v%04d", i))
	}
}

func TestCompactionReclaimsSpaceAndKeepsData(t *testing.T) {
	opts := testOpts(t)
	opts.MaxFileSize = 8 << 10
	s := open(t, opts)

	// Rewrite a small key space many times to manufacture garbage.
	for round := 0; round < 40; round++ {
		for i := 0; i < 20; i++ {
			mustPut(t, s, fmt.Sprintf("k%02d", i), fmt.Sprintf("round-%02d-value-%02d", round, i))
		}
	}
	for i := 0; i < 5; i++ {
		if err := s.Delete([]byte(fmt.Sprintf("k%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	before := dirSize(t, opts.Dir)
	if !s.shouldCompact() {
		t.Fatalf("expected compaction to be warranted, stats=%+v", s.Stats())
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after := dirSize(t, opts.Dir)
	if after >= before {
		t.Fatalf("compaction did not shrink the store: %d -> %d", before, after)
	}
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k%02d", i)
		if i < 5 {
			mustMiss(t, s, k)
		} else {
			mustGet(t, s, k, fmt.Sprintf("round-39-value-%02d", i))
		}
	}
	s.Close()

	// And the compacted store must still recover correctly.
	s2 := open(t, opts)
	if s2.Len() != 15 {
		t.Fatalf("after compaction+reopen %d keys, want 15", s2.Len())
	}
	for i := 5; i < 20; i++ {
		mustGet(t, s2, fmt.Sprintf("k%02d", i), fmt.Sprintf("round-39-value-%02d", i))
	}
}

func TestCompactionConcurrentWithTraffic(t *testing.T) {
	opts := testOpts(t)
	opts.MaxFileSize = 16 << 10
	s := open(t, opts)

	const n = 300
	for i := 0; i < n; i++ {
		mustPut(t, s, fmt.Sprintf("k%04d", i), fmt.Sprintf("v0-%04d", i))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := r.Intn(n)
				k := []byte(fmt.Sprintf("k%04d", i))
				if r.Intn(2) == 0 {
					if err := s.Put(k, []byte(fmt.Sprintf("v1-%04d", i))); err != nil {
						errCh <- err
						return
					}
				} else if _, err := s.Get(k); err != nil && err != ErrKeyNotFound {
					errCh <- err
					return
				}
			}
		}(w)
	}
	for i := 0; i < 3; i++ {
		if err := s.Compact(); err != nil {
			t.Errorf("Compact: %v", err)
		}
	}
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("worker: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := s.Get([]byte(fmt.Sprintf("k%04d", i))); err != nil {
			t.Fatalf("key %d lost across compaction: %v", i, err)
		}
	}
}

func TestConcurrentReadersWritersScanners(t *testing.T) {
	opts := testOpts(t)
	opts.MaxFileSize = 32 << 10
	s := open(t, opts)

	const keys = 500
	for i := 0; i < keys; i++ {
		mustPut(t, s, fmt.Sprintf("k%04d", i), "initial")
	}

	var wg sync.WaitGroup
	deadline := time.Now().Add(700 * time.Millisecond)
	fail := make(chan error, 16)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w) + 99))
			for time.Now().Before(deadline) {
				i := r.Intn(keys)
				k := []byte(fmt.Sprintf("k%04d", i))
				switch r.Intn(4) {
				case 0:
					if err := s.Put(k, []byte(fmt.Sprintf("w%d-%d", w, i))); err != nil {
						fail <- err
						return
					}
				case 1:
					if _, err := s.Get(k); err != nil && err != ErrKeyNotFound {
						fail <- err
						return
					}
				case 2:
					lo := fmt.Sprintf("k%04d", i)
					hi := fmt.Sprintf("k%04d", min(i+25, keys-1))
					if _, err := s.ScanSlice([]byte(lo), []byte(hi), 0); err != nil {
						fail <- err
						return
					}
				case 3:
					ks := [][]byte{k, []byte(fmt.Sprintf("batch-%d-%d", w, i))}
					vs := [][]byte{[]byte("b1"), []byte("b2")}
					if err := s.BatchPut(ks, vs); err != nil {
						fail <- err
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(fail)
	for err := range fail {
		t.Fatalf("concurrent worker: %v", err)
	}

	// The index and the log must still agree.
	n := 0
	if err := s.Scan(nil, nil, 0, func(k, v []byte) error { n++; return nil }); err != nil {
		t.Fatalf("final scan: %v", err)
	}
	if n != s.Len() {
		t.Fatalf("scan saw %d keys, index holds %d", n, s.Len())
	}
}

func TestManyKeysAcrossManyFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping volume test in short mode")
	}
	opts := testOpts(t)
	opts.MaxFileSize = 64 << 10
	s := open(t, opts)

	const n = 20000
	value := make([]byte, 64)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	// Random insertion order: the log is sequential regardless.
	perm := rand.New(rand.NewSource(7)).Perm(n)
	for _, i := range perm {
		if err := s.Put([]byte(fmt.Sprintf("key%08d", i)), value); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if s.Len() != n {
		t.Fatalf("%d keys, want %d", s.Len(), n)
	}
	ids, _ := listDataFiles(opts.Dir)
	if len(ids) < 10 {
		t.Fatalf("expected the log to be split into many files, got %d", len(ids))
	}
	// Ordered scan over a store whose data is spread across dozens of files.
	prev := ""
	count := 0
	err := s.Scan([]byte("key00000100"), []byte("key00000199"), 0, func(k, v []byte) error {
		if prev != "" && string(k) <= prev {
			return fmt.Errorf("scan out of order: %q after %q", k, prev)
		}
		prev = string(k)
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("scan returned %d keys, want 100", count)
	}
}

func TestDirectoryLockIsExclusive(t *testing.T) {
	opts := testOpts(t)
	s := open(t, opts)
	if _, err := Open(opts); err == nil {
		t.Fatal("second Open on the same directory succeeded")
	}
	s.Close()
	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	s2.Close()
}

func TestChecksumDetectsBitRot(t *testing.T) {
	opts := testOpts(t)
	opts.MaxFileSize = 1 << 30
	s := open(t, opts)
	mustPut(t, s, "k", "the quick brown fox")
	s.Close()

	p := lastDataFile(t, opts.Dir)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xFF // flip a bit inside the value
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	// The damaged record fails its checksum during recovery, so the key is
	// simply absent rather than silently wrong.
	if _, err := s2.Get([]byte("k")); err != ErrKeyNotFound {
		t.Fatalf("corrupt record surfaced as %v, want ErrKeyNotFound", err)
	}
}

func TestApplyRawIsIdempotentAndOrderInsensitive(t *testing.T) {
	src := open(t, testOpts(t))
	var stream [][]byte
	cancel := src.AddObserver(func(seq uint64, raw []byte) {
		stream = append(stream, append([]byte(nil), raw...))
	})
	mustPut(t, src, "a", "1")
	mustPut(t, src, "b", "2")
	mustPut(t, src, "a", "3")
	if err := src.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	cancel()

	dst := open(t, testOpts(t))
	// Apply out of order and twice over: the sequence numbers decide.
	for _, i := range []int{2, 0, 3, 1, 0, 3, 2} {
		if _, err := dst.ApplyRaw(stream[i]); err != nil {
			t.Fatalf("ApplyRaw: %v", err)
		}
	}
	mustGet(t, dst, "a", "3")
	mustMiss(t, dst, "b")
	if dst.Seq() < src.Seq() {
		t.Fatalf("replica sequence %d behind source %d", dst.Seq(), src.Seq())
	}
}

func dirSize(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if info, err := e.Info(); err == nil {
			total += info.Size()
		}
	}
	return total
}

// A BatchPut's terminator record can be superseded while the earlier records
// of the same batch are still live. Compaction then copies orphaned batch
// members into the merged file; if it kept their batch markers, recovery would
// stage them forever and throw them away.
func TestCompactionDropsBatchMarkersFromOrphanedRecords(t *testing.T) {
	opts := testOpts(t)
	opts.MaxFileSize = 1 << 20
	s := open(t, opts)

	keys := [][]byte{[]byte("b1"), []byte("b2"), []byte("b3")}
	vals := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	if err := s.BatchPut(keys, vals); err != nil {
		t.Fatal(err)
	}
	// Kill the terminator: b3 was the last record of the batch.
	mustPut(t, s, "b3", "replaced")
	// Give the merge something to do and something to keep.
	for i := 0; i < 50; i++ {
		mustPut(t, s, fmt.Sprintf("f%02d", i), "v")
		mustPut(t, s, fmt.Sprintf("f%02d", i), "v2")
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	s.Close()

	// Force the scan path, which is where the batch markers matter.
	hints, _ := filepath.Glob(filepath.Join(opts.Dir, "*.hint"))
	for _, h := range hints {
		if err := os.Remove(h); err != nil {
			t.Fatal(err)
		}
	}
	s2 := open(t, opts)
	mustGet(t, s2, "b1", "one")
	mustGet(t, s2, "b2", "two")
	mustGet(t, s2, "b3", "replaced")
}

// The same hazard on the replication path: a snapshot ships one record per
// key, so a batch member arrives at the follower without its terminator.
func TestSnapshotRecordsCarryNoBatchMarkers(t *testing.T) {
	srcOpts, dstOpts := testOpts(t), testOpts(t)
	dstOpts.MaxFileSize = 1 << 20
	src := open(t, srcOpts)

	keys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	vals := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	if err := src.BatchPut(keys, vals); err != nil {
		t.Fatal(err)
	}

	dst := open(t, dstOpts)
	if err := src.SnapshotRecords(func(raw []byte) error {
		_, err := dst.ApplyRaw(raw)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for i := range keys {
		mustGet(t, dst, string(keys[i]), string(vals[i]))
	}
	dst.Close()

	hints, _ := filepath.Glob(filepath.Join(dstOpts.Dir, "*.hint"))
	for _, h := range hints {
		os.Remove(h)
	}
	dst2 := open(t, dstOpts)
	if dst2.Len() != 3 {
		t.Fatalf("replica kept %d keys across a restart, want 3", dst2.Len())
	}
	for i := range keys {
		mustGet(t, dst2, string(keys[i]), string(vals[i]))
	}
}

// Records arriving from another node are not produced by our own validated
// write path, so ApplyRaw has to police them itself.
func TestApplyRawRejectsMalformedRecords(t *testing.T) {
	s := open(t, testOpts(t))

	// A well-formed record, used as the baseline.
	good := appendRecord(nil, &record{ts: 1, seq: 1, key: []byte("k"), value: []byte("v")})
	if _, err := s.ApplyRaw(good); err != nil {
		t.Fatalf("valid record rejected: %v", err)
	}
	mustGet(t, s, "k", "v")

	// An empty key would be read back as end-of-file by recovery, silently
	// truncating everything written after it.
	empty := appendRecord(nil, &record{ts: 1, seq: 2, key: []byte{}, value: []byte("v")})
	if _, err := s.ApplyRaw(empty); err == nil {
		t.Fatal("a record with an empty key was accepted")
	}

	for name, raw := range map[string][]byte{
		"truncated header": good[:10],
		"truncated body":   good[:len(good)-1],
		"flipped bit":      flip(good),
	} {
		if _, err := s.ApplyRaw(raw); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}

	// A rejected record must leave the store usable and its log intact.
	mustPut(t, s, "after", "ok")
	s.Close()
	s2, err := Open(Options{Dir: s.dir, SyncMode: SyncNever, MaxFileSize: 4 << 10,
		CompactionInterval: time.Hour, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	mustGet(t, s2, "k", "v")
	mustGet(t, s2, "after", "ok")
}

func flip(b []byte) []byte {
	out := append([]byte(nil), b...)
	out[len(out)-1] ^= 0xFF
	return out
}

// A delete must stay a delete even when an older record for the same key is
// applied afterwards. Records reach recovery in file order and reach a
// follower in network order, neither of which is sequence order.
func TestDeleteIsNotUndoneByAnOlderRecord(t *testing.T) {
	src := open(t, testOpts(t))
	var stream [][]byte
	cancel := src.AddObserver(func(seq uint64, raw []byte) {
		stream = append(stream, append([]byte(nil), raw...))
	})
	mustPut(t, src, "k", "value") // seq 1
	if err := src.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	} // seq 2
	cancel()

	dst := open(t, testOpts(t))
	// The tombstone arrives first, the value it supersedes second.
	if _, err := dst.ApplyRaw(stream[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.ApplyRaw(stream[0]); err != nil {
		t.Fatal(err)
	}
	mustMiss(t, dst, "k")
}

// The same hazard reached through recovery: compaction can copy a live record
// into a high-numbered merged file while a concurrent delete puts the
// tombstone in a lower-numbered one. Recovery walks files in id order, so it
// meets the tombstone first and the value it deleted second.
func TestRecoveryHonoursSequenceNotFileOrder(t *testing.T) {
	opts := testOpts(t)
	opts.MaxFileSize = 1 << 30
	s := open(t, opts)
	mustPut(t, s, "k", "v1")
	mustPut(t, s, "other", "keep")
	if err := s.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Lift the superseded record out of the log...
	ids, err := listDataFiles(opts.Dir)
	if err != nil || len(ids) == 0 {
		t.Fatalf("no data files: %v", err)
	}
	var stale []byte
	if _, err := scanFile(dataPath(opts.Dir, ids[0]), func(pos int64, raw []byte, r *record) error {
		if string(r.key) == "k" && !r.tombstone() {
			stale = append([]byte(nil), raw...)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if stale == nil {
		t.Fatal("could not find the superseded record")
	}
	// ...and plant it in a file that sorts after the one holding the tombstone,
	// exactly as a merge racing a delete would.
	newID := ids[len(ids)-1] + 1
	if err := os.WriteFile(dataPath(opts.Dir, newID), stale, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := open(t, opts)
	mustMiss(t, s2, "k")
	mustGet(t, s2, "other", "keep")
	if s2.Len() != 1 {
		t.Fatalf("%d live keys after recovery, want 1", s2.Len())
	}
}

// Tombstones live in the index so the rule above can be applied, so compaction
// has to be what removes them -- otherwise the index grows forever.
func TestCompactionForgetsTombstones(t *testing.T) {
	opts := testOpts(t)
	opts.MaxFileSize = 8 << 10
	s := open(t, opts)

	for i := 0; i < 100; i++ {
		mustPut(t, s, fmt.Sprintf("k%03d", i), "value")
	}
	for i := 0; i < 50; i++ {
		if err := s.Delete([]byte(fmt.Sprintf("k%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	st := s.Stats()
	if st.Keys != 50 || st.Tombstones != 50 {
		t.Fatalf("before compaction: %d keys, %d tombstones; want 50 and 50", st.Keys, st.Tombstones)
	}

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	st = s.Stats()
	if st.Keys != 50 || st.Tombstones != 0 {
		t.Fatalf("after compaction: %d keys, %d tombstones; want 50 and 0", st.Keys, st.Tombstones)
	}
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("k%03d", i)
		if i < 50 {
			mustMiss(t, s, k)
		} else {
			mustGet(t, s, k, "value")
		}
	}
	s.Close()

	s2 := open(t, opts)
	if s2.Stats().Tombstones != 0 {
		t.Fatalf("tombstones came back after a restart: %+v", s2.Stats())
	}
	for i := 0; i < 50; i++ {
		mustMiss(t, s2, fmt.Sprintf("k%03d", i))
	}
}

// A run of deleted keys longer than the scan chunk must not look like the end
// of the index.
func TestRangeCrossesLongRunsOfDeletedKeys(t *testing.T) {
	opts := testOpts(t)
	opts.MaxFileSize = 1 << 20
	s := open(t, opts)

	const n = 3000
	for i := 0; i < n; i++ {
		mustPut(t, s, fmt.Sprintf("k%05d", i), "v")
	}
	// Delete far more consecutive keys than one scan chunk holds.
	for i := 0; i < n-40; i++ {
		if err := s.Delete([]byte(fmt.Sprintf("k%05d", i))); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.ScanSlice(nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 40 {
		t.Fatalf("scan across %d tombstones returned %d pairs, want 40", n-40, len(got))
	}
	if string(got[0].Key) != fmt.Sprintf("k%05d", n-40) {
		t.Fatalf("first surviving key is %q", got[0].Key)
	}

	// And the same through a snapshot, which walks the index the same way.
	count := 0
	if err := s.SnapshotRecords(func(raw []byte) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 40 {
		t.Fatalf("snapshot shipped %d records, want 40", count)
	}
}
