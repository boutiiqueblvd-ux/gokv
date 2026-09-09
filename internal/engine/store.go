// Package engine implements a persistent key/value storage engine in the
// Bitcask style: an append-only log of records on disk, plus an in-memory
// index that maps every live key to the offset of its newest record.
//
// The shape of that design is what buys the requirements in the brief:
//
//   - every write is one sequential append, so write throughput does not
//     depend on the key distribution (random keys cost the same as sorted ones);
//   - every read is one index lookup plus at most one disk seek;
//   - only keys live in RAM, so the dataset may be far larger than memory;
//   - the log is checksummed and never overwritten in place, so recovery is a
//     forward scan that stops at the first torn record.
//
// The price is that the index must fit in memory and that compaction has to
// reclaim space in the background. Both trade-offs are discussed in SOLUTION.md.
package engine

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrKeyNotFound  = errors.New("engine: key not found")
	ErrClosed       = errors.New("engine: store is closed")
	ErrEmptyKey     = errors.New("engine: empty key")
	ErrKeyTooLarge  = errors.New("engine: key too large")
	ErrValueTooLong = errors.New("engine: value too large")
	ErrBatchLen     = errors.New("engine: batch key/value count mismatch")
	ErrLocked       = errors.New("engine: data directory is locked by another process")
)

// SyncMode controls how aggressively the log is pushed to stable storage.
type SyncMode int

const (
	// SyncInterval fsyncs on a timer. A crash can lose at most the last
	// interval of writes; the OS page cache absorbs the rest. This is the
	// default because it is the only setting that reaches high write
	// throughput on real disks.
	SyncInterval SyncMode = iota
	// SyncAlways fsyncs before every write is acknowledged. Nothing
	// acknowledged is ever lost; throughput drops to the disk's fsync rate.
	SyncAlways
	// SyncNever leaves durability entirely to the OS. Useful for tests.
	SyncNever
)

type Options struct {
	Dir                 string
	MaxFileSize         int64         // rotate the active file above this size
	SyncMode            SyncMode      //
	SyncEvery           time.Duration // used when SyncMode == SyncInterval
	CompactionThreshold float64       // reclaim when dead/total exceeds this
	CompactionInterval  time.Duration // how often to evaluate compaction
	Logger              *log.Logger
}

func (o *Options) applyDefaults() {
	if o.MaxFileSize <= 0 {
		o.MaxFileSize = 256 << 20
	}
	if o.SyncEvery <= 0 {
		o.SyncEvery = 200 * time.Millisecond
	}
	if o.CompactionThreshold <= 0 {
		o.CompactionThreshold = 0.4
	}
	if o.CompactionInterval <= 0 {
		o.CompactionInterval = 30 * time.Second
	}
	if o.Logger == nil {
		o.Logger = log.New(os.Stderr, "[engine] ", log.LstdFlags)
	}
}

// Observer is notified after records have been appended to the log. raw holds
// one or more complete encoded records; it must not be retained. Observers run
// on the writer's goroutine while the write lock is held, which is what keeps
// the replication stream in the same order as the log itself.
type Observer func(maxSeq uint64, raw []byte)

type Store struct {
	opts Options
	dir  string

	// mu guards the in-memory index (keys + order). Reads take it shared.
	mu    sync.RWMutex
	keys  map[string]*slnode
	order *skiplist

	// wmu serialises appends. It also guards seq and the active file.
	wmu     sync.Mutex
	active  *datafile
	nextID  uint32
	seq     uint64
	wbuf    []byte
	offs    []uint32
	recs    []record
	dirty   bool
	lastRot time.Time

	// fmu guards the open file set. Readers hold it shared for the duration
	// of a read so compaction cannot close a file out from under them.
	fmu   sync.RWMutex
	files map[uint32]*datafile

	obsMu sync.RWMutex
	obs   map[uint64]Observer
	obsID uint64

	live atomic.Int64
	dead atomic.Int64

	nPut, nGet, nDel, nScan, nMiss atomic.Uint64

	compactMu  sync.Mutex
	compacting atomic.Bool
	mergeSeq   atomic.Uint64

	lock   *os.File
	stop   chan struct{}
	wg     sync.WaitGroup
	closed atomic.Bool
}

func Open(opts Options) (*Store, error) {
	opts.applyDefaults()
	if opts.Dir == "" {
		return nil, errors.New("engine: Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := acquireLock(opts.Dir)
	if err != nil {
		return nil, err
	}
	s := &Store{
		opts:  opts,
		dir:   opts.Dir,
		keys:  make(map[string]*slnode),
		order: newSkiplist(),
		files: make(map[uint32]*datafile),
		obs:   make(map[uint64]Observer),
		lock:  lock,
		stop:  make(chan struct{}),
	}
	if err := s.recover(); err != nil {
		s.closeFiles()
		releaseLock(lock, opts.Dir)
		return nil, err
	}
	s.wg.Add(1)
	go s.background()
	return s, nil
}

func (s *Store) logf(format string, args ...any) { s.opts.Logger.Printf(format, args...) }

// --- recovery ---------------------------------------------------------------

func (s *Store) recover() error {
	start := time.Now()
	s.mergeSeq.Store(readManifest(s.dir))

	ids, err := listDataFiles(s.dir)
	if err != nil {
		return err
	}

	var (
		totalBytes  int64
		maxSeq      uint64
		reuseActive *datafile
	)
	for i, id := range ids {
		isLast := i == len(ids)-1
		var (
			commit  int64
			scanned bool
		)
		if hintExists(s.dir, id) {
			err := readHint(s.dir, id, func(key []byte, e entry, flags uint8) {
				if e.seq > maxSeq {
					maxSeq = e.seq
				}
				s.applyRecovered(key, e, flags&flagTombstone != 0)
			})
			if err != nil {
				s.logf("hint file for %d unusable (%v), rescanning data file", id, err)
			} else {
				scanned = true
			}
		}
		if !scanned {
			commit, err = s.scanIntoIndex(id, &maxSeq)
			if err != nil {
				return err
			}
		}

		df, err := openDataFile(s.dir, id, isLast)
		if err != nil {
			return err
		}
		if isLast && !scanned && commit < df.size.Load() {
			// The tail of the last file was torn by a crash, or holds a
			// BatchPut that never completed. Cut it off so appends resume
			// from a record boundary.
			s.logf("truncating %s from %d to %d bytes (torn tail)", filepath.Base(df.path), df.size.Load(), commit)
			if err := df.f.Truncate(commit); err != nil {
				return err
			}
			if err := df.f.Sync(); err != nil {
				return err
			}
			df.size.Store(commit)
		}
		totalBytes += df.size.Load()
		s.files[id] = df
		if id >= s.nextID {
			s.nextID = id + 1
		}
		// Only a file we scanned ourselves is known to be appendable: a file
		// with a hint has already been rotated or merged and is immutable.
		if isLast && !scanned && df.size.Load() < s.opts.MaxFileSize {
			reuseActive = df
		}
	}

	s.seq = maxSeq
	if reuseActive != nil {
		s.active = reuseActive
	} else {
		if err := s.openNewActive(); err != nil {
			return err
		}
	}

	var live int64
	for _, n := range s.keys {
		live += int64(n.ent.size)
	}
	s.live.Store(live)
	if d := totalBytes - live; d > 0 {
		s.dead.Store(d)
	}
	s.logf("recovered %d keys from %d files (%.1f MiB) in %s",
		len(s.keys), len(ids), float64(totalBytes)/(1<<20), time.Since(start).Round(time.Millisecond))
	return nil
}

// scanIntoIndex replays one data file. Records that belong to an unfinished
// BatchPut are staged and dropped if the terminating record never arrives, so
// a batch is all-or-nothing across a crash.
func (s *Store) scanIntoIndex(id uint32, maxSeq *uint64) (int64, error) {
	type staged struct {
		key  []byte
		e    entry
		tomb bool
	}
	var (
		pending []staged
		commit  int64
	)
	_, err := scanFile(dataPath(s.dir, id), func(pos int64, raw []byte, r *record) error {
		size := uint32(len(raw))
		if r.seq > *maxSeq {
			*maxSeq = r.seq
		}
		e := entry{pos: pos, seq: r.seq, fileID: id, size: size}
		if r.flags&flagBatch != 0 {
			pending = append(pending, staged{key: append([]byte(nil), r.key...), e: e, tomb: r.tombstone()})
			if r.flags&flagBatchEnd != 0 {
				for i := range pending {
					s.applyRecovered(pending[i].key, pending[i].e, pending[i].tomb)
				}
				pending = pending[:0]
				commit = pos + int64(size)
			}
			return nil
		}
		s.applyRecovered(r.key, e, r.tombstone())
		commit = pos + int64(size)
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(pending) > 0 {
		s.logf("discarding %d records of an incomplete batch in file %d", len(pending), id)
	}
	return commit, nil
}

// applyRecovered installs an entry if it is newer than what the index holds.
// Comparing sequence numbers rather than file order is what lets merged files
// carry fresh ids without resurrecting stale values.
func (s *Store) applyRecovered(key []byte, e entry, tomb bool) {
	if n, ok := s.keys[string(key)]; ok {
		if n.ent.seq >= e.seq {
			return
		}
		if tomb {
			delete(s.keys, n.key)
			s.order.remove(n.key)
			return
		}
		n.ent = e
		return
	}
	if tomb {
		return
	}
	k := string(key)
	s.keys[k] = s.order.insert(k, e)
}

// --- write path -------------------------------------------------------------

func validateKey(key []byte) error {
	switch {
	case len(key) == 0:
		return ErrEmptyKey
	case len(key) > MaxKeySize:
		return ErrKeyTooLarge
	}
	return nil
}

func (s *Store) Put(key, value []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLong
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	s.recs = append(s.recs[:0], record{ts: time.Now().UnixNano(), key: key, value: value})
	if err := s.appendLocked(s.recs); err != nil {
		return err
	}
	s.nPut.Add(1)
	return nil
}

// BatchPut writes many pairs with a single append and a single fsync. Every
// record carries a batch flag and the last one is marked as the terminator, so
// recovery either sees the whole batch or none of it.
func (s *Store) BatchPut(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return ErrBatchLen
	}
	if len(keys) == 0 {
		return nil
	}
	for i, k := range keys {
		if err := validateKey(k); err != nil {
			return err
		}
		if len(values[i]) > MaxValueSize {
			return ErrValueTooLong
		}
	}
	now := time.Now().UnixNano()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	// The record scratch buffer belongs to the write lock; building the batch
	// before taking it would let two writers scribble over each other.
	recs := s.recs[:0]
	for i := range keys {
		flags := uint8(flagBatch)
		if i == len(keys)-1 {
			flags |= flagBatchEnd
		}
		recs = append(recs, record{ts: now, flags: flags, key: keys[i], value: values[i]})
	}
	s.recs = recs
	if err := s.appendLocked(recs); err != nil {
		return err
	}
	s.nPut.Add(uint64(len(keys)))
	return nil
}

func (s *Store) Delete(key []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	s.mu.RLock()
	_, ok := s.keys[string(key)]
	s.mu.RUnlock()
	if !ok {
		return ErrKeyNotFound
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	s.recs = append(s.recs[:0], record{ts: time.Now().UnixNano(), flags: flagTombstone, key: key})
	if err := s.appendLocked(s.recs); err != nil {
		return err
	}
	s.nDel.Add(1)
	return nil
}

// appendLocked assigns sequence numbers, encodes, and appends. Callers hold wmu.
func (s *Store) appendLocked(recs []record) error {
	for i := range recs {
		s.seq++
		recs[i].seq = s.seq
	}
	return s.writeLocked(recs)
}

func (s *Store) writeLocked(recs []record) error {
	buf := s.wbuf[:0]
	offs := s.offs[:0]
	for i := range recs {
		offs = append(offs, uint32(len(buf)))
		buf = appendRecord(buf, &recs[i])
	}
	s.wbuf, s.offs = buf, offs

	if err := s.maybeRotateLocked(int64(len(buf))); err != nil {
		return err
	}
	af := s.active
	base := af.size.Load()
	if _, err := af.f.WriteAt(buf, base); err != nil {
		// Leave the log parseable: drop whatever made it to disk.
		af.f.Truncate(base)
		return err
	}
	af.size.Store(base + int64(len(buf)))
	s.dirty = true
	if s.opts.SyncMode == SyncAlways {
		if err := af.f.Sync(); err != nil {
			return err
		}
		s.dirty = false
	}

	s.mu.Lock()
	for i := range recs {
		e := entry{
			pos:    base + int64(offs[i]),
			seq:    recs[i].seq,
			fileID: af.id,
			size:   encodedSize(len(recs[i].key), len(recs[i].value)),
		}
		s.indexApplyLocked(recs[i].key, e, recs[i].tombstone())
	}
	s.mu.Unlock()

	s.notify(recs[len(recs)-1].seq, buf)
	return nil
}

// indexApplyLocked is the single place where the index changes. Callers hold mu.
func (s *Store) indexApplyLocked(key []byte, e entry, tomb bool) {
	if n, ok := s.keys[string(key)]; ok {
		if n.ent.seq >= e.seq {
			s.dead.Add(int64(e.size)) // arrived out of order: already stale
			return
		}
		s.dead.Add(int64(n.ent.size))
		s.live.Add(-int64(n.ent.size))
		if tomb {
			delete(s.keys, n.key)
			s.order.remove(n.key)
			s.dead.Add(int64(e.size))
			return
		}
		n.ent = e
		s.live.Add(int64(e.size))
		return
	}
	if tomb {
		s.dead.Add(int64(e.size))
		return
	}
	k := string(key)
	s.keys[k] = s.order.insert(k, e)
	s.live.Add(int64(e.size))
}

func (s *Store) maybeRotateLocked(incoming int64) error {
	cur := s.active.size.Load()
	if cur == 0 || cur+incoming <= s.opts.MaxFileSize {
		return nil
	}
	old := s.active
	if err := old.f.Sync(); err != nil {
		return err
	}
	if err := s.openNewActive(); err != nil {
		return err
	}
	s.dirty = false
	// The retired file is immutable now, so its hint can be built off the
	// writer's critical path.
	s.wg.Add(1)
	go func(id uint32) {
		defer s.wg.Done()
		if err := s.buildHint(id); err != nil {
			s.logf("hint generation for file %d failed: %v", id, err)
		}
	}(old.id)
	return nil
}

func (s *Store) openNewActive() error {
	df, err := createDataFile(s.dir, s.nextID)
	if err != nil {
		return err
	}
	s.nextID++
	s.fmu.Lock()
	s.files[df.id] = df
	s.fmu.Unlock()
	s.active = df
	s.lastRot = time.Now()
	return syncDir(s.dir)
}

// --- read path --------------------------------------------------------------

func (s *Store) Get(key []byte) ([]byte, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	s.nGet.Add(1)
	// Compaction can relocate a record between the index lookup and the disk
	// read. That is rare and always self-correcting, so retry rather than
	// serialise reads against the merge.
	for attempt := 0; attempt < 4; attempt++ {
		s.mu.RLock()
		n, ok := s.keys[string(key)]
		var e entry
		if ok {
			e = n.ent
		}
		s.mu.RUnlock()
		if !ok {
			s.nMiss.Add(1)
			return nil, ErrKeyNotFound
		}
		v, err := s.readValue(e)
		if err == errFileGone {
			continue
		}
		return v, err
	}
	return nil, errors.New("engine: read lost a race with compaction")
}

var errFileGone = errors.New("engine: data file was compacted away")

func (s *Store) readValue(e entry) ([]byte, error) {
	s.fmu.RLock()
	df := s.files[e.fileID]
	if df == nil {
		s.fmu.RUnlock()
		return nil, errFileGone
	}
	rec, _, err := df.readRecord(e)
	s.fmu.RUnlock()
	if err != nil {
		return nil, err
	}
	if rec.seq != e.seq {
		return nil, fmt.Errorf("engine: index points at seq %d but disk holds %d", e.seq, rec.seq)
	}
	if rec.tombstone() {
		return nil, ErrKeyNotFound
	}
	return append([]byte(nil), rec.value...), nil
}

// KV is one result of a range scan.
type KV struct {
	Key   []byte
	Value []byte
}

const scanChunk = 512

// Scan visits every key in [start, end] in ascending order, calling fn for
// each. An empty end scans to the last key. limit <= 0 means unlimited.
//
// The index lock is taken and released once per chunk instead of being held
// for the whole scan, so a range query over millions of keys cannot stall
// writers. The flip side is that a scan is not a snapshot: concurrent writes
// to keys ahead of the cursor are visible.
func (s *Store) Scan(start, end []byte, limit int, fn func(key, value []byte) error) error {
	s.nScan.Add(1)
	if len(end) > 0 && string(start) > string(end) {
		return nil
	}
	type ref struct {
		key string
		e   entry
	}
	var (
		refs      = make([]ref, 0, scanChunk)
		cursor    = string(start)
		exclusive = false
		endKey    = string(end)
		sent      int
	)
	for {
		refs = refs[:0]
		s.mu.RLock()
		for n := s.order.seek(cursor); n != nil; n = n.next[0] {
			if exclusive && n.key == cursor {
				continue
			}
			if endKey != "" && n.key > endKey {
				break
			}
			refs = append(refs, ref{key: n.key, e: n.ent})
			if len(refs) == scanChunk {
				break
			}
		}
		s.mu.RUnlock()

		if len(refs) == 0 {
			return nil
		}
		for _, r := range refs {
			v, err := s.readValue(r.e)
			if err == errFileGone {
				// Compaction moved the record after we snapshotted the
				// locator. Re-resolve the key rather than dropping it from
				// the result, which would make a scan silently lossy.
				v, err = s.reread(r.key)
			}
			if err != nil {
				if err == errFileGone || err == ErrKeyNotFound {
					continue // deleted while the scan was in flight
				}
				return err
			}
			if err := fn([]byte(r.key), v); err != nil {
				return err
			}
			sent++
			if limit > 0 && sent >= limit {
				return nil
			}
		}
		cursor, exclusive = refs[len(refs)-1].key, true
		if len(refs) < scanChunk {
			return nil
		}
	}
}

// reread resolves a key again through the index. It is the recovery path for a
// locator that went stale because compaction relocated the record underneath it.
func (s *Store) reread(key string) ([]byte, error) {
	s.mu.RLock()
	n, ok := s.keys[key]
	var e entry
	if ok {
		e = n.ent
	}
	s.mu.RUnlock()
	if !ok {
		return nil, ErrKeyNotFound
	}
	return s.readValue(e)
}

// ScanSlice is the convenience form of Scan used by the network layer.
func (s *Store) ScanSlice(start, end []byte, limit int) ([]KV, error) {
	var out []KV
	err := s.Scan(start, end, limit, func(k, v []byte) error {
		out = append(out, KV{Key: append([]byte(nil), k...), Value: v})
		return nil
	})
	return out, err
}

// --- housekeeping -----------------------------------------------------------

func (s *Store) background() {
	defer s.wg.Done()
	syncTick := time.NewTicker(s.opts.SyncEvery)
	defer syncTick.Stop()
	compactTick := time.NewTicker(s.opts.CompactionInterval)
	defer compactTick.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-syncTick.C:
			if s.opts.SyncMode == SyncInterval {
				if err := s.Sync(); err != nil {
					s.logf("periodic fsync failed: %v", err)
				}
			}
		case <-compactTick.C:
			if s.shouldCompact() {
				s.StartCompaction()
			}
		}
	}
}

func (s *Store) Sync() error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if !s.dirty || s.active == nil {
		return nil
	}
	if err := s.active.f.Sync(); err != nil {
		return err
	}
	s.dirty = false
	return nil
}

func (s *Store) Seq() uint64 {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	return s.seq
}

func (s *Store) MergeSeq() uint64 { return s.mergeSeq.Load() }

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.keys)
}

type Stats struct {
	Keys        int
	Files       int
	LiveBytes   int64
	DeadBytes   int64
	Seq         uint64
	Puts        uint64
	Gets        uint64
	Deletes     uint64
	Scans       uint64
	Misses      uint64
	Reclaimable float64
	Compacting  bool
}

func (s *Store) Stats() Stats {
	s.mu.RLock()
	keys := len(s.keys)
	s.mu.RUnlock()
	s.fmu.RLock()
	files := len(s.files)
	s.fmu.RUnlock()
	live, dead := s.live.Load(), s.dead.Load()
	var ratio float64
	if live+dead > 0 {
		ratio = float64(dead) / float64(live+dead)
	}
	return Stats{
		Keys: keys, Files: files, LiveBytes: live, DeadBytes: dead,
		Seq: s.Seq(), Puts: s.nPut.Load(), Gets: s.nGet.Load(),
		Deletes: s.nDel.Load(), Scans: s.nScan.Load(), Misses: s.nMiss.Load(),
		Reclaimable: ratio, Compacting: s.compacting.Load(),
	}
}

func (s *Store) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	close(s.stop)

	// Flush under the write lock first: background work is only ever started
	// from inside that critical section, so once it is released with closed
	// set nothing new can be spawned and the wait below is safe.
	s.wmu.Lock()
	var err error
	if s.active != nil && s.dirty {
		err = s.active.f.Sync()
		s.dirty = false
	}
	s.wmu.Unlock()
	s.wg.Wait()

	s.closeFiles()
	releaseLock(s.lock, s.dir)
	return err
}

func (s *Store) closeFiles() {
	s.fmu.Lock()
	for _, df := range s.files {
		df.close()
	}
	s.files = map[uint32]*datafile{}
	s.fmu.Unlock()
}

// --- observers --------------------------------------------------------------

func (s *Store) AddObserver(o Observer) (cancel func()) {
	s.obsMu.Lock()
	s.obsID++
	id := s.obsID
	s.obs[id] = o
	s.obsMu.Unlock()
	return func() {
		s.obsMu.Lock()
		delete(s.obs, id)
		s.obsMu.Unlock()
	}
}

func (s *Store) notify(maxSeq uint64, raw []byte) {
	s.obsMu.RLock()
	if len(s.obs) == 0 {
		s.obsMu.RUnlock()
		return
	}
	for _, o := range s.obs {
		o(maxSeq, raw)
	}
	s.obsMu.RUnlock()
}

// --- replica-side apply -----------------------------------------------------

// ApplyRaw installs records produced by another node verbatim, preserving
// their sequence numbers and timestamps. A record older than what the index
// already holds for that key is written but loses, which makes replication
// idempotent and order-insensitive.
func (s *Store) ApplyRaw(raw []byte) (uint64, error) {
	var (
		recs []record
		off  int
		max  uint64
	)
	for off < len(raw) {
		if off+recordHeaderSize > len(raw) {
			return 0, ErrCorrupt
		}
		size := int(encodedSize(int(le32(raw[off+21:])), int(le32(raw[off+25:]))))
		if off+size > len(raw) {
			return 0, ErrCorrupt
		}
		rec, err := decodeRecord(raw[off : off+size])
		if err != nil {
			return 0, err
		}
		recs = append(recs, rec)
		if rec.seq > max {
			max = rec.seq
		}
		off += size
	}
	if len(recs) == 0 {
		return 0, nil
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.closed.Load() {
		return 0, ErrClosed
	}
	if err := s.writeLocked(recs); err != nil {
		return 0, err
	}
	if max > s.seq {
		s.seq = max
	}
	return max, nil
}

// Reset drops every key and every data file. It backs the full-resync path,
// where a replica has fallen so far behind that the leader's log no longer
// covers the gap.
func (s *Store) Reset() error {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	s.mu.Lock()
	s.keys = make(map[string]*slnode)
	s.order = newSkiplist()
	s.mu.Unlock()

	s.fmu.Lock()
	for id, df := range s.files {
		df.close()
		os.Remove(df.path)
		os.Remove(hintPath(s.dir, id))
	}
	s.files = map[uint32]*datafile{}
	s.fmu.Unlock()

	s.live.Store(0)
	s.dead.Store(0)
	s.seq = 0
	s.mergeSeq.Store(0)
	writeManifest(s.dir, 0)
	if err := s.openNewActive(); err != nil {
		return err
	}
	s.dirty = false
	return nil
}

func le32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
