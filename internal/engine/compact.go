package engine

import (
	"errors"
	"os"
	"sort"
	"time"
)

// ErrNeedFullSync is returned when a follower asks to resume from a position
// the leader can no longer serve, because compaction has reclaimed the
// tombstones in between.
var ErrNeedFullSync = errors.New("engine: log no longer covers that sequence, full sync required")

// buildHint writes the sidecar index for a file that has become immutable.
// Every record in the file is listed, including superseded ones and
// tombstones, so replaying the hint is exactly equivalent to rescanning the
// data file -- just without reading the values back.
func (s *Store) buildHint(id uint32) error {
	hw, err := newHintWriter(s.dir, id)
	if err != nil {
		return err
	}
	_, err = scanFile(dataPath(s.dir, id), func(pos int64, raw []byte, r *record) error {
		return hw.add(r.key, entry{pos: pos, seq: r.seq, fileID: id, size: uint32(len(raw))}, r.flags)
	})
	if err != nil {
		hw.abort()
		return err
	}
	if err := hw.commit(); err != nil {
		return err
	}
	return syncDir(s.dir)
}

func (s *Store) shouldCompact() bool {
	if s.closed.Load() {
		return false
	}
	s.fmu.RLock()
	files := len(s.files)
	s.fmu.RUnlock()
	if files < 2 {
		return false
	}
	live, dead := s.live.Load(), s.dead.Load()
	if live+dead == 0 {
		return false
	}
	return float64(dead)/float64(live+dead) >= s.opts.CompactionThreshold
}

// Compacting reports whether a background merge is in flight.
func (s *Store) Compacting() bool { return s.compacting.Load() }

// StartCompaction kicks off a merge in the background and reports whether it
// started one. Compaction of a large store takes minutes, which is far longer
// than any sensible request timeout, so callers over the network get an
// acknowledgement rather than a blocking call.
func (s *Store) StartCompaction() bool {
	if !s.compacting.CompareAndSwap(false, true) {
		return false
	}
	go func() {
		defer s.compacting.Store(false)
		if err := s.Compact(); err != nil && !errors.Is(err, ErrClosed) {
			s.logf("compaction failed: %v", err)
		}
	}()
	return true
}

func (s *Store) allocFileID() uint32 {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	id := s.nextID
	s.nextID++
	return id
}

// Compact rewrites every immutable data file into fresh files that contain
// only the records still reachable from the index, then deletes the originals.
//
// It merges the whole immutable set at once rather than picking victims. That
// costs more I/O per pass, but it is the condition under which tombstones can
// be dropped safely: once no older file survives, there is nothing left for a
// missing tombstone to resurrect.
//
// Writers are never blocked. The merge holds only short locks -- one to
// snapshot the index, and one per record to repoint it -- and it re-checks
// each key before repointing, so a value overwritten mid-merge keeps the newer
// location.
func (s *Store) Compact() error {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	start := time.Now()

	// Seal the active file so the merge set is unambiguous.
	s.wmu.Lock()
	if s.active.size.Load() > 0 {
		if err := s.active.f.Sync(); err != nil {
			s.wmu.Unlock()
			return err
		}
		if err := s.openNewActive(); err != nil {
			s.wmu.Unlock()
			return err
		}
		s.dirty = false
	}
	barrier := s.seq
	activeID := s.active.id
	s.wmu.Unlock()

	s.fmu.RLock()
	mergeIDs := make([]uint32, 0, len(s.files))
	for id := range s.files {
		if id != activeID {
			mergeIDs = append(mergeIDs, id)
		}
	}
	s.fmu.RUnlock()
	if len(mergeIDs) == 0 {
		return nil
	}
	sort.Slice(mergeIDs, func(i, j int) bool { return mergeIDs[i] < mergeIDs[j] })
	inMerge := make(map[uint32]bool, len(mergeIDs))
	for _, id := range mergeIDs {
		inMerge[id] = true
	}

	// Snapshot the live keys that still live in the merge set, in key order.
	// Writing them out in that order also leaves merged files roughly sorted,
	// which makes later range scans read mostly sequentially.
	type item struct {
		key string
		e   entry
	}
	items := make([]item, 0, 1024)
	s.mu.RLock()
	s.order.scan("", "", func(n *slnode) bool {
		if inMerge[n.ent.fileID] {
			items = append(items, item{key: n.key, e: n.ent})
		}
		return true
	})
	s.mu.RUnlock()

	var (
		out    *datafile
		hw     *hintWriter
		outs   []*datafile
		hws    []*hintWriter
		copied int
	)
	newOut := func() error {
		id := s.allocFileID()
		df, err := createDataFile(s.dir, id)
		if err != nil {
			return err
		}
		h, err := newHintWriter(s.dir, id)
		if err != nil {
			df.close()
			os.Remove(df.path)
			return err
		}
		s.fmu.Lock()
		s.files[id] = df
		s.fmu.Unlock()
		out, hw = df, h
		outs, hws = append(outs, df), append(hws, h)
		return nil
	}
	cleanup := func() {
		for _, h := range hws {
			h.abort()
		}
		s.fmu.Lock()
		for _, df := range outs {
			delete(s.files, df.id)
			df.close()
			os.Remove(df.path)
		}
		s.fmu.Unlock()
	}

	for _, it := range items {
		s.fmu.RLock()
		src := s.files[it.e.fileID]
		if src == nil {
			s.fmu.RUnlock()
			continue
		}
		buf := make([]byte, it.e.size)
		_, rerr := src.f.ReadAt(buf, it.e.pos)
		s.fmu.RUnlock()
		if rerr != nil {
			cleanup()
			return rerr
		}
		rec, err := decodeRecord(buf)
		if err != nil {
			// Refuse to propagate a record we cannot verify; leave the
			// original file in place so the damage stays visible.
			cleanup()
			return err
		}
		if out == nil || out.size.Load()+int64(len(buf)) > s.opts.MaxFileSize {
			if err := newOut(); err != nil {
				cleanup()
				return err
			}
		}
		pos := out.size.Load()
		if _, err := out.f.WriteAt(buf, pos); err != nil {
			cleanup()
			return err
		}
		out.size.Add(int64(len(buf)))
		newEnt := entry{pos: pos, seq: rec.seq, fileID: out.id, size: uint32(len(buf))}
		if err := hw.add(rec.key, newEnt, rec.flags); err != nil {
			cleanup()
			return err
		}
		copied++

		// Repoint the index, unless a concurrent write already replaced this
		// version of the key.
		s.mu.Lock()
		if n, ok := s.keys[it.key]; ok && n.ent.seq == it.e.seq && n.ent.fileID == it.e.fileID {
			n.ent = newEnt
		}
		s.mu.Unlock()
	}

	for _, df := range outs {
		if err := df.f.Sync(); err != nil {
			cleanup()
			return err
		}
	}
	for _, h := range hws {
		if err := h.commit(); err != nil {
			cleanup()
			return err
		}
	}
	if err := syncDir(s.dir); err != nil {
		cleanup()
		return err
	}
	// Only now is it safe to say the old tombstones are gone.
	s.mergeSeq.Store(barrier)
	if err := writeManifest(s.dir, barrier); err != nil {
		s.logf("could not persist merge watermark: %v", err)
	}

	// Readers hold fmu for the whole of a read, so taking it exclusively here
	// guarantees nobody is mid-read on a file we are about to close.
	s.fmu.Lock()
	for _, id := range mergeIDs {
		if df, ok := s.files[id]; ok {
			df.close()
			os.Remove(df.path)
			delete(s.files, id)
		}
		os.Remove(hintPath(s.dir, id))
	}
	s.fmu.Unlock()
	syncDir(s.dir)

	s.recomputeUsage()
	s.logf("compacted %d files into %d (%d live records) in %s",
		len(mergeIDs), len(outs), copied, time.Since(start).Round(time.Millisecond))
	return nil
}

// recomputeUsage resets the live/dead accounting from ground truth after a
// merge, rather than trying to unwind the incremental counters.
func (s *Store) recomputeUsage() {
	var live int64
	s.mu.RLock()
	for _, n := range s.keys {
		live += int64(n.ent.size)
	}
	s.mu.RUnlock()

	var total int64
	s.fmu.RLock()
	for _, df := range s.files {
		total += df.size.Load()
	}
	s.fmu.RUnlock()

	s.live.Store(live)
	dead := total - live
	if dead < 0 {
		dead = 0
	}
	s.dead.Store(dead)
}

// --- replication sources ----------------------------------------------------

// RecordsSince streams every record newer than fromSeq, in file order. The
// receiver resolves ordering by sequence number, so file order is good enough.
func (s *Store) RecordsSince(fromSeq uint64, fn func(raw []byte) error) error {
	if fromSeq < s.mergeSeq.Load() {
		return ErrNeedFullSync
	}
	s.fmu.RLock()
	ids := make([]uint32, 0, len(s.files))
	for id := range s.files {
		ids = append(ids, id)
	}
	s.fmu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		_, err := scanFile(dataPath(s.dir, id), func(pos int64, raw []byte, r *record) error {
			if r.seq <= fromSeq {
				return nil
			}
			return fn(raw)
		})
		if err != nil {
			if os.IsNotExist(err) {
				continue // compacted away mid-stream
			}
			return err
		}
	}
	return nil
}

// rawRecord returns the encoded bytes of the record a locator points at,
// retrying through the index if compaction relocated it in the meantime.
func (s *Store) rawRecord(key string, e entry) ([]byte, error) {
	for attempt := 0; attempt < 3; attempt++ {
		s.fmu.RLock()
		df := s.files[e.fileID]
		if df == nil {
			s.fmu.RUnlock()
		} else {
			buf := make([]byte, e.size)
			_, err := df.f.ReadAt(buf, e.pos)
			s.fmu.RUnlock()
			if err == nil {
				if _, derr := decodeRecord(buf); derr == nil {
					return buf, nil
				}
			}
		}
		s.mu.RLock()
		n, ok := s.keys[key]
		if ok {
			e = n.ent
		}
		s.mu.RUnlock()
		if !ok {
			return nil, ErrKeyNotFound
		}
	}
	return nil, errFileGone
}

// SnapshotRecords streams the current live state, one record per key, in key
// order. It is what a follower receives when it is too far behind to catch up
// from the log.
func (s *Store) SnapshotRecords(fn func(raw []byte) error) error {
	const chunk = 512
	type ref struct {
		key string
		e   entry
	}
	refs := make([]ref, 0, chunk)
	cursor, exclusive := "", false
	for {
		refs = refs[:0]
		s.mu.RLock()
		for n := s.order.seek(cursor); n != nil; n = n.next[0] {
			if exclusive && n.key == cursor {
				continue
			}
			refs = append(refs, ref{key: n.key, e: n.ent})
			if len(refs) == chunk {
				break
			}
		}
		s.mu.RUnlock()
		if len(refs) == 0 {
			return nil
		}
		for _, r := range refs {
			// Re-resolve on every record: a snapshot is only useful to a
			// follower if it is complete, so a locator invalidated by a
			// concurrent merge must be looked up again, not skipped.
			buf, err := s.rawRecord(r.key, r.e)
			if err != nil {
				if err == ErrKeyNotFound || err == errFileGone {
					continue
				}
				return err
			}
			if err := fn(buf); err != nil {
				return err
			}
		}
		cursor, exclusive = refs[len(refs)-1].key, true
		if len(refs) < chunk {
			return nil
		}
	}
}
