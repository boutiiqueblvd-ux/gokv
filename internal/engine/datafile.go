package engine

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

const (
	dataSuffix = ".data"
	hintSuffix = ".hint"
	hintMagic  = "GOKVHNT1"

	// hintHeaderSize is the fixed part of a hint record:
	// seq(8) flags(1) pos(8) size(4) keyLen(4).
	hintHeaderSize = 25
)

// entry is what the in-memory index stores per key: enough to find the record
// on disk and nothing else. A small fixed-size locator plus the key itself is
// what keeps a dataset far larger than RAM addressable.
//
// Deleted keys keep an entry too, with tomb set. That costs memory until the
// next compaction, and it buys the invariant everything else depends on: the
// index always knows the sequence number of the newest record it has seen for
// a key, so an older record arriving later -- out of a merged file during
// recovery, or out of order on a replication stream -- can be recognised as
// stale instead of resurrecting a deleted key.
type entry struct {
	pos    int64  // byte offset of the record inside its data file
	seq    uint64 // sequence number of the record
	fileID uint32
	size   uint32 // full encoded record size
	tomb   bool   // the record is a tombstone
}

type datafile struct {
	id   uint32
	path string
	f    *os.File
	// size is the append offset. Only the writer mutates it (under Store.wmu)
	// but compaction and stats read it from other goroutines, so it is atomic.
	size atomic.Int64
}

func dataPath(dir string, id uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%010d%s", id, dataSuffix))
}

func hintPath(dir string, id uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%010d%s", id, hintSuffix))
}

func createDataFile(dir string, id uint32) (*datafile, error) {
	p := dataPath(dir, id)
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	return &datafile{id: id, path: p, f: f}, nil
}

func openDataFile(dir string, id uint32, write bool) (*datafile, error) {
	p := dataPath(dir, id)
	flag := os.O_RDONLY
	if write {
		flag = os.O_RDWR
	}
	f, err := os.OpenFile(p, flag, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	df := &datafile{id: id, path: p, f: f}
	df.size.Store(st.Size())
	return df, nil
}

func (d *datafile) close() error {
	if d.f == nil {
		return nil
	}
	return d.f.Close()
}

// readRecord pulls a full record back off disk and re-verifies its checksum,
// so silent bit rot surfaces as an error instead of a wrong answer.
func (d *datafile) readRecord(e entry) (record, []byte, error) {
	buf := make([]byte, e.size)
	if _, err := d.f.ReadAt(buf, e.pos); err != nil {
		return record{}, nil, err
	}
	rec, err := decodeRecord(buf)
	if err != nil {
		return record{}, nil, fmt.Errorf("%w in %s at %d", err, filepath.Base(d.path), e.pos)
	}
	return rec, buf, nil
}

// listDataFiles returns the ids of every data file in dir, ascending.
func listDataFiles(dir string) ([]uint32, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint32
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, dataSuffix) {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(name, dataSuffix), 10, 32)
		if err != nil {
			continue
		}
		ids = append(ids, uint32(n))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// --- hint files -------------------------------------------------------------
//
// A hint file is a compact sidecar holding only what the index needs, so
// restarting a node does not have to read back every value. It is written to a
// temporary name and renamed into place, so a hint file that exists is always
// complete. If it is missing we simply scan the data file instead.

type hintWriter struct {
	f    *os.File
	bw   *bufio.Writer
	tmp  string
	dest string
	buf  []byte
}

func newHintWriter(dir string, id uint32) (*hintWriter, error) {
	dest := hintPath(dir, id)
	tmp := dest + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	if _, err := bw.WriteString(hintMagic); err != nil {
		f.Close()
		os.Remove(tmp)
		return nil, err
	}
	return &hintWriter{f: f, bw: bw, tmp: tmp, dest: dest}, nil
}

func (h *hintWriter) add(key []byte, e entry, flags uint8) error {
	if cap(h.buf) < hintHeaderSize {
		h.buf = make([]byte, hintHeaderSize)
	}
	b := h.buf[:hintHeaderSize]
	binary.LittleEndian.PutUint64(b[0:8], e.seq)
	b[8] = flags
	binary.LittleEndian.PutUint64(b[9:17], uint64(e.pos))
	binary.LittleEndian.PutUint32(b[17:21], e.size)
	binary.LittleEndian.PutUint32(b[21:25], uint32(len(key)))
	if _, err := h.bw.Write(b); err != nil {
		return err
	}
	_, err := h.bw.Write(key)
	return err
}

func (h *hintWriter) commit() error {
	if err := h.bw.Flush(); err != nil {
		h.abort()
		return err
	}
	if err := h.f.Sync(); err != nil {
		h.abort()
		return err
	}
	if err := h.f.Close(); err != nil {
		os.Remove(h.tmp)
		return err
	}
	return os.Rename(h.tmp, h.dest)
}

func (h *hintWriter) abort() {
	h.f.Close()
	os.Remove(h.tmp)
}

// readHint replays a hint file. Any parse problem returns an error and the
// caller falls back to scanning the data file, so a bad hint is never fatal.
func readHint(dir string, id uint32, fn func(key []byte, e entry, flags uint8)) error {
	f, err := os.Open(hintPath(dir, id))
	if err != nil {
		return err
	}
	defer f.Close()

	br := bufio.NewReaderSize(f, 1<<20)
	magic := make([]byte, len(hintMagic))
	if _, err := io.ReadFull(br, magic); err != nil || string(magic) != hintMagic {
		return fmt.Errorf("engine: bad hint header for file %d", id)
	}
	var hdr [hintHeaderSize]byte
	key := make([]byte, 0, 64)
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		keyLen := binary.LittleEndian.Uint32(hdr[21:25])
		if keyLen == 0 || keyLen > MaxKeySize {
			return fmt.Errorf("engine: bad hint key length in file %d", id)
		}
		if cap(key) < int(keyLen) {
			key = make([]byte, keyLen)
		}
		key = key[:keyLen]
		if _, err := io.ReadFull(br, key); err != nil {
			return err
		}
		fn(key, entry{
			seq:    binary.LittleEndian.Uint64(hdr[0:8]),
			pos:    int64(binary.LittleEndian.Uint64(hdr[9:17])),
			size:   binary.LittleEndian.Uint32(hdr[17:21]),
			fileID: id,
		}, hdr[8])
	}
}

// removeOrphanHints deletes hint files with no matching data file, and any
// half-written temporary hint left behind by a crash.
func removeOrphanHints(dir string, dataIDs []uint32) error {
	live := make(map[uint32]bool, len(dataIDs))
	for _, id := range dataIDs {
		live[id] = true
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		switch {
		case strings.HasSuffix(name, hintSuffix+".tmp"):
			os.Remove(filepath.Join(dir, name))
		case strings.HasSuffix(name, hintSuffix):
			n, err := strconv.ParseUint(strings.TrimSuffix(name, hintSuffix), 10, 32)
			if err != nil || !live[uint32(n)] {
				os.Remove(filepath.Join(dir, name))
			}
		}
	}
	return nil
}

func hintExists(dir string, id uint32) bool {
	st, err := os.Stat(hintPath(dir, id))
	return err == nil && !st.IsDir()
}
