package engine

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// On-disk record layout. All integers are little-endian.
//
//	offset  size  field
//	0       4     crc32 (IEEE) of every byte after this field
//	4       8     timestamp, unix nanoseconds
//	12      8     sequence number, globally monotonic across the whole log
//	20      1     flags
//	21      4     key length
//	25      4     value length
//	29      ..    key bytes followed by value bytes
//
// The sequence number is what makes recovery order-independent: whichever
// record for a key carries the highest sequence wins, no matter which file it
// lives in. That in turn lets compaction write merged files with fresh (high)
// file ids without corrupting recovery.
const (
	recordHeaderSize = 29

	flagTombstone = 1 << 0 // the record deletes the key
	flagBatch     = 1 << 1 // the record belongs to a BatchPut
	flagBatchEnd  = 1 << 2 // the record is the last one of a BatchPut

	// MaxKeySize and MaxValueSize bound a single item. They exist so a
	// corrupt length field cannot make us allocate gigabytes during recovery.
	MaxKeySize   = 64 << 10  // 64 KiB
	MaxValueSize = 256 << 20 // 256 MiB
)

var (
	// ErrCorrupt is reported when a record fails its checksum or carries
	// implausible lengths. Recovery treats it as end-of-file.
	ErrCorrupt = errors.New("engine: corrupt record")
	errShort   = errors.New("engine: short record")
)

type record struct {
	ts    int64
	seq   uint64
	flags uint8
	key   []byte
	value []byte
}

func (r *record) tombstone() bool { return r.flags&flagTombstone != 0 }

func encodedSize(keyLen, valLen int) uint32 {
	return uint32(recordHeaderSize + keyLen + valLen)
}

// appendRecord encodes r onto dst and returns the extended slice.
func appendRecord(dst []byte, r *record) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, recordHeaderSize)...)
	dst = append(dst, r.key...)
	dst = append(dst, r.value...)
	buf := dst[start:]
	binary.LittleEndian.PutUint64(buf[4:12], uint64(r.ts))
	binary.LittleEndian.PutUint64(buf[12:20], r.seq)
	buf[20] = r.flags
	binary.LittleEndian.PutUint32(buf[21:25], uint32(len(r.key)))
	binary.LittleEndian.PutUint32(buf[25:29], uint32(len(r.value)))
	binary.LittleEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))
	return dst
}

// decodeRecord parses a complete encoded record and verifies its checksum.
// The returned key and value alias buf.
func decodeRecord(buf []byte) (record, error) {
	if len(buf) < recordHeaderSize {
		return record{}, errShort
	}
	keyLen := binary.LittleEndian.Uint32(buf[21:25])
	valLen := binary.LittleEndian.Uint32(buf[25:29])
	if int64(recordHeaderSize)+int64(keyLen)+int64(valLen) != int64(len(buf)) {
		return record{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != crc32.ChecksumIEEE(buf[4:]) {
		return record{}, ErrCorrupt
	}
	return record{
		ts:    int64(binary.LittleEndian.Uint64(buf[4:12])),
		seq:   binary.LittleEndian.Uint64(buf[12:20]),
		flags: buf[20],
		key:   buf[recordHeaderSize : recordHeaderSize+keyLen],
		value: buf[recordHeaderSize+keyLen:],
	}, nil
}

// scanFile walks a data file from the beginning, handing every intact record
// to fn. It stops at the first record that is truncated or fails its checksum,
// which is the normal outcome for a file that was open when the process died.
// The returned offset is the end of the last intact record.
func scanFile(path string, fn func(pos int64, raw []byte, r *record) error) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	br := bufio.NewReaderSize(f, 1<<20)
	var (
		pos int64
		hdr [recordHeaderSize]byte
		buf []byte
	)
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			// A clean EOF, or a header torn by a crash. Either way the file
			// ends here as far as we are concerned.
			return pos, nil
		}
		keyLen := binary.LittleEndian.Uint32(hdr[21:25])
		valLen := binary.LittleEndian.Uint32(hdr[25:29])
		if keyLen == 0 || keyLen > MaxKeySize || valLen > MaxValueSize {
			return pos, nil
		}
		size := recordHeaderSize + int(keyLen) + int(valLen)
		if cap(buf) < size {
			buf = make([]byte, size)
		}
		buf = buf[:size]
		copy(buf, hdr[:])
		if _, err := io.ReadFull(br, buf[recordHeaderSize:]); err != nil {
			return pos, nil
		}
		rec, err := decodeRecord(buf)
		if err != nil {
			return pos, nil
		}
		if err := fn(pos, buf, &rec); err != nil {
			return pos, err
		}
		pos += int64(size)
	}
}
