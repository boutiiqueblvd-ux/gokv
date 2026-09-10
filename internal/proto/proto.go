// Package proto defines the wire format spoken between clients and nodes, and
// between nodes themselves.
//
// Every message is a length-prefixed frame: a 4-byte big-endian length
// followed by that many bytes. The first byte of a request frame is an opcode;
// the first byte of a response frame is a status. Lengths are explicit rather
// than delimiter-based so keys and values can hold arbitrary binary data.
package proto

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MaxFrameSize bounds a single message so a bad length prefix cannot make a
// node allocate unbounded memory.
const MaxFrameSize = 320 << 20

type Op byte

const (
	OpPing     Op = 0x01
	OpPut      Op = 0x02
	OpGet      Op = 0x03
	OpDelete   Op = 0x04
	OpRange    Op = 0x05
	OpBatchPut Op = 0x06
	OpStats    Op = 0x07
	OpInfo     Op = 0x08
	OpCompact  Op = 0x09

	// Cluster-internal opcodes.
	OpReplicate Op = 0x20 // follower -> leader: stream me everything after seq N
	OpVote      Op = 0x21 // candidate -> peer: what do you see?
	OpAnnounce  Op = 0x22 // new leader -> peers: I am the leader for epoch E
	OpPromote   Op = 0x23 // operator -> node: become the leader now
)

func (o Op) String() string {
	switch o {
	case OpPing:
		return "PING"
	case OpPut:
		return "PUT"
	case OpGet:
		return "GET"
	case OpDelete:
		return "DELETE"
	case OpRange:
		return "RANGE"
	case OpBatchPut:
		return "BATCHPUT"
	case OpStats:
		return "STATS"
	case OpInfo:
		return "INFO"
	case OpCompact:
		return "COMPACT"
	case OpReplicate:
		return "REPLICATE"
	case OpVote:
		return "VOTE"
	case OpAnnounce:
		return "ANNOUNCE"
	case OpPromote:
		return "PROMOTE"
	}
	return fmt.Sprintf("OP(%#x)", byte(o))
}

type Status byte

const (
	StatusOK       Status = 0x00
	StatusNotFound Status = 0x01
	StatusError    Status = 0x02
	// StatusRedirect carries the address of the node that can accept writes.
	StatusRedirect Status = 0x03

	// Range replies stream: zero or more chunks then exactly one end frame.
	StatusRangeChunk Status = 0x04
	StatusRangeEnd   Status = 0x05

	// Replication stream frames.
	StatusReplData  Status = 0x06 // body is one or more raw engine records
	StatusFullSync  Status = 0x07 // discard local state, a snapshot follows
	StatusHeartbeat Status = 0x08
	// StatusLive marks the end of catch-up: everything after it arrives as it
	// is written, so the follower can go back to a tight liveness deadline.
	StatusLive Status = 0x09
)

var (
	ErrFrameTooLarge = errors.New("proto: frame exceeds maximum size")
	ErrMalformed     = errors.New("proto: malformed frame")
)

// --- framing ----------------------------------------------------------------

type Writer struct {
	bw  *bufio.Writer
	hdr [4]byte
}

func NewWriter(w io.Writer) *Writer { return &Writer{bw: bufio.NewWriterSize(w, 64<<10)} }

// WriteFrame writes one frame. It does not flush; call Flush when a logical
// reply is complete so several small frames can share a single syscall.
func (w *Writer) WriteFrame(payload []byte) error {
	if len(payload) > MaxFrameSize {
		return ErrFrameTooLarge
	}
	binary.BigEndian.PutUint32(w.hdr[:], uint32(len(payload)))
	if _, err := w.bw.Write(w.hdr[:]); err != nil {
		return err
	}
	_, err := w.bw.Write(payload)
	return err
}

func (w *Writer) Flush() error { return w.bw.Flush() }

type Reader struct {
	br  *bufio.Reader
	hdr [4]byte
	buf []byte
}

func NewReader(r io.Reader) *Reader { return &Reader{br: bufio.NewReaderSize(r, 64<<10)} }

// ReadFrame returns the next frame. The returned slice is reused by the next
// call, so callers that keep the bytes must copy them.
func (r *Reader) ReadFrame() ([]byte, error) {
	if _, err := io.ReadFull(r.br, r.hdr[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(r.hdr[:]))
	if n > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}
	if cap(r.buf) < n {
		r.buf = make([]byte, n)
	}
	r.buf = r.buf[:n]
	if _, err := io.ReadFull(r.br, r.buf); err != nil {
		return nil, err
	}
	return r.buf, nil
}

// --- payload encoding -------------------------------------------------------

// Enc builds a frame body.
type Enc struct{ B []byte }

func NewEnc(first byte, hint int) *Enc {
	e := &Enc{B: make([]byte, 0, hint+1)}
	e.B = append(e.B, first)
	return e
}

func (e *Enc) U8(v byte) { e.B = append(e.B, v) }
func (e *Enc) U32(v uint32) {
	e.B = binary.BigEndian.AppendUint32(e.B, v)
}
func (e *Enc) U64(v uint64) {
	e.B = binary.BigEndian.AppendUint64(e.B, v)
}
func (e *Enc) Bytes(b []byte) {
	e.U32(uint32(len(b)))
	e.B = append(e.B, b...)
}
func (e *Enc) Str(s string) {
	e.U32(uint32(len(s)))
	e.B = append(e.B, s...)
}
func (e *Enc) Raw(b []byte) { e.B = append(e.B, b...) }

// Dec reads a frame body. Once an error occurs every later call is a no-op, so
// a decoder can be driven straight through and checked once at the end.
type Dec struct {
	B   []byte
	pos int
	err error
}

func NewDec(b []byte) *Dec { return &Dec{B: b} }

func (d *Dec) fail() { d.err = ErrMalformed }

func (d *Dec) U8() byte {
	if d.err != nil || d.pos+1 > len(d.B) {
		d.fail()
		return 0
	}
	v := d.B[d.pos]
	d.pos++
	return v
}

func (d *Dec) U32() uint32 {
	if d.err != nil || d.pos+4 > len(d.B) {
		d.fail()
		return 0
	}
	v := binary.BigEndian.Uint32(d.B[d.pos:])
	d.pos += 4
	return v
}

func (d *Dec) U64() uint64 {
	if d.err != nil || d.pos+8 > len(d.B) {
		d.fail()
		return 0
	}
	v := binary.BigEndian.Uint64(d.B[d.pos:])
	d.pos += 8
	return v
}

// Bytes returns a sub-slice of the frame buffer; it is only valid until the
// next ReadFrame.
func (d *Dec) Bytes() []byte {
	n := int(d.U32())
	if d.err != nil || n < 0 || d.pos+n > len(d.B) {
		d.fail()
		return nil
	}
	v := d.B[d.pos : d.pos+n]
	d.pos += n
	return v
}

func (d *Dec) Str() string { return string(d.Bytes()) }

func (d *Dec) Rest() []byte {
	if d.err != nil {
		return nil
	}
	v := d.B[d.pos:]
	d.pos = len(d.B)
	return v
}

func (d *Dec) Err() error { return d.err }

func (d *Dec) Done() error {
	if d.err != nil {
		return d.err
	}
	if d.pos != len(d.B) {
		return fmt.Errorf("%w: %d trailing bytes", ErrMalformed, len(d.B)-d.pos)
	}
	return nil
}

// Remaining reports how many bytes of the body are still unread. Handlers use
// it to sanity-check a count field before allocating anything sized by it.
func (d *Dec) Remaining() int {
	if d.err != nil || d.pos > len(d.B) {
		return 0
	}
	return len(d.B) - d.pos
}

// Empty reports whether the decoder has consumed the whole body.
func (d *Dec) Empty() bool { return d.pos >= len(d.B) }
