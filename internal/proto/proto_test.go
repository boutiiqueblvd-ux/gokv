package proto

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	payloads := [][]byte{
		{},
		[]byte("hello"),
		bytes.Repeat([]byte{0x00, 0xff}, 5000),
	}
	for _, p := range payloads {
		if err := w.WriteFrame(p); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	r := NewReader(&buf)
	for i, want := range payloads {
		got, err := r.ReadFrame()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("frame %d round trip mismatch (%d vs %d bytes)", i, len(got), len(want))
		}
	}
	if _, err := r.ReadFrame(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestReaderRejectsOversizedFrame(t *testing.T) {
	var buf bytes.Buffer
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(MaxFrameSize)+1)
	buf.Write(hdr)
	if _, err := NewReader(&buf).ReadFrame(); err != ErrFrameTooLarge {
		t.Fatalf("got %v, want ErrFrameTooLarge", err)
	}
}

func TestEncodeDecodeBody(t *testing.T) {
	e := NewEnc(0x42, 0)
	e.U8(7)
	e.U32(1 << 20)
	e.U64(1 << 40)
	e.Bytes([]byte{0, 1, 2})
	e.Str("range")

	d := NewDec(e.B[1:])
	if got := d.U8(); got != 7 {
		t.Fatalf("U8 = %d", got)
	}
	if got := d.U32(); got != 1<<20 {
		t.Fatalf("U32 = %d", got)
	}
	if got := d.U64(); got != 1<<40 {
		t.Fatalf("U64 = %d", got)
	}
	if got := d.Bytes(); !bytes.Equal(got, []byte{0, 1, 2}) {
		t.Fatalf("Bytes = %v", got)
	}
	if got := d.Str(); got != "range" {
		t.Fatalf("Str = %q", got)
	}
	if err := d.Done(); err != nil {
		t.Fatalf("Done: %v", err)
	}
}

// A decoder must fail closed on a truncated body rather than panicking, since
// bodies arrive from the network.
func TestDecoderFailsClosedOnTruncation(t *testing.T) {
	e := NewEnc(0x01, 0)
	e.Bytes(bytes.Repeat([]byte("x"), 64))
	body := e.B[1:]

	for cut := 0; cut < len(body); cut++ {
		d := NewDec(body[:cut])
		d.Bytes()
		if d.Err() == nil {
			t.Fatalf("truncation at %d was not detected", cut)
		}
	}

	// A length prefix that overruns the frame must be rejected too.
	bad := make([]byte, 8)
	binary.BigEndian.PutUint32(bad, 1<<30)
	d := NewDec(bad)
	if d.Bytes(); d.Err() == nil {
		t.Fatal("oversized length inside a frame was accepted")
	}
}

func TestDoneRejectsTrailingBytes(t *testing.T) {
	d := NewDec([]byte{0, 0, 0, 0, 9, 9})
	d.U32()
	if err := d.Done(); err == nil {
		t.Fatal("trailing bytes accepted")
	}
}
