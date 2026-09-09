// Package client is the Go client for a gokv cluster.
//
// A client is given every node address. Reads may be served by any node;
// writes must reach the leader, and a follower that receives one answers with
// a redirect rather than an error, so the client discovers the leader by
// talking to whoever answers. Combined with retrying on a dead connection,
// that is what makes a leader failover invisible to calling code.
package client

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"gokv/internal/proto"
)

var (
	ErrKeyNotFound = errors.New("gokv: key not found")
	ErrNoNodes     = errors.New("gokv: no reachable node")
	ErrClosed      = errors.New("gokv: client is closed")
)

// RemoteError is an error reported by the server rather than by the transport.
type RemoteError struct{ Msg string }

func (e *RemoteError) Error() string { return "gokv: " + e.Msg }

type Options struct {
	// Timeout bounds a single request, including connecting.
	Timeout time.Duration
	// MaxAttempts bounds how many nodes a request may be retried against.
	MaxAttempts int
}

// Client is safe for concurrent use, but it serialises requests on a single
// connection. For parallel load, give each goroutine its own Client.
type Client struct {
	addrs   []string
	opts    Options
	mu      sync.Mutex
	conn    net.Conn
	r       *proto.Reader
	w       *proto.Writer
	current string
	next    int
	closed  bool
	// poisoned marks a connection abandoned part-way through a multi-frame
	// reply. The unread frames would otherwise be picked up as the answer to
	// the next request.
	poisoned bool
}

func New(addrs []string, opts Options) (*Client, error) {
	if len(addrs) == 0 {
		return nil, ErrNoNodes
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 2*len(addrs) + 2
	}
	return &Client{addrs: append([]string(nil), addrs...), opts: opts}, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return c.dropLocked()
}

func (c *Client) dropLocked() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn, c.r, c.w, c.current = nil, nil, nil, ""
	return err
}

// connectLocked attaches to a node, preferring the one a redirect pointed at
// and otherwise cycling through the configured addresses.
func (c *Client) connectLocked(prefer string) error {
	if c.conn != nil && (prefer == "" || prefer == c.current) {
		return nil
	}
	c.dropLocked()

	candidates := make([]string, 0, len(c.addrs)+1)
	if prefer != "" {
		candidates = append(candidates, prefer)
	}
	for i := 0; i < len(c.addrs); i++ {
		candidates = append(candidates, c.addrs[(c.next+i)%len(c.addrs)])
	}

	var lastErr error
	for _, addr := range candidates {
		conn, err := net.DialTimeout("tcp", addr, c.opts.Timeout)
		if err != nil {
			lastErr = err
			c.next++
			continue
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		c.conn, c.r, c.w, c.current = conn, proto.NewReader(conn), proto.NewWriter(conn), addr
		return nil
	}
	if lastErr == nil {
		lastErr = ErrNoNodes
	}
	return fmt.Errorf("%w: %v", ErrNoNodes, lastErr)
}

// call sends one request and lets handle consume the reply. handle may read
// more than one frame (range queries stream). A redirect or a transport
// failure is retried against another node.
func (c *Client) call(req []byte, handle func(first []byte, r *proto.Reader) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}

	prefer := ""
	var lastErr error
	for attempt := 0; attempt < c.opts.MaxAttempts; attempt++ {
		if err := c.connectLocked(prefer); err != nil {
			return err
		}
		prefer = ""
		c.conn.SetDeadline(time.Now().Add(c.opts.Timeout))

		frame, err := c.exchange(req)
		if err != nil {
			lastErr = err
			c.dropLocked()
			c.next++
			continue
		}
		switch proto.Status(frame[0]) {
		case proto.StatusRedirect:
			d := proto.NewDec(frame[1:])
			leader := d.Str()
			if leader == "" || leader == c.current {
				lastErr = errors.New("gokv: node has no leader yet")
				c.dropLocked()
				c.next++
				time.Sleep(50 * time.Millisecond)
				continue
			}
			prefer = leader
			continue
		case proto.StatusError:
			d := proto.NewDec(frame[1:])
			return &RemoteError{Msg: d.Str()}
		}
		err = handle(frame, c.r)
		if c.poisoned {
			c.dropLocked()
			c.poisoned = false
		}
		return err
	}
	if lastErr == nil {
		lastErr = ErrNoNodes
	}
	return lastErr
}

func (c *Client) exchange(req []byte) ([]byte, error) {
	if err := c.w.WriteFrame(req); err != nil {
		return nil, err
	}
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	frame, err := c.r.ReadFrame()
	if err != nil {
		return nil, err
	}
	if len(frame) == 0 {
		return nil, proto.ErrMalformed
	}
	return frame, nil
}

// Put stores value under key.
func (c *Client) Put(key, value []byte) error {
	e := proto.NewEnc(byte(proto.OpPut), len(key)+len(value)+8)
	e.Bytes(key)
	e.Bytes(value)
	return c.call(e.B, func(first []byte, _ *proto.Reader) error {
		if proto.Status(first[0]) != proto.StatusOK {
			return fmt.Errorf("gokv: unexpected status %#x for PUT", first[0])
		}
		return nil
	})
}

// Read returns the value stored under key, or ErrKeyNotFound.
func (c *Client) Read(key []byte) ([]byte, error) {
	e := proto.NewEnc(byte(proto.OpGet), len(key)+4)
	e.Bytes(key)
	var out []byte
	err := c.call(e.B, func(first []byte, _ *proto.Reader) error {
		if proto.Status(first[0]) == proto.StatusNotFound {
			return ErrKeyNotFound
		}
		d := proto.NewDec(first[1:])
		out = append([]byte(nil), d.Bytes()...)
		return d.Done()
	})
	return out, err
}

// Delete removes key. It returns ErrKeyNotFound if the key was not present.
func (c *Client) Delete(key []byte) error {
	e := proto.NewEnc(byte(proto.OpDelete), len(key)+4)
	e.Bytes(key)
	return c.call(e.B, func(first []byte, _ *proto.Reader) error {
		if proto.Status(first[0]) == proto.StatusNotFound {
			return ErrKeyNotFound
		}
		return nil
	})
}

// BatchPut writes many pairs in a single append and a single fsync.
func (c *Client) BatchPut(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return errors.New("gokv: BatchPut needs as many values as keys")
	}
	size := 8
	for i := range keys {
		size += len(keys[i]) + len(values[i]) + 8
	}
	e := proto.NewEnc(byte(proto.OpBatchPut), size)
	e.U32(uint32(len(keys)))
	for i := range keys {
		e.Bytes(keys[i])
		e.Bytes(values[i])
	}
	return c.call(e.B, func(first []byte, _ *proto.Reader) error {
		if proto.Status(first[0]) != proto.StatusOK {
			return fmt.Errorf("gokv: unexpected status %#x for BATCHPUT", first[0])
		}
		return nil
	})
}

// KV is one pair returned by a range query.
type KV struct {
	Key   []byte
	Value []byte
}

// ReadKeyRange returns every pair whose key falls in [startKey, endKey],
// in ascending key order. limit <= 0 asks for as much as the server allows.
func (c *Client) ReadKeyRange(startKey, endKey []byte, limit int) ([]KV, error) {
	var out []KV
	err := c.ScanKeyRange(startKey, endKey, limit, func(k, v []byte) error {
		out = append(out, KV{Key: append([]byte(nil), k...), Value: append([]byte(nil), v...)})
		return nil
	})
	return out, err
}

// ScanKeyRange streams a range without materialising it. fn must not retain
// the slices it is given.
func (c *Client) ScanKeyRange(startKey, endKey []byte, limit int, fn func(key, value []byte) error) error {
	e := proto.NewEnc(byte(proto.OpRange), len(startKey)+len(endKey)+16)
	e.Bytes(startKey)
	e.Bytes(endKey)
	e.U32(uint32(max(limit, 0)))
	return c.call(e.B, func(first []byte, r *proto.Reader) error {
		frame := first
		for {
			switch proto.Status(frame[0]) {
			case proto.StatusRangeEnd:
				return nil // the stream is finished; the connection is reusable
			case proto.StatusRangeChunk:
				d := proto.NewDec(frame[1:])
				for d.Err() == nil && !d.Empty() {
					k := d.Bytes()
					v := d.Bytes()
					if d.Err() != nil {
						break
					}
					if err := fn(k, v); err != nil {
						// The caller gave up mid-stream; the rest of the
						// reply is still in flight behind us.
						c.poisoned = true
						return err
					}
				}
				if err := d.Err(); err != nil {
					c.poisoned = true
					return err
				}
			case proto.StatusError:
				// The server terminated the stream itself, so nothing follows.
				d := proto.NewDec(frame[1:])
				return &RemoteError{Msg: d.Str()}
			default:
				c.poisoned = true
				return fmt.Errorf("gokv: unexpected status %#x in range reply", frame[0])
			}
			// Extend the deadline: a long range is many frames.
			c.conn.SetDeadline(time.Now().Add(c.opts.Timeout))
			var err error
			frame, err = r.ReadFrame()
			if err != nil {
				c.poisoned = true
				return err
			}
			if len(frame) == 0 {
				c.poisoned = true
				return proto.ErrMalformed
			}
		}
	})
}

func (c *Client) Ping() error {
	return c.call([]byte{byte(proto.OpPing)}, func(first []byte, _ *proto.Reader) error { return nil })
}

// Stats reports engine counters from whichever node answers.
func (c *Client) Stats() (map[string]string, error) { return c.pairs(proto.OpStats) }

// Info reports the cluster view of whichever node answers.
func (c *Client) Info() (map[string]string, error) { return c.pairs(proto.OpInfo) }

// InfoFrom reports the cluster view of one specific node, bypassing failover.
func InfoFrom(addr string, timeout time.Duration) (map[string]string, error) {
	c, err := New([]string{addr}, Options{Timeout: timeout, MaxAttempts: 1})
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.Info()
}

func (c *Client) pairs(op proto.Op) (map[string]string, error) {
	out := map[string]string{}
	err := c.call([]byte{byte(op)}, func(first []byte, _ *proto.Reader) error {
		d := proto.NewDec(first[1:])
		count := int(d.U32())
		for i := 0; i < count && d.Err() == nil; i++ {
			k := d.Str()
			out[k] = d.Str()
		}
		return d.Err()
	})
	return out, err
}

// Compact asks the node that answers to reclaim dead space now, and returns
// its stats afterwards.
func (c *Client) Compact() (map[string]string, error) { return c.pairs(proto.OpCompact) }

// Promote forces the node this client is attached to become the leader. It is
// an operator tool for demonstrating and testing failover.
func Promote(addr string, timeout time.Duration) error {
	c, err := New([]string{addr}, Options{Timeout: timeout, MaxAttempts: 1})
	if err != nil {
		return err
	}
	defer c.Close()
	return c.call([]byte{byte(proto.OpPromote)}, func(first []byte, _ *proto.Reader) error { return nil })
}
