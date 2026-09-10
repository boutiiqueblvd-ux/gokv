// Package node wires the storage engine to the network: a TCP server speaking
// the proto wire format, plus leader/follower replication and a simple
// automatic failover.
package node

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"gokv/internal/engine"
	"gokv/internal/proto"
)

type Role int32

const (
	Follower Role = iota
	Leader
)

func (r Role) String() string {
	if r == Leader {
		return "leader"
	}
	return "follower"
}

type Config struct {
	ID   string // stable identifier; defaults to Addr
	Addr string // address to listen on and to advertise to peers
	Dir  string // data directory

	Peers     []string // every node in the cluster, including this one
	Bootstrap bool     // start life as the leader
	Leader    string   // initial leader address for a follower

	// Listener, when set, is used instead of binding Addr. It lets a
	// supervisor (or a test) reserve the port before the node starts, so
	// nothing can take it in between.
	Listener net.Listener

	Engine            engine.Options
	HeartbeatInterval time.Duration // leader -> follower keepalive
	ElectionTimeout   time.Duration // silence before a follower starts an election
	MaxRangeResults   int           // server-side cap on a single range query
	Logger            *log.Logger
}

func (c *Config) applyDefaults() {
	if c.Addr == "" {
		c.Addr = "127.0.0.1:7070"
	}
	if c.ID == "" {
		c.ID = c.Addr
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = time.Second
	}
	if c.ElectionTimeout <= 0 {
		c.ElectionTimeout = 5 * time.Second
	}
	if c.MaxRangeResults <= 0 {
		c.MaxRangeResults = 100000
	}
	if c.Logger == nil {
		c.Logger = log.New(os.Stderr, "["+c.ID+"] ", log.LstdFlags|log.Lmicroseconds)
	}
	c.Engine.Dir = c.Dir
	if c.Engine.Logger == nil {
		c.Engine.Logger = c.Logger
	}
}

type Node struct {
	cfg   Config
	store *engine.Store
	ln    net.Listener

	mu          sync.RWMutex
	role        Role
	leaderAddr  string
	epoch       uint64
	lastContact time.Time
	wasLeader   bool // set when demoted, forces a full resync

	streamMu sync.Mutex
	streams  map[*replStream]struct{}

	// connMu guards the set of accepted connections so shutdown can unblock
	// handler goroutines that are parked in a read.
	connMu sync.Mutex
	conns  map[net.Conn]struct{}

	electing atomic.Bool
	closing  atomic.Bool
	stop     chan struct{}
	wg       sync.WaitGroup

	cancelObserver func()
	startedAt      time.Time
	nConns         atomic.Int64
}

func New(cfg Config) (*Node, error) {
	cfg.applyDefaults()
	st, err := engine.Open(cfg.Engine)
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:       cfg,
		store:     st,
		streams:   make(map[*replStream]struct{}),
		conns:     make(map[net.Conn]struct{}),
		stop:      make(chan struct{}),
		startedAt: time.Now(),
	}
	// Every node starts at epoch 1 so that the first promotion is always a
	// strictly higher epoch than the bootstrap leader's.
	n.epoch = 1
	if cfg.Bootstrap || len(cfg.Peers) <= 1 {
		n.role = Leader
		n.leaderAddr = cfg.Addr
	} else {
		n.role = Follower
		n.leaderAddr = cfg.Leader
	}
	n.lastContact = time.Now()
	// Every accepted write is fanned out to the live replication streams from
	// inside the engine's write lock, which is what keeps followers in the
	// same order as the leader's log.
	n.cancelObserver = st.AddObserver(n.fanout)
	return n, nil
}

func (n *Node) Store() *engine.Store { return n.store }
func (n *Node) Addr() string {
	if n.ln != nil {
		return n.ln.Addr().String()
	}
	return n.cfg.Addr
}

func (n *Node) Role() Role {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role
}

func (n *Node) LeaderAddr() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderAddr
}

func (n *Node) Epoch() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.epoch
}

func (n *Node) logf(format string, args ...any) { n.cfg.Logger.Printf(format, args...) }

// Start begins listening and launches the background replication machinery.
func (n *Node) Start() error {
	ln := n.cfg.Listener
	if ln == nil {
		var err error
		ln, err = net.Listen("tcp", n.cfg.Addr)
		if err != nil {
			n.store.Close()
			return err
		}
	}
	n.ln = ln
	n.logf("listening on %s as %s (epoch %d, %d keys)", ln.Addr(), n.Role(), n.Epoch(), n.store.Len())

	n.wg.Add(1)
	go n.acceptLoop()
	n.wg.Add(1)
	go n.followLoop()
	return nil
}

func (n *Node) Close() error {
	if n.closing.Swap(true) {
		return nil
	}
	close(n.stop)
	if n.ln != nil {
		n.ln.Close()
	}
	n.closeAllStreams()
	n.closeAllConns()
	n.wg.Wait()
	n.cancelObserver()
	return n.store.Close()
}

func (n *Node) acceptLoop() {
	defer n.wg.Done()
	for {
		conn, err := n.ln.Accept()
		if err != nil {
			if n.closing.Load() {
				return
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			n.logf("accept: %v", err)
			return
		}
		if !n.trackConn(conn) {
			conn.Close()
			return
		}
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			defer n.untrackConn(conn)
			n.nConns.Add(1)
			defer n.nConns.Add(-1)
			n.serveConn(conn)
		}()
	}
}

// trackConn registers a connection for shutdown. It reports false if the node
// is already closing, so no connection can be missed by Close.
func (n *Node) trackConn(c net.Conn) bool {
	n.connMu.Lock()
	defer n.connMu.Unlock()
	if n.closing.Load() {
		return false
	}
	n.conns[c] = struct{}{}
	return true
}

func (n *Node) untrackConn(c net.Conn) {
	n.connMu.Lock()
	delete(n.conns, c)
	n.connMu.Unlock()
}

// closeAllConns drops every accepted connection. Without it a shutdown would
// wait for idle clients to disconnect on their own.
func (n *Node) closeAllConns() {
	n.connMu.Lock()
	for c := range n.conns {
		c.Close()
	}
	n.conns = map[net.Conn]struct{}{}
	n.connMu.Unlock()
}

func (n *Node) serveConn(conn net.Conn) {
	defer conn.Close()
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	r := proto.NewReader(conn)
	w := proto.NewWriter(conn)

	for {
		if n.closing.Load() {
			return
		}
		frame, err := r.ReadFrame()
		if err != nil {
			if err != io.EOF && !n.closing.Load() && !isClosedConn(err) {
				n.logf("read from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if len(frame) == 0 {
			return
		}
		op := proto.Op(frame[0])
		d := proto.NewDec(frame[1:])

		// REPLICATE takes the connection over for the lifetime of the stream.
		if op == proto.OpReplicate {
			fromSeq := d.U64()
			if err := d.Done(); err != nil {
				writeError(w, err)
				return
			}
			n.serveReplication(conn, w, fromSeq)
			return
		}
		if err := n.handle(op, d, w); err != nil {
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

func isClosedConn(err error) bool {
	return errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrUnexpectedEOF)
}

func writeError(w *proto.Writer, err error) error {
	e := proto.NewEnc(byte(proto.StatusError), 64)
	e.Str(err.Error())
	if werr := w.WriteFrame(e.B); werr != nil {
		return werr
	}
	return w.Flush()
}

func writeStatus(w *proto.Writer, s proto.Status) error {
	return w.WriteFrame([]byte{byte(s)})
}

// redirectIfNotLeader replies with the leader's address for any write that
// reaches a follower, so clients converge on the leader without a coordinator.
func (n *Node) redirectIfNotLeader(w *proto.Writer) (bool, error) {
	if n.Role() == Leader {
		return false, nil
	}
	e := proto.NewEnc(byte(proto.StatusRedirect), 32)
	e.Str(n.LeaderAddr())
	return true, w.WriteFrame(e.B)
}

func (n *Node) handle(op proto.Op, d *proto.Dec, w *proto.Writer) error {
	switch op {
	case proto.OpPing:
		return writeStatus(w, proto.StatusOK)

	case proto.OpGet:
		key := d.Bytes()
		if err := d.Done(); err != nil {
			return writeError(w, err)
		}
		v, err := n.store.Get(key)
		switch {
		case err == engine.ErrKeyNotFound:
			return writeStatus(w, proto.StatusNotFound)
		case err != nil:
			return writeError(w, err)
		}
		e := proto.NewEnc(byte(proto.StatusOK), len(v)+4)
		e.Bytes(v)
		return w.WriteFrame(e.B)

	case proto.OpPut:
		key, val := d.Bytes(), d.Bytes()
		if err := d.Done(); err != nil {
			return writeError(w, err)
		}
		if redirected, err := n.redirectIfNotLeader(w); redirected || err != nil {
			return err
		}
		if err := n.store.Put(key, val); err != nil {
			return writeError(w, err)
		}
		return writeStatus(w, proto.StatusOK)

	case proto.OpDelete:
		key := d.Bytes()
		if err := d.Done(); err != nil {
			return writeError(w, err)
		}
		if redirected, err := n.redirectIfNotLeader(w); redirected || err != nil {
			return err
		}
		switch err := n.store.Delete(key); {
		case err == engine.ErrKeyNotFound:
			return writeStatus(w, proto.StatusNotFound)
		case err != nil:
			return writeError(w, err)
		}
		return writeStatus(w, proto.StatusOK)

	case proto.OpBatchPut:
		count := int(d.U32())
		if d.Err() != nil {
			return writeError(w, d.Err())
		}
		// Each pair needs at least two 4-byte length prefixes, so a count that
		// the remaining body cannot possibly hold is a malformed (or hostile)
		// frame. Checking it before allocating stops a bogus count from
		// reserving gigabytes.
		if count < 0 || count > d.Remaining()/8 {
			return writeError(w, fmt.Errorf("batch count %d does not fit in %d remaining bytes", count, d.Remaining()))
		}
		keys := make([][]byte, 0, count)
		vals := make([][]byte, 0, count)
		for i := 0; i < count; i++ {
			// These alias the frame buffer, which stays valid until the next
			// ReadFrame -- i.e. past this call. Copying a 300 MiB batch just
			// to hand it to a writer that encodes it immediately would double
			// the peak memory for no gain.
			k, v := d.Bytes(), d.Bytes()
			if d.Err() != nil {
				return writeError(w, d.Err())
			}
			keys = append(keys, k)
			vals = append(vals, v)
		}
		if err := d.Done(); err != nil {
			return writeError(w, err)
		}
		if redirected, err := n.redirectIfNotLeader(w); redirected || err != nil {
			return err
		}
		if err := n.store.BatchPut(keys, vals); err != nil {
			return writeError(w, err)
		}
		return writeStatus(w, proto.StatusOK)

	case proto.OpRange:
		start, end := append([]byte(nil), d.Bytes()...), append([]byte(nil), d.Bytes()...)
		limit := int(d.U32())
		if err := d.Done(); err != nil {
			return writeError(w, err)
		}
		return n.serveRange(start, end, limit, w)

	case proto.OpStats:
		return w.WriteFrame(encodePairs(n.statPairs()).B)

	case proto.OpInfo:
		return w.WriteFrame(encodePairs(n.infoPairs()).B)

	case proto.OpCompact:
		if err := d.Done(); err != nil {
			return writeError(w, err)
		}
		// Compaction normally runs on its own schedule; this exists so an
		// operator can reclaim space now. It is acknowledged rather than
		// awaited, because merging a large store outlives any request timeout.
		started := n.store.StartCompaction()
		pairs := append([]pair{{"compaction", map[bool]string{true: "started", false: "already running"}[started]}},
			n.statPairs()...)
		return w.WriteFrame(encodePairs(pairs).B)

	case proto.OpVote:
		return n.handleVote(d, w)

	case proto.OpAnnounce:
		return n.handleAnnounce(d, w)

	case proto.OpPromote:
		if err := d.Done(); err != nil {
			return writeError(w, err)
		}
		n.Promote()
		return writeStatus(w, proto.StatusOK)

	default:
		return writeError(w, fmt.Errorf("unknown opcode %s", op))
	}
}

// rangeChunkBytes bounds how much of a range reply is buffered before it goes
// out on the wire, so a scan of a huge key range streams rather than being
// materialised on either side.
const rangeChunkBytes = 256 << 10

func (n *Node) serveRange(start, end []byte, limit int, w *proto.Writer) error {
	if limit <= 0 || limit > n.cfg.MaxRangeResults {
		limit = n.cfg.MaxRangeResults
	}
	chunk := proto.NewEnc(byte(proto.StatusRangeChunk), rangeChunkBytes)
	total := uint64(0)
	var sendErr error

	err := n.store.Scan(start, end, limit, func(k, v []byte) error {
		chunk.Bytes(k)
		chunk.Bytes(v)
		total++
		if len(chunk.B) >= rangeChunkBytes {
			if sendErr = w.WriteFrame(chunk.B); sendErr != nil {
				return sendErr
			}
			if sendErr = w.Flush(); sendErr != nil {
				return sendErr
			}
			chunk = proto.NewEnc(byte(proto.StatusRangeChunk), rangeChunkBytes)
		}
		return nil
	})
	if sendErr != nil {
		return sendErr
	}
	if err != nil {
		return writeError(w, err)
	}
	if len(chunk.B) > 1 {
		if err := w.WriteFrame(chunk.B); err != nil {
			return err
		}
	}
	e := proto.NewEnc(byte(proto.StatusRangeEnd), 8)
	e.U64(total)
	return w.WriteFrame(e.B)
}

type pair struct{ k, v string }

func encodePairs(pairs []pair) *proto.Enc {
	e := proto.NewEnc(byte(proto.StatusOK), 256)
	e.U32(uint32(len(pairs)))
	for _, p := range pairs {
		e.Str(p.k)
		e.Str(p.v)
	}
	return e
}

// heapBytes reads live heap size without stopping the world. runtime.MemStats
// would, and STATS is reachable by any client -- polling it must not be a way
// to add pauses to everyone else's latency.
func heapBytes() uint64 {
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() == metrics.KindUint64 {
		return sample[0].Value.Uint64()
	}
	return 0
}

func (n *Node) statPairs() []pair {
	s := n.store.Stats()
	return []pair{
		{"keys", strconv.Itoa(s.Keys)},
		{"tombstones", strconv.Itoa(s.Tombstones)},
		{"files", strconv.Itoa(s.Files)},
		{"live_bytes", strconv.FormatInt(s.LiveBytes, 10)},
		{"dead_bytes", strconv.FormatInt(s.DeadBytes, 10)},
		{"reclaimable", strconv.FormatFloat(s.Reclaimable, 'f', 3, 64)},
		{"compacting", strconv.FormatBool(s.Compacting)},
		{"seq", strconv.FormatUint(s.Seq, 10)},
		{"puts", strconv.FormatUint(s.Puts, 10)},
		{"gets", strconv.FormatUint(s.Gets, 10)},
		{"deletes", strconv.FormatUint(s.Deletes, 10)},
		{"scans", strconv.FormatUint(s.Scans, 10)},
		{"misses", strconv.FormatUint(s.Misses, 10)},
		{"connections", strconv.FormatInt(n.nConns.Load(), 10)},
		{"goroutines", strconv.Itoa(runtime.NumGoroutine())},
		{"heap_bytes", strconv.FormatUint(heapBytes(), 10)},
		{"uptime", time.Since(n.startedAt).Round(time.Second).String()},
	}
}

// contactAge is only meaningful on a follower; a leader is its own source of
// truth, so reporting a stale timer there would just be confusing.
func contactAge(role Role, last time.Time) string {
	if role == Leader {
		return "-"
	}
	return time.Since(last).Round(time.Millisecond).String()
}

func (n *Node) infoPairs() []pair {
	n.mu.RLock()
	role, leader, epoch, last := n.role, n.leaderAddr, n.epoch, n.lastContact
	n.mu.RUnlock()
	n.streamMu.Lock()
	followers := len(n.streams)
	n.streamMu.Unlock()
	return []pair{
		{"id", n.cfg.ID},
		{"addr", n.Addr()},
		{"role", role.String()},
		{"leader", leader},
		{"epoch", strconv.FormatUint(epoch, 10)},
		{"seq", strconv.FormatUint(n.store.Seq(), 10)},
		{"keys", strconv.Itoa(n.store.Len())},
		{"followers", strconv.Itoa(followers)},
		{"last_leader_contact", contactAge(role, last)},
		{"peers", fmt.Sprint(n.cfg.Peers)},
	}
}
