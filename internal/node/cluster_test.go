package node_test

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"gokv/client"
	"gokv/internal/engine"
	"gokv/internal/node"
	"gokv/internal/proto"
)

// reserveAddr binds a port and keeps it. Nodes need to know their advertised
// address up front, so ":0" is not an option, and releasing the port before
// handing the address over would let anything -- including an outbound
// connection's ephemeral port -- take it first.
func reserveAddr(t *testing.T) (string, net.Listener) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l.Addr().String(), l
}

// relisten reclaims an address a stopped node was using.
func relisten(t *testing.T, addr string) net.Listener {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		l, err := net.Listen("tcp", addr)
		if err == nil {
			return l
		}
		if time.Now().After(deadline) {
			t.Fatalf("could not rebind %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type cluster struct {
	t     *testing.T
	addrs []string
	dirs  []string
	nodes []*node.Node
	cfgs  []node.Config
	lns   []net.Listener // reserved up front, handed to the node on start
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()
	c := &cluster{t: t}
	root := t.TempDir()
	for i := 0; i < size; i++ {
		addr, ln := reserveAddr(t)
		c.addrs = append(c.addrs, addr)
		c.lns = append(c.lns, ln)
		c.dirs = append(c.dirs, filepath.Join(root, fmt.Sprintf("n%d", i)))
	}
	for i := 0; i < size; i++ {
		cfg := node.Config{
			ID:                "n" + strconv.Itoa(i),
			Addr:              c.addrs[i],
			Dir:               c.dirs[i],
			Peers:             c.addrs,
			Bootstrap:         i == 0,
			Leader:            c.addrs[0],
			HeartbeatInterval: 100 * time.Millisecond,
			ElectionTimeout:   600 * time.Millisecond,
			Logger:            testLogger(t, "n"+strconv.Itoa(i)),
			Engine: engine.Options{
				MaxFileSize:        1 << 20,
				SyncMode:           engine.SyncNever,
				SyncEvery:          time.Hour,
				CompactionInterval: time.Hour,
			},
		}
		c.cfgs = append(c.cfgs, cfg)
		c.nodes = append(c.nodes, c.start(i))
	}
	t.Cleanup(c.stopAll)
	return c
}

// testLogger keeps node chatter out of passing tests but available on failure.
func testLogger(t *testing.T, id string) *log.Logger {
	if testing.Verbose() {
		return log.New(os.Stderr, "["+id+"] ", log.Lmicroseconds)
	}
	return log.New(io.Discard, "", 0)
}

func (c *cluster) start(i int) *node.Node {
	c.t.Helper()
	cfg := c.cfgs[i]
	if c.lns[i] != nil {
		cfg.Listener, c.lns[i] = c.lns[i], nil
	} else {
		cfg.Listener = relisten(c.t, cfg.Addr)
	}
	n, err := node.New(cfg)
	if err != nil {
		cfg.Listener.Close()
		c.t.Fatalf("node %d: %v", i, err)
	}
	if err := n.Start(); err != nil {
		c.t.Fatalf("node %d listen: %v", i, err)
	}
	return n
}

func (c *cluster) stop(i int) {
	if c.nodes[i] != nil {
		c.nodes[i].Close()
		c.nodes[i] = nil
	}
}

func (c *cluster) restart(i int) {
	c.stop(i)
	c.nodes[i] = c.start(i)
}

func (c *cluster) stopAll() {
	for i := range c.nodes {
		c.stop(i)
	}
}

func (c *cluster) client() *client.Client {
	c.t.Helper()
	cl, err := client.New(c.addrs, client.Options{Timeout: 5 * time.Second})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { cl.Close() })
	return cl
}

// clientFor talks to exactly one node, with no failover.
func (c *cluster) clientFor(i int) *client.Client {
	c.t.Helper()
	cl, err := client.New([]string{c.addrs[i]}, client.Options{Timeout: 5 * time.Second, MaxAttempts: 1})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { cl.Close() })
	return cl
}

func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", within, what)
}

func TestSingleNodeOverTheNetwork(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()

	if err := cl.Ping(); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Read([]byte("nope")); !errors.Is(err, client.ErrKeyNotFound) {
		t.Fatalf("missing key returned %v", err)
	}
	if err := cl.Put([]byte("greeting"), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	v, err := cl.Read([]byte("greeting"))
	if err != nil || string(v) != "hello" {
		t.Fatalf("Read = %q, %v", v, err)
	}

	// Binary-safe keys and values, including embedded NULs and newlines.
	weird := []byte{0x00, 0x01, '\n', 0xff, 'k'}
	blob := make([]byte, 300000)
	for i := range blob {
		blob[i] = byte(i)
	}
	if err := cl.Put(weird, blob); err != nil {
		t.Fatal(err)
	}
	got, err := cl.Read(weird)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(blob) || string(got) != string(blob) {
		t.Fatalf("binary round trip failed (%d bytes back)", len(got))
	}

	if err := cl.Delete([]byte("greeting")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Read([]byte("greeting")); !errors.Is(err, client.ErrKeyNotFound) {
		t.Fatalf("key survived delete: %v", err)
	}
	if err := cl.Delete([]byte("greeting")); !errors.Is(err, client.ErrKeyNotFound) {
		t.Fatalf("second delete returned %v", err)
	}
}

func TestBatchPutAndRangeOverTheNetwork(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()

	const n = 5000
	keys := make([][]byte, 0, n)
	vals := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		keys = append(keys, []byte(fmt.Sprintf("k%06d", i)))
		vals = append(vals, []byte(fmt.Sprintf("v%06d-%s", i, "payload")))
	}
	for i := 0; i < n; i += 500 {
		if err := cl.BatchPut(keys[i:i+500], vals[i:i+500]); err != nil {
			t.Fatalf("BatchPut at %d: %v", i, err)
		}
	}

	got, err := cl.ReadKeyRange([]byte("k001000"), []byte("k001099"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 100 {
		t.Fatalf("range returned %d pairs, want 100", len(got))
	}
	for i, kv := range got {
		if want := fmt.Sprintf("k%06d", 1000+i); string(kv.Key) != want {
			t.Fatalf("pair %d has key %q, want %q", i, kv.Key, want)
		}
	}

	// A range big enough to span several protocol chunks.
	count := 0
	prev := ""
	err = cl.ScanKeyRange(nil, nil, 0, func(k, v []byte) error {
		if prev != "" && string(k) <= prev {
			return fmt.Errorf("out of order: %q after %q", k, prev)
		}
		prev = string(k)
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("full scan saw %d pairs, want %d", count, n)
	}

	limited, err := cl.ReadKeyRange([]byte("k000000"), []byte("k999999"), 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 7 {
		t.Fatalf("limit ignored: %d pairs", len(limited))
	}
}

func TestReplicationReachesFollowers(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()

	for i := 0; i < 200; i++ {
		if err := cl.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%03d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := cl.Delete([]byte("k000")); err != nil {
		t.Fatal(err)
	}

	for i := 1; i < 3; i++ {
		follower := c.clientFor(i)
		eventually(t, 5*time.Second, fmt.Sprintf("follower %d to catch up", i), func() bool {
			v, err := follower.Read([]byte("k199"))
			return err == nil && string(v) == "v199"
		})
		// Deletes must replicate too, not just writes.
		if _, err := follower.Read([]byte("k000")); !errors.Is(err, client.ErrKeyNotFound) {
			t.Fatalf("follower %d still serves a deleted key: %v", i, err)
		}
		info, err := follower.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info["role"] != "follower" {
			t.Fatalf("node %d reports role %q", i, info["role"])
		}
	}
}

func TestFollowerRedirectsWrites(t *testing.T) {
	c := newCluster(t, 3)
	// This client only knows about a follower; the redirect has to take it to
	// the leader on its own.
	cl, err := client.New([]string{c.addrs[2]}, client.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	eventually(t, 5*time.Second, "follower to learn the leader", func() bool {
		info, err := cl.Info()
		return err == nil && info["leader"] == c.addrs[0]
	})
	if err := cl.Put([]byte("via-follower"), []byte("ok")); err != nil {
		t.Fatalf("write through a follower: %v", err)
	}
	v, err := cl.Read([]byte("via-follower"))
	if err != nil || string(v) != "ok" {
		t.Fatalf("read back = %q, %v", v, err)
	}
}

func TestFollowerCatchesUpAfterRestart(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()

	for i := 0; i < 50; i++ {
		if err := cl.Put([]byte(fmt.Sprintf("early%03d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	f := c.clientFor(2)
	eventually(t, 5*time.Second, "initial replication", func() bool {
		_, err := f.Read([]byte("early049"))
		return err == nil
	})

	// Take a follower down, keep writing, bring it back.
	c.stop(2)
	for i := 0; i < 500; i++ {
		if err := cl.Put([]byte(fmt.Sprintf("late%03d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	c.restart(2)
	f2 := c.clientFor(2)
	eventually(t, 10*time.Second, "follower to catch up on the backlog", func() bool {
		v, err := f2.Read([]byte("late499"))
		return err == nil && string(v) == "v"
	})
	if _, err := f2.Read([]byte("early000")); err != nil {
		t.Fatalf("follower lost pre-restart data: %v", err)
	}
}

func TestFullResyncAfterCompactionErasesTheGap(t *testing.T) {
	c := newCluster(t, 2)
	cl := c.client()

	for i := 0; i < 100; i++ {
		if err := cl.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("original")); err != nil {
			t.Fatal(err)
		}
	}
	f := c.clientFor(1)
	eventually(t, 5*time.Second, "first replication", func() bool {
		_, err := f.Read([]byte("k099"))
		return err == nil
	})

	// While the follower is away: overwrite everything, delete a slice of the
	// key space, then compact so the tombstones are gone from the log.
	c.stop(1)
	for i := 0; i < 100; i++ {
		if err := cl.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("rewritten")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		if err := cl.Delete([]byte(fmt.Sprintf("k%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.nodes[0].Store().Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	c.restart(1)
	f2 := c.clientFor(1)
	eventually(t, 10*time.Second, "follower to be rebuilt from a snapshot", func() bool {
		v, err := f2.Read([]byte("k099"))
		return err == nil && string(v) == "rewritten"
	})
	// The deleted range must be gone on the follower even though the
	// tombstones no longer exist anywhere in the leader's log.
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k%03d", i)
		if _, err := f2.Read([]byte(k)); !errors.Is(err, client.ErrKeyNotFound) {
			t.Fatalf("key %s was resurrected on the follower: %v", k, err)
		}
	}
}

func TestAutomaticFailoverWhenTheLeaderDies(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()

	for i := 0; i < 100; i++ {
		if err := cl.Put([]byte(fmt.Sprintf("pre%03d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i < 3; i++ {
		f := c.clientFor(i)
		eventually(t, 5*time.Second, "followers to be in sync before the kill", func() bool {
			_, err := f.Read([]byte("pre099"))
			return err == nil
		})
	}

	c.stop(0) // the leader dies

	eventually(t, 15*time.Second, "a surviving node to take leadership", func() bool {
		for i := 1; i < 3; i++ {
			if c.nodes[i] != nil && c.nodes[i].Role() == node.Leader {
				return true
			}
		}
		return false
	})

	// The client knows all three addresses and must find the new leader by
	// itself, without being reconfigured.
	var writeErr error
	eventually(t, 15*time.Second, "writes to succeed against the new leader", func() bool {
		writeErr = cl.Put([]byte("after-failover"), []byte("accepted"))
		return writeErr == nil
	})
	v, err := cl.Read([]byte("after-failover"))
	if err != nil || string(v) != "accepted" {
		t.Fatalf("read back after failover = %q, %v", v, err)
	}
	// Nothing written before the failover may be lost.
	for i := 0; i < 100; i += 10 {
		k := fmt.Sprintf("pre%03d", i)
		if _, err := cl.Read([]byte(k)); err != nil {
			t.Fatalf("pre-failover key %s lost: %v", k, err)
		}
	}
}

func TestDemotedLeaderRejoinsAndDiscardsDivergentWrites(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()
	if err := cl.Put([]byte("shared"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "replication", func() bool {
		v, err := c.clientFor(1).Read([]byte("shared"))
		return err == nil && string(v) == "v1"
	})

	// Force node 1 to take over while node 0 is still running, the way an
	// operator would during a planned failover.
	if err := client.Promote(c.addrs[1], 5*time.Second); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "node 0 to step down", func() bool {
		return c.nodes[0].Role() == node.Follower && c.nodes[1].Role() == node.Leader
	})

	newLeader := c.clientFor(1)
	if err := newLeader.Put([]byte("shared"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "the old leader to resync from the new one", func() bool {
		v, err := c.clientFor(0).Read([]byte("shared"))
		return err == nil && string(v) == "v2"
	})
}

func TestServerCapsRangeResults(t *testing.T) {
	c := newCluster(t, 1)
	c.stopAll()
	// Restart the single node with a deliberately small cap.
	cfg := c.cfgs[0]
	cfg.MaxRangeResults = 10
	cfg.Listener = relisten(t, cfg.Addr)
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	cl, err := client.New([]string{cfg.Addr}, client.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for i := 0; i < 50; i++ {
		if err := cl.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	got, err := cl.ReadKeyRange(nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Fatalf("server cap not applied: %d pairs returned", len(got))
	}
}

// A range reply is a stream of frames. A caller that gives up half way through
// leaves the rest of it in flight, and the next request on that connection
// would otherwise read those leftovers as its own answer.
func TestAbortedRangeDoesNotCorruptTheConnection(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()

	// Enough data that the reply spans several protocol chunks.
	value := make([]byte, 1024)
	for i := range value {
		value[i] = 'v'
	}
	keys := make([][]byte, 0, 200)
	vals := make([][]byte, 0, 200)
	for i := 0; i < 2000; i++ {
		keys = append(keys, []byte(fmt.Sprintf("k%06d", i)))
		vals = append(vals, value)
		if len(keys) == 200 {
			if err := cl.BatchPut(keys, vals); err != nil {
				t.Fatal(err)
			}
			keys, vals = keys[:0], vals[:0]
		}
	}
	if err := cl.Put([]byte("sentinel"), []byte("intact")); err != nil {
		t.Fatal(err)
	}

	giveUp := errors.New("caller stopped early")
	seen := 0
	err := cl.ScanKeyRange([]byte("k"), []byte("l"), 0, func(k, v []byte) error {
		seen++
		return giveUp
	})
	if !errors.Is(err, giveUp) {
		t.Fatalf("aborted scan returned %v, want the caller's error", err)
	}
	if seen != 1 {
		t.Fatalf("callback ran %d times after aborting, want 1", seen)
	}

	// The next requests must be answered correctly, not with leftover frames.
	for i := 0; i < 3; i++ {
		v, err := cl.Read([]byte("sentinel"))
		if err != nil || string(v) != "intact" {
			t.Fatalf("request %d after an aborted scan: %q, %v", i, v, err)
		}
	}
	got, err := cl.ReadKeyRange([]byte("k000000"), []byte("k000009"), 0)
	if err != nil {
		t.Fatalf("range after an aborted scan: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("range after an aborted scan returned %d pairs, want 10", len(got))
	}
}

// A count field arriving off the network sizes an allocation, so it has to be
// checked against what the frame can actually hold.
func TestServerRejectsImpossibleBatchCount(t *testing.T) {
	c := newCluster(t, 1)

	conn, err := net.Dial("tcp", c.addrs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	e := proto.NewEnc(byte(proto.OpBatchPut), 8)
	e.U32(0xFFFFFFF0) // claims four billion pairs in an 8-byte body
	w := proto.NewWriter(conn)
	if err := w.WriteFrame(e.B); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	frame, err := proto.NewReader(conn).ReadFrame()
	if err != nil {
		t.Fatalf("server did not answer a malformed batch: %v", err)
	}
	if proto.Status(frame[0]) != proto.StatusError {
		t.Fatalf("malformed batch got status %#x, want an error", frame[0])
	}

	// And the node must still be serving.
	cl := c.clientFor(0)
	if err := cl.Put([]byte("still"), []byte("alive")); err != nil {
		t.Fatalf("node unusable after a malformed frame: %v", err)
	}
}

// Nodes started without -bootstrap must still converge on a leader instead of
// waiting for one that will never appear.
func TestClusterWithoutBootstrapElectsALeader(t *testing.T) {
	root := t.TempDir()
	addrs := make([]string, 3)
	lns := make([]net.Listener, 3)
	for i := range addrs {
		addrs[i], lns[i] = reserveAddr(t)
	}
	nodes := make([]*node.Node, 0, 3)
	for i, addr := range addrs {
		n, err := node.New(node.Config{
			ID:       "n" + strconv.Itoa(i),
			Addr:     addr,
			Dir:      filepath.Join(root, "n"+strconv.Itoa(i)),
			Peers:    addrs,
			Listener: lns[i],
			// No Bootstrap, and no Leader: nobody has been told who leads.
			HeartbeatInterval: 100 * time.Millisecond,
			ElectionTimeout:   400 * time.Millisecond,
			Logger:            testLogger(t, "n"+strconv.Itoa(i)),
			Engine:            engine.Options{SyncMode: engine.SyncNever, CompactionInterval: time.Hour},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
		defer n.Close()
		nodes = append(nodes, n)
	}

	eventually(t, 15*time.Second, "exactly one node to take leadership", func() bool {
		leaders := 0
		for _, n := range nodes {
			if n.Role() == node.Leader {
				leaders++
			}
		}
		return leaders == 1
	})

	cl, err := client.New(addrs, client.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	eventually(t, 10*time.Second, "the elected leader to accept writes", func() bool {
		return cl.Put([]byte("elected"), []byte("ok")) == nil
	})
}

// A leader that is hung rather than dead still accepts TCP connections and
// then says nothing. Followers must notice the silence; if merely reaching the
// socket counted as contact, they would wait for it forever.
func TestHungLeaderIsDetected(t *testing.T) {
	// A stand-in leader: it accepts, holds the connection, and never speaks.
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	held := make(chan net.Conn, 8)
	go func() {
		for {
			conn, err := silent.Accept()
			if err != nil {
				return
			}
			select {
			case held <- conn:
			default:
				conn.Close()
			}
		}
	}()

	root := t.TempDir()
	addrs := make([]string, 3)
	lns := make([]net.Listener, 3)
	addrs[0] = silent.Addr().String()
	for i := 1; i < 3; i++ {
		addrs[i], lns[i] = reserveAddr(t)
	}
	nodes := make([]*node.Node, 0, 2)
	for i := 1; i < 3; i++ {
		n, err := node.New(node.Config{
			ID:                "n" + strconv.Itoa(i),
			Addr:              addrs[i],
			Dir:               filepath.Join(root, "n"+strconv.Itoa(i)),
			Peers:             addrs,
			Listener:          lns[i],
			Leader:            addrs[0], // the silent one
			HeartbeatInterval: 100 * time.Millisecond,
			ElectionTimeout:   500 * time.Millisecond,
			Logger:            testLogger(t, "n"+strconv.Itoa(i)),
			Engine:            engine.Options{SyncMode: engine.SyncNever, CompactionInterval: time.Hour},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
		defer n.Close()
		nodes = append(nodes, n)
	}

	eventually(t, 20*time.Second, "a follower to give up on the silent leader", func() bool {
		for _, n := range nodes {
			if n.Role() == node.Leader {
				return true
			}
		}
		return false
	})

	cl, err := client.New(addrs[1:], client.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	eventually(t, 10*time.Second, "the new leader to accept writes", func() bool {
		return cl.Put([]byte("past-the-hang"), []byte("ok")) == nil
	})
}
