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
)

// freePort reserves an address by binding and immediately releasing it. Nodes
// need to know their own advertised address up front, so ":0" is not an option.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

type cluster struct {
	t     *testing.T
	addrs []string
	dirs  []string
	nodes []*node.Node
	cfgs  []node.Config
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()
	c := &cluster{t: t}
	root := t.TempDir()
	for i := 0; i < size; i++ {
		c.addrs = append(c.addrs, freeAddr(t))
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
	n, err := node.New(c.cfgs[i])
	if err != nil {
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
