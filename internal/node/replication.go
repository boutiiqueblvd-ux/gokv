package node

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"gokv/internal/engine"
	"gokv/internal/proto"
)

// Replication is asynchronous leader -> follower log shipping.
//
// The leader hands every appended record to the live streams as it is written.
// A follower that connects announces the highest sequence number it holds; the
// leader replays whatever came after it and then stays attached. Because each
// record carries a globally monotonic sequence and the follower applies
// "newest sequence wins", replay is idempotent and insensitive to ordering --
// which is what makes reconnect-and-resume cheap.
//
// Acknowledgement is not part of the write path: a Put returns as soon as the
// leader has it. That is a deliberate availability/durability trade-off,
// discussed in SOLUTION.md.

// streamBuffer is how many pending records a follower may fall behind by
// before the leader drops the stream. Dropping is safe -- the follower
// reconnects and catches up from the log -- and it stops one slow follower
// from applying back-pressure to every writer.
const streamBuffer = 4096

type replStream struct {
	ch        chan []byte
	done      chan struct{}
	closeOnce sync.Once
	peer      string
}

func newReplStream(peer string) *replStream {
	return &replStream{ch: make(chan []byte, streamBuffer), done: make(chan struct{}), peer: peer}
}

func (s *replStream) close() { s.closeOnce.Do(func() { close(s.done) }) }

func (n *Node) addStream(s *replStream) {
	n.streamMu.Lock()
	n.streams[s] = struct{}{}
	n.streamMu.Unlock()
}

func (n *Node) removeStream(s *replStream) {
	n.streamMu.Lock()
	delete(n.streams, s)
	n.streamMu.Unlock()
	s.close()
}

func (n *Node) closeAllStreams() {
	n.streamMu.Lock()
	for s := range n.streams {
		s.close()
	}
	n.streamMu.Unlock()
}

// fanout runs on the writer's goroutine inside the engine write lock. It must
// never block, so a full stream buffer costs that follower its connection
// rather than costing the writer its latency.
func (n *Node) fanout(maxSeq uint64, raw []byte) {
	n.streamMu.Lock()
	if len(n.streams) == 0 {
		n.streamMu.Unlock()
		return
	}
	cp := append([]byte(nil), raw...)
	for s := range n.streams {
		select {
		case s.ch <- cp:
		default:
			n.logf("follower %s fell too far behind; dropping its stream", s.peer)
			s.close()
		}
	}
	n.streamMu.Unlock()
}

// serveReplication owns the connection for as long as the follower is attached.
func (n *Node) serveReplication(conn net.Conn, w *proto.Writer, fromSeq uint64) {
	peer := conn.RemoteAddr().String()
	if n.Role() != Leader {
		e := proto.NewEnc(byte(proto.StatusRedirect), 32)
		e.Str(n.LeaderAddr())
		w.WriteFrame(e.B)
		w.Flush()
		return
	}
	s := newReplStream(peer)
	// Attach before the catch-up scan so no write can slip through the gap
	// between "what the log holds" and "what the stream carries".
	n.addStream(s)
	defer n.removeStream(s)

	// Notice the follower going away: it sends nothing once attached, so any
	// read result at all means the connection is finished.
	go func() {
		buf := make([]byte, 1)
		conn.Read(buf)
		s.close()
	}()

	n.logf("follower %s attached from seq %d", peer, fromSeq)
	// Announce the current epoch straight away so a follower that reconnects
	// after an election learns who it is following before any data arrives.
	if err := n.writeHeartbeat(w); err != nil {
		return
	}
	if err := n.catchUp(w, fromSeq); err != nil {
		n.logf("catch-up for %s failed: %v", peer, err)
		return
	}

	hb := time.NewTicker(n.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-s.done:
			n.logf("follower %s detached", peer)
			return
		case raw := <-s.ch:
			if err := writeReplData(w, raw); err != nil {
				return
			}
			// Coalesce whatever else is queued into the same syscall.
		drain:
			for i := 0; i < 256; i++ {
				select {
				case more := <-s.ch:
					if err := writeReplData(w, more); err != nil {
						return
					}
				default:
					break drain
				}
			}
			if err := w.Flush(); err != nil {
				return
			}
		case <-hb.C:
			if err := n.writeHeartbeat(w); err != nil {
				return
			}
		}
	}
}

// writeHeartbeat doubles as the leader's liveness signal and as the channel
// through which followers learn the current epoch.
func (n *Node) writeHeartbeat(w *proto.Writer) error {
	n.mu.RLock()
	epoch, addr := n.epoch, n.leaderAddr
	n.mu.RUnlock()
	e := proto.NewEnc(byte(proto.StatusHeartbeat), 32)
	e.U64(epoch)
	e.Str(addr)
	if err := w.WriteFrame(e.B); err != nil {
		return err
	}
	return w.Flush()
}

func writeReplData(w *proto.Writer, raw []byte) error {
	e := proto.NewEnc(byte(proto.StatusReplData), len(raw))
	e.Raw(raw)
	return w.WriteFrame(e.B)
}

// catchUp replays the gap between the follower's position and now. If the gap
// predates the last compaction the log no longer holds the tombstones the
// follower would need, so it is sent a full snapshot instead.
func (n *Node) catchUp(w *proto.Writer, fromSeq uint64) error {
	sendAll := fromSeq == 0
	if !sendAll {
		err := n.store.RecordsSince(fromSeq, func(raw []byte) error {
			return writeReplData(w, raw)
		})
		if err == nil {
			return w.Flush()
		}
		if !errors.Is(err, engine.ErrNeedFullSync) {
			return err
		}
		sendAll = true
	}
	if sendAll {
		if err := writeStatus(w, proto.StatusFullSync); err != nil {
			return err
		}
		if err := n.store.SnapshotRecords(func(raw []byte) error {
			return writeReplData(w, raw)
		}); err != nil {
			return err
		}
	}
	return w.Flush()
}

// --- follower side ----------------------------------------------------------

// adoptEpoch records the epoch advertised by the leader we are streaming from.
// Without it a follower would still be at the epoch it booted with, and would
// ignore the announcement of a leader elected in the meantime.
func (n *Node) adoptEpoch(addr string, epoch uint64) {
	n.mu.Lock()
	if epoch >= n.epoch && n.role == Follower {
		n.epoch = epoch
		if addr != "" {
			n.leaderAddr = addr
		}
	}
	n.mu.Unlock()
}

func (n *Node) setLastContact() {
	n.mu.Lock()
	n.lastContact = time.Now()
	n.mu.Unlock()
}

func (n *Node) sinceLastContact() time.Duration {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return time.Since(n.lastContact)
}

// followLoop keeps a follower attached to the leader, and is also where the
// decision to call an election is made.
func (n *Node) followLoop() {
	defer n.wg.Done()
	backoff := 100 * time.Millisecond
	for {
		select {
		case <-n.stop:
			return
		default:
		}

		leader := n.LeaderAddr()
		if n.Role() == Leader || leader == "" || leader == n.cfg.Addr {
			if n.sleep(200 * time.Millisecond) {
				return
			}
			continue
		}

		err := n.replicateFrom(leader)
		if err != nil && !n.closing.Load() {
			n.logf("replication from %s ended: %v", leader, err)
		}
		if n.Role() == Follower && n.sinceLastContact() > n.cfg.ElectionTimeout {
			n.startElection()
			backoff = 100 * time.Millisecond
		} else {
			backoff = min(backoff*2, 2*time.Second)
		}
		if n.sleep(backoff) {
			return
		}
	}
}

func (n *Node) sleep(d time.Duration) (stopped bool) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-n.stop:
		return true
	case <-t.C:
		return false
	}
}

func (n *Node) replicateFrom(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	// A node that used to be the leader may hold writes the new leader never
	// saw. Rather than trying to reconcile divergent logs, it throws its state
	// away and takes a snapshot -- correctness over cleverness.
	n.mu.Lock()
	full := n.wasLeader
	n.wasLeader = false
	n.mu.Unlock()

	from := n.store.Seq()
	if full {
		from = 0
		n.logf("rejoining as follower after demotion: requesting a full snapshot")
	}

	w := proto.NewWriter(conn)
	e := proto.NewEnc(byte(proto.OpReplicate), 8)
	e.U64(from)
	if err := w.WriteFrame(e.B); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	n.setLastContact()

	r := proto.NewReader(conn)
	idle := 3 * n.cfg.HeartbeatInterval
	for {
		select {
		case <-n.stop:
			return nil
		default:
		}
		// A snapshot can take a while to arrive; the deadline only needs to be
		// generous enough to cover the heartbeat gap.
		conn.SetReadDeadline(time.Now().Add(max(idle, 30*time.Second)))
		frame, err := r.ReadFrame()
		if err != nil {
			if err == io.EOF {
				return errors.New("leader closed the stream")
			}
			return err
		}
		if len(frame) == 0 {
			return errors.New("empty frame from leader")
		}
		n.setLastContact()
		switch proto.Status(frame[0]) {
		case proto.StatusHeartbeat:
			d := proto.NewDec(frame[1:])
			leaderEpoch := d.U64()
			leaderAddr := d.Str()
			if d.Err() == nil {
				n.adoptEpoch(leaderAddr, leaderEpoch)
			}
		case proto.StatusFullSync:
			n.logf("leader ordered a full resync; discarding local state")
			if err := n.store.Reset(); err != nil {
				return err
			}
		case proto.StatusReplData:
			if _, err := n.store.ApplyRaw(frame[1:]); err != nil {
				return err
			}
		case proto.StatusRedirect:
			d := proto.NewDec(frame[1:])
			newLeader := d.Str()
			if newLeader != "" && newLeader != addr {
				n.logf("redirected to leader %s", newLeader)
				n.mu.Lock()
				n.leaderAddr = newLeader
				n.mu.Unlock()
			}
			return errors.New("peer is not the leader")
		case proto.StatusError:
			d := proto.NewDec(frame[1:])
			return errors.New("leader: " + d.Str())
		default:
			return errors.New("unexpected frame on the replication stream")
		}
	}
}
