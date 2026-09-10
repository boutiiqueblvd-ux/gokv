package node

import (
	"net"
	"time"

	"gokv/internal/proto"
)

// Failover here is deliberately small: it is a quorum-gated promotion, not a
// consensus protocol.
//
// A follower that has heard nothing from the leader for ElectionTimeout asks
// every peer what it sees. It promotes itself only if
//
//  1. a majority of the cluster answered,
//  2. none of them can still reach the old leader, and
//  3. it holds the highest sequence number among the answers (ties broken by
//     the lowest node id, so exactly one candidate can win a given round).
//
// Each promotion bumps an epoch, and an announcement with a higher epoch wins.
// A node that is demoted throws away its state and takes a snapshot from the
// new leader, so writes the old leader accepted alone are dropped rather than
// silently merged.
//
// What this is NOT: it has no persistent vote and no log matching, so under a
// network partition both sides can believe they lead. Section "What's
// incomplete" in SOLUTION.md says what Raft would buy instead.

type voteReply struct {
	id           string
	role         Role
	epoch        uint64
	seq          uint64
	leaderAddr   string
	leaderAlive  bool
	responseFrom string
}

func (n *Node) handleVote(d *proto.Dec, w *proto.Writer) error {
	candidateID := d.Str()
	candidateSeq := d.U64()
	if err := d.Done(); err != nil {
		return writeError(w, err)
	}
	n.mu.RLock()
	role, leader, epoch, last := n.role, n.leaderAddr, n.epoch, n.lastContact
	n.mu.RUnlock()

	alive := role == Leader || time.Since(last) <= n.cfg.ElectionTimeout
	n.logf("vote request from %s (seq %d): my role=%s seq=%d leader_alive=%v",
		candidateID, candidateSeq, role, n.store.Seq(), alive)

	e := proto.NewEnc(byte(proto.StatusOK), 64)
	e.Str(n.cfg.ID)
	e.U8(byte(role))
	e.U64(epoch)
	e.U64(n.store.Seq())
	e.Str(leader)
	if alive {
		e.U8(1)
	} else {
		e.U8(0)
	}
	return w.WriteFrame(e.B)
}

func (n *Node) handleAnnounce(d *proto.Dec, w *proto.Writer) error {
	leaderAddr := d.Str()
	leaderID := d.Str()
	epoch := d.U64()
	if err := d.Done(); err != nil {
		return writeError(w, err)
	}
	n.mu.Lock()
	if epoch <= n.epoch {
		cur := n.epoch
		n.mu.Unlock()
		n.logf("ignoring announcement from %s for epoch %d (already at %d)", leaderID, epoch, cur)
		return writeStatus(w, proto.StatusOK)
	}
	demoted := n.role == Leader
	n.role = Follower
	n.leaderAddr = leaderAddr
	n.epoch = epoch
	n.lastContact = time.Now()
	if demoted {
		n.wasLeader = true
	}
	n.mu.Unlock()

	if demoted {
		n.logf("stepping down: %s took leadership at epoch %d", leaderID, epoch)
		n.closeAllStreams()
	} else {
		n.logf("following new leader %s at epoch %d", leaderAddr, epoch)
	}
	return writeStatus(w, proto.StatusOK)
}

func (n *Node) peersExceptSelf() []string {
	out := make([]string, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		if p != n.cfg.Addr && p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (n *Node) clusterSize() int {
	size := len(n.peersExceptSelf()) + 1
	return size
}

func (n *Node) startElection() {
	if !n.electing.CompareAndSwap(false, true) {
		return
	}
	defer n.electing.Store(false)
	if n.Role() == Leader || n.closing.Load() {
		return
	}

	mySeq := n.store.Seq()
	myEpoch := n.Epoch()
	peers := n.peersExceptSelf()
	n.logf("leader silent for %s; starting an election at seq %d", n.sinceLastContact().Round(time.Millisecond), mySeq)

	responses := 1 // this node counts itself
	bestCandidate := true
	for _, p := range peers {
		reply, err := n.requestVote(p, mySeq)
		if err != nil {
			n.logf("peer %s unreachable during election: %v", p, err)
			continue
		}
		responses++
		if reply.role == Leader && reply.epoch >= myEpoch {
			n.logf("peer %s is already leading at epoch %d; following it", p, reply.epoch)
			n.adoptLeader(p, reply.epoch)
			return
		}
		if reply.leaderAlive && reply.leaderAddr != "" {
			n.logf("peer %s can still reach leader %s; standing down", p, reply.leaderAddr)
			n.adoptLeader(reply.leaderAddr, max(reply.epoch, myEpoch))
			return
		}
		if reply.seq > mySeq || (reply.seq == mySeq && reply.id < n.cfg.ID) {
			bestCandidate = false
		}
		if reply.epoch > myEpoch {
			myEpoch = reply.epoch
		}
	}

	if responses*2 <= n.clusterSize() {
		n.logf("election abandoned: only %d/%d nodes answered", responses, n.clusterSize())
		return
	}
	if !bestCandidate {
		n.logf("election deferred: another reachable node is further ahead")
		return
	}
	n.promoteTo(myEpoch + 1)
}

func (n *Node) adoptLeader(addr string, epoch uint64) {
	n.mu.Lock()
	n.leaderAddr = addr
	if epoch > n.epoch {
		n.epoch = epoch
	}
	n.lastContact = time.Now()
	n.mu.Unlock()
}

// Promote forces this node to become the leader. It backs the PROMOTE opcode,
// which exists so an operator (or a test) can trigger failover deterministically.
func (n *Node) Promote() {
	n.promoteTo(n.Epoch() + 1)
}

func (n *Node) promoteTo(epoch uint64) {
	n.mu.Lock()
	if n.role == Leader && n.epoch >= epoch {
		n.mu.Unlock()
		return
	}
	n.role = Leader
	n.leaderAddr = n.cfg.Addr
	n.epoch = epoch
	n.lastContact = time.Now()
	n.mu.Unlock()

	n.logf("promoted to leader at epoch %d with seq %d", epoch, n.store.Seq())
	for _, p := range n.peersExceptSelf() {
		go n.announceTo(p, epoch)
	}
}

func (n *Node) announceTo(addr string, epoch uint64) {
	e := proto.NewEnc(byte(proto.OpAnnounce), 64)
	e.Str(n.cfg.Addr)
	e.Str(n.cfg.ID)
	e.U64(epoch)
	if _, err := n.roundTrip(addr, e.B, 2*time.Second); err != nil {
		n.logf("could not announce leadership to %s: %v", addr, err)
	}
}

func (n *Node) requestVote(addr string, mySeq uint64) (voteReply, error) {
	e := proto.NewEnc(byte(proto.OpVote), 64)
	e.Str(n.cfg.ID)
	e.U64(mySeq)
	frame, err := n.roundTrip(addr, e.B, 1500*time.Millisecond)
	if err != nil {
		return voteReply{}, err
	}
	d := proto.NewDec(frame[1:])
	r := voteReply{responseFrom: addr}
	r.id = d.Str()
	r.role = Role(d.U8())
	r.epoch = d.U64()
	r.seq = d.U64()
	r.leaderAddr = d.Str()
	r.leaderAlive = d.U8() == 1
	if err := d.Done(); err != nil {
		return voteReply{}, err
	}
	return r, nil
}

// roundTrip is a one-shot request on a fresh connection. Cluster control
// messages are rare, so there is nothing to gain from pooling them, and a
// fresh connection cannot inherit a half-broken stream.
func (n *Node) roundTrip(addr string, req []byte, timeout time.Duration) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	w := proto.NewWriter(conn)
	if err := w.WriteFrame(req); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	frame, err := proto.NewReader(conn).ReadFrame()
	if err != nil {
		return nil, err
	}
	if len(frame) == 0 {
		return nil, proto.ErrMalformed
	}
	if proto.Status(frame[0]) == proto.StatusError {
		return nil, errFromFrame(frame)
	}
	return append([]byte(nil), frame...), nil
}

func errFromFrame(frame []byte) error {
	d := proto.NewDec(frame[1:])
	return &RemoteError{Msg: d.Str()}
}

type RemoteError struct{ Msg string }

func (e *RemoteError) Error() string { return "remote: " + e.Msg }
