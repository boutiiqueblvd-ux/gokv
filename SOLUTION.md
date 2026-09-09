# Solution: design, trade-offs, and gaps

## What was built

A network-available persistent key/value store, Go standard library only.

- **Storage engine** (`internal/engine`) — an append-only log of checksummed
  records plus an in-memory index. Bitcask-shaped, with a skip list bolted on
  so ordered range queries are possible, and a global sequence number on every
  record so recovery, compaction and replication all agree on which version of
  a key wins.
- **Server** (`internal/node`, `cmd/kvd`) — length-prefixed binary protocol over
  TCP, goroutine per connection.
- **Client** (`client`, `cmd/kvcli`) — leader discovery via redirects, retry
  across nodes, streaming range reads.
- **Replication and failover** — asynchronous log shipping to followers,
  quorum-gated promotion when the leader goes silent.
- **Tools** — `cmd/kvbench` load generator, plus `scripts/` for cluster,
  failover and `kill -9` crash demonstrations.

The five required operations map to `Put`, `Read`, `ReadKeyRange`, `BatchPut`
and `Delete` on the client, and to the same names on the engine (the engine
spells the range operation `Scan`/`ScanSlice` because it takes a callback).

---

## 1. Trade-offs I made, and what I rejected

### Log-structured (Bitcask) over an LSM tree or a B-tree

The requirements pull hard in one direction: *low latency per item*, *high
throughput on a stream of random writes*, *datasets much larger than RAM*,
*fast crash recovery*. A Bitcask-style log satisfies four of those almost by
construction:

| requirement | why this design gets it |
| --- | --- |
| low read latency | one hash lookup, then exactly one `pread`. No level cascade, no bloom filters, no read amplification |
| random-write throughput | every write is an append; the key distribution is irrelevant. No compaction on the write path, no page splits |
| bigger than RAM | only keys and a 32-byte locator live in memory. Values are never cached by us |
| crash recovery | a forward scan that stops at the first bad checksum, accelerated by hint sidecars |

**Rejected: an LSM tree.** It would have given me sorted data on disk for free,
much smaller memory per key, and better range locality. I rejected it because
it costs read amplification (a point read may touch several levels), needs
bloom filters and a block cache to claw that back, and compaction becomes a
correctness-critical background process rather than a space optimisation. For
the time available, an LSM would have been a half-built LSM. The concrete price
I pay is stated honestly below: **the index must fit in RAM**.

**Rejected: a B-tree.** Random-write throughput is exactly what a B+tree is
worst at — random page writes plus a WAL means writing the data twice.

### One in-memory index, two structures

`ReadKeyRange` needs ordering; `Read` wants O(1). I keep both: a
`map[string]*node` for point lookups and a skip list over the same nodes for
ordered traversal. The map entry costs roughly 50 extra bytes per key.

**Rejected: skip list alone** (point reads become ~20 pointer chases and each
one is a cache miss). **Rejected: map alone, sorting on demand** (a range query
would be O(n log n) over the whole key space — the opposite of "predictable
behaviour under large volume"). I chose to spend memory, and then to be honest
about the memory ceiling that creates.

I wrote the skip list rather than using a B-tree because it is about 120 lines,
needs no rebalancing, and the store already serialises index mutations under a
single lock, so a lock-free variant would have been complexity for nothing.

### Tombstones stay in the index

A delete does not remove the key from the in-memory index; it replaces the
entry with a tombstone that carries the delete's sequence number. Compaction is
what finally removes it.

This costs memory — deleted keys occupy index space until the next merge — and
it buys the invariant everything else leans on. If a delete simply erased the
key, the index would have no record that it had ever seen a higher sequence for
it, and an *older* record arriving afterwards would be indistinguishable from a
new write. "Afterwards" is not hypothetical: recovery walks files in id order
and a merged file's records are in key order, so an older record genuinely can
be read after the tombstone that killed it. The same is true of a replication
stream. This was a live bug, found in review, and the tests that pin it down
are `TestDeleteIsNotUndoneByAnOlderRecord` and
`TestRecoveryHonoursSequenceNotFileOrder`.

### Sequence numbers instead of file ordering

Every record carries a globally monotonic sequence. Recovery resolves duplicate
keys by "highest sequence wins" rather than by "latest file wins". That one
decision pays for itself three times:

1. **Compaction can write merged files with fresh, higher ids** without a stale
   record in a merged file shadowing a newer one in the active file.
2. **A partially-written merge is harmless.** Old and merged files can both be
   present after a crash; the duplicates resolve identically.
3. **Replication becomes idempotent and order-insensitive.** A follower can be
   sent overlapping records, or the same records twice, and converge.

### Durability is a knob, and the default is not "safest"

`-sync always` fsyncs before acknowledging: nothing acknowledged is ever lost,
at roughly 740 writes/s on this machine's (virtualised) disk. `-sync interval`
(the default) fsyncs on a 200 ms timer and reaches ~290k writes/s at the engine.
That is a three-orders-of-magnitude difference, so it has to be the operator's
decision, not mine.

Both were tested by SIGKILLing a real server mid-write stream
(`scripts/crash_test.sh`) — see the measurements below. Note the precise
guarantee: `-sync interval` survives a *process* crash intact, because written
data is already in the OS page cache; what it risks is a *machine* crash.

### BatchPut is a single append, and it is atomic across a crash

Batch records carry a batch flag and the last one is marked as the terminator.
Recovery stages batch records and only applies them when it sees the terminator,
so a crash mid-batch leaves no partial batch behind. That is one append and one
fsync for the whole batch, which is where the ~1.2M items/s figure comes from.

### Full merges only

Compaction merges *all* immutable files at once instead of picking victims.
That costs more I/O per pass. I chose it because it is precisely the condition
under which tombstones can be dropped safely: if no older file survives the
merge, a discarded tombstone has nothing left to resurrect. A partial merge that
drops tombstones is a data-resurrection bug waiting to happen, and the test
`TestFullResyncAfterCompactionErasesTheGap` exists because I nearly shipped the
replication half of exactly that bug.

Compaction never blocks writers. It snapshots the index, copies records, and
re-checks each key before repointing it, so a value overwritten mid-merge keeps
the newer location. Readers take a shared lock on the file set for the duration
of a read, which is what lets the merge close and unlink the old files safely.

### Asynchronous replication

A `Put` is acknowledged when the leader has it, not when a follower does. This
is an availability-over-durability choice: a slow or dead follower can never
add latency to a write, and a follower that falls more than 4096 records behind
has its stream dropped rather than being allowed to apply back-pressure. It
reconnects and catches up from its own sequence number.

The cost is real and I am not going to hide it: **a leader that dies can lose
writes it had already acknowledged.** Synchronous quorum replication would fix
that and is the obvious next step (see below).

### Failover is quorum-gated promotion, not consensus

A follower that has heard nothing for `-election-timeout` polls its peers. It
promotes itself only if a majority answered, none of them can still reach the
old leader, and it holds the highest sequence number (ties broken by node id, so
at most one candidate can win a round). Promotions bump an epoch; an
announcement with a higher epoch wins; a demoted leader discards its state and
takes a full snapshot from the new leader rather than trying to reconcile
divergent logs.

**Rejected: implementing Raft.** It is the right answer and I know it. In the
time available I would have produced an unverified Raft, which is worse than a
small mechanism whose limits I can state exactly. The limits are stated in
section 3.

---

## 2. Measurements

Intel i7-14700F, 15 GiB RAM, WSL2 on a virtualised disk. Reproduce with
`go test -bench` and `cmd/kvbench`.

**Engine, in-process** (`go test -run XXX -bench . -benchtime 3s ./internal/engine`):

| benchmark | result | notes |
| --- | --- | --- |
| `PutRandom` | 3.5 µs/op — ~290k writes/s | random keys, 128 B values, `-sync interval` |
| `PutRandomSyncAlways` | 1.54 ms/op — ~650 writes/s | fsync per write; this is the disk, not the code |
| `BatchPut100` | 69 µs/batch — ~1.45M items/s | one append, one fsync per batch |
| `GetRandom` | 832 ns/op | single-threaded |
| `GetParallel` | 162 ns/op — ~6.2M reads/s | 28 threads |
| `Scan100` | 38.5 µs — ~2.6M keys/s | 100-key range |

**Over the network** (`cmd/kvbench`, single node, loopback):

| workload | throughput | p50 | p99 |
| --- | --- | --- | --- |
| GET, 1 client | 7.2k ops/s | 129 µs | 302 µs |
| GET, 32 clients | 47k ops/s | 504 µs | 3.4 ms |
| GET, 128 clients | 98k ops/s | 887 µs | 7.0 ms |
| PUT, 32 clients | 44k ops/s | 531 µs | 3.8 ms |
| BatchPut ×100, 16 clients | 595k items/s | 2.4 ms | 15.9 ms |
| Range ×100 keys, 16 clients | 1.12M items/s | 938 µs | 5.3 ms |

Single-op throughput is bounded by the request/response round trip, not by the
engine: 832 ns of engine work sits inside a 129 µs round trip. Batching and
range queries, which amortise that round trip, are where the engine's real
speed shows.

**Larger than RAM** — 4M writes of 4 KiB values, 15.8 GiB on disk, on a machine
with 15 GiB of RAM:

| | |
| --- | --- |
| resident set of the server | **0.53 GiB** for 2.53M live keys |
| random reads across the whole 16 GiB set | **35k ops/s, p50 708 µs, p99 4.1 ms** |
| restart with hint files | **8.4 s** to rebuild 2.53M keys from 15.8 GiB |
| restart without hint files (deleted) | 14.8 s |
| compaction, 36.8% dead | 16 files → 10, 15.8 GiB → 11 GiB, **98 s**, reads served throughout at 32k ops/s |

Read latency across a dataset larger than memory stays in the same order of
magnitude as the in-cache case, which is the property the brief asked for.
Caveat: the page cache was warm from the write phase, so this understates a
truly cold read; the structural claim (one seek per read, index independent of
value size) is what the measurement supports.

**Crash friendliness** (`scripts/crash_test.sh`, real `kill -9`):

- `-sync always`: 425 writes acknowledged before the kill, **425 readable
  after restart, 0 lost.** Recovery found 426 records — one write reached disk
  but the acknowledgement never made it back out, which is the correct
  direction to err in.
- `-sync interval`: 5233 acknowledged, 0 lost, because a process kill does not
  clear the page cache. A machine-level crash is what this mode risks.

---

## 3. What is incomplete, and why

Honest list. Everything here is a real gap, not a rough edge.

**The index must fit in RAM.** Roughly 150 bytes per key (map entry, skip-list
node, key string, 32-byte locator). 100M keys is about 18 GB of index. Values
of any size are fine; *key count* is the ceiling. This is inherent to Bitcask
and it is the single biggest architectural limitation. An LSM tree is the fix.

**Replication is asynchronous, so failover can lose acknowledged writes.** If
a leader accepts a write and dies before shipping it, that write is gone — and
when the old node rejoins it discards its divergent tail on purpose. A
`-write-quorum` option that waits for N followers to acknowledge before
returning is a contained change (the fan-out point already exists) and is the
first thing I would add.

**The election is not consensus, and can split-brain under a partition.** There
is no persistent vote and no log matching. If a leader is isolated but alive,
the majority side elects a new leader while the old one keeps accepting writes
from any client that can still reach it. Nothing fences the old leader. A
correct fix is Raft, or at minimum a lease: the leader stops accepting writes
if it has not heard from a majority within an interval. I implemented neither.

**No snapshot isolation on range queries.** `ReadKeyRange` takes and releases
the index lock once per 512 keys so a big scan cannot stall writers. The
consequence is that a concurrent write to a key ahead of the cursor is visible
mid-scan. Deliberate — a consistent scan needs MVCC — but it is a real
semantic gap.

**Compaction rewrites everything.** Fine at 16 GiB (98 s), wasteful at 1 TiB.
Scoring files by dead-byte ratio and merging only the worst offenders is the
standard fix; it requires per-file liveness accounting and the tombstone rule
above, and I chose the safe version instead.

**A damaged record inside an old file is skipped, not repaired.** Recovery
stops scanning that file at the first bad checksum. For the active file that is
correct (it is a torn tail). For a middle file it means data after the damage in
that file is silently unavailable until the file is compacted away. It is
logged, but there is no repair path and no way to reconstruct from a replica.

**No authentication, TLS, or per-connection quotas.** Anything that can reach
the port can read and write everything. Fine for the exercise, not for a
network service.

**Cluster membership is static.** Peers come from a flag. Adding or removing a
node means restarting the others. There is no membership change protocol.

**A single write lock serialises all writers.** Correct and simple, and the
engine still does 290k writes/s, but group commit (batching concurrent writers
into one append and one fsync) would help a lot in `-sync always` mode, where
writers currently queue behind individual fsyncs.

**Not tested:** disk-full and I/O-error paths (the code returns the errors and
truncates partial appends, but I did not inject failures); clock skew (record
timestamps are informational only, so this should be harmless — untested);
partition scenarios beyond a clean process kill; more than 3 nodes.

---

## 4. What the review pass found

After the first working version I went back over the code hunting for bugs
rather than adding features. These are the ones that were real, all of them now
fixed with a regression test that fails without the fix.

**Deleted keys could come back.** The big one. Removing a key from the index on
delete threw away the sequence number that made the delete authoritative, so an
older record for that key — arriving out of a merged file during recovery, or
out of order on a replication stream — was treated as a fresh write. Fixed by
keeping tombstones in the index (see section 1). Two of the paths that reach it
are ordinary operation, not exotic failure: a merge racing a concurrent delete,
and a follower catching up across a merge boundary.

**Compaction could strand the index.** The index was repointed record by record
as the merge copied them. If the merge then failed part way — a read error, a
full disk — it deleted its own half-written output files, while index entries
already pointed into them. Those keys became unreadable until a restart.
Repointing now happens only after every output file is durable, in one pass, so
abandoning a merge can never strand anything.

**Compaction and snapshots leaked batch markers.** A `BatchPut` record carries
"part of a group, only valid once you see the terminator". When compaction
copies a live record out of a batch whose terminator has since been
overwritten, or a snapshot ships one record per key, that marker follows it and
recovery stages the record forever and then discards it — silent data loss on
the next restart without a hint file. The markers are now stripped whenever a
record is lifted out of the batch it was written in.

**An aborted range query corrupted the client connection.** Range replies are a
stream of frames. A caller that stopped early left the rest in the socket, and
the next request read those leftovers as its answer — a `Read` returning some
other key's value. The client now marks such a connection unusable and
reconnects. `TestAbortedRangeDoesNotCorruptTheConnection` catches it; without
the fix, the assertion fails with a range key answering a point read.

**A hung leader was not detected for a minute.** A follower marked "contact" as
soon as it connected, so a leader that accepted TCP and then said nothing kept
its followers waiting indefinitely, and the read deadline was 30 s regardless.
Contact is now only recorded on a frame actually received, the leader greets a
new follower before doing any work, and the deadline is tight until the leader
says it is live and tight again afterwards — generous only while a backlog is
being read off disk. `TestHungLeaderIsDetected` stands up a listener that
accepts and never speaks.

**A batch count off the network sized an allocation.** `BatchPut`'s pair count
was read and used to preallocate before it was checked, so a four-byte field
could ask a node to reserve tens of gigabytes. It is now validated against the
bytes the frame actually contains.

**A replicated record was trusted more than a local one.** `ApplyRaw` did not
apply the length limits the local write path enforces. An empty key is
especially nasty: recovery reads a zero key length as end-of-file, so a single
such record would silently truncate everything written after it.

**`Close` raced background compaction.** A merge started by the scheduler was
not tracked by the wait group, so shutdown could close the files underneath it.
Shutdown now waits on the compaction lock.

**`STATS` stopped the world.** It called `runtime.ReadMemStats` on every
request; any client could add GC pauses to everyone else's latency by polling
it. It reads `runtime/metrics` now.

Also fixed, smaller: nodes started without `-bootstrap` waited forever instead
of standing for election; orphaned hint files accumulated after compaction; a
large batch was held in memory twice; the test harness raced the OS for ports
(nodes can now be handed a pre-bound listener).

---

## 5. What I would do next, in order

1. **Write quorum.** Optional synchronous replication to N followers before
   acknowledging. Closes the acknowledged-write-loss window, which is the most
   embarrassing gap.
2. **Leader lease.** Cheap, and it removes the worst split-brain window without
   a full consensus implementation.
3. **Raft** for membership and leader election, replacing sections 2 and the
   ad-hoc epoch machinery.
4. **Scored compaction** instead of full merges, with per-file liveness stats.
5. **Group commit** on the write path.
6. **A crash-injection test harness** — kill at randomised offsets, verify the
   acknowledged set on every restart. `scripts/crash_test.sh` is one instance of
   what should be a loop.
7. **Index paging** (or the LSM rewrite) to lift the RAM ceiling on key count.

---

## 6. Things I changed while building

- **The index started as a map alone.** I discovered `ReadKeyRange` while
  re-reading the brief and added the skip list rather than sorting per query.
- **The record header did not originally carry a sequence number**; ordering was
  implied by file id. That broke the moment compaction wanted to write merged
  files with fresh ids, and it would have broken replication too. Adding an
  8-byte sequence to every record was the highest-leverage change in the project.
- **`BatchPut` built its record scratch buffer before taking the write lock.**
  The concurrency test caught it as `corrupt record in 0000000007.data at 29664`
  — two writers scribbling over the same buffer. Fixed by moving the build
  inside the lock; the test is `TestConcurrentReadersWritersScanners`.
- **`datafile.size` was a plain `int64`** written by the writer and read by
  compaction. `go test -race` caught it. Now `atomic.Int64`.
- **Shutdown hung** because `Close` waited on connection handlers parked in
  reads. Nodes now track accepted connections and close them on shutdown.
- **Followers ignored leadership announcements** because they never learned the
  current epoch — a promotion at epoch 1 could not displace a bootstrap leader
  also at epoch 1. The epoch now rides on the replication heartbeat.
- **Compaction over the network timed out** on the 16 GiB store: 98 s of merging
  against a 10 s client timeout. It is now acknowledged and runs in the
  background, with progress visible through `stats`.

## AI usage

See [AI_USAGE.md](AI_USAGE.md).
