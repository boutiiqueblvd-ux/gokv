# AI usage

## Tools

**Claude Code (Claude Opus)** in the terminal, for the whole build: design
discussion, writing code, writing tests, running them, and interpreting the
output. No other AI tools. No third-party libraries were used at any point —
the brief asked for standard library only, and `go.mod` has no `require` block.

The machine had no Go toolchain, so the first thing the session did was fetch
and unpack Go 1.27.1. Everything after that is ordinary build-test-fix.

## How I actually used it

The loop was: decide the shape myself, have the model write the mechanical
parts, then attack the result with tests. Concretely:

1. **Design decisions were mine, argued explicitly.** Bitcask over LSM,
   sequence numbers as the ordering authority, full merges so tombstones can be
   dropped safely, async replication with a documented loss window. I asked the
   model to argue for and against each and then chose; the reasoning is written
   up in SOLUTION.md in my own terms, not as a summary of what it said.
2. **The model wrote the volume**: record encoding, the skip list, framing,
   flag plumbing, CLI, benchmark harness. This is the code where an AI is
   genuinely fast and where mistakes are cheap to catch.
3. **Tests were the control.** `go test -race`, a `kill -9` script against a
   real server, a 16 GiB dataset on a 15 GiB machine. Every number in
   SOLUTION.md came from a run, not from the model's estimate.

## Outputs I accepted

- **The Bitcask skeleton** — append-only log, keydir, hint sidecars,
  merge-and-swap compaction. Standard, well-documented, and it matched the
  papers linked in the brief. I reviewed the record layout byte by byte before
  accepting it.
- **The skip list.** ~120 lines, textbook, and I verified its behaviour through
  the range tests (ordering, deletes disappearing from ranges, inverted ranges,
  a 20k-key store spread over dozens of files) rather than by reading it twice.
- **The framing layer** (4-byte length prefix, opcode/status byte). I did
  require one addition: `Dec` had to fail closed on truncated input, since
  bodies come off the network. `TestDecoderFailsClosedOnTruncation` walks every
  truncation offset of a frame — that test exists because I did not want to
  trust the decoder by inspection.

## Outputs I rejected or changed

- **Buffered writes to the active file.** The first draft used a
  `bufio.Writer`, which quietly breaks reads: a `ReadAt` cannot see bytes still
  sitting in the buffer, so a key could be acknowledged and then not found.
  Replaced with `WriteAt` at an explicit offset — one syscall per append, always
  visible, and a failed write truncates back to the record boundary so the log
  stays parseable.
- **Building hint files incrementally as records are written.** Proposed, and
  it would have added a write per write. Replaced with generating the hint in
  the background once a file is rotated and immutable. A missing hint is never
  fatal, so nothing is lost if the process dies first — and
  `TestHintFilesAreUsedAndCorrect` corrupts a hint file deliberately to prove
  recovery falls back to scanning.
- **A skip list as the only index.** Rejected: point reads would become ~20
  pointer chases. Keeping a hash map alongside costs ~50 bytes per key, and I
  would rather spend that and document the memory ceiling.
- **Dropping tombstones during a partial merge.** This one was subtle enough to
  be worth the space it gets in SOLUTION.md: dropping a tombstone while an older
  file survives resurrects the deleted key. The model's first compaction sketch
  merged a subset of files. I constrained it to full merges only, and wrote
  `TestFullResyncAfterCompactionErasesTheGap` to cover the replication-side
  version of the same hazard, where a follower would keep a key whose tombstone
  had been compacted away.
- **The initial record header had no sequence number.** Order was implied by
  file id. That collapses as soon as compaction writes merged files with fresh
  ids — a stale record in a merged file would shadow a newer one. Adding an
  8-byte sequence to every record fixed recovery, compaction and replication in
  one move; it is the change I would point at if asked what mattered most.

## Bugs the model wrote that testing caught

These are the interesting ones, because they are the failure mode of AI code:
plausible, well-commented, and wrong under concurrency.

| bug | how it surfaced | fix |
| --- | --- | --- |
| `BatchPut` built its shared record scratch buffer **before** taking the write lock | `TestConcurrentReadersWritersScanners` failed with `corrupt record in 0000000007.data at 29664` — two writers overwriting each other's buffer | moved the build inside the critical section |
| `datafile.size` read by compaction while the writer mutated it | `go test -race` | `atomic.Int64` |
| `Close` hung forever | the node test suite hit its 300 s timeout; the goroutine dump showed handlers parked in `ReadFrame` on idle client connections | nodes track accepted connections and close them on shutdown |
| followers ignored leadership announcements | `TestDemotedLeaderRejoinsAndDiscardsDivergentWrites` timed out — a promotion at epoch 1 could not displace a bootstrap leader also at epoch 1 | epoch now rides on the replication heartbeat so followers know the current one |
| synchronous compaction over the wire | the 16 GiB store took 98 s to merge against a 10 s client timeout | compaction is acknowledged and runs in the background |

The pattern is consistent: the model was reliable on structure and on
single-threaded logic, and unreliable exactly where Go concurrency is
unforgiving. I did not find any of these by reading the code. The race
detector, a concurrency stress test, and a timeout found all of them, which is
why I front-loaded those rather than reviewing more carefully.

## The review pass, and what it says about AI-written code

Once it all worked I stopped adding features and went looking for bugs on
purpose — reasoning about invariants rather than asking the model to review its
own output, which in my experience produces agreeable nonsense. Section 4 of
SOLUTION.md lists what came out. The most instructive one:

> A delete removed the key from the index. That is the obvious implementation,
> it passes every test you would naturally write, and it is wrong — because
> removing the key also discards the sequence number that made the delete
> authoritative. An older record for that key arriving afterwards then looks
> like a new write. And "afterwards" happens in normal operation: recovery
> walks files in id order while a merged file's records are in key order.

Nothing about that code looks suspicious. The bug lives in the interaction
between three components the model wrote at different times, each locally
correct. I found it by asking "what order can records reach this function in,
and does the code depend on that order?" — and then writing the test before the
fix, so I could watch it fail.

That is the thing I would tell someone starting a build like this: the model's
failures are not in the lines, they are in the seams. My existing test
`TestApplyRawIsIdempotentAndOrderInsensitive` had even applied the records in an
order that *contained* the bug, and passed anyway, because a later replay
happened to repair the damage. A test that exercises a bug is not the same as a
test that detects one.

Every fix in that pass landed with a regression test, and I checked each test by
reverting the fix and watching it fail — for the resurrection bug, the connection
desync, and the batch-marker loss. A regression test you have never seen fail is
a guess.

## How I validated

- `go test -race -count=3 ./...` — the engine's concurrency test runs mixed
  puts, gets, scans and batches from 8 goroutines against a store rotating
  files every 32 KiB, then asserts the index and the log still agree.
- **Crash tests against a real process.** `scripts/crash_test.sh` writes a
  stream, records exactly which writes the server acknowledged, `kill -9`s it
  mid-stream, restarts, and checks every acknowledgement. With `-sync always`:
  425 acknowledged, 425 recovered. That is the claim the brief cares about, and
  I did not want to make it from a unit test that fakes the crash.
- **Corruption tests.** Random garbage appended to the log; a bit flipped inside
  a stored value; a truncated batch; a corrupted hint file. Each has a test
  asserting the specific expected behaviour rather than "it doesn't crash".
- **Scale.** 4M writes of 4 KiB values → 15.8 GiB on a 15 GiB machine, 0.64 GiB
  resident, random reads at p50 713 µs, restart in 8.3 s. This is what turned
  "handles datasets larger than RAM" from a design claim into a measurement.
- **The cluster paths were exercised as processes**, not just in-process tests:
  `scripts/cluster_demo.sh` and `scripts/failover_demo.sh` start three `kvd`
  binaries, SIGKILL the leader, and show a follower taking over at epoch 2 in
  ~2.9 s with the client unchanged.

## What I would do differently

- **Write the concurrency stress test first.** Every real bug came from it or
  from `-race`. I wrote the happy-path tests first out of habit and paid for it.
- **Push back harder, earlier, on generated comments.** The first drafts
  explained *what* the code did. Comments that only restate the line are worse
  than none, because they age badly and lull the reviewer. I rewrote them to say
  *why* — the trade-off, the invariant, the failure that motivated the design.
- **Not let the model choose the durability default.** It defaulted to
  fsync-per-write, which is defensible but three orders of magnitude slower and
  hides a decision that belongs to the operator. Making it a flag, and
  measuring both modes, was the right call and I should have made it up front.
- **Be sceptical of confidently-correct concurrent code.** The buffered-writer
  bug and the batch-buffer race were both written with fluent, self-assured
  comments. Fluency correlates with nothing.
