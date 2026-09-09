# gokv — a network-available persistent key/value store

A log-structured key/value storage engine with a TCP server, a Go client,
leader/follower replication and automatic failover. **Go standard library only** —
no third-party modules, and `go.mod` has no `require` block.

```
Put(key, value)                  Read(key)                ReadKeyRange(start, end)
BatchPut(keys..., values...)     Delete(key)
```

Requires Go 1.21 or newer (`min`/`max` builtins); nothing else.

## Quick start

```bash
go build ./...

# single node
go run ./cmd/kvd -addr 127.0.0.1:7070 -dir ./data

# in another shell
go run ./cmd/kvcli -addrs 127.0.0.1:7070
gokv> put user:1 '{"name":"ada"}'
gokv> get user:1
gokv> fill item: 5000
gokv> range item:00000100 item:00000104
gokv> stats
```

Three nodes with replication and failover, end to end:

```bash
scripts/cluster_demo.sh     # leader + 2 followers, writes, redirects, reads
scripts/failover_demo.sh    # SIGKILL the leader, watch a follower take over
scripts/cluster_demo.sh --stop
```

Crash friendliness, verified against a real `kill -9`:

```bash
scripts/crash_test.sh always     # nothing acknowledged is ever lost
scripts/crash_test.sh interval   # reports what a crash costs in the fast mode
```

## Design in one picture

```
   client ──┐
            │  length-prefixed binary frames over TCP
            ▼
   ┌──────────────────────────────────────────────────────┐
   │ node        request handling, redirects, replication  │
   ├──────────────────────────────────────────────────────┤
   │ engine                                                │
   │                                                       │
   │   in memory          hash map:   key -> *node   O(1)  │
   │                      skip list:  ordered keys  O(log n)
   │                          │                            │
   │                          │ (file, offset, size, seq)  │
   │                          ▼                            │
   │   on disk    append-only log, one record per write    │
   │              000..1.data 000..2.data  000..3.data     │
   │              000..1.hint 000..2.hint     (active)     │
   └──────────────────────────────────────────────────────┘
```

Every write is an append to the tail of the log. The in-memory index holds only
the key and a 32-byte locator, so the values — the bulk of the data — never need
to fit in RAM. Deleted keys keep a tombstone entry until the next compaction, so
that "newest sequence wins" can be applied to every record, whatever order it
arrives in. A read is one map lookup plus one `pread`. Recovery replays the
log, or the compact `.hint` sidecars when they exist. Background compaction
merges the immutable files and drops superseded records.

See [SOLUTION.md](SOLUTION.md) for the trade-offs, the measurements, and an
honest list of what is missing. [AI_USAGE.md](AI_USAGE.md) covers how AI tools
were used.

## Layout

| path | what it holds |
| --- | --- |
| `internal/engine` | storage engine: records, log files, index, recovery, compaction |
| `internal/proto` | wire format: framing and payload encoding |
| `internal/node` | TCP server, replication, elections |
| `client` | Go client with leader discovery and failover |
| `cmd/kvd` | the server |
| `cmd/kvcli` | interactive shell and one-shot client |
| `cmd/kvbench` | load generator, reports throughput and latency percentiles |
| `scripts` | cluster, failover and crash demonstrations |

## Server flags

```
-addr              address to listen on and advertise      (127.0.0.1:7070)
-dir               data directory                          (./data)
-id                node id                                 (defaults to -addr)
-peers             every node in the cluster, comma separated
-bootstrap         start as the leader
-leader            leader address to follow at startup
-sync              always | interval | never               (interval)
-sync-every        fsync period when -sync=interval        (200ms)
-max-file-size     rotation threshold for the active file  (256 MiB)
-compact-threshold reclaim above this dead fraction        (0.4)
-compact-interval  how often compaction is considered      (30s)
-heartbeat         leader keepalive period                 (1s)
-election-timeout  silence before a follower calls an election (5s)
-max-range         server-side cap on one range query      (100000)
```

## Using the client library

```go
c, _ := client.New([]string{"127.0.0.1:7070", "127.0.0.1:7071"}, client.Options{})
defer c.Close()

c.Put([]byte("k"), []byte("v"))
v, err := c.Read([]byte("k"))
c.BatchPut(keys, values)
pairs, _ := c.ReadKeyRange([]byte("a"), []byte("m"), 0)
err = c.ScanKeyRange([]byte("a"), []byte("m"), 0, func(k, v []byte) error { ... })
c.Delete([]byte("k"))
```

Writes sent to a follower are answered with a redirect to the leader, and the
client follows it. If a node dies mid-request the client retries against the
others, so a failover does not need the application to be reconfigured.

## Tests

```bash
go test ./...                      # unit + integration
go test -race -count=3 ./...       # what CI should run
go test -run XXX -bench . -benchtime 3s ./internal/engine
```
