# gokv — A Network-Available Persistent Key/Value Store

A log-structured key/value storage engine with a TCP server, Go client, leader/follower replication, and automatic failover.

**Go standard library only** — no third-party modules. The `go.mod` file contains no `require` block.

`gokv` combines persistent storage, indexing, networking, replication, leader election, crash recovery, concurrency testing, and performance benchmarking in a single project.

---

## Why gokv?

`gokv` was built to explore how a database-like system can be implemented from first principles using only the Go standard library.

The project focuses on three major areas:

1. **Storage** — append-only persistence, indexing, recovery, tombstones, and compaction.
2. **Networking** — TCP client/server communication using a length-prefixed binary protocol.
3. **Distributed systems** — leader/follower replication, leader discovery, automatic election, failover, and client retry.

The implementation is accompanied by automated tests, race-detector validation, crash-recovery tests, failover demonstrations, and storage-engine benchmarks.

---

## Features

### Storage Engine

* Persistent log-structured key/value storage
* Append-only storage files
* In-memory hash-map index for fast point lookups
* Ordered skip-list index for range queries
* Compact record locators
* Tombstones for deleted keys
* Crash recovery
* Hint files for faster recovery
* Background compaction
* Configurable file rotation
* Configurable synchronization policy

### Database Operations

```text
Put(key, value)
Read(key)
ReadKeyRange(start, end)
BatchPut(keys..., values...)
Delete(key)
```

### Networking

* TCP client/server communication
* Length-prefixed binary protocol
* Concurrent client connections
* Server-side range-query limits
* Follower-to-leader redirects
* Automatic client retry

### Distributed System

* Three-node leader/follower cluster
* Replicated writes
* Leader discovery
* Follower redirects
* Automatic leader election
* Automatic failover
* Client failover and retry
* Failed-node recovery and rejoining

### Validation

* Unit tests
* Integration tests
* Go race detector
* Crash-recovery testing
* Leader-failover testing
* Replication demonstrations
* Storage-engine benchmarks

---

## Requirements

* **Go 1.21 or newer**

The project uses the Go `min`/`max` built-ins introduced in Go 1.21.

No third-party dependencies are required.

Build the complete project with:

```bash
go build ./...
```

---

# Quick Start

## Start a Single Node

Start the key/value server:

```bash
go run ./cmd/kvd -addr 127.0.0.1:7070 -dir ./data
```

In another terminal, start the client:

```bash
go run ./cmd/kvcli -addrs 127.0.0.1:7070
```

Example commands:

```text
gokv> put user:1 '{"name":"ada"}'

gokv> get user:1

gokv> fill item: 5000

gokv> range item:00000100 item:00000104

gokv> stats
```

---

# Basic Database Operations

## Put

```text
put user:1 '{"name":"ada"}'
```

Stores a key/value pair.

## Get

```text
get user:1
```

Retrieves a value by key.

## Delete

```text
delete user:1
```

Deletes a key by writing a tombstone record.

## Range Query

```text
range item:00000100 item:00000104
```

Returns keys within the specified range.

## Batch Data

```text
fill item: 5000
```

Generates a larger dataset for testing and range-query demonstrations.

## Statistics

```text
stats
```

Displays information about the current database state.

---

# Architecture

A request flows through three primary layers:

```text
                         CLIENT
                           │
                           │
              Length-Prefixed Binary
                 Frames over TCP
                           │
                           ▼
                ┌─────────────────────┐
                │      KVD NODE       │
                │                     │
                │ Request Handling    │
                │ Redirects           │
                │ Replication         │
                │ Leader Election     │
                │ Failover            │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │    STORAGE ENGINE   │
                │                     │
                │ In-Memory Index     │
                │                     │
                │ Hash Map            │
                │ key -> locator      │
                │ O(1) lookup         │
                │                     │
                │ Skip List           │
                │ Ordered Keys        │
                │ O(log n) operations │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │      ON DISK        │
                │                     │
                │ Append-Only Log     │
                │                     │
                │ 000..1.data         │
                │ 000..2.data         │
                │ 000..3.data         │
                │                     │
                │ Hint Files          │
                │ 000..1.hint         │
                │ 000..2.hint         │
                └─────────────────────┘
```

The three major layers are:

### 1. Client and Protocol

The client communicates with a server using TCP and a length-prefixed binary protocol.

### 2. Node

The node handles:

* Client requests
* Leader discovery
* Follower redirects
* Replication
* Elections
* Failover

### 3. Storage Engine

The storage engine handles:

* Persistent records
* In-memory indexing
* Recovery
* Range queries
* Tombstones
* Compaction

---

# Storage Engine

Every write is appended to the tail of the log.

The storage engine maintains compact metadata in memory while keeping the actual values primarily on disk.

A point lookup follows this general path:

```text
Map lookup
     ↓
Record locator
     ↓
File offset
     ↓
pread
     ↓
Value
```

This allows the system to keep the bulk of stored data on disk rather than requiring the entire dataset to fit in memory.

## Indexes

The storage engine uses two complementary indexes.

### Hash Map

The hash-map index provides fast point lookups:

```text
key
 │
 ▼
hash map
 │
 ▼
record locator
 │
 ▼
disk
```

Point lookups are approximately **O(1)** on average.

### Skip List

The ordered skip-list index maintains keys in sorted order and supports range operations.

```text
key ordering
     │
     ▼
skip list
     │
     ▼
range scan
```

Skip-list operations are approximately **O(log n)** for search/update operations.

---

# Persistence and Recovery

The storage engine uses append-only log files.

Instead of modifying records in place, updates are appended as new records.

Conceptually:

```text
PUT k=v1
PUT k=v2
DELETE k
PUT k=v3
```

The latest valid record determines the current state of the key.

## Tombstones

Deletes are represented using tombstone records.

A deleted key therefore remains represented in the log until compaction.

This is important because recovery must correctly apply the newest record regardless of the order in which records are encountered.

The storage engine follows a **newest sequence wins** rule when reconstructing state.

## Hint Files

Hint files can accelerate recovery by storing compact information about records that need to be reconstructed.

Recovery can therefore use:

```text
Append-only log
       or
Hint files + log
```

depending on the available recovery information.

## Compaction

Over time, an append-only log contains obsolete records.

For example:

```text
PUT user:1 = A
PUT user:1 = B
PUT user:1 = C
DELETE user:1
```

Only the latest state needs to survive compaction.

Background compaction merges immutable files and removes superseded records and obsolete tombstones where safe.

---

# Crash Recovery

The storage engine is designed to recover acknowledged writes after unexpected process termination.

Run the crash-recovery test:

```bash
scripts/crash_test.sh always
```

A demonstrated result was:

```text
server acknowledged 3000 writes before the kill

PASS: all 3000 acknowledged writes survived SIGKILL
```

This test forcefully terminates the server with `SIGKILL` and verifies that acknowledged writes survive the crash.

---

## Synchronization Modes

The server supports:

```text
-sync always
-sync interval
-sync never
```

The default mode is:

```text
-sync interval
```

The interval can be configured with:

```text
-sync-every 200ms
```

The faster synchronization mode can be tested with:

```bash
scripts/crash_test.sh interval
```

A demonstrated result was:

```text
result: lost 0 of 3000 acknowledged writes with -sync interval
```

The documented durability trade-off of interval synchronization is that data from up to one synchronization interval could potentially be lost if the process crashes before the next synchronization.

---

# Distributed Architecture

`gokv` supports a three-node leader/follower cluster.

```text
                         ┌─────────────┐
                         │   Client    │
                         └──────┬──────┘
                                │
                                ▼
                     ┌──────────────────┐
                     │    Leader Node   │
                     │     :7070        │
                     └────────┬─────────┘
                              │
                         Replication
                    ┌─────────┴─────────┐
                    │                   │
                    ▼                   ▼
             ┌────────────┐      ┌────────────┐
             │ Follower   │      │ Follower   │
             │   :7071    │      │   :7072    │
             └────────────┘      └────────────┘
```

The cluster uses a leader/follower model.

Writes are handled through the leader and replicated to followers.

If a client sends a write request to a follower, the follower returns a redirect to the leader. The client follows the redirect automatically.

If a node fails during a request, the client can retry against the remaining nodes.

This allows failover without requiring the application to be manually reconfigured.

---

# Distributed Cluster Demo

The project includes scripts for demonstrating replication and failover.

Start the cluster:

```bash
scripts/cluster_demo.sh
```

The demonstration starts:

```text
127.0.0.1:7070
127.0.0.1:7071
127.0.0.1:7072
```

The cluster demonstrates:

* Leader and follower roles
* Replicated writes
* Follower-to-leader redirects
* Reads after replication
* Continued operation after leader failure

Stop the cluster:

```bash
scripts/cluster_demo.sh --stop
```

---

# Leader Failover

Run the failover demonstration:

```bash
scripts/failover_demo.sh
```

The demonstration terminates the current leader using `SIGKILL` and waits for the remaining nodes to elect a new leader.

The same client configuration can then continue sending writes without application reconfiguration.

The demonstration verifies that:

1. A leader can fail.
2. The remaining nodes can elect a new leader.
3. The client can continue accepting writes.
4. Data written before failover remains available.
5. The failed node can restart and rejoin the cluster.

Conceptually:

```text
Leader
  │
  ├── Follower
  │
  └── Follower
       │
       │ Leader failure
       ▼
Election
       │
       ▼
New Leader
       │
       ▼
Client continues operating
```

---

# Using the Client Library

The Go client can be used directly from an application:

```go
c, _ := client.New(
    []string{
        "127.0.0.1:7070",
        "127.0.0.1:7071",
    },
    client.Options{},
)

defer c.Close()

c.Put([]byte("k"), []byte("v"))

v, err := c.Read([]byte("k"))

c.BatchPut(keys, values)

pairs, _ := c.ReadKeyRange(
    []byte("a"),
    []byte("m"),
    0,
)

err = c.ScanKeyRange(
    []byte("a"),
    []byte("m"),
    0,
    func(k, v []byte) error {
        // process key/value
        return nil
    },
)

c.Delete([]byte("k"))
```

Writes sent to a follower are answered with a redirect to the leader, and the client follows the redirect automatically.

If a node fails during a request, the client retries against available nodes so that failover does not require application reconfiguration.

---

# Project Layout

| Path              | Description                                                         |
| ----------------- | ------------------------------------------------------------------- |
| `internal/engine` | Storage engine: records, log files, index, recovery, and compaction |
| `internal/proto`  | Wire protocol: framing and payload encoding                         |
| `internal/node`   | TCP server, replication, elections, and distributed-system logic    |
| `client`          | Go client with leader discovery and failover                        |
| `cmd/kvd`         | Key/value server                                                    |
| `cmd/kvcli`       | Interactive shell and one-shot client                               |
| `cmd/kvbench`     | Load generator with throughput and latency reporting                |
| `scripts`         | Cluster, failover, and crash-recovery demonstrations                |

---

# Server Flags

| Flag                 | Description                                         | Default             |
| -------------------- | --------------------------------------------------- | ------------------- |
| `-addr`              | Address to listen on and advertise                  | `127.0.0.1:7070`    |
| `-dir`               | Data directory                                      | `./data`            |
| `-id`                | Node ID                                             | Defaults to `-addr` |
| `-peers`             | All nodes in the cluster, comma separated           | —                   |
| `-bootstrap`         | Start as the leader                                 | —                   |
| `-leader`            | Leader address to follow at startup                 | —                   |
| `-sync`              | Synchronization mode: `always`, `interval`, `never` | `interval`          |
| `-sync-every`        | fsync period when `-sync=interval`                  | `200ms`             |
| `-max-file-size`     | Active-file rotation threshold                      | `256 MiB`           |
| `-compact-threshold` | Dead-record fraction required for compaction        | `0.4`               |
| `-compact-interval`  | How often compaction is considered                  | `30s`               |
| `-heartbeat`         | Leader keepalive period                             | `1s`                |
| `-election-timeout`  | Silence before a follower calls an election         | `5s`                |
| `-max-range`         | Server-side cap for one range query                 | `100000`            |

---

# Testing

Run the complete test suite:

```bash
go test ./...
```

The project includes unit and integration tests for the engine, node, and protocol components.

A successful run includes results such as:

```text
ok      gokv/internal/engine
ok      gokv/internal/node
ok      gokv/internal/proto
```

Packages without test files may report:

```text
[no test files]
```

---

# Race Detection

Because the project contains concurrent components, the Go race detector is also used:

```bash
go test -race -count=3 ./...
```

Running the test suite multiple times with race detection helps identify data races that may not appear during a single execution.

The demonstrated test run completed successfully without reporting race conditions.

---

# Performance Benchmarks

Run the storage-engine benchmarks with:

```bash
go test -run XXX -bench . -benchtime 3s ./internal/engine
```

The benchmark suite includes:

```text
BenchmarkPutRandom-20
BenchmarkPutRandomSyncAlways-20
BenchmarkBatchPut100-20
BenchmarkGetRandom-20
BenchmarkGetParallel-20
BenchmarkScan100-20
```

A demonstrated run completed with:

```text
PASS
ok      gokv/internal/engine    30.137s
```

## Example Measurements

### Random PUT

```text
BenchmarkPutRandom-20
1000000 iterations
4455 ns/op
28.73 MB/s
```

Approximately:

```text
4.5 microseconds per operation
```

in this benchmark.

### Batch PUT

```text
BenchmarkBatchPut100-20
49881
86362 ns/op
148.21 MB/s
```

The benchmark processes 100 items per benchmark operation and achieved approximately:

```text
148 MB/s
```

in this run.

### Random GET

```text
BenchmarkGetRandom-20
3325984
1117 ns/op
```

Approximately:

```text
1.1 microseconds per operation
```

in this benchmark.

### Parallel GET

```text
BenchmarkGetParallel-20
17569912
211.8 ns/op
```

Approximately:

```text
212 nanoseconds per operation
```

in this benchmark.

### Range Scan

```text
BenchmarkScan100-20
75817
46359 ns/op
```

Approximately:

```text
46 microseconds per operation
```

in this benchmark.

> Benchmark results are environment-dependent and should be treated as measurements of this implementation rather than universal performance guarantees.

---

# Design Trade-offs

## 1. Durability vs. Performance

More frequent disk synchronization provides stronger durability guarantees but can increase I/O overhead.

The synchronization modes allow different points on this trade-off:

```text
Higher Durability
       ▲
       │
   Sync Always
       │
       │
       │
       │
       └──────────────────► Higher Performance
             Interval
```

`sync=interval` can provide better performance while accepting a bounded durability window.

---

## 2. Memory vs. Performance

The storage engine maintains an in-memory index to provide efficient key lookups.

This improves read performance but requires memory for index metadata.

The design therefore keeps the actual values primarily on disk while maintaining compact key-location information in memory.

```text
In-Memory Index
       │
       ▼
Fast Lookups
       │
       ▼
Additional Memory Usage
```

---

## 3. Availability vs. Complexity

Replication, leader election, and automatic failover improve availability.

However, distributed behavior introduces substantially more complexity than a single-node database.

```text
Replication
     +
Leader Election
     +
Failover
     │
     ▼
Higher Availability
     │
     ▼
Greater System Complexity
```

The project demonstrates this trade-off through its three-node cluster and leader-failure tests.

---

# AI Usage

AI tools were used as development assistants during the project.

AI was used to help with:

* Understanding the existing codebase
* Investigating errors and unexpected behavior
* Discussing architecture and implementation approaches
* Identifying potential improvements
* Reviewing possible design trade-offs

AI was **not treated as the final source of truth**.

The implementation was independently validated using:

* Automated tests
* Go's race detector
* Crash-recovery tests
* Leader-failover tests
* Replication demonstrations
* Performance benchmarks

This validation was important because the project's behavior had to be verified through actual execution rather than relying solely on generated suggestions.

For additional details, see:

* [**AI_USAGE.md**](AI_USAGE.md)

---

# Limitations and Future Improvements

## Network Failure Testing

Additional network-failure scenarios could be added, including:

* Connection interruptions
* Delayed messages
* Partial network failures
* Temporary node isolation
* Reconnection scenarios

## Observability

The system could provide more comprehensive:

* Metrics
* Structured logging
* Replication status
* Election status
* Recovery statistics
* Compaction statistics

## Benchmark Coverage

The benchmark suite could be expanded to cover:

* Different dataset sizes
* Different key distributions
* Different value sizes
* Concurrent workloads
* Replication workloads
* Recovery workloads
* Compaction workloads

## Replication and Recovery

More extensive testing could be added around:

* Multiple simultaneous node failures
* Recovery during active replication
* Rejoining after extended downtime
* Repeated leader elections
* Recovery under high write load

---

# Design Documentation

Additional project documentation is available in:

* [**SOLUTION.md**](SOLUTION.md) — design decisions, trade-offs, measurements, and known limitations
* [**AI_USAGE.md**](AI_USAGE.md) — details about how AI tools were used during development

---

# Conclusion

`gokv` is a persistent, network-accessible key/value store implemented using only the Go standard library.

The project combines:

* Persistent storage
* Append-only logging
* In-memory indexing
* Ordered range queries
* Batch operations
* TCP networking
* Binary protocol design
* Replication
* Leader election
* Automatic failover
* Crash recovery
* Concurrency testing
* Performance benchmarking

The primary goal was not only to implement the required functionality, but also to verify the system's behavior under failure conditions and measure its performance.

The project provides practical experience with:

* Go
* Persistent storage
* Log-structured storage engines
* Indexing
* Networking
* Distributed systems
* Replication
* Leader election
* Failover
* Crash recovery
* Concurrency
* Performance analysis

The result is a compact database-style system that demonstrates how storage, networking, and distributed-system concepts can be implemented and validated using the Go standard library alone.
