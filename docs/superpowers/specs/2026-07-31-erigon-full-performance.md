# Erigon Full-Performance Alignment

Date: 2026-07-31

Status: active implementation

## Goal

Bring go-tron's storage and sync pipeline to Erigon-class hardware efficiency
without changing java-tron consensus, transaction ordering, receipts, roots, or
wire behaviour. "Full performance" means mechanism and bottleneck parity, not
equal blocks/second across unrelated Ethereum and TRON workloads.

The production starting point on the mainnet sync host is approximately:

- 1,196 blocks/s during the early-chain sample;
- 2.69 CPU cores used during a 15-second CPU profile;
- Pebble `Get` at about 21% cumulative CPU and `Has` at about 13%;
- no Pebble write stalls, with roughly 0.8-1.0 GB compaction debt;
- a continuously non-empty downloader buffer and zero retry pressure;
- a functioning chain freezer moving 30,000 blocks in roughly four seconds per
  pass.

This says the downloader, EBS bandwidth, and compaction are not the current
hard limit. The next limit is the serial execution/state-read path.

## Non-Negotiable Correctness Gates

Every phase must retain all of the following:

- byte-identical block acceptance, transaction results, java account-state
  roots, internal commitment roots, dynamic properties, and fork decisions;
- transaction state effects published in canonical java-tron order;
- restart from every persisted stage boundary without datadir reset;
- reorg and unwind invalidation of every mutable cache entry;
- storage failures distinguished from confirmed missing rows;
- race-clean execution for all newly concurrent read and validation paths.

No performance result is accepted if it requires weakening one of these gates.

## Erigon Mechanisms And go-tron Work

### P1: Cache-coupled read ahead and single-lookup misses

Erigon's `execution/exec/blocks_read_ahead.go` deliberately populates the same
`StateCache` that `SharedDomains.GetLatest` probes. Merely warming the OS page
cache is called out as insufficient.

go-tron's first prefetch implementation accepted a `vmKVStore`. That wrapper
preserved ordinary reads but did not preserve the blockbuffer's typed
`GetNoCopyCachedState*` capabilities. Prefetch therefore warmed Pebble while
canonical execution still had to enter the blockbuffer cache independently.

The first implementation slice:

- forwards generic and typed no-copy cache reads through `vmKVStore`;
- forwards the durable backend's missing-key classifier;
- lets rawdb consume a confirmed missing-key classification without a second
  `Has` point lookup;
- lets `Buffer.Has` answer from an authoritative positive or negative base
  cache entry after checking overlays;
- removes defensive value copies from read-only state prefetch consumers.

Acceptance for P1 on a fixed transaction-dense replay window:

- identical roots and transaction info with prefetch off/on;
- zero state-prefetch errors and no race failures;
- at least 50% lower Pebble `Has` samples attributable to latest-state misses;
- at least 5% higher throughput or 10% lower state-read CPU, with no more than
  2% regression on empty/light blocks.

### P2: Stage-lifetime and cross-block read ahead

The current worker pool is created per block and only looks ahead within that
block. The next slice will attach read ahead to `canonicalRangeExecutor` /
`InsertSession`, enqueue deterministic keys while staged bodies are decoded,
and stop it at the session barrier. Values continue to land in the shared,
versioned base cache; overlay writes always win.

Required safeguards:

- bounded key and byte budgets rather than an unbounded session-wide `seen`
  map;
- per-block or versioned deduplication so a key changed by an earlier block can
  be warmed again;
- cache invalidation tied to blockbuffer flush, discard, reset, and unwind;
- backpressure counters separating queue saturation from read errors.

### P3: Parallel work that cannot change state order

Use idle cores before attempting parallel mutation:

- protobuf body decode and signature recovery (already partially parallel);
- static envelope validation whose result is rechecked at the serial boundary;
- deterministic prefetch-key extraction;
- receipt/log/trace encoding and recoverable derived-index ETL;
- cold segment reads and decompression.

Results are consumed in original block/transaction order. The serial path owns
the final accept/reject decision.

### P4: Conflict-aware speculative transaction execution

Erigon's parallel executor cannot be copied directly because TRON actuators,
resource accounting, maintenance, TAPOS, permission changes, and dynamic
properties have stronger ordered dependencies.

The safe target is optimistic execution with serial publication:

1. record physical/logical read and write sets for each transaction;
2. execute candidates against an immutable pre-wave snapshot plus versioned
   overlays;
3. detect read-after-write, write-after-read, write-after-write, system-key,
   balance/resource, VM call, and dynamic-property conflicts;
4. publish conflict-free results strictly in transaction order;
5. replay every conflicted or unsupported transaction on the authoritative
   serial StateDB;
6. compare the final journal, receipts, logs, fees, roots, and metadata with the
   serial reference in shadow mode before enabling publication.

Initial parallel eligibility will be intentionally narrow: simple transfers
with disjoint accounts and no shared fee/resource/system keys. Coverage expands
only with fixture and mainnet replay evidence.

### P5: Snapshot-first bootstrap and steady-state cold lifecycle

Erigon-class initial sync also requires avoiding execution from genesis when a
trusted snapshot is available. Complete the production path for signed catalog
distribution, resumable segment download, restore, recent-tail execution, and
automatic freezer/history build-merge-prune scheduling.

The hot Pebble database must reach a bounded steady state. Freezer, derived
indexes, and state history must keep up with import without monotonic compaction
debt or hidden hot-history growth.

## Benchmark And Production Acceptance

All comparisons use the same binary settings, datadir snapshot, hardware, Go
version, cache sizes, and fixed block ranges. Report blocks/s, transactions/s,
gas/energy work, CPU-seconds/block, allocations, Pebble point reads, cache
hits/misses, compaction bytes, write stalls, freezer lag, and stage progress.

Final alignment gates are:

- at least 1.5x throughput on the contract/transaction-dense replay suite and
  1.25x on the weighted replay suite relative to the 2026-07-31 baseline;
- no more than 3% regression on empty/light-block throughput;
- useful scaling across at least half of the assigned execution cores on the
  dense suite, without relying on downloader starvation;
- zero write stalls and bounded compaction debt during a 30-minute run;
- freezer/cold lifecycle throughput at or above sustained canonical import, or
  a demonstrably bounded lag;
- byte-identical serial/shadow roots and transaction results across the fixed
  replay suite, followed by restart and reorg drills;
- a 24-hour mainnet soak with clean stage verification and no datadir reset.

The final goal is reached only when both execution efficiency and snapshot/cold
lifecycle gates pass. A high blocks/s number over early empty blocks is useful
diagnostic evidence but is not an acceptance result.
