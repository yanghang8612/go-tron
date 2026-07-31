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

### P1: Shared latest-state cache substrate

Erigon's state read path depends on one process-level `StateCache` shared by
canonical `SharedDomains.GetLatest`, flush publication, unwind, and
`execution/exec/blocks_read_ahead.go`. Merely warming the OS page cache is not
equivalent because canonical execution would still pay the database lookup and
decode path.

go-tron's blockbuffer base cache is the corresponding shared substrate. It is
bounded, understands positive and negative rows, sits below rewindable
overlays, refreshes cached values after canonical flush, clears on out-of-band
discard, and rejects a durable read that races a same-key flush by comparing an
invalidation generation. Canonical latest-account, latest-KV, state-code, and
commitment reads already use this cache.

An earlier opt-in, per-block prefetch experiment was rejected because it added
hot/light-block overhead and was removed when latest-domain fresh sync became
mandatory. P2 does not restore that rollout or its compatibility flags. It
uses the current blockbuffer substrate and is always scoped to the bulk-sync
session.

The retained P1 substrate:

- exposes generic and typed no-copy cached reads directly through blockbuffer;
- forwards the durable backend's missing-key classifier so rawdb can consume a
  confirmed miss without a second `Has` point lookup;
- lets `Buffer.Has` answer from an authoritative positive or negative base
  cache entry after checking overlays;
- preserves snapshot-version and same-key invalidation rules for concurrent
  durable fills, flush, discard, and unwind.

Acceptance for P1 on a fixed transaction-dense replay window:

- identical roots and transaction info with prefetch off/on;
- zero state-prefetch errors and no race failures;
- at least 50% lower Pebble `Has` samples attributable to latest-state misses;
- at least 5% higher throughput or 10% lower state-read CPU, with no more than
  2% regression on empty/light blocks.

### P2: Session-owned cross-block read ahead

The implementation follows the chain-tip mechanism introduced by Erigon
commit `c10b394286`: start state reads once decoded bodies are available and
overlap them with ordered execution. It also incorporates the stale-fill
hardening from `317c00f81f` and `d580222edf`: asynchronous warmup may populate
an absent cache row, but it may not replace a fresher value published by flush
or unwind.

The go-tron adaptation is deliberately narrower than Erigon's Ethereum target
set and matches TRON's current latest-domain model:

- `canonicalRangeExecutor` owns one `StateReadAhead` for an entire bulk-sync
  `InsertSession`; ordinary single-block insertion does not create workers;
- the downloader submits a batch immediately after protobuf body decode and
  before execution planning, signature recovery, and ordered state execution;
  queue accounting reuses the retained wire length instead of walking the
  decoded protobuf again on the scheduling thread;
- a default pool of `GOMAXPROCS/4`, capped at four workers, drains a bounded
  queue of 64 blocks and 16 MiB; saturation drops hints and never stalls or
  changes canonical execution;
- each block extracts and deduplicates witness, owner, destination, receiver,
  account, transparent sender/receiver, and smart-contract addresses in stable
  order; contract targets additionally warm metadata and content-addressed
  bytecode after decoding the latest account envelope;
- rawdb schema accessors construct every physical key. `Buffer.Prefetch`
  checks rewindable overlays first, probes the exact cache canonical reads use,
  and directly admits the durable positive or negative result because the
  decoded block proves near-future demand;
- direct admission bypasses ordinary two-hit scan protection but remains under
  the existing byte limit and CLOCK eviction policy. It is `put-if-absent` at
  the observed per-key invalidation generation, so a late warmup cannot replace
  a flush result;
- deduplication is per block rather than session-wide. A later block may warm
  the same logical account again after an intervening canonical mutation;
- executor reset/abort advances an epoch. Workers stop an in-flight old-fork
  block at its next target and discard every queued old-epoch block. Session
  close advances the epoch, drains workers, and then closes the commit scope;
- malformed hints and storage errors are counted but never returned to the
  consensus path. Metrics distinguish queue pressure, present/missing rows,
  errors, stale work, and canonical cache hits that actually consumed a
  prefetched row.

There is no CLI flag, compatibility mode, or persistent prefetch state. This is
part of the latest-only fresh-sync pipeline and can be removed or redesigned
without a datadir migration.

### P3: Parallel work that cannot change state order

Use idle cores before attempting parallel mutation:

- protobuf body decode and signature recovery (already partially parallel);
- static envelope validation whose result is rechecked at the serial boundary;
- deterministic prefetch-key extraction;
- receipt/log/trace encoding and recoverable derived-index ETL;
- cold segment reads and decompression.

Results are consumed in original block/transaction order. The serial path owns
the final accept/reject decision.

The first implemented slice follows Erigon's separation between pure
preprocessing and ordered execution without introducing another worker pool:

- the existing bounded batch signature pool is now a shared immutable-block
  preprocessing pool;
- each block job computes the transaction Merkle root and, when supported by
  the consensus engine, recovers the witness signer;
- each transaction job computes the txid/signers, decodes the typed contract,
  and measures the complete, result-free, and result-only protobuf sizes;
- immutable block-member transactions retain those size facts, while mutable
  standalone transactions recompute them on every call so txpool/builders keep
  their existing mutation semantics;
- owner extraction consumes the memoized typed contract instead of performing
  protobuf reflection in the ordered envelope path;
- state read ahead consumes the same typed owner accessor, so it cannot create
  a second decode/cache representation;
- Merkle comparison, size/expiration rejection, permission lookup, TAPOS,
  actuator validation, and every state mutation remain at their original
  serial boundaries and observe the memoized value or error.

The pool still gates on transaction volume and reserves one `GOMAXPROCS` slot
under its automatic policy. There is no new CLI flag, compatibility branch, or
persistent cache format. Metrics expose preprocessed blocks, transactions, and
contract-decode errors so production profiles can verify that the ordered path
actually consumes the work rather than duplicating it.

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

#### P4.1: Authoritative-journal shadow planner

The first implementation slice is observe-only. It copies Erigon's separation
between transaction-local writes and conflict resolution, but derives the write
identity from go-tron's existing authoritative undo journal after serial
execution. It therefore cannot change a block result while the safe subset is
being measured.

`StateDB.VisitTransactionWritesSince` exposes allocation-free logical write
cells for account, account creation, account-KV, KV generation, storage, code,
contract metadata, witness, self-destruct, transient storage, and dynamic
property mutations. Unknown future journal entries are explicit unsafe cells,
never silently ignored. DynamicProperties separately reports whether a nested
transaction snapshot changed a chain-global property before its undo entries
are compacted into the block snapshot.

The initial Transfer access model has two read/write account cells (owner and
recipient) plus read-only block/TAPOS/fork configuration. A serially successful
transfer is shadow-eligible only when:

- both addresses already exist and are distinct;
- the actual journal contains account writes for both addresses and no other
  state family or address;
- no dynamic property changed, which excludes public/free-bandwidth counters,
  fee-pool/burn counters, and other shared accounting;
- no account creation, blackhole/system account, multi-sign/memo fee, or
  unknown write was observed.

Non-transfer transactions and unsafe transfers close the current wave. Address
overlap between otherwise eligible transfers starts a new wave, preserving
canonical order. Metrics report candidates, eligible/unsafe transactions,
barriers, dependencies, wave count, transactions in width-greater-than-one
waves, and the last block's maximum width. The planner is enabled only on the
canonical, envelope-validating block path and never runs for RPC replay or
engine-less fixtures.

This slice deliberately does not run speculative workers. The next gate is to
execute one shadow wave against an immutable parent snapshot, compare its full
journal and TransactionInfo with the serial reference, discard it
unconditionally, and collect mismatch/overhead evidence before any publication
path exists.

#### P4.2: Generic versioned-access shadow

Mainnet measurement of P4.1 showed that the deliberately narrow Transfer
subset was too sparse to justify a worker pool: only 0.73% of transactions were
eligible and roughly 0.008% of all transactions appeared in a wave wider than
one. P4.2 therefore moves the measurement boundary to Erigon's real OCC model
before spending execution resources on shadow workers.

Canonical serial execution installs one transaction-scoped access recorder on
StateDB and DynamicProperties. Reads are deduplicated into typed logical cells:

- account envelope, witness, code, contract metadata, self-destruct state;
- persistent and transient storage keyed by address and slot;
- account-KV keyed by owner, domain, and logical key, plus a separate namespace
  generation cell so reset/recreation invalidates old-generation reads;
- integer, string, and hash DynamicProperties keyed by property name.

State writes still come from the authoritative undo journal after successful
execution. Dynamic-property writes are captured at the typed setter because a
nested transaction snapshot is compacted into its parent before the processor
can inspect it. Getters route through recording helpers only while capture is
installed; disabled RPC/simulation paths pay a nil branch and retain existing
values and ownership behavior.

Prefix/range reads are conservatively unsupported until range-version cells
exist: recording only the rows returned by an iterator would miss a predecessor
inserting a previously absent key. Unknown future journal entries follow the
same rule. Neither case can enter speculative publication.

After each serial transaction, a block-local version map checks every captured
read against the last preceding writer. A match means the result would be valid
on its first block-start speculative attempt; a mismatch is classified by
account, storage, account-KV, dynamic-property, or other path. Blind write/write
overlap is reported separately but does not invalidate the result: Erigon can
publish such writes safely in original transaction order without re-execution.
Metrics also split first-pass validity between VM, Transfer, and other contract
families and report maximum dependency distance.

This remains observe-only. It never copies, reorders, or publishes a result.
The 64-disjoint-transfer microbenchmark measured about 1 microsecond per
transaction and four additional allocations per block-sized batch on the local
development machine. Live sync profiling is the deployment gate before the
next slice introduces discard-only workers and full journal/TransactionInfo
comparison.

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
