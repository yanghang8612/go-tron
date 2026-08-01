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

The recoverable TxLookup stage follows the same sequential-stage principle.
Its original catch-up pass reread every retained block through a strict helper
that issued a freezer probe followed by separate Pebble `Has` and `Get` point
reads. A production deployment caught one 4,096-block pass holding the sync
drain for more than four minutes while rebuilding transaction hashes. The
stage now reads a contiguous freezer range followed by one ordered `b-` prefix
iterator for the hot suffix, validates every expected number/protobuf, and
feeds the existing key-sorted ETL collector without changing its watermark
semantics. TxLookup bounds individual in-memory sorts at 8 MiB, and the ETL
collector uses its existing key-plus-sequence total order with ordinary
`sort.Slice`; stable sort added cost without changing duplicate collapse.
Stop requests interrupt between block rows and during k-way ETL merge. Partial
or interrupted work leaves the prior watermark authoritative and is safe to
rerun. Metrics separate ancient/hot rows, transactions, passes, interruptions,
and elapsed time under `sync/stage/tx_lookup/`.

The first sequential-reader production window processed 1,099 hot blocks and
99,132 transactions in 1.266 seconds (about 78,300 transactions/second), with
zero stage interruptions or errors. The prior point-read/stable-sort path had
been observed inside one 4,096-block pass for more than four minutes.

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

#### P4.3: Ordered settlement-delta normalization

The first P4.2 mainnet sample showed that generic OCC alone is not a useful
worker boundary: shared fee settlement made only 3.86% of transactions valid
on their block-start attempt. Account and DynamicProperties conflicts appeared
in 98.8% and 96.5% of conflicted transactions respectively. Starting workers
at that boundary would spend nearly all execution time replaying.

P4.3 models the Erigon-style separation between transaction execution and
ordered result application without changing canonical execution. Protocol
helpers label only their internal read-modify-write as a commutative settlement
access while continuing to execute the original serial mutation and journal:

- TRX fees credited to the genesis blackhole account;
- `burn_trx_amount` and `transaction_fee_pool`;
- cumulative `total_transaction_cost`, `total_create_account_cost`, and
  `total_create_witness_cost`.

The generic access mode retains ordinary read/write bits independently from
commutative read/write bits. If an actuator performs a real read of the same
cell elsewhere in the transaction, both bits remain present and normalized
validation still rejects a stale worker result. Ordinary transfers to the
blackhole address are not inferred from the destination address and remain
normal dependencies.

The following state is deliberately excluded because it is not a blind delta
at the transaction boundary: public bandwidth usage/time, account resource
windows, resource weights, transaction-fee-pool block reward subtraction, and
shielded-pool value (shielded validation reads the current value for bounds).
The shielded TRC10 fee credit also stays conservative until account-KV
generation and exact-row settlement are modelled together.

The raw P4.2 validator remains unchanged. A second normalized validator ignores
only a commutative helper's internal read-version mismatch, installs every
write version in canonical order, and reports raw versus normalized first-pass
validity overall and for VM/Transfer/other classes. Separate counters identify
which settlement family caused a raw conflict and how many transactions become
publishable under ordered delta application. This is still observe-only: no
delta is extracted, no worker runs, and no result is published.

On the local 64-transaction shared-settlement benchmark, canonical serial
execution remained allocation-equivalent. Enabling raw plus normalized shadow
validation added four allocations per batch and about 0.84 microseconds per
transaction. Production normalized-validity and CPU evidence is the gate for
extracting an explicit settlement-delta result and starting discard-only
workers.

The first fixed production sample at historical mainnet height approximately
4.70 million improved first-pass validity from 3.519% raw to 5.444% after
settlement normalization. That 54.7% relative gain confirmed the model, but the
absolute worker yield remained far too low: Account was still represented as
one very wide protobuf dependency.

#### P4.4: Erigon-style typed Account paths

Erigon does not hash every property of an address into one account version.
Its local `execution/state/versionmap.go` uses typed `AddressEntry` paths, and
`execution/state/versionedio.go` carries corresponding typed `ReadSet` and
`WriteSet` maps. P4.4 adopts that mechanism for TRON while retaining the wider
Account protobuf and split account-KV storage required by java-tron.

Hot logical Account reads and scalar mutations are now captured independently:

- existence and account type;
- balance, allowance, and latest-withdraw time;
- bandwidth usage/timestamps/free usage and the bandwidth recovery window;
- the compact-envelope frozen-resource fields.

Full-account APIs remain a hierarchy barrier. Each address has a full-write
version, an any-field-write version, and per-field versions. A typed field read
checks the latest full or matching-field writer; a full-account read checks any
preceding field writer. Account creation, deletion, and unclassified mutations
therefore still invalidate every typed field. The original whole-account
validator also remains live as the raw baseline.

Temporal history deliberately journals a scalar Account mutation with complete
pre-image bytes. Inline field-write coverage lets the observer recognize that
physical `accountChange` as an already classified scalar mutation. A real full
mutation records an explicit full write; a journaled Account write with no
field coverage falls back to a full barrier. This keeps measurement
conservative if a new setter is added without typed instrumentation.

TRON's split rows remain exact account-KV cells rather than Account fields.
Permission validation records the requested permission row even when the
decoded permission is cached. V1 frozen-bandwidth, `AccountResource`, and V2
frozen-resource point caches likewise replay their physical logical reads at
every transaction boundary. Prefix scans remain unsupported. These rules avoid
the false independence that would occur if a block-scoped object cache hid a
worker's transaction-scoped dependency.

The first hot call-site conversion removes whole-account reads from transaction
permission presence checks, TRX/TRC10 recipient-type validation, balance and
bandwidth scalar accounting, contract code/storage existence checks, and VM
energy-resource accounting. Freeze/unfreeze and other paths that inspect
multiple arbitrary Account fields retain the full barrier until separately
audited.

Metrics publish raw, settlement-normalized, and typed-plus-settlement
first-pass validity overall and by VM/Transfer/other class. Typed Account
conflicts are split into coarse, existence, type, balance, allowance,
bandwidth, and frozen-resource paths. This phase is still observe-only: the
serial StateDB remains authoritative and no worker result is executed or
published. Production typed validity and observer CPU determine whether the
next safe step is explicit settlement deltas or discard-only worker execution.

The first 32-second production sample processed 126,088 transactions at
historical height 4.85 million. Raw, settlement-normalized, and typed validity
were 4.0829%, 6.4360%, and 6.4590% respectively. The typed gain was only 29
transactions, while 112,008 of 117,809 typed-conflict transactions still
included a coarse Account dependency. CPU evidence identified
`ContractRuntime` as the sampled caller of the remaining hot
`getStateObject` path: contract validation, call setup, and energy settlement
were consuming the full Account version even though they need only existence
and contract metadata. The next P4.4 increment converts that shared VM path,
plus runtime-state/ABI point readers, to typed existence and their existing
metadata/account-KV cells before reassessing the irreducible per-owner
balance/resource conflicts.

After that conversion, a second 31-second sample processed 134,000
transactions. Coarse conflicts fell from 112,008 in the first sample to 5,094,
and VM typed validity improved from 2.7224% normalized to 3.0845%. Overall typed
validity nevertheless reached only 6.4604% because balance and bandwidth paths
still conflicted in 120,688 and 121,108 transactions. Those are real ordered
dependencies from repeated transactions by the same owner, not protobuf field
aliasing, so further Account splitting is not justified by the evidence.

#### P4.5: Explicit previous-sender dependencies

Erigon does not dispatch every transaction from the block-start snapshot. In
local `execution/stagedsync/exec3_parallel.go`, `processRequest` tracks
`prevSenderTx` and adds an execution dependency between consecutive tasks from
the same sender when no complete access list is available. Its version map and
transaction incarnations then handle dynamic cross-sender conflicts and
re-execution. This serializes an account's nonce/balance chain while leaving
different senders available to workers.

The go-tron observer now models that pre-execution rule on top of P4.4 typed
versions. It uses the already memoized contract owner, identifies the latest
writer for every stale typed read, and treats the conflict as resolved only
when that writer belongs to the same valid TRON owner. The direct previous-owner
edge makes all older writes in that sender chain settled before dispatch. A
latest writer from any different owner remains a conflict, regardless of state
family. Shielded or malformed owner identities receive no inferred edge.

Metrics expose sender-serialized first-pass validity overall and for
VM/Transfer/other classes, the number of transactions with an explicit sender
predecessor, typed conflicts resolved by that dependency, remaining conflicts,
and the last block's maximum sender-chain depth. This remains observe-only; no
task is dispatched and canonical execution is unchanged. A useful production
yield with bounded sender critical chains is the gate for implementing the
first discard-only worker scheduler.

The first production sender-dependency sample covered 140,232 transactions in
32 seconds. Typed first-pass validity rose from 5.3183% to 17.3420% after the
explicit sender edge, resolving 16,861 transactions; 68,003 transactions had a
same-sender predecessor. Transfer reached 77.3361%, but the dominant VM class
reached only 13.0235%, leaving 115,786 cross-sender conflicts. The last sampled
block had a sender-chain depth of 27. Before worker implementation, the observer
therefore splits this residual set by account field, storage, account-KV,
DynamicProperties, and other families; pre-sender balance/resource counts
cannot identify the cross-sender VM bottleneck.

Residual-family measurement confirmed that no single remaining typed cell can
be normalized away: across 132,510 transactions, sender-serialized conflicts
overlapped Account in 67.35%, DynamicProperties in 58.93%, account-KV in
43.56%, and storage in 24.77%. Shared balance and bandwidth dependencies alone
remained in 88,632 and 57,500 transactions. This is the dynamic dependency set
that an Erigon-style scheduler must wait for or retry.

#### P4.6: Dependency-ready DAG measurement

Block-start and sender-only validity are pessimistic bounds for Erigon MVCC.
Once a worker discovers a dependency or an incarnation validates, a later task
can run against the now-settled version without waiting for every earlier
transaction. P4.6 therefore builds an observe-only DAG from the actual typed
versions seen during canonical execution.

For each ordinary typed read, the observer finds the latest preceding writer
and places the transaction in the earliest wave after that writer's wave.
Audited settlement-delta reads do not form edges. Blind writes remain eligible
for the same wave because ordered publication resolves them. Unsupported range
reads and unknown journal entries form serial barrier waves; later known tasks
may become parallel only after the barrier.

Per-block metrics report the number of dependency waves, maximum wave width,
and transactions that belong to a wave wider than one. These are an attainable
parallelism bound using already discovered access sets, not a claim that the
first incarnation knows the future DAG. Sufficient width is the gate for a
discard-only scheduler that learns dependencies via estimates/retries; narrow
width means the historical workload itself is serial and worker construction
would add overhead without useful execution capacity.

The first production DAG sample covered 175,106 transactions in 1,904 blocks.
It formed 143,881 waves (1.217 transactions per wave), and 51,211 transactions
(29.25%) belonged to a wave wider than one. Random last-block sampling observed
a maximum width of 7, with 74 parallel-wave transactions in a 235-transaction
block. This is enough concurrency to reject a fully serial model, but the
transaction-count-only four-worker bound is only about 1.28x and treats a
simple transfer as equal to a VM invocation.

#### P4.7: Cost-weighted dependency waves

Before allocating real workers, time canonical transaction execution between
access-recorder installation and observation. Aggregate the measured durations
three ways: their serial sum; the sum of the longest transaction in each DAG
wave (unlimited workers); and a canonical-order greedy four-worker schedule
inside each wave. Both parallel estimates retain a conservative barrier between
waves, so a later dependency-aware scheduler may overlap unrelated adjacent
waves but cannot obtain less concurrency than this model because of the DAG.

The observer exports cumulative and last-block nanoseconds for all three
models. Production decides the next implementation step from their ratios,
not raw wave width: proceed to discard-only workers only when cost-weighted
four-worker gain justifies worker snapshots, retries, journal comparison, and
ordered publication. The timing remains observe-only and never affects
consensus state or transaction results.

The production cost sample covered 99,021 transactions over 926 observed
blocks in 20 seconds (4,951 transactions/s). Although 33.36% of transactions
belonged to a parallel wave, measured serial cost was 12.531 seconds versus
10.626 seconds for the four-worker wave schedule: only 1.179x. Unlimited
workers reduced it to 10.560 seconds (1.187x). Worker count is therefore not
the limiting factor under global wave barriers; direct dependency chains are.

#### P4.8: Erigon-style direct-edge ready queue

Wave barriers unnecessarily hold an independent transaction in level N+1
until every transaction in level N completes. Preserve the exact preceding
writer for every ordinary typed read, plus Erigon's explicit previous-sender
edge. Unknown/range transactions depend on every preceding transaction and
remain a barrier for every successor, retaining conservative correctness.

An observe-only event simulation dispatches the lowest canonical-index ready
transaction to four workers and releases each dependent when all its direct
predecessors complete. It also computes the unlimited-worker cost-weighted
critical path. These metrics distinguish scheduling loss from inherent state
dependency: the ready queue is the implementation model for discard-only
workers, while its critical path is the ceiling no worker count can exceed.
No simulated result is published.

The first production ready-queue sample covered 95,507 transactions in 826
observed blocks. Canonical execution cost 13.480 seconds. The global-wave
four-worker model needed 11.672 seconds (1.155x), while direct-edge four-worker
execution needed 10.017 seconds (1.346x). Its unlimited-worker critical path
was 10.010 seconds (1.347x), demonstrating that four workers already expose
nearly all available parallelism and that removing the wave barrier crosses
the 1.25x implementation gate.

#### P4.9: Sampled discard-only execution

Run the first real workers with no publication capability. Every 64th canonical
block captures one block-start StateDB copy and an independent DynamicProperties
copy. After serial execution discovers the exact graph, up to four workers
replay supported zero-indegree transactions from that immutable start state.

StateDB isolation alone is insufficient because actuators can write Context.DB
directly. Each worker therefore receives a read-through KV overlay: parent
reads remain visible, but puts and deletes are copied into worker-local maps
and discarded at the transaction boundary. Each worker owns its StateDB,
DynamicProperties, fork cache, result scratch, VM arenas, and balance trace.
State and dynamic-property snapshots are reverted after every task.

The comparison boundary is the complete TransactionInfo protobuf, including
receipt, result, logs, internal transactions, fees, return data, and diagnostic
resource fields. Metrics report sampled blocks, candidates, executions,
matches, mismatches, errors, state-copy time, and parallel execution wall time.
Canonical state and receipts remain authoritative regardless of comparison
outcome. Journal value comparison and non-zero-indegree predecessor materialization
remain required before any result can be published.

The first live worker sample covered 172 zero-indegree transactions in 30
sampled blocks. Only 57 TransactionInfo values matched; 111 mismatched and four
worker executions returned an error. Isolation overhead was acceptable at
about 5.0 ms of state copying and 0.64 ms of parallel execution per sampled
block (roughly 0.09 ms amortized per canonical block), but correctness is not.
Publication remains disabled. Follow-up metrics split mismatches by VM,
Transfer, and other contracts and by receipt, fee, contract result, logs,
internal transactions, and remaining fields before changing eligibility or
snapshot semantics.

Field-level production diagnostics then isolated 127 of 147 mismatches to
`TransactionInfo.contract_address`; transaction status, error message, and
identity all matched. This exposed an existing canonical receipt ownership bug:
the transaction-info slot borrowed ContractAddress from the block-reused
`applyTransactionScratch.result` embedded array, so the next VM transaction
overwrote earlier receipts. Each slot now owns and copies its 21-byte contract
address. Transfer workers remained byte-identical throughout this sample.

The remaining receipt-energy mismatches were not VM divergence. `StateDB.Copy`
omitted the active async-commit latest readers, so workers loaded an older
physical AccountResource row while canonical execution saw the unflushed
pending latest value. Copies now retain the same read-only latest views and
code store. The first post-fix production sample compared 262/262 complete
TransactionInfo values exactly, with every receipt subfield and execution class
at zero mismatches.

#### P4.10: Typed transaction write sets

Execution equivalence is necessary but not sufficient for publication. Extract
an Erigon-style typed WriteSet after every sampled canonical/worker execution
from the authoritative StateDB journal plus inline access recorder. Values own
their bytes and preserve presence, so delete and present-empty remain distinct.
Account scalars use field-level post-images instead of serializing the complete
Account; otherwise an independent worker would spuriously include unrelated
fields changed by a predecessor. Audited settlement cells carry transaction-
local signed deltas rather than their serial absolute baselines.

The first deployment exposed exactly that coarse-account representation bug:
only 24/45 worker write sets matched while all 45 TransactionInfo values did.
After removing the coarse journal post-image when typed scalar coverage is
complete, production reached 1,417/1,417 exact TransactionInfo matches and
1,417/1,417 exact state/DynamicProperties WriteSet matches across 245 sampled
blocks, with zero execution, comparison, or capture errors.

Complete the storage surface by wrapping the remaining direct Context.DB calls
with an exact-key raw-KV recorder. Reads join the same block-local version map;
successful puts/deletes contribute owned post-images to the same WriteSet. The
wrapper preserves blockbuffer cached reads, missing-key classification, and
ancient-aware block-hash lookup. Discard workers record the same accesses in
their private write overlay. Raw read/write-cell and conflict metrics
distinguish an exercised immutable direct-DB path from an unobserved path. This
remains observe-only until production proves that adding raw keys preserves the
100% result/WriteSet sample and ordered publication can apply every typed
family safely.

The first raw-key production sample covered 413,166 transactions and recorded
416,127 raw read cells (1.007 per transaction), with zero raw writes and zero
raw conflicts. This confirms the live direct-DB surface is effectively the
immutable TAPOS/block-index lookup rather than hidden mutable actuator state.
Across the same deployment, 385/385 sampled workers matched both complete
TransactionInfo and the combined typed/raw WriteSet. Repeated physical keys are
interned once per block and reused across transaction recorder resets, avoiding
one string allocation per TAPOS validation while preserving owned-key lifetime.

#### P4.11: Preflighted ordered write application

Introduce a publication primitive without routing canonical execution through
it yet. The applier first validates the complete WriteSet and accepts only
unambiguous typed families: account scalar fields, witness capsules, storage,
code, contract metadata, exact account-KV cells, transient storage,
DynamicProperties, and raw KV. Ordinary post-images replace the logical cell;
audited balance/dynamic settlement values are added as signed deltas to the
publisher's current ordered baseline. Presence bits drive storage/account-KV,
metadata, and raw deletion rather than collapsing deletion into an empty value.

Full-account replacement/deletion, account-KV generation reset, self-destruct,
unknown account fields, and malformed values fail preflight before any
mutation. The state-aware phase also rejects typed writes whose owner account
is absent unless the same WriteSet carries a validated fresh-account creation.
Applying disables the execution access recorder temporarily so publication
does not masquerade as another transaction. Unit fixtures cover
absolute field publication, commutative deltas over a newer baseline, storage,
account-KV/metadata/raw deletion, and preflight atomicity. Sampled workers now
report how many exact WriteSets are eligible for this narrow applier; actual
production publication remains disabled.

The first eligibility sample accepted 168 of 202 exact worker WriteSets
(83.2%) while all 202 TransactionInfo/WriteSet comparisons remained exact.
Overlapping rejection counters split the remaining set into full-account,
account-KV generation, self-destruct, unsupported account-field, and other
families. Production measurements choose the next schema extension rather than
broadening the applier speculatively.

A larger production sample accepted 689 of 738 exact worker WriteSets (93.4%).
All 49 rejected sets contained a full-account post-image; generation reset,
self-destruct, unsupported account-field, other rejection, and write-capture
error counters remained zero. Account creation is therefore the measured next
schema gap, rather than a hidden unsupported mutation family.

#### P4.12: Isolated ordered-write reapplication

For every sampled exact WriteSet that passes preflight, rewind the discard
worker to its block-start state and apply the set through the ordered
publication primitive in a second isolated transaction. Attach a fresh access
recorder to this verification pass, finalize at the normal transaction
boundary, extract the mutations produced by the applier, and compare that
second WriteSet exactly with the worker's original set. Revert both StateDB and
DynamicProperties afterward; raw writes remain inside the worker's private
overlay.

Commutative balance and DynamicProperties cells retain their signed-delta
identity during verification instead of being observed as absolute
post-images. Separate match, mismatch, and apply-error counters make schema or
implementation gaps visible before canonical publication is enabled. This
stage still performs no canonical mutation and cannot affect consensus output.

The first production pass kept 132/132 worker TransactionInfo and WriteSet
comparisons exact, but only 114 of 116 eligible sets reproduced their typed
WriteSet after application; two mismatched without an apply error. Canonical
publication therefore remains disabled. Audit found that applying a typed
AccountType field through CreateAccount incorrectly emitted a coarse Account
journal entry; a dedicated typed setter now preserves field identity. Mismatch
diagnostics also split missing/extra/presence/commutative/value differences by
cell family so the next production sample can distinguish this fix from any
remaining publication defect.

The diagnostic deployment then reproduced five mismatches among 309 eligible
sets. Every mismatch was a missing AccountKV key; extra-key, presence, value,
commutative, and all other cell-family counters stayed zero. This is the
expected shape of a transaction that writes a cell and returns it to its
block-start value: the worker WriteSet preserves write intent, while applying
only the final post-image correctly takes the setter's no-op path and emits no
journal entry. Verification now pre-registers each non-commutative input key
with its recorder before applying it, then captures the actual final logical
value. This preserves setter no-op optimization without masking an incorrect
post-state.

After that normalization, the next production window compared 397/397
TransactionInfo values and 397/397 original worker WriteSets exactly. All
350/350 eligible sets reproduced their complete WriteSet through the ordered
applier, with zero mismatch, apply error, or missing-key diagnostics; 47 sets
remained outside the schema because they carried a full-account post-image.

#### P4.13: Fresh-account publication

Close the measured account-creation gap without accepting ambiguous full-row
replacement. A full-account post-image is eligible only when it decodes as the
current flat-v3 envelope, its embedded address matches the logical key, its KV
root is flat, its incarnation generation is zero, and the ordered baseline has
no account at that address. A non-zero code hash additionally requires a code
post-image in the same WriteSet with an exactly matching hash.

The applier installs fresh accounts before their typed account-field,
AccountKV, code, contract-metadata, witness, or storage cells, removing Go map
order from creation semantics. Existing-account replacement, deletion, and
post-SELFDESTRUCT reincarnation remain serial fallbacks. This follows Erigon's
incarnation discipline: creation of generation zero is distinct from mutating
or reviving an existing account.

The first post-deployment window accepted all 305/305 sampled WriteSets and
reapplied all 305 exactly, with zero unsupported, mismatch, or apply-error
counts. TransactionInfo and original worker WriteSet comparisons were also
305/305 exact. The previously observed full-account rejection set therefore
consisted of fresh creations in this window, while the state-aware guard still
keeps replacement and reincarnation out of publication.

#### P4.14: Shared ordered shadow publisher

Per-transaction block-start reapplication proves each publication primitive,
but not their composition. Retain successful sampled worker WriteSets until
the block's workers finish, sort them by original transaction index, and apply
them cumulatively to one isolated block-start publisher. Each application is
recorded and its resulting WriteSet compared exactly before the next task sees
that newer baseline. Raw overlay writes are retained across tasks and all
commutative settlement values are applied as deltas.

Only zero-indegree tasks enter this initial publisher, so ordinary cells are
disjoint by the observed dependency graph while audited commutative cells may
overlap. Match, mismatch, and error counters are separate from the individual
worker counters. The publisher and raw overlay are discarded after the sampled
block; canonical state and the backing database remain untouched.

The first shared-publisher production window reached 659/659 exact
TransactionInfo, original WriteSet, individual reapplication, and cumulative
ordered-publication comparisons. All sampled sets were eligible; unsupported,
mismatch, and apply-error counters remained zero.

#### P4.15: Pre-serial Transfer execution

Move the first speculative work ahead of canonical execution without yet
publishing it. On sampled blocks, select Transfer transactions statically and
run them over four independent block-start StateDB/DynamicProperties copies
before entering the serial loop. Retain owned TransactionInfo and typed/raw
WriteSet results; each worker reverts its copy after the task, so one worker can
process multiple transfers without inheriting another speculative result.

After canonical execution builds the authoritative version graph, admit only
supported zero-indegree transfers whose canonical WriteSet capture completed.
Compare their full TransactionInfo and WriteSet, require the individual
publication check, then cumulatively apply the retained pre-execution results
to a fresh isolated ordered publisher. Separate metrics expose total transfers,
zero-indegree candidates, validated results, ordered results, errors, and
pre-execution wall time. This establishes the exact result carrier and
pre-dispatch boundary needed for the next step; canonical execution remains
serial and is still the sole source of persisted state.

The first production window pre-executed 307 transfers and admitted 134
zero-indegree candidates. All 134 matched canonical TransactionInfo, typed/raw
WriteSet, individual reapplication, and cumulative ordered publication; every
mismatch, unsupported, and error counter remained zero. Total sampled
pre-execution wall time was 315 ms, or about 1.03 ms per transfer, while the
observer ran only once per 64 blocks.

#### P4.16: Frozen worker read-version validation

Retain an owned read set with every pre-execution result before its reusable
recorder advances to another task. Immediately before canonical execution
reaches that transaction index, validate ordinary reads against the typed
account/path version hierarchy containing exactly the preceding transactions.
Commutative reads are exempt from ordinary version conflicts only when the
same result carries an audited commutative WriteSet delta for that path.

The publishability decision also preserves Erigon's previous-sender edge and
go-tron's conservative unknown/range-read barrier. Compare this independent
decision with the already captured dependency DAG and expose separate counters
for read conflicts, invalid deltas, unsupported accesses, sender conflicts,
barrier conflicts, and DAG agreement. This is still observe-only: it proves the
result-carried validation boundary before the same predicate is allowed to
select canonical ordered publication or serial fallback.

The first implementation attempted to reconstruct an intermediate decision
from the block-final latest-writer map. Production caught two false accepts in
105 candidates because a later writer can replace the earlier writer that made
an intermediate read stale. Moving validation to the real publication
boundary fixed the information loss. The corrected window reached 505/505 DAG
agreements, including 131 publishable results, with zero invalid deltas,
unsupported accesses, ordered mismatches, or execution errors.

#### P4.17: Balance-history result carrier

State and TransactionInfo equality are not sufficient when history capture is
enabled. A typed WriteSet contains each account's final balance but loses the
ordered sequence of balance operations that java-tron's BlockBalanceTrace API
persists. Initialize an isolated trace recorder on each sampled worker, retain
an owned copy of the completed transaction trace, and compare its identifiers,
status, operation order, addresses, and signed amounts with canonical history.

Transactions with no balance operations carry a nil trace; identifier-aware
capture prevents them from accidentally inheriting the preceding transaction's
trace. A pre-executed result is not eligible for the next canonical stage when
its balance trace differs, even if its post-state and receipt match.

The first production window reached 202/202 exact BalanceTrace comparisons
with zero trace mismatches. Read-version/DAG disagreement, ordered-publication
mismatch, and worker execution errors also remained zero in the same window.

#### P4.18: Opt-in canonical Transfer publication

Add `--exec.parallel-transfers` (or `GTRON_EXEC_PARALLEL_TRANSFERS=1`), disabled
by default. When enabled on the canonical block-import path, pre-execute plain
Transfer contracts from block start on four workers. Immediately before each
transaction index, publish only a result whose worker execution, typed/raw
WriteSet reapplication, zero-energy receipt, frozen read versions,
previous-sender edge, unknown-read barrier, and current-state applier preflight
all pass. Apply typed post-images and commutative deltas to canonical StateDB in
original block order, restore the worker read set to the live version tracker,
and append the owned BalanceTrace. Any unavailable, stale, or newly
inapplicable result runs through the existing serial path.

The option also exposes pre-executed, candidate, published, conflict fallback,
unavailable fallback, preflight fallback, error, and wall-time counters under
`core/parallel_transfer/`. It does not affect public ProcessBlock/tracing paths
or nodes that omit the flag. Production deployment remains gated on the P4.17
BalanceTrace sample; merging this stage alone therefore leaves the service on
the proven serial canonical path.

The current typed graph deliberately treats `public_net_usage` as an ordinary
read/write path. Multiple free-bandwidth transfers in one block consequently
fall back after the first publication even when their accounts are disjoint.
The next breadth step is to model the within-block recovered public-bandwidth
baseline plus ordered usage increments without weakening the limit check.

The first enabled production window reached 2,995,135 pre-executed transfers,
761,571 candidates, and 761,571 publications, with zero publication errors or
preflight failures. The remaining 2,232,926 conflict fallbacks made the shared
public-bandwidth cells the next measured breadth bottleneck. Average retained
pre-execution time was about 0.91 ms per transfer and ordered publication about
40.9 microseconds per published transfer.

#### P4.19: Conditional public-bandwidth reservation

Model java-tron's `useFreeNet` decision explicitly instead of labelling the
global public usage counter as an unconditional commutative delta. A worker
that successfully consumes free bandwidth retains the starting usage/time,
recovered usage, resource time, transaction byte delta, and public limit. The
carrier is accepted only when its retained post-images reproduce the same
recovery and limit decision.

At the original transaction position, the Erigon-style ordered publisher
ignores predecessor versions only for `public_net_usage` and
`public_net_time` covered by a valid carrier. It recovers the current canonical
usage once from the state left by preceding transactions, repeats java-tron's
`bytes <= limit - recoveredUsage` check, and temporarily normalizes the worker
post-image to `recoveredUsage + bytes`. If the limit changed or preceding
transactions exhausted it, the transaction falls back to the authoritative
serial path, which can charge bandwidth exactly as java-tron would. The
timestamp remains an idempotent block-scoped write.

`core/parallel_transfer/public_net/rebased` counts the subset whose ordered
baseline differs from the worker's block-start baseline. It is the direct
uplift attributable to this normalization rather than a workload-mix estimate.

Serial/parallel fixtures cover two disjoint free-bandwidth transfers with a
non-zero decaying starting usage: both publish and produce identical
TransactionInfo, account balances, dynamic usage/time, and state root. A
second fixture constrains the public limit so both block-start workers are
individually eligible but only the first ordered reservation fits; the second
falls back, including fee settlement, and again matches the serial root and
results. Production acceptance requires zero errors and preflight failures,
zero state/result divergence, and a material reduction in conflict fallbacks.

The first enabled reservation window covered 143,627 pre-executed transfers
over 27,702 blocks and published 45,158 (31.44%); 24,182 publications carried
a public-bandwidth reservation. The preceding binary's longer cumulative
window published 25.22%, so this is a positive directional result but not a
fixed-workload comparison. Public-limit fallbacks, publication/preflight
errors, and sampled info/write/apply/ordered/balance mismatches all remained
zero. The `rebased` counter added for the next window isolates the exact
incremental publications from workload mix.

That counter's first window covered 47,877 pre-executed transfers and 14,008
publications. Of those, 6,468 (46.2%) used an ordered baseline different from
block start and therefore would have conflicted on the old public usage/time
paths. Holding this exact workload constant, publication increased from about
7,540 to 14,008 (85.8%); public-limit, preflight, and publication errors stayed
zero.

#### P4.20: Sender-chain state forwarding

Erigon's previous-sender relation is a scheduling edge, not a reason to reject
every later transaction from that sender. The next sampled executor therefore
groups plain Transfers by their immediate valid owner predecessor and assigns
whole sender chains to workers. A worker executes each chain serially on one
private StateDB/DynamicProperties copy, installs each verified typed WriteSet
as the input state for the next member, and reverts the entire chain before
accepting another job. A non-Transfer transaction from the same owner breaks
the narrow chain until that actuator family can produce an equivalent carrier.

Every ordinary worker read now carries either block-start provenance or the
exact earlier transaction index whose forwarded value it consumed. At the
canonical transaction boundary, the latest typed writer must equal that
recorded source. An intervening cross-sender balance, resource, storage, or
dynamic-property write therefore rejects the result and preserves serial
replay. The direct previous-sender version is checked independently, so a
missing or replaced chain member also rejects publication.

The initial increment is observe-only and runs on the existing 1/64 sampled
blocks. It compares complete TransactionInfo, typed WriteSet, and BalanceTrace
against canonical execution. Valid conditional public-bandwidth reservations
exclude only `public_net_usage` and `public_net_time` from the raw WriteSet
comparison because their canonical values are intentionally rebased across all
sender chains; their admission inputs remain checked by P4.19. Raw-KV writes
are not forwarded in this first narrow carrier. Metrics under
`core/versioned_shadow/sender_chain/` expose groups, executed and forwarded
results, version-valid candidates, fully validated forwarded results, conflict
classes, mismatches, errors, and wall time.

The first production gate covered 65 sampled blocks and 217 Transfers. All 129
version-valid results matched; 59 results consumed same-sender forwarded state
and 20 of those passed exact version, TransactionInfo, WriteSet, and
BalanceTrace validation. Read-version conflicts rejected 88 results and the
explicit sender check rejected seven; info/write/trace mismatches and worker
errors remained zero.

After this gate, `--exec.parallel-transfers` selects sender-chain workers on
non-sampled blocks. A dependent result is publishable only if its immediate
speculative predecessor was itself published; a predecessor that fell back to
serial execution invalidates the retained descendants even when their numeric
writer index still matches. Every 64th block continues to use the independent
block-start executor plus serial fallbacks and runs the sender-chain observer,
preserving a continuous production canary instead of comparing published
results with themselves. Dedicated `core/parallel_transfer/sender_chain/`
counters report pre-executed dependents, version-valid candidates, actual
publications, and predecessor-fallback cascades so uplift is measured directly.

#### P4.21: Sender-chain incarnation and retry

Erigon does not permanently discard a sender suffix when an optimistic result
conflicts. Its scheduler increments that transaction's incarnation, puts it on
the priority retry queue, and validates the replacement against the versioned
state that has settled since the previous attempt. The first go-tron bridge
implements the same lifecycle as a sampled, observe-only executor: immediately
before canonical transaction `N`, a stale sender result is rebuilt from a deep
copy of the real canonical prefix `0..N-1`, then the remaining same-sender
suffix is executed serially and retains exact read-writer provenance.

A replacement result is frozen until its own canonical boundary. An
intervening cross-sender writer invalidates it and, when dependent sender work
remains, creates another incarnation from the newer prefix. Results executed
directly on the immediately preceding settled prefix may accept an unknown-read
barrier because no mutation can exist between their snapshot and validation;
later suffix members still require complete exact-key version coverage. Every
selected incarnation is compared with canonical TransactionInfo, typed
WriteSet, and BalanceTrace, and no retry state is published in this phase.

The synchronous deep copy is deliberate instrumentation, not the final
scheduler. Metrics under `core/versioned_shadow/sender_retry/` report attempts,
executions, recovered candidates, full validation, mismatches/errors, copy
nanoseconds, and execution nanoseconds. The production gate determines whether
recovered work justifies moving the mechanism onto an asynchronous
incarnation-priority queue backed by shared versioned state, avoiding a full
StateDB copy for each retry. To keep this diagnostic from dominating sampled
blocks, one block is capped at eight copies and 64 retry executions;
`budget_skipped` records work omitted by that guard.

The first production window covered 12,053 imported blocks and 170 sampled
sender-chain blocks. The source executor produced 444 valid candidates from
1,012 results. Seventeen retry attempts executed 100 incarnations and recovered
89 additional results that all matched canonical TransactionInfo, WriteSet,
and BalanceTrace; all mismatch and execution-error counters remained zero.
StateDB copies cost 31.52 ms in total (1.85 ms/attempt), while retry execution
cost 6.74 ms (67 microseconds/result). The recovered set would increase the
sampled valid population by 20%, but 339 budget skips showed that copy-per-
incarnation cannot scale to long sender chains.

#### P4.22: Reusable settled-prefix runner

The next observer replaces copy-per-incarnation with one lazy canonical-prefix
copy per retrying block. After the first retry, the runner advances that private
state in canonical order using the exact typed and raw TransactionWriteSets
already captured by the version map. DynamicProperties are refreshed from the
live boundary to include block-scoped mutations outside a transaction carrier.
Each speculative suffix runs inside one outer StateDB/DynamicProperties journal
snapshot and is wholly reverted, so the advanced prefix remains reusable by
the next incarnation.

If an intervening WriteSet contains a state family not yet supported by the
narrow ordered applier, the runner discards the partial view and refreshes once
from the real canonical prefix. Results carry an explicit monotonically
increasing incarnation and only the newest incarnation can be selected.
`sender_retry/prefix/` metrics distinguish full refreshes, reuse events,
transactions advanced, and advance time. This remains a 1/64 synchronous
canary; the production scheduler still requires asynchronous workers and a
retry-priority queue before canonical publication.

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
