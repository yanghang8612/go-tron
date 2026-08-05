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

The first reuse window covered 7,466 blocks and 113 sampled sender-chain
blocks. Twenty-six attempts executed 164 incarnations and recovered 62 fully
validated results with zero errors or output/state/trace mismatches. Seventeen
attempts refreshed a canonical prefix and nine reused one; the reused runners
advanced 108 canonical WriteSets in 1.90 ms. Average maintenance for a reused
attempt was 0.21 ms versus 2.41 ms per full refresh, an 11.4x reduction. The
remaining 94 budget skips came from synchronous suffix execution rather than
prefix maintenance.

#### P4.23: Asynchronous deadline projection

A direct goroutine handoff is not yet safe because the retry StateDB is private
but its actuator raw-KV parent is the live canonical block buffer. A background
retry could therefore observe raw writes beyond its settled prefix. Until that
view is frozen or versioned, the canary does not claim real concurrency.

Instead, every incarnation result records its real cumulative worker completion
time from the conflict boundary. The canonical recorder already measures each
transaction duration without observer overhead. At the result's publication
boundary, the scheduler compares worker completion with the sum of canonical
durations from the conflict through the preceding transaction. Results are
classified as ready, late, or unknown; only ready results that also pass the
TransactionInfo, WriteSet, and BalanceTrace gate count as asynchronously
recoverable. Metrics under `sender_retry/async_projection/` expose candidate,
ready/late/unknown, validated/recovered, ready slack, and lateness. This gate
determines whether a frozen raw view plus a retry-priority worker queue can
recover enough work before enabling actual background execution.

The first projection window covered 6,381 blocks and 93 sender-chain samples.
Twelve retry candidates were classified with no unknown deadlines: seven were
ready and five late. All seven projected-ready results passed the exact output,
WriteSet, and BalanceTrace checks. Four of the five late observations were the
conflicting transaction itself, whose publication deadline is necessarily
zero; seven of the eight future-suffix results were projected ready. The ready
results had 15.65 ms total slack, while total lateness was 0.48 ms. This is a
small sample, but it is sufficient to start a bounded real-concurrency canary.

#### P4.24: Frozen raw capability and actual async retry canary

One disjoint quarter of sampled blocks (height `128 mod 256`) now uses a real
background sender-retry worker instead of the synchronous retry observer. The
other three quarters preserve the synchronous implementation and deadline
projection as an uncontended reference. At a conflict boundary, the canonical
thread advances one reusable private runner to the settled prefix, clones the
read-version maps needed by that incarnation, and transfers exclusive worker
ownership to a goroutine. A fully buffered result channel streams completed
suffix members back without pacing the worker; the canonical thread polls it
at every transaction boundary and admits only results that arrived before
their boundary and still carry the newest incarnation.

The raw database is a strict capability rather than a live parent. Exact raw
keys recorded by the source pre-execution (normally the Transfer TAPOS row) are
copied through the settled-prefix overlay before dispatch. Known absence is
preserved. `Get` or `Has` of an unrecorded key fails and increments
`frozen_raw_misses`; the goroutine can never fall through to the mutable
canonical block buffer. Typed state remains the worker's private StateDB copy.
The worker is always joined on both successful completion and an early
canonical error, so block-scoped state cannot escape its lifetime.

Only one job owns the reusable runner in this canary. A conflicting boundary
reached while it is busy executes serially and invalidates the unfinished
descendants, preventing a result computed through an unvalidated predecessor
from being selected. Metrics under `sender_retry/async_actual/` report jobs,
busy skips, executed/ready/late/stale results, validated/recovered candidates,
version rejection classes (read, sender, barrier, unsupported, or invalid
delta), frozen keys/misses, dispatch/worker time, errors, and finish wait.
Production evidence from this canary will determine queue depth and whether to
add more frozen workers before any canonical publication path consumes retry
results.

The first actual window showed that canonical dispatch, not worker execution,
was the next local bottleneck: six jobs spent 14.22 ms in dispatch and 1.62 ms
in the background worker. The next carrier therefore snapshots only version
cells read by the source sender suffix instead of cloning every typed version
map accumulated in the block. A retry-only branch that reads a new cell remains
safe: without a captured expected writer, any canonical prefix writer makes
the result conflict and fall back. `frozen_version_cells` plus
`dispatch/{prefix,raw_freeze,version_snapshot}_nanos` separate the remaining
StateDB prefix cost from raw and version freezing.

The prefix copy is then removed from the conflict boundary as well. The
sender-chain observer already owns up to four private block-start StateDBs and
fully reverts each chain. On actual-async sample blocks it retains one clean
non-canonical worker (or creates a spare before canonical execution when only
one observer worker exists). The retry runner advances that prewarmed state
from block start through exact canonical WriteSets, then uses the same frozen
raw capability and ownership handoff. This mirrors Erigon's long-lived worker
state more closely: the conflict boundary performs ordered prefix advancement,
not a deep StateDB copy. `prewarmed_runners`, prefix refresh/reuse, advance
count, copy/advance time, and `dispatch/prefix_nanos` verify the intended path
in production without mixing the three synchronous reference cohorts into the
actual-worker prefix measurements.

The longer single-runner window covered 378 actual sample blocks and 397
jobs. All 397 prefix preparations reused incremental state with zero refreshes,
copy time, execution errors, or frozen-raw misses. However, 242 conflict
boundaries found the runner busy, and 561 of 1,691 streamed results became
stale after newer incarnations. Prefix advancement consumed 175.89 ms and
worker execution 144.80 ms; state maintenance was no longer the scaling
constraint, but one-owner serialization was.

The next canary therefore retains every clean copied sender-chain observer
state except `shadow.base`, which remains owned by the independent finish
canary. This yields up to three retry runners without another StateDB copy; a
single-chain block still creates the existing one safe spare. Each runner owns
its prefix StateDB, DynamicProperties, raw overlay, recorder, settled cursor,
and busy bit. Conflicts dispatch to idle runners in canonical order, overlapping
incarnations remain isolated, and only the newest incarnation can be admitted.
The shared result channel is sized for both the global execution budget and all
ownership-return events. `runner_capacity`, `max_inflight`, and `busy_skipped`
measure whether a later priority queue needs more capacity or only delayed
rescheduling.

The first pool window covered 53 actual blocks with aggregate capacity 98 and
50 jobs. Pool exhaustion fell to zero while every runner remained prewarmed;
there were again zero prefix refreshes, copies, execution errors, or raw misses.
The remaining waste was incarnation distance rather than capacity: 91 of 254
completed results were stale because jobs eagerly walked long sender suffixes
before nearer cross-sender writes created a newer incarnation.

Async jobs now reserve only the nearest four sender-chain transactions. The
whole suffix is still invalidated immediately, but far descendants are deferred
until canonical order approaches their publication boundary. This is the first
bounded incarnation-priority policy: lower transaction indices consume the
global 64-execution budget first, while later boundaries can create a newer
job from a more recent settled prefix. `lookahead_deferred` measures displaced
far-suffix work; ready, stale, recovered, job, and dispatch ratios determine
whether the window should expand, shrink, or become a dynamic heap.

The first bounded-lookahead deployment covered 25 actual blocks and 14 jobs.
All 28 executions completed with zero stale results, busy skips, errors, raw
misses, prefix refreshes, or StateDB copies; 11 results arrived before their
boundary and three fully validated recovered results matched the canonical
outputs. Every observed job contained only the mandatory conflict transaction
and one sender successor, so `lookahead_deferred` remained zero and this window
could not yet exercise the four-task bound.

To accelerate the long-chain gate without consuming retry output canonically,
three of the four sampled cohorts (`64`, `128`, and `192 mod 256`) now use the
actual async scheduler. The `0 mod 256` cohort remains on the synchronous
settled-prefix observer as a fixed correctness and deadline-projection
reference. This changes only observer coverage: canonical transfer publication
continues to use the already-gated sender-chain path.

The first three-cohort long-chain window covered 41 actual blocks and 60 jobs.
The four-task window deferred 464 far-suffix tasks and kept runner exhaustion,
copying, refreshes, raw misses, execution errors, and finish waits at zero.
However, 46 of 191 completed results were still stale: a worker that had
already been superseded by a newer suffix incarnation continued executing its
remaining local tasks until the four-task job ended.

Each async transaction now has a canonical-thread-owned atomic incarnation
token. A worker checks the token before every sender successor and stops as
soon as the task is no longer the newest incarnation. The done event returns
the number of skipped tasks so the block-wide 64-execution reservation can be
reused safely; `superseded_before_execute` exposes the avoided work. This is the
bounded-runner equivalent of Erigon's retry heap admitting only the current
transaction incarnation: an actuator already in progress is allowed to finish,
but known-stale descendants never start.

The first production window with cancellation covered eight actual sample
blocks and 25 jobs. Of 67 executions, 40 future results arrived before their
canonical boundary; 25 of the 26 late results were the mandatory zero-deadline
conflict transactions, and only one result was stale. Atomic checks prevented
26 additional superseded transactions from starting. All 25 selected results
matched canonical TransactionInfo, WriteSet, and BalanceTrace; runner busy,
prefix refresh/copy, frozen-raw miss, execution error, and finish wait remained
zero. This closes the actual-async observer gate without enabling new canonical
publication.

#### P4.25: Block-scoped incarnation priority queue

The retry pool now has an Erigon-style minimum-transaction heap in front of
its fixed runners. A conflict always creates a request even when every runner
is occupied. Exact raw values, the compact read-version carrier, and a copy of
DynamicProperties are frozen at the original canonical boundary, so later
dispatch cannot observe transactions which were not part of that incarnation.
The private runner reaches the requested prefix only by applying immutable
canonical WriteSets already captured in the block version map; queued work
never refreshes from a later live StateDB.

At each canonical boundary, completed workers return ownership and the heap
dispatches the lowest useful transaction index first. A request whose only
current tasks are already behind the boundary is removed before execution;
superseded tokens and unused execution reservations are reclaimed immediately.
A lazy StateDB copy remains solely as the safe same-boundary fallback when the
sender-chain observer could not provide a clean prewarmed runner. Metrics under
`async_actual/queue/` report enqueue/dequeue counts, saturation enqueues,
dropped tasks, per-block maximum depth, and wait time. The existing
`busy_skipped` counter is retained as a compatibility signal and should remain
zero under the queued scheduler.

This queue is still an observe-only bridge. Canonical publication remains on
the previously gated sender-chain executor, and one synchronous sampled cohort
remains as an independent reference. The next gate is to move prefix
advancement off the canonical goroutine onto workers backed by a genuinely
shared MVCC value layer, then compare queue wait and ready ratios before using
retried results canonically.

The first queued production window covered eight actual blocks. Eleven
requests were enqueued and all eleven dequeued, with zero saturation enqueues
or dropped tasks and 18.27 microseconds total queue wait. The workers executed
31 transactions: of 20 future-suffix results, 19 were ready and one was a stale
incarnation, while the eleven conflict transactions were the expected
zero-deadline late results. Three superseded tasks were stopped, one selected
result matched every canonical carrier, and errors, frozen-raw misses,
busy-skips, and finish waits remained zero.

#### P4.26: Worker-owned canonical-prefix advancement

Queue dispatch no longer advances a private StateDB on the canonical
goroutine. Once a compatible runner is selected, ownership is marked busy and
the goroutine first applies the immutable canonical WriteSets up to the
request's frozen boundary, installs the frozen DynamicProperties snapshot, and
then executes the incarnation. The shared block version map is append-only in
transaction order; every prefix cell read by the worker was finalized before
the request could be enqueued, and all workers are joined before the map leaves
block scope.

Prefix accounting is returned only through the runner's completion event, so
concurrent workers never mutate retry-wide statistics. A failed prefix returns
runner ownership, drops the unexecuted tasks, and reclaims their global
execution reservations. `async_actual/worker/prefix_{jobs,advances,nanos,errors}`
separates background state advancement from canonical dispatch. The older
`dispatch/prefix_nanos` metric now measures only the rare same-boundary lazy
copy fallback and should otherwise remain zero.

This removes the measured prefix-replay component from the serial boundary,
but the runner still materializes a private StateDB view. The next structural
step is a shared version-value carrier that lets retry execution resolve typed
reads directly by `(key, txIndex, incarnation)` before canonical publication is
considered.

The first worker-owned prefix window covered 20 jobs and 384 canonical
WriteSet advances. All prefix work completed in the background in 6.97 ms with
zero prefix errors, while canonical `dispatch/prefix_nanos` remained zero.
Canonical dispatch fell to 1.03 ms total, or 51.5 microseconds per job, versus
378.9 microseconds per job in the comparable pre-handoff window. Of 31
future-suffix executions, 30 results were ready, one result was
late, and none were stale; all nine selected observer results matched their
serial reference. Queue drops, copies, refreshes, raw misses, busy skips, and
finish waits remained zero.

#### P4.27: Ordered async-retry publication canary

One sampled cohort (`192 mod 256`) now allows a ready, version-valid async
sender incarnation to replace canonical Transfer execution. The `64` and
`128` cohorts retain real async execution followed by independent serial
comparison, and `0 mod 256` retains the synchronous retry reference. This
keeps three distinct correctness signals while measuring actual saved serial
work on one quarter of sampled blocks.

Publication reuses the gated parallel-Transfer path: it repeats public-network
limit admission against the real ordered DynamicProperties, preflights the
complete typed WriteSet, restores the worker read set into the canonical
version recorder, applies writes in transaction order, appends the retained
BalanceTrace and TransactionInfo, flushes domain changes, and falls back to
serial execution on any admission or preflight rejection. Metrics under
`core/parallel_transfer/sender_retry/` report candidates, publications,
fallbacks, errors, and publication time.

A published result is deliberately excluded from the independent
TransactionInfo/BalanceTrace validation count because those carriers became
canonical. The post-application WriteSet is still captured independently and
compared with the worker carrier under
`async_actual/published/write_set_{matches,mismatches}`. Tests execute the same
mixed conflict block serially and through the publication cohort, compare every
TransactionInfo, and require identical committed StateDB roots. Production must
show zero published WriteSet mismatches/errors before expanding beyond this
cohort or supporting another actuator family.

The first publication window covered 23,743 imported blocks. Thirteen async
retry candidates were admitted and all thirteen replaced canonical Transfer
execution. The independently captured ordered WriteSets matched 13/13;
publication preflight fallbacks, public-bandwidth admission fallbacks,
mismatches, errors, and end-of-block worker waits were all zero. Across the
same window the actual scheduler launched 135 jobs and 284 incarnations: 148
arrived ready, 135 arrived after their own boundary, and one superseded result
arrived stale.

#### P4.28: Ordinary-block incarnation scheduling

After the sampled publication gate passed, the same retry priority queue was
connected to every ordinary block with the opt-in Transfer publisher enabled.
This does not add another block-start execution pass: the existing canonical
sender-chain preexecutor transfers its clean worker StateDBs directly to the
retry scheduler after initial chains finish. Up to four retained workers then
consume the minimum-transaction retry heap while canonical execution advances.
The block-start `StateDB` copy count is therefore unchanged, and the first
conflict still performs no synchronous copy.

Ordinary blocks capture canonical typed WriteSets so background runners can
advance from their last settled prefix and so published results are audited at
block end. The sampled layout remains unchanged: `0 mod 256` is the synchronous
reference, `64` and `128` are non-publishing async observers, and `192` is the
publishing canary. Disabling the opt-in publisher disables ordinary-block
retries without disabling those sampled correctness observers.

Retry source admission now also inherits the sender-chain publication edge. A
forwarded block-start result is not treated as a current baseline merely
because it names the same predecessor transaction; that predecessor's exact
source incarnation must itself have been canonically published. This matches
Erigon's rule that validation is incarnation-sensitive and prevents a stale
sender ancestor from making its descendants look ready.

The first ordinary-block 57-second window imported 1,600 blocks (28.1
blocks/s) and published 42 retry results with zero fallback, mismatch, or
error. Background prefix replay consumed 76.6 ms total and end-of-block worker
wait consumed 2.05 ms. The block range was denser than the preceding sampled
process; uptime-normalized throughput was about 2,240 transactions/s versus
1,870 transactions/s before ordinary retries, so there was no initial
throughput regression after normalization.

Canonical WriteSet materialization is the remaining retry cost on the serial
path. Metrics under `core/versioned_shadow/write_set_capture/` therefore split
enabled blocks, transactions, captured cells, nanoseconds, unsupported
captures, and errors from the already measured background prefix replay. The
next production gate uses those counters to decide whether to compact the
carrier further or move Transfer reads directly onto shared versioned values.

#### P4.29: Retry-read projected prefix carriers

The first measurement window showed why a compact carrier is required: 65 of
750 blocks enabled retry capture, materializing 63,505 cells for 6,732
transactions in 78.3 ms (11.6 microseconds per transaction and 1.20 ms per
enabled block). Only eleven canonical executions were replaced in that window.
Even though prefix application is off-thread, full post-image materialization
remained serial work and cost more than the recovered Transfers.

Ordinary blocks now derive a block-scoped projection from the exact read sets
of retryable sender chains. Canonical prefix transactions materialize only
written paths that overlap those reads, including the hierarchical rule that a
full Account write invalidates a field read and any field write invalidates a
full Account read. Transactions that are themselves members of a retryable
sender chain retain complete WriteSets because their result may become the
canonical publication carrier and still requires a full post-application
audit. Sampled correctness cohorts also retain full capture.

The full journal is still visited for every projected transaction: an unknown
write remains a barrier and forces the existing refresh/fallback rather than
being hidden by the filter. A retry that discovers a branch-dependent read not
seen at block start is also safe because the unfiltered version map records all
canonical writers and rejects the omitted stale value at admission. Empty
projected prefix WriteSets advance the runner's settled index without invoking
the ordered applier or finalizer. Split full/filtered transaction, cell, and
nanosecond counters make the reduction directly measurable after deployment.

The first projected-carrier window reduced average materialized cells from
9.43 to 2.49 per captured transaction (74%) and capture time from 11.6 to 8.0
microseconds (31%). Prefix application fell from roughly 17.8 to 3.9
microseconds per advanced transaction. A subsequent stable 58-second window
imported 1,696 blocks / 169,442 transactions (29.2 blocks/s and 2,921 tx/s),
published 24/24 retries without mismatch or error, and measured 3.01
microseconds per prefix advance.

DynamicProperties and exact raw-KV inputs are already frozen from the canonical
enqueue boundary into dedicated request carriers. They are therefore excluded
from non-sender projected prefix WriteSets instead of being materialized and
replayed a second time. Filtered capture maps are allocated lazily and use a
smaller initial capacity; empty projected transactions retain their known
status but allocate no key/mode maps and skip prefix application. Empty counts
are exported alongside the full/filtered cell and timing split.

After also removing DynamicProperties/raw-KV cells from the projected carrier,
a stable 58-second window imported 1,600 blocks / 165,224 transactions (27.6
blocks/s and 2,849 tx/s) and published 210/210 retries without mismatch or
error. The 34,959 captured transactions materialized 75,260 cells (2.15 per
transaction) in 223.7 ms: 8,426 full captures averaged 6.35 cells / 8.06
microseconds, while 26,533 filtered captures averaged 0.82 cells / 5.87
microseconds and 4,778 of those were empty. Average capture cost was 6.40
microseconds per transaction and 0.78 ms per enabled block; prefix application
averaged 3.62 microseconds per advance.

#### P4.30: Mutation-time projected value carriers

The P4.29 filtered path still walked every undo-journal entry at the transaction
boundary, even when the retry projection retained no writes. Ordinary Transfer
retry projections now skip that scan when all retained key families have
complete inline-recorder coverage. Account/full-account and scalar-field writes
were already registered at their authoritative mutation sites. AccountKV puts,
deletes, and Erigon-style generation resets are now registered there as well,
covering the permission/incarnation reads observed in real Transfer retries.

The supported-kind decision is centralized in the state package rather than
being inferred by the scheduler. If a retry projection contains storage, code,
witness, contract metadata, transient storage, or any future unregistered kind,
capture automatically retains the P4.29 full-journal path. Sender-chain members
and sampled correctness cohorts also keep full capture for publication audit.
The optimized path only materializes final values for matching recorder writes;
it does not copy a worker StateDB, relax version validation, or change canonical
transaction ordering.

`core/versioned_shadow/write_set_capture/recorder_only_transactions` and
`recorder_only_nanos` expose the production hit rate and cost independently of
the existing full/filtered totals. The deployment gate remains zero published
WriteSet mismatch/error and zero unsupported capture; performance is evaluated
against P4.29's 5.87-microsecond filtered-capture and 0.78-ms enabled-block
baselines.

The first high-load 40-second P4.30 window imported 1,344 blocks / 119,572
transactions (33.6 blocks/s and 2,989 tx/s). All 8,926 filtered captures used
the recorder-only path; 65/65 retries published with matching WriteSets and
there were no capture unsupported/error, retry error, or end-of-block wait
events. Filtered capture averaged 4.98 microseconds per transaction versus the
P4.29 5.87-microsecond baseline (15% lower), and total capture averaged 0.57 ms
per enabled block versus 0.78 ms (27% lower). Prefix application averaged 2.33
microseconds per advance versus 3.62 microseconds (36% lower). The same window
compacted 3.48 GB in / 3.29 GB out and added about 280 MB estimated debt without
a write delay, so Pebble compaction remains the larger whole-node constraint.

#### P4.31: Write-only recorder index

P4.30 removed the undo-journal scan but its recorder capture still called the
general access visitor, walking reads that cannot contribute a post-image. The
same production window recorded about 89 read cells and 17 write cells per
versioned transaction. `TransactionAccessRecorder` now appends a key to a
reusable write-only slice exactly when that key first acquires an ordinary or
commutative write bit. Repeated writes update the existing mode without adding
another slice entry; reset clears retained key references while preserving
capacity for the next transaction.

Projected and full WriteSet collection now use `VisitWrites`, while read-set
capture and OCC validation retain the complete typed access maps. AccountKV
keys that transition from a prior read to a write own the string retained by
the write slice, so actuator scratch buffers cannot corrupt a later capture.
This follows Erigon's separate worker ReadSet/WriteSet carrier without adding a
second write hash table or changing mutation order. The production gate remains
100% recorder-only hits for ordinary filtered Transfer prefixes and zero
publication mismatch/error; the expected gain is removal of roughly five out
of six access-map visits at capture time.

The first high-load 40-second P4.31 window imported 1,312 blocks / 112,727
transactions (32.8 blocks/s and 2,818 tx/s). Recorder-only capture covered
8,015/8,015 filtered transactions; 22/22 retry publications matched and
unsupported capture, capture error, retry error, mismatch, and finish wait all
remained zero. Filtered capture fell from P4.30's 4.98 to 2.88 microseconds per
transaction (42% lower), or 51% below P4.29's 5.87-microsecond baseline. Total
capture fell from 0.57 to 0.36 ms per enabled block, 54% below P4.29's 0.78-ms
baseline. Pebble concurrently compacted 3.58 GB in / 3.40 GB out, reduced
estimated debt by about 104 MB, and recorded no write delay; after the carrier
reductions, sustained compaction and bursty downloader supply remain larger
whole-node constraints than projected WriteSet capture.

#### P4.32: Projection-aware lazy write carrier

Erigon's current versioned executor keeps independent typed `ReadSet` and
`WriteSet` structures; per-path write maps are checked out lazily on the first
write and released at transaction finalization. A transaction that does not
need an output carrier does not build one. P4.31 still appended write keys for
every version-observed transaction even though only retry-enabled and sampled
blocks consume canonical WriteSets, shifting unnecessary slice growth into the
roughly 90% no-capture path.

The recorder now configures its write-key carrier at `BeginTransaction` using
the already known block/transaction capture plan. Transactions outside a
WriteSet-capture block disable the carrier completely while retaining their
full OCC access maps. Full sender-chain/audit transactions retain every write.
Ordinary prefix transactions apply the retry-read projection when a key first
acquires a write bit, so excluded writes never enter the transaction-end slice;
AccountKV keys that do enter it still own borrowed scratch strings. The final
capture keeps its include check as a defensive invariant, and any projection
containing a recorder-incomplete kind still takes the P4.29 journal fallback.

On the local 64-Transfer versioned-shadow benchmark with WriteSet capture
disabled, five 100-ms samples moved from about 101.4 to 99.6 microseconds per
block (1.8% lower), 146 to 144 allocations, and roughly 35,733 to 35,460 bytes.
This is development evidence rather than a production throughput claim; the
deployment gate compares filtered capture against P4.31's 2.88 microseconds,
checks recorder-only coverage, and retains the zero mismatch/error requirement.

The first high-load 40-second P4.32 window imported 1,248 blocks / 113,583
transactions (31.2 blocks/s and 2,840 tx/s). All 19,071 filtered captures used
the recorder-only path and 271/271 retry publications matched; unsupported
capture, capture/retry error, mismatch, and finish wait remained zero. Filtered
capture fell from P4.31's 2.88 to 1.93 microseconds per transaction (33% lower),
67% below P4.29's 5.87-microsecond baseline. The remaining capture cost moved
to the 7,592 full/audit transactions: they consumed 48.6 ms (6.40 microseconds
each), more than the 36.8 ms consumed by over twice as many filtered captures.

Pebble simultaneously compacted 6.17 GB in / 4.84 GB out and reduced estimated
debt by about 1.09 GB. It reported no write delay, but async commit encountered
two backpressure events totalling 304 ms. The next execution slice should make
Transfer sender/audit WriteSets recorder-complete across their dynamic/raw
families and remove their full journal scan; at whole-node level, compaction
bandwidth and commit backpressure remain the larger throughput constraints.

#### P4.33: Complete mutation-time WriteSet

P4.32 optimized the optional retry carrier, but its OCC validator still walked
the StateDB undo journal twice per transaction: once to discover conflicts and
again to install versions. Full sender-chain/audit captures also retained the
journal materialization path. This differed from Erigon's executor, where the
worker owns a complete typed WriteSet independently of its ReadSet and returns
that carrier directly to ordered validation/publication.

The transaction recorder now retains one complete first-write-ordered key
carrier for OCC regardless of whether the block requests post-images. Its
optional capture carrier is separate: full captures reuse the OCC carrier,
projected captures append only matching keys, and no-capture transactions build
no second slice. This deliberately trades P4.32's two-allocation local win for
removing both transaction-end journal scans from every version-observed
transaction.

`journal.append` is now the authoritative completion hook for state families
that did not already record at their typed mutation sites: witness, storage,
code, contract metadata, self-destruct, and transient storage. Account scalar
pre-images do not reintroduce a coarse Account key when exact field writes are
present; AccountKV writes/generation resets deduplicate against their existing
inline records. DynamicProperties and raw KV keep their direct mutation-time
post-images. Journaled resource-weight changes are accepted only when the same
recorder is attached to their DynamicProperties target, preserving the
commutative delta classification. Unknown journal or inline write kinds mark a
separate write-unsupported barrier rather than silently producing a partial
carrier.

Ordinary full sender-chain carriers now take the same recorder-only path as
filtered prefixes. The 1/64 sampled full capture keeps the combined journal
path as an independent reference. New
`recorder_only_full_{transactions,nanos}` counters isolate the removed full
capture cost. Tests compare full recorder and journal post-images across typed
account, storage, AccountKV, and DynamicProperties cells, cover every journal
write family and unknown-write rejection, and exercise both sampled and
ordinary async retry publication. `go test ./... -p=1`, `go vet ./...`, and the
targeted race suite pass.

A short local 64-Transfer shadow benchmark measured about 96 microseconds per
block versus P4.32's roughly 100-microsecond development baseline. Allocation
count returned from 144 to 146 and bytes from about 35.46 KB to 35.72 KB because
the complete OCC carrier is now always retained; the production gate therefore
depends on the eliminated journal walks and full-capture counters, not this
small local result. Mainnet must retain zero unsupported capture, publication
mismatch, and retry error while reducing P4.32's 6.40-microsecond full-capture
cost and total critical-path capture time.

The first post-deployment 42-second window imported 1,026 blocks / 98,299
transactions. Ordinary full capture was recorder-only for 4,405/4,405
transactions and averaged 3.79 microseconds, 41% below P4.32's 6.40-microsecond
full baseline. Filtered capture averaged 1.44 microseconds, total capture was
0.317 ms per enabled block, and 252/252 retry publications matched with zero
unsupported capture, error, mismatch, or finish wait. This cold window was
storage-bound: compaction debt grew by 1.42 GB and 28 async-commit backpressure
events consumed 697 ms.

A following 52-second warm window imported 1,561 blocks / 143,713 transactions
(30.0 blocks/s and 2,764 tx/s) while Pebble compacted 3.63 GB in / 3.33 GB out
and reduced debt by 348 MB. Ordinary full recorder capture remained 100%
covered and averaged 3.86 microseconds; filtered capture averaged 2.11
microseconds and total capture fell to 0.278 ms per enabled block, 24% below
P4.32's 0.366-ms window. The complete carrier also reduced observed write
cells from about 16.8 to 9.5 per transaction by eliminating duplicate journal
visits. All 137 retry publications matched, with zero unsupported/error/
mismatch and only one 128-ms commit-backpressure event. Execution is no longer
the leading measured constraint; sustained compaction write amplification is
the next alignment target.

#### P4.34: Schema-owned physical-write attribution

A 30-second storage window after P4.33 showed 1.71 million coalesced
blockbuffer operations, about 2.65 GB of logical database writes, 329 MB of L0
flush output, 1.50 GB compaction input / 1.46 GB compaction output, and 3.39 GB
of OS-reported physical writes. The buffer already collapsed 2.55 million
input operations to those 1.71 million final rows, so another blind merge or
larger batch cannot explain which data should be removed. Existing metrics did
not attribute the final rows to commitment branches, latest state, temporal
history, staged bodies, blocks, or transaction indexes.

Rawdb now owns a low-cardinality physical-key classifier, and blockbuffer
exports sampled final-operation and logical-byte counters for each schema
family under `blockbuffer/flush/family/<family>/sampled_{ops,bytes}`. Only one
of every 32 successfully committed flush groups is scanned; the sampled-group
counter supplies the denominator. This keeps the diagnostic allocation-free
and limits its coalesced-flush benchmark overhead to roughly 0.6%, versus about
8% when every group was classified. It changes neither keys nor values,
coalescing, batch sizing, WAL behavior, or flush order.

The deployment sample will use family byte shares, rather than extrapolated
absolute counts, to choose the next change. A commitment-dominated result
points to Erigon-style branch aggregation/checkpointing; history dominance
points to earlier append-only cold migration and hot index pruning; staged or
canonical body dominance points to bypassing the LSM for immutable sequential
payloads. No storage family is removed solely from aggregate compaction data.

The first 63-second production sample imported 2,696 blocks / 147,313
transactions while blockbuffer collapsed 5,063,364 input operations to
3,279,102 final rows. Fourteen sampled flush groups attributed 70.85% of final
logical bytes to commitment branches and 25.78% to state history; account
latest, KV latest, metadata, and other rows together accounted for only 3.37%.
Commitment and history therefore explain 96.63% of the coalesced write stream.
Pebble concurrently compacted 4.78 GB in / 4.11 GB out, OS writes reached
6.48 GB, debt grew by 277 MB, and three backpressure events consumed 491 ms.

Lifetime counters supplied the scheduling detail missing from that short
window: 5,656 successful flush calls produced exactly 5,656 flush groups for
33,983 layers (6.01 layers/group). The 32 MiB group boundary never split a
flush call. Increasing only the Pebble batch limit would consequently leave
production unchanged; the aggregation opportunity is across the current
120-ms flush calls.

#### P4.35: Output-bounded solidified aggregation

The local Erigon implementation does encode touched branch slots during the
fold, but `ApplyDeferredBranchUpdates` merges each partial update with its
previous value before `DomainPut`; its hot latest domain still receives a
complete branch representation. Persisting delta chains and replaying them on
every branch read is therefore neither required for alignment nor acceptable
without Erigon's immutable-domain aggregation and branch cache. The transferable
strategy is to retain finalized work briefly and merge it before the hot-store
write.

Blockbuffer now separates the source aggregation bound from the final Pebble
batch bound. It first builds the existing 32 MiB window, then may admit more
solidified layers up to 128 MiB of source data only after projecting that
layer's exact last-writer-wins size delta. The final batch remains capped at
32 MiB, so Pebble's pooled allocation, WAL record size, and atomicity contract
do not grow. Unique append-only history stops the extension; repeated
commitment post-images can continue collapsing. The direct one-layer path
remains allocation-free, and already-durable overlay rows retain their removal
semantics.

The asynchronous flush collection window grows from 120 ms to 480 ms. At the
observed bulk-sync rate this should increase the merge population from roughly
six to roughly two dozen solidified layers while retaining only about half a
second of additional crash-replay tail. Buffered reads remain current, shutdown
interrupts the wait, and the final batch cap remains unchanged. New exact
input/output logical-byte counters and extended-group/layer counters make the
production A/B falsifiable: the gate is fewer flush groups, a lower output/input
byte ratio and lower disk/compaction bytes per imported block without more
write stalls, backpressure, or unbounded buffered layers.

The 480-ms canary produced two consistent post-restart windows. The first
imported 1,724 blocks / 86,289 transactions in 47 seconds and increased the
group population to 19.36 layers; the second imported 1,824 blocks / 91,962
transactions in 52 seconds at 18.36 layers/group. Final logical bytes were
33.7-34.5% of source bytes. The warmer window wrote about 361 KB and 1,132
final operations per block, respectively 14% and 7% below P4.34's approximately
420 KB / 1,216 operations per block, with zero Pebble write delay.

Physical bytes were not yet comparable: compaction debt fell by 441 MB in the
first window and grew by 248 MB while 4.88 GB was compacted in the second, so
the OS-write denominator was dominated by different pre-existing level work.
More importantly, extended groups remained zero and source input averaged only
about 19 MiB/group. The canary therefore proved the scheduling mechanism but
did not exercise the new output-bounded path. The next calibration raises the
wait to 960 ms: the observed import rate projects roughly 36-38 layers and
38 MiB of source data per call, while the measured final output projects only
about 13 MiB, comfortably below the unchanged 32 MiB final batch cap.

The 960-ms deployment then imported 1,994 blocks / 108,647 transactions in a
52-second warm window. It reached 40.39 layers/group; 46 of 49 groups crossed
the old source boundary and admitted 511 additional layers. Final bytes fell
to 28.56% of input, about 308 KB/block (26.8% below P4.34 and 14.8% below the
480-ms canary). Final operations fell to about 1,041/block (14.4% below P4.34).
There was again no Pebble write delay. Relative to the preceding 480-ms warm
window, OS writes per block fell about 13%, database writes 17%, compaction
input 17%, and compaction output 21%; both samples still had changing debt, so
these physical ratios remain directional rather than a fixed-range A/B.

The final group averaged only about 12.5 MiB despite the much larger source
window, and process memory stayed near 2.1 GiB. The next bounded calibration is
1,920 ms. At the observed rate it should aggregate roughly 80 layers / 86 MiB
of source data and remain below both the 32 MiB final-output and 128 MiB source
bounds. Shutdown still interrupts the timer, caught-up mainnet produces only
about one block during the interval, and bulk-sync crash replay remains a
bounded sub-two-second tail.

The 1,920-ms deployment reached the intended ceiling. Its first stable
52-second window imported 1,950 blocks / 116,750 transactions at 78.68
layers/group; 24 flush calls produced 25 final groups, proving that at least one
call hit a fixed source/output bound and split. Final bytes were 25.39% of
source, about 302 KB/block, and OS writes were about 2.44 MB/block. Compaction
debt grew by 3.13 GB in that window, then fell by 1.78 GB in the following
52-second window while importing another 1,613 blocks / 114,151 transactions.
Neither window recorded Pebble write delay; the debt-draining window had only
two async-commit backpressure events totalling 141 ms. The output-bounded
aggregation window therefore stays at 1,920 ms and will not be enlarged.

#### P4.36: Temporal-write attribution before index deferral

The remaining `state_history` family combines three physically and
semantically different streams: one small block-to-txNum row, block/sequence
changeset payloads required by unwind, and latest-key/block inverse-index rows
used by historical keyed reads. Erigon treats history values and their accessors
as separate aggregation products; delaying an accessor during far-behind staged
sync can be valid even when the underlying changeset must remain available.

The rawdb classifier now exposes `state_tx_range`, `state_changeset`, and
`state_change_index` independently at the same sampled final-flush boundary.
This changes no keys, writes, read paths, or retention policy. Production must
first establish byte/operation shares. Only an index-dominated result justifies
designing a far-sync deferred-index stage; changeset dominance instead points
to earlier cold segment construction or a sync-specific minimal unwind window.

Three production samples rejected index-only deferral as the primary change.
Changeset payload rows contributed 88.5% of temporal logical bytes, inverse
index rows 11.5%, and tx-range rows 0.03%; operation shares were about 69.5%,
30.5%, and 0.1%. At this height temporal rows also exceeded commitment bytes.
The next diagnostic samples one of every 128 already-encoded changesets and
attributes its RLP payload to previous value, next value, logical key, and fixed
metadata/framing. It adds no encoding or decoding and leaves all writes intact.
If previous+next images dominate, a far-sync unwind-only representation can
retain the previous image while omitting forward/history accessors until the
node enters its final retention horizon. If fixed metadata dominates, a compact
schema that derives block/sequence/hash context from the row and tx-range keys
is the appropriate first reduction.

Two post-deploy component windows then covered 4,390 blocks / 276,883
transactions and 17,204 sampled changesets. The combined payload was 4.79 MB:
previous images were 34.4%, next images 34.6%, fixed RLP metadata/framing 28.3%,
and logical keys 2.8%. A previous image existed on 97.6% of rows and a next
image on 99.8%. Both windows recorded zero Pebble write delay; combined
compaction debt changed by only about +233 MB while the node continued syncing.

#### P4.37: Erigon-style previous-image-only hot history

Current Erigon writes a mutation through `DomainBufferedWriter.PutWithPrev`,
whose history writer receives only `AddPrevValue(key, txNum, prev)`. The new
value is written once to the latest domain. Erigon unwind restores those
previous values, and as-of reads use the next mutation's previous image; it
does not duplicate the forward image into history.

go-tron's unwind and hot/cold as-of readers already have the same previous-image
semantics. The only non-test consumer of `Next` was the pruning checker. Its
complete code-hash closure is preserved without `Next`: the current image is
collected from AccountLatest, while every superseded image is collected from a
subsequent history row's `Prev`.

New hot changeset rows therefore encode `persistedStateDomainChange`, which
omits `NextExists` and `Next`. Commit capture may retain a borrowed next slice
long enough to compare before/after values and attribute omitted bytes, but the
rawdb writer neither clones nor persists it. This should remove about 34.6% of
changeset payload bytes, or about 30.6% of temporal logical bytes at the sampled
family mix, before Pebble write amplification. A read-only legacy RLP fallback
keeps the currently running test database inspectable during the transition;
fresh writes never use it, and it can be deleted after the next clean resync.

The first implementation intentionally leaves the cold binary record layout
unchanged. Production-built cold rows originate from the new hot decoder and
thus carry no next image (only the empty legacy fields); a later binary-format
revision can remove those final flag/length bytes together with duplicated
block context after the hot-write reduction is measured.

Two stable production windows after deployment covered 4,569 blocks / 246,133
transactions and 16,750 sampled changesets. Average encoded row size fell from
278.4 bytes in the paired P4.36 baseline to 180.3 bytes, a 35.2% reduction.
Normalized per imported block, final coalesced output fell 15.5%, OS and Pebble
writes both fell about 24.8%, compaction input fell 25.9%, and compaction output
fell 26.9%. Both windows recorded zero Pebble write delay. Compaction debt rose
during this sample, so the physical-byte deltas are directional, but the exact
logical-row result and repeated physical reductions pass the P4.37 gate.

#### P4.38: Hoist common block context out of hot changeset rows

After removing the forward image, fixed metadata and framing account for about
41.9% of the remaining hot changeset payload. Every row still repeats its block
number, block hash, and sequence even though the physical key already contains
`blockNum || seq`, and the one-per-block `StateTxRange` row already owns the
canonical block hash. Erigon similarly keeps transaction ordering in history
keys/indexes instead of duplicating block identity in each previous value.

New hot rows therefore persist only tx number, typed latest-domain identity,
logical key, and previous image. Direct block iteration reconstructs block
number and sequence from the physical key without another point read. The cold
builder already scans `StateTxRange`, so tx-range iteration stamps its block
hash onto each yielded change before encoding immutable history. Fork planning
reads and validates that same range once per orphan block before scanning its
changes, preserving the canonical-branch guard without storing or checking the
same hash on every row. Read-only fallbacks accept both preceding RLP layouts
during the current test deployment; fresh writes use only the compact layout.

Three post-deploy windows covered 9,024 blocks / 454,823 transactions and
30,087 sampled changesets. The exact logical result was 142.4 bytes/row, 21.0%
below P4.37; fixed metadata fell from 75.4 to 36.1 bytes/row, a 52.2%
reduction. Final coalesced output fell another 15.3% to about 234 KB/block.
The three windows ran at 37.7 blocks/s and 1,898 tx/s with zero Pebble write
delay and zero execution/equivalence errors. OS, database, and compaction byte
totals are not directly comparable because this run drained 2.52 GB of debt
while P4.37 accumulated 3.33 GB; the exact row result and lower pre-compaction
output pass the P4.38 gate.

#### P4.39: Erigon-style derived hot-history index stage

After P4.38, production attribution moved from 88.5/11.5% changeset/index to
74.5/25.4%. The empty-valued latest-key/block inverse rows are now one quarter
of temporal logical writes even though they are not authoritative for unwind,
commitment, or cold history construction.

Erigon does not publish this relation as an independently prefixed hot write
inside canonical execution. Its `InvertedIndexBufferedWriter` collects both
tx-to-key and key-to-tx relations with ETL, loads them in sorted order, and
later collates compressed immutable index files. go-tron's next adaptation is
a recoverable, hash-bound `StateHistoryIndex` stage: sync execution publishes
only tx ranges and previous-image changesets; after the commit barrier, a
bounded ETL pass derives inverse rows from solidified changesets. The stage
watermark advances only after the ordered load succeeds. Recent un-solidified
blocks remain queryable through a bounded direct changeset tail scan, so a
same-height fork can still discard their buffered rows without leaving durable
derived-index residue.

The implemented stage uses the prior solidified boundary as the one-time
baseline for databases written by the inline-index layout. Every subsequent
pass is capped by both verified `Finish` and `latest_solidified_block_num`,
checks each source `StateTxRange` hash against the canonical block hash, sorts
and duplicate-collapses inverse puts in an 8 MiB bounded ETL collector, and
publishes its hash-bound watermark only after load succeeds. Interrupted loads
are idempotently retried from the old watermark. Ordinary producer/gossip
imports still publish inverse rows and the stage watermark inside their
rewindable block layer; only the bulk-sync executor defers them. Sync waits for
at least 256 solidified blocks before opening an ETL collector (up to 4,096 per
pass), then forces the final sub-batch suffix at sync completion. Inline import
advances the watermark only from the immediately preceding hash-bound block, so
it cannot jump over a failed or deferred stage gap. Deep async sync sessions
also settle at a 4,096-applied-block checkpoint before immediately resuming;
continuous peer supply therefore cannot postpone TxLookup/StateHistoryIndex
publication indefinitely while still retaining executor/commit-scope reuse
across dozens of local chunks.

Archive readers split their bounded `(targetBlock, headBlock]` request at the
stage watermark: the covered prefix uses exact/prefix inverse seeks, while the
un-solidified suffix uses one ordered changeset range iterator and filters the
requested exact keys or prefixes while decoding. The original implementation
opened one Pebble/blockbuffer iterator per tail block; during fast mainnet sync
the observed watermark debt reached roughly 2,800 blocks, making iterator setup
and repeated overlay construction dominate historical RPC CPU. The range form
preserves the same block-pack repair precedence and early-stop semantics without
publishing rewind-unsafe inverse keys in canonical execution. A 2,800-block,
eight-change-per-block local benchmark fell from about 123 ms and 356 MB of
temporary reads per pass to 8.2 ms and 11.3 MB, roughly 15x faster and 31x
smaller by bytes allocated (repeat runs remained in the roughly 14--15x
range).
This applies even before cold history is installed; bounded readers no longer
take the legacy hot-only shortcut that assumes every changeset has an inverse
row. Sync exposes pass, block, change, ETL applied/input/batch, interruption,
and duration counters under `sync/stage/state_history_index/*` for the P4.39
production gate.

A later online concurrency profile showed the range iterator had removed the
history-read CPU bottleneck but not its queueing delay: six of eight concurrent
archive requests were waiting in `historyReaderAtContext` for `chainmu`, behind
either one 32-block sync insertion batch or a 4,096-block history-index ETL
pass. Sync insertion now retains its range executor and signature prewarm but
hands `chainmu` back after each fully published block. The history stage now
captures a committed-layer plus Pebble MVCC snapshot under the lock, performs
its read-only scan and sorted load without `chainmu`, and reacquires the lock
only to compare-and-publish the hash-bound watermark. A concurrent rewind or
inline advance changes the expected progress row and prevents stale watermark
publication; close is serialized with the stage pass. Non-snapshot test stores
retain the original locked fallback. This keeps canonical block publication
atomic while reducing the largest archive-reader exclusion regions from a
whole sync batch/ETL pass to one block or the two short stage planning/publish
sections.

The first per-block-handoff deployment still showed 1.55--6.52 second latency
across eight concurrent historical calls even though sync continued normally.
Its goroutine dump exposed a lock-scheduler mismatch rather than another long
critical section: `lockMutexContext` retried `TryLock` on a 10 ms timer, so its
RPC waiters never entered `sync.Mutex`'s fairness queue and the importer could
release one block then immediately reacquire for the next before any timer
woke. Contended context-aware acquisition now queues a real `Lock` waiter; an
unbuffered ownership handoff returns the acquired mutex to the request, while a
request whose context wins first makes the queued waiter release immediately
after acquisition. The uncontended path retains the allocation-free `TryLock`
fast path. This makes the per-block unlock an actual waiter handoff rather than
only a theoretical polling window.

After the initial deployment exposed one-collector-per-fragmented-drain
behavior, the production path was tightened in two steps: require at least 256
solidified blocks per ordinary pass, and settle a continuously supplied deep
sync session every 4,096 applied blocks. The final deployment first retired the
temporary build's 32,768-block index debt, then held a stable one-pass-per-stage
checkpoint cadence. Two ETL-inclusive windows covered 5,888 imported blocks /
350,104 transactions while the index stage processed 8,192 source blocks. The
stage itself sustained 526.7 source blocks/s, collapsed 3.78 million changes to
2.64 million latest-key/block puts, and consumed about 46.3 KB of collector
input per source block. Inline inverse-index sampled bytes and operations stayed
at zero, and Pebble recorded zero write delay.

The combined windows ran at 33.5 blocks/s and 1,989 tx/s. Their blocks were
18.0% more transaction-dense than P4.38, so physical comparison is normalized
per transaction: final coalesced output fell 16.3%, OS writes 5.5%, Pebble
writes 7.6%, compaction input 14.8%, and compaction output 5.7%. Compaction debt
also drained by 292 MB across the pair. Hot changeset encoding remained stable
at 138.5 bytes/sample row and online history-tail tests plus the full local
archive/reorg suite passed. These results pass P4.39; the remaining inverse-key
storage cost belongs to immutable compressed index construction rather than
canonical execution.

#### P4.40: Erigon-style previous-value-only cold history

P4.37 and P4.38 removed the forward image and duplicated block context from hot
Pebble rows, but the cold binary format still repeated `BlockNum`, `BlockHash`,
`Seq`, and an empty `Next` flag/length in every immutable record. Erigon's
history values contain the previous value while transaction/block ordering is
owned by immutable indexes and aggregation metadata, so retaining those fields
inside every compressed value adds decode and compaction volume without adding
authority.

StateDomainChange segment v5 now stores only `TxNum`, flat-domain identity,
owner/generation/domain/key, and `Prev`. The one-per-block `StateTxRange` table
restores `BlockNum` and `BlockHash`. Cold `Seq` is a deterministic packed value:
the high 32 bits are `TxNum - block.BeginTxNum`, and the low 32 bits are the
immutable segment record ordinal plus one. This remains ordered and unique even
when a history boundary splits one block between segments at different txNum
values; a dedicated split-block test protects that restore invariant. The v4
accessor's uint32 record-index bound also bounds the low word.

New in-memory and ETL builders emit v5 directly. Readers retain v1/v2 support
for existing test data. Compaction no longer copies source payload bytes or
arithmetically remaps old offsets: it stream-decodes each source record, emits
one v5 frame at a time, then rebuilds the tx index in one bounded scan and the
v4 accessor through the existing bounded ETL path. This makes mixed v2/v5
compaction format-safe and keeps peak conversion memory at one record plus the
configured ETL buffers.

On the deterministic 20,000-record storage corpus shaped like current
previous-only hot input, v5 reduced uncompressed segment bytes from 3,102,444
to 2,042,444 (34.17%). Whole-segment zstd bytes fell from 954,872 to 906,491
(5.07%); repeated legacy context already compressed well, so the larger gain is
less logical data to encode, decode, validate, and merge. These are local format
measurements, not a production throughput claim. The deployment gate must still
measure cold build/merge duration, segment bytes, cold-stage lag, and concurrent
sync/Pebble behavior.

#### P4.41: Erigon-style commitment leaf-key shortening

The post-P4.40 hot-sync window covered 2,460 blocks / 92,931 transactions in 47
seconds (52.34 blocks/s, 1,977 tx/s) with zero Pebble write delay. Final
coalesced output was 150.4 KB/block, but physical writes remained 2.30 MB/block
while compaction drained 470 MB of debt. Within a sampled coalesced flush,
CommitmentDomain branch rows were 60.1% of attributed bytes and hot changesets
were 36.7%; latest account/KV rows together were only 3.2%. Commitment was
therefore the next authoritative write family to reduce.

Erigon's current commitment branch format supports replacing plain leaf keys
with shortened identities before immutable storage (`BranchData.ReplacePlainKeys`
and `HasShortenedKeys`). go-tron's commitment tree already addresses every key
by `keccak256(len || physicalKey)`. Its persisted leaf nevertheless repeated the
complete 40-plus-byte physical AccountLatest/KVLatest key solely so the next
fold could hash it back to that same path.

New branches store a distinct path-leaf kind containing the existing 32-byte
key path plus the 32-byte leaf value hash. The value hash still commits to the
complete raw key and value; branch/node hashes are unchanged because they have
always consumed only child value hashes. Key identity and collision descent now
compare the stored path directly. The format retains the preceding raw-key leaf
decoder, and any legacy branch touched by a later fold is rewritten with path
leaves. Restart therefore supports a mixed tree without a whole-database
migration or a second authoritative keyspace.

The parallel root fold owns path identities in fixed-width branch fields, so
op sorting/compaction cannot leave buffered branches aliasing transient memory.
Sequential and parallel folds, rawdb-backed folds, raw-leaf update migration,
and raw-leaf collision splitting are byte/root-equivalent in tests. On a
deterministic 4,096-row corpus split evenly between production-shaped account
and contract-storage keys, encoded commitment branches fell from 447,136 to
318,112 bytes (28.86%). The production-shaped 256-update parallel BlockBuffer
benchmark retained roughly the same latency while allocations fell from
148--157 to 113--123 and allocated bytes from 328--335 KB to 294--310 KB per
fold. These local measurements establish the format win; the deployment gate
must still compare normalized commitment/coalesced/physical bytes and verify a
clean restart with zero commitment/equivalence errors.

The production deployment restarted cleanly on the mixed legacy/new datadir
and resumed active sync without rebuilding CommitmentDomain. Three windows
covered 7,013 blocks / 330,447 transactions in 160 seconds (43.83 blocks/s,
2,065 tx/s) with zero Pebble write delay and zero contract, parallel execution,
ordered-publication, commitment-stage, or history-stage errors/mismatches. Two
complete family samples reduced commitment bytes from 563.32 to 556.54 per
final coalesced operation (1.20%); the unchanged control family, changesets,
moved only 0.53%. The gain is smaller than the fresh-tree corpus because a
coalesced commitment operation contains dense hash ancestors and untouched
legacy leaves are migrated only when their branch changes.

Final coalesced bytes fell 1.91% per transaction across the three windows.
OS writes fell 28.7% per transaction and compaction bytes fell further, but
those are directional only: the new windows accumulated 348 MB of compaction
debt while the paired pre-deploy window drained 470 MB, and restart changed the
LSM work queue. The exact format/corpus result, normalized commitment sample,
clean mixed-format restart, and zero-error sync pass P4.41 without claiming a
large throughput win. The remaining commitment cost is the repeated complete
dense ancestor branch, not leaf-key bytes. Erigon also merges partial updates
with the prior branch before writing the hot latest domain, so the next useful
step is reducing complete-branch write frequency through longer aggregation
ownership and the cold immutable lifecycle, not persisting a delta chain.

#### P4.42: Block-packed hot changesets

After P4.41, `state_changeset` remained the other dominant hot family: complete
samples were 36--37% of attributed commitment-plus-history bytes and averaged
about 177 bytes per final coalesced Put. The encoded previous-image row was
about 143 bytes, leaving roughly 34 bytes of repeated physical changeset key
per mutation. Unwind, hot/cold history iteration, inverse-index rebuild, and
as-of reads all consume a whole candidate block changeset; only the diagnostic
point accessor addresses a sequence directly. This makes per-row Pebble/WAL
ownership pure write amplification on canonical sync.

P4.42 extends the Erigon-style buffered previous-value boundary from a
transaction flush to block finalization. `DomainChangeStage` still captures and
stamps each successful transaction at its exact txNum, but retains those owned
changes until the block-final mutation pass succeeds. The default registered
history domain then emits one versioned RLP block container under sequence zero
and publishes the same rebuildable inverse keys (or defers them in bulk sync)
for every logical row. Failed blocks never publish a partial container.

The container records its first sequence and preserves contiguous commit order.
Positive-sequence rows remain read-compatible for legacy databases, snapshot
restore, and repair tools. When both representations exist, a positive row has
the old physical-key overwrite precedence for its sequence; block deletion
walks the physical union so inverse keys from shadowed rows cannot leak. Direct
reads prefer a positive repair row and otherwise decode the block container.
No persisted delta chain or new consensus/state-root behavior is introduced.

A deterministic 128-row account-KV corpus reduced logical key-plus-value bytes
from 11,408 to 6,971 (38.89%). A 512-row encoding benchmark on Apple M1 Max
reduced publication time from roughly 143--146 us to 101--103 us, bytes
allocated from 184 KB to about 171 KB, and allocations from 3,072 to about
1,550. Production counters expose blocks, rows, encoded/logical bytes, avoided
Puts, and avoided key bytes; these local results select the implementation but
do not substitute for the deployment gate.

The first production canary restarted cleanly on the legacy-row database and
registered the new writer immediately. Two stable windows covered 5,024 blocks
and 172,977 transactions in 96.53 seconds (52.05 blocks/s, 1,792 tx/s, 34.43
transactions/block). They packed 1,711,225 logical rows into 5,063 block values,
avoiding 1,706,162 Puts (99.70%) and 59.72 MB of repeated physical keys. Against
the exact same packed payload plus those avoided keys, authoritative changeset
logical bytes fell 19.10%; this is an exact counterfactual, independent of LSM
queue state.

Three complete post-deploy family samples reduced the changeset/commitment byte
ratio from 0.922 in the preceding P4.41 samples to 0.624 (32.3% relative), while
commitment bytes remained stable at 555.94 bytes per coalesced operation. Final
coalesced output was 149 KB/block versus 184 KB/block in P4.41, but transaction
density also fell from 47.1 to 34.4, so this physical result is directional
rather than a matched-workload claim. The windows drained 948 MB of compaction
debt and recorded zero Pebble write delay, contract errors, canonical parallel
publication errors/mismatches, sender-retry errors/mismatches, or history-stage
interrupts. One independent sender-chain observer error fell back without any
state/receipt mismatch. Memory held was 4.6 GB with 128 goroutines. The
subsequent P4.43 deployment restarted after 70,859 uncompressed block packs
existed, resumed sync without rebuild, and processed their history/index tail
without error. That mixed-format restart completes the P4.42 production gate;
the cross-phase 30-minute and 24-hour soak gates remain separate.

#### P4.43: Benefit-gated hot changeset compression

Block ownership also creates a compression boundary that did not exist with
one value per mutation. The production Pebble profile deliberately leaves hot
levels uncompressed and enables Snappy only at the bottom level, so the P4.42
pack still enters blockbuffer, WAL, flush, and most compactions at about 148 raw
bytes per logical row. Repeated owners, typed-domain metadata, RLP framing, and
protobuf-like previous account images are compressible even when storage words
themselves are high entropy.

P4.43 wraps eligible packs in an unambiguous versioned Snappy envelope. Packs
below 1 KiB remain raw, and compression is retained only when stored bytes save
at least 12.5%, keeping small/incompressible blocks off the codec path. Readers
accept the new envelope, the uncompressed P4.42 container, and all positive-
sequence legacy/repair rows. They reject unknown envelope versions, corrupt
Snappy payloads, and decoded sizes above 128 MiB before allocation. Encode and
decode scratch buffers are pooled only when the persisted/decoded result owns
independent bytes; buffers above 4 MiB are left for GC rather than retained.

On a conservative 512-row corpus with high-entropy previous storage words, the
stored pack was 50.55% of raw. Publication took about 127--130 us versus
143--147 us for individual rows; compared with the uncompressed P4.42 pack it
adds roughly 27 us per block. Full-pack decode rose only from about 184--187 us
to 190--194 us (3--4%), while pooled decoded memory stayed near 382 KB versus
380 KB raw and added one allocation. The compressed writer retained roughly
187 KB and 1,551 allocations versus 184 KB / 3,072 for individual rows. These
numbers pass the local codec gate; stored/raw counters and a production window
must validate the actual mainnet mix and GC profile.

The production deployment restarted at approximately height 14,752,268 on the
mixed legacy-row/uncompressed-pack database and immediately resumed canonical
sync. Two stable windows covered 5,728 blocks / 217,265 transactions in 96.31
seconds (59.47 blocks/s, 2,256 tx/s, 37.93 transactions/block). All 5,697 new
packs passed the benefit gate and compressed 2,254,682 rows; zero packs stayed
raw. Stored payload was 126.65 MB versus 319.23 MB uncompressed (39.67%, a
60.33% reduction). Together with 78.71 MB of avoided per-row keys, actual pack
logical bytes were 68.14% below the exact individual-row counterfactual, and
99.75% of changeset Puts were eliminated.

Final coalesced output was 2,946 bytes/transaction, 32.1% below the P4.42
two-window canary's 4,339 bytes/transaction even though density was 10.2%
higher. Three complete family samples reduced changeset/commitment bytes from
0.624 in P4.42 to 0.204 (67.3%); commitment itself remained stable at 555.63
bytes per coalesced operation. Compaction debt drained 1.14 GB across the two
windows, so OS/compaction byte totals remain directional rather than a causal
codec measurement.

Both windows recorded zero Pebble write delay, contract errors, canonical
parallel publication errors/mismatches, sender-chain/retry mismatches, history
interrupts, or compressed-history errors. Held memory ended near 3.93 GB; a
later snapshot was 4.41 GB held / 1.74 GB used with 117 goroutines. Three
independent sender-chain observer errors occurred after the fixed windows and
fell back without a state/receipt mismatch; they are outside the history codec
path. Exact stored/raw counters, mixed-format restart, normalized family share,
and clean canonical windows pass P4.43. The longer resource/soak gates remain.

A subsequent deployment restarted after more than 31,000 compressed packs had
been published. The new process recovered height 14,788,223 and resumed from
the existing database. A post-restart 55-second window processed 3,072 blocks /
105,730 transactions (55.9 blocks/s, 1,922 transactions/s). All 3,101 new packs
were compressed; stored bytes were 40.8% of raw RLP and the exact packed form,
including avoided per-row keys, was 67.1% below the individual-row
counterfactual. Pebble write delay, history interrupts, and every result/write-
set mismatch stayed zero. One sender-chain observer fallback existed before the
window and did not increase. This completes compressed-format restart coverage.

#### P4.44: Lazy block-start execution views

A warm profile after P5.4 sampled 53.63 CPU-seconds over 20 wall-seconds. Even
with canonical parallel transfers disabled, `StateDB.Copy` consumed 4.86
CPU-seconds (9.06%): 3.14 seconds while preparing the sampled OCC block and
1.55 seconds while preparing its retry states. The copy serialized and decoded
every account retained in the four-block working cache, although almost all of
those accounts were clean and most speculative transactions never touched
them. This was observable safety-canary cost rather than useful canonical work.

Erigon execution workers share an immutable transaction/domain view and own
only their private changes. Go-tron's equivalent is the stable flat-latest and
blockbuffer parent view already shared by ordinary state copies. P4.44 adds a
narrow block-start copy path which starts with an empty account cache, eagerly
copies only the source `dirtyObjects` set, and hydrates clean accounts lazily
from that stable latest view. The ordinary `Copy` path retains its complete
deep-copy semantics for unrestricted RPC and debug callers.

The lifecycle boundary is explicit: all copy-owned execution must finish before
the canonical source publishes the block's latest-domain writes. At the current
call site the only normal pre-copy mutation is `writeHistoryBlockHash`; its
contract object is dirty and is therefore copied with the exact uncommitted
balance, account-KV, code, and storage overlay. The dirty set is already the
commit planner's complete per-block mutation superset. Tests cover lazy clean
rehydration, preservation of pending balance/storage writes, independent copy
mutation, ordinary-copy behavior, and race-checked sender retry/publication.

On Apple M1 Max, a 256-account block-start corpus with one dirty account reduced
copy cost from 251.6--254.6 us, 457.7 KB, and 1,048 allocations to 1.77--1.78 us,
3.5 KB, and 13 allocations: about 142x less latency and 99.2% fewer allocated
bytes. Production counters expose source, dirty, and omitted-clean object counts.
The deployment gate must confirm the expected omission ratio, remove
`StateDB.Copy` from the warm profile, preserve all observer/publisher equivalence
counters at zero, and measure end-to-end sync throughput independently of LSM
compaction state.

The production deployment resumed from the existing database after a roughly
14-second restart window and processed 30,247 blocks / 1,095,055 transactions
in its first 7.6 minutes. Excluding peer discovery, a 413-second window covered
27,808 blocks / 1,028,417 transactions (67.33 blocks/s and 2,490 tx/s). The
result is directional because transaction density and compaction debt differ
from the pre-deploy profile; the copy counters and profile provide the causal
measurement.

Across 468 sampled block starts, the source caches contained 8,641,095 account
objects (18,464 per copy on average); all were clean and omitted. Copy wall time
was 20.50 ms total, or 43.8 us per block start. All 2,510 speculative executions
and all 2,510 ordered applications matched canonical results, with zero
discard-worker, ordered-publication, or state/receipt mismatch. Four rare
sender-chain observer errors fell back safely; two occurred in the first 10,311
blocks and two over the next 19,936, with no associated equivalence mismatch.

At 25,959 session blocks, a warm 20-second profile sampled 54.56 CPU-seconds.
`StateDB.Copy`, `CopyBlockExecutionBase`, and `prepareTransferExecutionBlock`
had no samples, versus 4.86 CPU-seconds (9.06%) for `StateDB.Copy` before P4.44.
Commitment apply workers (24.60%), Pebble compaction (15.38%), signature-recovery
cgo (10.96%), commitment parent reads (9.73%), TVM (6.12%), and GC marking
(5.00%) are now the visible costs. Exact omission/copy timing, the warm profile,
continued sync, and clean equivalence counters pass the P4.44 deployment gate.

#### P4.45: Commitment fold shape telemetry

The warm P4.44 profile moved the next visible application bottleneck to
commitment folding: parent-state reads consumed 9.73% of sampled CPU and branch
node hashing 7.70%. Existing Erigon-aligned mechanisms already use a stable
one-snapshot parent view, reserved prefix cursors, adaptive branch caching,
eight-layer Bloom segments, and a bounded parallel root split. Replacing those
paths from aggregate CPU samples alone would risk optimizing the wrong shape.

Each fold now accumulates non-atomic local statistics for input/resolved update
counts, changed/error folds, wall time, parallel active splits/workers, branch hashes,
hash preimage bytes, and estimated Keccak permutation rounds. Parallel siblings
write isolated counters and merge only after their workers join; process metrics
are updated once per fold, so no atomic operation is added to recursive hashing.
The production sample will determine whether the next Erigon-style change should
target durable branch preloading, split scheduling/streaming, or hash work.

The production window covered 4,398 blocks and 108,095 transactions in 65
seconds (67.66 blocks/s and 1,663 tx/s). It recorded 4,417 changed folds with
1,090,100 resolved operations: about 247 operations per changed block. All 16
root splits were active on every changed fold. Per changed block, commitment
performed about 1,281 branch hashes over 555 KB of preimage and 4,341 estimated
Keccak permutations, taking 11.65 ms of aggregate fold wall time. Of 5.66
million branch hashes, 89.1% required multiple Keccak rounds. Parent branch
lookups resolved about 92% from the blockbuffer overlay, 3.9% from the branch
cache, and 3.6% from durable Pebble, so blanket durable preloading would target
only a small minority of reads.

A warm 20-second profile sampled 49.6 CPU-seconds. Commitment root workers
accounted for 19.74%, parent-session reads 8.45%, branch decoding 8.89%, node
hashing 6.33%, and Keccak 6.37%; Pebble compaction remained 15.18%. Commitment
used all 16 splits yet averaged less than one core over wall time because every
block waited for its slowest split before the next block's split work began.
Commitment errors and all canonical equivalence counters remained zero. These
measurements reject blanket preloading and select split scheduling/streaming as
the next experiment.

#### P4.46: Persistent ordered commitment lanes

Erigon's `StreamingCommitter` keeps top-nibble split state, schedules dirty
splits independently, and stitches their cells only at the ordered root
boundary. Go-tron's state capture currently materializes the final update batch
at block commit rather than emitting stable touches during execution, but its
bounded multi-layer blockbuffer provides a safe complementary overlap: keep one
persistent owner for each of the 16 first nibbles and let a lane begin block
N+1 as soon as that same lane has finished N. Other lanes may still be finishing
N, while root assembly and block publication remain strictly FIFO.

Each lane carries only its own root child and Keccak worker. A block receives a
fixed `LayerView`; its parent session exposes committed layers plus only older
in-flight layers, excluding the bound and newer layers. Per-lane FIFO ordering
therefore guarantees that an N+1 branch read observes N's finished same-prefix
write, while nibble-disjoint workers never depend on one another. The first
block after startup still uses the ordinary fold, retaining root validation,
snapshot repair, and full rebuild behavior before the persistent lanes are
seeded. Stores that do not advertise concurrent fixed-layer reads/writes retain
the serial fallback.

The scheduler owns at most `depth-1` jobs and uses an unbuffered foreground
handoff, so accepted jobs plus the newest executing layer never exceed the
blockbuffer's configured depth. Lane completion may be out of order internally;
metadata durability, head advancement, hooks, stage rows, and layer promotion
consume result channels in canonical block order. Any lane or publish error
poisons later work and enters the existing fail-fast discard/unwind path.
Canonical rewind increments a pipeline epoch after pending commits drain; the
first new-branch job validates and re-seeds all lane roots from the LCA view,
preventing an orphan tip from surviving in scheduler-local state. A
state-changing transfer-fork test covers this boundary rather than relying on
an unchanged-root empty-block fork.

The local 150-block/80-transfer memory-database comparison measured 165.3 ms
for synchronous insertion, 104.6 ms for the former asynchronous serial fold,
and 104.2 ms for the ordered lane pipeline. The 0.4% lane result is effectively
latency-neutral without Pebble read waits, while allocations fell from about
330.6K to 325.5K per run. The production gate must therefore establish the
causal value: pipeline enabled, maximum in-flight folds greater than one,
unchanged roots/equivalence, and reduced fold wall time or async backpressure
under real random reads. If neither improves, the lane scheduler should not be
credited with a throughput gain and within-block touch streaming remains the
next design step.

The production gate used a 99-second stable window covering 7,547 blocks and
215,155 transactions (76.23 blocks/s and 2,173 tx/s). Work shape remained close
to P4.45 at 249 resolved operations and 1,285 branch hashes per block. The
pipeline submitted 7,555 jobs, reached the configured maximum of three
simultaneous folds, and performed one measured fold per block instead of the
former root-presence fold plus update fold. Queue-inclusive commitment time was
7.48 ms/block, 35.8% below P4.45's 11.65 ms/block; async backpressure was 0.426
ms/block versus 1.24 ms/block, a 65.7% reduction. Parent reads resolved 92.88%
from overlay, 4.61% from cache, and 2.51% from Pebble.

Commitment and pipeline errors stayed zero. Ordered publisher, discard worker,
state/receipt, WriteSet, and balance-trace mismatches also stayed zero; one new
sender-chain observer error took its existing safe fallback. A warm 20-second
profile sampled 51.13 CPU-seconds: persistent commitment lanes accounted for
20.50%, Pebble compaction 17.76%, parent reads 8.31%, node hashing 8.02%, Keccak
8.55%, VM 6.42%, and GC marking 4.38%. In-flight utilization, lower fold time,
lower backpressure, continued sync, and clean equivalence pass P4.46.

#### P4.47: Erigon fastkeccak commitment hashing

After P4.46 removed the per-block split barrier, Keccak became the largest
individually replaceable commitment cost. The production work shape hashes
about 558 KB of branch preimage through 4,365 permutations per block, and 89%
of branch nodes need multiple permutation rounds. The commitment digest is
internal to go-tron's state root, so its bytes are consensus-sensitive even
though the implementation is not wire-visible.

Erigon uses `github.com/erigontech/fastkeccak`, with specialized amd64 BMI2 and
arm64 SHA3 assembly plus a portable legacy-Keccak fallback. Go-tron's pooled
hasher now uses the same implementation while retaining the destructive `Read`
path and zero-allocation buffers. Golden inputs at the padding boundary and
multi-round sizes compare every digest with `x/crypto/sha3` legacy Keccak.

On Apple M1 Max, a full 529-byte 16-child branch digest fell from about 1,417 ns
to 668 ns (2.12x), while a one-child digest fell from about 373 ns to 173 ns.
The complete no-op update fold improved from the P4.45 132--134 us range to
89.6--100.2 us over longer 100-iteration runs. The end-to-end in-memory async
benchmark remains execution-bound at about 104 ms/150 blocks, so production
must confirm reduced Keccak CPU and commitment wall/backpressure without any
root or equivalence mismatch.

The production binary selected fastkeccak's amd64 BMI2 assembly. A comparable
20-second warm profile reduced the flat commitment Keccak sample from P4.46's
4.37 CPU-seconds (8.55%) to 3.14 CPU-seconds (5.43%): 28.1% less absolute CPU
and 36.5% less profile share despite the later window doing more total work.
The 155-second gate imported 11,085 blocks / 353,761 transactions (71.52
blocks/s, 2,282 tx/s, 31.91 tx/block). It performed 11,077 measured folds at
about 254 resolved operations, 1,310 hashes, 569 KB of preimage, and 4,455
permutations per fold. All commitment, pipeline, ordered-publication, discard,
state/receipt, WriteSet, and balance-trace mismatch/error counters remained
zero; one sender-chain observer error used its existing safe fallback.

Fold time was 8.22 ms and async backpressure 0.578 ms per measured fold, both
higher than P4.46's unusually clean storage window. The profile explains the
difference: Pebble compaction rose from 17.76% to 26.83% and parent reads from
8.31% to 10.73%, while transaction density also increased. The gate therefore
credits P4.47 only with the directly observed hash-CPU reduction and clean byte
equivalence, not with an overall throughput gain.

#### P4.48: Binary indexed immutable commitment baseline

Erigon's deferred branch encoder does not leave sparse operands in the hot
latest domain: `ApplyDeferredBranchUpdates` merges each update with its prior
branch before `DomainPut`. Go-tron's 1.92-second output-bounded blockbuffer
already transfers that same last-writer-wins aggregation across roughly 93
blocks. Production still emits about 556 bytes per final commitment key because
the surviving value is a complete dense ancestor branch; another delta chain
or a longer Pebble batch would only move work onto random reads.

The next Erigon storage boundary is instead its immutable domain plus bounded
hot delta. A first lane-owned decoded-trunk prototype was deliberately rejected:
depths 1--3 added about 7.2 MiB of allocation per 150-block replay and made the
median in-memory ordered benchmark roughly 1% slower, while depth four added a
further 65,536 potential large `BranchData` entries. Copying a 1.3 KiB decoded
struct cost as much as the overlay lookup and decode it replaced.

The accepted first step replaces the production CommitmentBranch latest
snapshot's JSON/base64 document with the common immutable binary latest format.
Each build now emits a sorted `.seg`, ordinal `.lidx`, and sparse `.bt` B-tree;
the root prefix is encoded with a one-byte sentinel so every indexed key is
non-empty while nibble ordering is preserved. `Manager.GetCommitmentBranch`
uses the B-tree point-read path, while streaming iteration remains available
for bootstrap and repair. JSON reading remains confined to old/manual segment
fixtures; the fresh snap registry emits only binary files.

On 1,024 incompressible, dense 529-byte branch values, the old JSON file was
756,818 bytes. The complete binary family including both indexes was 562,664
bytes, a 25.7% reduction. More importantly, it establishes the indexed
immutable baseline required by the next step: retain only post-checkpoint
overrides/tombstones in Pebble, read misses from a persistently opened segment
view, merge bounded delta segments in the background, and clear hot branch rows
only after hash-bound publication and crash-safe coverage verification.

The read/write half of that boundary is now implemented. A fixed-size,
versioned `CommitmentBranchBase` marker binds an immutable branch snapshot by
txNum and commitment root to a non-zero hot generation. Marker-aware folds read
the generation delta first, treat an existing zero-length value as a tombstone,
and fall back to a persistently opened `.seg/.bt` point view only on physical
absence. The ordered 16-lane pipeline shares one concurrent `ReaderAt` view for
its full lifetime; serial folds close their view after use. Root, snapshot tx,
and B-tree availability are verified before activation, while an unmarked
database retains the original complete-hot-table path exactly.

All branch puts and sibling batches use the generation-qualified namespace.
Deletes over an immutable baseline write tombstones so cold branches cannot be
resurrected. Full rebuild invalidates the marker before removing the delta,
closes the cold view, and reconstructs a complete legacy table; mutable-state
reset removes every delta generation and the singleton marker. The
blockbuffer's fixed trunk/window cache recognizes the eight-byte generation as
schema rather than trie depth. Race tests cover 16 concurrent immutable reads,
multiple ordered in-flight blocks, tombstone shadowing, rebuild cleanup, and an
inverse-delta unwind followed by forward-root equivalence. Production counters
separate base opens, hot-delta hits, tombstones, cold hits, and cold misses.

The initial live publication half is now implemented. At the latest-snapshot
cadence, a short chain barrier drains ordered commits and durable flushes,
freezes the complete legacy branch table at the current canonical head, and
durably installs a `CommitmentBranchRotation` marker before import resumes.
The marker binds generation, txNum, root, block number, and block hash. All new
writes then target that generation's delta; misses fall back to the frozen
legacy table, while deletes remain explicit tombstones. Parallel folds reserve
independent cursor lanes for the delta and legacy physical prefixes.

The background builder streams the frozen table while blocks continue through
the delta. CommitmentRoot snapshot construction reads the rotation root rather
than the advancing live root. Completion requires the boundary to be
solidified and still canonical, verifies the published root metadata, opens the
indexed branch view, and independently derives its root branch. One atomic
batch then replaces the rotation marker with `CommitmentBranchBase`; only after
that marker is synced is the legacy table reclaimed. A crash before the swap
reopens `delta -> legacy`; a crash after it reopens `delta -> snapshot`, even if
legacy cleanup was interrupted. A non-canonical boundary invalidates the
rotation and rebuilds complete hot branches from authoritative latest rows.
The ordered pipeline epoch is invalidated at both marker transitions, and old
constructors can no longer silently write the ignored legacy namespace.

Integration coverage restarts in the first crash window, imports another block
while the snapshot is built, publishes the base, injects conflicting leftover
legacy data to model the second crash window, and proves that the immutable
snapshot plus live delta still derives the advanced root. Rotation starts,
resumes, solidification deferrals, rejections, completions, legacy hits/misses,
and delta/cold reads are separately observable.

Periodic next-generation merge is now implemented without returning branch
ownership to Pebble. Starting from an active base generation drains the ordered
pipeline, freezes that generation's bounded delta, and atomically redirects new
folds into the consecutive generation. Reads during the build resolve current
delta, frozen delta/tombstone, then immutable base. The builder pull-iterates the
old binary segment and frozen Pebble namespace in lexical order, applying delta
overwrites and tombstones while using bounded row memory; the new generation is
never included in the checkpoint being constructed.

After root, canonical-boundary, and solidification verification, the base
marker advances atomically and only the covered generations are reclaimed.
Both markers are a valid crash/restart state when their generations are
consecutive. A snapshot that was published before a solidification deferral is
recognized and retained on the next pass, so recovery does not require its
already-retired input base. Empty legacy cleanup is also detected before range
deletion to avoid adding a redundant Pebble tombstone on every later cycle.
Tests cover ordered current-over-frozen-over-base reads, frozen overrides and
deletes, streaming merge exclusion of concurrent writes, repeated publication
after deferral, second-generation promotion, old-delta reclamation, and live
root derivation from the promoted base plus retained new delta. Separate
frozen-delta hit, miss, and tombstone counters expose the periodic read path.
The remaining P4.48 work is the fresh snap-mode write-amplification production
gate.

#### P4.49: Shared version-value retry state

Erigon's `VersionMap` keeps typed values by logical path and transaction index,
and its versioned reader resolves the newest writer below the task index before
falling through to the durable state reader. Go-tron's async incarnation queue
already matched the scheduling half of that design, but each runner still
materialized the canonical prefix by applying every captured WriteSet to a
private `StateDB`. Moving that replay to the worker removed it from the serial
critical path without removing the duplicate work or mutable prefix ownership.

The block-local OCC carrier now stores immutable non-commutative post-images in
an append-only shared value map. Reads use strict floor semantics: a retry
frozen at transaction `n` can consume only writers `< n`, even while canonical
execution appends later versions concurrently. Account hydration composes the
latest full-account value with later typed scalar values and KV-generation
updates; exact account-KV point reads resolve through the same carrier. Local
task writes and sender-suffix forwarding remain StateDB-local and take
precedence. DynamicProperties and raw KV retain their existing exact request-
boundary snapshots, and range reads remain conservative OCC barriers.

Commutative settlement carriers are signed deltas rather than absolute values,
so they are deliberately excluded from shared floor reads. Their canonical
ordered-delta validation and publication path is unchanged. This prevents a
blackhole or fee accumulator delta from being mistaken for a post-image while
allowing a later ordinary absolute write to enter the shared map normally.

Retained sender-chain states are now immutable block-start templates. Each
incarnation creates a lazy dirty-only execution view in its background
goroutine, binds the shared reader to the request's fixed boundary, and then
executes the bounded sender suffix. The old worker-owned canonical-prefix
advancement path is no longer used by async jobs; it remains only for the fixed
synchronous reference cohort. If preexecution cannot retain a template, the
rare fallback stores an exact boundary copy on the queued request so runner
saturation cannot cause a later live-state copy.

Metrics under `core/versioned_shadow/shared_values/` report versions, unique
cells, reads, hits, misses, and skipped commutative deltas. Async metrics under
`sender_retry/async_actual/shared_state/` report job copies and errors. The
acceptance gate requires positive shared hits, zero async private-prefix
advances, zero shared-state/equivalence errors, and improved copy/replay cost on
a fixed dense production window before expanding beyond the Transfer family.

The first two post-deployment windows covered 159.37 measured seconds, 12,683
blocks, and 331,763 transactions (79.58 blocks/s and 2,081.7 transactions/s;
workload density 26.16 transactions/block). Across 2,492 shared-state jobs the
workers executed 5,149 tasks and published 1,094 retry results. They performed
177,769 version reads with 5,157 exact floor hits; private async prefix jobs and
advances both remained zero. Lazy job-state copies consumed 128.45 ms total, or
51.55 microseconds/job. Shared-state, async-retry, parallel-publication,
commitment-fold, and pipeline errors were all zero, as were TransactionInfo,
WriteSet, and BalanceTrace mismatches.

The same windows averaged 81.96 MB/s disk writes, 67.45 MB/s compaction input,
and 61.48 MB/s compaction output. Net estimated compaction debt increased by
only 77.3 MB and write delay remained zero. Commitment folding averaged 8.36
ms/block and async-commit backpressure 0.384 ms/block. A 20-second CPU profile
contained 59.93 CPU-seconds; the shared version-read subtree accounted for 0.09
CPU-seconds (0.15%) and did not enter the global hot list. The production gate
therefore passes the structural goal: canonical prefix replay is gone without
creating a shared-reader CPU hotspot or changing serial equivalence. The longer
canary and public-bandwidth gates remained separate at that point.

A later restart-free 182.40-second window completed the longer sender-chain
gate. The node imported 14,961 blocks and 423,871 transactions, or 82.02
blocks/s and 2,323.8 transactions/s at 28.33 transactions/block. Canonical
Transfer publication accepted 17,707 of 17,707 candidates with zero preflight
or publication errors. Async retry published 576 results from 4,218 shared-
state jobs while private-prefix jobs remained zero. TransactionInfo, WriteSet,
BalanceTrace, shared-state, retry, and canonical-publication mismatches/errors
all remained zero. Five independent sender-chain observer executions used the
existing safe fallback and did not enter canonical state.

#### P4.50: VM sender-chain execution canary

A fixed 100-block sample at the current historical-sync range contained 3,234
TriggerSmartContract transactions versus 146 TransferContract transactions;
TriggerSmartContract represented about 94.5% of all transactions. The existing
version observer also classified 204,316 of 922,486 VM transactions (22.15%)
as valid after explicit previous-sender ordering. VM execution is therefore the
first actuator-family expansion with material upside.

The design follows Erigon's separation between speculative execution and
ordered finalization. Erigon workers execute against versioned state without
sharing a mutable block gas pool; the ordered validator consumes gas, builds
receipt offsets, flushes writes, and prioritizes conflicted incarnations only
after a version-valid result reaches its transaction boundary. Go-tron's first
VM phase likewise remains observe-only. It must prove exact state and result
carriers before energy, bandwidth, receipt, and block-accumulator settlement
may replace canonical serial execution.

On every sampled 1/64 block, TriggerSmartContract and CreateSmartContract are
grouped into independent immediate-sender chains. A same-sender non-VM
predecessor breaks the chain because its state was not executed by that worker.
Each accepted predecessor forwards typed StateDB and DynamicProperties
post-images. Raw actuator writes use a three-level worker-local overlay:
current-transaction mutations, promoted sender-chain mutations, and the
immutable block-start parent. The promoted layer is cleared at the chain
boundary. Existing Transfer publication and retry code retains its prior
conservative raw-KV rejection until this new carrier passes production.

VM readiness accepts energy-bearing receipts, unlike the Transfer publisher's
zero-energy guard, but never makes a result canonical. At the real serial
boundary, the canary freezes the version verdict; after block execution it
compares the complete TransactionInfo, WriteSet, BalanceTrace, predecessor
acceptance chain, and exact raw/typed values. Public-bandwidth writes remain in
the VM comparison because no VM reservation/rebase carrier has been gated yet.
Metrics under `core/versioned_shadow/vm_sender_chain/` report blocks, groups,
executed and forwarded tasks, candidates, validated and forwarded-validated
results, read/sender conflicts, all three mismatch families, errors, and wall
time. Canonical VM publication and VM retry incarnations remain disabled until
this canary establishes their resource-order requirements.

The first restart-free production window covered 182.13 seconds, 13,936
blocks, and 405,331 transactions (76.52 blocks/s and 2,225.5 transactions/s).
Across 217 sampled blocks, the canary executed 5,212 VM transactions in 3,141
sender groups. Version validation admitted 984 candidates and full equivalence
accepted 841 (85.47% of candidates and 16.13% of executions), including 134
forwarded sender successors. All 143 rejected candidates had identical
TransactionInfo and BalanceTrace with a WriteSet difference; there were no
generic discard-worker mismatches. Twenty-eight unavailable executions fell
back safely. The canary consumed 812.15 ms total wall time, or 3.74 ms per
sampled block and 0.058 ms amortized per imported block. Canonical Transfer and
async-retry publication remained error-free.

A warm 20-second CPU profile contained 59.38 CPU-seconds. The combined
sender-chain/discard-worker focus accounted for 0.88 CPU-seconds (1.48%) and
did not enter the global hot list. The dominant process costs remained CGO,
fastkeccak, syscalls, memory movement, Snappy, and commitment assembly.

#### P4.51: VM mismatch attribution

The first canary's exact equality shape strongly implicates the two ordered
public-bandwidth cells, but canonical publication requires direct evidence.
The VM observer therefore splits candidate WriteSet mismatches into those
which become exact after excluding only `public_net_usage` and
`public_net_time`, and all other state mismatches. It separately counts valid
public-bandwidth reservations at candidate boundaries. Unavailable executions
are split into result, missing-info, WriteSet-capture, unsupported-applier,
applier-error, applier-mismatch, and family-readiness stages. These remain
diagnostic counters; no comparison exception or canonical VM publication is
enabled. The next production gate requires zero non-bandwidth state mismatch
before reusing the already-gated conditional public-net rebase and designing
ordered VM energy/block-usage settlement.

The attribution gate covered 122.56 seconds, 8,123 blocks, and 254,595
transactions (66.28 blocks/s and 2,077.3 transactions/s). Across 126 sampled
blocks the VM observer executed 3,408 transactions and admitted 592 version
candidates. Of these, 498 were already strict matches and all remaining 94
were public-bandwidth-only; other-state, TransactionInfo, and BalanceTrace
mismatches were zero. All 28 unavailable results were execution-stage failures
against the independent block-start/sender-chain view. WriteSet capture,
applier support, applier execution, and readiness errors were zero. Canonical
Transfer published 17,651 results without error during the same window.

#### P4.52: Transaction-boundary public-net projection

Classification proves that ignoring the two global cells recovers equality,
but canonical publication must reproduce their values and write-presence
semantics. Immediately after version validation and before serial execution,
the observer now evaluates each valid VM reservation against the canonical
`public_net_usage`, `public_net_time`, and limit at that exact transaction
boundary. It applies java-tron's recovery and limit formula in memory, records
whether admission succeeds and whether the baseline was rebased, and predicts
the ordered usage value. It predicts a time write only when the canonical
setter would change the current value; this preserves the serial WriteSet's
no-op omission rather than accepting a merely state-equivalent extra write.

After serial execution, the finish observer requires every non-public-net key
to remain byte-exact and compares the projected usage/time values, presence,
commutative flags, and widths with the canonical WriteSet. Metrics under
`vm_sender_chain/public_net/projection/` report candidates, admitted, rebased,
limit-rejected, matches, mismatches, and missing boundary projections. This is
still observe-only: neither the retained VM result nor canonical state is
mutated. The production gate requires every admitted projection to match and
zero missing/non-bandwidth mismatches before a VM publisher may reuse the
conditional reservation carrier.

The transaction-boundary projection gate covered 122.09 seconds, 8,915
blocks, and 258,375 transactions (73.02 blocks/s and 2,116.4
transactions/s). Across 140 sampled blocks the VM observer executed 3,474
transactions and admitted 741 version candidates. Of those, 618 were already
strict matches and the remaining 123 differed only in ordered public-bandwidth
cells. All 197 retained public-bandwidth reservations were admitted at their
exact serial boundary and all 197 projected WriteSets matched; 123 required a
rebased starting usage/time. Limit rejection, projection mismatch, missing
projection, other-state mismatch, TransactionInfo mismatch, and BalanceTrace
mismatch were all zero. The canonical Transfer/retry paths also reported zero
errors, establishing the public-bandwidth reservation as a safe ordered VM
carrier without enabling VM publication.

#### P4.53: Transaction-boundary block-energy projection

VM execution also changes the block-level adaptive-energy accumulator after
the transaction's state and receipt have been finalized. This write sits
outside the captured transaction WriteSet, so strict StateDB comparison alone
cannot prove that a retained VM result can reproduce the same block resource
boundary. The serial accumulator and observer now share one pure fork-aware
delta rule: before canonical execution, a version-valid retained VM receipt
projects the expected `block_energy_usage` post-image from the exact current
value; immediately after serial accumulation, the observer compares that
post-image before the next transaction can change it.

The projection preserves java-tron's two tiers exactly: adaptive energy off or
zero total usage is a no-op; before VERSION_3_6_5 only caller plus origin
stake-paid energy is added; after the fork the full `energy_usage_total` is
added. Storage is allocated only for the sampled VM cohort, and canonical VM
publication remains disabled. Metrics under
`vm_sender_chain/block_energy/projection/` distinguish final candidates,
observed and validated projections, exact matches, mismatches, and missing
boundaries. The production gate requires every observed final candidate to
match, with zero missing or mismatch, before this block-level carrier can join
public bandwidth in a small canonical VM publication cohort.

The block-energy gate covered 180.00 seconds, 11,580 blocks, and 414,376
transactions (64.33 blocks/s and 2,302.1 transactions/s, at 35.78
transactions/block). Across 180 sampled blocks the VM observer executed 5,415
transactions and admitted 1,108 final version candidates. Every candidate had
an observed and validated block-energy boundary, and all 1,108 projected
post-images matched serial accumulation; mismatch and missing counts were
zero. The public-bandwidth carrier independently admitted and matched all 219
reservations, including 142 rebased boundaries, with zero rejection, mismatch,
or missing projection. Strict state/receipt validation accepted 966 candidates;
the remaining 142 differed only in the already-proven public-bandwidth cells.
Other-state, TransactionInfo, and BalanceTrace mismatches remained zero.

The independent VM view had 25 execution-stage safe failures and no capture,
applier, or readiness failures. Canonical Transfer published all 14,156 formal
candidates without error; its async retry published 304/304 byte-exact
WriteSets, used 969 shared-state jobs, and performed no private worker-prefix
job or advance. VM canary wall time was 930.21 ms, or 5.17 ms per sampled
block and 0.080 ms per imported block. A separate warm 20.11-second CPU profile
recorded no sample in `projectBlockEnergyBoundary`, `blockEnergyUsageDelta`, or
`validateBlockEnergyBoundary`, so the added carrier remained below profiler
resolution. Both ordered resource carriers now satisfy the observe-only gate
for a deliberately small canonical VM publication cohort.

#### P4.54: Ordered VM canonical publication cohort

Erigon's parallel executor separates speculative execution from an ordered
apply loop: only a finalized, version-valid incarnation enters the publish
queue; the serial owner then advances block gas/receipt state, applies the
retained writes, and publishes indexes in transaction order. Go-tron's first
VM publisher follows the same boundary without widening the existing
eligibility model. Trigger/CreateSmartContract sender chains still execute on
independent workers, exact read-source validation still occurs at the
canonical transaction position, and only the ordered loop mutates canonical
StateDB, DynamicProperties, raw KV, receipt, and BalanceTrace state.

The initial cohort is deliberately sparse: only block numbers divisible by
1,024 may publish VM results, which is one of every sixteen existing 1/64 VM
samples. The other fifteen sampled cohorts retain serial execution as a live
reference. A candidate must pass complete result/applier readiness, exact read
versions including an actually published sender predecessor, conditional
public-net admission, full WriteSet preflight, and an unchanged block-energy
baseline. Publication then applies the retained typed/raw post-images, appends
the owned balance trace and TransactionInfo, flushes domain changes, and
advances `block_energy_usage` through the already-proven projected post-image.
Any pre-mutation rejection falls back to serial execution; an error after
mutation rejects and rolls back the whole block.

Metrics under `core/parallel_vm/` expose cohort blocks, preexecuted results,
candidates, publications, sender-chain publications, public-net
reservation/rebase outcomes, block-energy publications, each fallback stage,
fatal errors, and publication time. A three-transaction serial-equivalence
fixture interleaves two independent VM senders and returns to the first sender,
forcing one forwarded successor and two public-net rebases. All three retained
VM results publish while TransactionInfo, BalanceTrace, account balances,
public-net state, block-energy state, and the final StateDB root remain exact.
The production gate requires zero publisher errors, zero resource-carrier
fallback caused by a projection mismatch, continued exact sampled parity, and
an unchanged canonical block/receipt validation record before expanding the
cohort or adding VM retry incarnations.

The first publication window correctly preserved canonical state and receipts
but failed the stricter diagnostic gate. Over 246 seconds it published 78/78
VM candidates, including seven sender-chain successors and 15 public-net
reservations, with zero publisher/resource error. However, the independent
public-net observer reported eight WriteSet mismatches, exactly matching the
eight actual rebases. The ordered publisher had changed the usage value but
retained a worker `public_net_time` entry when the canonical setter would omit
that same-value write. Final state was equal, but WriteSet presence was not;
the window was therefore rejected rather than treated as success.

The conditional override now reproduces both ordered values and canonical key
presence. It temporarily inserts or removes `public_net_time` according to the
exact boundary's stored value, uses an independently owned encoded post-image,
and restores the worker's original usage/time entries after publication. This
also tightens the existing Transfer publisher. Unit coverage exercises both
directions (worker time write becoming a serial no-op, and an absent worker
time write becoming required), restoration, and a real three-VM publication
whose two rebases must remain exact in the sampled observer.

The corrected production gate covered 198 seconds, 14,554 blocks, and 456,548
transactions (73.51 blocks/s and 2,305.8 transactions/s, at 31.37
transactions/block). Fifteen publication cohorts preexecuted 363 VM results
and formally published all 62 admitted candidates. Seven were forwarded
sender-chain successors; all 15 public-net reservations published, including
eight actual rebases; all 62 block-energy post-images published. Publisher,
preflight, public-net, and block-energy errors/fallbacks were zero. Another
298 version-conflicted and three unavailable results safely remained serial.
Preexecution cost 3.26 ms per cohort and ordered publication averaged 87.3 us
per published VM transaction.

Across 229 independent sampled blocks in the same window, the observer
executed 5,579 VM transactions and admitted 1,116 final candidates. Block
energy matched 1,116/1,116 with zero missing or mismatch. Public bandwidth
matched 287/287, including 197 projected rebases, again with zero missing,
limit rejection, or mismatch. Strict comparison accepted 919 candidates and
the other 197 differed only in their correctly projected public-bandwidth
cells; other-state, TransactionInfo, and BalanceTrace mismatches were zero.
The 57 unavailable VM results were all safe independent-execution failures.
Canonical Transfer simultaneously published 28,128/28,128 candidates and
11,866/11,866 public-net reservations (8,214 rebased) without error; async
retry published 269/269 byte-exact WriteSets with no private-prefix work. The
small canonical VM cohort therefore passes its production parity gate.

#### P4.55: VM retry-incarnation observer

The first publisher deliberately leaves version-conflicted VM suffixes on the
serial path. Erigon instead schedules a new transaction incarnation against
the newest settled prefix, validates that incarnation, and gives earlier
retries priority before ordered apply. Before allowing VM retry publication,
go-tron reuses its existing sender-retry state machine as an observe-only
canary inside the unchanged 1/1024 VM cohort.

The retry engine is now policy-driven rather than Transfer-hard-coded. A
family supplies its complete readiness predicate and whether sender forwarding
may include raw KV. Transfer retains zero-energy readiness and conservative
typed-only forwarding; VM uses full result/applier readiness and the already
validated transaction/forwarded/parent raw-KV overlay. A conflicted VM
transaction with a remaining same-sender suffix is re-executed from an exact
canonical settled-prefix copy, its suffix consumes forwarded post-images, and
the newest incarnation is version-validated at each original transaction
boundary. It remains observer-only: the original 1/1024 publisher may publish
an already-valid source result, but recovered VM incarnations still wait for
serial execution and are compared afterward.

Dedicated metrics under `core/parallel_vm/retry/observe/` report blocks,
attempts, executions, candidates, validated/recovered results, mismatch/error
stages, prefix refresh/reuse/advance cost, execution cost, and projected
ready/late deadlines. A deterministic integration fixture inserts an external
balance change between three same-sender VM calls. The original latter two
results conflict; one settled-prefix retry rebuilds both, validates and
recovers 2/2 with zero Info/WriteSet mismatch, while the canonical publisher
still publishes only the unaffected first VM. Serial and canary runs retain
identical receipts, public/block resource state, and final StateDB root. The
production gate must demonstrate non-zero recovery with zero mismatch/error
and quantify synchronous copy/execution plus projected deadline shape before
moving VM retries onto the async shared-version scheduler.

The 2026-08-04 mainnet gate passed over an independent 230-second window at
heights 16,948,498--16,966,102: 17,604 blocks and 547,973 transactions, or
76.54 blocks/s, 2,382.49 tx/s, and 31.13 tx/block. Seventeen VM cohorts made
45 retry attempts and executed 232 new incarnations. All 126 candidates were
validated and all 126 recovered a result rejected from the original
block-start incarnation. Info, WriteSet, and BalanceTrace mismatches, retry
errors, and budget skips were all zero. The unchanged canonical VM publisher
simultaneously published 89/89 candidates with no error or resource/preflight
fallback; canonical Transfer retry published 715/715 with no error.

The synchronous measurement spent 38.736 ms copying 17 settled prefixes,
2.912 ms advancing 183 prefix WriteSets, and 55.005 ms executing the retry
suffixes: 96.654 ms total, 5.69 ms per sampled cohort, or about 5.49 us per
imported block. The strict no-wait deadline projection classified 10/126
(7.94%) recovered results ready, with 0.164 ms average slack, and 116/126 late,
with 0.395 ms average lateness. This is expected to classify the incarnation
at the conflict boundary as late because the conflict is only known at that
boundary; the useful non-blocking opportunity is in descendants rebuilt by
the same suffix job. The next gate therefore moves the observer to the actual
async shared-version scheduler while retaining serial fallback at every late
boundary, rather than permitting synchronous retry publication.

The independent 1/64 VM canary remained exact over 275 blocks: block-energy
projection matched 1,248/1,248, public-net projection matched 388/388, and all
266 apparent full-WriteSet differences were the already-accounted public-net
rebases with zero other mismatch. A 20-second CPU profile attributed 0.23% of
samples to the combined sender-retry family (including the existing Transfer
async scheduler); the synchronous `retryFrom` path itself accumulated only one
10 ms sample and was absent from the top hot functions.

#### P4.56: Actual async VM retry observer

The same 1/1024 VM cohort now moves retry execution off the canonical goroutine
without changing publication. VM sender-chain preexecution retains clean
block-start StateDB runners. At a conflict boundary the scheduler freezes only
the suffix's raw-KV reads, relevant version cells, and current dynamic
properties, then submits up to four same-sender incarnations to the existing
lowest-transaction-first retry queue. The worker copies a retained execution
base, reads canonical typed post-images through the shared version floor,
forwards typed and raw post-images inside its suffix, and returns versioned
results to the original transaction boundaries.

The canonical loop never waits at a boundary. Results already present and
version-valid become observer candidates; late or superseded incarnations stay
on serial execution, and finish waits only to reclaim block-scoped workers for
diagnostics. Retry publication remains disabled. Dedicated counters under
`core/parallel_vm/retry/async/` expose attempts/jobs, ready/late/stale results,
validated/recovered candidates, queue pressure, retained-runner capacity,
frozen raw/version input size, shared-state copy cost, execution/dispatch/finish
cost, and all mismatch/error stages. The integration fixture verifies the
shared-version path, retained runner ownership, complete result classification,
zero retry publication, and exact serial receipts, resource state, and final
StateDB root. The production gate requires non-zero ready and recovered results
with zero mismatch/error and enough queue/timing data to decide whether only
ready descendants should enter the canonical VM publisher.

The first production windows exposed a real storage-ordering hole before any
publication was enabled. A retry could obtain an exact `TransactionAccessStorage`
post-image from the shared VersionMap, but a retained sender-chain StateDB had
already executed and reverted an earlier incarnation. Journal replay restored
the block-start slot while deliberately retaining the object's dirty marker;
`CopyBlockExecutionBase` consequently copied that clean cached slot into the
async job, and `GetStateWithExist` accepted it before consulting the versioned
reader. The diagnostic signature was a same-key Storage value mismatch with
equal worker/canonical WriteSet shapes. Versioned storage reads now distinguish
task-local dirty slots from inherited clean cache entries: local and forwarded
writes still win, while each clean cached slot is checked once against the
frozen canonical prefix. A regression test recreates the execute/revert/copy
sequence and proves both the canonical override and subsequent task-local write
precedence.

The corrected 2026-08-04 mainnet gate passed on commit `96e4c7b6`. The fresh
process covered 100 widened 1/256 cohorts, 480 retry attempts, 477 jobs, and 820
returned incarnations. It classified 198 results ready, 562 late, and 60 stale;
27 boundary-ready candidates were all validated and all 27 recovered a result
rejected from the original block-start incarnation. Info, WriteSet, and
BalanceTrace mismatches were zero, including zero public-net-only and zero other
WriteSet mismatch. All ten error-stage counters (input, execution, contract-ret,
sender forwarding, missing info, WriteSet capture, apply unsupported, apply
error, apply mismatch, and finish) were zero, as were shared-state copy errors,
raw-view misses, invalid public-net carriers, and unsupported/delta-invalid
rejections. The 111 rejected on-time results were dominated by 109 exact read
conflicts; barrier counts overlap that total by design.

A 218-second suffix of that gate processed 11,200 blocks and 369,238
transactions, or 51.38 blocks/s, 1,693.75 tx/s, and 32.97 tx/block. Across the
whole fresh process the VM retry observer spent 372.012 ms executing, 51.167 ms
copying shared StateDB bases, 32.198 ms dispatching, 19.315 ms freezing relevant
version cells, 4.740 ms freezing raw inputs, and 6.304 ms waiting at block finish
only to reclaim observer workers. The independent canonical VM publisher
simultaneously published 137 results with zero block-energy, public-net, or
preflight fallback. This satisfies the P4.56 production gate. P4.57 may now
admit only descendants whose async result was already present and version-valid
at their own canonical boundary; late, stale, rejected, or incomplete results
must retain serial fallback without waiting.

#### P4.57: Boundary-ready async VM retry publication

Canonical publication is enabled first on `block % 1024 == 256`, one quarter of
the proven retry observer and disjoint from the existing block-start VM
publisher at `block % 1024 == 0`. The canonical loop performs only a
non-blocking event drain. `selectedResultForPublication` admits a result only if
the newest incarnation had already arrived at that transaction's boundary and
the exact read-version, sender predecessor, barrier, and commutative-delta
checks all passed. Because retry work is launched only after the conflict
transaction reaches its own boundary, this publishes descendants only; the
conflict transaction itself always retains serial execution.

Publication repeats both ordered resource gates against the live prefix.
Public bandwidth is recovered, limit-checked, and temporarily rebased through
the existing reservation override. Block energy is derived from the retry's
retained receipt and the exact current accumulator using the same
`blockEnergyUsageDelta` function as serial execution; the retry WriteSet must
not contain that out-of-transaction cell. Full StateDB/raw-KV preflight runs
before mutation. Any missing resource carrier, limit rejection, version
conflict, unsupported write family, late result, or unavailable result falls
through to normal serial execution without waiting.

Dedicated `core/parallel_vm/retry/async_publish/` metrics count selected blocks,
candidates, publications, block-energy/public-net/preflight fallbacks, fatal
publication errors, publication time, and post-publication WriteSet audit.
The deterministic integration fixture inserts unrelated canonical work between
the conflict and its VM sender descendant, proves the descendant is published
without a wait, and compares every TransactionInfo, public-bandwidth value,
block-energy value, balance result, and final StateDB root with serial
execution. The production gate requires non-zero publications and post-publish
WriteSet matches with zero mismatch, error, resource fallback, or preflight
fallback before the cohort is widened.

The 2026-08-04 mainnet gate passed on commit `3797a9b2`. Over 125 seconds it
processed 6,816 blocks and 233,298 transactions, or 54.53 blocks/s, 1,866.38
tx/s, and 34.23 tx/block. Eight retry-publication cohorts exposed seven
boundary-ready descendants; all seven were published and all seven
post-publication WriteSets matched. Block-energy, public-net, and preflight
fallbacks were zero, as were publisher errors, audit mismatches, the underlying
Info/WriteSet/BalanceTrace mismatches, and all ten retry error stages. Total
publication time was 486.951 us, or 69.6 us per result.

A separate 10-second CPU profile contained 25.88 CPU-seconds. The complete
sender-retry family accounted for 0.08 CPU-seconds (0.31%); asynchronous retry
execution accounted for 0.06 CPU-seconds (0.23%), while the canonical
publication path itself was below an individual 10 ms sample. The narrow gate
therefore adds no visible canonical CPU hotspot. The next widening may enable
the other two non-zero 256-block residues while retaining residue zero for the
independent block-start VM publisher.

#### P4.58: Widened async VM retry publication

Commit `bef2f18e` widened boundary-ready async VM retry publication from only
`block % 1024 == 256` to all three non-zero 256-block residues: 256, 512, and
768. Residue zero remains assigned to the independent block-start VM publisher,
so the two canonical paths stay disjoint while retry observation and
publication retain an exact 3/4 relationship.

The first fresh-process gate correctly failed before the widened cohort was
accepted. At 24 observer cohorts it reported one frozen raw-view miss and one
`contract_ret` error. The result never entered the publisher and canonical
execution fell back to serial, but a zero-error gate cannot treat safe fallback
as equivalence. The correlated counters and execution path identified a narrow
capability gap: TVM `BLOCKHASH` first probed a block body as generic raw KV. A
settled-prefix retry may legitimately take a different branch and request a
different in-window block number than block-start preexecution recorded, so
that immutable historical key was absent from the frozen raw set.

Commit `d1ca40a2` preserves only the canonical view's explicit
freezer-aware `BlockHashByNumberStrict` capability through the retry overlay.
TVM now prefers that strict capability before probing generic raw KV. The
wrapper is conditional: plain KV stores continue through the existing raw path,
and every other uncaptured retry raw key still fails closed rather than reading
the live block buffer. Repeated VM/core tests prove the strict reader is used
without a raw probe, the frozen execution DB retains it, and an arbitrary
uncaptured key remains rejected. The full suite, race tests, `go vet`, native
build, Linux/amd64 build, and the production cold-archive boundary audit all
passed.

The corrected 2026-08-04 mainnet gate passed on `d1ca40a2`. A fresh 180-second
window processed 10,065 blocks and 311,731 transactions, or 55.92 blocks/s,
1,731.84 tx/s, and 30.97 tx/block. It covered 40 retry-observer cohorts and 30
publication cohorts, preserving the exact 3/4 ratio. The scheduler accepted
222 attempts into 216 jobs and returned 363 incarnations: 85 ready, 266 late,
and 12 stale. Three results reached validation, one recovered an originally
rejected result, and the widened publisher selected two boundary-ready
descendants. Both were published and both post-publication WriteSets matched.

Info, WriteSet, and BalanceTrace mismatches were zero. All ten error-stage
counters were zero, including the repaired contract-ret path; frozen raw misses,
shared-state errors, invalid public-net carriers, publisher errors, audit
mismatches, and block-energy/public-net/preflight fallbacks were also zero.
Retry execution cost 147.109 ms total, shared StateDB copies 25.232 ms, dispatch
11.803 ms, version freezing 6.404 ms, raw freezing 1.797 ms, and finish-only
reclamation 2.113 ms. The two canonical publications cost 74.454 us total, or
37.2 us each.

A separate 10-second CPU profile contained 22.61 CPU-seconds. Actual async
retry launch work accounted for 0.04 CPU-seconds (0.18%). The complete sampled
discard-shadow family accounted for 0.48 CPU-seconds (2.12%), dominated by
0.43 CPU-seconds of the existing block-start sender-chain preexecution; the
canonical retry publisher remained below an individual 10 ms sample. This
clean 180-second window was sufficient to continue diagnosis but not to close
P4.58: the same process's longer window later reached more than 149 observer
cohorts and reported three execution-stage failures paired exactly with three
additional frozen raw misses. All 28 results that did enter the publisher still
matched their post-publication WriteSets, with zero publisher error or resource
fallback, but safe serial fallback does not satisfy the zero-error gate.

The BLOCKHASH fix therefore removed the original contract-ret signature but
did not cover every conditional raw dependency. P4.58 is reopened. The next
diagnostic records the last missing key's schema family, length, and first/last
eight bytes without weakening the frozen view. VM observation remains on the
residue-zero Transfer reference cohort until that key is classified and a
longer zero-error gate passes.

The diagnostic build `cae18568` reproduced the failure at observer cohort 87.
The missing key had length six and both captured words decoded to
`74 70 73 2d 27 ed 00 00`: `tps- || 0x27ed`, the RecentBlockStore/TAPOS ring
slot selected by a transaction's `ref_block_bytes`. TAPOS is a deterministic
envelope dependency, but the retry freezer previously learned raw keys only
from completed block-start read sets. A suffix transaction whose earlier
incarnation did not retain that read could therefore reach mandatory envelope
validation without its exact TAPOS slot.

The retry freezer now derives the schema-owned TAPOS key directly from every
suffix transaction and copies its value at the settled conflict boundary. It
does not preserve a live TAPOS reader: later parent changes cannot affect the
frozen value, and every unrelated uncaptured raw key remains fail-closed. A
regression fixture starts with no speculative read result, derives
`tps- || 0x27ed`, mutates the parent after freezing, and still reads the exact
boundary value. TAPOS rows are also classified as chain metadata by physical
write attribution instead of falling into `other`.

The TAPOS-frozen build `935c3154` crossed the prior cohort-87 trigger with zero
raw miss, confirming that repair. Its longer gate remained open: at observer
cohort 134, three results failed transaction contract-ret validation while raw
misses, shared-state errors, and Info/WriteSet/BalanceTrace mismatches stayed
zero. All 39 results admitted to the publisher still matched their audited
WriteSets with zero publisher error or resource fallback. The next diagnostic
retains no failed result but records the last mismatch's block, transaction
index, retry start, expected and actual contract-result enum, and transaction
hash prefix. This distinguishes wall-time OUT_OF_TIME behavior from a
state-dependent REVERT before choosing the next fix.

The enum diagnostic build `a3221732` reproduced the remaining signature at
block 17,668,864, transaction index 13, from retry boundary 11. The block
expected enum 2 (`REVERT`) while speculative execution returned enum 1
(`SUCCESS`); frozen-raw misses remained zero. This is not an OUT_OF_TIME path.
It is exactly the kind of outcome an Erigon-style optimistic executor may
produce from a stale prefix: the output itself can differ before its recorded
read versions have been checked against ordered predecessor writes.

Contract-result comparison is therefore no longer an execution-stage failure
for a structurally complete VM retry. The retry retains its read set and
post-images only long enough to run the exact transaction-boundary version
check. The ordinary readiness predicate remains false, so such a result cannot
forward writes to its sender suffix, become a selected publication candidate,
or reach the ordered publisher. Source preexecution is also required to pass
the same readiness predicate before it can suppress a newer retry.

The production counters now separate total contract-result mismatches into
`rejected_by_versions`, `version_clean`, `late`, `stale`, and `invalid` paths.
A version rejection is expected optimistic-conflict evidence. `late` and
`stale` results were never eligible at their boundary. `version_clean` means
the output differed despite an exact read-version match and remains a real
equivalence failure; `invalid` means the result was not structurally complete
enough to validate. The widened P4.58 gate therefore requires zero general
retry errors, frozen-raw misses, `version_clean`, and `invalid`, while allowing
non-zero mismatches only when fully accounted for by version rejection or
late/stale completion. Publication and post-publication audit requirements are
unchanged.

The fresh-process mainnet gate on `4a4ef529` closes P4.58. The accepted window
reached 261 retry-observer cohorts and 196 publication cohorts, matching the
expected 3/4 widened-publication ratio at the sample boundary. It scheduled
1,220 attempts into 1,211 jobs and returned 2,181 incarnations: 569 ready,
1,464 late, and 148 stale. Seventy-seven results reached boundary candidacy,
18 validated and recovered an
originally rejected source, and 59 were published. All 59 post-publication
WriteSets matched.

Five speculative contract-result mismatches were observed. Three were rejected
by exact read versions and two completed after their transaction boundary;
`version_clean` and `invalid` remained zero, so every mismatch belongs to an
expected optimistic terminal class. General retry errors, frozen-raw misses,
shared-state errors, Info/WriteSet/BalanceTrace mismatches, publisher errors,
audit mismatches, and all public-net/block-energy/preflight fallbacks were zero.
This window crossed both earlier failure points at observer cohorts 87 and 134.

Accumulated retry execution cost was 1,040.704 ms, shared StateDB copies
140.781 ms, dispatch 82.548 ms, version freezing 48.399 ms, raw freezing
13.642 ms, and finish wait 18.429 ms. The 59 canonical publications cost
3.550 ms total, or about 60.2 us each. A separate exact 181-second throughput
sample processed 11,136 blocks and 338,243 transactions: 61.52 blocks/s,
1,868.75 tx/s, and 30.37 tx/block.

A 10-second CPU profile contained 23.85 CPU-seconds. The complete sampled
discard-shadow/version family accounted for 1.17 CPU-seconds (4.91%), including
0.61 seconds (2.56%) in block-start sender preexecution and 0.18 seconds (0.75%)
in actual async retry launch work. Pebble compaction consumed 6.97 CPU-seconds
(29.22%), so storage compaction remains the materially larger CPU bottleneck.
With correctness, mismatch accounting, publication audit, and overhead gates
all clean, the held P4.59 co-scheduled canary may now be restored.

#### P4.59: First co-scheduled VM retry cohort

The next canary adds `block % 256 == 64` to VM retry observation and
publication. The existing residue-zero cohort runs beside the synchronous
Transfer reference; the new residue runs the VM and Transfer asynchronous
schedulers together. It remains disjoint from Transfer retry publication at
residue 192 and from the independent VM block-start publisher at
`block % 1024 == 0`. Therefore the first co-scheduling gate measures queue and
CPU interaction without yet admitting two retry publishers in the same block.

The deterministic publication fixture now runs at the new residue with a long
same-sender Transfer chain between the VM conflict and descendant. It proves
the co-scheduled path still publishes the boundary-ready VM result with exact
serial TransactionInfo, resource state, balances, and final root. Cohort tests
enumerate the new observation/publication residues and prove VM and Transfer
retry publishers remain disjoint over four complete 1024-block periods. The
integration test passed 20 repeated runs and three race-enabled runs; the full
suite, `go vet`, native build, and Linux/amd64 build also passed. The cohort was
not admitted: P4.58's longer window exposed the remaining frozen-raw miss after
this implementation was pushed, so the follow-up restores the residue-zero
baseline before deployment acceptance. Production admission remains blocked on
a longer P4.58 zero-error gate, followed by non-zero audited co-scheduled VM
publication, zero mismatch/error or resource fallback, and bounded VM/Transfer
queue and CPU growth.

After P4.58 closed, the residue-64 cohort was restored unchanged. VM retry
observation now runs at residues zero and 64 modulo 256. Publication includes
residue 64 and the residue-zero cohorts not reserved by the independent VM
block-start publisher at zero modulo 1024. Residue 64 co-schedules the VM and
Transfer asynchronous workers, but VM publication stays disjoint from the
Transfer publisher at residue 192. The production gate remains open until this
build returns non-zero audited residue-64 publication with zero correctness/
fallback failures and bounded queue and CPU growth.

The first fresh-process P4.59 window proved a real residue-64 publication: one
height interval containing a residue-64 VM publisher and no legacy residue-zero
VM publisher advanced the publication audit from 15/15 to 16/16. At observer
cohort 117 the aggregate publisher remained 31/31 with zero correctness,
frozen-raw, resource, or Transfer errors. It also exposed a diagnostics-only
terminal edge: a result-code mismatch may arrive while its incarnation is
current, then become stale when an earlier conflict invalidates the retained
sender suffix. The result was already unavailable to publication, but the
`stale` partition previously counted only results stale at arrival. Suffix
invalidation now classifies an available mismatch before incrementing its
incarnation, so total mismatches remain exactly partitioned without weakening
read-version or publication readiness.

The corrected production gate then ran through 319 VM observer cohorts and 279
publication cohorts. It produced 1,818 VM jobs and 112 canonical publications;
all 112 completed the post-publication write-set audit. Two contract-result
mismatches closed exactly as one version rejection plus one late result, with
zero version-clean, invalid, or stale mismatch. Frozen-raw, TransactionInfo,
write-set, balance-trace, publisher, resource, preflight, and Transfer errors
all remained zero. Transfer completed 9,031 jobs and 4,014 audited publications
without an error. Normalized Transfer queue-busy, maximum-depth, and dropped
rates improved about 5%, 10%, and 13% respectively from the P4.58 process;
VM queue growth remained bounded at small absolute rates.

Across an exact 181-second interval P4.59 imported 11,104 blocks and 321,388
transactions: 61.35 blocks/s and 1,775.62 transactions/s. Block throughput was
0.28% below P4.58 while transactions per block changed from 30.37 to 28.94, so
there is no measured light-block regression. A 10-second profile contained
25.96 CPU-seconds. The full discard-shadow/version family used 0.68 seconds
(2.62%), block-start preexecution 0.28 seconds (1.08%), and actual async launch
0.05 seconds (0.19%). Pebble compaction used 8.30 seconds (31.97%), making the
storage runtime the next measured cost after the co-scheduled retry gate closed.

### P5: Snapshot-first bootstrap and steady-state cold lifecycle

Erigon-class initial sync also requires avoiding execution from genesis when a
trusted snapshot is available. Complete the production path for signed catalog
distribution, resumable segment download, restore, recent-tail execution, and
automatic freezer/history build-merge-prune scheduling.

The hot Pebble database must reach a bounded steady state. Freezer, derived
indexes, and state history must keep up with import without monotonic compaction
debt or hidden hot-history growth.

The versioned production systemd template now selects `--prune.mode snap` for
the next fresh mainnet sync. The currently running test datadir is persistently
locked to `full`; changing its flag in place is intentionally rejected because
full mode has already pruned history that a genesis-covering cold build would
need. The operational gate must therefore install the template together with a
new empty datadir (preserving the current directory for rollback), then measure
the automatic v5 history build, merge, coverage-gated hot prune, and stage lag.
The deployment watcher only replaces the binary and restarts the installed
unit, so this datadir/service switch remains an explicit operator action rather
than an implicit destructive deploy step.

#### P5.1: Range-seek cold build source

An audit against Erigon's step-oriented `Aggregator.BuildFiles2` and
`InvertedIndex.collate` paths found a structural cold-build cost in go-tron.
Erigon builds a known step and seeks its tx-number cursor directly to that
step's lower bound. Go-tron's cold builder already computed an exact
`[startBlock, cutoffBlock]`, but discarded it before reading hot history. Each
5,000-block build then independently walked the complete `StateTxRange` prefix
for boundary discovery, record collation, tx-range counting, and tx-range
emission. Repeating that work as the chain grows makes cold construction trend
toward quadratic metadata scanning.

The cold builder now resumes boundary discovery at the hash-verified
`SnapshotBuild` stage plus one, and passes the exact block interval through the
domain registry into all four source iterators. `StateTxRange`'s big-endian
block suffix becomes the Pebble iterator start, and the physical upper bound
ends the scan before unrelated rows are decoded. The generic tx-number API is
unchanged and still scans conservatively; only a builder holding verified block
bounds may select the fast path. A lifecycle regression test proves that the
second segment's boundary discovery, record collation, preflight count, and
emission all seek from the prior published block rather than genesis.

On a Pebble database with 100,000 tx-range rows, reading the final 5,000-row
window fell from 22.1--22.9 ms and approximately 200,000 allocations to
1.10--1.13 ms and approximately 10,000 allocations, a 20x improvement matching
the 20x smaller logical scan. At a 15-million-block mainnet height and the
current 5,000-block build interval, the steady-state logical metadata scan is
about 3,000x smaller. The fresh snap-mode production gate must still measure
complete v5 segment build, compression, merge, prune, and import interference.

#### P5.2: Cold lifecycle production metrics

The cold lifecycle no longer depends on periodic log sampling to show whether
it can keep up with import. The runner publishes cumulative passes, errors,
segments built/compacted, and physical segment bytes under
`state/snapshot/cold/`. Its progress gauges separate the latest solidified and
eligible cutoff blocks from the per-pass selected cutoff and last published
block. Their difference is exported directly as `lag/blocks`; a bounded batch
therefore reports real backlog instead of appearing caught up merely because
the selected range completed successfully. Total pass, history-build,
compaction, and latest-snapshot durations are recorded independently in
nanoseconds.

The coverage-gated hot pruner publishes cumulative passes, errors, catch-up
skips, and deletion counts for tx ranges, domain-change blocks, commitment
checkpoints, and state-code rows under `state/prune/`, together with the last
solidified block and pass duration. These gauges remain registered in `full`
mode as a deployment regression signal; the fresh `snap` datadir adds the cold
builder gauges and is the production gate for build/merge/prune throughput.
Generated-byte accounting includes every newly written history/accessor/index
segment and any compaction outputs, so it measures actual lifecycle output
rather than logical source size.

#### P5.3: Continuous bounded backlog drain

Erigon's `Aggregator.BuildFilesInBackground` acquires a single background-build
slot and loops across every fully written step before starting its merge loop.
Go-tron's builder intentionally publishes smaller 5,000-block transactions,
but the lifecycle previously returned to a one-minute ticker after each one.
A restart with several ready ranges therefore added one minute of artificial
latency per segment even when storage and CPU were idle.

Each successful bounded pass now compares its published block with the
hash-verified eligible cutoff captured at pass start. While published coverage
is behind, the standalone builder and the production ordered lifecycle enqueue
one coalesced follow-up immediately. Every follow-up still executes the full
build -> atomic manifest publication -> coverage-gated hot prune sequence; no
parallel builder or unbounded work queue is introduced. A pass that publishes
nothing never requeues itself, so a missing/incomplete hot range falls back to
the normal maintenance interval instead of spinning. Shutdown is preferred
over queued catch-up work after the currently executing pass returns.

#### P5.4: Remove full latest-state scans from historical import

Unlike Erigon's incremental immutable-domain steps, go-tron's current latest
snapshot publication scans every latest-state key and replaces the prior file
family. Its 40,000-block cadence is roughly 33 hours at chain time, but only
about 13 minutes at a 50-block/s historical import rate. A genesis-to-85M replay
would therefore expose roughly 2,125 opportunities for a full-state rescan.

The intended startup guard also had a production wiring hole. `Runner.Start`
seeded the cadence watermark from the hash-bound stage or current solidified
head, but the composed `SnapshotLifecycle` owns the production goroutine and
never calls `Runner.Start`. The seed is now an idempotent `Prepare` phase shared
by both lifecycle forms, so restart and fresh-datadir startup cannot launch an
unplanned first-tick scan.

Production additionally forwards the sync service's active remaining-block
signal into the cold builder. When a latest build is due during an active sync
session, only that full-keyspace phase is deferred; bounded history build,
publication, compaction, and coverage-gated pruning continue normally. The
existing sync-complete hook requests another ordered lifecycle pass after the
importer becomes idle, at which point one latest snapshot is built at the
solidified watermark. `latest/deferred/sync` counts avoided scans and
`last/latest_build_block` exposes the seeded or published cadence boundary.

#### P5.5: Pebble v2 runtime canary and rejection

Erigon does not tune an LSM to retain the complete historical working set. Its
MDBX hot database is paired with step-oriented immutable domain/history files,
and the aggregator moves completed steps through build, merge, publication,
and pruning. P5.1--P5.4 implement that structural boundary for go-tron, but the
running full-mode datadir still profiled Pebble compaction at 31.97% of CPU.
Replacing the hot store with MDBX would combine a storage-engine rewrite with a
schema and transaction-model rewrite, so the next bounded experiment followed
current go-ethereum's Pebble v2.1.4 bridge instead.

The canary retained go-tron's measured memtable, target-file ramp, dynamic base,
transient-level compression, compaction concurrency, pooled batches, point-read
snapshots, and metrics. A legacy v1 database was ratcheted only to
`FormatFlushableIngest`, the oldest format understood by both Pebble v1 and v2;
the operation changed the manifest marker without rewriting SSTables. Tests
crossed the bridge, read through v2, and reopened the same database through v1,
making production rollback independent of a datadir restore.

The first v2 deployment restarted cleanly but exposed an API-semantic mapping
error. Pebble v1 treats zero-value L6 compression/filter fields as independent
defaults, whereas v2 inherits unset L1+ fields from the preceding level. L6
therefore inherited L5's no-compression and Bloom-filter policy. Against a
272-second immediately preceding v1 window with 45.32 transactions/block, the
first 200-second v2 window had 48.52 transactions/block but raised normalized
compaction input 70.44%, compaction output 121.06%, disk writes 89.28%, and
process CPU 9.91%. Estimated debt changed from draining 2.85 MB/s to growing
0.84 MB/s. The profile's Bloom construction and doubled output bytes identified
the mapping error; explicitly selecting Snappy plus `NoFilterPolicy` for L6
removed most of it.

The corrected runtime still failed admission. A 213-second window imported
8,608 blocks and 360,281 transactions (40.41 blocks/s, 1,691.46 transactions/s,
41.85 transactions/block). Relative to the nearby v1 window's 12,512 blocks and
567,031 transactions (46.00 blocks/s, 2,084.67 transactions/s), normalized
compaction input remained 13.85% higher, output 12.05% higher, Pebble-accounted
disk writes 11.80% higher, OS writes 6.33% higher, and process CPU per
transaction 16.43% higher. Throughput was lower by 12.15% per block and 18.86%
per transaction. Peer count rose from 17 to 20 and buffered blocks stayed above
2,000 throughout, so downloader starvation does not explain the result.
Compaction debt drained 1.78 MB/s and write stalls plus all execution,
publication, frozen-raw, resource, and equivalence errors remained zero, but a
10-second profile still attributed 31.70% of CPU to Pebble v2. This exceeds the
3% regression gate without reducing the target bottleneck.

Pebble v2 was therefore rejected and its runtime/dependencies removed. The
deployed shared bridge format remains directly readable by the restored Pebble
v1 code, so rollback needs no data conversion or reset. The result reinforces
Erigon's structural lesson: further compaction reduction should come from
moving immutable history out of the hot store and reducing logical hot writes,
not from a storage-engine version change alone.

The production rollback then reopened that same bridge-ratcheted datadir with
the Pebble v1-only binary. The process generation reset its async counters and
continued at block 18,079,331 with six syncing peers, 1,785 buffered blocks,
and no write/publication/state errors or restart loop. A 10-second profile
contained only `github.com/cockroachdb/pebble` frames and no `/pebble/v2`
module path. The same generation later reached block 18,121,210 after importing
42,155 session blocks and 2,048,149 transactions; it had recovered 30 syncing
peers, held 4,369 buffered blocks, and still reported zero async VM or
publication errors. This closes the rollback gate on the real datadir rather
than only the cross-version unit fixture.

#### P5.6: Immutable signed catalog hosting and resumable leases

The existing downloader already retained checksum-bound `.part` files and used
validated HTTP Range requests, but the hosting side was not generation-safe.
Each signed catalog pointed at mutable `manifest.json`. A runtime cold pass
could atomically replace that file after a client fetched the catalog but
before it fetched the manifest, causing a checksum race. After a later merge,
the retired-file lifecycle could also unlink a segment still referenced by a
download already in progress. Signature verification made both failures safe,
but not resumable or operationally reliable.

Catalog publication now copies the exact verified production manifest bytes to
`published/manifests/manifest-<generation>-<checksum>.json` before atomically
switching `snapshot-catalog.json`. The signed payload authenticates that
immutable relative path and checksum. Fetch writes the immutable object and
the local root view before publishing its local catalog, so a crash can leave
only harmless unreferenced downloads, never a catalog whose manifest is
missing. Legacy root-manifest catalogs remain read-only compatible.

Every published immutable manifest is also a segment-retention lease. Retired
file inspection combines the live manifest with all unexpired published views,
so a merge cannot reclaim an object needed by a resumable older download.
The history compactor applies the same lease set before its immediate obsolete
input cleanup; this is required because compaction previously bypassed the
periodic retired-file lifecycle. On first startup after upgrade, catalog
preflight also converts a valid legacy root-manifest catalog into an immutable
generation before any build or merge may mutate the root view.
Default GC preserves at least three catalog objects and a 24-hour grace; both
are explicit runtime flags. Expired manifest leases are removed before the
subsequent retired-segment pass, bounding obsolete storage without weakening
the current catalog.

An opt-in dedicated HTTP lifecycle serves only the signed catalog, immutable
published manifests, and exact active paths leased by those manifests. It has
no directory listing and does not expose mutable `manifest.json`, ETL files, or
unpublished data. GET/HEAD, ETag, immutable cache policy, and native Range
responses are covered by an end-to-end test which fetches and verifies a full
catalog through the real listener. Runtime bootstrap now uses that same
allowlisted listener in its regression, restores Pebble plus freezer, reopens
the chain at the signed boundary, and imports boundary+1; a separate two-node
P2P test syncs a restored node through its recent tail. The systemd template
accepts secrets through a protected optional environment file, and the Nginx
template preserves Range at `/snapshots/`. Official signer/key release remains
an operator artifact; no private key or unofficial trust root is compiled into
the binary.

#### P5.7: Erigon-aligned geometric history merges

The original cold-history selector treated every continuous binary history
file as one flat run. After each new 5,000-block leaf it could select the
already merged prefix together with the new tail and rewrite the complete
history again. Its default limit also multiplied a block count by a segment
count and then compared that value with a transaction-number span. Besides the
unit mismatch, repeated prefix replacement makes cold-file write amplification
quadratic as retained history grows.

Erigon instead assigns files to fixed aggregation steps. Its merge window ends
at the current step boundary, uses the rightmost set bit to select the largest
power-of-two-aligned range, and caps frozen files at 256 steps. Go-tron now
records the equivalent logical `aggregationSteps` on each history segment and
its inverted/accessor companions. A fresh 5,000-block cold build is one step;
the compactor sums the selected inputs. TRON transaction density can therefore
vary freely without using transaction numbers as a proxy for block-step size.

The selector now follows the same aligned-window rule and the same 256-step
default cap. A `2 + 1` layout is already stable; adding the next leaf selects
`2 + 1 + 1` directly as one 4-step output, so no intermediate 2-step tail is
written. A 256-step frozen prefix is excluded when two new leaves arrive, and
two complete 256-step files are never folded into an unbounded object. Missing
step metadata is interpreted as one leaf only for additive manifest decoding;
all new base and compacted writers stamp the field, and a companion whose step
count differs from its history payload cannot satisfy manifest validation.

This changes the structural upper bound from repeated whole-prefix rewriting
to logarithmic merge participation per leaf while preserving the existing
atomic manifest integration, published-generation leases, streamed history
copy, rebuilt indexes, and coverage-gated prune ordering. The fresh snap-mode
production gate still has to measure whether build plus geometric merge stays
ahead of sustained import and whether the 256-step cap is appropriate for the
mainnet segment-size distribution.

#### P5.8: Transaction-bounded base aggregation steps

Geometric merging bounds how often a leaf is rewritten, but its latency and
scratch-space bound still depends on the size of that leaf. Go-tron previously
cut every base history segment only at 5,000 blocks. Transaction density is not
constant across TRON history, so a dense 5,000-block range could make one cold
build, compression pass, and accessor construction many times larger than an
early sparse range. The compactor would remain logarithmic while individual
maintenance pauses and temporary files grew with the densest block window.

Erigon 3.4 defines one aggregation step as 390,625 txNums. Go-tron now applies
that value as a second base-step bound while preserving its 5,000-block cap.
The builder finds the first block whose end txNum reaches the target and always
includes that complete block; it never splits a TRON block merely to hit an
exact txNum. A single unusually dense block may therefore exceed the target,
while sparse history cannot expand beyond 5,000 blocks. Both searches use the
bounded physical `StateTxRange` iterator starting at the verified previous
publication boundary.

Every such base segment remains one logical `aggregationSteps` leaf. The txNum
bound controls peak collation/compression/index work and the block bound
controls metadata and derived-sidecar work; P5.7's power-of-two merger controls
long-run rewrite amplification. Regression coverage proves that a target
falling inside a block expands to its end, a block larger than the target stays
indivisible, immediate catch-up resumes at the next block, and manifest history
remains contiguous.

On a warm local Pebble database containing the maximum 5,000 candidate block
ranges at 100 txNums per block, selecting the block that contains txNum 390,625
took 0.926--0.936 ms across three benchmark runs. The additional bounded seek
is negligible beside segment collation and is executed only when the candidate
block range actually exceeds the txNum target.

#### P5.9: Single-pass history merge index construction

The first geometric compactor bounded rewrite amplification but still made
avoidable passes over every merge. It walked all source tx-range tables once
to count the merged rows and again to emit them. After streaming the record
payload into a new segment and compressing it, it decompressed the complete
output once for readability validation, a second time to reconstruct the
txNum index, and a third time to feed the sorted accessor builders. These
passes were bounded, but their CPU and read volume scaled with every merged
file and directly competed with canonical import.

The merge writer now reserves the tx-range count, streams and coalesces the
source tables once, then patches the final count in place. Its existing
transaction-ordered record writer is shared with cold collation: as each
already-decoded source record is re-encoded to v5, the writer emits the txNum
index entry at the exact logical output offset. Compression preserves those
logical offsets, so the index can be finalized beside the content-addressed
segment without reopening or decompressing that output.

This reduces tx-range source traversal from two passes to one and complete
merged-output record scans from three to two. The remaining passes have
different correctness or ordering responsibilities: the compressed-read
self-check prevents an unreadable file from reaching prune coverage, and the
accessor scan feeds two bounded ETL sort orders. Compaction remains one-record
bounded outside those collectors, and the same record writer validates global
tx/sequence ordering across source boundaries. The next independent step is
to feed the accessor collectors during this merge stream, after this smaller
single-pass-index change clears its build and production gates.

On the local Apple M1 Max benchmark, merging two compressed 8,000-record
segments (4,000 txNums total) improved from a five-run mean of 875.22 ms to
839.92 ms, a 4.03% reduction. Mean allocated bytes fell from 259.93 MB to
255.46 MB (1.72%), and allocations fell from 4.102 million to 3.970 million
(3.22%). Each sample used five fixed compactions; source construction was
outside the timed region, while merge, validation, accessor construction, and
manifest integration remained inside it.

#### P5.10: Streamed accessor collection during history merge

P5.9 left one avoidable full record pass: after validating the compressed
history output, compaction reopened it and decoded every record again to feed
the v4 exact-hash and owner/domain-prefix accessor sort orders. Erigon keeps
post-compression accessor construction as a distinct correctness phase; the
go-tron format still retains that rebuild path for cold collation, repair, and
cross-checking. During compaction, however, every source record has already
been decoded and its final logical output offset is known before the v5 frame
is written, so rereading the output adds no information.

Accessor v4 collection is now a reusable pair of bounded ETL collectors.
Normal cold construction can populate it by scanning the finished history
segment exactly as before. The compactor instead sends each decoded change,
logical offset, and record index to those same collectors immediately before
the shared ordered record writer emits the frame and txNum index. Once the
content-addressed segment path is known, the collectors produce the unchanged
v4 companion layout. The compressed-read self-check remains independent and
mandatory, so pruning still cannot trust a segment which the production reader
cannot decode.

A regression rebuilds the accessor from the final compressed output through
the old scan path and requires it to be byte-identical to the streamed result;
twenty consecutive runs passed. This reduces complete merged-output scans from
two to one without changing source verification, ETL ordering, manifest
publication, or the compressed history format. The 16,000-record warm-cache
microbenchmark could not resolve a repeatable wall-clock difference between
P5.9 and P5.10, so no timing gain is claimed from that small local sample; the
admitted result is the eliminated full decode pass plus byte-equivalent output,
subject to the fresh snap-mode large-file production gate.

#### P5.11: Streamed accessor collection during base collation

Every new 390,625-txNum aggregation leaf used the same redundant accessor pass
as compaction. The history record ETL first sorted hot changes into canonical
tx/sequence order and wrote the uncompressed logical segment plus txNum index.
After compression, the builder reopened that segment and decoded every record
again solely to populate the two accessor ETL orders. Unlike a geometric merge,
this cost occurs for every base leaf, so it directly determines whether the
cold lifecycle can remain ahead of sustained import.

The shared ordered record writer now owns an optional accessor collector pair.
Cold collation and compaction both attach the same v4 collectors, so the writer
validates ordering and range first, records the exact logical offset/index, and
then emits the history frame and txNum index. The post-compression accessor
rebuild function remains available as an independent repair and verification
path, but it is no longer on either normal write path. The mandatory finished-
segment check still decodes the compressed output before manifest publication.

The existing forced-spill cold-build test now exercises this path with a one-
byte ETL buffer. It verifies companion coverage and additionally requires the
streamed accessor to be byte-identical to an independent post-compression
rebuild; both that case and the compaction equivalent passed twenty consecutive
runs. Base-leaf output format, manifest ordering, compression, and prune gates
are unchanged. Large-file wall-clock and lifecycle-lag impact remains assigned
to the fresh snap-mode production gate.

#### P5.12: Single companion validation gate per merge input

Compaction source discovery independently ran the structural/checksum checker
for each history, txNum index, and accessor file, then immediately called the
cross-companion verifier. That verifier deliberately starts by invoking those
same three registered checks before proving index and accessor coverage against
the history payload. Immutable inputs were therefore fully hashed and decoded
twice before any merge output could be written.

Source discovery now calls only the cross-companion verifier. No validation is
removed: physical checksums, individual file structure and ordering, history
readability, index coverage, exact accessor coverage, and prefix-group coverage
all remain prerequisites to opening the input for copy. This mirrors Erigon's
separation between one static-file integrity gate and the subsequent merge,
while preserving go-tron's stronger companion coverage proof.

All compaction rejection tests, including an accessor whose internally valid
entry points at the wrong history record, passed ten consecutive runs. The
same 16,000-record local benchmark moved from a five-run mean of 798.27 ms to
790.08 ms (1.03%); the ranges overlap, so this is directional rather than an
admission claim. The deterministic result is removal of one checksum/structure
pass over all three source files per merge input.

#### P5.13: Fold history structure validation into index coverage

After P5.12, companion verification still decoded the complete history twice.
The registered history checker first validated every frame, tx range, tx/Seq
order, and logical end. The immediately following index-coverage proof then
read every record again in the same physical order to prove contiguous offsets,
record counts, and txNum ownership. Only the first pass checked Seq ordering and
the final logical offset, even though both values are already available in the
second pass.

The verifier now keeps the physical SHA-256 check as an independent immutable-
object gate, then performs record structure and index coverage in one sequential
logical scan. Each index entry must begin at the exact next record index and
offset; every decoded record must match the indexed txNum; Seq must be
nondecreasing within that txNum; the total record count must match the history
header; and the final decoded offset must equal the segment's logical size.
The registered standalone history checker remains unchanged for callers which
do not also verify companions.

Two adversarial legacy-v2 fixtures update their checksums and sidecars so they
cannot be rejected by object identity alone. One regresses Seq from 2 to 1
inside a txNum and the other appends a logical trailing byte; both are rejected
by the fused coverage pass in twenty consecutive runs. Compressed v5 and legacy
readers still share the same logical-offset interface. The small warm-cache
benchmark again overlapped its baseline, so the admitted claim is one fewer
complete input-history decompression pass, not a local wall-clock percentage.

#### P5.14: Fold index structure validation into coverage

The txNum index had the same remaining two-pass shape. Its standalone checker
hashed the file and scanned every entry to validate range and strict txNum
ordering; companion coverage then reopened the index and consumed every entry
again while proving record index, logical offset, count, and history ownership.

Companion verification now keeps a checksum-only immutable-index gate and
enforces the range and strict-order invariants in the coverage loop before an
entry may direct a history read. Header, physical size, version, and declared
entry count remain validated by the common index opener. Standalone callers
still receive the original full index checker.

An adversarial v2 fixture splits one valid two-record txNum group into two
separate consecutive index entries. Their offsets, record indices, counts,
history payload, accessor, sizes, and checksums are all internally consistent;
only the required one-entry-per-txNum ordering invariant is violated. The fused
coverage verifier rejected it in twenty consecutive runs, alongside the P5.13
sequence, trailing-byte, and checksum cases. This removes one complete index
entry-table pass per verified merge input without weakening its format gate.

#### P5.15: Treat merge sidecars as derived inputs

The remaining compaction profile was dominated by validating source sidecars,
not by writing the merged segment. Before a merge, the full companion verifier
walked history in physical order through the txNum index, then followed the
accessor's hash order back into random compressed history blocks, repeated that
for KV groups, and finally scanned history again for group coverage. On the
16,000-record benchmark this accounted for about 70% of sampled CPU time and
most `pread` calls. Erigon's merge path instead opens immutable segment inputs,
streams their canonical history, and builds new derived accessors from the
merged stream.

Compaction now keeps SHA-256 identity checks for history, index, and accessor;
validates companion format, range, and record-count headers; and then treats
history as the sole canonical merge input. The existing sequential copy still
decodes every record, enforces its logical end, streams tx ranges, and builds a
fresh txNum index and v4 accessor. A structurally valid but semantically wrong
old sidecar is therefore repaired rather than used to direct merge reads. Full
cross-file coverage verification is unchanged for snapshot fetch/install,
explicit manifest verification, and the hot-prune safety gate.

An adversarial test mutates a v4 exact accessor entry to another in-range record
and refreshes its checksum. Full companion verification rejects the source,
while compaction rebuilds correct companions whose full verification succeeds;
the test passed ten consecutive runs. Across five local benchmark runs, mean
time fell from 767.73 ms to 240.21 ms (3.20x), mean allocations from 3,350,938
to 779,569 (-76.7%), and mean allocated bytes from 229.93 MB to 114.93 MB
(-50.0%).

#### P5.16: Buffer sequential history-family emission

After removing random source-sidecar reads, half of the remaining profile was
individual `write(2)` calls. The history record writer emitted one frame per
file write, the txNum index emitted one entry per write, and the v4 exact/group
ETL appliers repeated the same pattern. Erigon's segment compressors instead
consume buffered sequential streams and flush at file/publication boundaries.

History, txNum index, v4 exact, group-offset, and group-payload output now use
256 KiB buffers. `Finish` flushes history and index before the index-count
backpatch, and flushes exact/group streams before `Sync`, concatenation, hashing,
and rename. Group record counts still use an in-place fixed-width backpatch, but
the payload flush occurs once per logical group rather than once per record.
This keeps ETL memory bounded and preserves the existing crash-publication
ordering.

The dominant v5 decoder now parses its compact layout directly and lets Key and
Prev reference the immutable frame payload owned by the returned change. The
private record writer retains that decoded row only until the next adjacent
order comparison, avoiding a second deep copy of every variable-width field.
A new test covers field round-trip, every truncated payload boundary, invalid
boolean encoding, and trailing bytes. Existing forced-spill tests continue to
prove byte-equivalent v4 accessor output, and the compaction/repair sequence
passed ten consecutive runs.

Across five local runs of the same 16,000-record benchmark, mean compaction time
fell from the P5.15 baseline of 240.21 ms to 143.20 ms (1.68x), mean allocations
from 779,569 to 509,371 (-34.7%), and mean allocated bytes from 114.93 MB to
109.60 MB (-4.6%). Combined with P5.15, the measured local compaction path is
5.36x faster than the pre-P5.15 baseline; sustained-import and soak gates remain
production acceptance work rather than a benchmark inference.

#### P5.17: Defer catch-up merge levels

The geometric selector bounded each leaf's rewrite depth, but the lifecycle
still invoked one merge immediately after every base build. During a fresh
sync, that eagerly emitted partial 2/4/8/... ranges even though the builder was
known to have a large backlog. Erigon first collates all ready aggregation
steps and then runs its merge loop, allowing a complete frozen range to be
selected without materializing those transient levels.

While a cold-builder pass still has historical catch-up work, history
compaction now requires the configured maximum logical span (256 steps by
default). The base segment is still atomically published before hot pruning,
so crash recovery, manifest leases, and prune coverage do not change; only
intermediate merge output is deferred. Once the builder has no immediate
backlog, the runner repeatedly applies the normal aligned selector until every
eligible range is drained rather than leaving one merge per maintenance
interval.

The deterministic 1,024-leaf shape rewrote 4,608 logical steps through eager
compaction and now rewrites exactly 1,024: a 4.5x reduction in compaction
output. Including the unchanged 1,024 base-leaf writes, total logical history
emission falls from 5,632 to 2,048 steps (2.75x). Lifecycle regressions verify
that the first three catch-up leaves remain loose, the fourth directly forms a
four-step test frozen file, trailing leaves remain visible, and a caught-up
runner drains two independent ready ranges in one pass. New counters expose
total merge passes, catch-up deferrals, and last-pass merge count for the fresh
snap-mode production gate.

#### P5.18: Single-pass base tx-range emission

Every cold base step previously opened its bounded hot `StateTxRange` source
twice: once to pre-count rows for the history record offset and again to emit
the table. This repeated Pebble iterator creation, RLP decoding, row allocation,
and validation for every frozen batch. Erigon's step collation consumes each
source once and derives file metadata while writing it.

The base builder now writes a zero count through a 256 KiB sequential buffer,
streams and validates each bounded range row once, flushes, and backpatches the
final count in the fixed header slot. The emitted count then determines the
unchanged first-record offset used by the txNum index. Maximum-size checks,
record ordering, companion verification, checksums, atomic rename, manifest
publication, and hot-prune coverage remain unchanged. Regression instrumentation
proves the production block-bounded tx-range callback is entered exactly once.

Across five local runs of a 5,000-block base-step benchmark, mean build time
fell from 269.48 ms to 258.96 ms (-3.9%) and mean allocations from 80,917 to
65,902 (-18.6%). This first fixture used the in-memory database; a subsequent
allocation profile attributed 97% of its approximately 1.036 GB cumulative
allocation to per-block iterator snapshots, not to ETL buffer reservation.

#### P5.19: One changeset iterator per base step

The production block-bounded history collation still called
`IterateStateDomainChanges` separately for every `StateTxRange` row. A default
5,000-block base step therefore opened, sought, and released as many as 5,000
Pebble/overlay iterators even though the physical changeset key is already
ordered by block and sequence. Erigon collates an aggregation step with bounded
ordered cursors and joins corresponding streams without rebuilding cursor
state per block.

The bounded block/tx iterator now first streams eligible transaction ranges
into a compact ordered `(blockNum, blockHash)` slice, then walks the complete
changeset block interval with the existing single range iterator. A monotonic
slice cursor attaches the authoritative block hash and filters the txNum window
without a hash table or random lookup. Packed-block decoding, positive-sequence
repair overwrite precedence, callback cancellation, corrupt-row propagation,
and output ordering remain those of the shared logical block-range reader.
Regression instrumentation requires zero per-block changeset iterators and
exactly one range iterator.

On real local Pebble, the sparse 5,000-block/one-change case remained within
the 3% light-workload gate (about +1.8% in the final sample) while allocations
fell about 35.9%. With one change per block, mean build time fell from 74.30 ms
to 67.54 ms (-9.1%) and allocations fell about 6.2%. The in-memory fixture's
former O(blocks-squared) iterator snapshots also disappeared, but that larger
synthetic gain is not treated as a production throughput claim.

#### P5.20: Direct ordered base-record emission

After the iterator join, each base history record still made a complete ETL
round trip. The builder encoded every decoded hot row into an ETL value, built
and copied a large sort key, wrote a temporary run, read and allocated both
fields again, decoded the value, and finally re-encoded the same row into the
history segment. Erigon's aggregation collation writes an already ordered step
stream directly to its compressor and builds derived indexes from that stream.

Fresh go-tron block packs are produced by monotonically increasing transaction
ordinals and mutation sequences. `WriteStateDomainChangeBlockRows` now makes
nondecreasing txNum an explicit physical invariant in addition to its existing
contiguous-sequence requirement. The base builder writes a zero record count,
streams the bounded hot iterator directly through the shared history/index/
accessor writer, flushes, and backpatches the final segment and txNum-index
counts. Key-ordered exact and group accessors retain their bounded ETL because
their order is independent of execution order.

The writer checks the complete canonical comparator between adjacent rows.
Fresh block-pack publication rejects disorder. If existing positive-sequence
legacy or repair rows violate that invariant, the builder discards the partial
temporary direct output and reruns only that range through the bounded record
ETL before publication; malformed output still cannot be published or pruned.
Tests prove the ordered path does not instantiate record ETL, cover the legacy
fallback, hot block-pack rejection, forced accessor spill, compressed and
uncompressed output, companion verification, and lifecycle publication.

Against the independently checked-out P5.19 binary on local Pebble, five
10-iteration sparse runs fell from 51.08 ms to 48.21 ms (-5.6%) and allocated
bytes fell 18.3%. Dense runs fell from 68.44 ms to 63.37 ms (-7.4%), allocated
bytes from 30.40 MB to 21.46 MB (-29.4%), and allocations from 300,750 to
200,655 (-33.3%).

#### P5.21: Trusted derived-file build validation

After collation completed, the base builder linearly reread all three finished
files before returning. The history pass is necessary because hot pruning
trusts manifest coverage: it exercises compressed blocks, record framing, and
the tx-range hydration path that becomes the only cold copy. The index and v4
accessor passes, however, repeated invariants already enforced while writing:
the history writer emits a strictly increasing txNum index with exact counts,
and the bounded ETL writers reject unsorted exact/group rows and count or layout
overflow before their files are finalized.

Erigon similarly treats successful collation/build writers as trusted and opens
their finished artifacts for the next build/integration stage instead of
replaying every derived entry immediately. go-tron now retains the complete
history replay, then reopens the new index and accessor to verify immutable
size, range, header count, version, and complete fixed layout in O(1). The
standalone `VerifyManifestFiles` path and companion semantic verifier remain
unchanged for imported, repaired, or later-audited files; only redundant checks
inside the same trusted build transaction are removed.

Against an independently checked-out P5.20 binary, five local Pebble runs of
the sparse benchmark were unchanged at 49.98 ms (+0.01% noise) while allocated
bytes fell 1.1%. Dense runs fell from 63.47 ms to 57.09 ms (-10.0%), allocated
bytes from 21.46 MB to 20.96 MB (-2.3%), and allocations from 200,658 to 185,622
(-7.5%).

#### P5.22: In-memory final ETL buffer

The bounded ETL collector previously forced its last buffer through a temporary
run even when collection never reached the configured memory limit. A 5,000-row
base step therefore sorted the exact and group accessors, wrote each as a run,
allocated 1 MiB buffered readers, decoded and allocated every run entry again,
and immediately deleted the temporary files. This behavior made the external
merge path mandatory for inputs that were already safely bounded in memory.

Erigon's collector calls `flushBuffer(true)` at load time and uses `KeepInRAM`
when no earlier data provider exists. go-tron's generic ETL now follows the same
rule: a never-spilled final buffer is sorted and duplicate-collapsed directly
into the existing applier; if any prefix already exceeded `BufferLimit`, the
remaining buffer is still spilled and all runs are merged exactly as before.
Interruption stats remain retryable, successful loads release the retained
rows, and forced-spill plus mixed disk/final-buffer regressions preserve the
bounded-memory contract.

Against an independently checked-out P5.21 binary, five local Pebble runs of
the sparse base build were unchanged at 48.28 ms versus 48.18 ms (-0.2%) while
allocated bytes fell from 9.32 MB to 5.12 MB (-45.1%). Dense runs fell from
56.39 ms to 51.14 ms (-9.3%), allocated bytes from 20.96 MB to 15.31 MB
(-27.0%), and allocations from 185,624 to 145,532 (-21.6%).

#### P5.23: Ownership-transfer ETL frames

After the final-buffer fast path, accessor collation still allocated each exact
or group sort key and value, passed them to ETL, and immediately cloned both
slices even though the producer never reused them. Dense base steps paid four
avoidable clones per state change before sorting. Erigon's collation buffers
form one owned representation of derived entries rather than retaining both a
producer copy and a collector copy.

The collector now exposes an explicit `PutOwned` contract for freshly allocated
final frames. State-history fallback records and v4 exact/group accessors
transfer their sort keys and values through it, and the escaped accessor sort
key precomputes its exact capacity so embedded zero bytes cannot trigger a
second growth allocation. Ordinary `Put` still copies key and value exactly as
before; a generic derived-index benchmark rejected a proposed combined-copy
change when Go allocation size classes made it slower, so unrelated ETL users
retain their measured baseline.

Two alternating runs of precompiled P5.22 and P5.23 benchmark binaries, each
with 50 iterations, kept the sparse build within the 3% gate at 48.07 ms versus
48.49 ms (+0.9%). Dense time fell from 53.87 ms to 52.79 ms (-2.0%), allocated
bytes from 15.31 MB to 14.02 MB (-8.4%), and allocations from 145,530 to 130,528
(-10.3%).

#### P5.24: Ordered parallel cold-history compression

Cold history conversion still encoded every independent 16 KiB Zstd block on
the builder goroutine. The format already makes those blocks independent, but
their table entries and compressed payload must reach disk in source order so
existing uncompressed accessor offsets remain valid. Erigon's paged segment
writer solves the equivalent constraint with a bounded worker queue followed by
an ordered reducer.

go-tron now uses the same producer / worker / reducer shape for large history
files. At most `workers*2` source chunks are in flight, workers share the
concurrency-safe stateless encoder, and one reducer appends completed frames by
sequence number. The worker count is `min(GOMAXPROCS, 4)`, limiting both memory
and competition with Pebble compaction. Files below 1 MiB retain the direct
single-worker path: measured 512 KiB inputs did not amortize goroutine and queue
setup, while 2 MiB inputs did. Serial and four-worker output is byte-identical
across empty, partial, exact-boundary, and multi-block cases; race coverage also
exercises the shared encoder.

Five local 10-iteration compression runs reduced the 8 MiB median from 21.34 ms
to 11.79 ms (1.81x throughput) and the 2 MiB median from 9.61 ms to 7.85 ms
(-18.3%). The independent 5,000-block base-build A/B stayed on the small-file
path and was unchanged at about 53.11 ms versus 53.12 ms. Seven alternating
precompiled compaction A/B pairs reduced median time from 132.64 ms to 130.32 ms
(-1.75%); bounded queue buffers added about 0.11 MB and 48 allocations during
that merge.

#### P5.25: Direct streaming history compression

P5.24 parallelized the second half of history finalization but still required a
complete uncompressed temp file. Base collation and compaction first wrote and
synced every logical history byte, reopened that file, read it a second time,
compressed it into another temp, and finally deleted the first copy. Erigon's
collation path instead feeds its segment compressor while records are emitted;
immutable build output does not make an uncompressed round trip through disk.

The history writer now retains only its first 16 KiB logical chunk, which holds
the fixed record and tx-range counts that builders backpatch with `WriteAt`.
Later chunks are compressed immediately into the bounded body temp; once the
stream crosses the measured 1 MiB parallel threshold, P5.24's worker queue and
ordered reducer encode subsequent chunks without changing their logical order.
At finalization the patched first chunk is encoded and prepended to the already
compressed body while body table offsets are shifted by its compressed length.
The raw compatibility gate still uses the ordinary temp writer.

This keeps the previous transactional boundary: only a synced, checksummed
compressed temp is renamed into its content-addressed path. The unordered legacy
fallback can abort all queued work, remove the body scratch, reset the retained
prefix, and rebuild through bounded ETL. Tests compare stream output byte for
byte with canonical whole-blob compression across empty, partial, exact-boundary
and multi-block inputs, exercise header backpatch and reset, verify scratch
cleanup, and run the shared encoder/reducer under the race detector.

Against independently compiled P5.24 binaries, ten alternating Pebble A/B pairs
reduced the sparse 5,000-block base-build median from 48.63 ms to 41.10 ms
(-15.5%) and the dense median from 52.82 ms to 45.34 ms (-14.1%), with allocation
counts slightly lower. Seven alternating compaction pairs reduced the median
from 129.80 ms to 119.13 ms (-8.2%). The direct stream microbenchmark reduced
the 2 MiB median from 9.81 ms to 8.87 ms with workers and the 8 MiB median from
21.06 ms to 13.82 ms (1.52x throughput).

#### P5.26: Trusted history build reopen

After P5.25 removed the uncompressed file round trip, a successful base build
still decoded the complete history again before publishing its manifest. That
pass revalidated tx-range ordering, record framing/order/count and logical end
even though the same build transaction had just enforced each invariant while
emitting the table and records. Compaction did the same full replay over its new
output after already decoding every source record and writing an exact known
count. Erigon trusts a successful collation/compressor output at this boundary
and reserves exhaustive semantic verification for external snapshot inputs and
offline audits.

Fresh base and compaction output now use a narrowly scoped trusted reopen. The
compressed reader still validates the complete physical block table, increasing
logical offsets, contiguous compressed offsets, physical file coverage and
declared uncompressed extent. The trusted layer additionally requires the
current history version, exact writer-observed logical end, exact record count,
exact tx-range count and minimum record framing space, then decodes payload
bytes at the first-record boundary, logical midpoint and final byte. Index and
accessor fixed-layout reopens remain unchanged.

The trust does not extend to downloaded, restored, pre-existing or compaction
input files. Manifest verification still checks physical checksums, validates
every tx-range, decodes every history record, proves index coverage and compares
every accessor entry against its history record. Corruption tests bind all
writer facts, damage a sampled compressed body block, require both trusted and
exhaustive validators to reject it, and retain the existing full-record
corruption suite.

Against independently compiled P5.25 binaries, ten alternating Pebble A/B pairs
reduced the dense base-build median from 45.80 ms to 43.70 ms (-4.6%), allocated
bytes from 14.03 MB to 10.60 MB (-24.5%) and allocations from about 130,537 to
95,408 (-26.9%). Sparse median time fell from 40.30 ms to 38.89 ms (-3.5%),
allocated bytes by 18.4% and allocations by 28.4%. Seven alternating compaction
pairs reduced median time from 118.35 ms to 113.13 ms (-4.4%), allocated bytes
by 7.9% and allocations by 19.3%.

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
