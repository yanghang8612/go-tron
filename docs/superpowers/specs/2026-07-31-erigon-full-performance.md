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
normally 18--20 block un-solidified suffix scans per-block changeset prefixes.
This applies even before cold history is installed; bounded readers no longer
take the legacy hot-only shortcut that assumes every changeset has an inverse
row. Sync exposes pass, block, change, ETL applied/input/batch, interruption,
and duration counters under `sync/stage/state_history_index/*` for the P4.39
production gate.

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
