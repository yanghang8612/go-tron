# MuHash3072 local latest-domain commitment (fresh genesis only)

## Authorization and scope

The user authorized the production refactor and, after independent review and
release approval, master push/deployment from a fresh genesis database. This
design and implementation have passed Root/Astra design review; release remains
subject to final independent audit and explicit deployment approval.
No legacy database/snapshot migration, compatibility fallback or branch rebuild
is provided. Java AccountStateRoot, StateRootAdapter's Java methods, header
protobufs/block IDs, VM and consensus behavior remain byte-for-byte unchanged.
The internal root remains out-of-band BlockStateRoot and must never fall back
to a Java header root when its own anchor is absent.

The frozen prototype artifacts under build/benchmarks/ remain immutable. The
27 original WIP paths listed by baseline-identity.json original27wip remain
byte-identical and are excluded from staging.

## Primitive and canonical element

common/muhash3072 implements the Bitcoin Core v30.0 primitive pinned at
 d0f6d9953a15d7c7111d46dcb76ab2bb18e5dee3, using math/big, SHA256 and ChaCha20.
SHA256(element) is the ChaCha20 key; zero nonce/counters0..5 yield384 bytes LE,
reduced modulo2^3072-1103717. Insert/Remove multiply numerator/denominator.
Mapped zero and invalid stored field elements fail closed. Every block clones
its parent, applies deltas and pays inverse + canonical384LE + SHA256.

The production complete state is the normalized384LE field element (den=1),
not just a32B digest. It can resume subsequent Insert/Remove operations exactly.
Strict state/head codecs bind magic/version, exact length and range1..p-1;
state0, p and trailing/truncated bytes are errors. Canonical normalization ->
reload -> later updates must agree with official/Python differential tests;
raw numerator/denominator bytes need not remain identical after normalization.

Elements retain the frozen prototype codec:

ASCII gtron/latest-domain/muhash3072/v1 + actualNUL + family:u8 +
fullPhysicalKeyLength:u64BE + fullPhysicalKey + valueLength:u64BE + rawValue.

Only account-latest, KV-generation and registered-domain KV-latest keys are
accepted; family is derived/validated from the complete physical key. Missing
rows are omitted; existing empty values are included. KV presence wrappers and
old-generation physical rows are committed verbatim. Generation change alone
does not remove retained old-generation rows.

## Rawdb API and stored model

All new physical prefixes are declared only in core/rawdb/schema.go. Public API:

- StateCommitmentDelta { Key, BeforeExists, Before, AfterExists, After }, with
  owned byte slices. Coalescing retains the first before and last after, checks
  supplied intermediate chains, sorts by physical key and removes exact noops.
- StateCommitmentHead { Scheme string, Version/ElementVersion uint16,
  BlockNum uint64, BlockHash/ParentHash common.Hash, TxNum uint64,
  DeltaID/Root common.Hash, State muhash3072.State }.
- ReadStateCommitmentHead(reader) (head, present, error),
  WriteStateCommitmentHead(writer, head), EncodeStateCommitmentHead(head),
  DecodeStateCommitmentHead(bytes), ValidateStateCommitmentHead(head).
  Validation covers scheme/codec, canonical state and SHA256(State)==Root.
- LatestDomainCommitmentScheme becomes state-flat-latest-muhash3072-v1.
  The existing latest-root and by-block BlockStateRoot anchors retain32B roots.
- StateCommitmentCheckpoint remains the lightweight BlockNum/BlockHash/Root/
  Scheme anchor. The new snapshot CommitmentHead dataset holds one complete
  StateCommitmentHead at an actual source cut, using StateCommitmentHeadKey()
  and the same strict codec. No full accumulator/197B identity table is added
  for every block.

HEAD alone stores the current complete accumulator, binding its block identity,
parent, transaction cut and canonical coalesced delta ID. Existing chain/block
identity and BlockStateRoot history are reused; no duplicated per-block identity
family is introduced. Empty set state is1 and digest is
c85525462fdcf30a2c18d6f4b92923000974355c2477f59594d2c205a1d25add.
Post-genesis root includes every genesis source row and is normally different.

## Capture and atomic publication

Before images are captured before each physical latest-source write/delete,
not from a post-write reader or from the after-only old touch collector. Account
and generation writes, owned KV fast path, generic KV writes/deletes and prefix
enumeration share one block-owned delta collector. Capture is independent of
history enablement. A genuinely loaded before may be reused; unknown before
performs a real same-parent-cut Get and owns it. Pending latest-writer overlays
must include prior source writes/previous async blocks; later same-key changes
coalesce from the already captured state. Prefix deletion enumerates every
physical affected key and includes preceding same-block puts. Capture failure
prevents the physical mutation/publication; no borrowed buffer survives release.

Synchronous and deferred commits consume the same owned delta. The async worker
is serialized over canonical block order; it can overlap execution of the next
block but does not retain old branch lanes, lookahead/prefetch or repair state.
It loads the parent HEAD through the layer-bound parent cut, clones/applies the
delta and writes full successor HEAD plus current internal root into that same
LayerView as source writes. Capture owns bytes until the worker has finished;
release/reset of StateDB or pooled batches cannot invalidate them.

Critically writeBlockMetadataBatch currently writes direct durable metadata
before source buffer flush. HEAD/current identity must never be written there.
Historical by-block root anchors/body/receipts may be written ahead as noncurrent
records; only source + HEAD + latest-root + recovery-current identity in a
successful canonical flush group define durable state. The in-memory canonical
head may precede that durable cut; all fallible tail stages and layer promotion
finish before in-memory head/hooks publish. Failed folds/tail stages discard the
suffix. A later flush failure is a committed storage failure: poison the process
and reopen using the durable HEAD, rather than pretending the block never ran.
Idempotent retry compares current identity, parent and delta; an old fork's same
hash/height is not a successful current retry. Publication remains ordered.

Genesis stages source, normalized HEAD, internal root and chain-current metadata
through one batch/layer with the already computable wire genesis block identity.
It must not expose a half-initialized source without its complete HEAD. Restart
of a known genesis does not rebuild/materialize or rescan source state.

## Restart, unwind and fork

Startup reads/validates the fixed-size HEAD, latest internal root and the block
record bound by HEAD.BlockHash/BlockNum; it does not scan source/branch tables,
restore an old branch tree or silently initialize a nonempty old database. A
missing/incompatible HEAD is a fresh-resync error. Direct chain metadata may be
ahead of the last source flush; durable HEAD identity/flush cut is authoritative
for reopening state and canonical recovery, with orphan/ahead metadata dealt
with explicitly. Historical query identity/root anchors remain separate from
current mutable source and do not become a guessed recoverable accumulator.

CollectStateUnwind must Get each affected CURRENT physical source before any
restoration, then use the real earliest persisted Prev in the reversed range
as target after. It must not use persisted history Next (not stored) or a
post-overwrite Get. The inverse delta is applied to a clone of current HEAD;
its result must equal the existing target block's internal digest before the
same temporary layer/batch publishes source/HEAD/latest root/current identity.
Wrong captures fail target-root verification before publication. Same-cycle,
complete owned history uses inverse transitions; a disposable inverse overlay
provides actual historical DP/brokerage values for pending-reward subtraction.
Cross-maintenance or missing history uses rare protected in-place genesis replay
from a versioned, bounded, wire-hash-bound genesis recipe. It does not copy a full
database or change Khaos/Java fork acceptance rules. Fresh production uses direct
history; legacy staging replay is rejected before mutation. Replay suppresses
prefix hooks and clears discarded-future proposal/version caches.

Inverse publication includes source, HEAD, pending rewards, current derived DP,
allocator/progress and a cleanup marker. Ancillary cleanup/ancient truncation is
synced before clearing that marker. Uncertain publication or cleanup poisons the
process; startup checks markers in constant time and refuses incomplete cuts.
Recipe replay similarly syncs an unpublished marker before destructive reset.

Snapshot acquisition drains work and pins the coherent base plus immutable
layers with NewReadSnapshotThrough; it does not flush unfinalized layers merely
to create a cold snapshot. Matching source, full HEAD, execution properties,
pending rewards and allocator cut are captured through that same pinned view.

## Snapshot/cold and cleanup contract

The secondary agent owns snapshots and command/config/tool consumers; rawdb
schema/accessors belong to the primary agent. Snapshot manifest schema is
explicitly new MuHash scheme/elementcodec1 and complete checkpoint at the exact
source/block/tx cut. GetCommitmentCheckpoint stays a lightweight digest/identity anchor.
GetCommitmentHead exposes the full value only at actual snapshot cuts;
publication verifies its state/digest/identity and source same-cut binding.
Restore verifies before atomically installing source + complete HEAD + root;
it cannot partially install while checking or substitute only a digest.

Branch datasets/accessors/rotation/base/delta repair, branch cache/trunk/tail,
partition/prefetch knobs and old staged tree production scheduling are removed
from active runtime/tools. No old snapshot v1, branch presence or Java-root
fallback may silently select legacy behavior. Java consensus-root code remains.

## Required review and verification

Root/Astra review this design before cross-layer edits. Tests cover official
vectors, randomized canonical reload, all three source families/present-empty,
RMW and unknown-before/history-disabled capture, repeated puts/delete/recreate,
prefix/generation, overlay isolation and capture ownership; real Pebble grouped
flush failure/retry/reopen; delayed async completion/close/fork; genesis idempotent
restart/HEAD-only startup; current metadata ahead of durable source; unwind
across flushed and unflushed layers; snapshot checkpoint/root same-cut restore.
Full-set small-state rebuild is a test oracle, not a runtime startup fallback.
Java genesis/block bytes and JavaAccountStateRoot regression tests remain gates.
Only after required tests, independent Astra code audit and Root release approval
may commits/push/deployment occur. No old datadir conversion is implemented.

Unwind publishes a bounded pending-cleanup marker with target source/HEAD and
current rewards/counters/DP/stages. It deletes this marker only after ancient
Sync and required orphan/history cleanup succeed. Failure poisons the live
process; startup, genesis setup and empty-state initialization reject the marker
in constant time. This deliberately requires a fresh prepared database or
explicit recovery; normal startup never scans or repairs an interrupted unwind.
