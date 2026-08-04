# Erigon Full-Performance Alignment Plan

**Spec:** [2026-07-31-erigon-full-performance.md](../specs/2026-07-31-erigon-full-performance.md)

## P1: Shared read-cache substrate

- [x] Keep latest-account, latest-KV, code, and commitment reads on the bounded
  blockbuffer base cache below rewindable overlays.
- [x] Reuse confirmed missing-key classification instead of issuing a second
  Pebble `Has`.
- [x] Let `Buffer.Has` consume authoritative positive/negative base-cache rows.
- [x] Preserve snapshot-version, flush-refresh, discard, and same-key
  invalidation semantics for concurrent fills.
- [x] Remove the rejected per-block compatibility-prefetch rollout and its
  configuration surface from the latest-only fresh-sync path.
- [ ] Collect before/after mainnet CPU, Pebble, and sync-throughput samples.

## P2: Session-scoped read ahead

- [x] Move worker ownership to `canonicalRangeExecutor` for
  sync sessions.
- [x] Enqueue future-block hints immediately after staged bodies decode.
- [x] Extract schema-owned account, contract-metadata, and code keys without
  mutating or validating canonical state.
- [x] Route direct admissions into the same bounded cache canonical reads use,
  retaining overlay precedence and same-key late-fill rejection.
- [x] Replace lifetime-wide deduplication with per-block deduplication plus a
  bounded block/byte queue.
- [x] Tie reset, abort, and close to a session epoch and cover queued/in-flight
  old-fork rejection plus late-fill/flush races with race-clean tests.
- [x] Add queue bytes, drops, present/missing rows, read errors, stale blocks,
  and useful canonical-prefetch-hit metrics.
- [ ] Validate roots/results and collect fixed-window before/after production
  throughput, CPU, Pebble reads, and useful-prefetch ratios.

## P3: Ordered parallel preprocessing

- [ ] Profile protobuf decode, signature, envelope, receipt/log, and derived
  index costs on a transaction-dense fixed replay.
- [x] Expand the bounded signature pool into a shared immutable-block
  preprocessing pool without adding workers or configuration.
- [x] Precompute transaction Merkle roots, typed contract decode, protobuf wire
  sizes, txid/signers, and witness recovery for sync batches.
- [x] Keep mutable standalone transaction sizing uncached and retain serial
  authoritative validation/error ordering.
- [x] Replace ordered owner reflection and read-ahead owner extraction with the
  shared typed-contract memo.
- [ ] Parallelize independent static validation with serial authoritative
  consumption beyond the implemented immutable facts.
- [x] Move deterministic latest-state key extraction and eligible reads ahead
  of the serial executor.
- [x] Replace TxLookup stage per-block Has+Get point reads with freezer range
  reads / one ordered Pebble iterator, bounded sort runs, and stoppable ETL.
- [ ] Parallelize derived receipt/log/trace encoding behind recoverable stages.

## P4: Conflict-aware execution

- [x] Define the logical transaction write-cell schema over the authoritative
  StateDB journal, with conservative unknown-entry handling.
- [x] Add an observe-only Transfer wave planner using actual serial writes,
  dynamic-property barriers, and address dependency detection.
- [x] Capture generic transaction read cells across StateDB account, storage,
  code, metadata, witness, account-KV, and per-key DynamicProperties paths.
- [x] Validate captured reads against a block-local version map and report
  first-pass validity/conflict families by VM, Transfer, and other contracts.
- [x] Label only audited commutative fee/cumulative settlement operations and
  measure raw versus ordered-delta-normalized first-pass validity without
  changing canonical execution.
- [x] Split hot TRON Account access into Erigon-style typed field paths with
  full-account hierarchy barriers, exact cached-row dependencies, and an
  observe-only typed first-pass validator.
- [x] Model Erigon's explicit previous-sender execution dependency on top of
  typed versions and measure the remaining cross-sender retry set and sender
  critical-chain depth before starting workers.
- [x] Build an observe-only dependency-ready DAG from actual typed read/write
  versions, with unknown/range reads as serial barriers, and report attainable
  wave width before allocating speculative workers.
- [x] Weight dependency-ready waves with measured canonical transaction cost
  and report unlimited-worker plus four-worker barrier schedules before worker
  implementation.
- [x] Preserve exact read-version and previous-sender edges, then simulate an
  Erigon-style four-worker ready queue without global wave barriers.
- [x] Add sampled zero-indegree discard-only workers with isolated StateDB,
  DynamicProperties, rawdb write overlays, and full TransactionInfo comparison.
- [x] Extract StateDB/DynamicProperties typed post-images and normalized
  settlement deltas, then compare sampled worker WriteSets with the serial
  reference.
- [x] Include direct Context.DB exact-key reads and raw put/delete post-images
  in version validation and sampled worker WriteSet comparison.
- [x] Implement a preflighted ordered applier for unambiguous typed post-images
  and settlement deltas, while explicitly rejecting account replacement,
  generation reset, self-destruct, and reincarnation publication.
- [x] Publish a full-account post-image only as a validated fresh absent-to-
  present creation, before applying its typed AccountKV/code/metadata cells.
- [x] Reapply eligible sampled worker WriteSets to isolated block-start copies
  and compare the resulting post-state before enabling canonical publication.
- [x] Accumulate successful sampled WriteSets in transaction order on one
  shared isolated publisher to validate multi-transaction ordered baselines.
- [x] Pre-execute Transfer transactions concurrently from block start before
  the serial loop, retain their results, and validate zero-indegree results
  against canonical TransactionInfo, WriteSet, and ordered publication.
- [x] Freeze each pre-executed Transfer read set and independently validate its
  typed versions, commutative deltas, sender edge, and unknown-read barrier
  against the canonical dependency DAG.
- [x] Retain and compare java-compatible per-transaction BalanceTrace operation
  order before allowing typed WriteSets to replace canonical execution.
- [x] Apply typed writes and settlement deltas at ordered publication after
  serial-equivalence fixtures and production samples pass.
- [ ] Execute eligible waves in discard-only shadow workers and compare full
  journals plus TransactionInfo with the serial reference.
- [x] Implement an opt-in narrow Transfer speculative executor with full
  TransactionInfo, WriteSet, and BalanceTrace result carriers.
- [x] Replay conflicts serially and publish only in original transaction order.
- [x] Reach the BalanceTrace production sample gate, enable
  `--exec.parallel-transfers`, and compare canonical throughput/state metrics.
- [x] Normalize the block-scoped public-bandwidth recovered baseline and usage
  increments so independent free-bandwidth transfers can publish together.
- [x] Add sampled previous-sender chain workers that forward verified typed
  state, retain exact read-source versions, and reject intervening writers.
- [x] Run the first sender-chain TransactionInfo/WriteSet/BalanceTrace
  production gate and enable canonical publication on non-sampled blocks.
- [x] Add sampled Erigon-style sender incarnations that rebuild a stale suffix
  from the settled canonical prefix and retry again after a later conflict.
- [x] Measure recovered work versus StateDB copy/execution cost and complete the
  sender-retry TransactionInfo/WriteSet/BalanceTrace production gate.
- [x] Replace copy-per-incarnation with a lazy reusable settled-prefix runner,
  exact ordered WriteSet advancement, and canonical refresh fallback.
- [x] Run the settled-prefix reuse production gate and verify copy reduction,
  prefix advancement coverage, and unchanged serial equivalence.
- [x] Add measured async deadline projection for each incarnation and classify
  ready, late, and unknown results at their canonical publication boundary.
- [x] Run the async projection production gate and size the frozen-raw-view
  worker pool and retry queue from observed ready rate and deadline slack.
- [x] Freeze exact raw-KV reads at the settled boundary and add a disjoint
  sampled single-worker async retry canary with strict capability misses,
  streamed results, incarnation invalidation, and error-path worker joining.
- [x] Replace actual-retry whole-block version-map clones with compact suffix
  read-version carriers and split dispatch prefix/raw/version timing.
- [x] Retain a clean sender-chain observer worker as the actual retry's
  block-start state and advance it by canonical WriteSets instead of copying
  StateDB at the conflict boundary.
- [x] Expand the actual canary to a bounded pool of clean observer runners,
  preserving exclusive ownership and newest-incarnation admission.
- [x] Prioritize the nearest four sender-chain boundaries per async job instead
  of eagerly executing an entire suffix that later incarnations invalidate.
- [x] Expand the actual async observer to three sampled cohorts while retaining
  one fixed synchronous cohort as the production reference.
- [x] Stop superseded async sender suffixes between transactions with atomic
  incarnation tokens and reclaim their unused execution reservations.
- [x] Run the actual async canary production gate and measure ready, late,
  stale, busy-skip, raw-miss, validation, and finish-wait rates.
- [x] Add a block-scoped minimum-transaction incarnation heap which freezes
  raw/version/dynamic inputs at enqueue time, retains requests through runner
  saturation, and drops expired superseded work before dispatch.
- [x] Move queued canonical-prefix WriteSet advancement onto the exclusively
  owned background runner and return prefix accounting/errors through its
  completion event without reading a later live StateDB.
- [x] Publish version-valid ready async sender-retry results on one sampled
  cohort, with public-bandwidth re-admission, typed-write preflight, exact
  ordered application, serial fallback, and retained observer cohorts.
- [x] Reuse ordinary sender-chain publisher states as the bounded retry-worker
  pool and promote ready incarnation results on all opt-in parallel blocks
  without another preexecution pass or conflict-boundary StateDB copy.
- [x] Measure ordinary retry WriteSet capture cells/time separately from
  background prefix replay and publication so critical-path cost is visible.
- [x] Project ordinary canonical WriteSets onto retry-suffix read cells, retain
  full carriers only for publishable sender members, and skip empty prefix
  applications while preserving unknown-write barriers.
- [x] Register projected AccountKV and generation writes at mutation time, then
  index first-write keys so filtered retry carriers avoid journal/read-map
  scans.
- [x] Split the complete OCC WriteSet carrier from the optional projected
  post-image carrier, fill journal-only state families at `journal.append`, and
  remove both transaction-end journal walks from version validation.
- [x] Run the complete mutation-time WriteSet production gate and compare full
  and filtered recorder capture cost, unsupported writes, retry publication,
  and sync throughput against P4.32.
- [x] Replace block-start full-cache `StateDB.Copy` with an Erigon-style lazy
  stable latest-domain view that eagerly owns only the current dirty overlay.
- [x] Run the lazy block-start copy production gate: verify omission ratio,
  observer/publisher equivalence, CPU-profile removal, and sync throughput.
- [x] Add fold-local commitment shape metrics without atomics in the recursive
  hash path, covering resolved ops, split/worker utilization, hash bytes/rounds,
  and wall time.
- [x] Run the commitment-shape production sample and select durable preloading,
  streaming split scheduling, or hash reduction from measured work ratios.
- [x] Add persistent first-nibble commitment owners which may advance across
  blocks independently while root assembly, metadata, stages, heads, and layer
  promotion remain canonically ordered.
- [x] Run the ordered commitment pipeline production gate: require more than
  one fold in flight, zero commitment/equivalence errors, and compare fold wall
  time plus enqueue backpressure against P4.45.
- [x] Replace the pooled commitment sponge with Erigon's assembly-accelerated
  fastkeccak after byte-equivalence and branch-shaped microbenchmarks.
- [x] Run the fastkeccak production gate and compare Keccak CPU, fold wall,
  backpressure, throughput, and all root/equivalence errors against P4.46.
- [x] Replace the JSON CommitmentBranch checkpoint with a sorted immutable
  binary segment, ordinal accessor, sparse B-tree, and indexed manager point
  reads; reject the lane-decoded-cache prototype on measured memory/latency.
- [x] Add a persistently opened immutable commitment view plus versioned hot
  overrides/tombstones, retaining hash-bound crash repair and reorg isolation.
- [x] Publish the first immutable commitment baseline with a crash-safe
  generation redirect, canonical/solidified boundary, root verification, and
  post-marker legacy cleanup while import continues through the hot delta.
- [x] Merge an active immutable baseline plus bounded delta/tombstones into the
  next generation, keep concurrent imports in a new delta, and reclaim the
  covered generation after verified publication.
- [ ] Run the fresh snap-mode write-amplification gate for the periodic
  immutable commitment lifecycle.
- [x] Replace sampled synchronous copies with an asynchronous incarnation-
  priority queue over shared versioned state before canonical enablement.
- [x] Run the shared-version retry production gate: require shared-value hits,
  zero private prefix advances/errors, unchanged serial equivalence, and lower
  retry copy/replay cost over a fixed dense window.
- [x] Validate canonical sender-chain publication ratios and the retained 1/64
  serial canary over a longer fixed production window.
- [ ] Run the public-bandwidth reservation production gate and compare
  published/conflict ratios plus limit fallbacks over a fixed height window.
- [ ] Expand eligibility by actuator family using java-tron fixtures and fixed
  mainnet replay windows.
- [x] Select Trigger/CreateSmartContract as the next actuator family from a
  fixed 100-block mainnet workload sample and Erigon's ordered-finalization
  design, while keeping canonical VM publication disabled.
- [x] Generalize immediate-sender scheduling to a family predicate and add a
  sampled 1/64 VM sender-chain preexecutor with exact read-source versions.
- [x] Add a transaction/forwarded/parent raw-KV overlay so sampled VM chains
  can consume exact predecessor post-images without changing the existing
  Transfer publisher's conservative raw-KV admission.
- [x] Compare VM TransactionInfo, full WriteSet including public bandwidth,
  BalanceTrace, and forwarded predecessor results under independent metrics.
- [x] Run the VM sender-chain production canary and quantify execution,
  candidate, validation, conflict, mismatch, error, and wall-time ratios.
- [x] Split VM candidate WriteSet mismatches into public-bandwidth-only versus
  other state and classify unavailable results by execution/capture/applier
  stage before designing canonical publication.
- [x] Run the VM mismatch-classification production gate and require zero
  non-bandwidth state mismatches before adding an ordered resource carrier.
- [x] Project each version-valid VM public-bandwidth reservation against the
  exact canonical transaction-boundary usage/time/limit without mutating state.
- [x] Compare projected public-net value and write-presence semantics with the
  serial WriteSet while retaining strict comparison for every other key.
- [x] Run the VM public-net projection production gate and require admitted
  projections to match serial writes with zero missing/other mismatches.
- [x] Share the fork-aware serial block-energy delta rule with the sampled VM
  observer instead of duplicating adaptive-energy settlement logic.
- [x] Project retained VM receipt energy at the exact canonical transaction
  boundary and validate `block_energy_usage` immediately after serial
  accumulation without mutating canonical state.
- [x] Run the VM block-energy projection production gate and require zero
  missing/mismatch across final version-valid VM candidates.
- [x] Add a deterministic 1/1024-block VM publication cohort that requires
  exact read versions plus admitted public-net and block-energy carriers.
- [x] Prove serial equivalence for independent and forwarded VM sender results,
  including TransactionInfo, BalanceTrace, resources, and final state root.
- [x] Run the small VM canonical-publication production gate before expanding
  the cohort.
- [ ] Add VM retry incarnations only after the first canonical cohort retains
  exact production parity.

## P5: Snapshot and cold steady state

- [x] Add allocation-free sampled physical-write attribution by rawdb schema
  family at the final coalesced blockbuffer/Pebble boundary.
- [x] Run the physical-write family production sample and select the first
  write-amplification reduction from commitment, history, or immutable body
  ownership evidence.
- [x] Separate source aggregation from final Pebble batch sizing, project each
  appended layer's exact coalesced-size delta, and retain the 32 MiB final cap.
- [x] Increase the bounded async solidified aggregation window from 120 ms to
  480 ms and expose exact input/output bytes plus extended group/layer counts.
- [x] Use the 480-ms production canary to verify logical-byte reduction, then
  raise the window to 960 ms so source input crosses the old 32 MiB boundary.
- [x] Verify the 960-ms output-bounded path and promote it to 1,920 ms while
  measured final/source sizes remain below the fixed 32/128 MiB caps.
- [x] Run the output-bounded aggregation production gate and compare layers per
  group, logical byte reduction, disk/compaction bytes per block, stalls,
  backpressure, and buffered-layer bounds against P4.34.
- [x] Split state-history physical attribution into tx-range, changeset, and
  inverse-index streams without changing their persisted schema or behavior.
- [x] Run the split temporal-write production sample and reject index-only
  deferral because changeset payloads dominate temporal logical bytes.
- [x] Add allocation-free sampled changeset component attribution for previous
  image, next image, logical key, and fixed RLP metadata/framing.
- [x] Run the changeset-component production sample and select Erigon-style
  previous-image-only history because next images are 34.6% of payload bytes.
- [x] Encode new hot changesets without the forward image, avoid cloning the
  borrowed next value, and retain a read-only legacy decoder during transition.
- [x] Run the previous-image-only production gate and compare changeset bytes,
  coalesced output, disk/compaction bytes, write stalls, and sync throughput.
- [x] Hoist hot changeset block number/sequence into its physical key and block
  hash into the one-per-block tx-range row, retaining one fork guard per block.
- [x] Run the context-hoisted hot-history production gate and compare sampled
  row fixed bytes plus normalized coalesced/disk/compaction bytes.
- [x] Add a hash-bound StateHistoryIndex stage which rebuilds solidified
  latest-key/block inverse rows through a bounded sorted ETL pass.
- [x] Defer inverse-index publication during bulk sync and serve the short
  un-solidified suffix through direct block-changeset scans.
- [x] Replace one iterator per unindexed-tail block with one ordered range
  iterator for exact, batch, and prefix historical reads while preserving
  block-pack repair and early-stop semantics.
- [x] Run the derived-index production gate and compare temporal family share,
  ETL cost, coalesced output, disk/compaction bytes, stalls, and query parity.
- [x] Remove next-image fields from the cold binary history format together
  with duplicated block/sequence context after the hot-row production gates.
- [ ] Run the cold-history v5 production gate and compare segment build/merge
  duration, raw/compressed bytes, cold-stage lag, and sync/compaction impact.
- [x] Shorten CommitmentDomain leaf identities from repeated full physical
  latest-domain keys to their already-computed 32-byte trie paths, while
  retaining read-only decoding and touched-branch migration for legacy rows.
- [x] Run the commitment path-leaf production gate and compare sampled
  commitment bytes, final coalesced bytes, physical writes, compaction, sync
  throughput, restart recovery, and root/equivalence errors.
- [x] Retain transaction-ordered previous values until block finalization and
  publish one versioned block-packed hot changeset value while preserving
  positive-sequence repair/legacy rows and all temporal read/unwind semantics.
- [x] Run the block-packed changeset production gate and compare rows per pack,
  avoided Put/key bytes, changeset/final-coalesced bytes, disk/compaction bytes,
  stalls, sync throughput, restart recovery, and archive/unwind errors.
- [x] Add a versioned, benefit-gated Snappy envelope for block-packed hot
  changesets so Pebble WAL and uncompressed hot levels do not retain redundant
  previous-value structure; pool transient encode/decode buffers safely.
- [x] Run the compressed block-pack production gate and compare stored/raw
  ratio, codec CPU, GC/memory, final coalesced and physical writes, stage lag,
  stalls, sync throughput, mixed-format restart, and history parity.
- [x] Switch the versioned production systemd template from finite hot-history
  `full` mode to Erigon-style `snap` mode for the next fresh mainnet sync.
- [ ] Install the snap-mode service against a new datadir and run the cold v5
  build/merge/prune gate from genesis without reusing the persisted full-mode
  retention lock.
- [x] Replace repeated genesis-to-head `StateTxRange` scans in each cold
  history build with hash-bound `SnapshotBuild` resume plus physical block-key
  seeks for boundary discovery, record collation, tx-range counting, and
  tx-range emission.
- [x] Benchmark the bounded cold-build source on Pebble and verify subsequent
  lifecycle passes use the previously published block boundary.
- [x] Export cold build/merge/lag and hot-prune counters plus phase durations
  so the fresh snap-mode production gate can distinguish import pressure from
  lifecycle backlog without relying on periodic log lines.
- [x] Drain verified cold-build backlog through coalesced bounded lifecycle
  passes instead of sleeping one full maintenance interval after every
  5,000-block segment.
- [x] Prepare the composed production lifecycle's latest-build watermark and
  defer full-keyspace latest snapshot scans while historical sync is active.
- [ ] Productionize signed snapshot catalog hosting and resumable bootstrap.
- [ ] Verify recent-tail execution and restart after restore.
- [ ] Tune freezer/history build-merge-prune throughput above sustained import.
- [ ] Run the 30-minute resource gate and 24-hour mainnet soak gate.
