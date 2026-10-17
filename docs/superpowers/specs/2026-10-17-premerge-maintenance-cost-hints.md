# Busy cold-history selection hints and outer maintenance timing

The busy state-domain-change streaming selector can revisit a large prefix of
original leaves that exceed its record/logical budget. The previous cache still
Stats every hit and retains three full refs and FileInfos per entry. Its 64 MiB
charge can reject later entries before the active working set fits. A captured
stack entered the original footer reader through this selector, but the observed
49.961 s outer call and 0.973 s inner work do not attribute the entire pre-inner
48.143 s interval to this mechanism.

## Selection hints, not source authorization

This supersedes the per-hit observation/retention policy in
`2026-10-17-cold-input-cost-cache.md`. Scope remains private Runner-owned,
BusyLeafOnly state-domain-change non-reference-output compaction. Ordinary and
reference output paths keep their existing readers.

A hint maps a SHA-256 digest to three uint64 costs: physical bytes, logical bytes,
and records. The versioned, domain-separated encoding length-prefixes strings
and encodes fixed-width integers for every raw SegmentRef field: Dataset, Domain,
Kind, FromTxNum, ToTxNum, AggregationSteps, Path, Size, Checksum. Fixed history,
inverted-index, accessor slots make companion ordering irrelevant. Raw checksum
text, raw steps and paths remain distinct; this does not normalize refs or use a
staging/proof ID. Field-inventory and per-field mutation tests fail when a new
SegmentRef field is omitted. Incomplete/unsupported identities use the original
reader without retention. This private eligibility helper is not a manifest
validator; production manifest and candidate validation retain that responsibility.

The cache is bound to an owned absolute directory path, actual directory identity
and staging reset epoch. A bind failure clears it and propagates the error. Hits
check context and return only costs, without source Stat/open/header reads.
Ordinary misses call the complete original cost reader and remember only a
successful uncancelled result. They intentionally omit the old cache's extra
before/after observations: these costs influence selection only.

Every selected candidate still removes its old hint and performs before-three-file
observations, the unchanged original cost reader (including path/FD observations
and header/footer checks), and after-three-file observations. Identity, size, mode
and mtime must remain stable. Canonical slot order can change the first error
reported when multiple companions fail simultaneously; fault-order equivalence is
not promised. Fresh errors/cancellation are propagated and not retained. Earlier
successful refreshes may remain numeric hints if a later group member fails.

The whole selected group's fresh costs must exactly equal the selection costs,
sum without overflow, and fit the unchanged physical/logical/record/source
budgets. A mismatch defers the whole group before its atomic proof claim, codec,
output or publication; the next attempt reselects from its current manifest and
successful fresh costs. No budget or retry loop is widened. The digest never
supplies a source ref/path, proof identity, payload, authenticated result or admit
boolean. Existing canonical checksum/codec/companion checks, proof claims,
staging publication locks, fresh Integrate, durable rebind and GC leases remain.

Under immutable checksummed source files an unchanged complete ref has stable
cost. In abnormal same-ref replacement/corruption/deletion, a warm unselected
hint may suppress discovery indefinitely. An overestimate can keep the source
unselected while later healthy groups merge. Reset/restart clears hints but does
not repair data or guarantee detection. A selected source still undergoes fresh
checks and original authentication. A digest collision or deliberately wrong hint
can cause poor selection, deferral or starvation; it grants no deletion authority.
Equal numeric costs cannot bypass selected source authentication. This is an
explicit discovery/liveness tradeoff, not equivalence to old error timing.

After successful Integrate, only selected retired source hints are deleted. No
catalog scan, persistent format, manifest change, readiness transition or cleanup
authorization follows from eviction. Eviction remains safe if later durable
rebind/cleanup fails; failure only loses optimization state.

## Retention and capacity

Keys are 32-byte arrays; values are 24-byte numeric costs. Entries retain no
FileInfo, ref, slice, borrowed string or manifest slab. The one directory binding
is separate. The fixed charge is 512 bytes per entry with both 131072-entry and
64 MiB limits; 34362 entries charge 17593344 bytes. New entries at capacity use
the original reader, preserving the cached prefix. Retired selected hints free
slots immediately. There is no FIFO list or auxiliary unbounded structure.

The charge is an allowance for retained map storage, not process RSS or transient
allocation. Directory binding, per-pass manifest/candidates/local cost maps,
hashing scratch, allocator garbage and other Runner work are outside it. The
measurement-only heap probe uses 131072 entries, four delete/reinsert cycles and
transient large ref strings; explicit post-GC measurements describe retained
heap, while intermediate readings include garbage and allocator effects. It does
not cause production GC. On Go 1.25.5 darwin/arm64, the full map retained 16788680 additional
HeapAlloc bytes after GC; four churn cycles and large transient refs retained
the same amount. Initial map growth without GC used 33574952 additional bytes.
Large-ref hashing intentionally allocated substantial transient scratch; these
results support the retained charge, not a total-allocation or RSS bound.

## Outer timing and hint counters

Each actual Runner registers one private observer using its MetricsNamespace.
Standalone callers may leave it nil. The observer performs no source IO and has
no ticker/per-candidate metric registration or clock sample. Cache counters are
plain values under existing Runner pass serialization and are published in
aggregated deltas at phase boundaries and completion.

`compaction/outer/` covers a single CompactHistoryDomain attempt, excluding
pre-call lease acquisition and Runner recovery/drain bookkeeping. Each drain
iteration is a separate attempt. Durations are nanoseconds; started timestamps
are Unix nanoseconds. Phase IDs are:

| ID | Phase | Scope |
| --- | --- | --- |
| 0 | idle | no active attempt |
| 1 | prepare | initial checks and staging reconciliation |
| 2 | manifest | initial production manifest load/validation |
| 3 | candidates | directory bind, candidate construction and early proof filter |
| 4 | select | budget selection and output-mode bookkeeping |
| 5 | revalidate | selected fresh metadata; existing non-busy post-claim budget loop |
| 6 | claim | atomic history-proof range claim |
| 7 | inner | codec and immediate pre-publication cancellation handling |
| 8 | integrate | publication-lock wait, fresh Integrate and durable staging rebind |
| 9 | cleanup | protected physical cleanup and deferred releases |

Current active/phase/outer_started_unix_nano/phase_started_unix_nano clear on every
return, including error/cancel/defer. `last/<phase>/duration` is reset at begin and
published incrementally; hints_used and candidates describe the current/recent
attempt. `last/duration` changes at finish, so during an active attempt this is a
mixed progress/latest-completed view, not an atomic last-completed snapshot.
Cumulative `total/duration`, per-phase totals, attempts and entered counters retain
history; outer and per-phase maxima retain per-attempt high water even after a
short later defer. Re-entered phases accumulate within the attempt before max.
On early returns, deferred releases are charged to the phase current at return.
Phase intervals partition outer time; outer totals and nested inner-codec timing
must not be added to phase totals. Boundaries are wall observations, not CPU or
exclusive disk attribution. Independent concurrent Runners need distinct namespaces.

`compaction/input_cost_hints/` exports cumulative hits, misses,
metadata_reader_calls, fresh_reader_calls, evicted_retired, capacity_reject and
resets; entries and charged_bytes describe retained state. Hits/misses cover
cacheable lookups only. metadata_reader_calls includes original-reader invocations
from misses, uncached fallback and fresh selected reads. fresh_reader_calls is
its selected subset, incremented immediately before invocation: a before-Stat
failure counts neither. These are not Stat/Open/ReadAt syscalls, physical device
IO or SST write amplification. Candidates is the post-proof-filter candidate
count, not visited leaves. hints_used=0 may leave retained-cache gauges unchanged.

## Validation and limits

Functional coverage in `core/state/snapshots/compactor_cost_cache_test.go` and
`history_compaction_outer_test.go` includes real V6 sources, independent stale-cost
dimensions, cancellation, missing/corrupt/changed selected inputs, delayed
unselected discovery, capacity fallback, reset/binding, identity encoding,
original budgets and existing lease/recovery behavior. Detailed command/results
and the coverage checklist are retained with the ignored verification evidence.

Controlled whole-merge I/O evidence uses a clean 5c61 archive plus frozen
symmetric forwarding instrumentation and the same legal V6 fixtures and
endpoints. Three serial AB/BA/AB pairs compare actual forwarded metadata
Stat/Open/ReadAt calls and bytes, selected outputs, semantic manifests including
progress, and reopened logical cold rows/files. The common byte-budget counterexample must make the old
representation fail to retain the working set while new hints fit. Timings, CPU,
allocation and heap results are separate from semantic/I/O acceptance. Raw
results and failed evidence are retained. All six formal arms passed. Each five-merge arm produced identical selected
ranges, semantic manifests (only PublishedUnix is volatile), 2068 all-row/per-key
oracles and immutable file hashes. Forwarded path Stat calls fell 10349 to 1013,
FD Stats 2513 to 977, explicit Opens 2552 to 1016 and actual ReadAt calls 7579 to 2971;
ReadAt returned bytes fell 337348 to 137668. This counts only instrumented sites,
excluding sequential authentication Read/io.Copy and library-internal opens.

Whole-five-merge wall ratios were 0.8430, 1.0854, 0.8267 (candidate/baseline): median
15.7% lower, with a retained 8.54% regression. CPU and allocated bytes were lower
in all three pairs. This local warm-filesystem, common 256 KiB capacity experiment
is not a production 64 MiB hit-ratio or throughput estimate. Standalone calls used
a nil observer and no active hot staging binding; actual durable rebind/Runner
lease/recovery behavior is protected by existing correctness tests, not claimed
as exercised by this timing fixture. The frozen ignored evidence retains the
format-correct independent Seq oracle and all earlier failed harness evidence.

This optimization targets repeated selection metadata reads. It does not reduce
payload merge rewrite geometry, prove reduced Pebble compaction write
amplification, eliminate manifest/candidate work, attribute a production plateau,
or predict sustained mainnet catch-up from a local warm-filesystem fixture.
