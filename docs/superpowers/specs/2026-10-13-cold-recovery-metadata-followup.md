# Cold recovery and metadata follow-up

The live six-minute capture found five transitions from moderate debt at 60%
duty to otherwise healthy hysteresis at 20%, plus repeated full-catalog copies
despite an effective manifest decode cache. Workloads must still be compared by
transactions/state changes as well as blocks.

## Recovery admission

Retain an already admitted moderate tier for the first healthy hysteresis
sample only when engine/device evidence remains fresh, observations are
comparable and not reset, L0 remains below its positive threshold, device await
is at most 2ms, and no hard/stall/device/L0/severe pressure exists. This is a
carryover, not a new promotion from a reused observation. Subsequent ordinary
recovery still requires two/three good samples. Any loss of evidence revokes the
carryover immediately; restoring the same observation cannot reacquire it.
The carryover preserves the installed recovery deadline. The November update
below describes a separate completion-based release. Expose recovery carryover
as a separate gauge without changing reason bit meanings.

## Immutable internal manifest loads

Public LoadManifest/LoadProductionManifest must continue returning detached,
mutable containers. Introduce a package-private read-only loading path that
may borrow the decode cache's private immutable manifest. Every load still
reads and compares the entire current file; no mtime/generation-only shortcut
or stale-read fallback is allowed. Borrowed generations survive replacement
because the cache never mutates its retained generation. Only audited internal
read-only consumers may use the path. Publication and progress mutation retain
owned manifests; JSON schema, validation and durable publication are unchanged.

The chain's source view also receives an opaque PinnedManifestView from its
already pinned Manager, avoiding two catalog clones and another path-index
sort. The read lease remains held for the same lifetime. Live managers cannot
export the opaque pinned view. Public OpenPinnedManager still validates and
detaches arbitrary caller input; Manager.Manifest still returns mutable copies.

## Measurement and validation

Measure complete load/read-decision work with the same synthetic catalog on
both paths, including byte authentication. Test public mutation isolation,
cache replacement/disable/rejection, changed same-length files and concurrent
publication. Run debt carryover transition/protection tests and related full
packages/race checks. Instrument event replay passes, overflow and retained
bytes before choosing a new cache budget. Any further LSM or event change must
be justified by fixed-input work and preserve strict source authentication.


## November 19 maintenance update

After a successful, completed throughput pass, the metadata observer can
release its recovery earlier when current level-3 capacity is supported by two
new comparable healthy engine and device samples observed after completion.
Recalculate from the original work cost and completion time. Check that the
new interval has elapsed before changing either the absolute deadline or the
same attempt's forced-busy retry interval. Request one normal pass, which
rechecks independent gates and current resource conditions. Failure, pending
outer maintenance, the moderate lane, configured cooldown and a preexisting
independent deadline retain their protection. A newly armed observation starts
a fresh healthy sample sequence.

Each manifest loader still authenticates exact current file bytes. Identical
concurrent loads can share an immutable validated decode outside the resident
cache lock. Local publication and budget changes supersede earlier decode
installation. Public views stay detached. Progress-only publication can
reuse a validated immutable over-budget view after checking the observed
encoding. Existing normal publication handles legacy and disabled-cache
cases.

A busy account route refresh can double its dictionary allowance after opening
an existing table of at least 128 million rows. An admitted healthy level-3
pass checks current engine/device pressure and independent CPU/memory headroom
again when a dictionary first needs the extra allowance. A rejected expansion
preserves the ordinary build and its full recovery accounting. When no route
was published, the rejection ends the pass before any leaf merge. The build still
attempts at most 64 new pairs, uses the existing bounded ETL buffer and may
publish more authenticated pairs in one singleton rewrite. The routing file
format, source authentication, strict verification and canonical fallback
remain in force.
