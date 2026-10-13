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
Existing recovery deadlines are not shortened. Expose recovery carryover as a
separate gauge without changing reason bit meanings.

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
