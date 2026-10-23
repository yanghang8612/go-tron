# Canonical snapshot cursor retention

Baseline production is 708e20fc0a94a8c14b7fbbda69e0de5843f19dfe.
VM execution stays serial. The existing fixed snapshot cut, StateDB memory-hit
stack fast path, callback ownership and rollback/cleanup rules remain unchanged.

Only the canonical KeyValueCursorSnapshot factory selects 16 retained SST
point/range-deletion pairs. Generic, prefetch and commitment cursors default to
four. The first four retained pairs always use their original inline slots. Only a
compatible miss needing a fifth pair allocates the remaining fixed overflow
backing once; neither backing array moves or grows. Slot-owned filters and whole-Get leases
are preserved, including borrowed tombstone spans and merge values. There is
no early unlease on wrapper Close.

DirectHistory may own two such cursors: at most 32 pairs / 64 component handles
across hot and history engines. This does not bound bytes or change node budgets.
Incompatible options and compatible all-leased fallbacks are disjoint local
counts whose sum is the existing fallback count, including failed opens.
Role metrics publish only on Close. Default Get and L6 filtering do not change.

A predeclared untimed 4/8/16/32 capacity proof used real overlapping L0 14/16,
32 independent 128-read sessions, identical map and ordinary Snapshot.Get
oracles. The smallest bound reducing all-leased fallback by at least 90% in
both was 16. High16 still fell back 3996 times; 32 was not selected. This
diagnostic is not evidence of end-to-end or online speed.

Version timing compares exact baseline production with the selected candidate,
with identical fixtures and explicitly different test-only constructors. High
14/16 require at least 10% lifecycle improvement and 50% fewer SST constructions;
low3/4 and existing memory/short/scatter/canonical/VM controls guard regressions.
All samples and failed candidates remain evidence; no performance-based capacity
selection, state-format/consensus/cache-policy changes or parallel VM is allowed.
Nested Pebble normal/race checks run independently of root full Sapling tests.
