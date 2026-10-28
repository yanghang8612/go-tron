# Bounded lightweight history bootstrap and fallback

Production is read-only during this work. The current release is 3e456e75.
Never deploy or restart it, reset its data, or mutate its manifest in experiments.
Use the existing fixed H45,836,827 copy for HTTP performance and value comparison.
Prioritize real timing and return values; do not repeat the billion-row state
scan or make full export/restore an experimental prerequisite.

The 11.57% observation was an early progress sample, not a fixed coverage cap.
Maintenance admits only 64 dictionary attempts through the optional merge gate
and 4x work recovery. A successful small-family publication returns before
metadata can run, starving that family for the entire small bootstrap.

Reuse a fully physically authenticated broad route's complete small-family
memberships for identical current history/accessor pair IDs. This migration can
reuse all covered sources without canonical source reads; new dictionary scans
retain separate source, byte, time, key and memory budgets. A range overlap is
only an admission hint. Preserve positive memberships and empty-family proofs.
Metadata may migrate only a covered source whose complete metadata family is
empty. Nonempty, uncovered and replaced pairs must use canonical dictionaries.
Strict import independently derives source membership as before. Projection
cache hits still authenticate current broad bytes. Optional capacity exhaustion
must not become a hard maintenance failure.

Alternate small and metadata maintenance opportunities. Allow page-sized cheap
metadata migration, but independently bound real source dictionary I/O. Work
budgets publish completed source prefixes, never partial absence proofs.

Uncovered authoritative fallback reuses parsed sparse fences and its current
key-directory group within one open descriptor and one batch. Keep all existing
CRC/bounds, cancellation, range, record and pair binding checks. Never persist
this cache across opens. Canonical state, MuHash elements, history formats and
frontiers do not change.

Compare the deployed reader and candidate with identical routing coverage first;
measure maintenance seeding and completed indexes separately. Record full HTTP
latency, exact values, CPU/allocations/reads, per-pass source coverage and costs.
Fresh process is not controlled OS-cold; report it accurately. No global cache
eviction or repeated full-state scan. Focused proof/corruption/replacement,
small restore/history, race and vet tests guard the changed paths.
