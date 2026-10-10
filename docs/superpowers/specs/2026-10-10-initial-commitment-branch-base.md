# Controlled first commitment branch base

The October 10 hot-store work first removes history mover/index serialization,
adds authenticated serial shared-chunk reuse, and bounds archive merges. The
next stage externalizes the complete legacy commitment branch table once.
Production currently has neither a branch base nor a rotation marker. Recheck
that state before activation; a successor rotation is outside this mode.

Use an explicit, default-off `initial-only` policy. It suppresses every general
latest build while enabled, including transient network-idle periods, and never
starts generation 2. Existing account/KV/code/checkpoint files and watermarks
remain authoritative; their refresh is postponed. This is a controlled migration
policy, not a long-term bound on the mutable delta.

Begin retains insertSessionGate -> chainmu, async drain, flush, durable rotation
marker, then redirects writes to generation 1. The legacy table is immutable
while import continues. Build only root and branch families. Reuse an already
published exact family after validating its five production files (root seg/bt,
branch seg/lidx/bt), hashes, ranges and bindings; corruption is an error, never
an instruction to overwrite a published artifact. No catalog-wide history scan.

Accept retains exact marker, canonical boundary, solidification and independent
root-branch verification. Persist the base/rotation swap before deleting legacy.
New branch B-trees sample every 32 rows, reducing a typical point-read block
from about 70 KiB to 17.5 KiB. Other datasets retain 128; readers and validation
use the file header, so existing published indexes need no rewrite. This spends
four times the sparse-index memory, still subject to the 256 MiB file cap and
the resource admission budget. It does not eliminate the negative delta lookup
or guarantee parity with the hot-store point read.
A cleanup-only entry point completes deletion after an accept/crash without
starting another rotation; it first verifies the accepted base and synchronizes
its marker. Delta-generation discovery seeks past each generation rather than
scanning the retained mutable generation. Logical range deletion is separate
from physical compaction and must not be reported as disk space recovered.

The initial build takes the shared heavy-work lease for the entire operation.
Admission requires fresh storage/CPU/memory signals and known free space with
an explicit output reserve above the normal free-space floor. A resource monitor
can cancel before the floor is exhausted. Cancellation preserves rotation and
legacy; construction is streaming but not checkpointed inside a file. Recovery
uses actual work duration, without delaying ordinary archive work after the lease
is released. Enable only after phase-one history progress has been measured.

Validation covers real writer/root equivalence, solid/canonical rejection,
post-publication reuse, post-accept cleanup retry, generation-seek complexity,
lease/pressure/space deferrals, and no periodic/full-latest work after a restart
or a sync-status change. A cold point-read replay must justify production
activation; synthetic local benchmarks alone do not prove production throughput.
