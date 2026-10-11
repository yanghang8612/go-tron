# Direct history database

The node writes historical state payload and indexes directly to a separate
Pebble database. Current state, canonical chain metadata and stage progress
remain in chaindata. Blockbuffer still owns recent reversible writes and flushes
solid history through the partitioned store.

The partition includes changesets, transaction ranges, shared chunks and bucket
metadata, postings, key directories, and the history-index stage singleton.
Existing cold snapshot formats and file leases remain unchanged.

Mixed physical batches persist and sync history before updating hot metadata.
This is ordered durability across two databases, not a transactional two-phase
commit. Existing commit-error and rollback behavior must remain tested. Writes
and dual snapshot creation share a mutex; long compactions do not hold it.

There is no runtime mover, ownership-route lifecycle, semantic certification,
claim, receipt or activation scan. Startup reads fixed-size layout markers and
a database pair ID. The ID prevents accidentally switching destinations after
source pages have already been deleted.

Existing data is converted offline with `gtron db move-history`. Old ownership
routes only choose which existing copy to retain. Pages are copied unchanged,
destination-synced, then immediately deleted and synced at source. Rerunning
resumes from the surviving source keys. No inventory, digest or content proof
is generated. Source ranges are compacted after conversion. Existing cold files
are neither rewritten nor authenticated by this operation.

During conversion the service and deployment timer stay stopped. An external
in-progress file blocks accidental starts. A completed-format marker allows
only binaries with direct-history capability, including on release rollback.
These guards never open or scan either database.

The old full posting/directory sweep remains disabled because its separate
snapshots can race with new imports. The ordinary bounded archive pruning path
is retained. Legacy staging helpers remain available only for offline conversion
and older diagnostic tests; normal node startup does not construct them.
