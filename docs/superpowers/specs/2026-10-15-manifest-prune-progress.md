# Immutable catalog reuse for completed prune progress

The 10/15 mainnet sample has a 35.6MB manifest and warm decode-cache hits, yet
metadata accounts for about 41% of observed cold lifecycle work. A completed
prune update previously loaded detached full catalogs, sorted/validated them,
serialized all JSON, then cloned all catalog containers and strings to seed the
cache. Even unchanged progress republished the whole catalog.

## Restricted progress transformation

Only UpdateHotPruneProgress and UpdateStateChangeIndexPruneProgress use this
private path. Every call first loads and authenticates all current file bytes
with the existing production loader. It advances only the three monotonic
completed prune cursors, preserving all other Progress fields, catalog rows,
visible bounds, generation/time, Chain identity and staging reset epoch.

If the just-authenticated immutable generation is still the resident validated
production cache entry, raw JSON is valid UTF8 and normal publication header
defaults are already set, reuse catalog validation and sorted order. Copy the
small Manifest header and Progress; share only immutable Chain/catalog strings,
rows and lookup metadata. The derived generation never escapes through a
mutable API. Public loads still clone containers; old borrowed generations are
never modified. Disabled/rejected/replaced cache entries, invalid UTF8 and legacy
unset publication headers fall back to ordinary owned publication validation.
A changed catalog still requires full normal validation. There is no public
trusted-publication API and no file identity/mtime shortcut.

An unchanged cursor returns only after fresh full-file authentication and a
successful directory fsync, without rewriting the manifest. This also repairs
the case where a prior rename succeeded but its directory fsync failed; repeated
failures must continue returning errors until the sync succeeds. This intentionally avoids normalizing/reseeding an
otherwise unchanged valid publication. Missing manifests retain the old no-op
behavior; malformed/unreadable files and invalid cache configuration still fail.

## Publication and concurrency

Actual updates retain complete JSON serialization and the same temp write,
file fsync, close, atomic rename and directory fsync sequence. Cache seeding
happens only after that succeeds. Progress cannot grant additional coverage and
runs after completed hot deletion just as before. Cold publish-before-prune,
segment authentication and destructive guard decisions are unchanged. No
progress is deferred, and restart progress remains in manifest.json.

One simple process-wide mutex serializes progress load/modify/publish against
public PublishManifest writes. It is bounded and has no directory-lock registry.
Readers remain lock free with respect to this writer mutex, using existing
cache synchronization. This does not establish cross-process CAS or a general
multi-writer transaction: external writers still require operational exclusion.
Public PublishManifest intentionally retains its existing complete caller
publication semantics; a caller supplying an old complete manifest may replace
newer progress, just as before. The progress updater itself cannot overwrite a
fresh catalog published by a concurrent in-process public writer.

## Measurement and tests

BenchmarkManifestProgressMaintenance runs unchanged on ff6f0820. It measures
complete advancing/no-op UpdateHotPruneProgress and public Publish+first Load on
70,198 active and 92,667 retired synthetic rows, with normal durable writes and
cache behavior. Full JSON writes remain a fixed cost: this change does not claim
incremental disk format or avoidance of reading the current file.

Tests compare exact output with full publication, restart results, public
mutation isolation and old-generation immutability; cover cache disable/reject,
invalid UTF8, unset headers, same-length current file replacement, corrupt file
and invalid-config errors, no-op inode preservation, concurrent full writes,
rename/directory-fsync failure, same-cursor durability retries, cache nonseeding
and temp cleanup. Run complete snapshots/pruning
packages and relevant race tests. Native and local benchmark results must be
reported separately from live sustained throughput.
