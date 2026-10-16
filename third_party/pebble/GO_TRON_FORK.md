# go-tron bounded snapshot Get cursor

Upstream module: `github.com/cockroachdb/pebble v1.1.5`.
Module checksum: `h1:5AAWCBWbat0uE0blr8qzufZP5tBjkRyy/jWe1QWLnvw=`.
Upstream go.mod checksum: `h1:17wO9el1YEigxkP/YtV8NtCivQDgoCyBg5c4VR/eOWo=`.
Original license is retained in LICENSE. Upstream has no separate NOTICE file.

UPSTREAM_FILES.json lists original source paths and SHA256 values, not patched
file hashes. All copied runtime files remain unchanged except get_iter.go,
whose October 16 patch is described below. The copy contains the runtime
dependency closure for the root
Pebble package plus go-tron's bloom use; all source/build-tag/platform variants
within those directories are retained. Broad upstream tests, fixtures, command
line tools and replay/metamorphic programs are omitted. Local additions are
get_cursor.go and its focused tests. The root go.mod replaces only this module;
no global module-cache files are changed.

The snapshot-only API, ownership, error attribution, fixed four-entry bound,
measurement limits and validation commands are documented in the parent repo's
`docs/superpowers/specs/2026-10-14-pebble-get-cursor.md`.

Focused nested-module checks (use the approved absolute Go1.25.5 and GOROOT):

```
go test -C third_party/pebble . -run 'TestGet(Cursor|Boundary)' -count=1 -timeout 60s
go test -C third_party/pebble . -race -run 'TestGet(Cursor|Boundary)' -count=1 -timeout 90s
```

Snapshot.Get is retained as a cursor differential oracle. Since both paths
share getIter, TestGetBoundaryScratchExpectedVersions independently checks
literal expected values across DB, snapshot, batch, cursor and merged iterator
reads. There is no
DB/batch cursor, merged-iterator replacement, format change or memory-budget
increase. GetCursor errors that were already reported by an operation are not
re-reported by Close; fresh cleanup errors still propagate.

## October 16: pooled Get boundary scratch

get_iter.go embeds one levelIterBoundaryContext in the already pooled getIter.
It resets that field before opening each L0 sublevel or non-L0 level instead of
allocating a separate context at each level. Only one levelIter is live at a
time. SST point/range iterators retain no pointer to the context, and the
existing Iterator.Close still clears the complete Get allocation before
returning it to the pool. DB.Get, Snapshot.Get, indexed Batch.Get and GetCursor
share this allocation change and preserve their existing resolver semantics.

The original get_iter.go hash in UPSTREAM_FILES.json remains the upstream
v1.1.5 hash; it must not be replaced with a hash of the patched file.
See docs/superpowers/specs/2026-10-16-pebble-get-boundary-scratch.md in the parent
repository for scope, measurements and validation.

## October 16: optional L6 filters for snapshot Get cursors

`GetCursorOptions.UseL6Filters` is opt-in and immutable per cursor; default Get
and ordinary DB/Snapshot.Get keep their prior policy. The existing getIter
resolver carries that bool only into L6 table options. Range-deletion iterators,
MVCC, merge resolution, four retained entries and ownership/error cleanup are
unchanged. The root adapter opts in for unbounded reads and separately enables
the existing compatible Bloom writer policy only in normal hot defaults. Legacy
SSTs without filters remain readable; no table version or provenance SHA in
UPSTREAM_FILES.json is replaced. See the root repository spec
`docs/superpowers/specs/2026-10-16-prefetch-l6-bloom.md` for costs and limits.
