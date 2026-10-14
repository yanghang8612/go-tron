# go-tron bounded snapshot Get cursor

Upstream module: `github.com/cockroachdb/pebble v1.1.5`.
Module checksum: `h1:5AAWCBWbat0uE0blr8qzufZP5tBjkRyy/jWe1QWLnvw=`.
Upstream go.mod checksum: `h1:17wO9el1YEigxkP/YtV8NtCivQDgoCyBg5c4VR/eOWo=`.
Original license is retained in LICENSE. Upstream has no separate NOTICE file.

UPSTREAM_FILES.json lists original source paths and SHA256 values. These files
are unchanged. The copy contains the runtime dependency closure for the root
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
go test -C third_party/pebble . -run TestGetCursor -count=1 -timeout 60s
go test -C third_party/pebble . -race -run TestGetCursor -count=1 -timeout 90s
```

The original Snapshot.Get is retained as the differential oracle. There is no
DB/batch cursor, merged-iterator replacement, format change or memory-budget
increase. GetCursor errors that were already reported by an operation are not
re-reported by Close; fresh cleanup errors still propagate.
