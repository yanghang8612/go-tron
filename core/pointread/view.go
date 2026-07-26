// Package pointread defines optional short-lived point-read capabilities used
// by storage hot paths. Keeping the interfaces outside rawdb and blockbuffer
// lets engines implement them without reversing package dependencies.
package pointread

// View holds a storage engine's read-side lifecycle lease across a burst of
// exact-key lookups. Implementations invoke fn synchronously; value is valid
// only until fn returns. Get is safe for concurrent use unless an engine
// documents otherwise; callers must wait for all reads before calling Close.
type View interface {
	Get(key []byte, fn func(value []byte) error) error
	Close() error
}

// Viewer is an optional storage capability. NewPointReadView does not promise
// a snapshot: callers that require an MVCC-stable sequence must use a different
// abstraction. It only amortizes engine lifecycle coordination across reads.
type Viewer interface {
	NewPointReadView() (View, error)
}

// Cursor performs callback-scoped exact-key reads through one reusable ordered
// cursor. A Cursor is owned by one caller and is not safe for concurrent use.
// Implementations must invoke fn synchronously and keep value valid until fn
// returns.
type Cursor interface {
	View(key []byte, fn func(value []byte) error) (bool, error)
	Close() error
}

// Snapshot is a stable point-in-time view of a durable key-value store.
// NewCursor restricts the cursor to keys beginning with prefix.
type Snapshot interface {
	NewCursor(prefix []byte) (Cursor, error)
	Close() error
}

// Snapshotter is an optional durable-store capability. Implementations keep
// the returned snapshot usable until Snapshot.Close, even if ordinary writes
// continue concurrently.
type Snapshotter interface {
	NewPointReadSnapshot() (Snapshot, error)
}

// CapacitySnapshotter optionally reserves storage for the caller's known
// number of independently owned cursors. It is semantically identical to
// Snapshotter; the hint only lets an engine coallocate short-lived cursor and
// bound state for one parallel read session.
type CapacitySnapshotter interface {
	NewPointReadSnapshotWithCapacity(cursors int) (Snapshot, error)
}

// CommitmentParentView resolves split physical keys against the parent state
// visible to one commitment fold. stable reports whether value may be retained
// after fn returns.
type CommitmentParentView interface {
	GetKeyParts(first, second []byte, fn func(value []byte, stable bool) error) (bool, error)
	Close() error
}

// CommitmentParentViewer is implemented by layered stores that can amortize
// durable point-read lifecycle coordination across one commitment fold.
type CommitmentParentViewer interface {
	NewCommitmentParentView() (CommitmentParentView, error)
}

// CommitmentParentSession resolves split physical keys against a stable parent
// state. Each reader index is exclusively owned by one fold worker, allowing an
// implementation to reuse a non-thread-safe ordered cursor without a lock.
// stable reports whether value may be retained after fn returns.
type CommitmentParentSession interface {
	ViewKeyParts(reader int, first, second []byte, fn func(value []byte, stable bool) error) (bool, error)
	Close() error
}

// CommitmentParentSessioner is discovered structurally by rawdb. Unsupported
// stores simply omit this interface and retain ordinary point reads.
type CommitmentParentSessioner interface {
	NewCommitmentParentReadSession(readers int) (CommitmentParentSession, error)
}
