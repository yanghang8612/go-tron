// Package pointread defines the optional snapshot/cursor capabilities used by
// sorted point-read workloads. The interfaces live outside rawdb and
// blockbuffer so storage engines can implement them without reversing package
// dependencies.
package pointread

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
