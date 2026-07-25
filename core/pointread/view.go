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
