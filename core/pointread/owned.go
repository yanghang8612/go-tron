package pointread

// OwnedKeyValueReader is an optional ownership guarantee for successful Get
// results. When true, every nonempty result is exclusively owned by the caller:
// it aliases neither engine/reader storage nor any other Get result, remains
// valid after subsequent reads or reader Close, and may be mutated by the caller.
// Wrappers must not infer or forward this guarantee unless their own Get keeps
// that contract. It grants no snapshot, presence, or thread-safety guarantee.
//
// Callers may omit a defensive copy after Get succeeds. They must preserve the
// underlying presence checks, reads, errors and content validation.
type OwnedKeyValueReader interface {
	GetReturnsOwnedBytes() bool
}
