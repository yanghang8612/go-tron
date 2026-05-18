//go:build !sapling

package zksnark

// Available reports whether the Sapling Pedersen backend is linked into this
// binary.
func Available() bool {
	return false
}

// Combine is the no-CGO fallback: returns ErrPedersenUnimplemented. Build
// with `-tags=sapling` to link the cgo backend.
func Combine(depth int, left, right PedersenHash) (PedersenHash, error) {
	_ = depth
	_ = left
	_ = right
	return PedersenHash{}, ErrPedersenUnimplemented
}

// Uncommitted is the no-CGO fallback. See Combine.
func Uncommitted() (PedersenHash, error) {
	return PedersenHash{}, ErrPedersenUnimplemented
}
