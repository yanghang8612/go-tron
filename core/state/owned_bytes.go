package state

import "unsafe"

// ownedBytesString turns permanently transferred immutable bytes into a map
// string without copying. The returned string keeps the backing allocation
// reachable; callers must never mutate value after this call.
func ownedBytesString(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(value), len(value))
}

// ownedStringBytes exposes an immutable string as bytes without allocating.
// The returned slice must never be mutated. Retaining it keeps the string's
// backing allocation reachable.
func ownedStringBytes(value string) []byte {
	if len(value) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(value), len(value))
}
