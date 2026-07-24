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
