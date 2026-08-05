//go:build windows

package snapshots

// Windows does not support fsync on directory handles. Atomic rename remains
// the strongest publication primitive available to the snapshot manifest.
func syncSnapshotDir(string) error { return nil }
