//go:build !windows

package snapshots

import (
	"errors"
	"os"
	"syscall"
)

func syncSnapshotDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		// Some FUSE filesystems do not support directory fsync. The manifest
		// file itself was still synced before the atomic rename.
		if errors.Is(err, os.ErrInvalid) {
			return nil
		}
		if pathErr, ok := err.(*os.PathError); ok && pathErr.Err == syscall.EINVAL {
			return nil
		}
		return err
	}
	return nil
}
