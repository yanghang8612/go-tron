package snapshots

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DefaultStaleTempFileAge leaves a full day for an independently running
// snapshot fetch/build command before startup considers its hidden scratch
// files abandoned.
const DefaultStaleTempFileAge = 24 * time.Hour

// StaleTempCleanupResult summarizes abandoned atomic-write scratch reclaimed
// before the in-process snapshot manager and builders start.
type StaleTempCleanupResult struct {
	Files uint64
	Bytes uint64
}

// CleanupStaleTempFiles removes old hidden *.tmp files below dir. Snapshot
// writers create these files beside their final output and atomically rename
// them only after validation; no manifest may reference the hidden scratch
// name. Symlinks and resumable/non-temporary files (including *.partial) are
// deliberately ignored.
func CleanupStaleTempFiles(dir string, olderThan time.Duration) (StaleTempCleanupResult, error) {
	var result StaleTempCleanupResult
	if dir == "" {
		return result, nil
	}
	cutoff := time.Now().Add(-olderThan)
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) && path == dir {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		name := entry.Name()
		if !strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".tmp") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.Mode().IsRegular() || (olderThan > 0 && info.ModTime().After(cutoff)) {
			return nil
		}
		if err := os.Remove(path); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		result.Files++
		if info.Size() > 0 {
			result.Bytes += uint64(info.Size())
		}
		return nil
	})
	return result, err
}
