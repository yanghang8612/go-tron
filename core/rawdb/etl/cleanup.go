package etl

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const collectorTempPrefix = "gtron-etl-"

// CleanupStats describes stale collector scratch removed from one owned parent
// directory. Bytes are measured before removal and are intended for operator
// diagnostics rather than filesystem accounting guarantees.
type CleanupStats struct {
	Directories int
	Files       int
	Bytes       uint64
}

// CleanupStaleCollectors removes private collector directories left behind by
// an interrupted process. It deliberately ignores non-matching entries and
// symlinks, so callers can safely use it on a node-owned ETL parent directory.
func CleanupStaleCollectors(parent string) (CleanupStats, error) {
	var stats CleanupStats
	entries, err := os.ReadDir(parent)
	if errors.Is(err, fs.ErrNotExist) {
		return stats, nil
	}
	if err != nil {
		return stats, err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), collectorTempPrefix) {
			continue
		}
		path := filepath.Join(parent, entry.Name())
		var files int
		var bytes uint64
		if err := filepath.WalkDir(path, func(_ string, item fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if item.IsDir() {
				return nil
			}
			info, err := item.Info()
			if err != nil {
				return err
			}
			files++
			if info.Size() > 0 {
				bytes += uint64(info.Size())
			}
			return nil
		}); err != nil {
			return stats, err
		}
		if err := os.RemoveAll(path); err != nil {
			return stats, err
		}
		stats.Directories++
		stats.Files += files
		stats.Bytes += bytes
	}
	return stats, nil
}
