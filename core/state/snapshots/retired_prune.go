package snapshots

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type PruneRetiredSegmentFilesResult struct {
	RetiredSegments    int
	FilesDeleted       int
	FilesMissing       int
	FilesSkippedActive int
	BytesDeleted       uint64
	DeletedPaths       []string
}

// PruneRetiredSegmentFiles removes physical files referenced only by the
// manifest's retired list. It leaves manifest.json and snapshot-catalog.json
// untouched so already signed catalogs keep authenticating the active view.
func PruneRetiredSegmentFiles(dir string) (*PruneRetiredSegmentFilesResult, error) {
	if dir == "" {
		return nil, errors.New("snapshots: retired segment prune directory is empty")
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	if _, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{}); err != nil {
		return nil, fmt.Errorf("snapshots: active segment preflight failed before retired prune: %w", err)
	}

	active := make(map[string]struct{}, len(manifest.Segments))
	for _, ref := range manifest.Segments {
		active[ref.Path] = struct{}{}
	}

	result := &PruneRetiredSegmentFilesResult{}
	seenRetired := make(map[string]struct{}, len(manifest.Retired))
	for _, ref := range manifest.Retired {
		result.RetiredSegments++
		if _, ok := active[ref.Path]; ok {
			result.FilesSkippedActive++
			continue
		}
		if _, ok := seenRetired[ref.Path]; ok {
			continue
		}
		seenRetired[ref.Path] = struct{}{}

		abs := filepath.Join(dir, ref.Path)
		stat, err := os.Lstat(abs)
		if os.IsNotExist(err) {
			result.FilesMissing++
			continue
		}
		if err != nil {
			return result, fmt.Errorf("snapshots: stat retired segment %q: %w", ref.Path, err)
		}
		if stat.IsDir() {
			return result, fmt.Errorf("snapshots: retired segment %q is a directory", ref.Path)
		}
		if err := os.Remove(abs); err != nil {
			return result, fmt.Errorf("snapshots: remove retired segment %q: %w", ref.Path, err)
		}
		result.FilesDeleted++
		result.BytesDeleted += uint64(stat.Size())
		result.DeletedPaths = append(result.DeletedPaths, ref.Path)
	}
	return result, nil
}
