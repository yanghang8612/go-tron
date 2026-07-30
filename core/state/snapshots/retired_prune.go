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

type RetiredSegmentFile struct {
	Path string
	Size uint64
}

type RetiredSegmentFileInspection struct {
	RetiredSegments    int
	FilesPresent       int
	FilesMissing       int
	FilesSkippedActive int
	BytesPresent       uint64
	PresentFiles       []RetiredSegmentFile
}

// InspectRetiredSegmentFiles reports physical files that are still present for
// manifest-retired segment refs without deleting anything.
func InspectRetiredSegmentFiles(dir string) (*RetiredSegmentFileInspection, error) {
	return inspectRetiredSegmentFiles(dir, "retired inspect")
}

// PruneRetiredSegmentFiles removes physical files referenced only by the
// manifest's retired list. It leaves manifest.json and snapshot-catalog.json
// untouched so already signed catalogs keep authenticating the active view.
func PruneRetiredSegmentFiles(dir string) (*PruneRetiredSegmentFilesResult, error) {
	inspection, err := inspectRetiredSegmentFiles(dir, "retired prune")
	if err != nil {
		return nil, err
	}
	result := &PruneRetiredSegmentFilesResult{
		RetiredSegments:    inspection.RetiredSegments,
		FilesMissing:       inspection.FilesMissing,
		FilesSkippedActive: inspection.FilesSkippedActive,
	}
	for _, file := range inspection.PresentFiles {
		if err := os.Remove(filepath.Join(dir, file.Path)); err != nil {
			return result, fmt.Errorf("snapshots: remove retired segment %q: %w", file.Path, err)
		}
		result.FilesDeleted++
		result.BytesDeleted += file.Size
		result.DeletedPaths = append(result.DeletedPaths, file.Path)
	}
	return result, nil
}

func inspectRetiredSegmentFiles(dir, operation string) (*RetiredSegmentFileInspection, error) {
	if dir == "" {
		return nil, errors.New("snapshots: retired segment prune directory is empty")
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	if _, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{}); err != nil {
		return nil, fmt.Errorf("snapshots: active segment preflight failed before %s: %w", operation, err)
	}

	active := make(map[string]struct{}, len(manifest.Segments))
	for _, ref := range manifest.Segments {
		active[ref.Path] = struct{}{}
	}

	result := &RetiredSegmentFileInspection{}
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
		result.FilesPresent++
		size := uint64(stat.Size())
		result.BytesPresent += size
		result.PresentFiles = append(result.PresentFiles, RetiredSegmentFile{Path: ref.Path, Size: size})
	}
	return result, nil
}
