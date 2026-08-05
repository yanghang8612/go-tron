package snapshots

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type PruneRetiredSegmentFilesResult struct {
	RetiredSegments       int
	FilesDeleted          int
	FilesMissing          int
	FilesSkippedActive    int
	FilesSkippedPublished int
	BytesDeleted          uint64
	DeletedPaths          []string
}

type RetiredSegmentFile struct {
	Path string
	Size uint64
}

type RetiredSegmentFileInspection struct {
	RetiredSegments       int
	FilesPresent          int
	FilesMissing          int
	FilesSkippedActive    int
	FilesSkippedPublished int
	BytesPresent          uint64
	PresentFiles          []RetiredSegmentFile
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
		RetiredSegments:       inspection.RetiredSegments,
		FilesMissing:          inspection.FilesMissing,
		FilesSkippedActive:    inspection.FilesSkippedActive,
		FilesSkippedPublished: inspection.FilesSkippedPublished,
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
	result := &RetiredSegmentFileInspection{RetiredSegments: len(manifest.Retired)}
	if len(manifest.Retired) == 0 {
		return result, nil
	}

	active := make(map[string]struct{}, len(manifest.Segments))
	for _, ref := range manifest.Segments {
		active[ref.Path] = struct{}{}
	}
	publishedActive := make(map[string]struct{})
	published, err := LoadPublishedSnapshotManifests(dir)
	if err != nil {
		return nil, fmt.Errorf("snapshots: load published manifest leases before %s: %w", operation, err)
	}
	for _, generation := range published {
		for _, ref := range generation.Manifest.Segments {
			publishedActive[ref.Path] = struct{}{}
		}
	}

	seenRetired := make(map[string]struct{}, len(manifest.Retired))
	for _, ref := range manifest.Retired {
		if _, ok := active[ref.Path]; ok {
			result.FilesSkippedActive++
			continue
		}
		if _, ok := publishedActive[ref.Path]; ok {
			result.FilesSkippedPublished++
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
	// Full companion verification is a deletion gate, not a periodic scrub.
	// SnapshotLifecycle calls this after every bounded catch-up build, so doing
	// an active-view scan when there is nothing to delete turns incremental
	// sync into an O(number of active history records) maintenance loop. Keep
	// the strict preflight immediately before returning deletion candidates.
	if len(result.PresentFiles) > 0 {
		if _, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{}); err != nil {
			return nil, fmt.Errorf("snapshots: active segment preflight failed before %s: %w", operation, err)
		}
	}
	return result, nil
}
