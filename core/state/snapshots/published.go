package snapshots

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	// Published snapshots are intentionally retained beyond the latest catalog
	// switch so a downloader holding the previous signed generation can finish
	// or resume its immutable segment requests.
	DefaultPublishedSnapshotRetain = 3
	DefaultPublishedSnapshotGrace  = 24 * time.Hour
)

type PublishedSnapshotManifest struct {
	Path     string
	Manifest *Manifest
}

type PrunePublishedSnapshotManifestsResult struct {
	Inspected int
	Retained  int
	Deleted   int
	Paths     []string
}

// LoadPublishedSnapshotManifests loads every complete immutable manifest
// generation. Temporary and unrelated files are ignored; a generation whose
// path does not match its exact contents is rejected rather than trusted as a
// segment-retention lease.
func LoadPublishedSnapshotManifests(dir string) ([]PublishedSnapshotManifest, error) {
	root := filepath.Join(dir, SnapshotPublishedDir)
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]PublishedSnapshotManifest, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "manifest-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		rel := filepath.ToSlash(filepath.Join(SnapshotPublishedDir, entry.Name()))
		if err := validateSnapshotCatalogManifestPath(rel); err != nil {
			return nil, err
		}
		data, err := os.ReadFile(filepath.Join(dir, rel))
		if err != nil {
			return nil, err
		}
		manifest, err := decodeProductionManifest(data)
		if err != nil {
			return nil, fmt.Errorf("snapshots: decode published manifest %q: %w", rel, err)
		}
		want, err := publishedSnapshotManifestPath(manifest.Generation, checksumBytes(data))
		if err != nil {
			return nil, err
		}
		if rel != want {
			return nil, fmt.Errorf("snapshots: published manifest path %q does not match contents, want %q", rel, want)
		}
		out = append(out, PublishedSnapshotManifest{Path: rel, Manifest: manifest})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Manifest.Generation == out[j].Manifest.Generation {
			return out[i].Path < out[j].Path
		}
		return out[i].Manifest.Generation < out[j].Manifest.Generation
	})
	return out, nil
}

// PrunePublishedSnapshotManifests expires old download leases. The catalog's
// current immutable manifest, the newest retain generations, and every
// generation within grace are always preserved. Segment reclamation happens in
// the subsequent retired-segment pass after the remaining leases are loaded.
func PrunePublishedSnapshotManifests(dir string, retain int, grace time.Duration) (*PrunePublishedSnapshotManifestsResult, error) {
	if retain <= 0 {
		retain = DefaultPublishedSnapshotRetain
	}
	if grace <= 0 {
		grace = DefaultPublishedSnapshotGrace
	}
	published, err := LoadPublishedSnapshotManifests(dir)
	if err != nil {
		return nil, err
	}
	result := &PrunePublishedSnapshotManifestsResult{Inspected: len(published)}
	if len(published) == 0 {
		return result, nil
	}
	currentPath := ""
	if catalog, err := LoadSnapshotCatalog(dir); err == nil {
		currentPath = catalog.ManifestPath
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	cutoff := time.Now().Add(-grace)
	newestStart := len(published) - retain
	if newestStart < 0 {
		newestStart = 0
	}
	for i, entry := range published {
		publishedAt := time.Unix(entry.Manifest.PublishedUnix, 0)
		keep := entry.Path == currentPath || i >= newestStart || publishedAt.After(cutoff)
		if keep {
			result.Retained++
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Path)); err != nil {
			return result, fmt.Errorf("snapshots: remove published manifest %q: %w", entry.Path, err)
		}
		result.Deleted++
		result.Paths = append(result.Paths, entry.Path)
	}
	return result, nil
}
