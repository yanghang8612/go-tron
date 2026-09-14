package snapshots

// Frozen publication and seed entry points preserve the full old validation
// work in the Publish+first-load baseline. The unchanged filesystem, cache
// ownership, budget and clone helpers remain shared with the candidate.
import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

func frozenPublishValidationManifest(dir string, manifest *Manifest) error {
	if manifest == nil {
		return errors.New("snapshots: nil manifest")
	}
	if manifest.Version == 0 {
		manifest.Version = CurrentManifestVersion
	}
	if manifest.Generation == 0 {
		manifest.Generation = 1
	}
	normalizeChainIdentity(manifest.Chain)
	if manifest.PublishedUnix == 0 {
		manifest.PublishedUnix = time.Now().Unix()
	}
	sortSegments(manifest.Segments)
	sortSegments(manifest.Retired)
	if err := frozenManifestValidate(manifest); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// Every publication is reread for byte-identity cache checks. Compact JSON
	// reduces both that input and serialization work without changing its schema.
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".manifest-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(dir, ManifestFile)); err != nil {
		return err
	}
	// The manifest is the publication boundary for cold coverage. Persist the
	// directory entry after the atomic rename before a caller may use that
	// coverage to delete duplicate hot rows.
	if err := syncSnapshotDir(dir); err != nil {
		return err
	}
	frozenRememberValidationManifest(data, manifest)
	return nil
}

func frozenRememberValidationManifest(data []byte, m *Manifest) {
	budget, err := manifestCacheConfiguredBudget()
	if err != nil {
		manifestCachePublicationBypasses.Inc(1)
		return
	}
	loadedManifestCache.frozenRetainValidationPublished(data, m, budget)
}

func (c *manifestDecodeCache) frozenRetainValidationPublished(data []byte, m *Manifest, budget uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resizeLocked(budget)
	// An idempotent progress publication, or a reader that won the race after
	// rename, may already have populated these exact bytes. Keep its detached
	// entry rather than cloning the whole catalog again.
	if c.manifest != nil && bytes.Equal(data, c.data) {
		manifestCachePublicationBypasses.Inc(1)
		return
	}
	c.clearLocked()
	manifestCacheCandidate.Update(0)
	if budget == 0 || !manifestStringsValidUTF8(m) {
		manifestCachePublicationBypasses.Inc(1)
		return
	}
	// PublishManifest permits base-valid legacy JSON history. Keep its distinct
	// production validation error exactly as a normal decode-cache miss does.
	if c.retainDecodedLocked(data, m, frozenValidateProductionHistorySegments(m), true) {
		manifestCachePublicationSeeds.Inc(1)
	} else {
		manifestCachePublicationBypasses.Inc(1)
	}
}
