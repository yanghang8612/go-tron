package snapshots

import (
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestPublishVerifySignedSnapshotCatalog(t *testing.T) {
	dir, identity, _ := writeVerifiableHistoryManifest(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	catalog, err := PublishSignedSnapshotCatalog(dir, priv)
	if err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	if catalog.ManifestPath != ManifestFile || catalog.ManifestChecksum == "" || catalog.Signature == "" {
		t.Fatalf("catalog = %+v", catalog)
	}
	loaded, report, err := VerifySignedSnapshotCatalog(dir, identity, []ed25519.PublicKey{pub})
	if err != nil {
		t.Fatalf("VerifySignedSnapshotCatalog: %v", err)
	}
	if loaded.Signer != catalog.Signer || loaded.ManifestChecksum != catalog.ManifestChecksum {
		t.Fatalf("loaded catalog = %+v, want signer/checksum from %+v", loaded, catalog)
	}
	if report.ActiveSegments != 3 {
		t.Fatalf("active segments = %d, want 3", report.ActiveSegments)
	}
}

func TestVerifySignedSnapshotCatalogRejectsManifestTamper(t *testing.T) {
	dir, identity, _ := writeVerifiableHistoryManifest(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := PublishSignedSnapshotCatalog(dir, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	manifestPath := filepath.Join(dir, ManifestFile)
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, append(data, '\n'), 0o644); err != nil {
		t.Fatalf("tamper manifest: %v", err)
	}

	_, _, err = VerifySignedSnapshotCatalog(dir, identity, []ed25519.PublicKey{pub})
	if err == nil || !strings.Contains(err.Error(), "manifest checksum") {
		t.Fatalf("tampered manifest error = %v, want checksum mismatch", err)
	}
}

func TestVerifySignedSnapshotCatalogRejectsUntrustedSigner(t *testing.T) {
	dir, identity, _ := writeVerifiableHistoryManifest(t)
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	untrustedPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey untrusted: %v", err)
	}
	if _, err := PublishSignedSnapshotCatalog(dir, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}

	_, _, err = VerifySignedSnapshotCatalog(dir, identity, []ed25519.PublicKey{untrustedPub})
	if err == nil || !strings.Contains(err.Error(), "not trusted") {
		t.Fatalf("untrusted signer error = %v, want not trusted", err)
	}
}

func TestRestoreSnapshotFromVerifiedCatalog(t *testing.T) {
	dir, identity, _ := writeVerifiableHistoryManifest(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := PublishSignedSnapshotCatalog(dir, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	restored := rawdb.NewMemoryDatabase()

	result, err := RestoreSnapshotFromVerifiedCatalog(restored, dir, identity, []ed25519.PublicKey{pub})
	if err != nil {
		t.Fatalf("RestoreSnapshotFromVerifiedCatalog: %v", err)
	}
	if result.RestoredTxNum != 12 || result.ChangesRestored != 2 || result.TxRangesRestored != 2 {
		t.Fatalf("result = %+v, want txNum=12, 2 changes, 2 tx ranges", result)
	}
	if got, ok, err := rawdb.ReadStageProgress(restored, rawdb.StageSnapshotInstall); err != nil || !ok || got != 12 {
		t.Fatalf("SnapshotInstall progress = %d ok=%v err=%v, want 12", got, ok, err)
	}
}

func TestRestoreSnapshotFromVerifiedCatalogUsesVerifiedManifest(t *testing.T) {
	dir, identity, owner := writeVerifiableHistoryManifest(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := PublishSignedSnapshotCatalog(dir, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	_, manifest, report, err := VerifySignedSnapshotCatalogManifest(dir, identity, []ed25519.PublicKey{pub})
	if err != nil {
		t.Fatalf("VerifySignedSnapshotCatalogManifest: %v", err)
	}
	if err := PublishManifest(dir, NewManifestForChain(99, 99, nil, identity)); err != nil {
		t.Fatalf("swap manifest after verification: %v", err)
	}

	restored := rawdb.NewMemoryDatabase()
	result, err := restoreSnapshotFromVerifiedManifestWithOptions(restored, dir, manifest, *report, RestoreVerifiedSnapshotOptions{})
	if err != nil {
		t.Fatalf("restoreSnapshotFromVerifiedManifestWithOptions: %v", err)
	}
	if result.RestoredTxNum != 12 || result.ChangesRestored != 2 || result.TxRangesRestored != 2 {
		t.Fatalf("result = %+v, want verified manifest boundary txNum=12 with 2 changes and 2 tx ranges", result)
	}
	if got, ok, err := rawdb.ReadStateDomainChange(restored, 1, 1); err != nil || !ok || got.Owner != owner {
		t.Fatalf("restored state change = %+v ok=%v err=%v, want verified manifest change for owner %x", got, ok, err, owner)
	}
	if got, ok, err := rawdb.ReadStageProgress(restored, rawdb.StageSnapshotInstall); err != nil || !ok || got != 12 {
		t.Fatalf("SnapshotInstall progress = %d ok=%v err=%v, want verified manifest txNum 12", got, ok, err)
	}
}
