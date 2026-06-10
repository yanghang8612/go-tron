package snapshots

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFetchRemoteSnapshot(t *testing.T) {
	source, identity, _ := writeVerifiableHistoryManifest(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := PublishSignedSnapshotCatalog(source, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	server := httptest.NewServer(http.FileServer(http.Dir(source)))
	defer server.Close()

	dest := t.TempDir()
	result, err := FetchRemoteSnapshot(context.Background(), FetchRemoteSnapshotOptions{
		BaseURL:     server.URL,
		Dir:         dest,
		Expected:    identity,
		TrustedKeys: []ed25519.PublicKey{pub},
	})
	if err != nil {
		t.Fatalf("FetchRemoteSnapshot: %v", err)
	}
	if result.Catalog == nil || result.Catalog.VisibleTxEnd != 12 {
		t.Fatalf("catalog = %+v, want visibleTxEnd=12", result.Catalog)
	}
	if result.Verification.ActiveSegments != 3 {
		t.Fatalf("active segments = %d, want 3", result.Verification.ActiveSegments)
	}
	if result.FilesDownloaded != 5 {
		t.Fatalf("files downloaded = %d, want catalog+manifest+3 segments", result.FilesDownloaded)
	}
	if result.BytesDownloaded == 0 {
		t.Fatal("bytes downloaded was zero")
	}
	if _, _, err := VerifySignedSnapshotCatalog(dest, identity, []ed25519.PublicKey{pub}); err != nil {
		t.Fatalf("VerifySignedSnapshotCatalog(dest): %v", err)
	}
}

func TestFetchRemoteSnapshotRejectsManifestChecksumMismatch(t *testing.T) {
	source, identity, _ := writeVerifiableHistoryManifest(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := PublishSignedSnapshotCatalog(source, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	manifestPath := filepath.Join(source, ManifestFile)
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, append(data, '\n'), 0o644); err != nil {
		t.Fatalf("tamper manifest: %v", err)
	}
	server := httptest.NewServer(http.FileServer(http.Dir(source)))
	defer server.Close()

	_, err = FetchRemoteSnapshot(context.Background(), FetchRemoteSnapshotOptions{
		BaseURL:     server.URL,
		Dir:         t.TempDir(),
		Expected:    identity,
		TrustedKeys: []ed25519.PublicKey{pub},
	})
	if err == nil || !strings.Contains(err.Error(), "manifest checksum") {
		t.Fatalf("error = %v, want manifest checksum mismatch", err)
	}
}

func TestSnapshotRemoteURLRejectsUnsafePath(t *testing.T) {
	for _, rel := range []string{"../manifest.json", "/manifest.json", "a/../manifest.json"} {
		if _, err := snapshotRemoteURL("https://example.invalid/snapshots", rel); err == nil {
			t.Fatalf("snapshotRemoteURL accepted unsafe path %q", rel)
		}
	}
	got, err := snapshotRemoteURL("https://example.invalid/snapshots", "history/a.seg")
	if err != nil {
		t.Fatalf("snapshotRemoteURL: %v", err)
	}
	if got != "https://example.invalid/snapshots/history/a.seg" {
		t.Fatalf("url = %q", got)
	}
}
