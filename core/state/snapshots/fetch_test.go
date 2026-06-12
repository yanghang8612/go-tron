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
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

func TestFetchRemoteSnapshotDownloadsSegmentsConcurrently(t *testing.T) {
	source, identity, _ := writeVerifiableHistoryManifest(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := PublishSignedSnapshotCatalog(source, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/")
		if rel != SnapshotCatalogFile && rel != ManifestFile {
			current := inFlight.Add(1)
			for {
				max := maxInFlight.Load()
				if current <= max || maxInFlight.CompareAndSwap(max, current) {
					break
				}
			}
			if current >= 2 {
				releaseOnce.Do(func() { close(release) })
			}
			select {
			case <-release:
			case <-time.After(500 * time.Millisecond):
			}
			defer inFlight.Add(-1)
		}
		http.FileServer(http.Dir(source)).ServeHTTP(w, r)
	}))
	defer server.Close()

	result, err := FetchRemoteSnapshot(context.Background(), FetchRemoteSnapshotOptions{
		BaseURL:                server.URL,
		Dir:                    t.TempDir(),
		Expected:               identity,
		TrustedKeys:            []ed25519.PublicKey{pub},
		MaxConcurrentDownloads: 2,
	})
	if err != nil {
		t.Fatalf("FetchRemoteSnapshot: %v", err)
	}
	if result.FilesDownloaded != 5 {
		t.Fatalf("files downloaded = %d, want catalog+manifest+3 segments", result.FilesDownloaded)
	}
	if maxInFlight.Load() < 2 {
		t.Fatalf("max concurrent segment downloads = %d, want at least 2", maxInFlight.Load())
	}
}

func TestNormaliseSnapshotFetchConcurrency(t *testing.T) {
	tests := []struct {
		name        string
		concurrency int
		want        int
		wantErr     string
	}{
		{name: "default", concurrency: 0, want: snapshotFetchDefaultConcurrency},
		{name: "explicit", concurrency: 2, want: 2},
		{name: "negative", concurrency: -1, wantErr: "must be non-negative"},
		{name: "too high", concurrency: snapshotFetchMaxConcurrencyLimit + 1, wantErr: "exceeds maximum"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normaliseSnapshotFetchConcurrency(tt.concurrency)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("normaliseSnapshotFetchConcurrency error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("normaliseSnapshotFetchConcurrency: %v", err)
			}
			if got != tt.want {
				t.Fatalf("normaliseSnapshotFetchConcurrency = %d, want %d", got, tt.want)
			}
		})
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
