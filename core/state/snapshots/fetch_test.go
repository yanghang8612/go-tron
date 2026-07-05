package snapshots

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
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

func TestFetchRemoteSnapshotDoesNotPublishManifestOnSegmentFailure(t *testing.T) {
	source, identity, _ := writeVerifiableHistoryManifest(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := PublishSignedSnapshotCatalog(source, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/")
		if rel != SnapshotCatalogFile && rel != ManifestFile {
			http.Error(w, "segment unavailable", http.StatusInternalServerError)
			return
		}
		http.FileServer(http.Dir(source)).ServeHTTP(w, r)
	}))
	defer server.Close()

	dest := t.TempDir()
	_, err = FetchRemoteSnapshot(context.Background(), FetchRemoteSnapshotOptions{
		BaseURL:                server.URL,
		Dir:                    dest,
		Expected:               identity,
		TrustedKeys:            []ed25519.PublicKey{pub},
		MaxConcurrentDownloads: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "500 Internal Server Error") {
		t.Fatalf("FetchRemoteSnapshot error = %v, want segment download failure", err)
	}
	for _, rel := range []string{SnapshotCatalogFile, ManifestFile} {
		if _, statErr := os.Stat(filepath.Join(dest, rel)); !os.IsNotExist(statErr) {
			t.Fatalf("%s stat err = %v, want not exist after failed fetch", rel, statErr)
		}
	}
}

func TestFetchRemoteSnapshotDoesNotPublishManifestOnSemanticFailure(t *testing.T) {
	source := t.TempDir()
	identity := ChainIdentity{
		ChainID:     1,
		NetworkID:   2,
		GenesisHash: strings.Repeat("01", 32),
	}
	segRef, accessorRef, btreeRef := writeLatestBinaryCompanionManifestForTest(t, source)
	corruptLatestBinaryCompanionSegmentChecksum(t, source, &accessorRef)
	if err := PublishManifest(source, NewManifestForChain(1, 10, []SegmentRef{segRef, accessorRef, btreeRef}, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	publishUncheckedSignedSnapshotCatalogForTest(t, source, priv)
	server := httptest.NewServer(http.FileServer(http.Dir(source)))
	defer server.Close()

	dest := t.TempDir()
	_, err = FetchRemoteSnapshot(context.Background(), FetchRemoteSnapshotOptions{
		BaseURL:     server.URL,
		Dir:         dest,
		Expected:    identity,
		TrustedKeys: []ed25519.PublicKey{pub},
	})
	if err == nil || !strings.Contains(err.Error(), "segment checksum mismatch") {
		t.Fatalf("FetchRemoteSnapshot stale sidecar error = %v, want segment checksum mismatch", err)
	}
	for _, rel := range []string{SnapshotCatalogFile, ManifestFile} {
		if _, statErr := os.Stat(filepath.Join(dest, rel)); !os.IsNotExist(statErr) {
			t.Fatalf("%s stat err = %v, want not exist after semantic verification failure", rel, statErr)
		}
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

func publishUncheckedSignedSnapshotCatalogForTest(t *testing.T, dir string, priv ed25519.PrivateKey) *SnapshotCatalog {
	t.Helper()
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	manifestChecksum, err := checksumFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatalf("checksum manifest: %v", err)
	}
	catalog := &SnapshotCatalog{
		Version:          SnapshotCatalogVersion,
		PublishedUnix:    time.Now().Unix(),
		ManifestPath:     ManifestFile,
		ManifestChecksum: manifestChecksum,
		VisibleTxStart:   manifest.VisibleTxStart,
		VisibleTxEnd:     manifest.VisibleTxEnd,
		Chain:            cloneChainIdentity(manifest.Chain),
		SignatureScheme:  snapshotCatalogSignatureScheme,
		Signer:           hex.EncodeToString(priv.Public().(ed25519.PublicKey)),
	}
	payload, err := catalog.signaturePayload()
	if err != nil {
		t.Fatalf("catalog signature payload: %v", err)
	}
	catalog.Signature = hex.EncodeToString(ed25519.Sign(priv, payload))
	if err := writeSnapshotCatalog(dir, catalog); err != nil {
		t.Fatalf("write unchecked catalog: %v", err)
	}
	return catalog
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

func TestValidateRemoteSnapshotFetchConcurrency(t *testing.T) {
	tests := []struct {
		name        string
		concurrency int
		wantErr     string
	}{
		{name: "default", concurrency: 0},
		{name: "explicit", concurrency: 2},
		{name: "negative", concurrency: -1, wantErr: "must be non-negative"},
		{name: "too high", concurrency: snapshotFetchMaxConcurrencyLimit + 1, wantErr: "exceeds maximum"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRemoteSnapshotFetchConcurrency(tt.concurrency)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateRemoteSnapshotFetchConcurrency(%d): %v", tt.concurrency, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateRemoteSnapshotFetchConcurrency(%d) err = %v, want %q", tt.concurrency, err, tt.wantErr)
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

func TestValidateRemoteSnapshotBaseURL(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		wantErr string
	}{
		{name: "http", baseURL: "http://example.invalid/snapshots"},
		{name: "https", baseURL: "https://example.invalid/snapshots"},
		{name: "file", baseURL: "file:///tmp/snapshots", wantErr: "unsupported snapshot URL scheme"},
		{name: "missing host", baseURL: "https:///snapshots", wantErr: "snapshot URL host is required"},
		{name: "query", baseURL: "https://example.invalid/snapshots?token=secret", wantErr: "must not contain query or fragment"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRemoteSnapshotBaseURL(tt.baseURL)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateRemoteSnapshotBaseURL(%q): %v", tt.baseURL, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateRemoteSnapshotBaseURL(%q) err = %v, want %q", tt.baseURL, err, tt.wantErr)
			}
		})
	}
}
