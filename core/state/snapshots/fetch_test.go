package snapshots

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
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
	catalog, err := PublishSignedSnapshotCatalog(source, priv)
	if err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	manifestPath := filepath.Join(source, catalog.ManifestPath)
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
	catalog, err := PublishSignedSnapshotCatalog(source, priv)
	if err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/")
		if rel != SnapshotCatalogFile && rel != catalog.ManifestPath {
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
	catalog, err := PublishSignedSnapshotCatalog(source, priv)
	if err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/")
		if rel != SnapshotCatalogFile && rel != catalog.ManifestPath {
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

func TestFetchSnapshotSegmentResumesPartialRange(t *testing.T) {
	ref, payload := snapshotFetchTestSegment(t)
	dest := t.TempDir()
	offset := len(payload) / 2
	writeSnapshotSegmentPartialForTest(t, dest, ref, payload[:offset])

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/"+ref.Path {
			http.NotFound(w, r)
			return
		}
		requests.Add(1)
		if got, want := r.Header.Get("Range"), fmt.Sprintf("bytes=%d-", offset); got != want {
			http.Error(w, "missing expected range", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, len(payload)-1, len(payload)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)-offset))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[offset:])
	}))
	defer server.Close()

	downloaded, bytes, err := fetchSnapshotSegment(context.Background(), server.Client(), server.URL, dest, ref)
	if err != nil {
		t.Fatalf("fetchSnapshotSegment: %v", err)
	}
	if !downloaded || bytes != uint64(len(payload)-offset) {
		t.Fatalf("fetch result downloaded=%v bytes=%d, want true/%d", downloaded, bytes, len(payload)-offset)
	}
	if requests.Load() != 1 {
		t.Fatalf("range requests = %d, want 1", requests.Load())
	}
	assertFetchedSnapshotSegmentForTest(t, dest, ref, payload)
}

func TestFetchSnapshotSegmentFallsBackWhenServerIgnoresRange(t *testing.T) {
	ref, payload := snapshotFetchTestSegment(t)
	dest := t.TempDir()
	offset := len(payload) / 2
	writeSnapshotSegmentPartialForTest(t, dest, ref, payload[:offset])

	var gotRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/"+ref.Path {
			http.NotFound(w, r)
			return
		}
		gotRange = r.Header.Get("Range")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	downloaded, bytes, err := fetchSnapshotSegment(context.Background(), server.Client(), server.URL, dest, ref)
	if err != nil {
		t.Fatalf("fetchSnapshotSegment: %v", err)
	}
	if !downloaded || bytes != uint64(len(payload)) {
		t.Fatalf("fetch result downloaded=%v bytes=%d, want true/%d", downloaded, bytes, len(payload))
	}
	if want := fmt.Sprintf("bytes=%d-", offset); gotRange != want {
		t.Fatalf("request range = %q, want %q", gotRange, want)
	}
	assertFetchedSnapshotSegmentForTest(t, dest, ref, payload)
}

func TestFetchSnapshotSegmentRejectsMismatchedContentRange(t *testing.T) {
	ref, payload := snapshotFetchTestSegment(t)
	dest := t.TempDir()
	offset := len(payload) / 2
	writeSnapshotSegmentPartialForTest(t, dest, ref, payload[:offset])

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/"+ref.Path {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)-offset))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[offset:])
	}))
	defer server.Close()

	_, _, err := fetchSnapshotSegment(context.Background(), server.Client(), server.URL, dest, ref)
	if err == nil || !strings.Contains(err.Error(), "Content-Range") {
		t.Fatalf("fetchSnapshotSegment error = %v, want Content-Range rejection", err)
	}
	if _, err := os.Stat(filepath.Join(dest, ref.Path)); !os.IsNotExist(err) {
		t.Fatalf("final segment stat error = %v, want no published segment", err)
	}
}

func TestFetchSnapshotSegmentPromotesCompletePartialWithoutRequest(t *testing.T) {
	ref, payload := snapshotFetchTestSegment(t)
	dest := t.TempDir()
	writeSnapshotSegmentPartialForTest(t, dest, ref, payload)

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "segment should not be requested", http.StatusInternalServerError)
	}))
	defer server.Close()

	downloaded, bytes, err := fetchSnapshotSegment(context.Background(), server.Client(), server.URL, dest, ref)
	if err != nil {
		t.Fatalf("fetchSnapshotSegment: %v", err)
	}
	if downloaded || bytes != 0 {
		t.Fatalf("fetch result downloaded=%v bytes=%d, want false/0", downloaded, bytes)
	}
	if requests.Load() != 0 {
		t.Fatalf("segment requests = %d, want 0", requests.Load())
	}
	assertFetchedSnapshotSegmentForTest(t, dest, ref, payload)
}

func snapshotFetchTestSegment(t *testing.T) (SegmentRef, []byte) {
	t.Helper()
	source, _, _ := writeVerifiableHistoryManifest(t)
	manifest, err := LoadProductionManifest(source)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	if len(manifest.Segments) == 0 {
		t.Fatal("test manifest has no segments")
	}
	ref := manifest.Segments[0]
	payload, err := os.ReadFile(filepath.Join(source, ref.Path))
	if err != nil {
		t.Fatalf("read source segment: %v", err)
	}
	if ref.Size != uint64(len(payload)) || len(payload) < 2 {
		t.Fatalf("source segment size=%d bytes=%d, want matching non-trivial payload", ref.Size, len(payload))
	}
	return ref, payload
}

func writeSnapshotSegmentPartialForTest(t *testing.T, dir string, ref SegmentRef, data []byte) {
	t.Helper()
	partial := snapshotSegmentPartialPath(filepath.Join(dir, ref.Path), ref.Checksum)
	if err := os.MkdirAll(filepath.Dir(partial), 0o755); err != nil {
		t.Fatalf("mkdir partial parent: %v", err)
	}
	if err := os.WriteFile(partial, data, 0o644); err != nil {
		t.Fatalf("write partial: %v", err)
	}
}

func assertFetchedSnapshotSegmentForTest(t *testing.T, dir string, ref SegmentRef, want []byte) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatalf("read fetched segment: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("fetched segment bytes mismatch")
	}
	partial := snapshotSegmentPartialPath(filepath.Join(dir, ref.Path), ref.Checksum)
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Fatalf("partial segment stat error = %v, want removed", err)
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
