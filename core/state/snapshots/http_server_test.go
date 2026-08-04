package snapshots

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapshotHTTPServerServesOnlyPublishedFilesWithRange(t *testing.T) {
	dir, identity, _ := writeVerifiableHistoryManifest(t)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	catalog, err := PublishSignedSnapshotCatalog(dir, privateKey)
	if err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	if len(manifest.Segments) == 0 {
		t.Fatal("manifest has no segments")
	}
	ref := manifest.Segments[0]
	payload, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if len(payload) < 4 {
		t.Fatalf("segment has only %d bytes", len(payload))
	}
	if err := os.WriteFile(filepath.Join(dir, "operator-secret"), []byte("secret"), 0o600); err != nil {
		t.Fatalf("write unpublished file: %v", err)
	}

	server := NewHTTPServer(HTTPServerConfig{Addr: "127.0.0.1:0", Dir: dir})
	if err := server.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer server.Stop()
	baseURL := "http://" + server.ListenAddr()

	assertSnapshotHTTPResponse(t, http.MethodGet, baseURL+"/"+SnapshotCatalogFile, "", http.StatusOK, "no-cache", nil)
	assertSnapshotHTTPResponse(t, http.MethodGet, baseURL+"/"+catalog.ManifestPath, "", http.StatusOK, "immutable", nil)
	assertSnapshotHTTPResponse(t, http.MethodGet, baseURL+"/"+ref.Path, "bytes=1-3", http.StatusPartialContent, "immutable", payload[1:4])
	assertSnapshotHTTPResponse(t, http.MethodHead, baseURL+"/"+ref.Path, "", http.StatusOK, "immutable", nil)
	assertSnapshotHTTPResponse(t, http.MethodGet, baseURL+"/operator-secret", "", http.StatusNotFound, "", nil)
	assertSnapshotHTTPResponse(t, http.MethodGet, baseURL+"/manifest.json", "", http.StatusNotFound, "", nil)
	assertSnapshotHTTPResponse(t, http.MethodPost, baseURL+"/"+SnapshotCatalogFile, "", http.StatusMethodNotAllowed, "", nil)

	downloaded := t.TempDir()
	result, err := FetchRemoteSnapshot(context.Background(), FetchRemoteSnapshotOptions{
		BaseURL:     baseURL,
		Dir:         downloaded,
		Expected:    identity,
		TrustedKeys: []ed25519.PublicKey{publicKey},
	})
	if err != nil {
		t.Fatalf("FetchRemoteSnapshot through publication server: %v", err)
	}
	if result.FilesDownloaded != len(manifest.Segments)+2 {
		t.Fatalf("files downloaded = %d, want %d", result.FilesDownloaded, len(manifest.Segments)+2)
	}
	if _, _, err := VerifySignedSnapshotCatalog(downloaded, identity, []ed25519.PublicKey{publicKey}); err != nil {
		t.Fatalf("VerifySignedSnapshotCatalog(downloaded): %v", err)
	}
}

func assertSnapshotHTTPResponse(t *testing.T, method, url, rangeHeader string, wantStatus int, wantCache string, wantBody []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s %s: %v", method, url, err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s status = %s body=%q, want %d", method, url, resp.Status, body, wantStatus)
	}
	if wantCache != "" && !containsHTTPToken(resp.Header.Get("Cache-Control"), wantCache) {
		t.Fatalf("%s %s Cache-Control = %q, want %q", method, url, resp.Header.Get("Cache-Control"), wantCache)
	}
	if wantBody != nil && string(body) != string(wantBody) {
		t.Fatalf("%s %s body = %x, want %x", method, url, body, wantBody)
	}
	if method == http.MethodHead && len(body) != 0 {
		t.Fatalf("HEAD %s returned %d body bytes", url, len(body))
	}
}

func containsHTTPToken(value, token string) bool {
	return strings.Contains(value, token)
}
