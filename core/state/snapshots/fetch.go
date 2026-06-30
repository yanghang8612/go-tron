package snapshots

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

const (
	snapshotFetchMetadataLimit       = 16 << 20
	snapshotFetchDefaultConcurrency  = 4
	snapshotFetchMaxConcurrencyLimit = 64
)

type FetchRemoteSnapshotOptions struct {
	BaseURL                string
	Dir                    string
	Expected               ChainIdentity
	TrustedKeys            []ed25519.PublicKey
	HTTPClient             *http.Client
	CheckRetired           bool
	MaxConcurrentDownloads int
}

type FetchRemoteSnapshotResult struct {
	Catalog         *SnapshotCatalog
	Verification    ManifestVerificationReport
	FilesDownloaded int
	BytesDownloaded uint64
}

// FetchRemoteSnapshot downloads a signed snapshot catalog, its manifest, and
// every active segment referenced by the verified manifest. The manifest's
// segment paths are not trusted until the catalog signature and manifest
// checksum have both passed.
func FetchRemoteSnapshot(ctx context.Context, opts FetchRemoteSnapshotOptions) (*FetchRemoteSnapshotResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	baseURL := strings.TrimSpace(opts.BaseURL)
	if baseURL == "" {
		return nil, errors.New("snapshots: remote snapshot base URL is required")
	}
	if strings.TrimSpace(opts.Dir) == "" {
		return nil, errors.New("snapshots: snapshot directory is required")
	}
	client := opts.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	result := &FetchRemoteSnapshotResult{}

	catalogURL, err := snapshotRemoteURL(baseURL, SnapshotCatalogFile)
	if err != nil {
		return nil, err
	}
	catalogBytes, err := fetchSnapshotMetadata(ctx, client, catalogURL)
	if err != nil {
		return nil, err
	}
	result.BytesDownloaded += uint64(len(catalogBytes))
	result.FilesDownloaded++
	catalog, err := decodeAndVerifyFetchedCatalog(catalogBytes, opts.Expected, opts.TrustedKeys)
	if err != nil {
		return nil, err
	}

	manifestURL, err := snapshotRemoteURL(baseURL, catalog.ManifestPath)
	if err != nil {
		return nil, err
	}
	manifestBytes, err := fetchSnapshotMetadata(ctx, client, manifestURL)
	if err != nil {
		return nil, err
	}
	result.BytesDownloaded += uint64(len(manifestBytes))
	result.FilesDownloaded++
	manifest, err := decodeAndVerifyFetchedManifest(manifestBytes, catalog, opts.Expected)
	if err != nil {
		return nil, err
	}
	refs := append([]SegmentRef(nil), manifest.Segments...)
	if opts.CheckRetired {
		refs = append(refs, manifest.Retired...)
	}
	files, bytes, err := fetchSnapshotSegments(ctx, client, baseURL, opts.Dir, refs, opts.MaxConcurrentDownloads)
	if err != nil {
		return nil, err
	}
	result.FilesDownloaded += files
	result.BytesDownloaded += bytes

	report, err := VerifyLoadedManifestFiles(opts.Dir, manifest, VerifyManifestOptions{
		ExpectedChain:     &opts.Expected,
		CheckRetired:      opts.CheckRetired,
		RequireRegistered: true,
		RequireChecksums:  true,
	})
	if err != nil {
		return nil, err
	}

	if err := writeSnapshotBytesAtomic(opts.Dir, SnapshotCatalogFile, catalogBytes); err != nil {
		return nil, err
	}
	if err := writeSnapshotBytesAtomic(opts.Dir, ManifestFile, manifestBytes); err != nil {
		return nil, err
	}

	result.Catalog = catalog
	result.Verification = *report
	return result, nil
}

func decodeAndVerifyFetchedCatalog(data []byte, expected ChainIdentity, trustedKeys []ed25519.PublicKey) (*SnapshotCatalog, error) {
	var catalog SnapshotCatalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, err
	}
	normalizeSnapshotCatalog(&catalog)
	if err := catalog.Validate(); err != nil {
		return nil, err
	}
	if err := catalog.ValidateChainIdentity(expected); err != nil {
		return nil, err
	}
	if err := catalog.VerifySignature(trustedKeys); err != nil {
		return nil, err
	}
	return &catalog, nil
}

func decodeAndVerifyFetchedManifest(data []byte, catalog *SnapshotCatalog, expected ChainIdentity) (*Manifest, error) {
	if catalog == nil {
		return nil, errors.New("snapshots: nil snapshot catalog")
	}
	gotChecksum := checksumBytes(data)
	if !strings.EqualFold(gotChecksum, catalog.ManifestChecksum) {
		return nil, fmt.Errorf("snapshots: catalog manifest checksum %s, got %s", catalog.ManifestChecksum, gotChecksum)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	normalizeManifest(&manifest)
	sortSegments(manifest.Segments)
	sortSegments(manifest.Retired)
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	if err := manifest.ValidateProduction(); err != nil {
		return nil, err
	}
	if err := manifest.ValidateChainIdentity(expected); err != nil {
		return nil, err
	}
	if manifest.VisibleTxStart != catalog.VisibleTxStart || manifest.VisibleTxEnd != catalog.VisibleTxEnd {
		return nil, fmt.Errorf("snapshots: catalog visible range [%d,%d], manifest range [%d,%d]",
			catalog.VisibleTxStart, catalog.VisibleTxEnd, manifest.VisibleTxStart, manifest.VisibleTxEnd)
	}
	return &manifest, nil
}

func fetchSnapshotMetadata(ctx context.Context, client *http.Client, fileURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("snapshots: GET %s: %s", fileURL, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, snapshotFetchMetadataLimit+1))
	if err != nil {
		return nil, err
	}
	if len(data) > snapshotFetchMetadataLimit {
		return nil, fmt.Errorf("snapshots: metadata %s exceeds %d bytes", fileURL, snapshotFetchMetadataLimit)
	}
	return data, nil
}

type fetchSnapshotSegmentResult struct {
	downloaded bool
	bytes      uint64
	err        error
}

func fetchSnapshotSegments(ctx context.Context, client *http.Client, baseURL, dir string, refs []SegmentRef, concurrency int) (int, uint64, error) {
	workers, err := normaliseSnapshotFetchConcurrency(concurrency)
	if err != nil {
		return 0, 0, err
	}
	uniqueRefs := uniqueSnapshotSegmentRefs(refs)
	if len(uniqueRefs) == 0 {
		return 0, 0, nil
	}
	if workers > len(uniqueRefs) {
		workers = len(uniqueRefs)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan SegmentRef)
	results := make(chan fetchSnapshotSegmentResult, len(uniqueRefs))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ref := range jobs {
				downloaded, bytes, err := fetchSnapshotSegment(ctx, client, baseURL, dir, ref)
				results <- fetchSnapshotSegmentResult{downloaded: downloaded, bytes: bytes, err: err}
				if err != nil {
					cancel()
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, ref := range uniqueRefs {
			select {
			case <-ctx.Done():
				return
			case jobs <- ref:
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	var files int
	var bytes uint64
	var firstErr error
	for result := range results {
		if result.err != nil && firstErr == nil {
			firstErr = result.err
			cancel()
			continue
		}
		if result.err == nil && result.downloaded {
			files++
			bytes += result.bytes
		}
	}
	if firstErr != nil {
		return 0, 0, firstErr
	}
	return files, bytes, nil
}

func uniqueSnapshotSegmentRefs(refs []SegmentRef) []SegmentRef {
	out := make([]SegmentRef, 0, len(refs))
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if _, ok := seen[ref.Path]; ok {
			continue
		}
		seen[ref.Path] = struct{}{}
		out = append(out, ref)
	}
	return out
}

func normaliseSnapshotFetchConcurrency(concurrency int) (int, error) {
	if concurrency < 0 {
		return 0, fmt.Errorf("snapshots: fetch concurrency %d must be non-negative", concurrency)
	}
	if concurrency == 0 {
		return snapshotFetchDefaultConcurrency, nil
	}
	if concurrency > snapshotFetchMaxConcurrencyLimit {
		return 0, fmt.Errorf("snapshots: fetch concurrency %d exceeds maximum %d", concurrency, snapshotFetchMaxConcurrencyLimit)
	}
	return concurrency, nil
}

func fetchSnapshotSegment(ctx context.Context, client *http.Client, baseURL, dir string, ref SegmentRef) (bool, uint64, error) {
	if err := validateSegmentRef(ref); err != nil {
		return false, 0, err
	}
	if strings.TrimSpace(ref.Checksum) == "" {
		return false, 0, fmt.Errorf("snapshots: segment %q missing required checksum", ref.Path)
	}
	if err := checkSegmentFileMetadata(dir, ref, true); err == nil {
		return false, 0, nil
	}
	fileURL, err := snapshotRemoteURL(baseURL, ref.Path)
	if err != nil {
		return false, 0, err
	}
	abs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return false, 0, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return false, 0, err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		_ = tmp.Close()
		return false, 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		_ = tmp.Close()
		return false, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = tmp.Close()
		return false, 0, fmt.Errorf("snapshots: GET %s: %s", fileURL, resp.Status)
	}
	if ref.Size != 0 && resp.ContentLength >= 0 && uint64(resp.ContentLength) != ref.Size {
		_ = tmp.Close()
		return false, 0, fmt.Errorf("snapshots: segment %q content length %d, want %d", ref.Path, resp.ContentLength, ref.Size)
	}

	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, hash), resp.Body)
	if err != nil {
		_ = tmp.Close()
		return false, 0, err
	}
	if ref.Size != 0 && uint64(n) != ref.Size {
		_ = tmp.Close()
		return false, 0, fmt.Errorf("snapshots: segment %q size %d, want %d", ref.Path, n, ref.Size)
	}
	gotChecksum := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(gotChecksum, ref.Checksum) {
		_ = tmp.Close()
		return false, 0, fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, gotChecksum, ref.Checksum)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return false, 0, err
	}
	if err := tmp.Close(); err != nil {
		return false, 0, err
	}
	if err := os.Rename(tmpName, abs); err != nil {
		return false, 0, err
	}
	cleanup = false
	return true, uint64(n), nil
}

func writeSnapshotBytesAtomic(dir, relPath string, data []byte) error {
	if relPath == "" || filepath.IsAbs(relPath) || filepath.Clean(relPath) != relPath || relPath == "." || hasParentDir(relPath) {
		return fmt.Errorf("snapshots: invalid relative snapshot path %q", relPath)
	}
	abs := filepath.Join(dir, relPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
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
	return os.Rename(tmpName, abs)
}

func snapshotRemoteURL(baseURL, relPath string) (string, error) {
	if relPath == "" || path.IsAbs(relPath) || path.Clean(relPath) != relPath || relPath == "." || hasParentDir(relPath) {
		return "", fmt.Errorf("snapshots: invalid relative remote path %q", relPath)
	}
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("snapshots: unsupported snapshot URL scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return "", errors.New("snapshots: snapshot URL host is required")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("snapshots: snapshot base URL must not contain query or fragment")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/" + relPath
	return u.String(), nil
}

func checksumBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}
