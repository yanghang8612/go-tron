package snapshots

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
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

// ValidateRemoteSnapshotBaseURL applies the same URL rules used by the remote
// fetcher before callers perform local side effects such as resetting a target
// snapshot directory.
func ValidateRemoteSnapshotBaseURL(baseURL string) error {
	_, err := snapshotRemoteURL(baseURL, SnapshotCatalogFile)
	return err
}

// ValidateRemoteSnapshotFetchConcurrency applies the same bounded worker-count
// rules used by FetchRemoteSnapshot before callers perform local side effects.
func ValidateRemoteSnapshotFetchConcurrency(concurrency int) error {
	_, err := normaliseSnapshotFetchConcurrency(concurrency)
	return err
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

	// Publish the immutable manifest object before the mutable root view, and
	// publish the signed catalog last. A crash can leave harmless downloaded
	// objects behind, but can never expose a local catalog whose manifest has
	// not reached durable storage yet.
	if catalog.ManifestPath != ManifestFile {
		if err := writeSnapshotBytesAtomic(opts.Dir, catalog.ManifestPath, manifestBytes); err != nil {
			return nil, err
		}
	}
	if err := writeSnapshotBytesAtomic(opts.Dir, ManifestFile, manifestBytes); err != nil {
		return nil, err
	}
	if err := writeSnapshotBytesAtomic(opts.Dir, SnapshotCatalogFile, catalogBytes); err != nil {
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
	if ref.Size == 0 {
		return fetchSnapshotSegmentFresh(ctx, client, fileURL, abs, ref)
	}
	return fetchSnapshotSegmentResumable(ctx, client, fileURL, abs, ref)
}

// fetchSnapshotSegmentFresh keeps the legacy full-download flow for the rare
// manifest entries without a known size. Production manifests carry a size and
// use the resumable path below.
func fetchSnapshotSegmentFresh(ctx context.Context, client *http.Client, fileURL, abs string, ref SegmentRef) (bool, uint64, error) {
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
	if err := syncSnapshotDirectory(filepath.Dir(abs)); err != nil {
		return false, 0, err
	}
	cleanup = false
	return true, uint64(n), nil
}

// fetchSnapshotSegmentResumable persists incomplete bytes in a checksum-bound
// partial file. A changed remote catalog receives a distinct partial path, so
// stale bytes can never be appended to a different signed segment revision.
func fetchSnapshotSegmentResumable(ctx context.Context, client *http.Client, fileURL, abs string, ref SegmentRef) (bool, uint64, error) {
	partialPath := snapshotSegmentPartialPath(abs, ref.Checksum)
	partial, offset, checksum, err := openSnapshotSegmentPartial(partialPath, ref.Size)
	if err != nil {
		return false, 0, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = partial.Close()
		}
	}()

	if offset == ref.Size {
		if snapshotSegmentChecksumMatches(checksum, ref.Checksum) {
			if err := promoteSnapshotSegmentPartial(partial, partialPath, abs); err != nil {
				return false, 0, err
			}
			closed = true
			return false, 0, nil
		}
		if err := resetSnapshotSegmentPartial(partial); err != nil {
			return false, 0, err
		}
		checksum.Reset()
		offset = 0
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return false, 0, err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, 0, err
	}
	defer resp.Body.Close()

	if offset > 0 && resp.StatusCode == http.StatusOK {
		// Some static HTTP servers do not support Range. Reset only after the
		// server has accepted the request, then verify the full response below.
		if err := resetSnapshotSegmentPartial(partial); err != nil {
			return false, 0, err
		}
		checksum.Reset()
		offset = 0
	}
	switch {
	case offset == 0 && resp.StatusCode == http.StatusOK:
	case offset > 0 && resp.StatusCode == http.StatusPartialContent:
		if err := validateSnapshotRangeResponse(resp.Header.Get("Content-Range"), offset, ref.Size); err != nil {
			return false, 0, err
		}
	default:
		return false, 0, fmt.Errorf("snapshots: GET %s: %s", fileURL, resp.Status)
	}

	remaining := ref.Size - offset
	if resp.ContentLength >= 0 && uint64(resp.ContentLength) != remaining {
		return false, 0, fmt.Errorf("snapshots: segment %q content length %d, want %d", ref.Path, resp.ContentLength, remaining)
	}
	limited := &snapshotFetchLimitReader{r: resp.Body, remaining: remaining}
	n, err := io.Copy(io.MultiWriter(partial, checksum), limited)
	if err != nil {
		return false, 0, err
	}
	if limited.remaining != 0 {
		return false, 0, fmt.Errorf("snapshots: segment %q size %d, want %d", ref.Path, offset+uint64(n), ref.Size)
	}
	if err := ensureSnapshotFetchBodyExhausted(resp.Body); err != nil {
		return false, 0, fmt.Errorf("snapshots: segment %q response exceeds expected size %d: %w", ref.Path, remaining, err)
	}
	if !snapshotSegmentChecksumMatches(checksum, ref.Checksum) {
		_ = partial.Close()
		closed = true
		_ = os.Remove(partialPath)
		return false, 0, fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, snapshotSegmentChecksum(checksum), ref.Checksum)
	}
	if err := promoteSnapshotSegmentPartial(partial, partialPath, abs); err != nil {
		return false, 0, err
	}
	closed = true
	return true, uint64(n), nil
}

type snapshotFetchLimitReader struct {
	r         io.Reader
	remaining uint64
}

func (r *snapshotFetchLimitReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if uint64(len(p)) > r.remaining {
		p = p[:int(r.remaining)]
	}
	n, err := r.r.Read(p)
	if n > 0 {
		r.remaining -= uint64(n)
	}
	return n, err
}

func ensureSnapshotFetchBodyExhausted(body io.Reader) error {
	var extra [1]byte
	for {
		n, err := body.Read(extra[:])
		if n > 0 {
			return errors.New("unexpected extra response byte")
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func snapshotSegmentPartialPath(abs, checksum string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(checksum))))
	return filepath.Join(filepath.Dir(abs), "."+filepath.Base(abs)+"."+hex.EncodeToString(sum[:])+".part")
}

func openSnapshotSegmentPartial(path string, expectedSize uint64) (*os.File, uint64, hash.Hash, error) {
	if info, err := os.Lstat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, 0, nil, err
		}
	} else if !info.Mode().IsRegular() {
		return nil, 0, nil, fmt.Errorf("snapshots: partial segment %q is not a regular file", path)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, 0, nil, err
	}
	closeWithError := func(err error) (*os.File, uint64, hash.Hash, error) {
		_ = file.Close()
		return nil, 0, nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return closeWithError(err)
	}
	if !info.Mode().IsRegular() {
		return closeWithError(fmt.Errorf("snapshots: partial segment %q is not a regular file", path))
	}
	offset := uint64(info.Size())
	if offset > expectedSize {
		if err := resetSnapshotSegmentPartial(file); err != nil {
			return closeWithError(err)
		}
		offset = 0
	}
	checksum := sha256.New()
	if offset > 0 {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return closeWithError(err)
		}
		if _, err := io.Copy(checksum, file); err != nil {
			return closeWithError(err)
		}
	}
	if _, err := file.Seek(int64(offset), io.SeekStart); err != nil {
		return closeWithError(err)
	}
	return file, offset, checksum, nil
}

func resetSnapshotSegmentPartial(file *os.File) error {
	if err := file.Truncate(0); err != nil {
		return err
	}
	_, err := file.Seek(0, io.SeekStart)
	return err
}

func promoteSnapshotSegmentPartial(file *os.File, partialPath, abs string) error {
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(partialPath, abs); err != nil {
		return err
	}
	return syncSnapshotDirectory(filepath.Dir(abs))
}

func snapshotSegmentChecksum(checksum hash.Hash) string {
	return "sha256:" + hex.EncodeToString(checksum.Sum(nil))
}

func snapshotSegmentChecksumMatches(checksum hash.Hash, expected string) bool {
	return strings.EqualFold(snapshotSegmentChecksum(checksum), expected)
}

func validateSnapshotRangeResponse(value string, offset, size uint64) error {
	parts := strings.Split(strings.TrimSpace(value), "/")
	if len(parts) != 2 || !strings.HasPrefix(parts[0], "bytes ") {
		return fmt.Errorf("snapshots: invalid Content-Range %q", value)
	}
	rangeParts := strings.Split(strings.TrimPrefix(parts[0], "bytes "), "-")
	if len(rangeParts) != 2 {
		return fmt.Errorf("snapshots: invalid Content-Range %q", value)
	}
	start, err := parseSnapshotRangeUint(rangeParts[0], value)
	if err != nil {
		return err
	}
	end, err := parseSnapshotRangeUint(rangeParts[1], value)
	if err != nil {
		return err
	}
	total, err := parseSnapshotRangeUint(parts[1], value)
	if err != nil {
		return err
	}
	if start != offset || end < start || total != size || end != size-1 {
		return fmt.Errorf("snapshots: Content-Range %q does not match requested bytes=%d- or size %d", value, offset, size)
	}
	return nil
}

func parseSnapshotRangeUint(value, contentRange string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("snapshots: invalid Content-Range %q", contentRange)
	}
	return parsed, nil
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
	if err := os.Rename(tmpName, abs); err != nil {
		return err
	}
	return syncSnapshotDirectory(filepath.Dir(abs))
}

func syncSnapshotDirectory(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
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
