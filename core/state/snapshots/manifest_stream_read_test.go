package snapshots

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestManifestStreamingReadMatchesWholeFile(t *testing.T) {
	for _, refs := range []int{0, 4, 1200} {
		t.Run(testManifestStreamName(refs), func(t *testing.T) {
			t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
			t.Setenv(manifestCacheBudgetEnv, "")
			loadedManifestCache.clear()
			t.Cleanup(loadedManifestCache.clear)
			dir := t.TempDir()
			m := cachedManifestFixture(refs)
			if err := PublishManifest(dir, m); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, ManifestFile)
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			assertLoad := func(raw []byte) {
				t.Helper()
				want, wantErr := decodeProductionManifest(raw)
				got, gotErr := LoadProductionManifest(dir)
				if (gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() || !reflect.DeepEqual(got, want) {
					t.Fatalf("streamed load differs from decoding current bytes: got=%v err=%v want=%v err=%v", got, gotErr, want, wantErr)
				}
			}
			assertLoad(original)
			// A trailing byte must be read and checked, even after all cached
			// bytes matched. Space remains valid JSON but is a different input.
			appended := append(bytes.Clone(original), ' ')
			if err := os.WriteFile(path, appended, 0o600); err != nil {
				t.Fatal(err)
			}
			misses := manifestCacheMisses.Snapshot().Count()
			assertLoad(appended)
			if manifestCacheMisses.Snapshot().Count() != misses+1 {
				t.Fatal("appended byte reused the old cached file")
			}
			assertLoad(appended)
			truncated := bytes.Clone(original[:len(original)-1])
			if err := os.WriteFile(path, truncated, 0o600); err != nil {
				t.Fatal(err)
			}
			assertLoad(truncated)
			// Atomic same-length replacement must not be accepted because
			// generation, size, inode or timestamps happened to look familiar.
			changed := bytes.Replace(original, []byte("123450"), []byte("123451"), 1)
			if len(changed) != len(original) || bytes.Equal(changed, original) {
				t.Fatal("invalid replacement fixture")
			}
			tmp := filepath.Join(dir, "replacement.tmp")
			if err := os.WriteFile(tmp, changed, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(tmp, path); err != nil {
				t.Fatal(err)
			}
			assertLoad(changed)
			if _, err := os.Stat(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadProductionManifest(dir); err == nil {
				t.Fatal("missing current file returned stale cached manifest")
			}
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadProductionManifest(dir); err == nil {
				t.Fatal("unreadable current file returned stale cached manifest")
			}
		})
	}
}

func testManifestStreamName(refs int) string {
	if refs == 0 {
		return "empty"
	}
	if refs < 100 {
		return "small"
	}
	return "multiple-chunks"
}

type manifestFinalRead struct {
	data []byte
	err  error
}

func (r *manifestFinalRead) Read(dst []byte) (int, error) {
	n := copy(dst, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, r.err
	}
	return n, nil
}

func TestManifestStreamingFinalReadAndError(t *testing.T) {
	cached := []byte(`{"version":1,"segments":[]}`)
	for _, tc := range []struct {
		name  string
		input []byte
		err   error
		equal bool
	}{
		{"equal-eof", cached, io.EOF, true},
		{"last-extra-byte-eof", append(bytes.Clone(cached), ' '), io.EOF, false},
		{"short-eof", cached[:len(cached)-1], io.EOF, false},
		{"read-error", cached, errors.New("read failed"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, equal, err := compareManifestReader(&manifestFinalRead{data: tc.input, err: tc.err}, cached)
			if (err == nil) != (tc.err == io.EOF) || equal != tc.equal || err == nil && !equal && !bytes.Equal(data, tc.input) {
				t.Fatalf("result data=%q equal=%t err=%v", data, equal, err)
			}
		})
	}
}

func TestManifestStreamingReadCacheReplacement(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	t.Setenv(manifestCacheBudgetEnv, "")
	loadedManifestCache.clear()
	t.Cleanup(loadedManifestCache.clear)
	dir := t.TempDir()
	first := cachedManifestFixture(1200)
	if err := PublishManifest(dir, first); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ManifestFile)
	firstBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	second := cachedManifestFixture(1200)
	second.Generation, second.Progress.LatestBuildTxNum = 8, 8
	secondBytes, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	// The compare reader holds the first cache bytes outside the lock. A
	// publisher may replace the resident before its final pointer check.
	loadedManifestCache.mu.Lock()
	oldData, oldManifest := loadedManifestCache.data, loadedManifestCache.manifest
	loadedManifestCache.mu.Unlock()
	if _, equal, err := readManifestAgainstCache(path, oldData); err != nil || !equal {
		t.Fatalf("first read: equal=%t err=%v", equal, err)
	}
	if err := os.WriteFile(path, secondBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	loadedManifestCache.retainPublished(secondBytes, second, manifestCacheDefaultBytes)
	loadedManifestCache.mu.Lock()
	replaced := loadedManifestCache.manifest != oldManifest
	loadedManifestCache.mu.Unlock()
	if !replaced {
		t.Fatal("test did not replace resident")
	}
	if !bytes.Equal(oldData, firstBytes) {
		t.Fatal("writer mutated bytes still held by a comparing reader")
	}
	got, err := LoadProductionManifest(dir)
	if err != nil || got.Generation != 8 || got.Progress.LatestBuildTxNum != 8 {
		t.Fatalf("reader returned obsolete cache after replacement: %+v, %v", got, err)
	}
}
