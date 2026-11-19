package snapshots

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestManifestDecodeFlightSharesExactResultWithoutMakingItResident(t *testing.T) {
	var c manifestDecodeCache
	c.resizeLocked(1)
	m := manifestValidationFixture(1)
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	f := &manifestDecodeFlight{data: data, done: make(chan struct{})}
	c.flight = f
	out := make(chan *Manifest, 1)
	go func() {
		loaded, _ := c.decodeMode(bytes.Clone(data), true, 1, false)
		out <- loaded
	}()
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		shared := f.shared
		c.mu.Unlock()
		if shared {
			break
		}
		if time.Now().After(deadline) {
			close(f.done)
			t.Fatal("loader did not join in-flight decode")
		}
		runtime.Gosched()
	}
	decoded, err := decodeProductionManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	f.manifest = decoded
	c.flight = nil
	close(f.done)
	c.mu.Unlock()
	loaded := <-out
	if loaded == nil || &loaded.Segments[0] == &decoded.Segments[0] || c.manifest != nil {
		t.Fatal("waiter failed to detach rejected generation")
	}
}

func TestManifestDecodeObservedOldFileDoesNotReplaceNewPublication(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	t.Setenv(manifestCacheBudgetEnv, "805306368")
	loadedManifestCache.clear()
	t.Cleanup(loadedManifestCache.clear)

	dir := t.TempDir()
	prior := manifestValidationFixture(1)
	if err := PublishManifest(dir, prior); err != nil {
		t.Fatal(err)
	}
	initialBytes, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	revision := loadedManifestCache.revision
	current := cloneDecodedManifest(prior)
	current.Generation++
	if err := PublishManifest(dir, current); err != nil {
		t.Fatal(err)
	}
	observed, err := loadedManifestCache.decodeObserved(initialBytes, true, 768<<20, true, revision, nil)
	if err != nil || observed.Generation != prior.Generation {
		t.Fatalf("old file observation: %#v %v", observed, err)
	}
	if loadedManifestCache.manifest.Generation != current.Generation || !loadedManifestCache.publication {
		t.Fatal("old decode evicted a newer publication")
	}
}
