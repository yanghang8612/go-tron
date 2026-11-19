package snapshots

import (
	"fmt"
	"sync"
	"testing"
)

// Keep this fixture compatible with the parent revision so both binaries
// exercise the same write, complete disk reads, pruning update and reopen.
func BenchmarkConcurrentManifestLifecycle(b *testing.B) {
	for _, tc := range []struct {
		name    string
		budget  uint64
		restart bool
	}{
		{"rejected-restart", 1, true},
		{"768MiB-restart", 768 << 20, true},
		{"768MiB-warm", 768 << 20, false},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
			b.Setenv(manifestCacheBudgetEnv, fmt.Sprint(tc.budget))
			b.Cleanup(loadedManifestCache.clear)
			dir := b.TempDir()
			m := manifestValidationMixedFixture()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.Generation++
				if err := PublishManifest(dir, m); err != nil {
					b.Fatal(err)
				}
				if tc.restart {
					loadedManifestCache.clear()
				}
				start := make(chan struct{})
				errs := make(chan error, 4)
				var wg sync.WaitGroup
				for j := 0; j < cap(errs); j++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						loaded, _, err := loadCurrentProductionManifest(dir)
						if err == nil && (loaded == nil || loaded.Generation != m.Generation) {
							err = fmt.Errorf("wrong generation")
						}
						errs <- err
					}()
				}
				close(start)
				wg.Wait()
				close(errs)
				for err := range errs {
					if err != nil {
						b.Fatal(err)
					}
				}
				if err := UpdateHotPruneProgress(dir, uint64(i+1), uint64(i+1)); err != nil {
					b.Fatal(err)
				}
				manager, err := OpenManager(dir)
				if err != nil {
					b.Fatal(err)
				}
				height, exists, err := manager.HotPrunedThroughBlock()
				if err != nil || !exists || height != uint64(i+1) {
					b.Fatalf("restart progress %d/%t: %v", height, exists, err)
				}
			}
		})
	}
}
