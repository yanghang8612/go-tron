package snapshots

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Both binaries exercise the same real publication, Manager refresh, progress
// maintenance, and final verification. The seed-race case lets the reader open
// the newly durable path while the public publisher is finishing its cache seed.
func BenchmarkInterleavedManifestLifecycle(b *testing.B) {
	for _, tc := range []struct {
		name     string
		seedRace bool
	}{
		{"768MiB-writer-reader-overlap", false},
		{"768MiB-publication-seed-race", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
			b.Setenv(manifestCacheBudgetEnv, "805306368")
			loadedManifestCache.clear()
			b.Cleanup(loadedManifestCache.clear)
			dir := b.TempDir()
			m := manifestValidationMixedFixture()
			if err := PublishManifest(dir, m); err != nil {
				b.Fatal(err)
			}
			manager, err := OpenManager(dir)
			if err != nil {
				b.Fatal(err)
			}
			var readerTime, pruneTime, publishTime time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.Generation++
				m.Progress = &Progress{HotPruneBlockNum: uint64(i), HotPruneTxNum: uint64(i)}
				wantGeneration := m.Generation
				wantProgress := uint64(i + 1)
				ready := make(chan struct{})
				readerStarted, pruneStarted := make(chan struct{}), make(chan struct{})
				errs := make(chan error, 2)
				var startOnce sync.Once
				start := func() { startOnce.Do(func() { close(ready) }) }
				go func() {
					<-ready
					started := time.Now()
					close(readerStarted)
					got, err := manager.currentManifest()
					if err == nil && (got == nil || got.Generation != wantGeneration || len(got.Segments) != len(m.Segments)) {
						err = fmt.Errorf("manager refresh observed an unexpected generation")
					}
					readerTime += time.Since(started)
					errs <- err
				}()
				go func() {
					<-ready
					started := time.Now()
					close(pruneStarted)
					err := UpdateHotPruneProgress(dir, wantProgress, wantProgress)
					pruneTime += time.Since(started)
					errs <- err
				}()
				started := time.Now()
				var publishErr error
				if tc.seedRace {
					manifestPublicationMu.Lock()
					publishErr = publishManifestLockedWithSync(dir, m, func(path string) error {
						if err := syncSnapshotDir(path); err != nil {
							return err
						}
						start()
						<-readerStarted
						<-pruneStarted
						return nil
					})
					manifestPublicationMu.Unlock()
				} else {
					publishErr = PublishManifest(dir, m)
				}
				publishTime += time.Since(started)
				start()
				readerErr, pruneErr := <-errs, <-errs
				if publishErr != nil || readerErr != nil || pruneErr != nil {
					b.Fatalf("lifecycle errors: publication=%v read/prune=%v %v", publishErr, readerErr, pruneErr)
				}
				height, exists, err := manager.HotPrunedThroughBlock()
				if err != nil || !exists || height != wantProgress {
					b.Fatalf("durable progress=%d exists=%t want=%d err=%v", height, exists, wantProgress, err)
				}
			}
			b.ReportMetric(float64(readerTime.Nanoseconds())/float64(b.N), "read-refresh-ns/op")
			b.ReportMetric(float64(pruneTime.Nanoseconds())/float64(b.N), "prune-progress-ns/op")
			b.ReportMetric(float64(publishTime.Nanoseconds())/float64(b.N), "publish-ns/op")
		})
	}
}
