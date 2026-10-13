package snapshots

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestPinnedHistoryMetadataReusesOnlyManagerOwnedGeneration(t *testing.T) {
	dir := t.TempDir()
	m := cachedManifestFixture(8)
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	live, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	if live.PinnedManifestView().manifest != nil {
		t.Fatal("a live manager exported a pinned metadata view")
	}
	pinned, release, err := live.PinHistoryReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	view := pinned.PinnedManifestView().manifest
	if view == nil || view != live.manifest || view.lookup != live.manifest.lookup {
		t.Fatal("pin copied manager-owned metadata or rebuilt its immutable index")
	}
	want := cloneManifest(view)
	public := pinned.Manifest()
	public.Segments[0].Path = "caller mutation"
	public.Progress.HotPruneBlockNum++
	if !reflect.DeepEqual(cloneManifest(view), want) {
		t.Fatal("public Manifest method mutated pinned metadata")
	}
	next := cloneManifest(m)
	next.Generation++
	if err := PublishManifest(dir, next); err != nil {
		t.Fatal(err)
	}
	newPin, releaseNext, err := live.PinHistoryReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer releaseNext()
	if newPin.PinnedManifestView().manifest.Generation != next.Generation || !reflect.DeepEqual(cloneManifest(view), want) {
		t.Fatal("a newer publication changed the earlier pinned generation")
	}
	// OpenPinnedManager is a separate public entry point with mutable caller
	// input. It must keep its detach/validation boundary.
	publicPin, err := OpenPinnedManager(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	m.Segments[0].Path = "mutated public input"
	if publicPin.PinnedManifestView().manifest.Segments[0].Path == m.Segments[0].Path {
		t.Fatal("public pinned-manager constructor borrowed mutable input")
	}
}

// Frozen pre-change pin plus the Manifest copy made by the source-view bridge.
func baselinePinnedHistoryMetadata(m *Manager) (*Manifest, func(), error) {
	if !historyReadLeaseGate.TryRLock() {
		return nil, nil, ErrHistoryReadLeaseBusy
	}
	release := sync.OnceFunc(historyReadLeaseGate.RUnlock)
	manifest, err := m.currentManifest()
	if err != nil {
		release()
		return nil, nil, err
	}
	pinned, err := openPinnedManager(m.dir, manifest, m.chainVerificationCache)
	if err != nil {
		release()
		return nil, nil, err
	}
	return pinned.Manifest(), release, nil
}

func BenchmarkPinnedHistoryMetadata(b *testing.B) {
	for _, n := range []int{100, 5000, 12000} {
		for _, baseline := range []bool{true, false} {
			name := "immutable-pin"
			if baseline {
				name = "baseline-copy"
			}
			b.Run(fmt.Sprintf("%d/%s", n, name), func(b *testing.B) {
				dir := b.TempDir()
				fixture := cachedManifestFixture(n)
				fixture.Retired = nil // the measured current node has no retired refs
				if err := PublishManifest(dir, fixture); err != nil {
					b.Fatal(err)
				}
				manager, err := OpenManager(dir)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var manifest *Manifest
					var release func()
					if baseline {
						manifest, release, err = baselinePinnedHistoryMetadata(manager)
					} else {
						var pinned *Manager
						pinned, release, err = manager.PinHistoryReadView()
						if err == nil {
							manifest = pinned.PinnedManifestView().manifest
						}
					}
					if err != nil {
						b.Fatal(err)
					}
					_, coverageErr := coldSnapshotVisibleTxEndFromManifest(manifest, SegmentDatasetStateDomainChange)
					release()
					if coverageErr != nil {
						b.Fatal(coverageErr)
					}
				}
			})
		}
	}
}
