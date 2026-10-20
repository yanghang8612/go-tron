package snapshots

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func retiredRoutingFixture(t *testing.T) (string, *Manifest, SegmentRef) {
	t.Helper()
	dir, m, _ := accountRoutingFixture(t, 9)
	m = addAccountRoutingFixture(t, dir, m, 1)
	old := mustAccountRoutingRef(t, m)
	m = addAccountRoutingFixture(t, dir, m, 64)
	return dir, m, old
}

func TestRetiredHistoryRoutingOnlyDeletesUnleasedAuxiliaryFiles(t *testing.T) {
	dir, m, old := retiredRoutingFixture(t)
	before, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	canonical := make(map[string][32]byte)
	for _, ref := range m.Segments {
		if ref.Kind != SegmentAccountHistoryRouting {
			data, err := os.ReadFile(filepath.Join(dir, ref.Path))
			if err != nil {
				t.Fatal(err)
			}
			canonical[ref.Path] = sha256.Sum256(data)
		}
	}
	result, err := pruneRetiredHistoryRoutingContext(context.Background(), dir, retiredMetadataSweepCursor{})
	if err != nil || result.FilesDeleted != 1 || result.BytesDeleted != old.Size {
		t.Fatalf("prune=%+v err=%v", result, err)
	}
	if _, err := os.Stat(filepath.Join(dir, old.Path)); !os.IsNotExist(err) {
		t.Fatal("old auxiliary file retained", err)
	}
	if _, err := os.Stat(filepath.Join(dir, mustAccountRoutingRef(t, m).Path)); err != nil {
		t.Fatal("active index lost", err)
	}
	after, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("cleanup changed signed metadata", err)
	}
	for path, want := range canonical {
		data, err := os.ReadFile(filepath.Join(dir, path))
		if err != nil || sha256.Sum256(data) != want {
			t.Fatal("canonical file changed", path, err)
		}
	}
	if err := checkAccountHistoryRoutingContext(context.Background(), dir, m, mustAccountRoutingRef(t, m)); err != nil {
		t.Fatal(err)
	}
}

func TestRetiredHistoryRoutingPinsReadersAndPublishedGenerations(t *testing.T) {
	dir, m, old := retiredRoutingFixture(t)
	r, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, release, err := r.PinHistoryReadView()
	if err != nil {
		t.Fatal(err)
	}
	result, err := pruneRetiredHistoryRoutingContext(context.Background(), dir, retiredMetadataSweepCursor{})
	release()
	if err != nil || !result.Deferred || result.FilesDeleted != 0 {
		t.Fatalf("reader lease: %+v %v", result, err)
	}
	published := cloneManifest(m)
	for i, ref := range published.Segments {
		if ref.Kind == SegmentAccountHistoryRouting {
			published.Segments[i] = old
		}
	}
	data, err := json.Marshal(published)
	if err != nil {
		t.Fatal(err)
	}
	name, err := publishedSnapshotManifestPath(published.Generation, checksumBytes(data))
	if err != nil {
		t.Fatal(err)
	}
	if err := publishImmutableSnapshotManifest(dir, name, data); err != nil {
		t.Fatal(err)
	}
	result, err = pruneRetiredHistoryRoutingContext(context.Background(), dir, retiredMetadataSweepCursor{})
	if err != nil || result.FilesDeleted != 0 {
		t.Fatalf("published lease: %+v %v", result, err)
	}
	if _, err := os.Stat(filepath.Join(dir, old.Path)); err != nil {
		t.Fatal(err)
	}
}

func TestRetiredHistoryRoutingCancellationMigrationAndBadLeases(t *testing.T) {
	for _, mode := range []string{"cancel", "migration", "bad-published", "many-published", "symlink", "catalog-symlink"} {
		t.Run(mode, func(t *testing.T) {
			dir, _, old := retiredRoutingFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "cancel":
				cancel()
			case "migration":
				if err := os.WriteFile(filepath.Join(dir, historyReferenceMigrationJournalFile), nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "bad-published", "many-published":
				root := filepath.Join(dir, SnapshotPublishedDir)
				if err := os.MkdirAll(root, 0755); err != nil {
					t.Fatal(err)
				}
				if mode == "bad-published" {
					if err := os.WriteFile(filepath.Join(root, "manifest-bad.json"), []byte("{}"), 0600); err != nil {
						t.Fatal(err)
					}
				} else {
					for i := 0; i < 65; i++ {
						if err := os.WriteFile(filepath.Join(root, fmt.Sprint(i)), nil, 0600); err != nil {
							t.Fatal(err)
						}
					}
				}
			case "symlink":
				path := filepath.Join(dir, old.Path)
				if err := os.Rename(path, path+".saved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".saved", path); err != nil {
					t.Fatal(err)
				}
			case "catalog-symlink":
				if err := os.Symlink(filepath.Join(dir, "absent"), filepath.Join(dir, SnapshotCatalogFile)); err != nil {
					t.Fatal(err)
				}
			}
			result, err := pruneRetiredHistoryRoutingContext(ctx, dir, retiredMetadataSweepCursor{})
			if result.FilesDeleted != 0 {
				t.Fatal("protected object deleted", result)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if _, e := os.Lstat(filepath.Join(dir, old.Path)); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestRetiredHistoryRoutingBoundsCursorAndIdentity(t *testing.T) {
	dir, m, _ := retiredRoutingFixture(t)
	for i := 0; i < 300; i++ {
		data := append(append([]byte(nil), accountRoutingMagic[:]...), []byte(fmt.Sprint(i))...)
		sum := sha256.Sum256(data)
		ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccountHistoryRouting, FromTxNum: 1, ToTxNum: 3, Size: uint64(len(data)), Checksum: fmt.Sprintf("sha256:%x", sum), LookupRoot: fmt.Sprintf("sha256:%x", sum), Path: fmt.Sprintf("history/account-routing-1-3-%x.route", sum)}
		if !historyRoutingRetiredObject(ref) {
			t.Fatal("bad fixture identity")
		}
		if err := os.WriteFile(filepath.Join(dir, ref.Path), data, 0600); err != nil {
			t.Fatal(err)
		}
		m.Retired = append(m.Retired, ref)
	}
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	var cursor retiredMetadataSweepCursor
	deleted := 0
	for pass := 0; pass < 100; pass++ {
		r, err := pruneRetiredHistoryRoutingContext(context.Background(), dir, cursor)
		if err != nil {
			t.Fatal(err)
		}
		if r.Scanned > historyRoutingRetiredScanLimit || r.FilesDeleted > historyRoutingRetiredFileLimit {
			t.Fatal("unbounded cleanup", r)
		}
		cursor = r.Next
		deleted += r.FilesDeleted
	}
	if deleted != 301 {
		t.Fatalf("cursor starved: removed %d", deleted)
	}
	bad := mustAccountRoutingRef(t, m)
	bad.Path = "history/state-domain-change.seg"
	if historyRoutingRetiredObject(bad) {
		t.Fatal("canonical path accepted")
	}
	before := cloneManifest(m).Progress
	after, err := LoadProductionManifest(dir)
	if err != nil || !reflect.DeepEqual(before, after.Progress) {
		t.Fatal("frontier changed", err)
	}
}

func TestSnapshotCatalogPublisherDefersDuringPhysicalRetirement(t *testing.T) {
	release, ok := tryHistoryRetirement()
	if !ok {
		t.Fatal("exclusive fixture lease")
	}
	defer release()
	_, err := PublishSignedSnapshotCatalog(t.TempDir(), ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)))
	if !errors.Is(err, ErrHistoryReadLeaseBusy) {
		t.Fatal("publisher did not pin files", err)
	}
}
