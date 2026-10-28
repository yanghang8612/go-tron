package snapshots

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func addMetadataRouting(t *testing.T, dir string, m *Manifest, limit int) *Manifest {
	t.Helper()
	ref, built, err := BuildMetadataHistoryRoutingContext(context.Background(), dir, m, limit)
	if err != nil || !built {
		t.Fatalf("build %v %v", built, err)
	}
	next, err := NewAggregator(dir).Integrate(m.VisibleTxStart, m.VisibleTxEnd, []SegmentRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func TestMetadataHistoryRoutingPartialAndPositive(t *testing.T) {
	dir, m, queries := routingFixture(t)
	original := cloneManifest(m)
	oracle, err := OpenPinnedManager(dir, original)
	if err != nil {
		t.Fatal(err)
	}
	meta := queries[7]
	missing := meta
	missing.Key = []byte("non-meta-canonical-key")
	maximum := meta
	maximum.Generation = math.MaxUint64
	queries = append(queries, missing, maximum)
	for pass := 0; pass < 3; pass++ {
		m = addMetadataRouting(t, dir, m, 1)
		count := 0
		for _, ref := range m.Segments {
			if ref.Kind == SegmentMetadataHistoryRouting {
				count++
				if err := checkMetadataHistoryRoutingContext(context.Background(), dir, m, ref); err != nil {
					t.Fatal(err)
				}
			}
		}
		if count != pass+1 {
			t.Fatal("earlier pages replaced", count)
		}
		reader, err := OpenPinnedManager(dir, m)
		if err != nil {
			t.Fatal(err)
		}
		for from := uint64(1); from <= 4; from++ {
			got, err := reader.FirstStateDomainChangesContext(context.Background(), from, 4, queries)
			if err != nil {
				t.Fatal(err)
			}
			for i, q := range queries {
				var want *rawdb.StateDomainChange
				err = oracle.IterateStateDomainChangesByKey(from, 4, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key, func(row *rawdb.StateDomainChange) (bool, error) { want = row; return false, nil })
				if err != nil || !reflect.DeepEqual(got[i], want) {
					t.Fatalf("pass %d from %d query %d: got %+v want %+v err %v", pass, from, i, got[i], want, err)
				}
			}
		}
	}
	if _, built, err := BuildMetadataHistoryRoutingContext(context.Background(), dir, m, 64); err != nil || built {
		t.Fatal("rebuilt covered pages", built, err)
	}
	if !reflect.DeepEqual(m.Progress, original.Progress) {
		t.Fatal("changed frontiers")
	}
	if _, err := VerifyLoadedManifestFiles(dir, m, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataHistoryRoutingBloomOwnsFullKeyAndConservativeSaturation(t *testing.T) {
	q := StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 9}, Generation: math.MaxUint64, Domain: kvdomains.ContractMetadata, Key: []byte("arbitrary/key")}
	key := stateDomainChangeBinaryAccessorLookupKey(q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key)
	var f metadataSourceFilter
	if f.mayContain(key) {
		t.Fatal("empty proof positive")
	}
	f.add(key)
	if !f.mayContain(key) || f.count != 1 {
		t.Fatal("false negative")
	}
	other := append([]byte(nil), key...)
	other[10] ^= 1
	if f.mayContain(other) {
		t.Fatal("fixture owner alias")
	}
	other = append([]byte(nil), key...)
	other[22] ^= 1
	if f.mayContain(other) {
		t.Fatal("fixture generation alias")
	}
	f.bits = bytes.Repeat([]byte{255}, metadataRoutingBloomBytes)
	if !f.mayContain(other) {
		t.Fatal("saturation must request fallback")
	}
}

func TestMetadataHistoryRoutingEmptyProofAvoidsSourcesAndFailsClosed(t *testing.T) {
	dir, m, queries := routingFixture(t)
	// The existing verifiable restore fixture has no metadata rows.
	emptyDir, _, _ := writeVerifiableHistoryManifest(t)
	empty, err := LoadManifest(emptyDir)
	if err != nil {
		t.Fatal(err)
	}
	empty = addMetadataRouting(t, emptyDir, empty, 256)
	for _, r := range empty.Segments {
		if r.Kind == SegmentHistory || r.Kind == SegmentAccessor {
			if err = os.Remove(filepath.Join(emptyDir, r.Path)); err != nil {
				t.Fatal(err)
			}
		}
	}
	r, err := OpenPinnedManager(emptyDir, empty)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.FirstStateDomainChangesContext(context.Background(), empty.VisibleTxStart, empty.VisibleTxEnd, []StateHistoryQuery{queries[7]})
	if err != nil || got[0] != nil {
		t.Fatal("empty proof opened canonical sources", got, err)
	}
	// A referenced damaged page must never become an empty proof.
	m = addMetadataRouting(t, dir, m, 256)
	var ref SegmentRef
	for _, r := range m.Segments {
		if r.Kind == SegmentMetadataHistoryRouting {
			ref = r
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1
	if err = os.WriteFile(filepath.Join(dir, ref.Path), data, 0600); err != nil {
		t.Fatal(err)
	}
	r, err = OpenPinnedManager(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.FirstStateDomainChangesContext(context.Background(), 1, 3, []StateHistoryQuery{queries[7]}); err == nil {
		t.Fatal("corrupt page accepted")
	}
	// A self-consistent but false absence is caught by independent strict scan.
	for i := metadataRoutingHeaderBytes; i < len(data); {
		copy(data[i+40:i+40+metadataRoutingBloomBytes], make([]byte, metadataRoutingBloomBytes))
		i += 40 + metadataRoutingBloomBytes
	}
	ref.Checksum = fmt.Sprintf("sha256:%x", sha256.Sum256(data))
	if err = os.WriteFile(filepath.Join(dir, ref.Path), data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = readMetadataHistoryRouting(context.Background(), dir, ref); err != nil {
		t.Fatal(err)
	}
	if err = checkMetadataHistoryRoutingContext(context.Background(), dir, m, ref); err == nil {
		t.Fatal("forged omission passed strict verification")
	}
}

func TestMetadataHistoryRoutingCancelledAndReplacedSource(t *testing.T) {
	dir, m, queries := routingFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, built, err := BuildMetadataHistoryRoutingContext(ctx, dir, m, 1); built || !errors.Is(err, context.Canceled) {
		t.Fatal(built, err)
	}
	m = addMetadataRouting(t, dir, m, 256)
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	q := queries[7]
	q.Key = []byte("replacement")
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{1}, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{BlockNum: 1, BlockHash: common.Hash{1}, TxNum: 1, FlatDomain: q.FlatDomain, Owner: q.Owner, Generation: q.Generation, Domain: q.Domain, Key: q.Key, PrevExists: true, Prev: []byte("new")}); err != nil {
		t.Fatal(err)
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 1, "history/state-domain-change-metadata-replacement.seg")
	if err != nil {
		t.Fatal(err)
	}
	m, err = NewAggregator(dir).Integrate(1, 1, refs)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		r, err := OpenPinnedManager(dir, m)
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.FirstStateDomainChangesContext(context.Background(), 1, 3, []StateHistoryQuery{q})
		if err != nil || got[0] == nil || string(got[0].Prev) != "new" {
			t.Fatal("replacement hidden", got, err)
		}
		if i == 0 {
			m = addMetadataRouting(t, dir, m, 1)
		}
	}
}

func TestMetadataHistoryRoutingVerifiedSignedRestore(t *testing.T) {
	dir, identity, _ := writeVerifiableHistoryManifest(t)
	m, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	m = addMetadataRouting(t, dir, m, 1)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = PublishSignedSnapshotCatalog(dir, priv); err != nil {
		t.Fatal(err)
	}
	for _, signed := range []bool{false, true} {
		db := rawdb.NewMemoryDatabase()
		if signed {
			result, err := RestoreSnapshotFromVerifiedCatalog(db, dir, identity, []ed25519.PublicKey{pub})
			if err != nil || result.ChangesRestored != 2 {
				t.Fatal(result, err)
			}
		} else {
			result, err := RestoreSnapshotFromVerifiedManifestWithOptions(db, dir, identity, strictBoundaryOptions(t, db))
			if err != nil || result.ChangesRestored != 2 {
				t.Fatal(result, err)
			}
		}
		census, err := rawdb.InspectStateKVGenerations(context.Background(), db, true, nil, nil)
		db.Close()
		if err != nil || !census.MuHashVerified || !census.Complete {
			t.Fatal(census, err)
		}
	}
}

func TestMetadataHistoryRoutingUnsupportedSourcesDoNotStarveTail(t *testing.T) {
	dir, m := smallManyRoutingFixture(t)
	count := 0
	for i := range m.Segments {
		ref := &m.Segments[i]
		if ref.Kind != SegmentAccessor {
			continue
		}
		if count < 64 {
			data, err := os.ReadFile(filepath.Join(dir, ref.Path))
			if err != nil {
				t.Fatal(err)
			}
			binary.BigEndian.PutUint32(data[8:12], 0)
			if err = os.WriteFile(filepath.Join(dir, ref.Path), data, 0600); err != nil {
				t.Fatal(err)
			}
			ref.Checksum = fmt.Sprintf("sha256:%x", sha256.Sum256(data))
		}
		count++
	}
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	// One page-sized pass can skip the old 64-source unsupported prefix.
	if built, err := refreshMetadataHistoryRoutingContext(context.Background(), dir, false); !built || err != nil {
		t.Fatal("supported tail starved", built, err)
	}
	next, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	sources := uint64(0)
	for _, ref := range next.Segments {
		if ref.Kind == SegmentMetadataHistoryRouting {
			sources += ref.Rows
		}
	}
	if sources != 6 {
		t.Fatal("unsupported sources acquired absence proof", sources)
	}
}

func TestMetadataHistoryRoutingRetirementProtectsReadersAndPublishedPages(t *testing.T) {
	for _, mode := range []string{"unleased", "reader", "published"} {
		t.Run(mode, func(t *testing.T) {
			dir, m, queries := routingFixture(t)
			m = addMetadataRouting(t, dir, m, 256)
			var old SegmentRef
			for _, ref := range m.Segments {
				if ref.Kind == SegmentMetadataHistoryRouting {
					old = ref
				}
			}
			before := cloneManifest(m)
			// Replacing a source gives its page a different exact pair identity.
			db := rawdb.NewMemoryDatabase()
			defer db.Close()
			q := queries[7]
			if err := rawdb.WriteStateTxRange(db, 1, common.Hash{1}, 1, 1); err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{BlockNum: 1, BlockHash: common.Hash{1}, TxNum: 1, FlatDomain: q.FlatDomain, Owner: q.Owner, Generation: q.Generation, Domain: q.Domain, Key: q.Key, PrevExists: true, Prev: []byte("replaced")}); err != nil {
				t.Fatal(err)
			}
			refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 1, "history/state-domain-change-metadata-retirement.seg")
			if err != nil {
				t.Fatal(err)
			}
			m, err = NewAggregator(dir).Integrate(1, 1, refs)
			if err != nil {
				t.Fatal(err)
			}
			m = addMetadataRouting(t, dir, m, 1)
			if !historyRoutingRetiredObject(old) {
				t.Fatal("page not classified as derived auxiliary")
			}
			release := func() {}
			if mode == "reader" {
				r, err := OpenManager(dir)
				if err != nil {
					t.Fatal(err)
				}
				_, release, err = r.PinHistoryReadView()
				if err != nil {
					t.Fatal(err)
				}
			}
			defer release()
			if mode == "published" {
				data, err := json.Marshal(before)
				if err != nil {
					t.Fatal(err)
				}
				name, err := publishedSnapshotManifestPath(before.Generation, checksumBytes(data))
				if err != nil {
					t.Fatal(err)
				}
				if err = publishImmutableSnapshotManifest(dir, name, data); err != nil {
					t.Fatal(err)
				}
			}
			result, err := pruneRetiredHistoryRoutingContext(context.Background(), dir, retiredMetadataSweepCursor{})
			if err != nil {
				t.Fatal(err)
			}
			_, statErr := os.Stat(filepath.Join(dir, old.Path))
			if mode == "unleased" {
				if result.FilesDeleted != 1 || !os.IsNotExist(statErr) {
					t.Fatal("unleased page retained", result, statErr)
				}
			} else if result.FilesDeleted != 0 || statErr != nil {
				t.Fatal("protected page removed", result, statErr)
			}
		})
	}
}
