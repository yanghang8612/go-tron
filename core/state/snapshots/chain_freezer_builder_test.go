package snapshots

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestBuildChainFreezerSnapshotPassBuildsContiguousBatches(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()
	appendChainFreezerTestRows(t, store, 0, 4)

	cfg := ChainFreezerSnapshotConfig{
		Dir:         filepath.Join(root, "snapshot"),
		BatchBlocks: 2,
	}
	first, err := BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil {
		t.Fatalf("first chain-freezer snapshot pass: %v", err)
	}
	if !first.Built || first.FromBlock != 0 || first.ToBlock != 1 || first.ColdHead != 2 {
		t.Fatalf("first result = %+v, want build [0,1] with cold head 2", first)
	}
	mgr, err := OpenManager(cfg.Dir)
	if err != nil {
		t.Fatalf("OpenManager after first pass: %v", err)
	}
	if covered, err := mgr.ChainIndexRangeCovered(0, 1); err != nil || !covered {
		t.Fatalf("ChainIndexRangeCovered(0,1) = %v/%v, want true/nil", covered, err)
	}

	second, err := BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil {
		t.Fatalf("second chain-freezer snapshot pass: %v", err)
	}
	if !second.Built || second.FromBlock != 2 || second.ToBlock != 3 || second.ColdHead != 4 {
		t.Fatalf("second result = %+v, want build [2,3] with cold head 4", second)
	}
	third, err := BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil {
		t.Fatalf("third chain-freezer snapshot pass: %v", err)
	}
	if !third.Built || third.FromBlock != 4 || third.ToBlock != 4 || third.ColdHead != 5 {
		t.Fatalf("third result = %+v, want build [4,4] with cold head 5", third)
	}

	idle, err := BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil {
		t.Fatalf("idle chain-freezer snapshot pass: %v", err)
	}
	if idle.Built || idle.ColdHead != 5 {
		t.Fatalf("idle result = %+v, want no build at cold head 5", idle)
	}
	if covered, err := mgr.ChainIndexRangeCovered(0, 4); err != nil || !covered {
		t.Fatalf("ChainIndexRangeCovered(0,4) = %v/%v, want true/nil", covered, err)
	}
}

func TestBuildChainFreezerSnapshotPassRegistersTrustedBuildProof(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()
	appendChainFreezerTestRows(t, store, 0, 1)
	dir := filepath.Join(root, "snapshot")
	cache := NewChainFreezerVerificationCache(dir)
	cfg := ChainFreezerSnapshotConfig{
		Dir:               dir,
		BatchBlocks:       2,
		VerificationCache: cache,
	}

	built, err := BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil {
		t.Fatalf("build pass: %v", err)
	}
	if !built.Built || built.ColdHead != 2 {
		t.Fatalf("build result = %+v, want trusted build through block 1", built)
	}
	stats := cache.Stats()
	if stats.TrustedRecorded != 1 || stats.FullVerified != 0 || stats.Entries != 1 {
		t.Fatalf("trusted build stats = %+v", stats)
	}

	idle, err := BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil {
		t.Fatalf("same-process idle pass: %v", err)
	}
	if idle.Built || idle.ColdHead != 2 {
		t.Fatalf("same-process idle result = %+v, want cold head 2", idle)
	}
	stats = cache.Stats()
	if stats.MemoryHits != 1 || stats.FullVerified != 0 {
		t.Fatalf("same-process trusted stats = %+v", stats)
	}

	restarted := NewChainFreezerVerificationCache(dir)
	cfg.VerificationCache = restarted
	idle, err = BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil {
		t.Fatalf("restart idle pass: %v", err)
	}
	if idle.Built || idle.ColdHead != 2 {
		t.Fatalf("restart idle result = %+v, want cold head 2", idle)
	}
	stats = restarted.Stats()
	if stats.PersistentHits != 1 || stats.FullVerified != 0 || stats.Entries != 1 {
		t.Fatalf("restart trusted stats = %+v", stats)
	}
}

func TestFusedChainFreezerCompanionBuildMatchesStandaloneBuilders(t *testing.T) {
	source := eventLogTestAncient{rows: map[string]map[uint64][]byte{
		rawdb.AncientBlocksTable:     {},
		rawdb.AncientTxInfosTable:    {},
		rawdb.AncientStateRootsTable: {},
	}}
	genesis := canonicalBoundaryTestBlock(t, 0)
	genesisRaw, err := genesis.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	source.rows[rawdb.AncientBlocksTable][0] = genesisRaw
	source.rows[rawdb.AncientTxInfosTable][0] = nil
	source.rows[rawdb.AncientStateRootsTable][0] = chainFreezerTestStateRoot(0)
	for n := uint64(1); n <= 2; n++ {
		block, _, txInfosRaw := chainFreezerBlockWithTx(t, n)
		blockRaw, err := block.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		source.rows[rawdb.AncientBlocksTable][n] = blockRaw
		source.rows[rawdb.AncientTxInfosTable][n] = txInfosRaw
		source.rows[rawdb.AncientStateRootsTable][n] = chainFreezerTestStateRoot(n)
	}

	dir := t.TempDir()
	legacyFreezer, err := BuildChainFreezerSegmentFromAncient(source, dir, "legacy/freezer.seg", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	legacyAccessor, err := BuildChainFreezerAccessorSegmentFromChainFreezerSegment(dir, legacyFreezer, "legacy/accessor.idx")
	if err != nil {
		t.Fatal(err)
	}
	legacyIndex, err := BuildChainIndexSegmentFromChainFreezerSegment(dir, legacyFreezer, "legacy/index.idx")
	if err != nil {
		t.Fatal(err)
	}
	fused, err := buildChainFreezerCompanionSegmentsFromAncient(source, dir,
		"fused/freezer.seg", "fused/accessor.idx", "fused/index.idx", 0, 2, RestoreETLOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fusedByKind := make(map[SegmentKind]SegmentRef, len(fused))
	for _, ref := range fused {
		fusedByKind[ref.Kind] = ref
	}
	for _, legacy := range []SegmentRef{legacyFreezer, legacyAccessor, legacyIndex} {
		got, ok := fusedByKind[legacy.Kind]
		if !ok {
			t.Fatalf("fused refs missing %s", legacy.Kind)
		}
		if got.Size != legacy.Size || got.Checksum != legacy.Checksum {
			t.Fatalf("%s metadata = %d/%s, want %d/%s", legacy.Kind, got.Size, got.Checksum, legacy.Size, legacy.Checksum)
		}
		gotRaw, err := os.ReadFile(filepath.Join(dir, got.Path))
		if err != nil {
			t.Fatal(err)
		}
		wantRaw, err := os.ReadFile(filepath.Join(dir, legacy.Path))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(gotRaw, wantRaw) {
			t.Fatalf("%s fused bytes differ from standalone output", legacy.Kind)
		}
	}
	if err := verifyChainFreezerSnapshotCompanionsSinglePass(dir,
		fusedByKind[SegmentChainFreezer], fusedByKind[SegmentChainIndex], fusedByKind[SegmentChainFreezerAccessor], true); err != nil {
		t.Fatalf("verify fused companions: %v", err)
	}
}

func BenchmarkBuildChainFreezerCompanions(b *testing.B) {
	const blocks = 10_000
	source := eventLogTestAncient{rows: map[string]map[uint64][]byte{
		rawdb.AncientBlocksTable:     make(map[uint64][]byte, blocks),
		rawdb.AncientTxInfosTable:    make(map[uint64][]byte, blocks),
		rawdb.AncientStateRootsTable: make(map[uint64][]byte, blocks),
	}}
	for n := uint64(0); n < blocks; n++ {
		block := coretypes.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
				Number: int64(n), Timestamp: int64(1_000 + n),
			}},
		})
		raw, err := block.Marshal()
		if err != nil {
			b.Fatal(err)
		}
		source.rows[rawdb.AncientBlocksTable][n] = raw
		source.rows[rawdb.AncientTxInfosTable][n] = nil
		source.rows[rawdb.AncientStateRootsTable][n] = nil
	}
	dir := b.TempDir()
	b.Run("standalone-multi-scan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			prefix := fmt.Sprintf("legacy-%d", i)
			freezerRef, err := BuildChainFreezerSegmentFromAncient(source, dir, prefix+"/freezer.seg", 0, blocks-1)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := BuildChainFreezerAccessorSegmentFromChainFreezerSegment(dir, freezerRef, prefix+"/accessor.idx"); err != nil {
				b.Fatal(err)
			}
			if _, err := BuildChainIndexSegmentFromChainFreezerSegment(dir, freezerRef, prefix+"/index.idx"); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fused-single-source-pass", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			prefix := fmt.Sprintf("fused-%d", i)
			if _, err := buildChainFreezerCompanionSegmentsFromAncient(source, dir,
				prefix+"/freezer.seg", prefix+"/accessor.idx", prefix+"/index.idx",
				0, blocks-1, RestoreETLOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestBuildChainFreezerSnapshotPassUsesHeavyWorkGateLazily(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()
	appendChainFreezerTestRows(t, store, 0, 1)
	gate := maintenance.NewHeavyWorkGate()
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("hold heavy-work gate")
	}
	cfg := ChainFreezerSnapshotConfig{
		Dir:           filepath.Join(root, "snapshot"),
		HeavyWorkGate: gate,
	}
	deferred, err := BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil || !deferred.ResourceDeferred || deferred.Built {
		t.Fatalf("deferred pass = %+v/%v", deferred, err)
	}
	release()
	built, err := BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil || !built.Built || built.ResourceDeferred {
		t.Fatalf("admitted pass = %+v/%v", built, err)
	}

	// An up-to-date pass must release the verification gate promptly.
	idle, err := BuildChainFreezerSnapshotPass(store, nil, cfg)
	if err != nil || idle.Built || idle.ResourceDeferred {
		t.Fatalf("idle pass = %+v/%v", idle, err)
	}
	if release, ok := gate.TryAcquire(); !ok {
		t.Fatal("idle chain-freezer pass did not release heavy-work gate")
	} else {
		release()
	}
}

func TestVerifyChainFreezerSnapshotCompanionsSinglePassMatchesStrictGates(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()
	appendChainFreezerTestRows(t, store, 0, 4)
	dir := filepath.Join(root, "snapshot")
	freezerRef, err := BuildChainFreezerSegmentFromAncient(store, dir, "", 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	accessorRef, err := BuildChainFreezerAccessorSegmentFromChainFreezerSegment(dir, freezerRef, "")
	if err != nil {
		t.Fatal(err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(dir, freezerRef, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckChainFreezerSegment(dir, freezerRef); err != nil {
		t.Fatalf("strict freezer gate: %v", err)
	}
	if err := VerifyChainFreezerAccessorSegmentAgainstChainFreezer(dir, accessorRef, freezerRef); err != nil {
		t.Fatalf("strict accessor gate: %v", err)
	}
	if err := VerifyChainIndexSegmentAgainstChainFreezer(dir, indexRef, freezerRef); err != nil {
		t.Fatalf("strict index gate: %v", err)
	}
	if err := verifyChainFreezerSnapshotCompanionsSinglePass(dir, freezerRef, indexRef, accessorRef, true); err != nil {
		t.Fatalf("single-pass gate: %v", err)
	}

	offsets, err := chainFreezerRowOffsets(dir, freezerRef)
	if err != nil {
		t.Fatal(err)
	}
	offsets[1] = offsets[2]
	offsets[2]++
	staleAccessor, err := writeChainFreezerAccessorSegment(dir, SegmentRef{
		Dataset:   SegmentDatasetChainFreezer,
		Kind:      SegmentChainFreezerAccessor,
		FromTxNum: 0,
		ToTxNum:   4,
		Path:      "chain/stale-accessor.idx",
	}, offsets)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckChainFreezerAccessorSegment(dir, staleAccessor); err != nil {
		t.Fatalf("stale accessor structural gate: %v", err)
	}
	if err := verifyChainFreezerSnapshotCompanionsSinglePass(dir, freezerRef, indexRef, staleAccessor, true); err == nil || !strings.Contains(err.Error(), "offset for block 1") {
		t.Fatalf("single-pass stale accessor error = %v, want exact offset mismatch", err)
	}
}

func TestChainFreezerVerificationCacheMemoryAndRestartRoutes(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()
	appendChainFreezerTestRows(t, store, 0, 4)
	dir := filepath.Join(root, "snapshot")
	built, err := NewAggregator(dir).BuildChainFreezer(store, 0, 4)
	if err != nil {
		t.Fatal(err)
	}

	cache := NewChainFreezerVerificationCache(dir)
	head, err := verifiedChainFreezerSnapshotHeadWithCache(dir, built.Manifest, cache)
	if err != nil || head != 5 {
		t.Fatalf("full cached head = %d/%v, want 5/nil", head, err)
	}
	stats := cache.Stats()
	if stats.FullVerified != 1 || stats.MemoryHits != 0 || stats.PersistentHits != 0 || stats.Entries != 1 {
		t.Fatalf("full stats = %+v", stats)
	}

	head, err = verifiedChainFreezerSnapshotHeadWithCache(dir, built.Manifest, cache)
	if err != nil || head != 5 {
		t.Fatalf("memory cached head = %d/%v, want 5/nil", head, err)
	}
	stats = cache.Stats()
	if stats.FullVerified != 1 || stats.MemoryHits != 1 || stats.PersistentHits != 0 || stats.Entries != 1 {
		t.Fatalf("memory stats = %+v", stats)
	}

	restarted := NewChainFreezerVerificationCache(dir)
	if err := restarted.LoadError(); err != nil {
		t.Fatalf("restart cache load: %v", err)
	}
	head, err = verifiedChainFreezerSnapshotHeadWithCache(dir, built.Manifest, restarted)
	if err != nil || head != 5 {
		t.Fatalf("persistent cached head = %d/%v, want 5/nil", head, err)
	}
	stats = restarted.Stats()
	if stats.FullVerified != 0 || stats.MemoryHits != 0 || stats.PersistentHits != 1 || stats.Entries != 1 {
		t.Fatalf("persistent stats = %+v", stats)
	}
}

func TestChainFreezerVerificationCacheRejectsSameSizeTamper(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()
	appendChainFreezerTestRows(t, store, 0, 4)
	dir := filepath.Join(root, "snapshot")
	built, err := NewAggregator(dir).BuildChainFreezer(store, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	cache := NewChainFreezerVerificationCache(dir)
	if _, err := verifiedChainFreezerSnapshotHeadWithCache(dir, built.Manifest, cache); err != nil {
		t.Fatal(err)
	}
	indexRef := chainIndexRefs(built.Manifest)[0]
	path := filepath.Join(dir, indexRef.Path)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Second)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}

	if _, err := verifiedChainFreezerSnapshotHeadWithCache(dir, built.Manifest, cache); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("same-process tamper error = %v, want checksum mismatch", err)
	}
	restarted := NewChainFreezerVerificationCache(dir)
	if _, err := verifiedChainFreezerSnapshotHeadWithCache(dir, built.Manifest, restarted); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("restart tamper error = %v, want checksum mismatch", err)
	}
}

func TestChainFreezerVerificationCacheMalformedFileFallsBackAndRepairs(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()
	appendChainFreezerTestRows(t, store, 0, 1)
	dir := filepath.Join(root, "snapshot")
	built, err := NewAggregator(dir).BuildChainFreezer(store, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, chainFreezerVerificationCacheFile), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	cache := NewChainFreezerVerificationCache(dir)
	if err := cache.LoadError(); err == nil {
		t.Fatal("malformed cache load succeeded")
	}
	if _, err := verifiedChainFreezerSnapshotHeadWithCache(dir, built.Manifest, cache); err != nil {
		t.Fatalf("fallback full verification: %v", err)
	}
	if stats := cache.Stats(); stats.FullVerified != 1 || stats.LoadErrors != 1 || stats.Entries != 1 {
		t.Fatalf("fallback stats = %+v", stats)
	}
	repaired := NewChainFreezerVerificationCache(dir)
	if err := repaired.LoadError(); err != nil {
		t.Fatalf("repaired cache load: %v", err)
	}
}

func TestEventLogVerificationCacheMemoryRestartAndTamper(t *testing.T) {
	dir, manifest := buildEventLogVerificationFixture(t, 3, 2)
	cache := NewChainFreezerVerificationCache(dir)
	head, err := verifiedIndexedEventLogHeadWithCache(dir, manifest, cache)
	if err != nil || head != 4 {
		t.Fatalf("full event head = %d/%v, want 4/nil", head, err)
	}
	stats := cache.Stats()
	if stats.EventFullVerified != 1 || stats.EventMemoryHits != 0 || stats.EventPersistentHits != 0 || stats.EventEntries != 1 {
		t.Fatalf("full event stats = %+v", stats)
	}
	head, err = verifiedIndexedEventLogHeadWithCache(dir, manifest, cache)
	if err != nil || head != 4 {
		t.Fatalf("memory event head = %d/%v, want 4/nil", head, err)
	}
	stats = cache.Stats()
	if stats.EventMemoryHits != 1 || stats.EventPersistentHits != 0 {
		t.Fatalf("memory event stats = %+v", stats)
	}

	restarted := NewChainFreezerVerificationCache(dir)
	if err := restarted.LoadError(); err != nil {
		t.Fatal(err)
	}
	head, err = verifiedIndexedEventLogHeadWithCache(dir, manifest, restarted)
	if err != nil || head != 4 {
		t.Fatalf("persistent event head = %d/%v, want 4/nil", head, err)
	}
	stats = restarted.Stats()
	if stats.EventFullVerified != 0 || stats.EventPersistentHits != 1 || stats.EventEntries != 1 {
		t.Fatalf("persistent event stats = %+v", stats)
	}

	indexRef := eventLogIndexRefs(manifest)[0]
	path := filepath.Join(dir, indexRef.Path)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Second)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	if _, err := verifiedIndexedEventLogHeadWithCache(dir, manifest, restarted); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("same-process event tamper error = %v, want checksum mismatch", err)
	}
	if _, err := verifiedIndexedEventLogHeadWithCache(dir, manifest, NewChainFreezerVerificationCache(dir)); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("restart event tamper error = %v, want checksum mismatch", err)
	}
}

func TestChainFreezerVerificationCachePersistsChainAndEventProofsTogether(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()
	appendChainFreezerTestRows(t, store, 0, 1)
	dir := filepath.Join(root, "snapshot")
	chainBuilt, err := NewAggregator(dir).BuildChainFreezer(store, 0, 1)
	if err != nil {
		t.Fatal(err)
	}

	db := rawdb.NewMemoryChainDB()
	topic := common.Hash{0xaa}
	block, infos := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{{
		Address: eventLogTestAddress(0x31),
		Topics:  [][]byte{topic[:]},
	}})
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos); err != nil {
		t.Fatal(err)
	}
	eventBuilt, err := NewAggregator(dir).BuildEventLogs(db, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(eventBuilt.Manifest.Segments) <= len(chainBuilt.Manifest.Segments) {
		t.Fatalf("combined manifest segments = %d, initial chain segments = %d", len(eventBuilt.Manifest.Segments), len(chainBuilt.Manifest.Segments))
	}

	cache := NewChainFreezerVerificationCache(dir)
	if _, err := verifiedChainFreezerSnapshotHeadWithCache(dir, eventBuilt.Manifest, cache); err != nil {
		t.Fatal(err)
	}
	if _, err := verifiedIndexedEventLogHeadWithCache(dir, eventBuilt.Manifest, cache); err != nil {
		t.Fatal(err)
	}
	restarted := NewChainFreezerVerificationCache(dir)
	if err := restarted.LoadError(); err != nil {
		t.Fatal(err)
	}
	stats := restarted.Stats()
	if stats.Entries != 1 || stats.EventEntries != 1 {
		t.Fatalf("combined restart stats = %+v", stats)
	}
	if _, err := verifiedChainFreezerSnapshotHeadWithCache(dir, eventBuilt.Manifest, restarted); err != nil {
		t.Fatal(err)
	}
	if _, err := verifiedIndexedEventLogHeadWithCache(dir, eventBuilt.Manifest, restarted); err != nil {
		t.Fatal(err)
	}
	stats = restarted.Stats()
	if stats.PersistentHits != 1 || stats.EventPersistentHits != 1 {
		t.Fatalf("combined persistent stats = %+v", stats)
	}
}

func BenchmarkVerifiedIndexedEventLogHeadCache(b *testing.B) {
	dir, manifest := buildEventLogVerificationFixture(b, 1_000, 10)
	eventRefs := eventLogRefs(manifest)
	indexRef := eventLogIndexRefs(manifest)[0]
	warmCache := NewChainFreezerVerificationCache(dir)
	if _, err := verifiedIndexedEventLogHeadWithCache(dir, manifest, warmCache); err != nil {
		b.Fatal(err)
	}
	b.Run("legacy-prefix", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := verifiedIndexedEventLogHead(dir, manifest); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("single-semantic-pass", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if err := verifyEventLogIndexSegmentAgainstEventLogs(dir, indexRef, eventRefs); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("persistent-checksum", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			cache := NewChainFreezerVerificationCache(dir)
			if _, err := verifiedIndexedEventLogHeadWithCache(dir, manifest, cache); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("memory-identity", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := verifiedIndexedEventLogHeadWithCache(dir, manifest, warmCache); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func buildEventLogVerificationFixture(t testing.TB, blocks, logsPerBlock uint64) (string, *Manifest) {
	t.Helper()
	db := rawdb.NewMemoryChainDB()
	for blockNum := uint64(1); blockNum <= blocks; blockNum++ {
		logs := make([]*corepb.TransactionInfo_Log, 0, logsPerBlock)
		for logIndex := uint64(0); logIndex < logsPerBlock; logIndex++ {
			var topic common.Hash
			topic[0] = byte(blockNum)
			topic[1] = byte(logIndex)
			logs = append(logs, &corepb.TransactionInfo_Log{
				Address: eventLogTestAddress(byte(blockNum + logIndex)),
				Topics:  [][]byte{topic[:]},
				Data:    []byte{byte(logIndex)},
			})
		}
		block, infos := eventLogTestBlock(t, blockNum, logs)
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(db, blockNum, infos); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	built, err := NewAggregator(dir).BuildEventLogs(db, 1, blocks)
	if err != nil {
		t.Fatal(err)
	}
	return dir, built.Manifest
}

func BenchmarkVerifyChainFreezerSnapshotCompanions(b *testing.B) {
	const blocks = 10_000
	dir := b.TempDir()
	rows := make([]chainFreezerRow, blocks)
	for n := uint64(0); n < blocks; n++ {
		block := coretypes.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:    int64(n),
					Timestamp: int64(1_000 + n),
				},
			},
		})
		raw, err := block.Marshal()
		if err != nil {
			b.Fatalf("marshal block %d: %v", n, err)
		}
		rows[n] = chainFreezerRow{blockNum: n, blockRaw: raw}
	}
	freezerRef := writeChainFreezerSegmentRowsForTest(b, dir, "", 0, blocks-1, rows)
	accessorRef, err := BuildChainFreezerAccessorSegmentFromChainFreezerSegment(dir, freezerRef, "")
	if err != nil {
		b.Fatal(err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(dir, freezerRef, "")
	if err != nil {
		b.Fatal(err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{freezerRef, accessorRef, indexRef})
	warmCache := NewChainFreezerVerificationCache(dir)
	if _, err := verifiedChainFreezerSnapshotHeadWithCache(dir, manifest, warmCache); err != nil {
		b.Fatal(err)
	}

	b.Run("legacy-multi-pass", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if err := CheckChainFreezerSegment(dir, freezerRef); err != nil {
				b.Fatal(err)
			}
			if err := VerifyChainFreezerAccessorSegmentAgainstChainFreezer(dir, accessorRef, freezerRef); err != nil {
				b.Fatal(err)
			}
			if err := VerifyChainIndexSegmentAgainstChainFreezer(dir, indexRef, freezerRef); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("single-pass", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if err := verifyChainFreezerSnapshotCompanionsSinglePass(dir, freezerRef, indexRef, accessorRef, true); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("persistent-checksum", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			cache := NewChainFreezerVerificationCache(dir)
			if _, err := verifiedChainFreezerSnapshotHeadWithCache(dir, manifest, cache); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("memory-identity", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := verifiedChainFreezerSnapshotHeadWithCache(dir, manifest, warmCache); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestBuildChainFreezerSnapshotPassRejectsUncoveredLocalTail(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()
	appendChainFreezerTestRows(t, store, 0, 2)
	if _, err := store.TruncateTail(1); err != nil {
		t.Fatalf("TruncateTail: %v", err)
	}

	_, err := BuildChainFreezerSnapshotPass(store, nil, ChainFreezerSnapshotConfig{Dir: filepath.Join(root, "snapshot")})
	if err == nil || !strings.Contains(err.Error(), "exceeds verified cold coverage") {
		t.Fatalf("BuildChainFreezerSnapshotPass error = %v, want uncovered local-tail rejection", err)
	}
}

func TestBuildChainFreezerSnapshotPassBuildsIndexedEventLogs(t *testing.T) {
	root := t.TempDir()
	store := openChainFreezerTestStore(t, filepath.Join(root, "ancient"))
	defer store.Close()

	block0 := canonicalBoundaryTestBlock(t, 0)
	block1, _, txInfosRaw := chainFreezerBlockWithTx(t, 1)
	var ret corepb.TransactionRet
	if err := proto.Unmarshal(txInfosRaw, &ret); err != nil {
		t.Fatalf("unmarshal transaction infos: %v", err)
	}
	address := []byte{common.AddressPrefixMainnet, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40, 0x41, 0x42, 0x43, 0x44}
	topic := common.Hash{0xaa}
	ret.Transactioninfo[0].Log = []*corepb.TransactionInfo_Log{{
		Address: address,
		Topics:  [][]byte{topic[:]},
		Data:    []byte{0x01, 0x02},
	}}
	txInfosRaw, err := proto.Marshal(&ret)
	if err != nil {
		t.Fatalf("marshal transaction infos: %v", err)
	}
	appendChainFreezerRawRows(t, store, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfosRaw},
	})

	disk := rawdb.NewMemoryDatabase()
	chain := rawdb.NewChainDB(disk, rawdb.NewFreezerReader(store))
	if err := rawdb.WriteBlock(chain, block0); err != nil {
		t.Fatalf("WriteBlock(0): %v", err)
	}
	if err := rawdb.WriteBlock(chain, block1); err != nil {
		t.Fatalf("WriteBlock(1): %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(chain, 1, ret.Transactioninfo); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock(1): %v", err)
	}

	snapshotDir := filepath.Join(root, "snapshot")
	cache := NewChainFreezerVerificationCache(snapshotDir)
	result, err := BuildChainFreezerSnapshotPass(store, chain, ChainFreezerSnapshotConfig{
		Dir:               snapshotDir,
		BatchBlocks:       2,
		BuildEventLogs:    true,
		VerificationCache: cache,
	})
	if err != nil {
		t.Fatalf("BuildChainFreezerSnapshotPass: %v", err)
	}
	if !result.Built || !result.EventLogBuilt || result.EventLogFromBlock != 1 || result.EventLogToBlock != 1 {
		t.Fatalf("result = %+v, want chain and indexed event-log build through block 1", result)
	}
	stats := cache.Stats()
	if stats.TrustedRecorded != 1 || stats.EventTrustedRecorded != 1 || stats.FullVerified != 0 || stats.EventFullVerified != 0 || stats.Entries != 1 || stats.EventEntries != 1 {
		t.Fatalf("trusted chain/event build stats = %+v", stats)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	if covered, err := mgr.EventLogIndexedRangeCovered(1, 1); err != nil || !covered {
		t.Fatalf("EventLogIndexedRangeCovered(1,1) = %v/%v, want true/nil", covered, err)
	}
	var rows []rawdb.EventLog
	if err := mgr.IterateEventLogs(1, 1, rawdb.EventLogFilter{}, func(row rawdb.EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs: %v", err)
	}
	if len(rows) != 1 || rows[0].BlockNum != 1 || string(rows[0].Log.GetData()) != string([]byte{0x01, 0x02}) {
		t.Fatalf("event rows = %+v, want one archived block-1 log", rows)
	}
	if stage, ok, err := rawdb.ReadStageProgressRow(chain, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || !stage.HasBlockHash || stage.BlockNum != 1 || stage.BlockHash != block1.Hash() {
		t.Fatalf("event-log stage = %+v ok=%v err=%v, want hash-bound block 1", stage, ok, err)
	}
}
