package rawdb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"reflect"
	"slices"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestStateChangePostingEncodingRoundTripAndRejectsMalformed(t *testing.T) {
	blocks := make([]uint64, StateChangePostingFrameRows)
	for i := range blocks {
		blocks[i] = 10 + uint64(i*i+1)
	}
	encoded, err := encodeStateChangePosting(blocks)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeStateChangePosting(blocks[0], encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, blocks) {
		t.Fatalf("decoded blocks differ: got %v want %v", decoded, blocks)
	}
	for _, bad := range [][]byte{nil, {0xff, 1}, {stateChangePostingValueVersion, 0}, {stateChangePostingValueVersion, 2, 0}, {stateChangePostingValueVersion, 0x81, 0x02}} {
		if _, err := decodeStateChangePosting(1, bad); err == nil {
			t.Fatalf("accepted malformed posting %x", bad)
		}
	}
}

func TestSingletonStateChangePostingValidation(t *testing.T) {
	value, err := encodeStateChangePosting([]uint64{17})
	if err != nil {
		t.Fatal(err)
	}
	if singleton, err := isSingletonStateChangePosting(17, value); err != nil || !singleton {
		t.Fatalf("singleton = %v, err = %v", singleton, err)
	}
	packed, err := encodeStateChangePosting([]uint64{17, 19})
	if err != nil {
		t.Fatal(err)
	}
	if singleton, err := isSingletonStateChangePosting(17, packed); err != nil || singleton {
		t.Fatalf("packed singleton = %v, err = %v", singleton, err)
	}
	for _, malformed := range [][]byte{
		append(append([]byte(nil), value...), 0),
		{stateChangePostingValueVersion, 2, 0},
	} {
		if _, err := isSingletonStateChangePosting(17, malformed); err == nil {
			t.Fatalf("accepted malformed singleton posting %x", malformed)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		singleton, err := isSingletonStateChangePosting(17, value)
		if err != nil || !singleton {
			t.Fatalf("singleton = %v, err = %v", singleton, err)
		}
	}); allocs != 0 {
		t.Fatalf("singleton posting validation allocated %.2f objects, want zero", allocs)
	}
}

func TestStateChangePostingDeduperIsExactAndResets(t *testing.T) {
	var d stateChangePostingDeduper
	hashes := make([][sha256.Size]byte, 16)
	for i := range hashes {
		hashes[i] = sha256.Sum256([]byte{byte(i)})
		if d.Seen(hashes[i]) {
			t.Fatalf("fresh hash %d reported as seen", i)
		}
	}
	for i := range hashes {
		if !d.Seen(hashes[i]) {
			t.Fatalf("repeated hash %d was not retained", i)
		}
	}
	d.Reset()
	for i := range hashes {
		if d.Seen(hashes[i]) {
			t.Fatalf("reset hash %d reported as seen", i)
		}
	}
}

func TestStateChangePostingLiveExactAndPrefix(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{common.AddressPrefixMainnet, 0x33}
	writePostingTestChange(t, db, owner, 1, "slot/a", true)
	writePostingTestChange(t, db, owner, 2, "slot/a", true)
	writePostingTestChange(t, db, owner, 4, "slot/b", true)
	if got := collectPostingTestBlocks(t, db, owner, []byte("slot/a")); !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("exact blocks = %v", got)
	}
	seen := make(map[uint64]bool)
	if err := IterateStateDomainChangeBlocksByPrefix(db, owner, 0, kvdomains.ContractStorage, []byte("slot/"), func(blockNum uint64) (bool, error) {
		seen[blockNum] = true
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, map[uint64]bool{1: true, 2: true, 4: true}) {
		t.Fatalf("prefix blocks = %v", seen)
	}
}

func TestStateChangePostingCollectorPacksFixed256FramesAndIsRerunnable(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{common.AddressPrefixMainnet, 0x55}
	collector, err := newStateChangePostingCollector(1, 300, etl.Options{TempDir: t.TempDir(), BufferLimit: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()
	for blockNum := uint64(1); blockNum <= 300; blockNum++ {
		change := writePostingTestChange(t, db, owner, blockNum, "slot", false)
		if err := collector.Collect(change); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := collector.Load(db, func() bool { return true }); !errors.Is(err, ErrStateHistoryIndexRebuildInterrupted) {
		t.Fatalf("interrupted load = %v", err)
	}
	result, err := collector.Load(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.SourceRows != 300 || result.DirectoryRows != 1 || result.PostingRows != 2 {
		t.Fatalf("build result = %+v", result)
	}
	latestKey := StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, []byte("slot"))
	it := db.NewIterator(stateChangePostingHashPrefix(stateChangePostingHash(latestKey)), nil)
	defer it.Release()
	var sizes []int
	for it.Next() {
		first := binary.BigEndian.Uint64(it.Key()[len(it.Key())-8:])
		blocks, err := decodeStateChangePosting(first, it.Value())
		if err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, len(blocks))
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sizes, []int{256, 44}) {
		t.Fatalf("frame sizes = %v", sizes)
	}
}

func TestStateChangePostingHashCandidateRequiresOriginalKeyMatch(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{common.AddressPrefixMainnet, 0x44}
	writePostingTestChange(t, db, owner, 7, "other", true)
	targetLatest := StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, []byte("target"))
	value, err := encodeStateChangePosting([]uint64{7})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(stateChangePostingKey(stateChangePostingHash(targetLatest), 7), value); err != nil {
		t.Fatal(err)
	}
	if got := collectPostingTestBlocks(t, db, owner, []byte("target")); len(got) != 0 {
		t.Fatalf("false hash candidates escaped collision check: %v", got)
	}
}

func TestPruneStaleStateChangePostingIndexDeletesOnlyFullyStaleFrames(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{common.AddressPrefixMainnet, 0x66}
	collector, err := newStateChangePostingCollector(1, 2, etl.Options{TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	for blockNum := uint64(1); blockNum <= 2; blockNum++ {
		change := writePostingTestChange(t, db, owner, blockNum, "slot", false)
		if err := collector.Collect(change); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := collector.Load(db, nil); err != nil {
		t.Fatal(err)
	}
	collector.Close()
	if err := DeleteStateDomainChanges(db, 1); err != nil {
		t.Fatal(err)
	}
	stats, err := PruneStaleStateChangePostingIndex(db)
	if err != nil || stats.PostingRowsDeleted != 0 {
		t.Fatalf("mixed frame prune = (%+v,%v)", stats, err)
	}
	if err := DeleteStateDomainChanges(db, 2); err != nil {
		t.Fatal(err)
	}
	stats, err = PruneStaleStateChangePostingIndex(db)
	if err != nil || stats.PostingRowsDeleted != 1 || stats.DirectoryRowsDeleted != 1 {
		t.Fatalf("stale frame prune = (%+v,%v)", stats, err)
	}
}

func TestPruneStaleStateChangePostingIndexThroughContextHonorsCancellation(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stats, err := PruneStaleStateChangePostingIndexThroughContext(ctx, db, 100)
	if !errors.Is(err, context.Canceled) || stats != (StateChangePostingPruneResult{}) {
		t.Fatalf("canceled sweep = (%+v,%v), want zero/context.Canceled", stats, err)
	}
}

func TestPruneStaleStateChangePostingIndexThroughUsesLiveHashDirectory(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{common.AddressPrefixMainnet, 0x67}
	stale := writePostingTestChange(t, db, owner, 1, "stale", false)
	stale2 := writePostingTestChange(t, db, owner, 2, "stale", false)
	live := writePostingTestChange(t, db, owner, 3, "live", false)
	collector, err := newStateChangePostingCollector(1, 3, etl.Options{TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()
	if err := collector.Collect(stale); err != nil {
		t.Fatal(err)
	}
	if err := collector.Collect(stale2); err != nil {
		t.Fatal(err)
	}
	if err := collector.Collect(live); err != nil {
		t.Fatal(err)
	}
	if _, err := collector.Load(db, nil); err != nil {
		t.Fatal(err)
	}
	if err := DeleteStateDomainChanges(db, stale.BlockNum); err != nil {
		t.Fatal(err)
	}
	if err := DeleteStateDomainChanges(db, stale2.BlockNum); err != nil {
		t.Fatal(err)
	}
	countingDB := &stateChangePostingIteratorCountingDB{KeyValueStore: db}
	var phases []string
	stats, err := PruneStaleStateChangePostingIndexThroughContextWithProgress(
		context.Background(),
		countingDB,
		2,
		func(progress StateChangePostingPruneProgress) {
			phases = append(phases, progress.Phase)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if stats.PostingRowsScanned != 2 || stats.PostingRowsDeleted != 1 || stats.DirectoryRowsScanned != 2 || stats.DirectoryRowsDeleted != 1 {
		t.Fatalf("watermark prune stats = %+v", stats)
	}
	if countingDB.iterators != 3 {
		t.Fatalf("watermark prune iterators = %d, want posting + one crossing changeset + directory", countingDB.iterators)
	}
	if !slices.Contains(phases, "postings-complete") || !slices.Contains(phases, "directory-complete") {
		t.Fatalf("progress phases = %v", phases)
	}
	staleLatest := mustStateDomainChangeLatestKey(t, stale)
	if ok, err := db.Has(stateChangeKeyDirectoryKey(staleLatest)); err != nil || ok {
		t.Fatalf("stale directory survived: ok=%v err=%v", ok, err)
	}
	liveLatest := mustStateDomainChangeLatestKey(t, live)
	if ok, err := db.Has(stateChangeKeyDirectoryKey(liveLatest)); err != nil || !ok {
		t.Fatalf("live directory missing: ok=%v err=%v", ok, err)
	}
	if got := collectPostingTestBlocks(t, db, owner, []byte("live")); !reflect.DeepEqual(got, []uint64{3}) {
		t.Fatalf("live posting blocks = %v", got)
	}
}

type stateChangePostingIteratorCountingDB struct {
	ethdb.KeyValueStore
	iterators uint64
}

func (db *stateChangePostingIteratorCountingDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	db.iterators++
	return db.KeyValueStore.NewIterator(prefix, start)
}

func TestPosting256SelectionRecordsFullMainnetBenchmark(t *testing.T) {
	const currentBytes = uint64(30_435_403_003)
	const posting256Bytes = uint64(14_999_336_550)
	const posting1024Bytes = uint64(14_950_447_871)
	if StateChangePostingFrameRows != 256 {
		t.Fatalf("production frame = %d", StateChangePostingFrameRows)
	}
	if saved := currentBytes - posting256Bytes; saved != 15_436_066_453 {
		t.Fatalf("posting-256 savings = %d", saved)
	}
	if marginal := posting256Bytes - posting1024Bytes; marginal != 48_888_679 {
		t.Fatalf("256-to-1024 marginal savings = %d", marginal)
	}
}

func writePostingTestChange(t *testing.T, db ethdb.KeyValueStore, owner common.Address, blockNum uint64, key string, indexed bool) *StateDomainChange {
	t.Helper()
	change := &StateDomainChange{BlockNum: blockNum, TxNum: blockNum, Seq: 1, FlatDomain: StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.ContractStorage, Key: []byte(key), PrevExists: true, Prev: []byte("before")}
	var err error
	if indexed {
		err = WriteStateDomainChange(db, change)
	} else {
		err = WriteStateDomainChangeRow(db, change)
	}
	if err != nil {
		t.Fatal(err)
	}
	return change
}

func collectPostingTestBlocks(t *testing.T, db ethdb.KeyValueStore, owner common.Address, key []byte) []uint64 {
	t.Helper()
	var blocks []uint64
	if err := IterateStateDomainChangeBlocks(db, owner, 0, kvdomains.ContractStorage, key, func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	return blocks
}
