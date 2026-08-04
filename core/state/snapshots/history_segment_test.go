package snapshots

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func BenchmarkBuildStateDomainChangeHistoryTxRanges(b *testing.B) {
	b.Run("sparse", func(b *testing.B) { benchmarkBuildStateDomainChangeHistoryTxRanges(b, false) })
	b.Run("dense", func(b *testing.B) { benchmarkBuildStateDomainChangeHistoryTxRanges(b, true) })
}

func benchmarkBuildStateDomainChangeHistoryTxRanges(b *testing.B, dense bool) {
	db, err := rawdb.NewPebbleDB(b.TempDir(), 64, 64)
	if err != nil {
		b.Fatalf("open Pebble: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	const blocks = uint64(5_000)
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x5c}, common.AccountIDLength)...))
	for block := uint64(1); block <= blocks; block++ {
		if err := rawdb.WriteStateTxRange(db, block, common.Hash{byte(block)}, block, block); err != nil {
			b.Fatalf("write tx range %d: %v", block, err)
		}
		if dense || block == 1 {
			change := &rawdb.StateDomainChange{
				BlockNum: block, BlockHash: common.Hash{byte(block)}, TxNum: block, Seq: 1,
				FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner,
				Domain: kvdomains.ContractStorage, Key: []byte("slot"), NextExists: true, Next: []byte("value"),
			}
			if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{change}); err != nil {
				b.Fatalf("write state change %d: %v", block, err)
			}
		}
	}
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		b.Fatal("missing state-domain-change registry")
	}
	root := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(root, fmt.Sprintf("run-%d", i))
		if err := os.Mkdir(dir, 0o755); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_, err = buildStateDomainChangeHistoryBinarySegmentsFromDBRange(db, dir, SegmentRef{
			Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
			FromTxNum: 1, ToTxNum: blocks, Path: "history/state-domain-change-1-5000.seg",
		}, cfg, etl.Options{}, &stateDomainChangeHistoryBlockRange{from: 1, to: blocks})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestStateDomainChangeHistorySegmentBuildOpenAndCheck(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x55}, common.AccountIDLength)...))

	if err := rawdb.WriteStateTxRange(db, 2, common.Hash{0x02}, 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateTxRange(db, 3, common.Hash{0x03}, 3, 3); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  common.Hash{0x02},
		TxNum:      2,
		Seq:        2,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 0,
		Domain:     kvdomains.SystemReward,
		Key:        []byte("b"),
		PrevExists: true,
		Prev:       []byte("b1"),
		NextExists: true,
		Next:       []byte("b2"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  common.Hash{0x02},
		TxNum:      2,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 0,
		Domain:     kvdomains.SystemReward,
		Key:        []byte("a"),
		PrevExists: true,
		Prev:       []byte("a1"),
		NextExists: true,
		Next:       []byte("a2"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   3,
		BlockHash:  common.Hash{0x03},
		TxNum:      3,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVGeneration,
		Owner:      owner,
		NextExists: true,
		Next:       rawdb.EncodeStateKVGenerationValue(1),
	}); err != nil {
		t.Fatal(err)
	}

	ref, err := BuildStateDomainChangeHistorySegmentFromDB(db, dir, 2, 2, "history/state-domain-change-2.json")
	if err != nil {
		t.Fatalf("build state-domain-change history: %v", err)
	}
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory || ref.Size == 0 || ref.Checksum == "" {
		t.Fatalf("ref = %+v", ref)
	}
	seg, err := OpenStateDomainChangeSegment(dir, ref)
	if err != nil {
		t.Fatalf("open state-domain-change history: %v", err)
	}
	if len(seg.Changes) != 2 || seg.Changes[0].Seq != 1 || seg.Changes[1].Seq != 2 {
		t.Fatalf("changes not filtered/sorted: %+v", seg.Changes)
	}
	if len(seg.TxRanges) != 1 || seg.TxRanges[0].BlockNum != 2 || seg.TxRanges[0].BeginTxNum != 2 || seg.TxRanges[0].EndTxNum != 2 {
		t.Fatalf("tx ranges = %+v, want block 2 range [2,2]", seg.TxRanges)
	}
	if err := PublishManifest(dir, NewManifest(2, 2, []SegmentRef{ref})); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	if _, err := OpenManager(dir); err == nil {
		t.Fatal("production manager accepted legacy JSON history")
	}
	if _, err := OpenStateDomainChangeSegment(dir, SegmentRef{
		Dataset:   ref.Dataset,
		Kind:      ref.Kind,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		Path:      ref.Path,
		Size:      ref.Size,
		Checksum:  "sha256:bad",
	}); err == nil {
		t.Fatal("bad state-domain-change checksum accepted")
	}
}

func TestStateDomainChangeHistorySegmentFiltersSameBlockByTxNum(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x56}, common.AccountIDLength)...))
	begin, end, err := rawdb.NextStateTxRange(0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateTxRange(db, 8, common.Hash{0x08}, begin, end); err != nil {
		t.Fatal(err)
	}
	for i, txNum := range []uint64{begin, begin + 1, begin + 2, end} {
		if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
			BlockNum:   8,
			BlockHash:  common.Hash{0x08},
			TxNum:      txNum,
			Seq:        uint64(i + 1),
			FlatDomain: rawdb.StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     kvdomains.SystemReward,
			Key:        []byte{byte('a' + i)},
			NextExists: true,
			Next:       []byte{byte('1' + i)},
		}); err != nil {
			t.Fatalf("write change %d: %v", i, err)
		}
	}

	ref, err := BuildStateDomainChangeHistorySegmentFromDB(db, dir, begin+1, begin+2, "history/state-domain-change-8-partial.json")
	if err != nil {
		t.Fatalf("build state-domain-change history: %v", err)
	}
	seg, err := OpenStateDomainChangeSegment(dir, ref)
	if err != nil {
		t.Fatalf("open state-domain-change history: %v", err)
	}
	if len(seg.Changes) != 2 || seg.Changes[0].TxNum != begin+1 || seg.Changes[1].TxNum != begin+2 {
		t.Fatalf("filtered changes = %+v, want txNums [%d,%d]", seg.Changes, begin+1, begin+2)
	}
}

func TestManagerReadsHistoryFromTieredSnapshotDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory symlinks require elevated privileges on Windows")
	}

	dir := t.TempDir()
	coldHistoryDir := t.TempDir()
	if err := os.Symlink(coldHistoryDir, filepath.Join(dir, "history")); err != nil {
		t.Fatalf("link cold history directory: %v", err)
	}

	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x59}, common.AccountIDLength)...))
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{0x01}, 10, 10); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   1,
		BlockHash:  common.Hash{0x01},
		TxNum:      10,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Domain:     kvdomains.ContractStorage,
		Key:        []byte("slot/a"),
		NextExists: true,
		Next:       []byte("value"),
	}); err != nil {
		t.Fatal(err)
	}

	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 10, "history/state-domain-change-10-10.seg")
	if err != nil {
		t.Fatalf("build tiered history: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(10, 10, refs)); err != nil {
		t.Fatalf("publish tiered manifest: %v", err)
	}
	resolved, err := filepath.EvalSymlinks(filepath.Join(dir, refs[0].Path))
	if err != nil {
		t.Fatalf("resolve tiered history segment: %v", err)
	}
	resolvedColdHistoryDir, err := filepath.EvalSymlinks(coldHistoryDir)
	if err != nil {
		t.Fatalf("resolve cold history directory: %v", err)
	}
	if filepath.Dir(resolved) != resolvedColdHistoryDir {
		t.Fatalf("history segment path = %s, want directory %s", resolved, resolvedColdHistoryDir)
	}

	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open tiered manager: %v", err)
	}
	var changes []*rawdb.StateDomainChange
	if err := mgr.IterateStateDomainChangesByKey(10, 10, rawdb.StateFlatDomainKVLatest, owner, 0, kvdomains.ContractStorage, []byte("slot/a"), func(change *rawdb.StateDomainChange) (bool, error) {
		changes = append(changes, change)
		return true, nil
	}); err != nil {
		t.Fatalf("read tiered history: %v", err)
	}
	if len(changes) != 1 || changes[0].PrevExists || changes[0].NextExists {
		t.Fatalf("tiered history changes = %+v, want one absent preimage", changes)
	}
}

func TestManagerIteratesStateDomainChangesByAccessorKey(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x57}, common.AccountIDLength)...))
	other := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x58}, common.AccountIDLength)...))

	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{0x01}, 10, 11); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   1,
		BlockHash:  common.Hash{0x01},
		TxNum:      10,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 3,
		Domain:     kvdomains.ContractStorage,
		Key:        []byte("slot/a"),
		PrevExists: true,
		Prev:       []byte("old-a"),
		NextExists: true,
		Next:       []byte("new-a"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   1,
		BlockHash:  common.Hash{0x01},
		TxNum:      11,
		Seq:        2,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      other,
		Generation: 3,
		Domain:     kvdomains.ContractStorage,
		Key:        []byte("slot/a"),
		NextExists: true,
		Next:       []byte("other"),
	}); err != nil {
		t.Fatal(err)
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 11, "history/state-domain-change-10-11.seg")
	if err != nil {
		t.Fatalf("build binary history: %v", err)
	}
	var published []SegmentRef
	for _, ref := range refs {
		published = append(published, ref)
	}
	if len(published) != 3 {
		t.Fatalf("published refs = %+v, want history+accessor+index", published)
	}
	if err := PublishManifest(dir, NewManifest(10, 11, published)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	gotRange, ok, err := mgr.StateTxRangeForBlock(1)
	if err != nil || !ok {
		t.Fatalf("state tx range from binary cold segment: ok=%v err=%v", ok, err)
	}
	if gotRange.BlockNum != 1 || gotRange.BeginTxNum != 10 || gotRange.EndTxNum != 11 {
		t.Fatalf("state tx range from binary cold segment = %+v", gotRange)
	}
	var got []*rawdb.StateDomainChange
	if err := mgr.IterateStateDomainChangesByKey(10, 11, rawdb.StateFlatDomainKVLatest, owner, 3, kvdomains.ContractStorage, []byte("slot/a"), func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate by key: %v", err)
	}
	if len(got) != 1 || got[0].Owner != owner || string(got[0].Prev) != "old-a" {
		t.Fatalf("got changes = %+v", got)
	}
	got = nil
	if err := mgr.IterateStateDomainChangesByPrefix(10, 11, owner, 3, kvdomains.ContractStorage, []byte("slot/"), func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate by prefix: %v", err)
	}
	if len(got) != 1 || got[0].Owner != owner || string(got[0].Key) != "slot/a" || string(got[0].Prev) != "old-a" {
		t.Fatalf("prefix changes = %+v", got)
	}
}

func TestBuildStateDomainChangeHistoryStreamsAccessorETL(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x59}, common.AccountIDLength)...))

	for _, row := range []*rawdb.StateTxRange{
		{BlockNum: 1, BlockHash: common.Hash{0x01}, BeginTxNum: 10, EndTxNum: 11},
		{BlockNum: 2, BlockHash: common.Hash{0x02}, BeginTxNum: 12, EndTxNum: 13},
	} {
		if err := rawdb.WriteStateTxRange(db, row.BlockNum, row.BlockHash, row.BeginTxNum, row.EndTxNum); err != nil {
			t.Fatalf("write tx range: %v", err)
		}
	}
	changes := []*rawdb.StateDomainChange{
		// Fresh block packs are physically ordered by txNum/sequence. Prefix-heavy
		// keys still force both accessor collectors through their external sort.
		{BlockNum: 1, BlockHash: common.Hash{0x01}, TxNum: 10, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.ContractStorage, Key: []byte{0x00}, NextExists: true, Next: []byte("one")},
		{BlockNum: 1, BlockHash: common.Hash{0x01}, TxNum: 11, Seq: 2, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.ContractStorage, Key: []byte{0x00, 0x00}, NextExists: true, Next: []byte("two")},
		{BlockNum: 2, BlockHash: common.Hash{0x02}, TxNum: 12, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.ContractStorage, Key: []byte{0x00}, NextExists: true, Next: []byte("three")},
		{BlockNum: 2, BlockHash: common.Hash{0x02}, TxNum: 13, Seq: 2, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.ContractStorage, Key: []byte{0x01}, NextExists: true, Next: []byte("four")},
	}
	if err := rawdb.WriteStateDomainChangeBlockRows(db, changes[:2]); err != nil {
		t.Fatalf("write block 1 state changes: %v", err)
	}
	if err := rawdb.WriteStateDomainChangeBlockRows(db, changes[2:]); err != nil {
		t.Fatalf("write block 2 state changes: %v", err)
	}

	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		t.Fatal("missing state-domain-change registry")
	}
	originalTxRangeIterator := cfg.IterateHotHistoryTxRangeBorrowed
	var txRangeScans int
	cfg.IterateHotHistoryTxRangeBorrowed = func(db ethdb.Iteratee, fromBlock, toBlock uint64, fn func(*rawdb.StateTxRange) (bool, error)) error {
		txRangeScans++
		return originalTxRangeIterator(db, fromBlock, toBlock, fn)
	}
	result, err := buildStateDomainChangeHistoryBinarySegmentsFromDBRange(db, dir, SegmentRef{
		Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
		FromTxNum: 10, ToTxNum: 13, Path: "history/state-domain-change-10-13.seg",
	}, cfg, etl.Options{TempDir: filepath.Join(dir, "etl-scratch"), BufferLimit: 1}, &stateDomainChangeHistoryBlockRange{from: 1, to: 2})
	if err != nil {
		t.Fatalf("build streamed binary history: %v", err)
	}
	if txRangeScans != 1 {
		t.Fatalf("tx-range scans = %d, want one streamed pass", txRangeScans)
	}
	if result.recordETL.Collected != 0 || result.recordETL.SpilledRuns != 0 {
		t.Fatalf("ordered build used record ETL: %+v", result.recordETL)
	}
	if result.accessorETL.SpilledRuns < 2 {
		t.Fatalf("accessor ETL spilled %d runs, want forced external spill", result.accessorETL.SpilledRuns)
	}
	if result.accessorETL.Applied != 4 {
		t.Fatalf("accessor ETL applied %d entries, want 4", result.accessorETL.Applied)
	}
	if len(result.refs) != 3 {
		t.Fatalf("streamed refs = %+v, want history+accessor+index", result.refs)
	}
	assertAccessorMatchesPostCompressionRebuild(t, dir, result.refs[0], result.refs[1])
	if err := verifyStateDomainChangeBinaryCompanionsAgainstSegment(dir, result.refs[0], result.refs[2], result.refs[1]); err != nil {
		t.Fatalf("verify streamed companions: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(10, 13, result.refs)); err != nil {
		t.Fatalf("publish streamed history manifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open streamed history manager: %v", err)
	}
	var exact []uint64
	if err := mgr.IterateStateDomainChangesByKey(10, 13, rawdb.StateFlatDomainKVLatest, owner, 0, kvdomains.ContractStorage, []byte{0x00}, func(change *rawdb.StateDomainChange) (bool, error) {
		exact = append(exact, change.TxNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate streamed history by exact key: %v", err)
	}
	if len(exact) != 2 || exact[0] != 10 || exact[1] != 12 {
		t.Fatalf("exact key txNums = %v, want [10 12]", exact)
	}
	var prefix []uint64
	if err := mgr.IterateStateDomainChangesByPrefix(10, 13, owner, 0, kvdomains.ContractStorage, []byte{0x00}, func(change *rawdb.StateDomainChange) (bool, error) {
		prefix = append(prefix, change.TxNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate streamed history by prefix: %v", err)
	}
	if len(prefix) != 3 || prefix[0] != 10 || prefix[1] != 12 || prefix[2] != 11 {
		t.Fatalf("prefix key txNums = %v, want [10 12 11]", prefix)
	}
}

func TestBuildStateDomainChangeHistoryFallsBackForUnorderedHotRows(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x5d}, common.AccountIDLength)...))
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{0x01}, 10, 11); err != nil {
		t.Fatal(err)
	}
	for _, change := range []*rawdb.StateDomainChange{
		{BlockNum: 1, TxNum: 11, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.ContractStorage, Key: []byte("first")},
		{BlockNum: 1, TxNum: 10, Seq: 2, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.ContractStorage, Key: []byte("second")},
	} {
		if err := rawdb.WriteStateDomainChange(db, change); err != nil {
			t.Fatal(err)
		}
	}
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		t.Fatal("missing state-domain-change registry")
	}
	// The compatibility fallback intentionally consumes retired standalone rows.
	// Disable the fresh-schema borrowed iterator for this one transition test.
	cfg.IterateHotHistoryBlockTxBorrowed = nil
	result, err := buildStateDomainChangeHistoryBinarySegmentsFromDBRange(db, dir, SegmentRef{
		Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
		FromTxNum: 10, ToTxNum: 11, Path: "history/state-domain-change-10-11.seg",
	}, cfg, etl.Options{TempDir: filepath.Join(dir, "etl-scratch"), BufferLimit: 1}, &stateDomainChangeHistoryBlockRange{from: 1, to: 1})
	if err != nil {
		t.Fatalf("build unordered hot history through fallback: %v", err)
	}
	if result.recordETL.Collected != 2 || result.recordETL.Applied != 2 || result.recordETL.SpilledRuns == 0 {
		t.Fatalf("record ETL fallback stats = %+v, want two externally sorted rows", result.recordETL)
	}
	if len(result.refs) != 3 {
		t.Fatalf("fallback refs = %+v, want history+accessor+index", result.refs)
	}
	changes, err := openStateDomainChangeHistoryChanges(dir, result.refs[0])
	if err != nil {
		t.Fatalf("open fallback history: %v", err)
	}
	if len(changes) != 2 || changes[0].TxNum != 10 || changes[1].TxNum != 11 {
		t.Fatalf("fallback txNums = %+v, want [10 11]", changes)
	}
	if err := verifyStateDomainChangeBinaryCompanionsAgainstSegment(dir, result.refs[0], result.refs[2], result.refs[1]); err != nil {
		t.Fatalf("verify fallback companions: %v", err)
	}
}

func TestBuildStateDomainChangeHistoryStreamHonorsCompressionGate(t *testing.T) {
	previous := CompressHistorySegments
	CompressHistorySegments = false
	defer func() { CompressHistorySegments = previous }()

	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x5b}, common.AccountIDLength)...))
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{0x01}, 10, 10); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum: 1, BlockHash: common.Hash{0x01}, TxNum: 10, Seq: 1,
		FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner,
		Domain: kvdomains.ContractStorage, Key: []byte("slot"), NextExists: true, Next: []byte("value"),
	}); err != nil {
		t.Fatal(err)
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 10, "history/state-domain-change-10-10.seg")
	if err != nil {
		t.Fatalf("build uncompressed streamed history: %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("refs = %+v, want history+accessor+index", refs)
	}
	file, err := os.Open(filepath.Join(dir, refs[0].Path))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var magic [8]byte
	if _, err := file.ReadAt(magic[:], 0); err != nil {
		t.Fatal(err)
	}
	if string(magic[:]) == compressedBlockMagic {
		t.Fatalf("streamed history segment was compressed with gate disabled")
	}
	if err := verifyStateDomainChangeBinaryCompanionsAgainstSegment(dir, refs[0], refs[2], refs[1]); err != nil {
		t.Fatalf("verify uncompressed streamed companions: %v", err)
	}
}

func TestValidateBuiltStateDomainChangeBinaryFilesRejectsCorruptTailGroup(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x5e}, common.AccountIDLength)...))
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{0x01}, 10, 10); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum: 1, BlockHash: common.Hash{0x01}, TxNum: 10, Seq: 1,
		FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner,
		Domain: kvdomains.ContractStorage, Key: []byte("slot"), NextExists: true, Next: []byte("value"),
	}); err != nil {
		t.Fatal(err)
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 10, "history/state-domain-change-10-10.seg")
	if err != nil {
		t.Fatalf("build history: %v", err)
	}
	accessorPath := filepath.Join(dir, refs[1].Path)
	accessor, header, accessorSize, err := openStateDomainChangeBinaryAccessorReader(dir, refs[1])
	if err != nil {
		t.Fatal(err)
	}
	layout, err := stateDomainChangeBinaryAccessorV4LayoutAt(accessor, accessorSize, header)
	if err != nil {
		_ = accessor.Close()
		t.Fatal(err)
	}
	groupOffset, err := readStateDomainChangeBinaryAccessorV3GroupOffsetAt(accessor, layout, layout.groupCount-1)
	if closeErr := accessor.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(accessorPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteAt(make([]byte, 8), int64(groupOffset+stateDomainChangeBinaryAccessorV3GroupKeySize))
	closeErr := file.Close()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	history, logicalSize, historyHeader, err := openHistorySegmentForRead(dir, refs[0])
	if err != nil {
		t.Fatal(err)
	}
	txRangeCount, _, err := stateDomainChangeBinaryTxRangeTableBoundsAt(history, logicalSize, refs[0], historyHeader)
	if closeErr := history.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := validateBuiltStateDomainChangeBinaryFiles(dir, refs[0], refs[2], refs[1], logicalSize, 1, historyHeader.count, txRangeCount); err == nil {
		t.Fatal("corrupt built accessor tail group passed layout self-check")
	}
}

func TestManagerRestoreStateDomainHistoryLoadsThroughSortedETL(t *testing.T) {
	dir := t.TempDir()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x5a}, common.AccountIDLength)...))
	changes := []*rawdb.StateDomainChange{
		{
			BlockNum:   20,
			BlockHash:  common.Hash{0x20},
			TxNum:      100,
			Seq:        1,
			FlatDomain: rawdb.StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 4,
			Domain:     kvdomains.ContractStorage,
			Key:        []byte("slot/a"),
			PrevExists: true,
			Prev:       []byte("old-a"),
			NextExists: true,
			Next:       []byte("new-a"),
		},
		{
			BlockNum:   21,
			BlockHash:  common.Hash{0x21},
			TxNum:      101,
			Seq:        1,
			FlatDomain: rawdb.StateFlatDomainKVGeneration,
			Owner:      owner,
			PrevExists: true,
			Prev:       rawdb.EncodeStateKVGenerationValue(3),
			NextExists: true,
			Next:       rawdb.EncodeStateKVGenerationValue(4),
		},
	}
	explicitRanges := []*rawdb.StateTxRange{
		{BlockNum: 19, BlockHash: common.Hash{0x19}, BeginTxNum: 99, EndTxNum: 99},
		{BlockNum: 20, BlockHash: common.Hash{0x20}, BeginTxNum: 100, EndTxNum: 100},
		{BlockNum: 21, BlockHash: common.Hash{0x21}, BeginTxNum: 101, EndTxNum: 101},
	}
	segRef, idxRef, accessorRef, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 99,
		ToTxNum:   101,
		Path:      "history/state-domain-change-99-101.seg",
	}, changes, explicitRanges)
	if err != nil {
		t.Fatalf("write binary history: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(99, 101, []SegmentRef{segRef, accessorRef, idxRef})); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}

	direct := newHistoryRestoreOrderWriter()
	for _, change := range changes {
		if err := rawdb.WriteStateDomainChangeRow(direct, change); err != nil {
			t.Fatalf("capture change row key: %v", err)
		}
		if err := rawdb.WriteStateDomainChangeInverseIndex(direct, change); err != nil {
			t.Fatalf("capture inverse index key: %v", err)
		}
	}
	for _, row := range explicitRanges {
		if err := rawdb.WriteStateTxRange(direct, row.BlockNum, row.BlockHash, row.BeginTxNum, row.EndTxNum); err != nil {
			t.Fatalf("capture tx-range key: %v", err)
		}
	}
	if sort.SliceIsSorted(direct.putKeys, func(i, j int) bool {
		return bytes.Compare(direct.putKeys[i], direct.putKeys[j]) < 0
	}) {
		t.Fatal("test setup produced already-sorted direct history restore order")
	}
	writer := newHistoryRestoreOrderWriter()
	result, err := mgr.RestoreStateDomainHistory(writer, 99, 101)
	if err != nil {
		t.Fatalf("RestoreStateDomainHistory: %v", err)
	}
	if result.ChangesRestored != 2 || result.TxRangesRestored != 3 {
		t.Fatalf("restore result = %+v, want 2 changes and 3 tx ranges", result)
	}
	if !sort.SliceIsSorted(writer.putKeys, func(i, j int) bool {
		return bytes.Compare(writer.putKeys[i], writer.putKeys[j]) < 0
	}) {
		t.Fatalf("history restore put keys are not sorted by physical key: %x", writer.putKeys)
	}
	if len(writer.deleteKeys) != 0 {
		t.Fatalf("history restore deletes = %d, want 0", len(writer.deleteKeys))
	}

	etlTemp := filepath.Join(t.TempDir(), "etl-scratch")
	if _, err := mgr.RestoreStateDomainHistoryWithOptions(newHistoryRestoreOrderWriter(), 99, 101, RestoreETLOptions{
		TempDir:     etlTemp,
		BufferLimit: 1,
	}); err != nil {
		t.Fatalf("RestoreStateDomainHistoryWithOptions: %v", err)
	}
	if _, err := os.Stat(etlTemp); err != nil {
		t.Fatalf("custom state history restore ETL temp dir stat: %v", err)
	}
}

func TestManagerIteratesStateDomainChangesStopsBeforeReadingRestOfBinaryRange(t *testing.T) {
	dir := t.TempDir()
	segRef, idxRef, accessorRef, changes := writeStreamingStopHistorySegment(t, dir, false)
	segRef = corruptStateDomainChangeBinaryRecordFrameLength(t, dir, segRef, idxRef, 1)
	if err := PublishManifest(dir, NewManifest(1, 2, []SegmentRef{segRef, accessorRef, idxRef})); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	var got []*rawdb.StateDomainChange
	if err := mgr.IterateStateDomainChanges(1, 2, func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change)
		return false, nil
	}); err != nil {
		t.Fatalf("stream range stopped before corrupt record: %v", err)
	}
	if len(got) != 1 || got[0].TxNum != changes[0].TxNum || string(got[0].Key) != string(changes[0].Key) {
		t.Fatalf("streamed changes = %+v", got)
	}
}

func TestManagerIteratesStateDomainChangesByKeyStopsBeforeReadingRestOfBinaryAccessor(t *testing.T) {
	dir := t.TempDir()
	segRef, idxRef, accessorRef, changes := writeStreamingStopHistorySegment(t, dir, true)
	segRef = corruptStateDomainChangeBinaryRecordFrameLength(t, dir, segRef, idxRef, 1)
	if err := PublishManifest(dir, NewManifest(1, 2, []SegmentRef{segRef, accessorRef, idxRef})); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	var got []*rawdb.StateDomainChange
	if err := mgr.IterateStateDomainChangesByKey(1, 2, rawdb.StateFlatDomainKVLatest, changes[0].Owner, changes[0].Generation, changes[0].Domain, changes[0].Key, func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change)
		return false, nil
	}); err != nil {
		t.Fatalf("stream key stopped before corrupt record: %v", err)
	}
	if len(got) != 1 || got[0].TxNum != changes[0].TxNum || string(got[0].Key) != string(changes[0].Key) {
		t.Fatalf("streamed keyed changes = %+v", got)
	}
}

func TestManagerIteratesStateDomainChangesByPrefixStopsCallbackBeforeReadingRestOfBinaryAccessor(t *testing.T) {
	dir := t.TempDir()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x5b}, common.AccountIDLength)...))
	first := binaryStateDomainChange(1, 1, 1, "slot/a")
	first.Owner = owner
	first.Generation = 5
	first.Domain = kvdomains.ContractStorage
	second := binaryStateDomainChange(2, 2, 1, "zz/b")
	second.Owner = owner
	second.Generation = 5
	second.Domain = kvdomains.ContractStorage
	segRef, idxRef, accessorRef, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 1,
		ToTxNum:   2,
		Path:      "history/state-domain-change-prefix-stream-stop.seg",
	}, []*rawdb.StateDomainChange{first, second})
	if err != nil {
		t.Fatalf("write binary history: %v", err)
	}
	segRef = corruptStateDomainChangeBinaryRecordFrameLength(t, dir, segRef, idxRef, 1)
	if err := PublishManifest(dir, NewManifest(1, 2, []SegmentRef{segRef, accessorRef, idxRef})); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	var got []*rawdb.StateDomainChange
	if err := mgr.IterateStateDomainChangesByPrefix(1, 2, owner, 5, kvdomains.ContractStorage, []byte("slot/"), func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change)
		return false, nil
	}); err != nil {
		t.Fatalf("stream prefix stopped before corrupt record: %v", err)
	}
	if len(got) != 1 || got[0].TxNum != first.TxNum || string(got[0].Key) != string(first.Key) {
		t.Fatalf("streamed prefix changes = %+v", got)
	}
}

func TestManagerIteratesStateDomainChangesByPrefixV4SeeksPastEarlierGroupRecords(t *testing.T) {
	dir := t.TempDir()
	owner := binaryAddress(0x5c)
	changes := make([]*rawdb.StateDomainChange, 0, 128)
	for i := 0; i < 128; i++ {
		change := binaryStateDomainChange(uint64(i+1), uint64(i+1), 1, "")
		change.Owner = owner
		change.Generation = 5
		change.Domain = kvdomains.ContractStorage
		change.Key = []byte{byte(i), 0x01}
		changes = append(changes, change)
	}
	segRef, idxRef, accessorRef, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 1,
		ToTxNum:   128,
		Path:      "history/state-domain-change-prefix-v4-seek.seg",
	}, changes)
	if err != nil {
		t.Fatalf("write binary history: %v", err)
	}
	if data := mustReadFile(t, filepath.Join(dir, accessorRef.Path)); binary.BigEndian.Uint32(data[8:12]) != stateDomainChangeBinaryVersionV4 {
		t.Fatalf("accessor version = %d, want %d", binary.BigEndian.Uint32(data[8:12]), stateDomainChangeBinaryVersionV4)
	}
	segRef = corruptStateDomainChangeBinaryRecordFrameLength(t, dir, segRef, idxRef, 0)
	if err := PublishManifest(dir, NewManifest(1, 128, []SegmentRef{segRef, accessorRef, idxRef})); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	var got []*rawdb.StateDomainChange
	if err := mgr.IterateStateDomainChangesByPrefix(1, 128, owner, 5, kvdomains.ContractStorage, []byte{0x7f}, func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatalf("v4 prefix seek read corrupt earlier record: %v", err)
	}
	if len(got) != 1 || !bytes.Equal(got[0].Key, []byte{0x7f, 0x01}) {
		t.Fatalf("v4 prefix result = %+v, want key 7f01", got)
	}
}

func writeStreamingStopHistorySegment(t *testing.T, dir string, sameKey bool) (SegmentRef, SegmentRef, SegmentRef, []*rawdb.StateDomainChange) {
	t.Helper()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x59}, common.AccountIDLength)...))
	first := binaryStateDomainChange(1, 1, 1, "slot/a")
	first.Owner = owner
	first.Generation = 5
	first.Domain = kvdomains.ContractStorage
	secondKey := "slot/b"
	if sameKey {
		secondKey = "slot/a"
	}
	second := binaryStateDomainChange(2, 2, 1, secondKey)
	second.Owner = owner
	second.Generation = 5
	second.Domain = kvdomains.ContractStorage
	segRef, idxRef, accessorRef, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 1,
		ToTxNum:   2,
		Path:      "history/state-domain-change-stream-stop.seg",
	}, []*rawdb.StateDomainChange{first, second})
	if err != nil {
		t.Fatalf("write binary history: %v", err)
	}
	return segRef, idxRef, accessorRef, []*rawdb.StateDomainChange{first, second}
}

func corruptStateDomainChangeBinaryRecordFrameLength(t *testing.T, dir string, segRef SegmentRef, idxRef SegmentRef, recordIndex int) SegmentRef {
	t.Helper()
	index, err := readStateDomainChangeBinaryIndex(dir, idxRef)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	if recordIndex < 0 || recordIndex >= len(index) {
		t.Fatalf("record index %d outside index length %d", recordIndex, len(index))
	}
	data := mustReadFile(t, filepath.Join(dir, segRef.Path))
	offset := index[recordIndex].offset
	if offset+4 > uint64(len(data)) {
		t.Fatalf("record offset %d outside segment size %d", offset, len(data))
	}
	binary.BigEndian.PutUint32(data[offset:offset+4], ^uint32(0))
	setStateDomainChangeBinaryRefMetadata(&segRef, data)
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, segRef.Path), data); err != nil {
		t.Fatalf("write corrupted segment: %v", err)
	}
	return segRef
}

type historyRestoreOrderWriter struct {
	putKeys    [][]byte
	deleteKeys [][]byte
}

func newHistoryRestoreOrderWriter() *historyRestoreOrderWriter {
	return &historyRestoreOrderWriter{}
}

func (w *historyRestoreOrderWriter) Put(key, value []byte) error {
	w.putKeys = append(w.putKeys, append([]byte(nil), key...))
	return nil
}

func (w *historyRestoreOrderWriter) Delete(key []byte) error {
	w.deleteKeys = append(w.deleteKeys, append([]byte(nil), key...))
	return nil
}
