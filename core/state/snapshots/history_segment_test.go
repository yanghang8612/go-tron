package snapshots

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

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
