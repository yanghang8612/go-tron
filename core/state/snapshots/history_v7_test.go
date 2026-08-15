package snapshots

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestStateDomainChangeIndexV7RoundTripAndCorruption(t *testing.T) {
	dir := t.TempDir()
	file, name, err := createStateDomainChangeBinaryTempFileInDir(dir, "history.idx")
	if err != nil {
		t.Fatal(err)
	}
	if err := writeStateDomainChangeBinaryHeaderToVersion(file, stateDomainChangeBinaryIndexMagic, 1_000, 2_000, 0, stateDomainChangeBinaryIndexVersion); err != nil {
		t.Fatal(err)
	}
	const count = 700
	offset := uint64(12345)
	recordIndex := uint64(0)
	for i := uint64(0); i < count; i++ {
		entry := stateDomainChangeBinaryTxOffset{txNum: 1_000 + i, offset: offset, recordIndex: recordIndex, count: 1 + i%5}
		if err := writeStateDomainChangeBinaryIndexEntryTo(file, entry); err != nil {
			t.Fatal(err)
		}
		offset += 17 + i%11
		recordIndex += entry.count
	}
	if err := writeStateDomainChangeBinaryHeaderCount(file, count); err != nil {
		t.Fatal(err)
	}
	file, name, err = rewriteStateDomainChangeBinaryIndexV7(file, name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close(); _ = os.Remove(name) }()
	stat, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if uint64(stat.Size()) >= uint64(stateDomainChangeBinaryHeaderSize)+count*stateDomainChangeBinaryIndexEntrySize {
		t.Fatalf("V7 index size %d did not shrink fixed layout", stat.Size())
	}
	header, err := readStateDomainChangeBinaryHeaderAt(file, stateDomainChangeBinaryIndexMagic)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := openStateDomainChangeBinaryIndexV7Reader(file, uint64(stat.Size()), header)
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []uint64{0, 1, 255, 256, 511, 699} {
		entry, err := readStateDomainChangeBinaryIndexEntryAt(reader, index)
		if err != nil {
			t.Fatal(err)
		}
		if entry.txNum != 1_000+index || entry.count != 1+index%5 {
			t.Fatalf("entry %d = %+v", index, entry)
		}
	}
	frame := reader.frames[1]
	var one [1]byte
	if _, err := file.ReadAt(one[:], int64(frame.dataOff)); err != nil {
		t.Fatal(err)
	}
	one[0] ^= 0x80
	if _, err := file.WriteAt(one[:], int64(frame.dataOff)); err != nil {
		t.Fatal(err)
	}
	reader.cacheValid = false
	if _, err := readStateDomainChangeBinaryIndexEntryAt(reader, 256); err == nil {
		t.Fatal("corrupt V7 index frame was accepted")
	}
}

func TestStateDomainChangeV7FramedAccessorQuery(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, make([]byte, common.AccountIDLength)...))
	changes := make([]*rawdb.StateDomainChange, 0, 300)
	for txNum := uint64(1); txNum <= 300; txNum++ {
		var hash common.Hash
		binary.BigEndian.PutUint64(hash[24:], txNum)
		if err := rawdb.WriteStateTxRange(db, txNum, hash, txNum, txNum); err != nil {
			t.Fatal(err)
		}
		change := &rawdb.StateDomainChange{
			BlockNum: txNum, BlockHash: hash, TxNum: txNum, Seq: 1,
			FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: 9,
			Domain: kvdomains.ContractStorage, Key: []byte("hot-slot"), PrevExists: true, Prev: []byte{byte(txNum)},
		}
		changes = append(changes, change)
		if err := rawdb.WriteStateDomainChange(db, change); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 300, "history/state-domain-change-1-300.seg")
	if err != nil {
		t.Fatal(err)
	}
	var historyRef, accessorRef, indexRef SegmentRef
	for _, ref := range refs {
		switch ref.Kind {
		case SegmentHistory:
			historyRef = ref
		case SegmentAccessor:
			accessorRef = ref
		case SegmentInverted:
			indexRef = ref
		}
	}
	accessor, header, size, err := openStateDomainChangeBinaryAccessorReader(dir, accessorRef)
	if err != nil {
		t.Fatal(err)
	}
	if header.version != stateDomainChangeBinaryVersionV7 {
		t.Fatalf("accessor version = %d", header.version)
	}
	if err := checkStateDomainChangeBinaryAccessorV7(accessor, size); err != nil {
		_ = accessor.Close()
		t.Fatal(err)
	}
	_ = accessor.Close()
	index, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, indexRef)
	if err != nil {
		t.Fatal(err)
	}
	if indexHeader.version != stateDomainChangeBinaryVersionV7 {
		t.Fatalf("index version = %d", indexHeader.version)
	}
	_ = index.Close()
	_, _, legacyAccessor, err := encodeStateDomainChangeBinarySegmentV6(1, 300, changes)
	if err != nil {
		t.Fatal(err)
	}
	accessorInfo, err := os.Stat(filepath.Join(dir, accessorRef.Path))
	if err != nil {
		t.Fatal(err)
	}
	indexInfo, err := os.Stat(filepath.Join(dir, indexRef.Path))
	if err != nil {
		t.Fatal(err)
	}
	if uint64(accessorInfo.Size())*2 >= uint64(len(legacyAccessor)) {
		t.Fatalf("V7 accessor size %d did not halve V6 size %d", accessorInfo.Size(), len(legacyAccessor))
	}
	legacyIndexSize := uint64(stateDomainChangeBinaryHeaderSize) + 300*stateDomainChangeBinaryIndexEntrySize
	t.Logf("V7/V6 synthetic hot-key bytes: accessor=%d/%d index=%d/%d", accessorInfo.Size(), len(legacyAccessor), indexInfo.Size(), legacyIndexSize)
	if uint64(indexInfo.Size())*2 >= legacyIndexSize {
		t.Fatalf("V7 index size %d did not halve V2 size %d", indexInfo.Size(), legacyIndexSize)
	}
	lookup := stateDomainChangeBinaryAccessorLookupKey(rawdb.StateFlatDomainKVLatest, owner, 9, kvdomains.ContractStorage, []byte("hot-slot"))
	var got []uint64
	err = iterateStateDomainChangeBinarySegmentByAccessorFile(dir, historyRef, accessorRef, lookup, 130, 260, func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change.TxNum)
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 131 || got[0] != 130 || got[len(got)-1] != 260 {
		t.Fatalf("framed lookup = %d rows [%d,%d]", len(got), got[0], got[len(got)-1])
	}
}
