package rawdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestStateTxRangeRoundTrip(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	hash := common.Hash{0xaa}
	if _, ok, err := ReadStateTxRange(db, 7); err != nil || ok {
		t.Fatalf("pre-read = ok:%v err:%v", ok, err)
	}
	if err := WriteStateTxRange(db, 7, hash, 7, 7); err != nil {
		t.Fatalf("write tx range: %v", err)
	}
	got, ok, err := ReadStateTxRange(db, 7)
	if err != nil || !ok {
		t.Fatalf("read tx range = ok:%v err:%v", ok, err)
	}
	if got.BlockNum != 7 || got.BlockHash != hash || got.BeginTxNum != 7 || got.EndTxNum != 7 {
		t.Fatalf("range = %+v", got)
	}
}

func TestNextStateTxRangeUsesCompactGlobalSequence(t *testing.T) {
	begin, end, err := NextStateTxRange(41, 3)
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if begin != 42 || end != 45 {
		t.Fatalf("range = [%d,%d], want [42,45]", begin, end)
	}
	txNum, err := StateTxNumAt(begin, 2)
	if err != nil {
		t.Fatalf("tx num at: %v", err)
	}
	if txNum != 44 {
		t.Fatalf("tx num = %d, want 44", txNum)
	}
	if _, err := StateTxNumAt(^uint64(0), 1); err == nil {
		t.Fatal("expected overflowing ordinal to fail")
	}
	if _, _, err := NextStateTxRange(^uint64(0), 0); err == nil {
		t.Fatal("expected overflowing parent end to fail")
	}
}

func TestStateTxNumAtBlockEndUsesStoredRangeAndLegacyFallback(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	got, err := StateTxNumAtBlockEnd(db, 7)
	if err != nil {
		t.Fatal(err)
	}
	if got != 7 {
		t.Fatalf("fallback tx num = %d, want legacy block number 7", got)
	}
	begin, end, err := NextStateTxRange(41, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteStateTxRange(db, 7, common.Hash{0x07}, begin, end); err != nil {
		t.Fatal(err)
	}
	got, err = StateTxNumAtBlockEnd(db, 7)
	if err != nil {
		t.Fatal(err)
	}
	if got != end {
		t.Fatalf("stored end tx num = %d, want %d", got, end)
	}
}

func TestStateDomainChangeRoundTripAndIteration(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	change1 := &StateDomainChange{
		BlockNum:   9,
		BlockHash:  common.Hash{0x09},
		TxNum:      9,
		Seq:        1,
		FlatDomain: StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 3,
		Domain:     kvdomains.SystemReward,
		Key:        []byte("reward/1"),
		PrevExists: true,
		Prev:       []byte("old"),
		NextExists: true,
		Next:       []byte("new"),
	}
	change2 := &StateDomainChange{
		BlockNum:   9,
		BlockHash:  common.Hash{0x09},
		TxNum:      9,
		Seq:        2,
		FlatDomain: StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 3,
		Domain:     kvdomains.SystemReward,
		Key:        []byte("reward/2"),
		PrevExists: true,
		Prev:       []byte("gone"),
	}
	if err := WriteStateDomainChange(db, change1); err != nil {
		t.Fatalf("write change1: %v", err)
	}
	if err := WriteStateDomainChange(db, change2); err != nil {
		t.Fatalf("write change2: %v", err)
	}

	got, ok, err := ReadStateDomainChange(db, 9, 1)
	if err != nil || !ok {
		t.Fatalf("read change = ok:%v err:%v", ok, err)
	}
	if got.FlatDomain != StateFlatDomainKVLatest || got.Domain != kvdomains.SystemReward || !bytes.Equal(got.Prev, []byte("old")) || !bytes.Equal(got.Next, []byte("new")) {
		t.Fatalf("change = %+v", got)
	}
	got.Prev[0] = 'x'
	reread, _, _ := ReadStateDomainChange(db, 9, 1)
	if bytes.Equal(reread.Prev, got.Prev) {
		t.Fatal("ReadStateDomainChange returned aliased bytes")
	}

	var seqs []uint64
	if err := IterateStateDomainChanges(db, 9, func(change *StateDomainChange) (bool, error) {
		seqs = append(seqs, change.Seq)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate changes: %v", err)
	}
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("seqs = %v", seqs)
	}

	var blocks []uint64
	if err := IterateStateDomainChangeBlocks(db, owner, 3, kvdomains.SystemReward, []byte("reward/1"), func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate inverse: %v", err)
	}
	if len(blocks) != 1 || blocks[0] != 9 {
		t.Fatalf("inverse blocks = %v", blocks)
	}
}

func TestStateDomainChangeRowAndInverseIndexPublishSeparately(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x22}
	change := &StateDomainChange{
		BlockNum:   10,
		BlockHash:  common.Hash{0x10},
		TxNum:      42,
		Seq:        1,
		FlatDomain: StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 4,
		Domain:     kvdomains.SystemReward,
		Key:        []byte("reward/split"),
		PrevExists: true,
		Prev:       []byte("old"),
		NextExists: true,
		Next:       []byte("new"),
	}
	if err := WriteStateDomainChangeRow(db, change); err != nil {
		t.Fatalf("write row: %v", err)
	}
	if got, ok, err := ReadStateDomainChange(db, 10, 1); err != nil || !ok || !bytes.Equal(got.Next, []byte("new")) {
		t.Fatalf("read row = %+v ok:%v err:%v", got, ok, err)
	}
	var blocks []uint64
	if err := IterateStateDomainChangeBlocks(db, owner, 4, kvdomains.SystemReward, []byte("reward/split"), func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate before index: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("row-only publish created inverse blocks %v", blocks)
	}
	if err := WriteStateDomainChangeInverseIndex(db, change); err != nil {
		t.Fatalf("write inverse index: %v", err)
	}
	if err := IterateStateDomainChangeBlocks(db, owner, 4, kvdomains.SystemReward, []byte("reward/split"), func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate after index: %v", err)
	}
	if len(blocks) != 1 || blocks[0] != 10 {
		t.Fatalf("inverse blocks = %v, want [10]", blocks)
	}
}

func TestIterateStateDomainChangeBlocksByKeyDispatchesFlatDomains(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x23}
	changes := []*StateDomainChange{
		{
			BlockNum:   11,
			TxNum:      11,
			Seq:        1,
			FlatDomain: StateFlatDomainAccountLatest,
			Owner:      owner,
			NextExists: true,
			Next:       []byte("account"),
		},
		{
			BlockNum:   12,
			TxNum:      12,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 5,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("reward/generic"),
			NextExists: true,
			Next:       []byte("kv"),
		},
		{
			BlockNum:   13,
			TxNum:      13,
			Seq:        1,
			FlatDomain: StateFlatDomainKVGeneration,
			Owner:      owner,
			NextExists: true,
			Next:       EncodeStateKVGenerationValue(5),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change %d: %v", change.BlockNum, err)
		}
	}
	tests := []struct {
		name       string
		flatDomain StateFlatDomain
		generation uint64
		domain     kvdomains.KVDomain
		key        []byte
		want       uint64
	}{
		{name: "account", flatDomain: StateFlatDomainAccountLatest, want: 11},
		{name: "kv", flatDomain: StateFlatDomainKVLatest, generation: 5, domain: kvdomains.SystemReward, key: []byte("reward/generic"), want: 12},
		{name: "generation", flatDomain: StateFlatDomainKVGeneration, want: 13},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var blocks []uint64
			if err := IterateStateDomainChangeBlocksByKey(db, tt.flatDomain, owner, tt.generation, tt.domain, tt.key, func(blockNum uint64) (bool, error) {
				blocks = append(blocks, blockNum)
				return true, nil
			}); err != nil {
				t.Fatalf("iterate: %v", err)
			}
			if len(blocks) != 1 || blocks[0] != tt.want {
				t.Fatalf("blocks = %v, want [%d]", blocks, tt.want)
			}
		})
	}
}

func TestIterateStateDomainChangesByKeyFiltersTxWindowAndKey(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x24}
	other := common.Address{0x41, 0x25}
	for _, row := range []StateTxRange{
		{BlockNum: 20, BlockHash: common.Hash{0x20}, BeginTxNum: 20, EndTxNum: 20},
		{BlockNum: 21, BlockHash: common.Hash{0x21}, BeginTxNum: 21, EndTxNum: 21},
		{BlockNum: 22, BlockHash: common.Hash{0x22}, BeginTxNum: 22, EndTxNum: 22},
	} {
		if err := WriteStateTxRange(db, row.BlockNum, row.BlockHash, row.BeginTxNum, row.EndTxNum); err != nil {
			t.Fatalf("write range %d: %v", row.BlockNum, err)
		}
	}
	changes := []*StateDomainChange{
		{
			BlockNum:   20,
			TxNum:      20,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("reward/a"),
			NextExists: true,
			Next:       []byte("too-old"),
		},
		{
			BlockNum:   21,
			TxNum:      21,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("reward/a"),
			NextExists: true,
			Next:       []byte("match"),
		},
		{
			BlockNum:   21,
			TxNum:      21,
			Seq:        2,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      other,
			Generation: 1,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("reward/a"),
			NextExists: true,
			Next:       []byte("other-owner"),
		},
		{
			BlockNum:   22,
			TxNum:      22,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("reward/b"),
			NextExists: true,
			Next:       []byte("other-key"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change %+v: %v", change, err)
		}
	}
	var got []*StateDomainChange
	if err := IterateStateDomainChangesByKey(db, 20, 21, StateFlatDomainKVLatest, owner, 1, kvdomains.SystemReward, []byte("reward/a"), func(change *StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate changes: %v", err)
	}
	if len(got) != 1 || got[0].BlockNum != 21 || string(got[0].Next) != "match" {
		t.Fatalf("changes = %+v, want only block 21 match", got)
	}
}

func TestIterateStateDomainChangesByKeyBlockRangeSeeksPastOldHistory(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x27}
	for _, blockNum := range []uint64{1, 100, 101} {
		if err := WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatalf("write range %d: %v", blockNum, err)
		}
		if err := WriteStateDomainChange(db, &StateDomainChange{
			BlockNum:   blockNum,
			TxNum:      blockNum,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("reward/every-block"),
			NextExists: true,
			Next:       []byte{byte(blockNum)},
		}); err != nil {
			t.Fatalf("write change %d: %v", blockNum, err)
		}
	}
	// A bounded iterator must neither point-read history before its lower
	// block bound nor continue beyond its upper bound. It also does not need
	// StateTxRange for the in-range block: the block bounds already imply that
	// every candidate change is inside the requested end-of-block tx window.
	// Malformed rows on all three blocks make any accidental range read fail
	// deterministically.
	if err := db.Put(stateTxRangeKey(1), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(stateTxRangeKey(100), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(stateTxRangeKey(101), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	var got []*StateDomainChange
	if err := IterateStateDomainChangesByKeyBlockRange(db, 99, 100, 99, 100, StateFlatDomainKVLatest, owner, 1, kvdomains.SystemReward, []byte("reward/every-block"), func(change *StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate bounded changes: %v", err)
	}
	if len(got) != 1 || got[0].BlockNum != 100 {
		t.Fatalf("bounded changes = %+v, want only block 100", got)
	}
}

func TestReadStateKVAsOfTxNumStopsAtFirstSubsequentChange(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x28}
	key := []byte("reward/frequent")
	for _, blockNum := range []uint64{2, 3} {
		if err := WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatalf("write range %d: %v", blockNum, err)
		}
		if err := WriteStateDomainChange(db, &StateDomainChange{
			BlockNum:   blockNum,
			TxNum:      blockNum,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     kvdomains.SystemReward,
			Key:        key,
			PrevExists: true,
			Prev:       []byte{byte(blockNum - 2)},
			NextExists: true,
			Next:       []byte{byte(blockNum - 1)},
		}); err != nil {
			t.Fatalf("write change %d: %v", blockNum, err)
		}
	}
	// The second block remains in the inverse index but its changeset row is
	// unreadable. A point-in-time read at tx 1 only needs block 2's Prev and
	// must stop before touching block 3.
	if err := db.Put(stateChangeSetKey(3, 1), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	value, ok, err := ReadStateKVAsOfTxNum(db, owner, 1, kvdomains.SystemReward, key, 1, 3)
	if err != nil {
		t.Fatalf("read as of first change: %v", err)
	}
	if !ok || !bytes.Equal(value, []byte{0}) {
		t.Fatalf("value = %x ok=%v, want 00/true", value, ok)
	}
}

func TestReadFirstStateDomainChangeByKeyBlockRangeUsesPrefixSeek(t *testing.T) {
	base := ethrawdb.NewMemoryDatabase()
	db := &prefixSeekingHistoryDB{Database: base}
	owner := common.Address{0x41, 0x29}
	key := []byte("reward/seek")
	for _, blockNum := range []uint64{100, 101} {
		if err := WriteStateDomainChange(db, &StateDomainChange{
			BlockNum: blockNum, TxNum: blockNum, Seq: 1,
			FlatDomain: StateFlatDomainKVLatest, Owner: owner, Generation: 1,
			Domain: kvdomains.SystemReward, Key: key,
			PrevExists: true, Prev: []byte{byte(blockNum)}, NextExists: true, Next: []byte{byte(blockNum + 1)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	change, err := ReadFirstStateDomainChangeByKeyBlockRange(db, 99, 101, 99, 101, StateFlatDomainKVLatest, owner, 1, kvdomains.SystemReward, key)
	if err != nil {
		t.Fatal(err)
	}
	if change == nil || change.BlockNum != 100 {
		t.Fatalf("first change = %+v, want block 100", change)
	}
	if db.seekCalls != 1 || db.inverseIteratorCalls != 0 {
		t.Fatalf("seek calls = %d inverse iterator calls = %d, want 1/0", db.seekCalls, db.inverseIteratorCalls)
	}
}

type prefixSeekingHistoryDB struct {
	ethdb.Database
	seekCalls            int
	inverseIteratorCalls int
}

func (db *prefixSeekingHistoryDB) SeekPrefix(prefix, start []byte) (key, value []byte, ok bool, err error) {
	db.seekCalls++
	it := db.Database.NewIterator(prefix, start)
	defer it.Release()
	if !it.Next() {
		return nil, nil, false, it.Error()
	}
	return append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...), true, nil
}

func (db *prefixSeekingHistoryDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	if bytes.HasPrefix(prefix, stateChangeInversePrefix) {
		db.inverseIteratorCalls++
	}
	return db.Database.NewIterator(prefix, start)
}

func TestIterateStateDomainChangesByPrefixFiltersTxWindowAndPrefix(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x26}
	for _, row := range []StateTxRange{
		{BlockNum: 30, BlockHash: common.Hash{0x30}, BeginTxNum: 30, EndTxNum: 30},
		{BlockNum: 31, BlockHash: common.Hash{0x31}, BeginTxNum: 31, EndTxNum: 31},
	} {
		if err := WriteStateTxRange(db, row.BlockNum, row.BlockHash, row.BeginTxNum, row.EndTxNum); err != nil {
			t.Fatalf("write range %d: %v", row.BlockNum, err)
		}
	}
	changes := []*StateDomainChange{
		{
			BlockNum:   30,
			TxNum:      30,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 2,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("acct/a"),
			NextExists: true,
			Next:       []byte("too-old"),
		},
		{
			BlockNum:   31,
			TxNum:      31,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 2,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("acct/a"),
			NextExists: true,
			Next:       []byte("a"),
		},
		{
			BlockNum:   31,
			TxNum:      31,
			Seq:        2,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 2,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("other/b"),
			NextExists: true,
			Next:       []byte("b"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change %+v: %v", change, err)
		}
	}
	var got []*StateDomainChange
	if err := IterateStateDomainChangesByPrefix(db, 30, 31, owner, 2, kvdomains.SystemReward, []byte("acct/"), func(change *StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate prefix changes: %v", err)
	}
	if len(got) != 1 || got[0].BlockNum != 31 || string(got[0].Key) != "acct/a" {
		t.Fatalf("prefix changes = %+v, want only acct/a at block 31", got)
	}
}

func TestIterateStateDomainChangesByTxRangeSameBlock(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x21}
	begin, end, err := NextStateTxRange(100, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteStateTxRange(db, 12, common.Hash{0x12}, begin, end); err != nil {
		t.Fatal(err)
	}
	for i, txNum := range []uint64{begin, begin + 1, end} {
		if err := WriteStateDomainChange(db, &StateDomainChange{
			BlockNum:   12,
			BlockHash:  common.Hash{0x12},
			TxNum:      txNum,
			Seq:        uint64(i + 1),
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     kvdomains.SystemReward,
			Key:        []byte{byte('a' + i)},
			NextExists: true,
			Next:       []byte{byte('1' + i)},
		}); err != nil {
			t.Fatalf("write change %d: %v", i, err)
		}
	}

	var got []uint64
	if err := IterateStateDomainChangesByTxRange(db, begin+1, begin+1, func(change *StateDomainChange) (bool, error) {
		got = append(got, change.Seq)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate tx range: %v", err)
	}
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("seqs in tx range = %v, want [2]", got)
	}
}

func TestStateDomainChangeRejectsUntypedRows(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	err := WriteStateDomainChange(db, &StateDomainChange{
		BlockNum: 1,
		TxNum:    1,
		Seq:      1,
		Owner:    common.Address{0x41, 0x01},
		Domain:   kvdomains.SystemReward,
		Key:      []byte("legacy"),
	})
	if err == nil {
		t.Fatal("untyped generic KV changeset row accepted")
	}
}

func TestDeleteStateDomainChangesUsesPointDeletes(t *testing.T) {
	db := &rangeDeleteCountingStore{KeyValueStore: ethrawdb.NewMemoryDatabase()}
	owner := common.Address{0x41, 0x01}
	for seq, key := range [][]byte{[]byte("reward/1"), []byte("reward/2")} {
		if err := WriteStateDomainChange(db, &StateDomainChange{
			BlockNum:   9,
			BlockHash:  common.Hash{0x09},
			TxNum:      9,
			Seq:        uint64(seq + 1),
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 3,
			Domain:     kvdomains.SystemReward,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("old"),
			NextExists: true,
			Next:       []byte("new"),
		}); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	if err := DeleteStateDomainChanges(db, 9); err != nil {
		t.Fatalf("delete changes: %v", err)
	}
	if db.rangeDeletes != 0 {
		t.Fatalf("DeleteStateDomainChanges used DeleteRange %d time(s)", db.rangeDeletes)
	}
	rows := 0
	if err := IterateStateDomainChanges(db, 9, func(change *StateDomainChange) (bool, error) {
		rows++
		return true, nil
	}); err != nil {
		t.Fatalf("iterate changes: %v", err)
	}
	if rows != 0 {
		t.Fatalf("forward changes survived: %d", rows)
	}
	var blocks []uint64
	if err := IterateStateDomainChangeBlocks(db, owner, 3, kvdomains.SystemReward, []byte("reward/1"), func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate inverse: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("inverse blocks survived: %v", blocks)
	}
}

func TestDeleteStateDomainChangesDoesNotRescanDeferredDeletes(t *testing.T) {
	base := ethrawdb.NewMemoryDatabase()
	owner := common.Address{common.AddressPrefixMainnet, 0x01}
	rows := resetScanBatch + 1
	for seq := 1; seq <= rows; seq++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(seq))
		if err := WriteStateDomainChange(base, &StateDomainChange{
			BlockNum:   9,
			BlockHash:  common.Hash{0x09},
			TxNum:      uint64(seq),
			Seq:        uint64(seq),
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 3,
			Domain:     kvdomains.SystemReward,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("old"),
			NextExists: true,
			Next:       []byte("new"),
		}); err != nil {
			t.Fatalf("write change %d: %v", seq, err)
		}
	}

	// Reads and iterators deliberately see only base. Deletes remain invisible
	// until Flush, matching pruning's committed-reader/uncommitted-batch store.
	deferred := newDeferredDeleteStateStore(base, rows*2+100)
	if err := DeleteStateDomainChanges(deferred, 9); err != nil {
		t.Fatalf("delete deferred state changes: %v", err)
	}
	if deferred.deleteCalls != rows*2 {
		t.Fatalf("delete calls = %d, want %d", deferred.deleteCalls, rows*2)
	}
	if err := deferred.Flush(); err != nil {
		t.Fatalf("flush deferred deletes: %v", err)
	}
	remaining := 0
	if err := IterateStateDomainChanges(base, 9, func(*StateDomainChange) (bool, error) {
		remaining++
		return true, nil
	}); err != nil {
		t.Fatalf("iterate remaining changes: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("remaining state changes = %d", remaining)
	}
}

type deferredDeleteStateStore struct {
	base           ethdb.KeyValueStore
	pending        ethdb.Batch
	deleteCalls    int
	maxDeleteCalls int
}

func newDeferredDeleteStateStore(base ethdb.KeyValueStore, maxDeleteCalls int) *deferredDeleteStateStore {
	return &deferredDeleteStateStore{
		base:           base,
		pending:        base.NewBatch(),
		maxDeleteCalls: maxDeleteCalls,
	}
}

func (s *deferredDeleteStateStore) Has(key []byte) (bool, error) {
	return s.base.Has(key)
}

func (s *deferredDeleteStateStore) Get(key []byte) ([]byte, error) {
	return s.base.Get(key)
}

func (s *deferredDeleteStateStore) Put(key, value []byte) error {
	return s.base.Put(key, value)
}

func (s *deferredDeleteStateStore) Delete(key []byte) error {
	s.deleteCalls++
	if s.maxDeleteCalls > 0 && s.deleteCalls > s.maxDeleteCalls {
		return fmt.Errorf("deferred delete limit exceeded: %d", s.deleteCalls)
	}
	return s.pending.Delete(key)
}

func (s *deferredDeleteStateStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	return s.base.NewIterator(prefix, start)
}

func (s *deferredDeleteStateStore) Flush() error {
	defer s.pending.Reset()
	return s.pending.Write()
}

type rangeDeleteCountingStore struct {
	ethdb.KeyValueStore
	rangeDeletes int
}

func (db *rangeDeleteCountingStore) DeleteRange(start, end []byte) error {
	db.rangeDeletes++
	return db.KeyValueStore.DeleteRange(start, end)
}

func TestReadStateKVAsOfRollsBackChanges(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	key := []byte("history/key")

	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemReward, key, []byte("v7"))
	changes := []*StateDomainChange{
		{
			BlockNum:   3,
			TxNum:      3,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     kvdomains.SystemReward,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("v2"),
			NextExists: true,
			Next:       []byte("v3"),
		},
		{
			BlockNum:   5,
			TxNum:      5,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     kvdomains.SystemReward,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("v3"),
			NextExists: true,
			Next:       []byte("v5"),
		},
		{
			BlockNum:   7,
			TxNum:      7,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     kvdomains.SystemReward,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("v5"),
			NextExists: true,
			Next:       []byte("v7"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	tests := []struct {
		block uint64
		want  []byte
	}{
		{7, []byte("v7")},
		{6, []byte("v5")},
		{5, []byte("v5")},
		{4, []byte("v3")},
		{3, []byte("v3")},
		{2, []byte("v2")},
	}
	for _, tt := range tests {
		got, ok, err := ReadStateKVAsOf(db, owner, 0, kvdomains.SystemReward, key, tt.block, 7)
		if err != nil || !ok || !bytes.Equal(got, tt.want) {
			t.Fatalf("as-of block %d = %q ok:%v err:%v, want %q", tt.block, got, ok, err, tt.want)
		}
	}
}

func TestReadStateAccountLatestAsOfTxNum(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x42}
	begin, end, err := NextStateTxRange(100, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteStateTxRange(db, 15, common.Hash{0x15}, begin, end); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateAccountLatest(db, owner, []byte("account-v2")); err != nil {
		t.Fatal(err)
	}
	changes := []*StateDomainChange{
		{
			BlockNum:   15,
			BlockHash:  common.Hash{0x15},
			TxNum:      begin,
			Seq:        1,
			FlatDomain: StateFlatDomainAccountLatest,
			Owner:      owner,
			NextExists: true,
			Next:       []byte("account-v1"),
		},
		{
			BlockNum:   15,
			BlockHash:  common.Hash{0x15},
			TxNum:      begin + 1,
			Seq:        2,
			FlatDomain: StateFlatDomainAccountLatest,
			Owner:      owner,
			PrevExists: true,
			Prev:       []byte("account-v1"),
			NextExists: true,
			Next:       []byte("account-v2"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatal(err)
		}
	}
	got, ok, err := ReadStateAccountLatestAsOfTxNum(db, owner, begin, end)
	if err != nil || !ok || !bytes.Equal(got, []byte("account-v1")) {
		t.Fatalf("account as-of tx0 = %q ok=%v err=%v", got, ok, err)
	}
	got, ok, err = ReadStateAccountLatestAsOfTxNum(db, owner, begin+1, end)
	if err != nil || !ok || !bytes.Equal(got, []byte("account-v2")) {
		t.Fatalf("account as-of tx1 = %q ok=%v err=%v", got, ok, err)
	}
	got, ok, err = ReadStateAccountLatestAsOfTxNum(db, owner, begin-1, end)
	if err != nil || ok {
		t.Fatalf("account before creation = %q ok=%v err=%v", got, ok, err)
	}
}

func TestReadStateKVAsOfHandlesCreatedKey(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	key := []byte("created")
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemReward, key, []byte("new"))
	if err := WriteStateDomainChange(db, &StateDomainChange{
		BlockNum:   4,
		TxNum:      4,
		Seq:        1,
		FlatDomain: StateFlatDomainKVLatest,
		Owner:      owner,
		Domain:     kvdomains.SystemReward,
		Key:        key,
		NextExists: true,
		Next:       []byte("new"),
	}); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := ReadStateKVAsOf(db, owner, 0, kvdomains.SystemReward, key, 3, 4); err != nil || ok {
		t.Fatalf("created key before creation = %q ok:%v err:%v", got, ok, err)
	}
}

func TestReadStateKVAsOfTxNumWithinBlock(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x22}
	domain := kvdomains.SystemReward
	key := []byte("txnum/key")
	begin, end, err := NextStateTxRange(100, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteStateTxRange(db, 13, common.Hash{0x13}, begin, end); err != nil {
		t.Fatal(err)
	}
	mustWriteStateKVLatest(t, db, owner, 0, domain, key, []byte("v2"))
	changes := []*StateDomainChange{
		{
			BlockNum:   13,
			TxNum:      begin,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     domain,
			Key:        key,
			NextExists: true,
			Next:       []byte("v1"),
		},
		{
			BlockNum:   13,
			TxNum:      begin + 1,
			Seq:        2,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     domain,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("v1"),
			NextExists: true,
			Next:       []byte("v2"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	tests := []struct {
		target uint64
		want   string
		ok     bool
	}{
		{end, "v2", true},
		{begin + 1, "v2", true},
		{begin, "v1", true},
		{begin - 1, "", false},
	}
	for _, tt := range tests {
		got, ok, err := ReadStateKVAsOfTxNum(db, owner, 0, domain, key, tt.target, end)
		if err != nil || ok != tt.ok || string(got) != tt.want {
			t.Fatalf("as-of tx %d = %q ok:%v err:%v, want %q ok:%v", tt.target, got, ok, err, tt.want, tt.ok)
		}
	}
}

func TestIterateStateKVAsOfPrefixRollsBackRange(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	domain := kvdomains.SystemReward

	mustWriteStateKVLatest(t, db, owner, 0, domain, []byte("acct/a"), []byte("a3"))
	mustWriteStateKVLatest(t, db, owner, 0, domain, []byte("acct/b"), []byte("b3"))
	mustWriteStateKVLatest(t, db, owner, 0, domain, []byte("other/c"), []byte("c3"))
	changes := []*StateDomainChange{
		{
			BlockNum:   2,
			TxNum:      2,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     domain,
			Key:        []byte("acct/a"),
			PrevExists: true,
			Prev:       []byte("a1"),
			NextExists: true,
			Next:       []byte("a2"),
		},
		{
			BlockNum:   3,
			TxNum:      3,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     domain,
			Key:        []byte("acct/a"),
			PrevExists: true,
			Prev:       []byte("a2"),
			NextExists: true,
			Next:       []byte("a3"),
		},
		{
			BlockNum:   3,
			TxNum:      3,
			Seq:        2,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     domain,
			Key:        []byte("acct/b"),
			NextExists: true,
			Next:       []byte("b3"),
		},
		{
			BlockNum:   3,
			TxNum:      3,
			Seq:        3,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     domain,
			Key:        []byte("other/c"),
			PrevExists: true,
			Prev:       []byte("c2"),
			NextExists: true,
			Next:       []byte("c3"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	got := make(map[string]string)
	if err := IterateStateKVAsOfPrefix(db, owner, 0, domain, []byte("acct/"), 2, 3, func(key, value []byte) (bool, error) {
		got[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate as-of prefix: %v", err)
	}
	if len(got) != 1 || got["acct/a"] != "a2" {
		t.Fatalf("as-of prefix at block 2 = %v, want only acct/a=a2", got)
	}

	got = make(map[string]string)
	if err := IterateStateKVAsOfPrefix(db, owner, 0, domain, []byte("acct/"), 3, 3, func(key, value []byte) (bool, error) {
		got[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate head prefix: %v", err)
	}
	if len(got) != 2 || got["acct/a"] != "a3" || got["acct/b"] != "b3" {
		t.Fatalf("as-of prefix at head = %v", got)
	}
}

func TestReadStateAccountKVAsOfCrossesGenerationReset(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x02}
	domain := kvdomains.SystemReward
	key := []byte("cycle")

	if err := WriteStateKVGeneration(db, owner, 1); err != nil {
		t.Fatal(err)
	}
	mustWriteStateKVLatest(t, db, owner, 0, domain, key, []byte("old2"))
	mustWriteStateKVLatest(t, db, owner, 1, domain, key, []byte("new"))
	changes := []*StateDomainChange{
		{
			BlockNum:   2,
			TxNum:      2,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 0,
			Domain:     domain,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("old1"),
			NextExists: true,
			Next:       []byte("old2"),
		},
		{
			BlockNum:   3,
			TxNum:      3,
			Seq:        1,
			FlatDomain: StateFlatDomainKVGeneration,
			Owner:      owner,
			NextExists: true,
			Next:       EncodeStateKVGenerationValue(1),
		},
		{
			BlockNum:   4,
			TxNum:      4,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     domain,
			Key:        key,
			NextExists: true,
			Next:       []byte("new"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	tests := []struct {
		block uint64
		want  string
		ok    bool
	}{
		{4, "new", true},
		{3, "", false},
		{2, "old2", true},
		{1, "old1", true},
	}
	for _, tt := range tests {
		got, ok, err := ReadStateAccountKVAsOf(db, owner, domain, key, tt.block, 4)
		if err != nil || ok != tt.ok || string(got) != tt.want {
			t.Fatalf("account kv as-of block %d = %q ok:%v err:%v, want %q ok:%v", tt.block, got, ok, err, tt.want, tt.ok)
		}
	}
	if gen, ok, err := ReadStateKVGenerationAsOf(db, owner, 2, 4); err != nil || ok || gen != 0 {
		t.Fatalf("generation as-of block 2 = %d ok:%v err:%v, want default 0 without row", gen, ok, err)
	}
}

func TestReadStateAccountKVAsOfTxNumCrossesGenerationResetWithinBlock(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x23}
	domain := kvdomains.SystemReward
	key := []byte("generation/txnum")
	begin, end, err := NextStateTxRange(100, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteStateTxRange(db, 14, common.Hash{0x14}, begin, end); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateKVGeneration(db, owner, 1); err != nil {
		t.Fatal(err)
	}
	mustWriteStateKVLatest(t, db, owner, 0, domain, key, []byte("old"))
	mustWriteStateKVLatest(t, db, owner, 1, domain, key, []byte("new"))
	changes := []*StateDomainChange{
		{
			BlockNum:   14,
			TxNum:      begin,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 0,
			Domain:     domain,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("old0"),
			NextExists: true,
			Next:       []byte("old"),
		},
		{
			BlockNum:   14,
			TxNum:      begin + 1,
			Seq:        2,
			FlatDomain: StateFlatDomainKVGeneration,
			Owner:      owner,
			NextExists: true,
			Next:       EncodeStateKVGenerationValue(1),
		},
		{
			BlockNum:   14,
			TxNum:      begin + 1,
			Seq:        3,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     domain,
			Key:        key,
			NextExists: true,
			Next:       []byte("new"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	tests := []struct {
		target uint64
		want   string
		ok     bool
	}{
		{begin + 1, "new", true},
		{begin, "old", true},
		{begin - 1, "old0", true},
	}
	for _, tt := range tests {
		got, ok, err := ReadStateAccountKVAsOfTxNum(db, owner, domain, key, tt.target, end)
		if err != nil || ok != tt.ok || string(got) != tt.want {
			t.Fatalf("account kv as-of tx %d = %q ok:%v err:%v, want %q ok:%v", tt.target, got, ok, err, tt.want, tt.ok)
		}
	}
}

func TestIterateStateAccountKVAsOfPrefixCrossesGenerationReset(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x03}
	domain := kvdomains.SystemReward

	if err := WriteStateKVGeneration(db, owner, 1); err != nil {
		t.Fatal(err)
	}
	mustWriteStateKVLatest(t, db, owner, 0, domain, []byte("acct/a"), []byte("a2"))
	mustWriteStateKVLatest(t, db, owner, 0, domain, []byte("acct/b"), []byte("b2"))
	mustWriteStateKVLatest(t, db, owner, 1, domain, []byte("acct/c"), []byte("c4"))
	changes := []*StateDomainChange{
		{
			BlockNum:   2,
			TxNum:      2,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 0,
			Domain:     domain,
			Key:        []byte("acct/a"),
			PrevExists: true,
			Prev:       []byte("a1"),
			NextExists: true,
			Next:       []byte("a2"),
		},
		{
			BlockNum:   3,
			TxNum:      3,
			Seq:        1,
			FlatDomain: StateFlatDomainKVGeneration,
			Owner:      owner,
			NextExists: true,
			Next:       EncodeStateKVGenerationValue(1),
		},
		{
			BlockNum:   4,
			TxNum:      4,
			Seq:        1,
			FlatDomain: StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 1,
			Domain:     domain,
			Key:        []byte("acct/c"),
			NextExists: true,
			Next:       []byte("c4"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	got := make(map[string]string)
	if err := IterateStateAccountKVAsOfPrefix(db, owner, domain, []byte("acct/"), 2, 4, func(key, value []byte) (bool, error) {
		got[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate account kv as-of prefix: %v", err)
	}
	if len(got) != 2 || got["acct/a"] != "a2" || got["acct/b"] != "b2" {
		t.Fatalf("account kv prefix as-of block 2 = %v, want acct/a=a2 acct/b=b2", got)
	}

	got = make(map[string]string)
	if err := IterateStateAccountKVAsOfPrefix(db, owner, domain, []byte("acct/"), 1, 4, func(key, value []byte) (bool, error) {
		got[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate account kv as-of prefix: %v", err)
	}
	if len(got) != 2 || got["acct/a"] != "a1" || got["acct/b"] != "b2" {
		t.Fatalf("account kv prefix as-of block 1 = %v, want acct/a=a1 acct/b=b2", got)
	}
}
