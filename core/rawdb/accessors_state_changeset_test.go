package rawdb

import (
	"bytes"
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

	var iterated []*StateDomainChange
	if err := IterateStateDomainChanges(db, 9, func(change *StateDomainChange) (bool, error) {
		iterated = append(iterated, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate changes: %v", err)
	}
	if len(iterated) != 2 || iterated[0] == iterated[1] || iterated[0].Seq != 1 || iterated[1].Seq != 2 {
		t.Fatalf("iterated changes = %+v", iterated)
	}
	if !bytes.Equal(iterated[0].Prev, []byte("old")) || !bytes.Equal(iterated[1].Prev, []byte("gone")) {
		t.Fatalf("retained iterator values = %q, %q", iterated[0].Prev, iterated[1].Prev)
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

func BenchmarkWriteStateDomainChangeAccountPayload(b *testing.B) {
	db := ethrawdb.NewMemoryDatabase()
	payload := bytes.Repeat([]byte{0xab}, 16*1024)
	change := &StateDomainChange{
		BlockNum:   10,
		BlockHash:  common.Hash{0x10},
		TxNum:      42,
		Seq:        1,
		FlatDomain: StateFlatDomainAccountLatest,
		Owner:      common.Address{0x41, 0x22},
		PrevExists: true,
		Prev:       payload,
		NextExists: true,
		Next:       payload,
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(change.Prev) + len(change.Next)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := WriteStateDomainChangeRow(db, change); err != nil {
			b.Fatal(err)
		}
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
