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

func TestStateDomainChangeRoundTripAndIteration(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	change1 := &StateDomainChange{
		BlockNum:   9,
		BlockHash:  common.Hash{0x09},
		TxNum:      9,
		Seq:        1,
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
	if got.Domain != kvdomains.SystemReward || !bytes.Equal(got.Prev, []byte("old")) || !bytes.Equal(got.Next, []byte("new")) {
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

func TestDeleteStateDomainChangesUsesPointDeletes(t *testing.T) {
	db := &rangeDeleteCountingStore{KeyValueStore: ethrawdb.NewMemoryDatabase()}
	owner := common.Address{0x41, 0x01}
	for seq, key := range [][]byte{[]byte("reward/1"), []byte("reward/2")} {
		if err := WriteStateDomainChange(db, &StateDomainChange{
			BlockNum:   9,
			BlockHash:  common.Hash{0x09},
			TxNum:      9,
			Seq:        uint64(seq + 1),
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

func TestUnwindStateDomainChangesRestoresLatestIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	createdKey := []byte("created")
	updatedKey := []byte("updated")
	deletedKey := []byte("deleted")

	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemReward, createdKey, []byte("new"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemReward, updatedKey, []byte("new"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemReward, deletedKey, []byte("old"))
	_ = DeleteStateKVLatest(db, owner, 0, kvdomains.SystemReward, deletedKey)

	changes := []*StateDomainChange{
		{
			BlockNum:   10,
			TxNum:      10,
			Seq:        1,
			Owner:      owner,
			Domain:     kvdomains.SystemReward,
			Key:        createdKey,
			NextExists: true,
			Next:       []byte("new"),
		},
		{
			BlockNum:   10,
			TxNum:      10,
			Seq:        2,
			Owner:      owner,
			Domain:     kvdomains.SystemReward,
			Key:        updatedKey,
			PrevExists: true,
			Prev:       []byte("old"),
			NextExists: true,
			Next:       []byte("new"),
		},
		{
			BlockNum:   10,
			TxNum:      10,
			Seq:        3,
			Owner:      owner,
			Domain:     kvdomains.SystemReward,
			Key:        deletedKey,
			PrevExists: true,
			Prev:       []byte("old"),
		},
	}
	for _, change := range changes {
		if err := WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}

	if err := UnwindStateDomainChanges(db, 10); err != nil {
		t.Fatalf("unwind: %v", err)
	}
	if _, ok, err := ReadStateKVLatest(db, owner, 0, kvdomains.SystemReward, createdKey); err != nil || ok {
		t.Fatalf("created key after unwind = ok:%v err:%v", ok, err)
	}
	if got, ok, err := ReadStateKVLatest(db, owner, 0, kvdomains.SystemReward, updatedKey); err != nil || !ok || !bytes.Equal(got, []byte("old")) {
		t.Fatalf("updated key after unwind = %q ok:%v err:%v", got, ok, err)
	}
	if got, ok, err := ReadStateKVLatest(db, owner, 0, kvdomains.SystemReward, deletedKey); err != nil || !ok || !bytes.Equal(got, []byte("old")) {
		t.Fatalf("deleted key after unwind = %q ok:%v err:%v", got, ok, err)
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

func TestReadStateKVAsOfHandlesCreatedKey(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	key := []byte("created")
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemReward, key, []byte("new"))
	if err := WriteStateDomainChange(db, &StateDomainChange{
		BlockNum:   4,
		TxNum:      4,
		Seq:        1,
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
