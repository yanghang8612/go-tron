package rawdb

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
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
}
