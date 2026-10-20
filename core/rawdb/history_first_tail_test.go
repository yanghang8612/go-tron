package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// A posting miss must not hide an authoritative unindexed tail mutation.
func TestFirstStateDomainChangePostingMissStillReadsUnindexedTail(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	defer db.Close()
	owner := common.Address{0x41, 0x6a}
	key := []byte("tail-only-key")
	row := &StateDomainChange{
		BlockNum: 3, TxNum: 3, Seq: 1, FlatDomain: StateFlatDomainKVLatest,
		Owner: owner, Generation: 7, Domain: kvdomains.ContractStorage, Key: key,
		PrevExists: true, Prev: []byte("before"), NextExists: true, Next: []byte("after"),
	}
	if err := WriteStateDomainChangeRow(db, row); err != nil {
		t.Fatal(err)
	}
	if err := WriteStageProgressWithHash(db, StageStateHistoryIndex, 2, common.Hash{2}); err != nil {
		t.Fatal(err)
	}
	var collected []*StateDomainChange
	if err := IterateStateDomainChangesByKeyBlockRange(db, 1, 3, 1, 3,
		StateFlatDomainKVLatest, owner, 7, kvdomains.ContractStorage, key,
		func(change *StateDomainChange) (bool, error) { collected = append(collected, change); return true, nil }); err != nil {
		t.Fatal(err)
	}
	if len(collected) != 1 || collected[0].BlockNum != 3 {
		t.Fatalf("full collector control: %+v", collected)
	}
	first, err := ReadFirstStateDomainChangeByKeyBlockRange(db, 1, 3, 1, 3,
		StateFlatDomainKVLatest, owner, 7, kvdomains.ContractStorage, key)
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || first.BlockNum != 3 {
		t.Fatalf("first-change seek missed unindexed block 3 after empty indexed span [2,2]: got %+v; full collector returned correct block 3", first)
	}
}
